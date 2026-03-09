"""
Extended backtesting with Kelly criterion, drawdown analysis, and monthly breakdown.
Supports 3-model ensemble: LightGBM + XGBoost + CatBoost.
"""

import numpy as np
import pandas as pd
import joblib
from config import FEATURE_NAMES, CATEGORICAL_FEATURES, MODEL_DIR


def load_model_and_data():
    meta = joblib.load(MODEL_DIR / "meta.pkl")
    lgbm_model = joblib.load(MODEL_DIR / "lgbm_model.pkl")
    medians = meta["medians"]
    feature_names = meta["feature_names"]

    # Load optional models
    xgb_model = None
    catboost_model = None
    meta_learner = None
    try:
        xgb_model = joblib.load(MODEL_DIR / "xgb_ranker.pkl")
    except FileNotFoundError:
        pass
    try:
        catboost_model = joblib.load(MODEL_DIR / "catboost_model.pkl")
    except FileNotFoundError:
        pass
    try:
        meta_learner = joblib.load(MODEL_DIR / "meta_learner.pkl")
    except FileNotFoundError:
        pass

    ensemble_weights = meta.get("ensemble_weights", {"lgbm": 1.0, "xgb": 0.0, "catboost": 0.0})

    df = pd.read_parquet("data/training_data.parquet")
    df = df.sort_values("race_date")
    dates = df["race_date"].unique()
    cutoff = dates[int(len(dates) * 0.83)]
    test = df[df["race_date"] > cutoff].copy()

    available = [f for f in feature_names if f in test.columns]
    X = test[available].copy()
    for col in X.columns:
        if col in medians:
            X[col] = X[col].fillna(medians[col])
        else:
            X[col] = X[col].fillna(X[col].median())

    # LightGBM predictions
    lgbm_probs = lgbm_model.predict_proba(X)[:, 1]
    test["lgbm_prob"] = lgbm_probs

    # XGBoost predictions (softmax per race)
    if xgb_model is not None:
        # Need sorted by race_id for XGBoost
        test_sorted = test.sort_values("race_id").reset_index(drop=True)
        X_sorted = test_sorted[available].copy()
        for col in X_sorted.columns:
            if col in medians:
                X_sorted[col] = X_sorted[col].fillna(medians[col])
            else:
                X_sorted[col] = X_sorted[col].fillna(X_sorted[col].median())

        xgb_scores = xgb_model.predict(X_sorted)
        test_sorted["xgb_score"] = xgb_scores

        def race_softmax(group):
            scores = group["xgb_score"].values
            exp_scores = np.exp(scores - scores.max())
            probs = exp_scores / exp_scores.sum()
            group = group.copy()
            group["xgb_prob"] = probs
            return group

        test_sorted = test_sorted.groupby("race_id", group_keys=False).apply(race_softmax)
        # Re-sort back to date order
        test = test.merge(test_sorted[["race_id", "horse_id", "xgb_prob"]],
                          on=["race_id", "horse_id"], how="left")
        test["xgb_prob"] = test["xgb_prob"].fillna(0)
    else:
        test["xgb_prob"] = 0

    # CatBoost predictions (convert categoricals to string)
    if catboost_model is not None:
        X_cb = X.copy()
        for name in CATEGORICAL_FEATURES:
            if name in X_cb.columns:
                X_cb[name] = X_cb[name].fillna(-1).astype(int).astype(str)
        catboost_probs = catboost_model.predict_proba(X_cb)[:, 1]
        test["catboost_prob"] = catboost_probs
    else:
        test["catboost_prob"] = 0

    # Ensemble
    w = ensemble_weights
    test["model_prob"] = (
        w["lgbm"] * test["lgbm_prob"] +
        w["xgb"] * test["xgb_prob"] +
        w["catboost"] * test["catboost_prob"]
    )

    print(f"Ensemble weights: lgbm={w['lgbm']:.2f}, xgb={w['xgb']:.2f}, catboost={w['catboost']:.2f}")
    return test


def simulate_betting(df, min_edge=0.05, kelly_fraction=0.25, initial_bankroll=1000):
    """Simulate betting with Kelly criterion staking."""
    df = df.dropna(subset=["sp"]).copy()
    df["market_prob"] = 1.0 / df["sp"]
    df["edge"] = df["model_prob"] - df["market_prob"]

    bets = df[df["edge"] > min_edge].copy()

    if len(bets) == 0:
        print(f"No qualifying bets found at edge > {min_edge:.0%}.")
        return

    # Kelly staking
    bets["kelly_full"] = (bets["model_prob"] * bets["sp"] - 1) / (bets["sp"] - 1)
    bets["kelly_stake"] = np.clip(bets["kelly_full"] * kelly_fraction, 0, 0.05)

    bankroll = initial_bankroll
    bankroll_history = [bankroll]
    peak = bankroll

    for _, bet in bets.iterrows():
        stake = bankroll * bet["kelly_stake"]
        if bet["won"] == 1:
            profit = stake * (bet["sp"] - 1)
        else:
            profit = -stake

        bankroll += profit
        bankroll_history.append(bankroll)
        peak = max(peak, bankroll)

    total_bets = len(bets)
    winners = bets["won"].sum()
    strike_rate = winners / total_bets
    final_bankroll = bankroll
    roi = (final_bankroll - initial_bankroll) / initial_bankroll * 100
    max_drawdown = min((b - peak) / peak * 100 for b, peak_val in
                       zip(bankroll_history, [max(bankroll_history[:i+1]) for i in range(len(bankroll_history))]))

    print(f"\n{'='*50}")
    print(f"BETTING SIMULATION RESULTS (edge > {min_edge:.0%})")
    print(f"{'='*50}")
    print(f"Kelly fraction:     {kelly_fraction}")
    print(f"Total bets:         {total_bets}")
    print(f"Winners:            {winners} ({strike_rate:.1%})")
    print(f"Initial bankroll:   ${initial_bankroll:,.0f}")
    print(f"Final bankroll:     ${final_bankroll:,.0f}")
    print(f"ROI:                {roi:+.1f}%")
    print(f"Max drawdown:       {max_drawdown:.1f}%")
    print(f"Avg edge on bets:   {bets['edge'].mean():.1%}")
    print(f"Avg odds on bets:   {bets['sp'].mean():.1f}")

    # Monthly breakdown
    bets["month"] = pd.to_datetime(bets["race_date"]).dt.to_period("M")
    monthly = bets.groupby("month").agg(
        bets_count=("won", "count"),
        winners=("won", "sum"),
        avg_sp=("sp", "mean"),
        avg_edge=("edge", "mean"),
    )
    monthly["strike_rate"] = monthly["winners"] / monthly["bets_count"]
    monthly["flat_pnl"] = bets.groupby("month").apply(
        lambda x: (x["sp"] * x["won"] - 1).sum()
    )
    print(f"\n{'='*50}")
    print(f"MONTHLY BREAKDOWN")
    print(f"{'='*50}")
    print(monthly.to_string())

    return bets


def main():
    print("Loading model and test data...")
    test = load_model_and_data()
    print(f"Test set: {len(test)} runners, {test['won'].sum()} winners")

    for edge in [0.03, 0.05, 0.08, 0.10]:
        simulate_betting(test, min_edge=edge)


if __name__ == "__main__":
    main()
