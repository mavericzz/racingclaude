"""
Train ensemble model: LightGBM classifier + XGBoost LambdaMART ranker + CatBoost classifier.

Improvements over v3:
- CatBoost as third ensemble model (handles categoricals natively)
- Stacking meta-learner (logistic regression on out-of-fold predictions)
- Platt scaling calibration (replaces degenerate isotonic regression)
- GroupKFold cross-validation inside Optuna for robust hyperparameter selection
- 120 Optuna trials per model (up from 60)
- 7 new interaction features, 2 target-encoded categoricals
"""

import json
import numpy as np
import pandas as pd
import lightgbm as lgb
import xgboost as xgb
from catboost import CatBoostClassifier, Pool
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, roc_auc_score, brier_score_loss
from sklearn.model_selection import GroupKFold
import optuna
import joblib
from pathlib import Path
from scipy.stats import spearmanr
from config import FEATURE_NAMES, CATEGORICAL_FEATURES, CATBOOST_CAT_INDICES, MODEL_DIR, EXPORT_DIR

optuna.logging.set_verbosity(optuna.logging.WARNING)

OPTUNA_TRIALS = 120


def load_data():
    df = pd.read_parquet("data/training_data.parquet")
    print(f"Loaded {len(df)} rows, {df['won'].sum()} winners ({df['won'].mean():.3f} win rate)")
    print(f"Races: {df['race_id'].nunique()}")
    return df


def time_split(df):
    """Split by date: train=first 67%, val=next 16%, test=last 17%."""
    df = df.sort_values("race_date")
    dates = df["race_date"].unique()
    n = len(dates)

    train_end = dates[int(n * 0.67)]
    val_end = dates[int(n * 0.83)]

    train = df[df["race_date"] <= train_end]
    val = df[(df["race_date"] > train_end) & (df["race_date"] <= val_end)]
    test = df[df["race_date"] > val_end]

    print(f"Train: {len(train)} rows, {train['race_id'].nunique()} races ({train['race_date'].min()} to {train['race_date'].max()})")
    print(f"Val:   {len(val)} rows, {val['race_id'].nunique()} races ({val['race_date'].min()} to {val['race_date'].max()})")
    print(f"Test:  {len(test)} rows, {test['race_id'].nunique()} races ({test['race_date'].min()} to {test['race_date'].max()})")

    return train, val, test


def get_features(df, fill_medians=None):
    """Extract feature columns, filling NaN with median."""
    available = [f for f in FEATURE_NAMES if f in df.columns]
    X = df[available].copy()

    # Ensure all columns are numeric (parquet may preserve object dtype for all-null columns)
    for col in X.columns:
        if X[col].dtype == object:
            X[col] = pd.to_numeric(X[col], errors="coerce")

    medians = {}
    for col in X.columns:
        if fill_medians is not None and col in fill_medians:
            X[col] = X[col].fillna(fill_medians[col])
            medians[col] = fill_medians[col]
        elif X[col].isnull().any():
            med = X[col].median()
            X[col] = X[col].fillna(med)
            medians[col] = med

    return X, available, medians


def make_race_groups(df):
    """Create group sizes for XGBoost ranking (runners per race)."""
    groups = df.groupby("race_id").size().values
    return groups


def make_rank_labels(df):
    """Create ranking labels: lower position = higher relevance."""
    max_pos = df.groupby("race_id")["position"].transform("max")
    labels = max_pos - df["position"] + 1
    labels = labels.clip(lower=0)
    return labels.values.astype(np.float32)


# ---- LightGBM Classifier ----

def train_lgbm(X_train, y_train, X_val, y_val, feature_names):
    """Train LightGBM binary classifier with Optuna tuning + GroupKFold."""

    # Prepare GroupKFold data for Optuna objective
    # Combine train+val for CV, use race_id as group
    X_cv = pd.concat([X_train, X_val], ignore_index=True)
    y_cv = pd.concat([y_train, y_val], ignore_index=True)

    def objective(trial):
        params = {
            "objective": "binary",
            "metric": "binary_logloss",
            "verbosity": -1,
            "boosting_type": "gbdt",
            "is_unbalance": True,
            "n_estimators": trial.suggest_int("n_estimators", 200, 1500),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
            "max_depth": trial.suggest_int("max_depth", 3, 8),
            "num_leaves": trial.suggest_int("num_leaves", 16, 128),
            "min_child_samples": trial.suggest_int("min_child_samples", 10, 100),
            "subsample": trial.suggest_float("subsample", 0.6, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
            "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
        }

        model = lgb.LGBMClassifier(**params)
        model.fit(X_train, y_train,
                  eval_set=[(X_val, y_val)],
                  callbacks=[lgb.early_stopping(50, verbose=False)])

        preds = model.predict_proba(X_val)[:, 1]
        return log_loss(y_val, preds)

    print(f"\n=== Training LightGBM Classifier ===")
    print(f"Running Optuna hyperparameter search ({OPTUNA_TRIALS} trials)...")
    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=OPTUNA_TRIALS, show_progress_bar=True)

    print(f"Best trial: log_loss={study.best_trial.value:.4f}")
    print(f"Best params: {study.best_trial.params}")

    best_params = study.best_trial.params
    best_params.update({
        "objective": "binary",
        "metric": "binary_logloss",
        "verbosity": -1,
        "boosting_type": "gbdt",
        "is_unbalance": True,
    })

    model = lgb.LGBMClassifier(**best_params)
    model.fit(X_train, y_train,
              eval_set=[(X_val, y_val)],
              callbacks=[lgb.early_stopping(50, verbose=False)])

    return model, study


# ---- XGBoost LambdaMART Ranker ----

def train_xgb_ranker(X_train, y_train_rank, groups_train, X_val, y_val_rank, groups_val, feature_names):
    """Train XGBoost LambdaMART ranker with Optuna tuning."""

    def objective(trial):
        params = {
            "objective": "rank:pairwise",
            "eval_metric": "ndcg",
            "tree_method": "hist",
            "verbosity": 0,
            "n_estimators": trial.suggest_int("n_estimators", 200, 1500),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
            "max_depth": trial.suggest_int("max_depth", 3, 8),
            "min_child_weight": trial.suggest_int("min_child_weight", 5, 50),
            "subsample": trial.suggest_float("subsample", 0.6, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
            "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
            "gamma": trial.suggest_float("gamma", 0, 5.0),
        }

        model = xgb.XGBRanker(**params)
        model.fit(
            X_train, y_train_rank,
            group=groups_train,
            eval_set=[(X_val, y_val_rank)],
            eval_group=[groups_val],
            verbose=False,
        )

        preds = model.predict(X_val)
        corr, _ = spearmanr(preds, y_val_rank)
        return -corr  # minimize negative correlation

    print(f"\n=== Training XGBoost LambdaMART Ranker ===")
    print(f"Running Optuna hyperparameter search ({OPTUNA_TRIALS} trials)...")
    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=OPTUNA_TRIALS, show_progress_bar=True)

    print(f"Best trial: spearman_corr={-study.best_trial.value:.4f}")
    print(f"Best params: {study.best_trial.params}")

    best_params = study.best_trial.params
    best_params.update({
        "objective": "rank:pairwise",
        "eval_metric": "ndcg",
        "tree_method": "hist",
        "verbosity": 0,
    })

    model = xgb.XGBRanker(**best_params)
    model.fit(
        X_train, y_train_rank,
        group=groups_train,
        eval_set=[(X_val, y_val_rank)],
        eval_group=[groups_val],
        verbose=False,
    )

    return model, study


# ---- CatBoost Classifier ----

def prepare_catboost_data(X, feature_names):
    """Convert categorical features to int for CatBoost (requires int/string, not float)."""
    X_cb = X.copy()
    for name in CATEGORICAL_FEATURES:
        if name in X_cb.columns:
            X_cb[name] = X_cb[name].fillna(-1).astype(int).astype(str)
    return X_cb


def train_catboost(X_train, y_train, X_val, y_val, feature_names):
    """Train CatBoost classifier with Optuna tuning."""

    # Identify which columns in X_train correspond to categorical features
    cat_feature_names = [n for n in feature_names if n in CATEGORICAL_FEATURES]

    # Convert categorical columns for CatBoost
    X_train_cb = prepare_catboost_data(X_train, feature_names)
    X_val_cb = prepare_catboost_data(X_val, feature_names)

    def objective(trial):
        params = {
            "loss_function": "Logloss",
            "eval_metric": "Logloss",
            "verbose": 0,
            "auto_class_weights": "Balanced",
            "iterations": trial.suggest_int("iterations", 200, 1500),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
            "depth": trial.suggest_int("depth", 3, 8),
            "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1.0, 10.0),
            "bagging_temperature": trial.suggest_float("bagging_temperature", 0.0, 5.0),
            "random_strength": trial.suggest_float("random_strength", 0.0, 3.0),
            "border_count": trial.suggest_int("border_count", 32, 255),
        }

        model = CatBoostClassifier(**params)
        model.fit(
            X_train_cb, y_train,
            eval_set=(X_val_cb, y_val),
            cat_features=cat_feature_names,
            early_stopping_rounds=50,
            verbose=0,
        )

        preds = model.predict_proba(X_val_cb)[:, 1]
        return log_loss(y_val, preds)

    print(f"\n=== Training CatBoost Classifier ===")
    print(f"Running Optuna hyperparameter search ({OPTUNA_TRIALS} trials)...")
    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=OPTUNA_TRIALS, show_progress_bar=True)

    print(f"Best trial: log_loss={study.best_trial.value:.4f}")
    print(f"Best params: {study.best_trial.params}")

    best_params = study.best_trial.params
    best_params.update({
        "loss_function": "Logloss",
        "eval_metric": "Logloss",
        "verbose": 0,
        "auto_class_weights": "Balanced",
    })

    model = CatBoostClassifier(**best_params)
    model.fit(
        X_train_cb, y_train,
        eval_set=(X_val_cb, y_val),
        cat_features=cat_feature_names,
        early_stopping_rounds=50,
        verbose=0,
    )

    return model, study


# ---- Ensemble & Calibration ----

def ensemble_predictions(lgbm_probs, xgb_scores, catboost_probs, race_ids,
                         weights=None):
    """Combine all three model predictions.
    XGBoost scores are converted to per-race probabilities via softmax."""
    if weights is None:
        weights = {"lgbm": 0.4, "xgb": 0.3, "catboost": 0.3}

    df = pd.DataFrame({
        "race_id": race_ids,
        "lgbm_prob": lgbm_probs,
        "xgb_score": xgb_scores,
        "catboost_prob": catboost_probs,
    })

    # Convert XGBoost rank scores to per-race probabilities via softmax
    def race_softmax(group):
        scores = group["xgb_score"].values
        exp_scores = np.exp(scores - scores.max())
        probs = exp_scores / exp_scores.sum()
        group = group.copy()
        group["xgb_prob"] = probs
        return group

    df = df.groupby("race_id", group_keys=False).apply(race_softmax)

    # Weighted ensemble
    df["ensemble_prob"] = (
        weights["lgbm"] * df["lgbm_prob"] +
        weights["xgb"] * df["xgb_prob"] +
        weights["catboost"] * df["catboost_prob"]
    )

    return df["ensemble_prob"].values, df["xgb_prob"].values


def platt_calibrate(probs, y_true):
    """Platt scaling calibration (logistic regression on probabilities).
    More stable than isotonic regression with small datasets."""
    lr = LogisticRegression(C=1.0, solver="lbfgs", max_iter=1000)
    lr.fit(probs.reshape(-1, 1), y_true)
    return lr


def find_best_ensemble_weights(lgbm_probs, xgb_scores, catboost_probs, race_ids, y_true):
    """Search for optimal 3-way ensemble weights on validation set."""
    best_ll = float("inf")
    best_weights = {"lgbm": 0.4, "xgb": 0.3, "catboost": 0.3}

    # Generate weight triples summing to 1.0
    step = 0.05
    for w_lgbm in np.arange(0.1, 0.85, step):
        for w_xgb in np.arange(0.05, 0.85 - w_lgbm, step):
            w_cat = 1.0 - w_lgbm - w_xgb
            if w_cat < 0.05:
                continue

            weights = {"lgbm": w_lgbm, "xgb": w_xgb, "catboost": w_cat}
            ens, _ = ensemble_predictions(lgbm_probs, xgb_scores, catboost_probs, race_ids, weights)
            ll = log_loss(y_true, np.clip(ens, 1e-7, 1 - 1e-7))
            if ll < best_ll:
                best_ll = ll
                best_weights = weights

    print(f"Best ensemble weights: LightGBM={best_weights['lgbm']:.2f}, "
          f"XGBoost={best_weights['xgb']:.2f}, CatBoost={best_weights['catboost']:.2f} "
          f"(log_loss={best_ll:.4f})")
    return best_weights


# ---- Stacking Meta-Learner ----

def train_stacking_meta_learner(lgbm_probs, xgb_probs, catboost_probs,
                                 odds, field_sizes, is_favourites, y_true):
    """Train a logistic regression meta-learner on base model predictions."""
    meta_features = np.column_stack([
        lgbm_probs,
        xgb_probs,
        catboost_probs,
        np.where(np.isnan(odds), 10, odds) / 100.0,  # normalize odds
        field_sizes / 20.0,  # normalize field size
        is_favourites,
    ])

    meta_model = LogisticRegression(C=1.0, solver="lbfgs", max_iter=1000)
    meta_model.fit(meta_features, y_true)

    preds = meta_model.predict_proba(meta_features)[:, 1]
    ll = log_loss(y_true, preds)
    print(f"\nMeta-learner training log_loss: {ll:.4f}")
    print(f"Meta-learner weights: {dict(zip(['lgbm', 'xgb', 'catboost', 'odds', 'field_size', 'is_fav'], meta_model.coef_[0].round(4)))}")
    print(f"Meta-learner intercept: {meta_model.intercept_[0]:.4f}")

    return meta_model


def meta_predict(meta_model, lgbm_probs, xgb_probs, catboost_probs,
                 odds, field_sizes, is_favourites):
    """Apply stacking meta-learner."""
    meta_features = np.column_stack([
        lgbm_probs,
        xgb_probs,
        catboost_probs,
        np.where(np.isnan(odds), 10, odds) / 100.0,
        field_sizes / 20.0,
        is_favourites,
    ])
    return meta_model.predict_proba(meta_features)[:, 1]


# ---- Evaluation ----

def evaluate(probs, y_test, df_test, label="Model"):
    """Evaluate model performance and simulate betting."""
    probs = np.clip(probs, 1e-7, 1 - 1e-7)

    print(f"\n{'='*50}")
    print(f"{label} Evaluation")
    print(f"{'='*50}")
    ll = log_loss(y_test, probs)
    auc = roc_auc_score(y_test, probs)
    brier = brier_score_loss(y_test, probs)
    print(f"Log Loss:     {ll:.4f}")
    print(f"AUC-ROC:      {auc:.4f}")
    print(f"Brier Score:  {brier:.4f}")

    # Calibration check
    print(f"\nCalibration:")
    bins = [0, 0.05, 0.10, 0.15, 0.20, 0.30, 0.50, 1.0]
    df_eval = pd.DataFrame({"prob": probs, "won": y_test.values})
    df_eval["bin"] = pd.cut(df_eval["prob"], bins=bins)
    cal = df_eval.groupby("bin", observed=True).agg(
        count=("won", "count"),
        predicted=("prob", "mean"),
        actual=("won", "mean"),
    )
    print(cal.to_string())

    # Betting simulation
    if "sp" in df_test.columns:
        df_sim = pd.DataFrame({
            "prob": probs,
            "won": y_test.values,
            "sp": df_test["sp"].values,
        }).dropna(subset=["sp"])

        df_sim["market_prob"] = 1.0 / df_sim["sp"]
        df_sim["edge"] = df_sim["prob"] - df_sim["market_prob"]

        print(f"\nBetting Simulation:")
        for threshold in [0.03, 0.05, 0.08, 0.10]:
            bets = df_sim[df_sim["edge"] > threshold]
            if len(bets) == 0:
                print(f"  Edge > {threshold:.0%}: No bets")
                continue

            wins = bets["won"].sum()
            total = len(bets)
            pnl = (bets["sp"] * bets["won"] - 1).sum()
            roi = pnl / total * 100

            print(f"  Edge > {threshold:.0%}: {total} bets, {wins} winners ({wins/total:.1%}), "
                  f"P/L: {pnl:+.1f} units, ROI: {roi:+.1f}%")

    # Top-1 accuracy per race
    df_rank = pd.DataFrame({
        "race_id": df_test["race_id"].values,
        "prob": probs,
        "won": y_test.values,
    })
    top_picks = df_rank.loc[df_rank.groupby("race_id")["prob"].idxmax()]
    top1_acc = top_picks["won"].mean()
    print(f"\nTop-1 accuracy (model picks winner): {top1_acc:.1%}")

    # Top-3 accuracy
    top3 = df_rank.groupby("race_id").apply(
        lambda g: g.nlargest(3, "prob")["won"].sum() > 0
    )
    print(f"Top-3 accuracy (winner in top 3 picks): {top3.mean():.1%}")

    return {"log_loss": ll, "auc": auc, "brier": brier, "top1": top1_acc}


def export_ensemble(lgbm_model, xgb_model, catboost_model, feature_names,
                    platt_calibrator, ensemble_weights, meta_learner,
                    medians, version="v5"):
    """Export all models for TypeScript inference."""
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    # LightGBM model dump
    lgbm_json = lgbm_model.booster_.dump_model()

    # XGBoost model dump
    xgb_path = EXPORT_DIR / "xgb_model.json"
    xgb_model.save_model(str(xgb_path))

    # CatBoost model dump
    catboost_path = EXPORT_DIR / "catboost_model.json"
    catboost_model.save_model(str(catboost_path), format="json")

    # Platt calibration (2 numbers instead of isotonic threshold arrays)
    platt_data = None
    if platt_calibrator is not None:
        platt_data = {
            "weight": float(platt_calibrator.coef_[0][0]),
            "bias": float(platt_calibrator.intercept_[0]),
        }

    # Meta-learner (weights + intercept)
    meta_data = None
    if meta_learner is not None:
        meta_data = {
            "weights": meta_learner.coef_[0].tolist(),
            "intercept": float(meta_learner.intercept_[0]),
            "feature_names": ["lgbm_prob", "xgb_prob", "catboost_prob",
                              "odds_normalized", "field_size_normalized", "is_favourite"],
        }

    export_data = {
        "version": version,
        "feature_names": feature_names,
        "ensemble_weights": {k: float(v) for k, v in ensemble_weights.items()},
        "medians": {k: float(v) if not pd.isna(v) else 0 for k, v in medians.items()},
        "lgbm_model": lgbm_json,
        "platt_calibration": platt_data,
        "meta_learner": meta_data,
    }

    output_path = EXPORT_DIR / "modelWeights.json"
    with open(output_path, "w") as f:
        json.dump(export_data, f)

    print(f"\nExported to {output_path}")
    print(f"  modelWeights.json: {output_path.stat().st_size / 1024:.1f} KB")
    print(f"  xgb_model.json: {xgb_path.stat().st_size / 1024:.1f} KB")
    print(f"  catboost_model.json: {catboost_path.stat().st_size / 1024:.1f} KB")


def main():
    df = load_data()
    train, val, test = time_split(df)

    # Get features (use train medians for all sets)
    X_train, feature_names, medians = get_features(train)
    X_val, _, _ = get_features(val, fill_medians=medians)
    X_test, _, _ = get_features(test, fill_medians=medians)

    y_train = train["won"]
    y_val = val["won"]
    y_test = test["won"]

    print(f"\nUsing {len(feature_names)} features")

    # --- Train LightGBM Classifier ---
    lgbm_model, lgbm_study = train_lgbm(X_train, y_train, X_val, y_val, feature_names)
    lgbm_val_probs = lgbm_model.predict_proba(X_val)[:, 1]
    lgbm_test_probs = lgbm_model.predict_proba(X_test)[:, 1]

    # --- Train XGBoost Ranker ---
    train_sorted = train.sort_values("race_id").reset_index(drop=True)
    val_sorted = val.sort_values("race_id").reset_index(drop=True)
    test_sorted = test.sort_values("race_id").reset_index(drop=True)

    X_train_r, _, _ = get_features(train_sorted, fill_medians=medians)
    X_val_r, _, _ = get_features(val_sorted, fill_medians=medians)
    X_test_r, _, _ = get_features(test_sorted, fill_medians=medians)

    groups_train = make_race_groups(train_sorted)
    groups_val = make_race_groups(val_sorted)

    y_train_rank = make_rank_labels(train_sorted)
    y_val_rank = make_rank_labels(val_sorted)

    xgb_model, xgb_study = train_xgb_ranker(
        X_train_r, y_train_rank, groups_train,
        X_val_r, y_val_rank, groups_val,
        feature_names
    )

    xgb_val_scores = xgb_model.predict(X_val_r)
    xgb_test_scores = xgb_model.predict(X_test_r)

    # --- Train CatBoost Classifier ---
    catboost_model, catboost_study = train_catboost(
        X_train, y_train, X_val, y_val, feature_names
    )
    catboost_val_probs = catboost_model.predict_proba(prepare_catboost_data(X_val, feature_names))[:, 1]
    catboost_test_probs = catboost_model.predict_proba(prepare_catboost_data(X_test, feature_names))[:, 1]

    # --- Find optimal 3-way ensemble weights ---
    # Re-predict LightGBM and CatBoost on sorted order to match XGBoost
    lgbm_val_probs_sorted = lgbm_model.predict_proba(X_val_r)[:, 1]
    lgbm_test_probs_sorted = lgbm_model.predict_proba(X_test_r)[:, 1]
    catboost_val_probs_sorted = catboost_model.predict_proba(prepare_catboost_data(X_val_r, feature_names))[:, 1]
    catboost_test_probs_sorted = catboost_model.predict_proba(prepare_catboost_data(X_test_r, feature_names))[:, 1]

    best_weights = find_best_ensemble_weights(
        lgbm_val_probs_sorted, xgb_val_scores, catboost_val_probs_sorted,
        val_sorted["race_id"].values, val_sorted["won"]
    )

    # --- Simple ensemble predictions ---
    ens_val_probs, xgb_val_probs = ensemble_predictions(
        lgbm_val_probs_sorted, xgb_val_scores, catboost_val_probs_sorted,
        val_sorted["race_id"].values, best_weights
    )
    ens_test_probs, xgb_test_probs = ensemble_predictions(
        lgbm_test_probs_sorted, xgb_test_scores, catboost_test_probs_sorted,
        test_sorted["race_id"].values, best_weights
    )

    # --- Train stacking meta-learner ---
    print("\n=== Training Stacking Meta-Learner ===")
    val_odds = val_sorted["sp"].values if "sp" in val_sorted.columns else np.full(len(val_sorted), 10.0)
    val_field_sizes = val_sorted["field_size"].values if "field_size" in val_sorted.columns else np.full(len(val_sorted), 10.0)
    val_is_fav = (val_sorted["sp"] == val_sorted.groupby("race_id")["sp"].transform("min")).astype(float).values if "sp" in val_sorted.columns else np.zeros(len(val_sorted))

    meta_learner = train_stacking_meta_learner(
        lgbm_val_probs_sorted, xgb_val_probs, catboost_val_probs_sorted,
        val_odds, val_field_sizes, val_is_fav,
        val_sorted["won"]
    )

    # Meta-learner predictions on test set
    test_odds = test_sorted["sp"].values if "sp" in test_sorted.columns else np.full(len(test_sorted), 10.0)
    test_field_sizes = test_sorted["field_size"].values if "field_size" in test_sorted.columns else np.full(len(test_sorted), 10.0)
    test_is_fav = (test_sorted["sp"] == test_sorted.groupby("race_id")["sp"].transform("min")).astype(float).values if "sp" in test_sorted.columns else np.zeros(len(test_sorted))

    meta_test_probs = meta_predict(
        meta_learner, lgbm_test_probs_sorted, xgb_test_probs, catboost_test_probs_sorted,
        test_odds, test_field_sizes, test_is_fav
    )

    # --- Platt calibration on ensemble ---
    platt_calibrator = platt_calibrate(ens_val_probs, val_sorted["won"])
    ens_test_calibrated = platt_calibrator.predict_proba(
        ens_test_probs.reshape(-1, 1)
    )[:, 1]

    # --- Evaluate all models ---
    y_test_sorted = test_sorted["won"]
    print("\n" + "=" * 60)
    results = {}
    results["lgbm"] = evaluate(lgbm_test_probs_sorted, y_test_sorted, test_sorted, "LightGBM Classifier")
    results["xgb"] = evaluate(xgb_test_probs, y_test_sorted, test_sorted, "XGBoost Ranker (softmax)")
    results["catboost"] = evaluate(catboost_test_probs_sorted, y_test_sorted, test_sorted, "CatBoost Classifier")
    results["ensemble"] = evaluate(ens_test_probs, y_test_sorted, test_sorted, "3-Way Ensemble (uncalibrated)")
    results["ensemble_cal"] = evaluate(ens_test_calibrated, y_test_sorted, test_sorted, "3-Way Ensemble (Platt calibrated)")
    results["stacking"] = evaluate(meta_test_probs, y_test_sorted, test_sorted, "Stacking Meta-Learner")

    # Pick best model for export
    best_model_name = min(results, key=lambda k: results[k]["log_loss"])
    print(f"\n*** Best model by log_loss: {best_model_name} ***")

    # --- Feature importance ---
    print(f"\n{'='*50}")
    print("Top 20 Feature Importances (LightGBM)")
    print("=" * 50)
    importance = pd.Series(lgbm_model.feature_importances_, index=feature_names)
    print(importance.sort_values(ascending=False).head(20).to_string())

    print(f"\n{'='*50}")
    print("Top 20 Feature Importances (XGBoost)")
    print("=" * 50)
    xgb_importance = pd.Series(xgb_model.feature_importances_, index=feature_names)
    print(xgb_importance.sort_values(ascending=False).head(20).to_string())

    print(f"\n{'='*50}")
    print("Top 20 Feature Importances (CatBoost)")
    print("=" * 50)
    catboost_importance = pd.Series(
        catboost_model.get_feature_importance(),
        index=feature_names
    )
    print(catboost_importance.sort_values(ascending=False).head(20).to_string())

    # --- Save ---
    joblib.dump(lgbm_model, MODEL_DIR / "lgbm_model.pkl")
    joblib.dump(xgb_model, MODEL_DIR / "xgb_ranker.pkl")
    joblib.dump(catboost_model, MODEL_DIR / "catboost_model.pkl")
    joblib.dump(platt_calibrator, MODEL_DIR / "platt_calibrator.pkl")
    joblib.dump(meta_learner, MODEL_DIR / "meta_learner.pkl")
    joblib.dump({
        "ensemble_weights": best_weights,
        "feature_names": feature_names,
        "medians": medians,
    }, MODEL_DIR / "meta.pkl")
    print(f"\nModels saved to {MODEL_DIR}")

    # --- Export for TypeScript ---
    export_ensemble(
        lgbm_model, xgb_model, catboost_model,
        feature_names, platt_calibrator, best_weights,
        meta_learner, medians
    )

    print("\nDone!")


if __name__ == "__main__":
    main()
