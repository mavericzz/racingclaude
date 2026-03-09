"""
Extract training data from PostgreSQL into a pandas DataFrame.
Each row = one runner in one race, with features + target (won = position == 1).

Weight normalization: all weights converted to KG
- < 15: stone -> kg (*6.35029)
- 48-75: already kg
- 100-150: lbs -> kg (*0.453592)
"""

import pandas as pd
import psycopg2
import numpy as np
from config import DATABASE_URL, FEATURE_NAMES

QUERY = """
WITH runner_data AS (
    SELECT
        ru.race_id,
        ru.horse_id,
        m.meeting_date AS race_date,
        res.position,
        res.sp_decimal AS sp,
        rc.field_size,
        rc.distance_m,
        rc.going,
        rc.class AS race_class,
        m.venue_id,

        -- Raw weight (mixed units) and jockey claim
        ru.weight_lbs AS raw_weight,
        ru.jockey_claim,
        ru.draw AS barrier_draw,
        ru.rating AS benchmark_rating,
        ru.jockey_id,
        ru.trainer_id,
        ru.headgear,

        -- Last race info from form history
        (SELECT hfh.distance_m FROM horse_form_history hfh
         WHERE hfh.horse_id = ru.horse_id AND hfh.race_date < m.meeting_date
         ORDER BY hfh.race_date DESC LIMIT 1) AS last_distance_m,

        (SELECT hfh.weight_carried FROM horse_form_history hfh
         WHERE hfh.horse_id = ru.horse_id AND hfh.race_date < m.meeting_date
         ORDER BY hfh.race_date DESC LIMIT 1) AS last_weight_raw,

        (SELECT hfh.jockey_id FROM horse_form_history hfh
         WHERE hfh.horse_id = ru.horse_id AND hfh.race_date < m.meeting_date
         ORDER BY hfh.race_date DESC LIMIT 1) AS last_jockey_id,

        (SELECT hfh.venue_id FROM horse_form_history hfh
         WHERE hfh.horse_id = ru.horse_id AND hfh.race_date < m.meeting_date
         ORDER BY hfh.race_date DESC LIMIT 1) AS last_venue_id,

        (SELECT hfh.class FROM horse_form_history hfh
         WHERE hfh.horse_id = ru.horse_id AND hfh.race_date < m.meeting_date
         ORDER BY hfh.race_date DESC LIMIT 1) AS last_race_class,

        (SELECT hfh.headgear FROM horse_form_history hfh
         WHERE hfh.horse_id = ru.horse_id AND hfh.race_date < m.meeting_date
         ORDER BY hfh.race_date DESC LIMIT 1) AS last_headgear,

        -- Track experience: runs and wins at this venue
        (SELECT COUNT(*) FROM horse_form_history hfh
         WHERE hfh.horse_id = ru.horse_id AND hfh.venue_id = m.venue_id
         AND hfh.race_date < m.meeting_date
        ) AS track_runs,

        (SELECT COUNT(*) FILTER (WHERE position = 1) FROM horse_form_history hfh
         WHERE hfh.horse_id = ru.horse_id AND hfh.venue_id = m.venue_id
         AND hfh.race_date < m.meeting_date
        ) AS track_wins,

        -- Track+distance experience
        (SELECT COUNT(*) FROM horse_form_history hfh
         WHERE hfh.horse_id = ru.horse_id AND hfh.venue_id = m.venue_id
         AND ABS(hfh.distance_m - rc.distance_m) <= 100
         AND hfh.race_date < m.meeting_date
        ) AS track_distance_runs,

        (SELECT COUNT(*) FILTER (WHERE position = 1) FROM horse_form_history hfh
         WHERE hfh.horse_id = ru.horse_id AND hfh.venue_id = m.venue_id
         AND ABS(hfh.distance_m - rc.distance_m) <= 100
         AND hfh.race_date < m.meeting_date
        ) AS track_distance_wins,

        -- Speed figures
        (SELECT sf.adjusted_speed_figure FROM speed_figures sf
         JOIN races rc2 ON sf.race_id = rc2.race_id
         JOIN meetings m2 ON rc2.meeting_id = m2.meeting_id
         WHERE sf.horse_id = ru.horse_id AND m2.meeting_date < m.meeting_date
         ORDER BY m2.meeting_date DESC LIMIT 1) AS last_speed_figure,

        (SELECT AVG(sub.adjusted_speed_figure) FROM (
            SELECT sf.adjusted_speed_figure FROM speed_figures sf
            JOIN races rc2 ON sf.race_id = rc2.race_id
            JOIN meetings m2 ON rc2.meeting_id = m2.meeting_id
            WHERE sf.horse_id = ru.horse_id AND m2.meeting_date < m.meeting_date
            ORDER BY m2.meeting_date DESC LIMIT 5
         ) sub) AS avg_speed_figure_last5,

        (SELECT MAX(sub.adjusted_speed_figure) FROM (
            SELECT sf.adjusted_speed_figure FROM speed_figures sf
            JOIN races rc2 ON sf.race_id = rc2.race_id
            JOIN meetings m2 ON rc2.meeting_id = m2.meeting_id
            WHERE sf.horse_id = ru.horse_id AND m2.meeting_date < m.meeting_date
            ORDER BY m2.meeting_date DESC LIMIT 5
         ) sub) AS best_speed_figure_last5,

        -- Days since last run
        (SELECT m.meeting_date - MAX(hfh.race_date)
         FROM horse_form_history hfh
         WHERE hfh.horse_id = ru.horse_id AND hfh.race_date < m.meeting_date
        ) AS days_since_last_run,

        -- Consistency: stddev of last 10 positions
        (SELECT 1.0 - COALESCE(STDDEV(sub.position) / NULLIF(AVG(sub.field_size), 0), 0.5)
         FROM (SELECT position, field_size FROM horse_form_history
               WHERE horse_id = ru.horse_id AND race_date < m.meeting_date
               ORDER BY race_date DESC LIMIT 10) sub
         WHERE sub.position IS NOT NULL
        ) AS consistency_index,

        -- Form momentum: avg last 3 speed figs - avg prev 3
        (SELECT AVG(s1.fig) - COALESCE(AVG(s2.fig), AVG(s1.fig))
         FROM (
            SELECT sf.adjusted_speed_figure AS fig, ROW_NUMBER() OVER (ORDER BY m2.meeting_date DESC) AS rn
            FROM speed_figures sf
            JOIN races rc2 ON sf.race_id = rc2.race_id
            JOIN meetings m2 ON rc2.meeting_id = m2.meeting_id
            WHERE sf.horse_id = ru.horse_id AND m2.meeting_date < m.meeting_date
         ) s1
         LEFT JOIN LATERAL (
            SELECT sf.adjusted_speed_figure AS fig
            FROM speed_figures sf
            JOIN races rc2 ON sf.race_id = rc2.race_id
            JOIN meetings m2 ON rc2.meeting_id = m2.meeting_id
            WHERE sf.horse_id = ru.horse_id AND m2.meeting_date < m.meeting_date
            ORDER BY m2.meeting_date DESC OFFSET 3 LIMIT 3
         ) s2 ON true
         WHERE s1.rn <= 3
        ) AS form_momentum,

        (SELECT AVG(hfh.beaten_lengths)
         FROM (SELECT beaten_lengths FROM horse_form_history
               WHERE horse_id = ru.horse_id AND race_date < m.meeting_date
               ORDER BY race_date DESC LIMIT 5) hfh
        ) AS avg_beaten_lengths_5,

        -- Fitness: runs in last 90 days
        (SELECT COUNT(*) FROM horse_form_history
         WHERE horse_id = ru.horse_id
         AND race_date >= m.meeting_date - INTERVAL '90 days'
         AND race_date < m.meeting_date
        ) AS fitness_score,

        -- Spell status
        CASE
            WHEN (SELECT m.meeting_date - MAX(hfh.race_date) FROM horse_form_history hfh
                  WHERE hfh.horse_id = ru.horse_id AND hfh.race_date < m.meeting_date) > 60
                 OR (SELECT COUNT(*) FROM horse_form_history WHERE horse_id = ru.horse_id AND race_date < m.meeting_date) = 0
            THEN 0
            WHEN (SELECT hfh.days_since_prev_run FROM horse_form_history hfh
                  WHERE hfh.horse_id = ru.horse_id AND hfh.race_date < m.meeting_date
                  ORDER BY hfh.race_date DESC LIMIT 1) > 60
            THEN 1
            ELSE 3
        END AS spell_status,

        -- Career win pct from form history
        (SELECT CASE WHEN COUNT(*) > 0
            THEN COUNT(*) FILTER (WHERE position = 1)::DECIMAL / COUNT(*) * 100
            ELSE NULL END
         FROM horse_form_history WHERE horse_id = ru.horse_id AND race_date < m.meeting_date
        ) AS career_win_pct,

        -- Career place pct (top 3)
        (SELECT CASE WHEN COUNT(*) > 0
            THEN COUNT(*) FILTER (WHERE position <= 3)::DECIMAL / COUNT(*) * 100
            ELSE NULL END
         FROM horse_form_history WHERE horse_id = ru.horse_id AND race_date < m.meeting_date
        ) AS career_place_pct,

        -- Total career starts
        (SELECT COUNT(*) FROM horse_form_history
         WHERE horse_id = ru.horse_id AND race_date < m.meeting_date
        ) AS career_starts,

        -- Distance features from form history
        (SELECT CASE WHEN COUNT(*) >= 2
            THEN COUNT(*) FILTER (WHERE position = 1)::DECIMAL / COUNT(*) * 100
            ELSE NULL END
         FROM horse_form_history
         WHERE horse_id = ru.horse_id AND race_date < m.meeting_date
         AND ABS(distance_m - rc.distance_m) <= 100
        ) AS distance_win_pct,

        -- Going win pct
        (SELECT CASE WHEN COUNT(*) >= 2
            THEN COUNT(*) FILTER (WHERE position = 1)::DECIMAL / COUNT(*) * 100
            ELSE NULL END
         FROM horse_form_history
         WHERE horse_id = ru.horse_id AND race_date < m.meeting_date
         AND LOWER(going) = LOWER(rc.going)
        ) AS going_win_pct,

        -- Wet track specialist
        (SELECT
            COALESCE(
              (COUNT(*) FILTER (WHERE position = 1 AND LOWER(going) IN ('soft','heavy','soft to heavy','very soft'))::DECIMAL
               / NULLIF(COUNT(*) FILTER (WHERE LOWER(going) IN ('soft','heavy','soft to heavy','very soft')), 0) * 100),
              0)
            -
            COALESCE(
              (COUNT(*) FILTER (WHERE position = 1 AND LOWER(going) IN ('good','good to firm','firm'))::DECIMAL
               / NULLIF(COUNT(*) FILTER (WHERE LOWER(going) IN ('good','good to firm','firm')), 0) * 100),
              0)
         FROM horse_form_history
         WHERE horse_id = ru.horse_id AND race_date < m.meeting_date
         HAVING COUNT(*) >= 3
        ) AS is_wet_track_specialist,

        -- Running style
        CASE
            WHEN (SELECT hfh.running_style FROM horse_form_history hfh
                  WHERE hfh.horse_id = ru.horse_id AND hfh.race_date < m.meeting_date AND hfh.running_style IS NOT NULL
                  ORDER BY hfh.race_date DESC LIMIT 1) = 'leader' THEN 0
            WHEN (SELECT hfh.running_style FROM horse_form_history hfh
                  WHERE hfh.horse_id = ru.horse_id AND hfh.race_date < m.meeting_date AND hfh.running_style IS NOT NULL
                  ORDER BY hfh.race_date DESC LIMIT 1) = 'on-pace' THEN 1
            WHEN (SELECT hfh.running_style FROM horse_form_history hfh
                  WHERE hfh.horse_id = ru.horse_id AND hfh.race_date < m.meeting_date AND hfh.running_style IS NOT NULL
                  ORDER BY hfh.race_date DESC LIMIT 1) = 'mid' THEN 2
            WHEN (SELECT hfh.running_style FROM horse_form_history hfh
                  WHERE hfh.horse_id = ru.horse_id AND hfh.race_date < m.meeting_date AND hfh.running_style IS NOT NULL
                  ORDER BY hfh.race_date DESC LIMIT 1) = 'backmarker' THEN 3
            ELSE 2
        END AS running_style,

        -- Connection features from materialized views
        ts.win_pct AS trainer_win_pct,
        ts.place_pct AS trainer_place_pct,
        tss.win_pct AS trainer_first_up_win_pct,
        js.win_pct AS jockey_win_pct,
        js.place_pct AS jockey_place_pct,
        cs.win_pct AS combo_win_pct,

        -- v5: API form stats from runner_form_stats
        (SELECT first::DECIMAL / NULLIF(total, 0) * 100
         FROM runner_form_stats WHERE horse_id = ru.horse_id AND race_id = ru.race_id AND stat_type = 'course'
        ) AS api_course_win_pct,
        (SELECT first::DECIMAL / NULLIF(total, 0) * 100
         FROM runner_form_stats WHERE horse_id = ru.horse_id AND race_id = ru.race_id AND stat_type = 'course_distance'
        ) AS api_course_distance_win_pct,
        (SELECT first::DECIMAL / NULLIF(total, 0) * 100
         FROM runner_form_stats WHERE horse_id = ru.horse_id AND race_id = ru.race_id AND stat_type = 'distance'
        ) AS api_distance_win_pct,
        (SELECT first::DECIMAL / NULLIF(total, 0) * 100
         FROM runner_form_stats WHERE horse_id = ru.horse_id AND race_id = ru.race_id AND stat_type = 'last_ten'
        ) AS api_last10_win_pct,

        -- v5: RPR from results (last and avg)
        (SELECT res2.rpr FROM results res2
         JOIN runners ru2 ON res2.race_id = ru2.race_id AND res2.horse_id = ru2.horse_id
         JOIN races rc2 ON ru2.race_id = rc2.race_id
         JOIN meetings m2 ON rc2.meeting_id = m2.meeting_id
         WHERE res2.horse_id = ru.horse_id AND m2.meeting_date < m.meeting_date AND res2.rpr IS NOT NULL
         ORDER BY m2.meeting_date DESC LIMIT 1) AS last_rpr,
        (SELECT AVG(sub.rpr) FROM (
            SELECT res2.rpr FROM results res2
            JOIN runners ru2 ON res2.race_id = ru2.race_id AND res2.horse_id = ru2.horse_id
            JOIN races rc2 ON ru2.race_id = rc2.race_id
            JOIN meetings m2 ON rc2.meeting_id = m2.meeting_id
            WHERE res2.horse_id = ru.horse_id AND m2.meeting_date < m.meeting_date AND res2.rpr IS NOT NULL
            ORDER BY m2.meeting_date DESC LIMIT 5
        ) sub) AS avg_rpr_last5,

        -- v5: Beaten lengths from last 5 (individual values for trend)
        (SELECT ARRAY_AGG(sub.bl ORDER BY sub.rn) FROM (
            SELECT beaten_lengths AS bl, ROW_NUMBER() OVER (ORDER BY race_date DESC) AS rn
            FROM horse_form_history
            WHERE horse_id = ru.horse_id AND race_date < m.meeting_date AND beaten_lengths IS NOT NULL
            LIMIT 5
        ) sub) AS beaten_lengths_array,

        -- v5: Form string from runners table
        ru.form AS form_string,

        -- v5: Trainer at venue actual stats
        (SELECT COUNT(*) FROM horse_form_history hfh
         JOIN runners ru3 ON hfh.race_id = ru3.race_id AND ru3.trainer_id = ru.trainer_id
         WHERE hfh.venue_id = m.venue_id AND hfh.race_date < m.meeting_date
        ) AS trainer_venue_runs,
        (SELECT COUNT(*) FILTER (WHERE hfh.position = 1) FROM horse_form_history hfh
         JOIN runners ru3 ON hfh.race_id = ru3.race_id AND ru3.trainer_id = ru.trainer_id
         WHERE hfh.venue_id = m.venue_id AND hfh.race_date < m.meeting_date
        ) AS trainer_venue_wins,

        -- v5: Sire info
        h.sire_id

    FROM runners ru
    JOIN races rc ON ru.race_id = rc.race_id
    JOIN meetings m ON rc.meeting_id = m.meeting_id
    JOIN results res ON ru.race_id = res.race_id AND ru.horse_id = res.horse_id
    LEFT JOIN horses h ON ru.horse_id = h.id
    LEFT JOIN mv_trainer_stats ts ON ru.trainer_id = ts.trainer_id
    LEFT JOIN mv_trainer_spell_stats tss ON ru.trainer_id = tss.trainer_id AND tss.spell_status = 'first-up'
    LEFT JOIN mv_jockey_stats js ON ru.jockey_id = js.jockey_id
    LEFT JOIN mv_combo_stats cs ON ru.trainer_id = cs.trainer_id AND ru.jockey_id = cs.jockey_id
    WHERE res.position IS NOT NULL
      AND ru.scratched = FALSE
    ORDER BY m.meeting_date
)
SELECT * FROM runner_data;
"""


def normalize_weight_kg(w):
    """Convert weight to kg regardless of source unit."""
    if pd.isna(w) or w is None:
        return np.nan
    w = float(w)
    if w < 15:       # stone (e.g. 8, 9)
        return w * 6.35029
    elif w < 75:     # already kg (e.g. 54, 57.5)
        return w
    elif w < 150:    # lbs (e.g. 119, 126)
        return w * 0.453592
    return np.nan


def extract():
    print("Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)

    print("Extracting training data (this may take a few minutes)...")
    df = pd.read_sql(QUERY, conn)
    conn.close()

    print(f"Raw data: {len(df)} rows")

    # --- Weight normalization to KG ---
    df["weight_kg"] = df["raw_weight"].apply(normalize_weight_kg)
    df["last_weight_kg"] = df["last_weight_raw"].apply(normalize_weight_kg)

    # Weight carried = weight - jockey claim (in kg)
    df["weight_carried_kg"] = df["weight_kg"] - df["jockey_claim"].fillna(0) * 0.453592
    # For apprentice claims that are already in lbs, the claim column is usually small (1-4 kg)
    # In AU racing, claims are in kg directly (1.5, 2, 3, 4)
    df["weight_carried_kg"] = df["weight_kg"] - df["jockey_claim"].fillna(0)

    # Weight change from last run (in kg)
    df["weight_change_kg"] = df["weight_kg"] - df["last_weight_kg"]

    # Weight vs field average
    race_avg_weight = df.groupby("race_id")["weight_carried_kg"].transform("mean")
    df["weight_vs_field_avg"] = df["weight_carried_kg"] - race_avg_weight

    # --- Distance features ---
    df["last_distance_m"] = df["last_distance_m"].astype(float)
    df["distance_change"] = df["distance_m"] - df["last_distance_m"]

    # --- Jockey features ---
    # Same jockey as last run
    df["same_jockey"] = (df["jockey_id"] == df["last_jockey_id"]).astype(int)
    df["same_jockey"] = df["same_jockey"].where(df["last_jockey_id"].notna(), np.nan)

    # Jockey upgrade score: current jockey win% - last jockey win%
    # Need to map last_jockey_id to their win%
    conn2 = psycopg2.connect(DATABASE_URL)
    jockey_stats = pd.read_sql("SELECT jockey_id, win_pct, place_pct FROM mv_jockey_stats", conn2)

    last_jockey_stats = jockey_stats.rename(columns={
        "jockey_id": "last_jockey_id",
        "win_pct": "last_jockey_win_pct",
        "place_pct": "last_jockey_place_pct",
    })
    df = df.merge(last_jockey_stats, on="last_jockey_id", how="left")
    df["jockey_upgrade_score"] = df["jockey_win_pct"] - df["last_jockey_win_pct"]

    # --- Track experience features ---
    df["track_win_pct"] = np.where(
        df["track_runs"] >= 2,
        df["track_wins"] / df["track_runs"] * 100,
        np.nan
    )
    df["track_distance_win_pct"] = np.where(
        df["track_distance_runs"] >= 2,
        df["track_distance_wins"] / df["track_distance_runs"] * 100,
        np.nan
    )
    # Boolean: has run at this track before
    df["has_track_experience"] = (df["track_runs"] > 0).astype(int)
    # Boolean: has won at this track before
    df["has_track_win"] = (df["track_wins"] > 0).astype(int)

    # --- Class features ---
    from class_parser import parse_class_numeric
    df["class_numeric"] = df["race_class"].apply(parse_class_numeric)
    df["last_class_numeric"] = df["last_race_class"].apply(parse_class_numeric)
    df["class_change"] = df["class_numeric"] - df["last_class_numeric"]

    # Is apprentice
    df["is_apprentice"] = (df["jockey_claim"].fillna(0) > 0).astype(int)
    df["apprentice_claim"] = df["jockey_claim"].fillna(0)

    # --- Barrier bias score ---
    try:
        barrier_stats = pd.read_sql(
            "SELECT venue_id, distance_bucket, barrier_group, win_pct FROM mv_barrier_stats",
            conn2
        )
        if len(barrier_stats) > 0:
            avg_win_pct = barrier_stats["win_pct"].mean()
            barrier_stats["bias"] = barrier_stats["win_pct"] - avg_win_pct
            df["barrier_group"] = pd.cut(
                df["barrier_draw"],
                bins=[0, 4, 8, 99],
                labels=["inside", "mid", "outside"]
            ).astype(str)
            group_bias = barrier_stats.groupby("barrier_group")["bias"].mean()
            df["barrier_bias_score"] = df["barrier_group"].map(group_bias).fillna(0)
            df.drop(columns=["barrier_group"], inplace=True)
        else:
            df["barrier_bias_score"] = 0
    except Exception:
        df["barrier_bias_score"] = 0

    conn2.close()

    # Leader count in field
    df["leader_count_in_field"] = df.groupby("race_id")["running_style"].transform(
        lambda x: (x == 0).sum()
    )

    # --- Gear change features ---
    df_sorted = df.sort_values(["horse_id", "race_date"]).copy()
    # Use last_headgear from SQL for gear changes
    df["gear_change_signal"] = (
        (df["headgear"] != df["last_headgear"]) &
        df["headgear"].notna()
    ).astype(int)

    def has_blinkers(hg):
        if pd.isna(hg) or hg is None:
            return False
        return any(x in str(hg).lower() for x in ['b', 'bl', 'blink'])

    df["blinkers_first_time"] = (
        df["headgear"].apply(has_blinkers) & ~df["last_headgear"].apply(has_blinkers)
    ).astype(int)

    # Trainer at venue win pct (will be overridden by v5 actual venue stats below)

    # --- Odds features ---
    df["current_odds"] = df["sp"]
    df["market_implied_prob"] = np.where(df["sp"] > 1, 1.0 / df["sp"], np.nan)
    race_min_odds = df.groupby("race_id")["sp"].transform("min")
    df["is_favourite"] = (df["sp"] == race_min_odds).astype(int)

    # --- Odds movement from odds_snapshots ---
    odds_query = """
    WITH ranked AS (
        SELECT race_id, horse_id, win_odds, observed_at,
            ROW_NUMBER() OVER (PARTITION BY race_id, horse_id ORDER BY observed_at ASC) AS rn_first,
            ROW_NUMBER() OVER (PARTITION BY race_id, horse_id ORDER BY observed_at DESC) AS rn_last,
            COUNT(*) OVER (PARTITION BY race_id, horse_id) AS snap_count
        FROM odds_snapshots
        WHERE win_odds IS NOT NULL
    )
    SELECT
        r1.race_id, r1.horse_id,
        r1.win_odds AS open_odds,
        r2.win_odds AS close_odds,
        r1.snap_count,
        r3.win_odds AS penult_odds
    FROM ranked r1
    JOIN ranked r2 ON r1.race_id = r2.race_id AND r1.horse_id = r2.horse_id AND r2.rn_last = 1
    LEFT JOIN ranked r3 ON r1.race_id = r3.race_id AND r1.horse_id = r3.horse_id AND r3.rn_last = 2
    WHERE r1.rn_first = 1
    """
    try:
        odds_df = pd.read_sql(odds_query, conn)
        df = df.merge(odds_df, on=["race_id", "horse_id"], how="left")
        df["odds_movement"] = (df["open_odds"] - df["close_odds"]).fillna(0)
        df["odds_movement_pct"] = np.where(
            df["open_odds"].notna() & (df["open_odds"] > 1),
            (df["open_odds"] - df["close_odds"]) / df["open_odds"] * 100, 0)
        df["is_plunge"] = (df["odds_movement_pct"] > 20).astype(int)
        df["late_money_steam"] = np.where(
            df["penult_odds"].notna() & (df["penult_odds"] > 1),
            (df["penult_odds"] - df["close_odds"]) / df["penult_odds"] * 100, 0)
        df["odds_volatility"] = np.abs(df["odds_movement_pct"]) * 0.5
        df.drop(columns=["open_odds", "close_odds", "snap_count", "penult_odds"], errors="ignore", inplace=True)
        odds_coverage = df["odds_movement"].ne(0).sum()
        print(f"  Odds movement coverage: {odds_coverage}/{len(df)} ({100*odds_coverage/len(df):.1f}%)")
    except Exception as e:
        print(f"  Warning: Could not load odds snapshots: {e}")
        df["odds_movement"] = 0
        df["odds_movement_pct"] = 0
        df["is_plunge"] = 0
        df["late_money_steam"] = 0
        df["odds_volatility"] = 0

    # --- Interaction & derived features ---
    # Barrier draw ratio: normalized by field size
    df["barrier_draw_ratio"] = df["barrier_draw"] / df["field_size"].clip(lower=1)

    # Speed vs field average
    race_avg_speed = df.groupby("race_id")["avg_speed_figure_last5"].transform("mean")
    df["speed_vs_field_avg"] = df["avg_speed_figure_last5"] - race_avg_speed

    # Jockey-trainer interaction: quality of both connections
    df["jockey_trainer_interaction"] = (
        df["jockey_win_pct"].fillna(0) * df["trainer_win_pct"].fillna(0) / 100.0
    )

    # Class-distance affinity
    df["class_distance_affinity"] = df["distance_win_pct"] * (
        1 + df["class_change"].clip(-2, 2) / 10.0
    )

    # Odds × speed interaction
    df["odds_x_speed"] = df["current_odds"] * df["avg_speed_figure_last5"]

    # Recency-weighted speed figure (exponential decay)
    # Need individual speed figs for this — use SQL subquery approach
    def compute_recency_weighted_speed(row, df_full):
        """Compute exponential-decay weighted speed figure from last 5 figs."""
        # We approximate using last_speed_figure and avg (weighted toward recent)
        # For proper computation we'd need individual figs, but this approximation works
        last = row.get("last_speed_figure")
        avg5 = row.get("avg_speed_figure_last5")
        best = row.get("best_speed_figure_last5")
        if pd.isna(last):
            return np.nan
        if pd.isna(avg5):
            return last
        # Approximate: weight last fig higher than average
        # Decay weights [1.0, 0.7, 0.49, 0.34, 0.24] sum to 2.77
        # last_fig gets weight 1.0/2.77=0.36, rest avg gets 1.77/2.77=0.64
        return 0.36 * last + 0.64 * avg5

    df["recency_weighted_speed"] = df.apply(
        lambda row: compute_recency_weighted_speed(row, df), axis=1
    )

    # Career stage: bucket career starts
    df["career_stage"] = pd.cut(
        df["career_starts"],
        bins=[-1, 5, 15, 30, 9999],
        labels=[0, 1, 2, 3]
    ).astype(float)

    # --- v5: RPR vs field average ---
    race_avg_rpr = df.groupby("race_id")["avg_rpr_last5"].transform("mean")
    df["rpr_vs_field_avg"] = df["avg_rpr_last5"] - race_avg_rpr
    df["field_avg_rpr"] = race_avg_rpr

    # Field strength rank: horse RPR percentile within race (0=best, 1=worst)
    df["field_strength_rank"] = df.groupby("race_id")["avg_rpr_last5"].rank(ascending=False, pct=True)
    df["field_strength_rank"] = df["field_strength_rank"].where(df["avg_rpr_last5"].notna(), np.nan)

    # --- v5: Beaten lengths trend (slope of last 5) ---
    def compute_bl_trend(bl_array):
        if bl_array is None or not isinstance(bl_array, (list, np.ndarray)):
            return np.nan, np.nan
        bl = [float(x) for x in bl_array if x is not None and not np.isnan(float(x))]
        if len(bl) < 2:
            return np.nan, min(bl) if bl else np.nan
        # Simple linear regression slope: negative = improving (shorter margins)
        x = np.arange(len(bl))
        slope = np.polyfit(x, bl, 1)[0]
        return slope, min(bl)

    bl_results = df["beaten_lengths_array"].apply(
        lambda arr: pd.Series(compute_bl_trend(arr), index=["beaten_lengths_trend", "best_beaten_lengths_5"])
    )
    df["beaten_lengths_trend"] = bl_results["beaten_lengths_trend"]
    df["best_beaten_lengths_5"] = bl_results["best_beaten_lengths_5"]

    # --- v5: Form string parsing ---
    def parse_form_string(form_str):
        if not form_str or not isinstance(form_str, str):
            return np.nan, 0
        # Form string like "1x234" - recent positions from right to left
        # Map: 1=5, 2=3, 3=2, 4-9=1, 0/x/f/p=0
        score = 0
        wins = 0
        chars = list(form_str.strip()[-8:])  # last 8 characters
        weights = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3]
        for i, ch in enumerate(reversed(chars)):
            w = weights[i] if i < len(weights) else 0.3
            if ch == '1':
                score += 5 * w
                wins += 1
            elif ch == '2':
                score += 3 * w
            elif ch == '3':
                score += 2 * w
            elif ch.isdigit():
                score += 1 * w
            # x, f, p, etc = 0
        return score, wins

    form_results = df["form_string"].apply(
        lambda f: pd.Series(parse_form_string(f), index=["form_string_score", "recent_wins_count"])
    )
    df["form_string_score"] = form_results["form_string_score"]
    df["recent_wins_count"] = form_results["recent_wins_count"]

    # --- v5: Missing data indicator flags ---
    df["has_form_history"] = (df["career_starts"] > 0).astype(int)
    df["has_speed_figure"] = df["last_speed_figure"].notna().astype(int)
    df["has_rpr"] = df["last_rpr"].notna().astype(int)

    # --- v5: Trainer at venue actual win pct (replaces proxy) ---
    df["trainer_at_venue_win_pct"] = np.where(
        df["trainer_venue_runs"] >= 3,
        df["trainer_venue_wins"] / df["trainer_venue_runs"] * 100,
        df["trainer_win_pct"]  # fallback to overall if < 3 runs at venue
    )

    # --- v5: Sire distance affinity ---
    # Compute sire offspring performance at similar distances
    conn_sire = psycopg2.connect(DATABASE_URL)
    sire_stats = pd.read_sql("""
        SELECT h.sire_id,
               rc.distance_m,
               COUNT(*) as total,
               COUNT(*) FILTER (WHERE res.position = 1) as wins
        FROM results res
        JOIN runners ru ON res.race_id = ru.race_id AND res.horse_id = ru.horse_id
        JOIN horses h ON ru.horse_id = h.id
        JOIN races rc ON ru.race_id = rc.race_id
        WHERE h.sire_id IS NOT NULL AND res.position IS NOT NULL
        GROUP BY h.sire_id, rc.distance_m
        HAVING COUNT(*) >= 3
    """, conn_sire)
    conn_sire.close()

    # Bucket distances into bands (1000-1200, 1200-1400, 1400-1600, 1600-2000, 2000+)
    dist_bins = [0, 1200, 1400, 1600, 2000, 9999]
    sire_stats["dist_band"] = pd.cut(sire_stats["distance_m"], bins=dist_bins, labels=False)
    df["dist_band"] = pd.cut(df["distance_m"], bins=dist_bins, labels=False)

    sire_band_stats = sire_stats.groupby(["sire_id", "dist_band"]).agg(
        total=("total", "sum"), wins=("wins", "sum")
    ).reset_index()
    sire_band_stats["sire_distance_win_pct"] = sire_band_stats["wins"] / sire_band_stats["total"] * 100
    sire_band_stats["sire_progeny_count"] = sire_band_stats["total"]

    df = df.merge(
        sire_band_stats[["sire_id", "dist_band", "sire_distance_win_pct", "sire_progeny_count"]],
        on=["sire_id", "dist_band"], how="left"
    )
    df.drop(columns=["dist_band"], inplace=True)

    # --- Target-encoded categoricals ---
    from sklearn.preprocessing import TargetEncoder
    te = TargetEncoder(smooth="auto", cv=5, target_type="binary")
    # spell_status target encoding
    spell_vals = df[["spell_status"]].copy()
    spell_vals["spell_status"] = spell_vals["spell_status"].astype(float)
    df["spell_status_te"] = te.fit_transform(spell_vals, df["position"] == 1).ravel()

    # running_style target encoding
    style_vals = df[["running_style"]].copy()
    style_vals["running_style"] = style_vals["running_style"].astype(float)
    te2 = TargetEncoder(smooth="auto", cv=5, target_type="binary")
    df["running_style_te"] = te2.fit_transform(style_vals, df["position"] == 1).ravel()

    # --- Ensure all feature columns are numeric (some SQL NULLs come as object) ---
    for col in FEATURE_NAMES:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- Targets ---
    df["won"] = (df["position"] == 1).astype(int)
    df["placed"] = (df["position"] <= 3).astype(int)

    # Select final columns
    meta_cols = ["race_id", "horse_id", "race_date", "position", "sp", "won", "placed"]
    final_cols = FEATURE_NAMES + meta_cols
    df_final = df[[c for c in final_cols if c in df.columns]]

    # Save
    import os
    os.makedirs("data", exist_ok=True)
    output_path = "data/training_data.parquet"
    df_final.to_parquet(output_path, index=False)
    print(f"\nSaved {len(df_final)} rows to {output_path}")
    print(f"Features: {len(FEATURE_NAMES)}")
    print(f"Win rate: {df_final['won'].mean():.3f}")
    print(f"\nNull rates:")
    null_rates = df_final[FEATURE_NAMES].isnull().mean().sort_values(ascending=False)
    print(null_rates.to_string())

    # Feature stats
    print(f"\n=== Key Feature Stats ===")
    for col in ["weight_carried_kg", "weight_change_kg", "last_distance_m", "distance_change",
                 "same_jockey", "jockey_upgrade_score", "track_runs", "track_win_pct",
                 "has_track_experience", "career_starts"]:
        if col in df_final.columns:
            print(f"  {col}: mean={df_final[col].mean():.2f}, non-null={df_final[col].notna().mean():.1%}")

    return df_final


if __name__ == "__main__":
    extract()
