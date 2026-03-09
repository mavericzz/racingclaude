import os
from dotenv import load_dotenv
from pathlib import Path

# Load .env from project root
load_dotenv(Path(__file__).parent.parent / ".env")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://rahulsharma@localhost:5432/racingclaude")

FEATURE_NAMES = [
    # Form & Performance (12)
    "last_speed_figure", "avg_speed_figure_last5", "best_speed_figure_last5",
    "days_since_last_run", "consistency_index", "form_momentum",
    "avg_beaten_lengths_5", "fitness_score", "spell_status",
    "benchmark_rating", "career_win_pct", "career_place_pct",
    # Career experience (1)
    "career_starts",
    # Class & Weight (7) - all in KG
    "class_numeric", "class_change",
    "weight_carried_kg", "weight_change_kg", "weight_vs_field_avg",
    "is_apprentice", "apprentice_claim",
    # Distance (3)
    "last_distance_m", "distance_change", "distance_win_pct",
    # Track experience (6)
    "track_runs", "track_win_pct", "track_distance_win_pct",
    "has_track_experience", "has_track_win",
    "going_win_pct",
    # Wet track (1)
    "is_wet_track_specialist",
    # Pace & Barrier (5)
    "running_style", "barrier_draw", "barrier_bias_score",
    "field_size", "leader_count_in_field",
    # Jockey & Trainer connections (10)
    "trainer_win_pct", "trainer_place_pct", "trainer_first_up_win_pct",
    "jockey_win_pct", "jockey_place_pct", "combo_win_pct",
    "same_jockey", "jockey_upgrade_score",
    "gear_change_signal", "blinkers_first_time",
    # Trainer at venue (1)
    "trainer_at_venue_win_pct",
    # Market (4 + 4 v6 odds movement)
    "current_odds", "odds_movement", "market_implied_prob", "is_favourite",
    "odds_movement_pct", "is_plunge", "late_money_steam", "odds_volatility",
    # Interaction & derived features (7)
    "barrier_draw_ratio",           # barrier_draw / field_size
    "speed_vs_field_avg",           # horse avg_speed - race avg_speed
    "jockey_trainer_interaction",   # jockey_win_pct * trainer_win_pct / 100
    "class_distance_affinity",      # distance_win_pct * (1 + clip(class_change, -2, 2) / 10)
    "odds_x_speed",                 # current_odds * avg_speed_figure_last5
    "recency_weighted_speed",       # exponential-decay weighted avg of last 5 speed figs
    "career_stage",                 # bucket: 0=debut, 1=developing, 2=established, 3=veteran
    # Target-encoded categoricals (2)
    "spell_status_te",              # target-encoded spell status
    "running_style_te",             # target-encoded running style
    # --- v5 NEW FEATURES ---
    # API form stats (4) - from runner_form_stats table
    "api_course_win_pct",           # win% at this course (from API stats)
    "api_course_distance_win_pct",  # win% at course+distance (from API stats)
    "api_distance_win_pct",         # win% at this distance (from API stats)
    "api_last10_win_pct",           # win% in last 10 races (from API stats)
    # RPR features (3) - Racing Post Rating from results table
    "last_rpr",                     # most recent RPR
    "avg_rpr_last5",                # average of last 5 RPRs
    "rpr_vs_field_avg",             # horse RPR minus race avg RPR
    # Beaten lengths trend (2)
    "beaten_lengths_trend",         # slope of last 5 beaten lengths (negative = improving)
    "best_beaten_lengths_5",        # best (lowest) beaten lengths in last 5
    # Field strength (2)
    "field_avg_rpr",                # average RPR of all competitors
    "field_strength_rank",          # horse RPR rank within field (0=best, 1=worst)
    # Form string (2) - parsed from runners.form
    "form_string_score",            # numeric score from recent form string
    "recent_wins_count",            # count of wins in form string
    # Missing data indicators (3)
    "has_form_history",             # 1 if horse has any form history
    "has_speed_figure",             # 1 if horse has any speed figures
    "has_rpr",                      # 1 if horse has any RPR data
    # Trainer at venue actual (1)
    "trainer_venue_runs",           # trainer's total runs at this venue
    # Sire features (2)
    "sire_distance_win_pct",        # sire's offspring win% at this distance band
    "sire_progeny_count",           # number of sire's progeny in DB (confidence signal)
]

CATEGORICAL_FEATURES = [
    "spell_status", "running_style", "is_apprentice",
    "blinkers_first_time", "is_favourite", "same_jockey",
    "has_track_experience", "has_track_win", "gear_change_signal",
    "has_form_history", "has_speed_figure", "has_rpr", "is_plunge",
]

# Indices of categorical features for CatBoost (computed at import time)
CATBOOST_CAT_INDICES = [FEATURE_NAMES.index(f) for f in CATEGORICAL_FEATURES]

MODEL_DIR = Path(__file__).parent / "models"
MODEL_DIR.mkdir(exist_ok=True)

EXPORT_DIR = Path(__file__).parent.parent / "src" / "ai" / "models"
