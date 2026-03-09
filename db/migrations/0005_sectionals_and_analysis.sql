-- Sectional times from punters.com.au scraper
CREATE TABLE IF NOT EXISTS sectional_times (
  id SERIAL PRIMARY KEY,
  race_id TEXT NOT NULL,
  horse_name TEXT NOT NULL,
  horse_id TEXT,
  horse_number INTEGER,
  barrier INTEGER,
  speed_800m DECIMAL(6,2),
  speed_600m DECIMAL(6,2),
  speed_400m DECIMAL(6,2),
  speed_200m DECIMAL(6,2),
  speed_finish DECIMAL(6,2),
  speed_avg DECIMAL(6,2),
  scraper_odds DECIMAL(10,3),
  source TEXT DEFAULT 'punters',
  scraped_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(race_id, horse_name)
);
CREATE INDEX IF NOT EXISTS idx_sectionals_race ON sectional_times(race_id);
CREATE INDEX IF NOT EXISTS idx_sectionals_horse ON sectional_times(horse_id);

-- Extra form data from punters.com.au not in TheRacingAPI
CREATE TABLE IF NOT EXISTS scraper_form_data (
  id SERIAL PRIMARY KEY,
  race_id TEXT NOT NULL,
  horse_name TEXT NOT NULL,
  horse_id TEXT,
  jockey TEXT,
  trainer TEXT,
  weight_kg DECIMAL(6,2),
  form_string TEXT,
  career_starts INTEGER,
  career_wins INTEGER,
  career_places INTEGER,
  win_pct DECIMAL(5,2),
  place_pct DECIMAL(5,2),
  prize_money_text TEXT,
  form_comment TEXT,
  form_flags JSONB,
  source TEXT DEFAULT 'punters',
  scraped_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(race_id, horse_name)
);
CREATE INDEX IF NOT EXISTS idx_scraper_form_race ON scraper_form_data(race_id);

-- Post-race prediction vs actual comparison
CREATE TABLE IF NOT EXISTS prediction_results (
  id SERIAL PRIMARY KEY,
  race_id TEXT NOT NULL,
  horse_id TEXT NOT NULL,
  model_version TEXT,
  -- Pre-race snapshot
  predicted_win_prob DECIMAL(6,4),
  predicted_rank INTEGER,
  market_odds_at_prediction DECIMAL(10,3),
  edge_pct DECIMAL(6,2),
  verdict TEXT,
  -- Actual result
  actual_position INTEGER,
  actual_sp DECIMAL(10,3),
  beaten_lengths DECIMAL(6,2),
  -- Analysis
  prediction_correct BOOLEAN,
  value_bet_correct BOOLEAN,
  profit_loss DECIMAL(10,2),
  -- Change flags vs previous run
  jockey_changed BOOLEAN,
  weight_changed_kg DECIMAL(6,2),
  distance_changed_m INTEGER,
  class_changed BOOLEAN,
  going_changed BOOLEAN,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(race_id, horse_id, model_version)
);
CREATE INDEX IF NOT EXISTS idx_pred_results_race ON prediction_results(race_id);

-- Per-race aggregate analysis
CREATE TABLE IF NOT EXISTS race_analysis (
  id SERIAL PRIMARY KEY,
  race_id TEXT NOT NULL UNIQUE,
  model_version TEXT,
  top_pick_position INTEGER,
  top_pick_won BOOLEAN,
  any_value_bet_won BOOLEAN,
  value_bets_count INTEGER DEFAULT 0,
  value_bets_won INTEGER DEFAULT 0,
  total_staked DECIMAL(10,2) DEFAULT 0,
  total_return DECIMAL(10,2) DEFAULT 0,
  race_pnl DECIMAL(10,2) DEFAULT 0,
  pace_scenario TEXT,
  leader_800m_speed DECIMAL(6,2),
  winner_closing_speed DECIMAL(6,2),
  analyzed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Add sectional columns to horse_form_history
ALTER TABLE horse_form_history ADD COLUMN IF NOT EXISTS sectional_finish_speed DECIMAL(6,2);
ALTER TABLE horse_form_history ADD COLUMN IF NOT EXISTS sectional_avg_speed DECIMAL(6,2);
ALTER TABLE horse_form_history ADD COLUMN IF NOT EXISTS acceleration_profile DECIMAL(6,2);
