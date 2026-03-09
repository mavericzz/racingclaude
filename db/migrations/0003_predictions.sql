-- 0003_predictions.sql: Predictions, bet tracking, blackbook

CREATE TABLE IF NOT EXISTS predictions (
  id SERIAL PRIMARY KEY,
  race_id TEXT NOT NULL,
  horse_id TEXT NOT NULL,
  model_version TEXT NOT NULL,
  predicted_win_prob DECIMAL(6,4),
  predicted_place_prob DECIMAL(6,4),
  fair_odds_win DECIMAL(10,3),
  edge_pct DECIMAL(6,2),
  kelly_fraction DECIMAL(6,4),
  verdict TEXT,             -- value-winner, dutch-candidate, fair-price, false-favourite, pass
  features_json JSONB,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(race_id, horse_id, model_version)
);

CREATE TABLE IF NOT EXISTS bet_tracker (
  id SERIAL PRIMARY KEY,
  race_id TEXT NOT NULL,
  horse_id TEXT NOT NULL,
  bet_type TEXT NOT NULL,   -- win, place, each-way
  stake DECIMAL(10,2),
  odds_taken DECIMAL(10,3),
  predicted_prob DECIMAL(6,4),
  edge_pct DECIMAL(6,2),
  result TEXT,              -- win, place, lose, pending
  pnl DECIMAL(10,2),
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS blackbook (
  id SERIAL PRIMARY KEY,
  horse_id TEXT NOT NULL REFERENCES horses(id),
  note TEXT,
  alert_conditions JSONB,   -- {"min_odds": 5.0, "tracks": [...], "going": ["soft"]}
  is_active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ingestion_log (
  id SERIAL PRIMARY KEY,
  job TEXT NOT NULL,
  endpoint TEXT,
  date_range TEXT,
  status TEXT DEFAULT 'running',
  records_processed INTEGER DEFAULT 0,
  error_message TEXT,
  started_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_predictions_race ON predictions(race_id);
CREATE INDEX IF NOT EXISTS idx_bet_tracker_race ON bet_tracker(race_id);
CREATE INDEX IF NOT EXISTS idx_blackbook_horse ON blackbook(horse_id);
