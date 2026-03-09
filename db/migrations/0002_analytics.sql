-- 0002_analytics.sql: Results, form history, speed figures, odds

CREATE TABLE IF NOT EXISTS results (
  id SERIAL PRIMARY KEY,
  race_id TEXT NOT NULL,
  horse_id TEXT NOT NULL,
  position INTEGER,
  sp_decimal DECIMAL(10,3),
  beaten_lengths DECIMAL(6,2),
  race_time TEXT,
  official_rating INTEGER,
  rpr INTEGER,
  prize DECIMAL(12,2),
  comment TEXT,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(race_id, horse_id)
);

CREATE TABLE IF NOT EXISTS horse_form_history (
  id SERIAL PRIMARY KEY,
  horse_id TEXT NOT NULL,
  race_id TEXT NOT NULL,
  race_date DATE NOT NULL,
  venue_id TEXT,
  distance_m INTEGER,
  going TEXT,
  class TEXT,
  position INTEGER,
  beaten_lengths DECIMAL(6,2),
  sp_decimal DECIMAL(10,3),
  weight_carried DECIMAL(6,2),
  jockey_id TEXT,
  trainer_id TEXT,
  headgear TEXT,
  rating_before INTEGER,
  rating_after INTEGER,
  race_time TEXT,
  field_size INTEGER,
  days_since_prev_run INTEGER,
  running_style TEXT,       -- leader, on-pace, mid, backmarker
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(horse_id, race_id)
);

CREATE TABLE IF NOT EXISTS runner_form_stats (
  id SERIAL PRIMARY KEY,
  horse_id TEXT NOT NULL,
  race_id TEXT NOT NULL,
  stat_type TEXT NOT NULL,  -- course, course_distance, distance, ground_firm, ground_good, ground_heavy, ground_soft, jockey, last_ten, last_twelve_months
  total INTEGER DEFAULT 0,
  first INTEGER DEFAULT 0,
  second INTEGER DEFAULT 0,
  third INTEGER DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(horse_id, race_id, stat_type)
);

CREATE TABLE IF NOT EXISTS speed_figures (
  id SERIAL PRIMARY KEY,
  horse_id TEXT NOT NULL,
  race_id TEXT NOT NULL,
  raw_time_secs DECIMAL(8,2),
  distance_m INTEGER,
  going TEXT,
  track_variant DECIMAL(6,2),
  adjusted_speed_figure DECIMAL(8,2),
  par_time_secs DECIMAL(8,2),
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(horse_id, race_id)
);

CREATE TABLE IF NOT EXISTS odds_snapshots (
  id SERIAL PRIMARY KEY,
  race_id TEXT NOT NULL,
  horse_id TEXT NOT NULL,
  bookmaker TEXT,
  win_odds DECIMAL(10,3),
  place_odds DECIMAL(10,3),
  observed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(race_id, horse_id, bookmaker, observed_at)
);

CREATE INDEX IF NOT EXISTS idx_results_race ON results(race_id);
CREATE INDEX IF NOT EXISTS idx_results_horse ON results(horse_id);
CREATE INDEX IF NOT EXISTS idx_form_history_horse ON horse_form_history(horse_id);
CREATE INDEX IF NOT EXISTS idx_form_history_date ON horse_form_history(race_date);
CREATE INDEX IF NOT EXISTS idx_form_stats_horse_race ON runner_form_stats(horse_id, race_id);
CREATE INDEX IF NOT EXISTS idx_speed_figures_horse ON speed_figures(horse_id);
CREATE INDEX IF NOT EXISTS idx_odds_race_horse ON odds_snapshots(race_id, horse_id);
