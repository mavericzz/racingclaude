-- AI Analysis & Learning Tables
-- Stores AI-generated race analyses, tracks accuracy, and aggregates learning insights

-- Store AI analysis results for learning
CREATE TABLE IF NOT EXISTS ai_analyses (
  id SERIAL PRIMARY KEY,
  race_id TEXT NOT NULL REFERENCES races(race_id),
  analysis TEXT NOT NULL,
  ai_top_picks JSONB,
  ai_dangers JSONB,
  ai_pace_call TEXT,
  model_used TEXT,
  tokens_prompt INT,
  tokens_completion INT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(race_id)
);

-- Track AI prediction accuracy separately
CREATE TABLE IF NOT EXISTS ai_prediction_results (
  id SERIAL PRIMARY KEY,
  race_id TEXT NOT NULL REFERENCES races(race_id),
  ai_top_pick_horse_id TEXT,
  ai_top_pick_won BOOLEAN,
  ai_top_pick_position INT,
  ai_pace_call TEXT,
  actual_pace TEXT,
  pace_call_correct BOOLEAN,
  scratching_count INT DEFAULT 0,
  track_changed BOOLEAN DEFAULT FALSE,
  original_going TEXT,
  final_going TEXT,
  analyzed_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(race_id)
);

-- Model learning insights (aggregated patterns)
CREATE TABLE IF NOT EXISTS ai_learning_insights (
  id SERIAL PRIMARY KEY,
  insight_type TEXT NOT NULL,
  insight_key TEXT NOT NULL,
  insight_data JSONB NOT NULL,
  sample_size INT,
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(insight_type, insight_key)
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_ai_analyses_race ON ai_analyses(race_id);
CREATE INDEX IF NOT EXISTS idx_ai_pred_results_race ON ai_prediction_results(race_id);
CREATE INDEX IF NOT EXISTS idx_ai_learning_type ON ai_learning_insights(insight_type);
