-- 0001_core.sql: Core tables for Australian thoroughbred racing

CREATE TABLE IF NOT EXISTS venues (
  venue_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  state TEXT,               -- NSW, VIC, QLD, SA, WA, TAS, ACT, NT
  country TEXT DEFAULT 'AU',
  track_direction TEXT,     -- clockwise, anticlockwise, straight
  track_circumference_m INTEGER,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS meetings (
  meeting_id TEXT PRIMARY KEY,
  venue_id TEXT REFERENCES venues(venue_id),
  meeting_date DATE NOT NULL,
  rail_position TEXT,
  weather TEXT,
  track_condition TEXT,     -- firm, good, soft, heavy
  source TEXT DEFAULT 'TRA_AU',
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS races (
  race_id TEXT PRIMARY KEY,
  meeting_id TEXT REFERENCES meetings(meeting_id),
  race_number INTEGER NOT NULL,
  race_name TEXT,
  class TEXT,               -- BM72, CL1, MDN, G1, etc.
  race_group TEXT,          -- Group 1, Group 2, Listed, ungrouped
  distance_m INTEGER,
  race_status TEXT,         -- Results, Abandoned, etc.
  off_time TIMESTAMPTZ,
  prize_total DECIMAL(12,2),
  field_size INTEGER,
  going TEXT,               -- track condition at race time
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(meeting_id, race_number)
);

CREATE TABLE IF NOT EXISTS horses (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  sire TEXT,
  sire_id TEXT,
  dam TEXT,
  dam_id TEXT,
  damsire TEXT,
  damsire_id TEXT,
  age TEXT,
  sex TEXT,
  colour TEXT,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS jockeys (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  location TEXT,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS trainers (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  location TEXT,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS owners (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS runners (
  id SERIAL PRIMARY KEY,
  race_id TEXT NOT NULL REFERENCES races(race_id),
  horse_id TEXT REFERENCES horses(id),
  jockey_id TEXT REFERENCES jockeys(id),
  trainer_id TEXT REFERENCES trainers(id),
  owner_id TEXT REFERENCES owners(id),
  horse TEXT,
  number INTEGER,
  draw INTEGER,
  weight_lbs DECIMAL(6,2),
  jockey_claim INTEGER DEFAULT 0,
  form TEXT,                -- form string e.g. "1x234"
  headgear TEXT,            -- b=blinkers, v=visor, t=tongue tie, etc.
  headgear_run TEXT,
  wind_surgery TEXT,
  rating INTEGER,           -- official rating
  sp_decimal DECIMAL(10,3),
  position INTEGER,
  margin TEXT,
  comment TEXT,
  scratched BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(race_id, horse_id)
);

CREATE INDEX IF NOT EXISTS idx_runners_race_id ON runners(race_id);
CREATE INDEX IF NOT EXISTS idx_runners_horse_id ON runners(horse_id);
CREATE INDEX IF NOT EXISTS idx_runners_jockey_id ON runners(jockey_id);
CREATE INDEX IF NOT EXISTS idx_runners_trainer_id ON runners(trainer_id);
CREATE INDEX IF NOT EXISTS idx_races_meeting_id ON races(meeting_id);
CREATE INDEX IF NOT EXISTS idx_races_off_time ON races(off_time);
CREATE INDEX IF NOT EXISTS idx_meetings_date ON meetings(meeting_date);
