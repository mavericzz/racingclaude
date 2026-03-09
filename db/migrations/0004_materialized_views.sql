-- 0004_materialized_views.sql: Stats views for ML features

-- Trainer stats (rolling 365 days)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_trainer_stats AS
SELECT
  r.trainer_id,
  t.name AS trainer_name,
  COUNT(*) AS total_runs,
  COUNT(CASE WHEN res.position = 1 THEN 1 END) AS wins,
  COUNT(CASE WHEN res.position <= 3 THEN 1 END) AS places,
  COUNT(DISTINCT r.horse_id) AS unique_horses,
  ROUND(COUNT(CASE WHEN res.position = 1 THEN 1 END)::DECIMAL / NULLIF(COUNT(*), 0) * 100, 2) AS win_pct,
  ROUND(COUNT(CASE WHEN res.position <= 3 THEN 1 END)::DECIMAL / NULLIF(COUNT(*), 0) * 100, 2) AS place_pct,
  COALESCE(SUM(res.prize), 0) AS total_prize_money,
  ROUND(AVG(res.official_rating), 1) AS avg_rating
FROM runners r
JOIN results res ON r.race_id = res.race_id AND r.horse_id = res.horse_id
JOIN races rc ON r.race_id = rc.race_id
JOIN trainers t ON r.trainer_id = t.id
WHERE rc.off_time >= CURRENT_DATE - INTERVAL '365 days'
  AND r.scratched = FALSE
GROUP BY r.trainer_id, t.name;

-- Jockey stats (rolling 365 days)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_jockey_stats AS
SELECT
  r.jockey_id,
  j.name AS jockey_name,
  COUNT(*) AS total_rides,
  COUNT(CASE WHEN res.position = 1 THEN 1 END) AS wins,
  COUNT(CASE WHEN res.position <= 3 THEN 1 END) AS places,
  COUNT(DISTINCT r.horse_id) AS unique_horses,
  ROUND(COUNT(CASE WHEN res.position = 1 THEN 1 END)::DECIMAL / NULLIF(COUNT(*), 0) * 100, 2) AS win_pct,
  ROUND(COUNT(CASE WHEN res.position <= 3 THEN 1 END)::DECIMAL / NULLIF(COUNT(*), 0) * 100, 2) AS place_pct,
  COALESCE(SUM(res.prize), 0) AS total_prize_money,
  ROUND(AVG(res.official_rating), 1) AS avg_rating
FROM runners r
JOIN results res ON r.race_id = res.race_id AND r.horse_id = res.horse_id
JOIN races rc ON r.race_id = rc.race_id
JOIN jockeys j ON r.jockey_id = j.id
WHERE rc.off_time >= CURRENT_DATE - INTERVAL '365 days'
  AND r.scratched = FALSE
GROUP BY r.jockey_id, j.name;

-- Trainer-Jockey combo stats
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_combo_stats AS
SELECT
  r.trainer_id,
  r.jockey_id,
  t.name AS trainer_name,
  j.name AS jockey_name,
  COUNT(*) AS total_runs,
  COUNT(CASE WHEN res.position = 1 THEN 1 END) AS wins,
  ROUND(COUNT(CASE WHEN res.position = 1 THEN 1 END)::DECIMAL / NULLIF(COUNT(*), 0) * 100, 2) AS win_pct,
  COUNT(CASE WHEN res.position <= 3 THEN 1 END) AS places,
  ROUND(COUNT(CASE WHEN res.position <= 3 THEN 1 END)::DECIMAL / NULLIF(COUNT(*), 0) * 100, 2) AS place_pct
FROM runners r
JOIN results res ON r.race_id = res.race_id AND r.horse_id = res.horse_id
JOIN races rc ON r.race_id = rc.race_id
JOIN trainers t ON r.trainer_id = t.id
JOIN jockeys j ON r.jockey_id = j.id
WHERE rc.off_time >= CURRENT_DATE - INTERVAL '365 days'
  AND r.scratched = FALSE
GROUP BY r.trainer_id, r.jockey_id, t.name, j.name
HAVING COUNT(*) >= 3;

-- Trainer first-up / second-up / third-up stats
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_trainer_spell_stats AS
SELECT
  hfh.trainer_id,
  t.name AS trainer_name,
  CASE
    WHEN hfh.days_since_prev_run IS NULL OR hfh.days_since_prev_run > 60 THEN 'first-up'
    ELSE 'fit'
  END AS spell_status,
  COUNT(*) AS total_runs,
  COUNT(CASE WHEN hfh.position = 1 THEN 1 END) AS wins,
  ROUND(COUNT(CASE WHEN hfh.position = 1 THEN 1 END)::DECIMAL / NULLIF(COUNT(*), 0) * 100, 2) AS win_pct,
  COUNT(CASE WHEN hfh.position <= 3 THEN 1 END) AS places,
  ROUND(COUNT(CASE WHEN hfh.position <= 3 THEN 1 END)::DECIMAL / NULLIF(COUNT(*), 0) * 100, 2) AS place_pct
FROM horse_form_history hfh
JOIN trainers t ON hfh.trainer_id = t.id
WHERE hfh.race_date >= CURRENT_DATE - INTERVAL '365 days'
GROUP BY hfh.trainer_id, t.name, spell_status;

-- Track bias by venue/distance/going
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_track_bias AS
SELECT
  rc.meeting_id,
  m.venue_id,
  v.name AS venue_name,
  CASE
    WHEN rc.distance_m <= 1100 THEN 'sprint'
    WHEN rc.distance_m <= 1400 THEN 'short'
    WHEN rc.distance_m <= 1800 THEN 'mile'
    WHEN rc.distance_m <= 2200 THEN 'middle'
    ELSE 'staying'
  END AS distance_bucket,
  rc.going,
  COUNT(*) AS total_races,
  -- Running style bias (requires horse_form_history populated)
  COUNT(CASE WHEN hfh.running_style = 'leader' AND res.position = 1 THEN 1 END) AS leader_wins,
  COUNT(CASE WHEN hfh.running_style = 'on-pace' AND res.position = 1 THEN 1 END) AS on_pace_wins,
  COUNT(CASE WHEN hfh.running_style = 'mid' AND res.position = 1 THEN 1 END) AS mid_wins,
  COUNT(CASE WHEN hfh.running_style = 'backmarker' AND res.position = 1 THEN 1 END) AS back_wins,
  -- Barrier draw bias
  COUNT(CASE WHEN r.draw BETWEEN 1 AND 4 AND res.position = 1 THEN 1 END) AS inside_draw_wins,
  COUNT(CASE WHEN r.draw BETWEEN 5 AND 8 AND res.position = 1 THEN 1 END) AS middle_draw_wins,
  COUNT(CASE WHEN r.draw >= 9 AND res.position = 1 THEN 1 END) AS outside_draw_wins
FROM races rc
JOIN meetings m ON rc.meeting_id = m.meeting_id
JOIN venues v ON m.venue_id = v.venue_id
JOIN runners r ON rc.race_id = r.race_id
JOIN results res ON r.race_id = res.race_id AND r.horse_id = res.horse_id
LEFT JOIN horse_form_history hfh ON r.horse_id = hfh.horse_id AND r.race_id = hfh.race_id
WHERE rc.off_time >= CURRENT_DATE - INTERVAL '365 days'
  AND r.scratched = FALSE
GROUP BY rc.meeting_id, m.venue_id, v.name, distance_bucket, rc.going;

-- Barrier stats by venue/distance
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_barrier_stats AS
SELECT
  m.venue_id,
  v.name AS venue_name,
  CASE
    WHEN rc.distance_m <= 1100 THEN 'sprint'
    WHEN rc.distance_m <= 1400 THEN 'short'
    WHEN rc.distance_m <= 1800 THEN 'mile'
    WHEN rc.distance_m <= 2200 THEN 'middle'
    ELSE 'staying'
  END AS distance_bucket,
  CASE
    WHEN r.draw BETWEEN 1 AND 4 THEN 'inside'
    WHEN r.draw BETWEEN 5 AND 8 THEN 'middle'
    WHEN r.draw >= 9 THEN 'outside'
  END AS barrier_group,
  COUNT(*) AS total_runs,
  COUNT(CASE WHEN res.position = 1 THEN 1 END) AS wins,
  ROUND(COUNT(CASE WHEN res.position = 1 THEN 1 END)::DECIMAL / NULLIF(COUNT(*), 0) * 100, 2) AS win_pct,
  COUNT(CASE WHEN res.position <= 3 THEN 1 END) AS places,
  ROUND(COUNT(CASE WHEN res.position <= 3 THEN 1 END)::DECIMAL / NULLIF(COUNT(*), 0) * 100, 2) AS place_pct
FROM runners r
JOIN races rc ON r.race_id = rc.race_id
JOIN meetings m ON rc.meeting_id = m.meeting_id
JOIN venues v ON m.venue_id = v.venue_id
JOIN results res ON r.race_id = res.race_id AND r.horse_id = res.horse_id
WHERE rc.off_time >= CURRENT_DATE - INTERVAL '365 days'
  AND r.scratched = FALSE
  AND r.draw IS NOT NULL
GROUP BY m.venue_id, v.name, distance_bucket, barrier_group;
