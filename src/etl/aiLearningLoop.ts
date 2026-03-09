/**
 * AI Learning Loop
 *
 * Post-race: scores AI predictions against actual results,
 * aggregates learning insights for future analyses.
 *
 * Called as part of the autoUpdate pipeline after results are compared.
 */

import { query, pool } from '../lib/database.js'
import pino from 'pino'

const log = pino({ name: 'ai-learning' })

export async function updateAILearning(): Promise<{ scored: number; insights: number }> {
  let scored = 0
  let insights = 0

  // 1. Score AI predictions that have results but no ai_prediction_results yet
  const unscored = await query<{
    race_id: string
    ai_top_picks: any
    ai_pace_call: string | null
  }>(`
    SELECT aa.race_id, aa.ai_top_picks, aa.ai_pace_call
    FROM ai_analyses aa
    WHERE EXISTS (SELECT 1 FROM results res WHERE res.race_id = aa.race_id)
      AND NOT EXISTS (SELECT 1 FROM ai_prediction_results apr WHERE apr.race_id = aa.race_id)
  `)

  for (const race of unscored) {
    try {
      const topPicks = Array.isArray(race.ai_top_picks) ? race.ai_top_picks : []
      const topPick = topPicks[0] ?? null

      // Get actual result for AI top pick
      let topPickWon = false
      let topPickPosition: number | null = null

      if (topPick?.horse_id) {
        const result = await query<{ position: number }>(`
          SELECT position FROM results
          WHERE race_id = $1 AND horse_id = $2
        `, [race.race_id, topPick.horse_id])
        if (result.length > 0) {
          topPickPosition = result[0].position
          topPickWon = result[0].position === 1
        }
      }

      // Get actual pace scenario from race_analysis
      const raceAnalysis = await query<{ pace_scenario: string | null }>(`
        SELECT pace_scenario FROM race_analysis WHERE race_id = $1
      `, [race.race_id])
      const actualPace = raceAnalysis[0]?.pace_scenario ?? null

      const paceCorrect = race.ai_pace_call && actualPace
        ? race.ai_pace_call.toLowerCase() === actualPace.toLowerCase()
        : null

      // Check scratchings and track changes
      const scratchCount = await query<{ cnt: number }>(`
        SELECT COUNT(*)::int AS cnt FROM runners
        WHERE race_id = $1 AND scratched = TRUE
      `, [race.race_id])

      const trackInfo = await query<{ track_condition: string | null; going: string | null }>(`
        SELECT m.track_condition, r.going
        FROM races r JOIN meetings m ON m.meeting_id = r.meeting_id
        WHERE r.race_id = $1
      `, [race.race_id])

      const tc = trackInfo[0]?.track_condition ?? null
      const going = trackInfo[0]?.going ?? null
      const trackChanged = tc && going ? tc.toLowerCase() !== going.toLowerCase() : false

      await pool.query(`
        INSERT INTO ai_prediction_results
          (race_id, ai_top_pick_horse_id, ai_top_pick_won, ai_top_pick_position,
           ai_pace_call, actual_pace, pace_call_correct,
           scratching_count, track_changed, original_going, final_going)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (race_id) DO UPDATE SET
          ai_top_pick_won = EXCLUDED.ai_top_pick_won,
          ai_top_pick_position = EXCLUDED.ai_top_pick_position,
          actual_pace = EXCLUDED.actual_pace,
          pace_call_correct = EXCLUDED.pace_call_correct
      `, [
        race.race_id, topPick?.horse_id ?? null, topPickWon, topPickPosition,
        race.ai_pace_call, actualPace, paceCorrect,
        scratchCount[0]?.cnt ?? 0, trackChanged, tc, going,
      ])

      scored++
    } catch (err: any) {
      log.error({ raceId: race.race_id, err: err.message }, 'Failed to score AI prediction')
    }
  }

  // 2. Aggregate learning insights (only if we have enough data)
  const totalScored = await query<{ cnt: number }>(`
    SELECT COUNT(*)::int AS cnt FROM ai_prediction_results
  `)

  if (totalScored[0]?.cnt >= 5) {
    insights += await aggregateVenueBias()
    insights += await aggregatePacePatterns()
    insights += await aggregateScratchingImpact()
    insights += await aggregateConditionShift()
    insights += await aggregateModelWeaknesses()
    insights += await detectSystematicErrors()
  }

  log.info({ scored, insights }, 'AI learning update complete')
  return { scored, insights }
}

async function upsertInsight(type: string, key: string, data: any, sampleSize: number): Promise<number> {
  await pool.query(`
    INSERT INTO ai_learning_insights (insight_type, insight_key, insight_data, sample_size, updated_at)
    VALUES ($1, $2, $3, $4, NOW())
    ON CONFLICT (insight_type, insight_key) DO UPDATE SET
      insight_data = EXCLUDED.insight_data,
      sample_size = EXCLUDED.sample_size,
      updated_at = NOW()
  `, [type, key, JSON.stringify(data), sampleSize])
  return 1
}

async function aggregateVenueBias(): Promise<number> {
  let count = 0
  const venues = await query<{
    venue_name: string; total: number; wins: number;
  }>(`
    SELECT v.name AS venue_name, COUNT(*)::int AS total,
           COUNT(*) FILTER (WHERE apr.ai_top_pick_won)::int AS wins
    FROM ai_prediction_results apr
    JOIN races r ON r.race_id = apr.race_id
    JOIN meetings m ON m.meeting_id = r.meeting_id
    JOIN venues v ON v.venue_id = m.venue_id
    GROUP BY v.name
    HAVING COUNT(*) >= 3
  `)

  for (const v of venues) {
    const winRate = (v.wins / v.total * 100).toFixed(1)
    count += await upsertInsight('venue_bias', v.venue_name, {
      ai_win_rate: parseFloat(winRate),
      total_races: v.total,
      wins: v.wins,
      note: `AI top-pick win rate at ${v.venue_name}: ${winRate}% (${v.wins}/${v.total})`,
    }, v.total)
  }
  return count
}

async function aggregatePacePatterns(): Promise<number> {
  let count = 0
  const patterns = await query<{
    ai_pace_call: string; total: number; correct: number;
  }>(`
    SELECT ai_pace_call, COUNT(*)::int AS total,
           COUNT(*) FILTER (WHERE pace_call_correct)::int AS correct
    FROM ai_prediction_results
    WHERE ai_pace_call IS NOT NULL AND actual_pace IS NOT NULL
    GROUP BY ai_pace_call
    HAVING COUNT(*) >= 3
  `)

  for (const p of patterns) {
    const accuracy = (p.correct / p.total * 100).toFixed(1)
    count += await upsertInsight('pace_pattern', p.ai_pace_call, {
      accuracy: parseFloat(accuracy),
      total: p.total,
      correct: p.correct,
      note: `AI pace call "${p.ai_pace_call}" accuracy: ${accuracy}% (${p.correct}/${p.total})`,
    }, p.total)
  }
  return count
}

async function aggregateScratchingImpact(): Promise<number> {
  const data = await query<{
    had_scratchings: boolean; total: number; wins: number;
  }>(`
    SELECT (scratching_count > 0) AS had_scratchings,
           COUNT(*)::int AS total,
           COUNT(*) FILTER (WHERE ai_top_pick_won)::int AS wins
    FROM ai_prediction_results
    GROUP BY had_scratchings
    HAVING COUNT(*) >= 3
  `)

  let count = 0
  for (const d of data) {
    const label = d.had_scratchings ? 'with_scratchings' : 'no_scratchings'
    const winRate = (d.wins / d.total * 100).toFixed(1)
    count += await upsertInsight('scratching_impact', label, {
      ai_win_rate: parseFloat(winRate),
      total: d.total,
      wins: d.wins,
      note: `AI accuracy ${label.replace('_', ' ')}: ${winRate}% (${d.wins}/${d.total})`,
    }, d.total)
  }
  return count
}

async function aggregateConditionShift(): Promise<number> {
  const data = await query<{
    changed: boolean; total: number; wins: number;
  }>(`
    SELECT track_changed AS changed,
           COUNT(*)::int AS total,
           COUNT(*) FILTER (WHERE ai_top_pick_won)::int AS wins
    FROM ai_prediction_results
    GROUP BY track_changed
    HAVING COUNT(*) >= 3
  `)

  let count = 0
  for (const d of data) {
    const label = d.changed ? 'track_changed' : 'track_stable'
    const winRate = (d.wins / d.total * 100).toFixed(1)
    count += await upsertInsight('condition_shift', label, {
      ai_win_rate: parseFloat(winRate),
      total: d.total,
      wins: d.wins,
      note: `AI accuracy when ${label.replace('_', ' ')}: ${winRate}% (${d.wins}/${d.total})`,
    }, d.total)
  }
  return count
}

async function aggregateModelWeaknesses(): Promise<number> {
  let count = 0

  // Compare AI vs ML by going condition
  const byGoing = await query<{
    going_group: string; total: number; ai_wins: number; ml_wins: number;
  }>(`
    SELECT
      CASE
        WHEN r.going ILIKE '%heavy%' THEN 'Heavy'
        WHEN r.going ILIKE '%soft%' THEN 'Soft'
        WHEN r.going ILIKE '%good%' THEN 'Good'
        WHEN r.going ILIKE '%firm%' THEN 'Firm'
        ELSE 'Other'
      END AS going_group,
      COUNT(*)::int AS total,
      COUNT(*) FILTER (WHERE apr.ai_top_pick_won)::int AS ai_wins,
      COUNT(*) FILTER (WHERE ra.top_pick_won)::int AS ml_wins
    FROM ai_prediction_results apr
    JOIN race_analysis ra ON ra.race_id = apr.race_id
    JOIN races r ON r.race_id = apr.race_id
    GROUP BY going_group
    HAVING COUNT(*) >= 5
  `)

  for (const g of byGoing) {
    const aiRate = (g.ai_wins / g.total * 100).toFixed(1)
    const mlRate = (g.ml_wins / g.total * 100).toFixed(1)
    const better = g.ai_wins > g.ml_wins ? 'AI' : g.ml_wins > g.ai_wins ? 'ML' : 'equal'

    count += await upsertInsight('model_weakness', `going_${g.going_group.toLowerCase()}`, {
      ai_win_rate: parseFloat(aiRate),
      ml_win_rate: parseFloat(mlRate),
      total: g.total,
      better,
      note: `On ${g.going_group} tracks: AI ${aiRate}% vs ML ${mlRate}% (${g.total} races). ${better === 'AI' ? 'AI outperforms — trust your adjustments' : better === 'ML' ? 'ML outperforms — trust the model more' : 'Equal performance'}`,
    }, g.total)
  }
  return count
}

async function detectSystematicErrors(): Promise<number> {
  let count = 0

  // Check if AI consistently over-rates leaders
  const leaderBias = await query<{ total: number; wins: number }>(`
    SELECT COUNT(*)::int AS total,
           COUNT(*) FILTER (WHERE apr.ai_top_pick_won)::int AS wins
    FROM ai_prediction_results apr
    JOIN ai_analyses aa ON aa.race_id = apr.race_id
    WHERE aa.ai_top_picks IS NOT NULL
      AND EXISTS (
        SELECT 1 FROM horse_form_history fh
        WHERE fh.horse_id = COALESCE(
          (SELECT canonical_id FROM horse_id_map WHERE aus_id = (aa.ai_top_picks->0->>'horse_id')),
          (aa.ai_top_picks->0->>'horse_id')
        )
        AND fh.running_style = 'leader'
        ORDER BY fh.race_date DESC LIMIT 1
      )
  `)

  if (leaderBias[0]?.total >= 5) {
    const winRate = (leaderBias[0].wins / leaderBias[0].total * 100).toFixed(1)
    if (parseFloat(winRate) < 20) {
      count += await upsertInsight('systematic_error', 'leader_over_rating', {
        win_rate: parseFloat(winRate),
        total: leaderBias[0].total,
        note: `You tend to over-rate leaders as top picks (${winRate}% win rate). Consider closers more when pace is hot.`,
      }, leaderBias[0].total)
    }
  }

  // Check recent streak (last 5 all wrong = systematic issue)
  const recentResults = await query<{ won: boolean }>(`
    SELECT ai_top_pick_won AS won FROM ai_prediction_results
    ORDER BY analyzed_at DESC LIMIT 5
  `)
  if (recentResults.length >= 5 && recentResults.every(r => !r.won)) {
    count += await upsertInsight('systematic_error', 'losing_streak', {
      streak: 5,
      note: 'WARNING: Your last 5 top picks all lost. Re-evaluate your weighting — consider trusting market signals more heavily.',
    }, 5)
  }

  return count
}
