/**
 * API server for RacingClaude frontend.
 * Serves meetings, races, runners, and AI predictions.
 */
import 'dotenv/config'
import express from 'express'
import cors from 'cors'
import { query, pool } from '../lib/database.js'
import { readFileSync } from 'fs'
import { LGBMPredictor } from '../ai/models/lgbmInference.js'
import { analyzeRace } from '../ai/models/betting.js'
import { storePredictions } from '../etl/autoUpdateResults.js'
import { computeAllFeatures, flattenFeatures } from '../ai/features/index.js'
import { generateAIAnalysis } from './aiAnalysis.js'

const app = express()
app.use(cors())
app.use(express.json())

// Auto-run migrations for AI learning tables (idempotent)
async function runMigrations() {
  try {
    await pool.query(`
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
      CREATE TABLE IF NOT EXISTS ai_learning_insights (
        id SERIAL PRIMARY KEY,
        insight_type TEXT NOT NULL,
        insight_key TEXT NOT NULL,
        insight_data JSONB NOT NULL,
        sample_size INT,
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(insight_type, insight_key)
      );
    `)
    console.log('AI learning tables ready')
  } catch (err) {
    console.warn('AI migration skipped (tables may already exist):', (err as Error).message)
  }
}
runMigrations()

// Resolve hrs_aus_* IDs to canonical hrs_* IDs for data lookups
async function resolveHorseIds(horseIds: string[]): Promise<Map<string, string>> {
  if (horseIds.length === 0) return new Map()
  const ausIds = horseIds.filter(id => id.startsWith('hrs_aus_'))
  if (ausIds.length === 0) return new Map()
  const mappings = await query<{ aus_id: string; canonical_id: string }>(
    `SELECT aus_id, canonical_id FROM horse_id_map WHERE aus_id = ANY($1)`,
    [ausIds]
  )
  return new Map(mappings.map(m => [m.aus_id, m.canonical_id]))
}

// Get the lookup ID for a horse (canonical if mapped, otherwise original)
function lookupId(horseId: string, idMap: Map<string, string>): string {
  return idMap.get(horseId) ?? horseId
}

// Load ensemble model at startup
const modelPath = new URL('../ai/models/modelWeights.json', import.meta.url)
const modelJson = JSON.parse(readFileSync(modelPath, 'utf-8'))
let xgbJson: unknown = undefined
try {
  const xgbPath = new URL('../ai/models/xgb_model.json', import.meta.url)
  xgbJson = JSON.parse(readFileSync(xgbPath, 'utf-8'))
} catch { /* XGBoost model optional */ }
let catboostJson: unknown = undefined
try {
  const catboostPath = new URL('../ai/models/catboost_model.json', import.meta.url)
  catboostJson = JSON.parse(readFileSync(catboostPath, 'utf-8'))
} catch { /* CatBoost model optional */ }
const predictor = new LGBMPredictor(modelJson, xgbJson, catboostJson)
const featureNames: string[] = modelJson.feature_names
const ew = predictor.ensembleWeights
console.log(`Model loaded: ${predictor.version}, ${featureNames.length} features, weights: lgbm=${ew.lgbm.toFixed(2)} xgb=${ew.xgb.toFixed(2)} cat=${ew.catboost.toFixed(2)}`)

// GET /api/meetings?date=YYYY-MM-DD
app.get('/api/meetings', async (req, res) => {
  try {
    const date = (req.query.date as string) || new Date().toISOString().slice(0, 10)

    const meetings = await query<{
      meeting_id: string; venue_name: string; state: string;
      meeting_date: string; weather: string; track_condition: string;
    }>(
      `SELECT m.meeting_id, v.name AS venue_name, v.state, m.meeting_date::text,
              m.weather, m.track_condition
       FROM meetings m
       JOIN venues v ON m.venue_id = v.venue_id
       WHERE m.meeting_date = $1::date
       ORDER BY v.name`,
      [date]
    )

    // Get races for each meeting with top runners
    const result = await Promise.all(meetings.map(async (m) => {
      const races = await query<{
        race_id: string; race_number: number; race_name: string;
        distance_m: number; class: string; going: string;
        off_time: string; field_size: number; prize_total: number;
      }>(
        `SELECT race_id, race_number, race_name, distance_m, class, going,
                off_time, field_size, prize_total
         FROM races
         WHERE meeting_id = $1
           AND COALESCE(is_trial, FALSE) = FALSE
           AND COALESCE(is_jump_out, FALSE) = FALSE
         ORDER BY COALESCE(race_number, 0), off_time`,
        [m.meeting_id]
      )

      // Get runners for each race (top 6 by odds) + prediction summaries
      const racesWithRunners = await Promise.all(races.map(async (race) => {
        const runners = await query<{
          horse_id: string; horse: string; number: number;
          draw: number; sp_decimal: number; jockey_name: string;
        }>(
          `SELECT ru.horse_id, ru.horse, ru.number, ru.draw,
                  COALESCE(ru.sp_decimal, (
                    SELECT os.win_odds FROM odds_snapshots os
                    WHERE os.race_id = ru.race_id AND os.horse_id = ru.horse_id
                    ORDER BY os.observed_at DESC LIMIT 1
                  )) AS sp_decimal,
                  j.name AS jockey_name
           FROM runners ru
           LEFT JOIN jockeys j ON ru.jockey_id = j.id
           WHERE ru.race_id = $1 AND ru.scratched = FALSE
           ORDER BY COALESCE(ru.sp_decimal, (
             SELECT os.win_odds FROM odds_snapshots os
             WHERE os.race_id = ru.race_id AND os.horse_id = ru.horse_id
             ORDER BY os.observed_at DESC LIMIT 1
           ), 999), ru.number
           LIMIT 8`,
          [race.race_id]
        )

        // Get winner for completed races
        const winnerRow = await query<{ horse: string; sp_decimal: number | null }>(`
          SELECT ru.horse, res.sp_decimal::float
          FROM results res
          JOIN runners ru ON res.race_id = ru.race_id AND res.horse_id = ru.horse_id
          WHERE res.race_id = $1 AND res.position = 1
          LIMIT 1
        `, [race.race_id])
        const winner = winnerRow.length > 0 ? {
          horseName: winnerRow[0].horse?.replace(' (AUS)', '').replace(' (NZ)', ''),
          sp: winnerRow[0].sp_decimal,
        } : null

        // Get stored prediction summary if available
        const predSummary = await query<{
          horse_id: string; horse_name: string;
          predicted_rank: number; predicted_win_prob: number;
          verdict: string; edge_pct: number;
          market_odds: number;
        }>(`
          SELECT pr.horse_id, ru.horse as horse_name,
                 pr.predicted_rank, pr.predicted_win_prob::float,
                 pr.verdict, pr.edge_pct::float,
                 pr.market_odds_at_prediction::float as market_odds
          FROM prediction_results pr
          LEFT JOIN runners ru ON pr.race_id = ru.race_id AND pr.horse_id = ru.horse_id
          WHERE pr.race_id = $1
          ORDER BY pr.predicted_rank
          LIMIT 5
        `, [race.race_id])

        const topPick = predSummary.length > 0 ? {
          horseName: predSummary[0].horse_name,
          winProb: predSummary[0].predicted_win_prob,
          verdict: predSummary[0].verdict,
        } : null

        const valueBetCount = predSummary.filter(p =>
          p.verdict === 'strong-value' || p.verdict === 'value'
        ).length

        return { ...race, runners, winner, topPick, valueBetCount }
      }))

      return { ...m, races: racesWithRunners }
    }))

    res.json(result)
  } catch (err) {
    console.error('GET /api/meetings error:', err)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// GET /api/meetings/dates - available dates
app.get('/api/meetings/dates', async (_req, res) => {
  try {
    const dates = await query<{ meeting_date: string; count: number }>(
      `SELECT m.meeting_date::text, COUNT(DISTINCT m.meeting_id)::int AS count
       FROM meetings m
       WHERE EXISTS (
         SELECT 1 FROM races r
         WHERE r.meeting_id = m.meeting_id
           AND COALESCE(r.is_trial, FALSE) = FALSE
           AND COALESCE(r.is_jump_out, FALSE) = FALSE
       )
       GROUP BY m.meeting_date
       ORDER BY ABS(m.meeting_date - CURRENT_DATE), m.meeting_date DESC
       LIMIT 60`
    )
    res.json(dates)
  } catch (err) {
    console.error('GET /api/meetings/dates error:', err)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// GET /api/races/:raceId
app.get('/api/races/:raceId', async (req, res) => {
  try {
    const { raceId } = req.params

    const race = await query<{
      race_id: string; race_name: string; race_number: number;
      distance_m: number; class: string; going: string;
      off_time: string; field_size: number; prize_total: number;
      meeting_id: string; venue_name: string; state: string; meeting_date: string;
    }>(
      `SELECT r.race_id, r.race_name, r.race_number, r.distance_m, r.class, r.going,
              r.off_time, r.field_size, r.prize_total, r.meeting_id,
              v.name AS venue_name, v.state, m.meeting_date::text
       FROM races r
       JOIN meetings m ON r.meeting_id = m.meeting_id
       JOIN venues v ON m.venue_id = v.venue_id
       WHERE r.race_id = $1`,
      [raceId]
    )

    if (race.length === 0) {
      return res.status(404).json({ error: 'Race not found' })
    }

    // Get runners
    const runners = await query<{
      horse_id: string; horse: string; number: number; draw: number;
      weight_lbs: number; jockey_claim: number; rating: number;
      sp_decimal: number; form: string; headgear: string;
      jockey_name: string; trainer_name: string; scratched: boolean;
      jockey_id: string; trainer_id: string;
    }>(
      `SELECT ru.horse_id, ru.horse, ru.number, ru.draw, ru.weight_lbs,
              ru.jockey_claim, ru.rating,
              COALESCE(ru.sp_decimal, (
                SELECT os.win_odds FROM odds_snapshots os
                WHERE os.race_id = ru.race_id AND os.horse_id = ru.horse_id
                ORDER BY os.observed_at DESC LIMIT 1
              )) AS sp_decimal,
              ru.form, ru.headgear,
              j.name AS jockey_name, t.name AS trainer_name, ru.scratched,
              ru.jockey_id, ru.trainer_id
       FROM runners ru
       LEFT JOIN jockeys j ON ru.jockey_id = j.id
       LEFT JOIN trainers t ON ru.trainer_id = t.id
       WHERE ru.race_id = $1
       ORDER BY ru.number, ru.draw`,
      [raceId]
    )

    // Get results if available
    const results = await query<{
      horse_id: string; position: number; sp_decimal: number; beaten_lengths: number;
    }>(
      `SELECT horse_id, position, sp_decimal, beaten_lengths
       FROM results WHERE race_id = $1`,
      [raceId]
    )
    const resultMap = new Map(results.map(r => [r.horse_id, r]))

    // Get speed figures and form stats per runner (with horse ID mapping)
    const horseIds = runners.filter(r => !r.scratched).map(r => r.horse_id)
    const idMap = await resolveHorseIds(horseIds)
    const lookupIds = [...new Set(horseIds.map(id => lookupId(id, idMap)))]

    // Last speed figure per horse
    const speedFigs = lookupIds.length > 0 ? await query<{
      horse_id: string; last_fig: number; avg_fig: number; runs: number; wins: number;
    }>(
      `SELECT
        sf_data.horse_id,
        sf_data.last_fig,
        sf_data.avg_fig,
        COALESCE(form.runs, 0) AS runs,
        COALESCE(form.wins, 0) AS wins
       FROM (
        SELECT DISTINCT ON (sf.horse_id) sf.horse_id,
          sf.adjusted_speed_figure AS last_fig,
          (SELECT AVG(sf2.adjusted_speed_figure) FROM speed_figures sf2
           WHERE sf2.horse_id = sf.horse_id) AS avg_fig
        FROM speed_figures sf
        JOIN races r2 ON sf.race_id = r2.race_id
        JOIN meetings m2 ON r2.meeting_id = m2.meeting_id
        WHERE sf.horse_id = ANY($1)
        ORDER BY sf.horse_id, m2.meeting_date DESC
       ) sf_data
       LEFT JOIN (
        SELECT horse_id, COUNT(*)::int AS runs,
               COUNT(*) FILTER (WHERE position = 1)::int AS wins
        FROM horse_form_history
        GROUP BY horse_id
       ) form ON sf_data.horse_id = form.horse_id`,
      [lookupIds]
    ) : []
    const speedMap = new Map(speedFigs.map(s => [s.horse_id, s]))

    // Jockey/trainer stats
    const jockeyIds = [...new Set(runners.map(r => (r as any).jockey_id).filter(Boolean))]
    const trainerIds = [...new Set(runners.map(r => (r as any).trainer_id).filter(Boolean))]

    const jockeyStats = jockeyIds.length > 0 ? await query<{
      jockey_id: string; win_pct: number;
    }>(
      `SELECT jockey_id, win_pct FROM mv_jockey_stats WHERE jockey_id = ANY($1)`,
      [jockeyIds]
    ) : []
    const jockeyStatsMap = new Map(jockeyStats.map(j => [j.jockey_id, j]))

    const trainerStats = trainerIds.length > 0 ? await query<{
      trainer_id: string; win_pct: number;
    }>(
      `SELECT trainer_id, win_pct FROM mv_trainer_stats WHERE trainer_id = ANY($1)`,
      [trainerIds]
    ) : []
    const trainerStatsMap = new Map(trainerStats.map(t => [t.trainer_id, t]))

    const runnersWithResults = runners.map(r => {
      const lid = lookupId(r.horse_id, idMap)
      const speed = speedMap.get(lid)
      const jStats = jockeyStatsMap.get((r as any).jockey_id)
      const tStats = trainerStatsMap.get((r as any).trainer_id)
      return {
        ...r,
        result: resultMap.get(r.horse_id) || null,
        lastSpeedFig: speed?.last_fig ?? null,
        avgSpeedFig: speed?.avg_fig ?? null,
        careerRuns: speed?.runs ?? null,
        careerWins: speed?.wins ?? null,
        jockeyWinPct: jStats?.win_pct ?? null,
        trainerWinPct: tStats?.win_pct ?? null,
      }
    })

    res.json({ ...race[0], runners: runnersWithResults })
  } catch (err) {
    console.error('GET /api/races/:raceId error:', err)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// GET /api/predictions/:raceId
app.get('/api/predictions/:raceId', async (req, res) => {
  try {
    const { raceId } = req.params

    const race = await query<{
      race_id: string; distance_m: number; going: string; field_size: number;
      meeting_date: string;
    }>(
      `SELECT r.race_id, r.distance_m, r.going, r.field_size, m.meeting_date::text
       FROM races r
       JOIN meetings m ON r.meeting_id = m.meeting_id
       WHERE r.race_id = $1`,
      [raceId]
    )

    if (race.length === 0) {
      return res.status(404).json({ error: 'Race not found' })
    }
    const r = race[0]

    // Get active runners with best available odds
    const runners = await query<{
      horse_id: string; horse: string; draw: number; weight_lbs: number;
      jockey_claim: number; rating: number; sp_decimal: number; live_odds: number;
    }>(
      `SELECT ru.horse_id, ru.horse, ru.draw, ru.weight_lbs, ru.jockey_claim,
              ru.rating, ru.sp_decimal,
              (SELECT os.win_odds FROM odds_snapshots os
               WHERE os.race_id = ru.race_id AND os.horse_id = ru.horse_id
               ORDER BY os.observed_at DESC LIMIT 1) AS live_odds
       FROM runners ru
       WHERE ru.race_id = $1 AND ru.scratched = FALSE
       ORDER BY ru.number`,
      [raceId]
    )

    if (runners.length === 0) {
      return res.json({ raceId, predictions: [], valueBets: [] })
    }

    const raceDate = new Date(r.meeting_date + 'T00:00:00')

    // Build full feature vectors using computeAllFeatures (resolves canonical IDs internally)
    const predictions = await Promise.all(runners.map(async runner => {
      const fv = await computeAllFeatures(runner.horse_id, raceId, raceDate)
      const { names, values } = flattenFeatures(fv)

      // Convert to features dict
      const features: Record<string, number | null> = {}
      names.forEach((name, i) => { features[name] = values[i] })

      // Best available odds for market features
      const bestOdds = Number(runner.sp_decimal) || Number(runner.live_odds) || null

      return {
        horseId: runner.horse_id,
        horseName: runner.horse ?? 'Unknown',
        features,
        marketOdds: bestOdds || 10,
      }
    }))

    // Compute speed_vs_field_avg (relative to race average)
    const speeds = predictions.map(p => p.features['avg_speed_figure_last5']).filter(v => v != null) as number[]
    if (speeds.length > 0) {
      const raceAvgSpeed = speeds.reduce((a, b) => a + b, 0) / speeds.length
      for (const p of predictions) {
        const spd = p.features['avg_speed_figure_last5']
        p.features['speed_vs_field_avg'] = spd != null ? spd - raceAvgSpeed : null
      }
    }

    // Compute RPR-based per-race features
    const rprs = predictions.map(p => p.features['avg_rpr_last5']).filter(v => v != null) as number[]
    if (rprs.length > 0) {
      const raceAvgRpr = rprs.reduce((a, b) => a + b, 0) / rprs.length
      // Sort RPRs descending for rank computation
      const sortedRprs = [...rprs].sort((a, b) => b - a)
      for (const p of predictions) {
        const rpr = p.features['avg_rpr_last5']
        p.features['rpr_vs_field_avg'] = rpr != null ? rpr - raceAvgRpr : null
        p.features['field_avg_rpr'] = raceAvgRpr
        // field_strength_rank: 0 = best RPR in field, 1 = worst
        if (rpr != null && sortedRprs.length > 1) {
          const idx = sortedRprs.indexOf(rpr)
          p.features['field_strength_rank'] = idx / (sortedRprs.length - 1)
        } else {
          p.features['field_strength_rank'] = null
        }
      }
    }

    // Run model
    const raceResults = predictor.predictRace(
      predictions.map(p => ({ horseId: p.horseId, features: p.features }))
    )

    // Betting analysis
    const bets = analyzeRace(
      raceResults.map(pred => {
        const runner = predictions.find(p => p.horseId === pred.horseId)!
        return {
          horseId: pred.horseId,
          horseName: runner.horseName,
          modelProb: pred.winProb,
          marketOdds: runner.marketOdds,
        }
      })
    )

    const predictionResults = raceResults.map(pred => {
      const runner = predictions.find(p => p.horseId === pred.horseId)!
      const bet = bets.find(b => b.horseId === pred.horseId)!
      return {
        horseId: pred.horseId,
        horseName: runner.horseName,
        rank: pred.rank,
        winProb: pred.winProb,
        marketOdds: runner.marketOdds,
        fairOdds: pred.winProb > 0 ? 1 / pred.winProb : 999,
        edgePct: bet.edgePct,
        kellyFraction: bet.kellyFraction,
        recommendedStake: bet.recommendedStake,
        verdict: bet.verdict,
      }
    })

    const valueBets = predictionResults.filter(
      p => p.verdict === 'strong-value' || p.verdict === 'value'
    )

    // Persist predictions for post-race analysis (fire and forget)
    storePredictions(raceId, predictor.version, predictionResults.map(p => ({
      horseId: p.horseId,
      rank: p.rank,
      winProb: p.winProb,
      marketOdds: p.marketOdds,
      edgePct: p.edgePct,
      verdict: p.verdict,
    }))).catch(err => console.error('Failed to persist predictions:', err))

    res.json({ raceId, predictions: predictionResults, valueBets })
  } catch (err) {
    console.error('GET /api/predictions/:raceId error:', err)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// GET /api/races/:raceId/sectionals
app.get('/api/races/:raceId/sectionals', async (req, res) => {
  try {
    const { raceId } = req.params

    // Get race context (distance, going, class)
    const raceCtx = await query<{
      distance_m: number | null; going: string | null; class: string | null;
      venue_name: string; track_condition: string | null;
    }>(`
      SELECT r.distance_m, r.going, r.class, v.name as venue_name, m.track_condition
      FROM races r
      JOIN meetings m ON r.meeting_id = m.meeting_id
      JOIN venues v ON m.venue_id = v.venue_id
      WHERE r.race_id = $1
    `, [raceId])

    const race = raceCtx[0] ?? null

    // Get sectionals joined with runner weight/barrier
    const sectionals = await query<{
      horse_name: string; horse_id: string | null; horse_number: number;
      barrier: number | null;
      speed_800m: number | null; speed_600m: number | null;
      speed_400m: number | null; speed_200m: number | null;
      speed_finish: number | null; speed_avg: number | null;
      scraper_odds: number | null;
      weight_kg: number | null;
    }>(`
      SELECT st.horse_name, st.horse_id, st.horse_number, st.barrier,
             st.speed_800m::float, st.speed_600m::float, st.speed_400m::float,
             st.speed_200m::float, st.speed_finish::float, st.speed_avg::float,
             st.scraper_odds::float,
             COALESCE(sfd.weight_kg, ru.weight_lbs)::float as weight_kg
      FROM sectional_times st
      LEFT JOIN scraper_form_data sfd ON st.race_id = sfd.race_id AND st.horse_name = sfd.horse_name
      LEFT JOIN runners ru ON st.race_id = ru.race_id AND st.horse_id = ru.horse_id
      WHERE st.race_id = $1
      ORDER BY st.horse_number
    `, [raceId])

    // Compute field averages per split
    const splits = ['speed_800m', 'speed_600m', 'speed_400m', 'speed_200m', 'speed_finish', 'speed_avg'] as const
    const averages: Record<string, number | null> = {}
    for (const split of splits) {
      const vals = sectionals.map(s => s[split]).filter((v): v is number => v !== null)
      averages[split] = vals.length > 0 ? vals.reduce((a, b) => a + b, 0) / vals.length : null
    }

    // Compute field average weight for weight-adjustment
    const weights = sectionals.map(s => s.weight_kg).filter((v): v is number => v !== null)
    const avgWeight = weights.length > 0 ? weights.reduce((a, b) => a + b, 0) / weights.length : null

    // Weight-adjusted speeds: ~0.5 km/h per kg above/below field avg weight
    // Heavier horse at same raw speed → higher adjusted speed (performing better)
    const WGT_ADJUSTMENT_PER_KG = 0.5
    const adjusted = sectionals.map(s => {
      const wAdj = (s.weight_kg != null && avgWeight != null)
        ? (s.weight_kg - avgWeight) * WGT_ADJUSTMENT_PER_KG
        : 0

      const adjust = (raw: number | null) => raw != null ? Math.round((raw + wAdj) * 100) / 100 : null

      return {
        ...s,
        adj_speed_800m: adjust(s.speed_800m),
        adj_speed_600m: adjust(s.speed_600m),
        adj_speed_400m: adjust(s.speed_400m),
        adj_speed_200m: adjust(s.speed_200m),
        adj_speed_finish: adjust(s.speed_finish),
        adj_speed_avg: adjust(s.speed_avg),
        weight_diff_kg: s.weight_kg != null && avgWeight != null
          ? Math.round((s.weight_kg - avgWeight) * 10) / 10 : null,
      }
    })

    // Adjusted field averages
    const adjSplits = ['adj_speed_800m', 'adj_speed_600m', 'adj_speed_400m', 'adj_speed_200m', 'adj_speed_finish', 'adj_speed_avg'] as const
    const adjAverages: Record<string, number | null> = {}
    for (const split of adjSplits) {
      const vals = adjusted.map(s => s[split]).filter((v): v is number => v !== null)
      adjAverages[split] = vals.length > 0 ? vals.reduce((a, b) => a + b, 0) / vals.length : null
    }

    res.json({
      raceId,
      raceContext: race ? {
        distance_m: race.distance_m,
        going: race.going,
        class: race.class,
        venue: race.venue_name,
        trackCondition: race.track_condition,
      } : null,
      avgWeight,
      sectionals: adjusted,
      fieldAverages: averages,
      adjFieldAverages: adjAverages,
    })
  } catch (err) {
    console.error('GET /api/races/:raceId/sectionals error:', err)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// GET /api/races/:raceId/analysis
app.get('/api/races/:raceId/analysis', async (req, res) => {
  try {
    const { raceId } = req.params

    const analysis = await query(`
      SELECT race_id, model_version, top_pick_position, top_pick_won,
             any_value_bet_won, value_bets_count, value_bets_won,
             total_staked::float, total_return::float, race_pnl::float,
             pace_scenario, leader_800m_speed::float, winner_closing_speed::float
      FROM race_analysis WHERE race_id = $1 LIMIT 1
    `, [raceId])

    const predictions = await query(`
      SELECT horse_id, predicted_win_prob::float, predicted_rank,
             market_odds_at_prediction::float as market_odds,
             edge_pct::float, verdict, actual_position, actual_sp::float,
             beaten_lengths::float, prediction_correct, value_bet_correct,
             profit_loss::float, jockey_changed, weight_changed_kg::float,
             distance_changed_m, class_changed, going_changed
      FROM prediction_results
      WHERE race_id = $1
      ORDER BY predicted_rank
    `, [raceId])

    res.json({
      raceId,
      analysis: analysis[0] || null,
      predictions,
    })
  } catch (err) {
    console.error('GET /api/races/:raceId/analysis error:', err)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// GET /api/analysis/summary?days=7
app.get('/api/analysis/summary', async (req, res) => {
  try {
    const days = parseInt(req.query.days as string) || 7

    const summary = await query<{
      total_races: number; top_pick_wins: number; top_pick_win_rate: number;
      value_bets_total: number; value_bets_won: number; value_bet_strike_rate: number;
      total_staked: number; total_return: number; cumulative_pnl: number; roi_pct: number;
    }>(`
      SELECT
        COUNT(*)::int as total_races,
        COUNT(*) FILTER (WHERE top_pick_won)::int as top_pick_wins,
        ROUND(COUNT(*) FILTER (WHERE top_pick_won)::decimal / NULLIF(COUNT(*), 0) * 100, 1) as top_pick_win_rate,
        COALESCE(SUM(value_bets_count), 0)::int as value_bets_total,
        COALESCE(SUM(value_bets_won), 0)::int as value_bets_won,
        ROUND(COALESCE(SUM(value_bets_won), 0)::decimal / NULLIF(COALESCE(SUM(value_bets_count), 0), 0) * 100, 1) as value_bet_strike_rate,
        COALESCE(SUM(total_staked), 0)::float as total_staked,
        COALESCE(SUM(total_return), 0)::float as total_return,
        COALESCE(SUM(race_pnl), 0)::float as cumulative_pnl,
        ROUND(COALESCE(SUM(race_pnl), 0)::decimal / NULLIF(COALESCE(SUM(total_staked), 0), 0) * 100, 1) as roi_pct
      FROM race_analysis ra
      JOIN races r ON r.race_id = ra.race_id
      JOIN meetings m ON m.meeting_id = r.meeting_id
      WHERE m.meeting_date >= CURRENT_DATE - ($1 || ' days')::interval
    `, [days])

    // Recent race results
    const recentRaces = await query(`
      SELECT ra.race_id, ra.top_pick_position, ra.top_pick_won,
             ra.value_bets_count, ra.value_bets_won, ra.race_pnl::float,
             ra.pace_scenario, ra.analyzed_at,
             r.race_name, r.race_number, v.name as venue_name, m.meeting_date::text
      FROM race_analysis ra
      JOIN races r ON r.race_id = ra.race_id
      JOIN meetings m ON m.meeting_id = r.meeting_id
      JOIN venues v ON v.venue_id = m.venue_id
      WHERE m.meeting_date >= CURRENT_DATE - ($1 || ' days')::interval
      ORDER BY m.meeting_date DESC, r.race_number
      LIMIT 50
    `, [days])

    res.json({ summary: summary[0] || null, recentRaces })
  } catch (err) {
    console.error('GET /api/analysis/summary error:', err)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// GET /api/races/:raceId/scratchings - scratched horses + impact
app.get('/api/races/:raceId/scratchings', async (req, res) => {
  try {
    const { raceId } = req.params

    // Get scratched runners with their form data
    const scratched = await query<{
      horse_id: string; horse: string; draw: number;
      jockey: string; trainer: string; sp_decimal: number;
      last_odds: number; avg_speed: number; last_finish: number;
    }>(`
      SELECT ru.horse_id, ru.horse, ru.draw,
             ru.jockey, ru.trainer, ru.sp_decimal,
             (SELECT os.win_odds FROM odds_snapshots os
              WHERE os.race_id = ru.race_id AND os.horse_id = ru.horse_id
              ORDER BY os.observed_at DESC LIMIT 1) AS last_odds,
             (SELECT AVG(sf.speed_figure) FROM speed_figures sf
              WHERE sf.horse_id = COALESCE(
                (SELECT canonical_id FROM horse_id_map WHERE aus_id = ru.horse_id),
                ru.horse_id
              )) AS avg_speed,
             (SELECT fh.finish_position FROM horse_form_history fh
              WHERE fh.horse_id = COALESCE(
                (SELECT canonical_id FROM horse_id_map WHERE aus_id = ru.horse_id),
                ru.horse_id
              )
              ORDER BY fh.race_date DESC LIMIT 1) AS last_finish
      FROM runners ru
      WHERE ru.race_id = $1 AND ru.scratched = TRUE
      ORDER BY ru.number
    `, [raceId])

    // Get active field size for context
    const active = await query<{ count: number }>(
      `SELECT COUNT(*)::int AS count FROM runners WHERE race_id = $1 AND scratched = FALSE`,
      [raceId]
    )

    const original = await query<{ count: number }>(
      `SELECT COUNT(*)::int AS count FROM runners WHERE race_id = $1`,
      [raceId]
    )

    res.json({
      raceId,
      scratchedRunners: scratched.map(s => ({
        horseId: s.horse_id,
        horseName: s.horse,
        draw: s.draw,
        jockey: s.jockey,
        trainer: s.trainer,
        lastOdds: Number(s.last_odds) || Number(s.sp_decimal) || null,
        avgSpeed: s.avg_speed ? Number(Number(s.avg_speed).toFixed(1)) : null,
        lastFinish: s.last_finish ? Number(s.last_finish) : null,
      })),
      originalFieldSize: original[0]?.count ?? 0,
      activeFieldSize: active[0]?.count ?? 0,
    })
  } catch (err) {
    console.error('GET /api/races/:raceId/scratchings error:', err)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// POST /api/ai-analysis/:raceId - generate AI race analysis via OpenRouter
app.post('/api/ai-analysis/:raceId', async (req, res) => {
  try {
    if (!process.env.OPENROUTER_API_KEY) {
      return res.status(503).json({ error: 'AI analysis not configured. Set OPENROUTER_API_KEY.' })
    }
    const { raceId } = req.params
    const force = req.query.force === 'true'

    // If force, delete cached analysis first
    if (force) {
      await pool.query('DELETE FROM ai_analyses WHERE race_id = $1', [raceId])
    }

    const result = await generateAIAnalysis(raceId)
    res.json(result)
  } catch (err: any) {
    console.error('POST /api/ai-analysis/:raceId error:', err)
    const status = err.message?.includes('Rate limited') ? 429
      : err.message?.includes('credits exhausted') ? 402
      : err.message?.includes('not found') ? 404
      : 500
    res.status(status).json({ error: err.message || 'AI analysis failed' })
  }
})

// POST /api/admin/update-results - trigger auto-update
app.post('/api/admin/update-results', async (_req, res) => {
  try {
    const { fetchMissingResults, compareWithPredictions } = await import('../etl/autoUpdateResults.js')
    const resultCount = await fetchMissingResults()
    const comparison = await compareWithPredictions(predictor.version)
    res.json({ resultCount, ...comparison })
  } catch (err) {
    console.error('POST /api/admin/update-results error:', err)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// In production, serve the Vite build
if (process.env.NODE_ENV === 'production') {
  const { resolve } = await import('path')
  const distPath = resolve(import.meta.dirname, '../../dist')
  app.use(express.static(distPath))
  // SPA fallback: serve index.html for any non-API route
  app.use((req, res, next) => {
    if (req.path.startsWith('/api')) return next()
    res.sendFile(resolve(distPath, 'index.html'))
  })
}

const PORT = process.env.PORT || process.env.API_PORT || 3004
app.listen(PORT, () => {
  console.log(`API server running on http://localhost:${PORT}`)
})

// Graceful shutdown
process.on('SIGTERM', async () => {
  await pool.end()
  process.exit(0)
})
