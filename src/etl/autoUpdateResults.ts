/**
 * Auto-update results for finished races and compare with predictions.
 * 1. Find races that should have results (past off_time / past meeting_date)
 * 2. Check if results exist in DB already
 * 3. Fetch results from TheRacingAPI
 * 4. Compare predictions with actual results
 * 5. Populate prediction_results and race_analysis tables
 */

import pino from 'pino'
import { query, upsert, batchUpsert, pool } from '../lib/database.js'
import { traClient } from '../lib/traClient.js'
import { ingestResults } from './ingestResults.js'
import { parseClassNumeric } from '../ai/features/classWeightFeatures.js'

const log = pino({ name: 'auto-update' })

interface PredictionRow {
  race_id: string
  horse_id: string
  predicted_win_prob: number
  predicted_rank: number
  market_odds: number
  edge_pct: number
  verdict: string
}

interface ResultRow {
  race_id: string
  horse_id: string
  position: number | null
  sp_decimal: number | null
  beaten_lengths: number | null
}

/**
 * Find AU races needing results: meeting_date <= today AND no results in DB.
 * Returns race details needed to fetch from API.
 */
async function findRacesNeedingResults(): Promise<Array<{
  race_id: string; meeting_id: string; race_number: number; meeting_date: string
}>> {
  return query<{
    race_id: string; meeting_id: string; race_number: number; meeting_date: string
  }>(`
    SELECT r.race_id, r.meeting_id, r.race_number, m.meeting_date::text as meeting_date
    FROM races r
    JOIN meetings m ON r.meeting_id = m.meeting_id
    WHERE m.meeting_date <= CURRENT_DATE
      AND m.meeting_date >= CURRENT_DATE - INTERVAL '7 days'
      AND NOT EXISTS (SELECT 1 FROM results res WHERE res.race_id = r.race_id)
    ORDER BY m.meeting_date DESC, r.race_number
  `)
}

function parseSP(sp: string | number | undefined | null): number | null {
  if (!sp) return null
  const s = String(sp)
  const fracMatch = s.match(/^(\d+)\/(\d+)$/)
  if (fracMatch) return parseFloat(fracMatch[1]) / parseFloat(fracMatch[2]) + 1
  const dec = parseFloat(s)
  return isNaN(dec) ? null : dec
}

function parseBeatenLengths(btn: string | undefined | null): number | null {
  if (!btn) return null
  const specials: Record<string, number> = { nk: 0.3, hd: 0.2, shd: 0.1, snk: 0.2, dht: 0, nse: 0.05 }
  const lower = btn.toLowerCase().trim()
  if (lower in specials) return specials[lower]
  const n = parseFloat(lower)
  return isNaN(n) ? null : n
}

/**
 * Fetch results for AU races using the /v1/australia/meets/{id}/races/{num} endpoint.
 * Also falls back to /v1/results for non-AU races.
 */
export async function fetchMissingResults(): Promise<number> {
  const races = await findRacesNeedingResults()
  if (races.length === 0) {
    log.info('No races needing results')
    return 0
  }

  log.info({ count: races.length }, 'Races needing results')
  let total = 0

  // Group by meeting for efficiency
  const byMeeting = new Map<string, typeof races>()
  for (const r of races) {
    const list = byMeeting.get(r.meeting_id) || []
    list.push(r)
    byMeeting.set(r.meeting_id, list)
  }

  // Try TRA /v1/results for non-AU dates first
  const dates = [...new Set(races.map(r => r.meeting_date))]
  for (const dateStr of dates) {
    try {
      const count = await ingestResults(dateStr, dateStr)
      total += count
      if (count > 0) log.info({ date: dateStr, results: count }, 'Major results fetched')
    } catch (err) {
      log.warn({ date: dateStr, err }, 'Failed to fetch major results')
    }
  }

  // Now fetch AU-specific results via meets endpoint
  for (const [meetingId, meetingRaces] of byMeeting) {
    if (!meetingId.startsWith('met_aus_')) continue

    for (const race of meetingRaces) {
      // Skip if results were already fetched by ingestResults above
      const existing = await query('SELECT 1 FROM results WHERE race_id = $1 LIMIT 1', [race.race_id])
      if (existing.length > 0) continue

      try {
        const raceData = await traClient.getAustraliaRace(meetingId, race.race_number) as any
        if (!raceData?.runners || raceData.race_status !== 'Results') continue

        for (const runner of raceData.runners) {
          if (!runner.horse_id || runner.scratched) continue
          const position = runner.position ? parseInt(runner.position) : null
          if (position === null) continue

          await upsert(
            'results',
            ['race_id', 'horse_id', 'position', 'sp_decimal', 'beaten_lengths', 'comment'],
            [
              race.race_id,
              runner.horse_id,
              position,
              parseSP(runner.sp),
              parseBeatenLengths(runner.btn_distance ?? runner.margin),
              runner.comment ?? null,
            ],
            ['race_id', 'horse_id']
          )
        }

        // Also update runner positions in runners table
        for (const runner of raceData.runners) {
          if (!runner.horse_id) continue
          await query(
            `UPDATE runners SET position = $3, sp_decimal = COALESCE(sp_decimal, $4), margin = $5
             WHERE race_id = $1 AND horse_id = $2`,
            [race.race_id, runner.horse_id, runner.position ? parseInt(runner.position) : null, parseSP(runner.sp), runner.margin ?? null]
          )
        }

        total++
        log.info({ raceId: race.race_id, raceNum: race.race_number, runners: raceData.runners.length }, 'AU race results fetched')
      } catch (err) {
        log.warn({ raceId: race.race_id, err }, 'Failed to fetch AU race results')
      }
    }
  }

  return total
}

/**
 * Compare stored predictions with actual results.
 * Populates prediction_results and race_analysis tables.
 */
export async function compareWithPredictions(modelVersion: string): Promise<{
  racesAnalyzed: number
  predictionsCompared: number
}> {
  // Find races that have both results AND stored predictions, but no analysis yet
  const races = await query<{ race_id: string }>(`
    SELECT DISTINCT pr.race_id
    FROM prediction_results pr
    WHERE pr.actual_position IS NULL
      AND EXISTS (SELECT 1 FROM results res WHERE res.race_id = pr.race_id)
  `)

  if (races.length === 0) {
    log.info('No races to compare')
    return { racesAnalyzed: 0, predictionsCompared: 0 }
  }

  let racesAnalyzed = 0
  let predictionsCompared = 0

  for (const { race_id } of races) {
    // Get predictions for this race
    const predictions = await query<PredictionRow>(`
      SELECT race_id, horse_id, predicted_win_prob::float, predicted_rank,
             market_odds_at_prediction::float as market_odds, edge_pct::float, verdict
      FROM prediction_results
      WHERE race_id = $1 AND model_version = $2
    `, [race_id, modelVersion])

    if (predictions.length === 0) continue

    // Get results
    const results = await query<ResultRow>(`
      SELECT race_id, horse_id, position, sp_decimal::float, beaten_lengths::float
      FROM results WHERE race_id = $1
    `, [race_id])
    const resultMap = new Map(results.map(r => [r.horse_id, r]))

    // Get change flags for each horse
    for (const pred of predictions) {
      const result = resultMap.get(pred.horse_id)
      if (!result) continue

      const isWinner = result.position === 1
      const isValueBet = pred.verdict === 'strong-value' || pred.verdict === 'value'
      const profitLoss = isValueBet
        ? (isWinner ? (pred.market_odds - 1) : -1)
        : 0

      // Get change flags from form history
      const changes = await getChangeFlags(pred.horse_id, race_id)

      await query(`
        UPDATE prediction_results SET
          actual_position = $3,
          actual_sp = $4,
          beaten_lengths = $5,
          prediction_correct = $6,
          value_bet_correct = $7,
          profit_loss = $8,
          jockey_changed = $9,
          weight_changed_kg = $10,
          distance_changed_m = $11,
          class_changed = $12,
          going_changed = $13
        WHERE race_id = $1 AND horse_id = $2 AND model_version = $14
      `, [
        race_id, pred.horse_id,
        result.position,
        result.sp_decimal,
        result.beaten_lengths,
        pred.predicted_rank === 1 && isWinner,  // top pick correct
        isValueBet ? isWinner : null,
        profitLoss,
        changes.jockeyChanged,
        changes.weightChangedKg,
        changes.distanceChangedM,
        changes.classChanged,
        changes.goingChanged,
        modelVersion,
      ])
      predictionsCompared++
    }

    // Compute race-level analysis
    await computeRaceAnalysis(race_id, modelVersion, predictions, resultMap)
    racesAnalyzed++
  }

  log.info({ racesAnalyzed, predictionsCompared }, 'Prediction comparison complete')
  return { racesAnalyzed, predictionsCompared }
}

async function getChangeFlags(horseId: string, raceId: string): Promise<{
  jockeyChanged: boolean | null
  weightChangedKg: number | null
  distanceChangedM: number | null
  classChanged: boolean | null
  goingChanged: boolean | null
}> {
  const defaults = {
    jockeyChanged: null, weightChangedKg: null,
    distanceChangedM: null, classChanged: null, goingChanged: null,
  }

  try {
    // Current race info
    const race = await query<{
      distance_m: number | null; going: string | null; class: string | null;
      meeting_date: string
    }>(`
      SELECT r.distance_m, r.going, r.class, m.meeting_date::text
      FROM races r JOIN meetings m ON r.meeting_id = m.meeting_id
      WHERE r.race_id = $1
    `, [raceId])
    if (!race[0]) return defaults

    const runner = await query<{ jockey_id: string | null; weight_lbs: number | null }>(`
      SELECT jockey_id, weight_lbs FROM runners WHERE race_id = $1 AND horse_id = $2
    `, [raceId, horseId])

    // Last race from form history
    const lastForm = await query<{
      jockey_id: string | null; weight_carried: number | null;
      distance_m: number | null; going: string | null; class: string | null
    }>(`
      SELECT jockey_id, weight_carried, distance_m, going, class
      FROM horse_form_history
      WHERE horse_id = $1 AND race_date < $2::date
      ORDER BY race_date DESC LIMIT 1
    `, [horseId, race[0].meeting_date])

    if (!lastForm[0]) return defaults

    const last = lastForm[0]
    const curr = runner[0]

    return {
      jockeyChanged: curr?.jockey_id && last.jockey_id
        ? curr.jockey_id !== last.jockey_id : null,
      weightChangedKg: curr?.weight_lbs && last.weight_carried
        ? Number(curr.weight_lbs) - Number(last.weight_carried) : null,
      distanceChangedM: race[0].distance_m && last.distance_m
        ? race[0].distance_m - last.distance_m : null,
      classChanged: race[0].class && last.class
        ? parseClassNumeric(race[0].class) !== parseClassNumeric(last.class) : null,
      goingChanged: race[0].going && last.going
        ? race[0].going.toLowerCase() !== last.going.toLowerCase() : null,
    }
  } catch {
    return defaults
  }
}

async function computeRaceAnalysis(
  raceId: string,
  modelVersion: string,
  predictions: PredictionRow[],
  resultMap: Map<string, ResultRow>
) {
  const topPick = predictions.reduce((best, p) =>
    p.predicted_rank < best.predicted_rank ? p : best
  )
  const topResult = resultMap.get(topPick.horse_id)

  const valueBets = predictions.filter(p =>
    p.verdict === 'strong-value' || p.verdict === 'value'
  )
  const valueBetResults = valueBets.map(p => ({
    pred: p,
    result: resultMap.get(p.horse_id),
  }))

  const valueBetsWon = valueBetResults.filter(v => v.result?.position === 1).length
  const totalStaked = valueBets.length  // $1 per value bet
  const totalReturn = valueBetResults.reduce((sum, v) => {
    if (v.result?.position === 1) return sum + v.pred.market_odds
    return sum
  }, 0)

  // Pace scenario from sectionals
  const sectionals = await query<{
    horse_id: string; speed_800m: number | null; speed_finish: number | null
  }>(`
    SELECT horse_id, speed_800m::float, speed_finish::float
    FROM sectional_times WHERE race_id = $1
  `, [raceId])

  let paceScenario: string | null = null
  let leader800m: number | null = null
  let winnerClosing: number | null = null

  if (sectionals.length > 0) {
    const speeds800 = sectionals.filter(s => s.speed_800m !== null)
    if (speeds800.length > 0) {
      leader800m = Math.max(...speeds800.map(s => s.speed_800m!))
      const avg800 = speeds800.reduce((s, v) => s + v.speed_800m!, 0) / speeds800.length
      // Fast pace = leader > 105% of average
      paceScenario = leader800m > avg800 * 1.05 ? 'fast' : leader800m < avg800 * 0.95 ? 'slow' : 'even'
    }

    const winnerId = [...resultMap.entries()].find(([, r]) => r.position === 1)?.[0]
    if (winnerId) {
      const winnerSec = sectionals.find(s => s.horse_id === winnerId)
      winnerClosing = winnerSec?.speed_finish ?? null
    }
  }

  await upsert(
    'race_analysis',
    [
      'race_id', 'model_version', 'top_pick_position', 'top_pick_won',
      'any_value_bet_won', 'value_bets_count', 'value_bets_won',
      'total_staked', 'total_return', 'race_pnl',
      'pace_scenario', 'leader_800m_speed', 'winner_closing_speed',
    ],
    [
      raceId, modelVersion,
      topResult?.position ?? null,
      topResult?.position === 1,
      valueBetsWon > 0,
      valueBets.length,
      valueBetsWon,
      totalStaked,
      totalReturn,
      totalReturn - totalStaked,
      paceScenario,
      leader800m,
      winnerClosing,
    ],
    ['race_id'],
  )
}

/**
 * Store predictions for later comparison with results.
 */
export async function storePredictions(
  raceId: string,
  modelVersion: string,
  predictions: Array<{
    horseId: string
    rank: number
    winProb: number
    marketOdds: number
    edgePct: number
    verdict: string
  }>
): Promise<void> {
  const cols = [
    'race_id', 'horse_id', 'model_version',
    'predicted_win_prob', 'predicted_rank',
    'market_odds_at_prediction', 'edge_pct', 'verdict',
  ]
  const rows = predictions.map(p => [
    raceId, p.horseId, modelVersion,
    p.winProb, p.rank, p.marketOdds, p.edgePct, p.verdict,
  ])

  await batchUpsert('prediction_results', cols, rows, ['race_id', 'horse_id', 'model_version'])
}

// CLI entry — only runs when executed directly
const isDirectExecution = process.argv[1]?.includes('autoUpdateResults')
if (isDirectExecution) main()

async function main() {
  const modelVersion = process.argv[2] || 'v3'
  log.info({ modelVersion }, 'Running auto-update')

  try {
    // Step 1: Fetch missing results
    const resultCount = await fetchMissingResults()
    console.log(`Results fetched: ${resultCount}`)

    // Step 2: Compare predictions with results
    const comparison = await compareWithPredictions(modelVersion)
    console.log(`Races analyzed: ${comparison.racesAnalyzed}`)
    console.log(`Predictions compared: ${comparison.predictionsCompared}`)
  } catch (err) {
    log.error(err, 'Auto-update failed')
    process.exit(1)
  } finally {
    await pool.end()
  }
}
