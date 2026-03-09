/**
 * Odds Polling Script
 *
 * Periodically fetches latest odds from TheRacingAPI for upcoming races.
 * Stores each snapshot in odds_snapshots to track movement over time.
 *
 * Captures plunges, drifts, and late money signals that are critical for predictions.
 *
 * Usage: npx tsx src/scripts/pollOdds.ts [--once] [--interval 15]
 *   --once:     Run once and exit (don't loop)
 *   --interval: Minutes between polls (default: 15, min: 5)
 */

import { traClient } from '../lib/traClient.js'
import { query, pool } from '../lib/database.js'
import pino from 'pino'

const log = pino({ name: 'poll-odds' })

function toNum(val: string | number | undefined | null): number | null {
  if (val === undefined || val === null || val === '') return null
  const n = typeof val === 'string' ? parseFloat(val) : val
  return isNaN(n) ? null : n
}

interface UpcomingRace {
  race_id: string
  meeting_id: string
  race_number: number
  off_time: string
  venue_name: string
  mins_to_race: number
}

async function getUpcomingRaces(): Promise<UpcomingRace[]> {
  // Get races within the next 12 hours that haven't had results yet
  const rows = await query<UpcomingRace>(`
    SELECT
      r.race_id, r.meeting_id, r.race_number, r.off_time,
      COALESCE(v.name, m.venue_id, 'Unknown') AS venue_name,
      EXTRACT(EPOCH FROM (r.off_time - NOW())) / 60 AS mins_to_race
    FROM races r
    JOIN meetings m ON m.meeting_id = r.meeting_id
    LEFT JOIN venues v ON v.venue_id = m.venue_id
    WHERE r.off_time > NOW() - INTERVAL '5 minutes'
      AND r.off_time < NOW() + INTERVAL '12 hours'
      AND NOT EXISTS (
        SELECT 1 FROM results res WHERE res.race_id = r.race_id
      )
    ORDER BY r.off_time ASC
  `)
  return rows
}

async function pollOddsForRace(race: UpcomingRace): Promise<number> {
  try {
    const raceData = await traClient.getAustraliaRace(race.meeting_id, race.race_number)
    const runners = raceData?.runners ?? []
    if (runners.length === 0) return 0

    const now = new Date().toISOString()
    let count = 0

    for (const r of runners) {
      if (!r.horse_id || r.scratched) continue
      if (!r.odds || !Array.isArray(r.odds)) continue

      for (const o of r.odds) {
        if (!o.bookmaker) continue
        const winOdds = toNum(o.win_odds)
        const placeOdds = toNum(o.place_odds)
        if (winOdds === null && placeOdds === null) continue

        await pool.query(`
          INSERT INTO odds_snapshots (race_id, horse_id, bookmaker, win_odds, place_odds, observed_at)
          VALUES ($1, $2, $3, $4, $5, $6)
          ON CONFLICT (race_id, horse_id, bookmaker, observed_at) DO NOTHING
        `, [race.race_id, r.horse_id, o.bookmaker, winOdds, placeOdds, now])
        count++
      }
    }

    return count
  } catch (err: any) {
    log.error({ raceId: race.race_id, err: err.message }, 'Failed to poll odds')
    return 0
  }
}

async function runPoll(): Promise<void> {
  const races = await getUpcomingRaces()
  if (races.length === 0) {
    log.info('No upcoming races found')
    return
  }

  log.info({ races: races.length }, 'Polling odds for upcoming races')

  // Prioritize races closer to off time (poll those more aggressively)
  const closestFirst = races.sort((a, b) => a.mins_to_race - b.mins_to_race)

  let totalSnapshots = 0
  let racesPolled = 0

  for (const race of closestFirst) {
    const count = await pollOddsForRace(race)
    totalSnapshots += count
    racesPolled++

    const mins = Number(race.mins_to_race)
    const timeLabel = mins <= 0
      ? 'STARTED'
      : mins < 10
        ? `${mins.toFixed(0)}m (CLOSE)`
        : `${mins.toFixed(0)}m`

    if (count > 0) {
      log.info({
        venue: race.venue_name,
        race: `R${race.race_number}`,
        time: timeLabel,
        odds: count,
      }, 'Odds captured')
    }
  }

  // Log movement detection
  await detectMovements()

  log.info({ racesPolled, totalSnapshots }, 'Poll complete')
}

async function detectMovements(): Promise<void> {
  // Find significant odds movements in races starting within 30 minutes
  const movements = await query<{
    venue_name: string
    race_number: number
    horse_name: string
    open_odds: number
    latest_odds: number
    pct_change: number
    snapshots: number
  }>(`
    WITH race_horses AS (
      SELECT DISTINCT os.race_id, os.horse_id,
        FIRST_VALUE(os.win_odds) OVER (PARTITION BY os.race_id, os.horse_id ORDER BY os.observed_at ASC) AS open_odds,
        FIRST_VALUE(os.win_odds) OVER (PARTITION BY os.race_id, os.horse_id ORDER BY os.observed_at DESC) AS latest_odds,
        COUNT(*) OVER (PARTITION BY os.race_id, os.horse_id) AS snapshots
      FROM odds_snapshots os
      JOIN races r ON r.race_id = os.race_id
      WHERE r.off_time > NOW() - INTERVAL '5 minutes'
        AND r.off_time < NOW() + INTERVAL '60 minutes'
        AND os.win_odds IS NOT NULL
    )
    SELECT DISTINCT
      COALESCE(v.name, m.venue_id, 'Unknown') AS venue_name, rc.race_number,
      ru.horse AS horse_name,
      rh.open_odds, rh.latest_odds,
      ROUND(((rh.open_odds - rh.latest_odds) / rh.open_odds * 100)::numeric, 1) AS pct_change,
      rh.snapshots
    FROM race_horses rh
    JOIN races rc ON rc.race_id = rh.race_id
    JOIN meetings m ON m.meeting_id = rc.meeting_id
    LEFT JOIN venues v ON v.venue_id = m.venue_id
    JOIN runners ru ON ru.race_id = rh.race_id AND ru.horse_id = rh.horse_id
    WHERE rh.snapshots > 2
      AND rh.open_odds > 1
      AND ABS(rh.open_odds - rh.latest_odds) / rh.open_odds > 0.15
    ORDER BY pct_change DESC
    LIMIT 10
  `)

  for (const m of movements) {
    const direction = m.pct_change > 0 ? 'PLUNGE' : 'DRIFT'
    log.warn({
      signal: direction,
      venue: m.venue_name,
      race: `R${m.race_number}`,
      horse: m.horse_name,
      from: `$${m.open_odds}`,
      to: `$${m.latest_odds}`,
      change: `${m.pct_change > 0 ? '+' : ''}${m.pct_change}%`,
      snapshots: m.snapshots,
    }, `${direction} detected`)
  }
}

async function main() {
  const args = process.argv.slice(2)
  const once = args.includes('--once')
  const intervalIdx = args.indexOf('--interval')
  const intervalMins = intervalIdx >= 0 ? Math.max(5, parseInt(args[intervalIdx + 1]) || 15) : 15

  log.info({ mode: once ? 'single' : 'continuous', intervalMins }, 'Odds poller starting')

  // Run immediately
  await runPoll()

  if (once) {
    await pool.end()
    return
  }

  // Continuous mode: poll at interval
  const intervalMs = intervalMins * 60 * 1000
  log.info(`Next poll in ${intervalMins} minutes...`)

  setInterval(async () => {
    try {
      await runPoll()
      log.info(`Next poll in ${intervalMins} minutes...`)
    } catch (err) {
      log.error({ err }, 'Poll cycle failed')
    }
  }, intervalMs)
}

main().catch(err => {
  log.error(err)
  process.exit(1)
})
