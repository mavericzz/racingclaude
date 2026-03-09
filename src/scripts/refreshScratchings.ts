/**
 * Refresh Scratchings
 *
 * Polls TheRacingAPI for today's + tomorrow's races, detects newly scratched
 * horses, updates the database, and logs the impact on field composition.
 *
 * When a horse is scratched:
 * - The `runners.scratched` flag is set to TRUE
 * - All subsequent prediction requests automatically exclude it (server filters)
 * - Field-relative features (speed_vs_field_avg, field_strength_rank) recalculate
 * - Market odds shift — pollOdds captures the new prices
 *
 * Usage: npx tsx src/scripts/refreshScratchings.ts [--once]
 */

import { traClient } from '../lib/traClient.js'
import { query, pool } from '../lib/database.js'
import pino from 'pino'

const log = pino({ name: 'refresh-scratchings' })

interface RaceToCheck {
  race_id: string
  meeting_id: string
  race_number: number
  venue_name: string
  off_time: string
  field_size: number
}

interface ScratchEvent {
  raceId: string
  venue: string
  raceNumber: number
  horseName: string
  horseId: string
  previousFieldSize: number
  newFieldSize: number
}

async function getRacesToCheck(): Promise<RaceToCheck[]> {
  return query<RaceToCheck>(`
    SELECT
      r.race_id, r.meeting_id, r.race_number,
      COALESCE(v.name, m.venue_id, 'Unknown') AS venue_name,
      r.off_time,
      r.field_size
    FROM races r
    JOIN meetings m ON m.meeting_id = r.meeting_id
    LEFT JOIN venues v ON v.venue_id = m.venue_id
    WHERE m.meeting_date >= CURRENT_DATE
      AND m.meeting_date <= CURRENT_DATE + 1
      AND r.off_time > NOW() - INTERVAL '10 minutes'
      AND NOT EXISTS (SELECT 1 FROM results res WHERE res.race_id = r.race_id)
    ORDER BY r.off_time ASC
  `)
}

async function checkScratchingsForRace(race: RaceToCheck): Promise<ScratchEvent[]> {
  const events: ScratchEvent[] = []

  try {
    const raceData = await traClient.getAustraliaRace(race.meeting_id, race.race_number)
    const apiRunners = raceData?.runners ?? []
    if (apiRunners.length === 0) return events

    // Get current DB state of runners for this race
    const dbRunners = await query<{ horse_id: string; horse: string; scratched: boolean }>(
      `SELECT horse_id, horse, scratched FROM runners WHERE race_id = $1`,
      [race.race_id]
    )
    const dbMap = new Map(dbRunners.map(r => [r.horse_id, r]))

    let newScratchCount = 0

    for (const apiRunner of apiRunners) {
      if (!apiRunner.horse_id) continue
      const dbRunner = dbMap.get(apiRunner.horse_id)
      if (!dbRunner) continue

      // Detect NEW scratchings (was not scratched, now is)
      if (apiRunner.scratched && !dbRunner.scratched) {
        await pool.query(
          `UPDATE runners SET scratched = TRUE WHERE race_id = $1 AND horse_id = $2`,
          [race.race_id, apiRunner.horse_id]
        )
        newScratchCount++

        events.push({
          raceId: race.race_id,
          venue: race.venue_name,
          raceNumber: race.race_number,
          horseName: dbRunner.horse,
          horseId: apiRunner.horse_id,
          previousFieldSize: race.field_size,
          newFieldSize: 0, // computed below
        })
      }

      // Also handle un-scratchings (rare but possible if API corrects)
      if (!apiRunner.scratched && dbRunner.scratched) {
        await pool.query(
          `UPDATE runners SET scratched = FALSE WHERE race_id = $1 AND horse_id = $2`,
          [race.race_id, apiRunner.horse_id]
        )
        log.info({
          venue: race.venue_name,
          race: `R${race.race_number}`,
          horse: dbRunner.horse,
        }, 'UN-SCRATCHED (reinstated)')
      }
    }

    // Update field_size on the race if any scratching changed
    if (newScratchCount > 0) {
      const { rows } = await pool.query(
        `SELECT COUNT(*) AS active FROM runners WHERE race_id = $1 AND scratched = FALSE`,
        [race.race_id]
      )
      const newFieldSize = Number(rows[0].active)

      await pool.query(
        `UPDATE races SET field_size = $1 WHERE race_id = $2`,
        [newFieldSize, race.race_id]
      )

      // Update events with the new field size
      for (const ev of events) {
        ev.newFieldSize = newFieldSize
      }
    }

    return events
  } catch (err: any) {
    if (!err.message?.includes('404')) {
      log.error({ raceId: race.race_id, err: err.message }, 'Failed to check scratchings')
    }
    return events
  }
}

async function logScratchImpact(events: ScratchEvent[]): Promise<void> {
  for (const ev of events) {
    // Fetch the scratched horse's last form to show what the field loses
    const form = await query<{ avg_speed: number; last_finish: number }>(
      `SELECT
         AVG(sf.speed_figure) AS avg_speed,
         (SELECT fh.finish_position FROM horse_form_history fh
          WHERE fh.horse_id = (SELECT canonical_id FROM horse_id_map WHERE aus_id = $1)
             OR fh.horse_id = $1
          ORDER BY fh.race_date DESC LIMIT 1) AS last_finish
       FROM speed_figures sf
       WHERE sf.horse_id = (SELECT canonical_id FROM horse_id_map WHERE aus_id = $1)
          OR sf.horse_id = $1`,
      [ev.horseId]
    )

    const f = form[0]
    const speedStr = f?.avg_speed ? `avg speed ${Number(f.avg_speed).toFixed(1)}` : 'no speed data'
    const finishStr = f?.last_finish ? `last finish pos ${f.last_finish}` : ''

    log.warn({
      signal: 'SCRATCHING',
      venue: ev.venue,
      race: `R${ev.raceNumber}`,
      horse: ev.horseName,
      fieldChange: `${ev.previousFieldSize} → ${ev.newFieldSize}`,
      form: [speedStr, finishStr].filter(Boolean).join(', '),
    }, `SCRATCHED: ${ev.horseName}`)
  }

  // Log to ingestion_log
  if (events.length > 0) {
    try {
      await pool.query(
        `INSERT INTO ingestion_log (source, records_processed, status, details)
         VALUES ('scratchings', $1, 'success', $2)`,
        [events.length, JSON.stringify(events)]
      )
    } catch { /* table may not exist */ }
  }
}

async function main() {
  const once = process.argv.includes('--once')

  log.info({ mode: once ? 'single' : 'continuous' }, 'Scratching monitor starting')

  const run = async () => {
    const races = await getRacesToCheck()
    if (races.length === 0) {
      log.info('No upcoming races to check')
      return
    }

    log.info({ races: races.length }, 'Checking for scratchings')

    const allEvents: ScratchEvent[] = []
    for (const race of races) {
      const events = await checkScratchingsForRace(race)
      allEvents.push(...events)
    }

    if (allEvents.length > 0) {
      await logScratchImpact(allEvents)
      log.info({ scratchings: allEvents.length }, 'New scratchings detected')
    } else {
      log.info('No new scratchings')
    }
  }

  await run()

  if (!once) {
    // Re-check every 10 minutes
    setInterval(async () => {
      try { await run() } catch (err) { log.error({ err }, 'Scratch check failed') }
    }, 10 * 60 * 1000)
  } else {
    await pool.end()
  }
}

main().catch(err => {
  log.error(err)
  process.exit(1)
})
