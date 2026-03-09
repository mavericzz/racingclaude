/**
 * Ingest meetings + runners for the next N days (default 5).
 * Also fetches horse histories for any runners missing form data.
 *
 * Usage: npx tsx src/scripts/ingestFuture.ts [days=5] [--with-histories]
 */
import { ingestMeetings } from '../etl/ingestMeetings.js'
import { ingestRunnersForRace } from '../etl/ingestRunners.js'
import { query, pool } from '../lib/database.js'
import { traClient } from '../lib/traClient.js'
import pino from 'pino'

const log = pino({ name: 'ingest-future' })

function formatDate(d: Date): string {
  return d.toISOString().slice(0, 10)
}

async function main() {
  const args = process.argv.slice(2)
  const daysArg = args.find(a => !a.startsWith('--'))
  const days = parseInt(daysArg ?? '5')
  const withHistories = args.includes('--with-histories')

  log.info({ days, withHistories }, 'Ingesting future meetings')

  const today = new Date()
  let totalRaces = 0
  let totalRunners = 0

  // Step 1: Ingest meetings for each day
  for (let i = 0; i <= days; i++) {
    const d = new Date(today)
    d.setDate(d.getDate() + i)
    const dateStr = formatDate(d)

    try {
      const races = await ingestMeetings(dateStr)
      totalRaces += races
      log.info({ date: dateStr, races }, `Day ${i}: meetings ingested`)
    } catch (err: any) {
      if (err.message?.includes('404') || err.message?.includes('No meets')) {
        log.info({ date: dateStr }, `Day ${i}: no meetings found`)
      } else {
        log.warn({ date: dateStr, err: err.message }, `Day ${i}: failed`)
      }
    }
  }

  log.info({ totalRaces }, 'All meetings ingested')

  // Step 2: Fetch runners for races that don't have them yet
  log.info('Fetching runners for new races...')
  const racesNeedingRunners = await query<{ race_id: string; meeting_id: string; race_number: number }>(
    `SELECT r.race_id, r.meeting_id, r.race_number
     FROM races r
     JOIN meetings m ON r.meeting_id = m.meeting_id
     LEFT JOIN runners ru ON r.race_id = ru.race_id
     WHERE ru.id IS NULL
       AND m.meeting_date >= CURRENT_DATE
       AND r.race_number IS NOT NULL
     ORDER BY m.meeting_date, r.off_time`
  )

  log.info({ racesNeedingRunners: racesNeedingRunners.length }, 'Races needing runner data')

  for (let i = 0; i < racesNeedingRunners.length; i++) {
    const race = racesNeedingRunners[i]
    try {
      const count = await ingestRunnersForRace(race.meeting_id, race.race_number, race.race_id)
      totalRunners += count
      if ((i + 1) % 20 === 0 || i === racesNeedingRunners.length - 1) {
        log.info({ progress: `${i + 1}/${racesNeedingRunners.length}`, runners: totalRunners }, 'Runner progress')
      }
    } catch (err: any) {
      log.debug({ raceId: race.race_id, err: err.message }, 'Runner fetch failed')
    }
  }

  log.info({ totalRunners, totalRaces }, 'Future races + runners ingested')

  // Step 3: Summary
  const summary = await pool.query(`
    SELECT
      m.meeting_date,
      COUNT(DISTINCT m.meeting_id) AS meetings,
      COUNT(DISTINCT r.race_id) AS races,
      COUNT(DISTINCT ru.horse_id) AS runners
    FROM meetings m
    JOIN races r ON m.meeting_id = r.meeting_id
    LEFT JOIN runners ru ON r.race_id = ru.race_id AND ru.scratched = FALSE
    WHERE m.meeting_date >= CURRENT_DATE AND m.meeting_date <= CURRENT_DATE + CAST($1 AS INTEGER)
    GROUP BY m.meeting_date
    ORDER BY m.meeting_date
  `, [days])

  for (const row of summary.rows) {
    log.info({
      date: row.meeting_date,
      meetings: row.meetings,
      races: row.races,
      runners: row.runners,
    }, 'Day summary')
  }

  await pool.end()
}

main().catch((e) => {
  log.error(e, 'Future ingestion failed')
  process.exit(1)
})
