import { format, subMonths, addDays } from 'date-fns'
import { ingestMeetings } from '../etl/ingestMeetings.js'
import { ingestAllRunners } from '../etl/ingestRunners.js'
import { ingestResults } from '../etl/ingestResults.js'
import { pool, execute } from '../lib/database.js'
import pino from 'pino'

const log = pino({ name: 'backfill' })

async function backfill() {
  const monthsBack = parseInt(process.argv[2] ?? '12')
  const stepDays = 14 // 2-week chunks (API max range = 365 days)

  const endDate = new Date()
  const startDate = subMonths(endDate, monthsBack)

  log.info({
    from: format(startDate, 'yyyy-MM-dd'),
    to: format(endDate, 'yyyy-MM-dd'),
    monthsBack,
  }, 'Starting backfill')

  // Step 1: Ingest historical results (this is the primary historical data source)
  // /v1/results returns completed races with runners, positions, times, etc.
  // It also creates venues, meetings, races, horses, jockeys, trainers
  log.info('=== Step 1: Ingesting Historical Results ===')
  let cursor = new Date(startDate)
  let totalResults = 0

  while (cursor < endDate) {
    const chunkEnd = new Date(Math.min(addDays(cursor, stepDays - 1).getTime(), endDate.getTime()))
    const from = format(cursor, 'yyyy-MM-dd')
    const to = format(chunkEnd, 'yyyy-MM-dd')

    try {
      const results = await ingestResults(from, to)
      totalResults += results
      log.info({ from, to, results, totalResults }, 'Chunk complete')
    } catch (err) {
      log.error({ from, to, err }, 'Chunk failed, continuing...')
    }

    cursor = addDays(chunkEnd, 1)
  }

  log.info({ totalResults }, 'Step 1 complete: Historical results ingested')

  // Step 2: Ingest today's meetings + runners (for live racecards / upcoming races)
  log.info('=== Step 2: Ingesting Today\'s Meetings ===')
  try {
    const todayRaces = await ingestMeetings()
    log.info({ todayRaces }, 'Today\'s meetings ingested')

    // Fetch runners for today's races
    log.info('=== Step 3: Ingesting Runners for Today ===')
    await ingestAllRunners()
  } catch (err) {
    log.error({ err }, 'Today\'s meetings/runners failed')
  }

  // Step 4: Refresh materialized views
  log.info('=== Step 4: Refreshing Materialized Views ===')
  const views = [
    'mv_trainer_stats', 'mv_jockey_stats', 'mv_combo_stats',
    'mv_trainer_spell_stats', 'mv_track_bias', 'mv_barrier_stats',
  ]
  for (const view of views) {
    try {
      await execute(`REFRESH MATERIALIZED VIEW ${view}`)
      log.info({ view }, 'View refreshed')
    } catch (err) {
      log.warn({ view, err }, 'View refresh failed (may need data first)')
    }
  }

  // Summary
  const counts = await pool.query(`
    SELECT
      (SELECT COUNT(*) FROM venues) AS venues,
      (SELECT COUNT(*) FROM meetings) AS meetings,
      (SELECT COUNT(*) FROM races) AS races,
      (SELECT COUNT(*) FROM runners) AS runners,
      (SELECT COUNT(*) FROM horses) AS horses,
      (SELECT COUNT(*) FROM results) AS results,
      (SELECT COUNT(*) FROM jockeys) AS jockeys,
      (SELECT COUNT(*) FROM trainers) AS trainers
  `)

  log.info(counts.rows[0], 'Backfill complete - database summary')
  await pool.end()
}

backfill().catch((e) => {
  log.error(e, 'Backfill failed')
  process.exit(1)
})
