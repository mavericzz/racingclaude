/**
 * Daily pipeline: ingest 4 days of future meetings/runners + scraper data + results.
 *
 * Usage: npx tsx src/scripts/dailyPipeline.ts
 */
import pino from 'pino'
import { pool, query } from '../lib/database.js'
import { ingestMeetings } from '../etl/ingestMeetings.js'
import { ingestRunnersForRace } from '../etl/ingestRunners.js'
import { ingestScraperData } from '../etl/ingestScraperData.js'
import { fetchMissingResults, compareWithPredictions } from '../etl/autoUpdateResults.js'

const log = pino({ name: 'daily-pipeline' })
const FUTURE_DAYS = 4

function formatDate(d: Date): string {
  return d.toISOString().slice(0, 10)
}

async function main() {
  const startTime = Date.now()
  const today = new Date()
  const todayStr = formatDate(today)

  console.log(`\n=== RacingClaude Daily Pipeline ===`)
  console.log(`Date: ${todayStr}`)
  console.log(`Future days: ${FUTURE_DAYS}`)
  console.log(`Started: ${new Date().toISOString()}\n`)

  // Step 1: Ingest meetings for today + next 4 days
  console.log('Step 1: Ingesting meetings (today + 4 days)...')
  let totalRaces = 0
  for (let i = 0; i <= FUTURE_DAYS; i++) {
    const d = new Date(today)
    d.setDate(d.getDate() + i)
    const dateStr = formatDate(d)
    try {
      const races = await ingestMeetings(dateStr)
      totalRaces += races
      console.log(`  ${dateStr}: ${races} races`)
    } catch (err: any) {
      if (err.message?.includes('404') || err.message?.includes('No meets')) {
        console.log(`  ${dateStr}: no meetings`)
      } else {
        console.log(`  ${dateStr}: error - ${err.message}`)
      }
    }
  }
  console.log(`  Total races: ${totalRaces}`)

  // Step 2: Fetch runners for races missing them
  console.log('\nStep 2: Fetching runners for new races...')
  const racesNeedingRunners = await query<{ race_id: string; meeting_id: string; race_number: number }>(
    `SELECT r.race_id, r.meeting_id, r.race_number
     FROM races r
     JOIN meetings m ON r.meeting_id = m.meeting_id
     LEFT JOIN runners ru ON r.race_id = ru.race_id
     WHERE ru.id IS NULL
       AND m.meeting_date >= CURRENT_DATE
       AND m.meeting_date <= CURRENT_DATE + 4
       AND r.race_number IS NOT NULL
     ORDER BY m.meeting_date, r.off_time`
  )
  let totalRunners = 0
  console.log(`  Races needing runners: ${racesNeedingRunners.length}`)
  for (const race of racesNeedingRunners) {
    try {
      const count = await ingestRunnersForRace(race.meeting_id, race.race_number, race.race_id)
      totalRunners += count
    } catch {
      // API may not have runner data for future races yet
    }
  }
  console.log(`  Runners ingested: ${totalRunners}`)

  // Step 3: Import scraper data for today
  console.log('\nStep 3: Importing scraper data...')
  try {
    const scraperStats = await ingestScraperData(todayStr)
    console.log(`  Venues: ${scraperStats.venuesMatched}/${scraperStats.venues}`)
    console.log(`  Sectionals: ${scraperStats.sectionalRows}, Form: ${scraperStats.formRows}`)
    console.log(`  Horse match: ${scraperStats.horsesMatched}/${scraperStats.horsesMatched + scraperStats.horsesUnmatched}`)
  } catch (err: any) {
    if (err.message?.includes('not found')) {
      console.log('  No scraper output for today (skipping)')
    } else {
      console.log(`  Error: ${err.message}`)
    }
  }

  // Step 4: Fetch missing results for past races
  console.log('\nStep 4: Fetching missing results...')
  try {
    const resultCount = await fetchMissingResults()
    console.log(`  Results fetched: ${resultCount}`)
  } catch (err: any) {
    console.log(`  Error: ${err.message}`)
  }

  // Step 5: Compare predictions
  console.log('\nStep 5: Comparing predictions...')
  try {
    const comparison = await compareWithPredictions('v3')
    console.log(`  Races analyzed: ${comparison.racesAnalyzed}`)
    console.log(`  Predictions compared: ${comparison.predictionsCompared}`)
  } catch (err: any) {
    console.log(`  Error: ${err.message}`)
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)
  console.log(`\n=== Complete in ${elapsed}s ===`)

  // Log to ingestion_log
  try {
    await query(`
      INSERT INTO ingestion_log (source, records_processed, status, details)
      VALUES ('daily-pipeline', $1, 'success', $2)
    `, [totalRaces + totalRunners, JSON.stringify({ date: todayStr, totalRaces, totalRunners })])
  } catch {
    // ingestion_log may not exist
  }

  await pool.end()
}

main().catch((e) => {
  log.error(e, 'Daily pipeline failed')
  process.exit(1)
})
