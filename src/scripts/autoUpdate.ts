/**
 * Daily pipeline orchestrator.
 * Run after scraper finishes or on a schedule.
 *
 * Steps:
 * 1. Import scraper data for today
 * 2. Fetch results for finished races
 * 3. Compare predictions with results
 * 4. Log summary
 *
 * Usage: npx tsx src/scripts/autoUpdate.ts [date]
 */

import pino from 'pino'
import { pool, query } from '../lib/database.js'
import { ingestScraperData } from '../etl/ingestScraperData.js'
import { fetchMissingResults, compareWithPredictions } from '../etl/autoUpdateResults.js'

const log = pino({ name: 'auto-update' })

async function main() {
  const dateStr = process.argv[2] || new Date().toISOString().split('T')[0]
  const startTime = Date.now()

  console.log(`\n=== RacingClaude Auto-Update ===`)
  console.log(`Date: ${dateStr}`)
  console.log(`Started: ${new Date().toISOString()}\n`)

  try {
    // Step 1: Import scraper data
    console.log('Step 1: Importing scraper data...')
    try {
      const scraperStats = await ingestScraperData(dateStr)
      console.log(`  Venues matched: ${scraperStats.venuesMatched}/${scraperStats.venues}`)
      console.log(`  Races processed: ${scraperStats.racesProcessed}`)
      console.log(`  Sectional rows: ${scraperStats.sectionalRows}`)
      console.log(`  Form data rows: ${scraperStats.formRows}`)
      console.log(`  Horse match rate: ${scraperStats.horsesMatched}/${scraperStats.horsesMatched + scraperStats.horsesUnmatched}`)
    } catch (err: any) {
      if (err?.message?.includes('not found')) {
        console.log('  No scraper output for this date (skipping)')
      } else {
        log.error(err, 'Scraper import failed')
        console.log(`  Error: ${err.message}`)
      }
    }

    // Step 2: Fetch missing results
    console.log('\nStep 2: Fetching missing results...')
    const resultCount = await fetchMissingResults()
    console.log(`  Results fetched: ${resultCount}`)

    // Step 3: Compare predictions with results
    console.log('\nStep 3: Comparing predictions with results...')
    const comparison = await compareWithPredictions('v3')
    console.log(`  Races analyzed: ${comparison.racesAnalyzed}`)
    console.log(`  Predictions compared: ${comparison.predictionsCompared}`)

    // Summary
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)
    console.log(`\n=== Complete in ${elapsed}s ===`)

    // Log to ingestion_log if table exists
    try {
      await query(`
        INSERT INTO ingestion_log (source, records_processed, status, details)
        VALUES ('auto-update', $1, 'success', $2)
      `, [
        resultCount + comparison.predictionsCompared,
        JSON.stringify({ date: dateStr, resultCount, ...comparison }),
      ])
    } catch {
      // ingestion_log table may not exist
    }
  } catch (err) {
    log.error(err, 'Auto-update failed')
    console.error('Auto-update failed:', err)
    process.exit(1)
  } finally {
    await pool.end()
  }
}

main()
