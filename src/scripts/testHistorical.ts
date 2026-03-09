import { traClient } from '../lib/traClient.js'

async function main() {
  // Test 1: Results endpoint with correct param names (start_date/end_date)
  console.log('=== Test 1: /v1/results with start_date/end_date (1 week ago) ===')
  try {
    const results = await traClient.getResults({ region: 'aus', start_date: '2026-02-28', end_date: '2026-03-06', limit: 5 })
    console.log('Results count:', results.results?.length)
    if (results.results?.length > 0) {
      for (const r of results.results) {
        console.log(`  ${r.date} | ${r.course} | ${r.race_name} | ${r.distance} | runners: ${r.runners?.length}`)
      }
      // Show first runner detail
      const firstRunner = results.results[0].runners?.[0]
      if (firstRunner) {
        console.log('\nFirst runner detail:', JSON.stringify({
          horse: firstRunner.horse, horse_id: firstRunner.horse_id,
          position: firstRunner.position, sp: firstRunner.sp,
          jockey: firstRunner.jockey, trainer: firstRunner.trainer,
          time: firstRunner.time, btn: firstRunner.btn,
        }, null, 2))
      }
    }
  } catch (e: any) {
    console.log('Error:', e.message)
  }

  // Test 2: Historical data from months ago
  console.log('\n=== Test 2: /v1/results from 6 months ago ===')
  try {
    const results = await traClient.getResults({ region: 'aus', start_date: '2025-09-01', end_date: '2025-09-07', limit: 3 })
    console.log('Results count:', results.results?.length)
    if (results.results?.length > 0) {
      for (const r of results.results) {
        console.log(`  ${r.date} | ${r.course} | ${r.race_name} | runners: ${r.runners?.length}`)
      }
    }
  } catch (e: any) {
    console.log('Error:', e.message)
  }

  // Test 3: Australia meets (today only, single date param)
  console.log('\n=== Test 3: /v1/australia/meets (today) ===')
  try {
    const meets = await traClient.getAustraliaMeets()
    const meetsList = Array.isArray(meets) ? meets : (meets as any)?.meets ?? []
    console.log('Meets count:', meetsList.length)
    for (const m of meetsList.slice(0, 3)) {
      console.log(`  ${m.date} | ${m.course} | races: ${m.races?.length}`)
    }
  } catch (e: any) {
    console.log('Error:', e.message)
  }
}

main().catch(e => { console.error(e); process.exit(1) })
