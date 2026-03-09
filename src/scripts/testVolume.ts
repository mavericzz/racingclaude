import { traClient } from '../lib/traClient.js'

async function main() {
  // Test with a full month to see total volume
  console.log('=== 1 month of AU results ===')
  let total = 0
  let skip = 0
  while (true) {
    const r = await traClient.getResults({ region: 'aus', start_date: '2026-02-01', end_date: '2026-02-28', limit: 100, skip })
    const races = r.results ?? []
    total += races.length
    if (races.length === 0 || races.length < 100) break
    skip += 100
  }
  console.log('Total AU races in Feb 2026:', total)

  // Try a different month
  console.log('\n=== 1 month older results ===')
  total = 0
  skip = 0
  while (true) {
    const r = await traClient.getResults({ region: 'aus', start_date: '2025-11-01', end_date: '2025-11-30', limit: 100, skip })
    const races = r.results ?? []
    total += races.length
    if (races.length === 0 || races.length < 100) break
    skip += 100
  }
  console.log('Total AU races in Nov 2025:', total)

  // Try without region filter to see what we get
  console.log('\n=== Without region filter (1 week) ===')
  const r3 = await traClient.getResults({ start_date: '2026-02-24', end_date: '2026-03-02', limit: 100 })
  const allRaces = r3.results ?? []
  console.log('Total races (no region):', allRaces.length)
  const regions = [...new Set(allRaces.map(x => x.region))]
  console.log('Regions:', regions)
}

main().catch(e => { console.error(e.message); process.exit(1) })
