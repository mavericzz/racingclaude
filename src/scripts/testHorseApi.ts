/**
 * Quick test: try fetching horse results using different ID formats.
 */
import { traClient } from '../lib/traClient.js'
import { query, pool } from '../lib/database.js'

async function main() {
  // Get a few horse IDs of each type
  const ausHorses = await query<{ id: string; name: string }>(
    `SELECT id, name FROM horses WHERE id LIKE 'hrs_aus_%' LIMIT 3`
  )
  const regHorses = await query<{ id: string; name: string }>(
    `SELECT id, name FROM horses WHERE id LIKE 'hrs_%' AND id NOT LIKE 'hrs_aus_%' LIMIT 3`
  )

  console.log('=== Testing hrs_aus_* IDs ===')
  for (const h of ausHorses) {
    try {
      const resp = await traClient.getHorseResults(h.id, { limit: 5 })
      console.log(`OK: ${h.id} (${h.name}) -> ${resp?.results?.length ?? 0} results`)
    } catch (e: any) {
      console.log(`FAIL: ${h.id} (${h.name}) -> ${e.message?.slice(0, 80)}`)
    }
  }

  console.log('\n=== Testing hrs_* IDs ===')
  for (const h of regHorses) {
    try {
      const resp = await traClient.getHorseResults(h.id, { limit: 5 })
      console.log(`OK: ${h.id} (${h.name}) -> ${resp?.results?.length ?? 0} results`)
    } catch (e: any) {
      console.log(`FAIL: ${h.id} (${h.name}) -> ${e.message?.slice(0, 80)}`)
    }
  }

  // Test: can we get results for a race from the AU meets?
  console.log('\n=== Testing individual race result ===')
  const auRaces = await query<{ race_id: string; race_name: string }>(
    `SELECT r.race_id, r.race_name FROM races r JOIN meetings m ON r.meeting_id = m.meeting_id
     WHERE m.meeting_date = '2026-03-07' AND m.source = 'TRA_AU' LIMIT 3`
  )
  for (const race of auRaces) {
    try {
      const resp = await traClient.getResult(race.race_id)
      const runners = (resp as any)?.runners ?? []
      console.log(`OK: ${race.race_id} (${race.race_name}) -> ${runners.length} runners`)
      if (runners.length > 0) {
        console.log(`  First runner: ${runners[0].horse} (${runners[0].horse_id}) pos=${runners[0].position}`)
      }
    } catch (e: any) {
      console.log(`FAIL: ${race.race_id} -> ${e.message?.slice(0, 100)}`)
    }
  }

  await pool.end()
}

main().catch(e => { console.error(e); process.exit(1) })
