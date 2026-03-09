/**
 * Link hrs_aus_* horse IDs to their hrs_* counterparts by name matching.
 * Creates a horse_id_map table and updates runners/results to use canonical IDs.
 * Also fetches horse histories for unmatched AU horses using the results API.
 */
import { query, execute, pool } from '../lib/database.js'
import { traClient } from '../lib/traClient.js'
import { upsert } from '../lib/database.js'
import pino from 'pino'

const log = pino({ name: 'link-horses' })

function normalizeName(name: string): string {
  return name.toLowerCase()
    .replace(/\s*\(aus\)\s*/gi, '')
    .replace(/\s*\(nz\)\s*/gi, '')
    .replace(/\s*\(ire\)\s*/gi, '')
    .replace(/\s*\(gb\)\s*/gi, '')
    .replace(/\s*\(usa\)\s*/gi, '')
    .replace(/\s*\(fr\)\s*/gi, '')
    .replace(/\s*\(ger\)\s*/gi, '')
    .replace(/\s*\(jpn\)\s*/gi, '')
    .trim()
}

async function main() {
  // Step 1: Create horse_id_map table
  log.info('Creating horse_id_map table...')
  await execute(`
    CREATE TABLE IF NOT EXISTS horse_id_map (
      aus_id TEXT PRIMARY KEY,
      canonical_id TEXT NOT NULL,
      horse_name TEXT,
      matched_by TEXT DEFAULT 'name'
    )
  `)

  // Step 2: Find all hrs_aus_* horses
  const ausHorses = await query<{ id: string; name: string }>(
    `SELECT id, name FROM horses WHERE id LIKE 'hrs_aus_%'`
  )
  log.info({ count: ausHorses.length }, 'AU horses found')

  // Step 3: Find all hrs_* horses (canonical)
  const canonHorses = await query<{ id: string; name: string }>(
    `SELECT id, name FROM horses WHERE id LIKE 'hrs_%' AND id NOT LIKE 'hrs_aus_%'`
  )
  log.info({ count: canonHorses.length }, 'Canonical horses found')

  // Build name -> canonical ID lookup
  const nameToCanon = new Map<string, string>()
  for (const h of canonHorses) {
    nameToCanon.set(normalizeName(h.name), h.id)
  }

  // Step 4: Match by name
  let matched = 0
  let unmatched = 0
  const unmatchedHorses: { id: string; name: string }[] = []

  for (const h of ausHorses) {
    const normalized = normalizeName(h.name)
    const canonId = nameToCanon.get(normalized)
    if (canonId) {
      await upsert(
        'horse_id_map',
        ['aus_id', 'canonical_id', 'horse_name', 'matched_by'],
        [h.id, canonId, h.name, 'name'],
        ['aus_id']
      )
      matched++
    } else {
      unmatchedHorses.push(h)
      unmatched++
    }
  }

  log.info({ matched, unmatched }, 'Name matching complete')

  // Step 5: For unmatched horses, try to fetch their results from the API
  // We can search by horse name via the results endpoint
  log.info(`Fetching histories for ${Math.min(unmatchedHorses.length, 500)} unmatched horses...`)

  let newMatches = 0
  let fetched = 0
  const batchSize = 3

  for (let i = 0; i < Math.min(unmatchedHorses.length, 500); i += batchSize) {
    const batch = unmatchedHorses.slice(i, i + batchSize)

    await Promise.allSettled(batch.map(async (horse) => {
      try {
        // Try searching results by horse name
        const searchResp = await traClient.get<{ results: any[] }>(
          '/v1/horses/search',
          { name: normalizeName(horse.name) }
        )
        const results = searchResp?.results ?? []
        if (results.length > 0) {
          const match = results[0]
          if (match.horse_id && !match.horse_id.startsWith('hrs_aus_')) {
            // Found a canonical ID
            await upsert(
              'horse_id_map',
              ['aus_id', 'canonical_id', 'horse_name', 'matched_by'],
              [horse.id, match.horse_id, horse.name, 'api_search'],
              ['aus_id']
            )
            newMatches++

            // Also fetch their full history
            try {
              const histResp = await traClient.getHorseResults(match.horse_id, { limit: 100 })
              const races = histResp?.results ?? []
              fetched += races.length
            } catch { /* ignore */ }
          }
        }
      } catch { /* ignore search errors */ }
    }))

    if ((i + batchSize) % 50 < batchSize) {
      log.info({ progress: i + batchSize, total: Math.min(unmatchedHorses.length, 500), newMatches, fetched }, 'Search progress')
    }
  }

  log.info({ newMatches, fetched }, 'API search complete')

  // Step 6: Summary
  const mapCount = await query<{ count: number }>(
    `SELECT COUNT(*)::int AS count FROM horse_id_map`
  )
  const todayCoverage = await query<{ total: number; mapped: number }>(
    `SELECT
      COUNT(DISTINCT r.horse_id)::int AS total,
      COUNT(DISTINCT CASE WHEN hfh.horse_id IS NOT NULL OR m2.canonical_id IS NOT NULL THEN r.horse_id END)::int AS mapped
     FROM runners r
     JOIN races rc ON r.race_id = rc.race_id
     JOIN meetings m ON rc.meeting_id = m.meeting_id
     LEFT JOIN horse_form_history hfh ON r.horse_id = hfh.horse_id
     LEFT JOIN horse_id_map m2 ON r.horse_id = m2.aus_id
     WHERE m.meeting_date >= CURRENT_DATE AND r.scratched = FALSE`
  )

  log.info({
    totalMappings: mapCount[0]?.count,
    todayHorses: todayCoverage[0]?.total,
    todayWithData: todayCoverage[0]?.mapped,
  }, 'Horse linking complete')

  await pool.end()
}

main().catch(e => { log.error(e); process.exit(1) })
