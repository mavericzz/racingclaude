/**
 * Enrich races with missing data by fetching /v1/results/{race_id}
 * The batch /v1/results endpoint doesn't return distance, class, pattern, etc.
 * The individual endpoint has: dist_m, dist_f, class, pattern, age_band, surface
 */
import { traClient } from '../lib/traClient.js'
import { query, pool } from '../lib/database.js'
import pino from 'pino'

const log = pino({ name: 'enrich-races' })

async function main() {
  // Get all races missing distance_m
  const races = await query<{ race_id: string }>(
    `SELECT race_id FROM races WHERE distance_m IS NULL ORDER BY race_id`
  )

  log.info({ total: races.length }, 'Races missing distance')

  let updated = 0
  let errors = 0

  for (let i = 0; i < races.length; i++) {
    const { race_id } = races[i]
    try {
      const result = await traClient.getResult(race_id)

      const distM = (result as any).dist_m ? parseFloat((result as any).dist_m) : null
      const pattern = (result as any).pattern ?? null
      const raceClass = (result as any).class ?? null
      const ageBand = (result as any).age_band ?? null
      const surface = (result as any).surface ?? null
      const off = (result as any).off_dt ?? null

      // Update race with enriched data
      await pool.query(
        `UPDATE races SET
          distance_m = COALESCE($1, distance_m),
          class = COALESCE($2, class),
          race_group = COALESCE($3, race_group),
          off_time = COALESCE($4::timestamptz, off_time)
        WHERE race_id = $5`,
        [distM ? Math.round(distM) : null, raceClass || pattern || null, pattern, off, race_id]
      )

      // Also update runner weight_lbs and headgear from detailed results
      const runners = (result as any).runners ?? []
      for (const r of runners) {
        if (!r.horse_id) continue
        const weightLbs = r.weight_lbs ? parseFloat(r.weight_lbs) : null
        const headgear = r.headgear || null
        const jockeyClaim = r.jockey_claim_lbs ? parseInt(r.jockey_claim_lbs) : null

        await pool.query(
          `UPDATE runners SET
            weight_lbs = COALESCE($1, weight_lbs),
            headgear = COALESCE($2, headgear),
            jockey_claim = COALESCE($3, jockey_claim)
          WHERE race_id = $4 AND horse_id = $5`,
          [weightLbs, headgear, jockeyClaim, race_id, r.horse_id]
        )
      }

      updated++
      if ((i + 1) % 50 === 0) {
        log.info({ progress: `${i + 1}/${races.length}`, updated, errors }, 'Progress')
      }
    } catch (err: any) {
      errors++
      log.debug({ race_id, err: err.message }, 'Failed to enrich')
    }
  }

  log.info({ updated, errors }, 'Enrichment complete')

  // Verify
  const counts = await pool.query(`
    SELECT
      COUNT(*) AS total_races,
      COUNT(distance_m) AS with_distance,
      COUNT(class) AS with_class
    FROM races
  `)
  log.info(counts.rows[0], 'Race data coverage')

  await pool.end()
}

main().catch(e => { log.error(e); process.exit(1) })
