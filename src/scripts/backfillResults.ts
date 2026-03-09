/**
 * Backfill AU results for the last N months via /v1/results endpoint.
 * Then re-match horse IDs and rebuild form history + speed figures.
 */
import { ingestResults } from '../etl/ingestResults.js'
import { buildFormHistory } from '../etl/buildFormHistory.js'
import { computeSpeedFigures } from '../etl/computeSpeedFigures.js'
import { execute, pool } from '../lib/database.js'
import pino from 'pino'

const log = pino({ name: 'backfill-results' })

async function main() {
  const months = parseInt(process.argv[2] ?? '18')
  log.info({ months }, 'Backfilling AU results')

  const end = new Date()
  let total = 0

  for (let i = 0; i < months; i++) {
    const chunkEnd = new Date(end)
    chunkEnd.setMonth(chunkEnd.getMonth() - i)
    const chunkStart = new Date(chunkEnd)
    chunkStart.setMonth(chunkStart.getMonth() - 1)
    const from = chunkStart.toISOString().slice(0, 10)
    const to = chunkEnd.toISOString().slice(0, 10)

    try {
      const n = await ingestResults(from, to)
      total += n
      log.info({ from, to, results: n, total }, 'Chunk complete')
    } catch (err: any) {
      log.warn({ from, to, err: err.message }, 'Chunk failed')
    }
  }

  log.info({ total }, 'Results backfill complete')

  // Re-match horse IDs
  log.info('Re-matching horse IDs...')
  const mapResult = await pool.query(`
    INSERT INTO horse_id_map (aus_id, canonical_id, horse_name)
    SELECT DISTINCT ON (h1.id) h1.id, h2.id, h1.name
    FROM horses h1 JOIN horses h2 ON
      LOWER(REGEXP_REPLACE(h1.name, '\\s*\\((AUS|NZ|IRE|GB|USA|FR|GER|JPN)\\)\\s*$', '', 'i'))
      = LOWER(REGEXP_REPLACE(h2.name, '\\s*\\((AUS|NZ|IRE|GB|USA|FR|GER|JPN)\\)\\s*$', '', 'i'))
    WHERE h1.id LIKE 'hrs_aus_%' AND h2.id LIKE 'hrs_%' AND h2.id NOT LIKE 'hrs_aus_%'
    ON CONFLICT (aus_id) DO UPDATE SET canonical_id = EXCLUDED.canonical_id
  `)
  log.info({ mappings: mapResult.rowCount }, 'Horse ID matching updated')

  // Rebuild form history
  log.info('Rebuilding form history...')
  const formCount = await buildFormHistory()
  log.info({ formHistory: formCount }, 'Form history rebuilt')

  // Recompute speed figures
  log.info('Recomputing speed figures...')
  const speedCount = await computeSpeedFigures()
  log.info({ speedFigures: speedCount }, 'Speed figures recomputed')

  // Refresh materialized views
  log.info('Refreshing materialized views...')
  for (const view of ['mv_trainer_stats', 'mv_jockey_stats', 'mv_combo_stats',
    'mv_trainer_spell_stats', 'mv_track_bias', 'mv_barrier_stats']) {
    try {
      await execute(`REFRESH MATERIALIZED VIEW ${view}`)
      log.info({ view }, 'Refreshed')
    } catch (err: any) {
      log.warn({ view, err: err.message }, 'Refresh failed')
    }
  }

  // Summary
  const summary = await pool.query(`
    SELECT
      (SELECT COUNT(*) FROM results) AS results,
      (SELECT COUNT(*) FROM horse_form_history) AS form_history,
      (SELECT COUNT(*) FROM speed_figures) AS speed_figures,
      (SELECT COUNT(*) FROM horse_id_map) AS mappings,
      (SELECT COUNT(DISTINCT r.horse_id) FROM runners r
       JOIN races rc ON r.race_id = rc.race_id JOIN meetings m ON rc.meeting_id = m.meeting_id
       WHERE m.meeting_date >= CURRENT_DATE AND r.scratched = FALSE) AS future_horses,
      (SELECT COUNT(DISTINCT r.horse_id) FROM runners r
       JOIN races rc ON r.race_id = rc.race_id JOIN meetings m ON rc.meeting_id = m.meeting_id
       LEFT JOIN horse_id_map map ON r.horse_id = map.aus_id
       LEFT JOIN horse_form_history hfh ON COALESCE(map.canonical_id, r.horse_id) = hfh.horse_id
       WHERE m.meeting_date >= CURRENT_DATE AND r.scratched = FALSE AND hfh.horse_id IS NOT NULL) AS future_with_form
  `)
  const s = summary.rows[0]
  log.info({
    results: s.results,
    formHistory: s.form_history,
    speedFigures: s.speed_figures,
    horseMappings: s.mappings,
    futureHorses: s.future_horses,
    futureWithForm: s.future_with_form,
    futureCoverage: `${((s.future_with_form / s.future_horses) * 100).toFixed(1)}%`,
  }, 'Backfill complete')

  await pool.end()
}

main().catch(e => { log.error(e); process.exit(1) })
