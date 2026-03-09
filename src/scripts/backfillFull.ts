/**
 * Full backfill pipeline:
 * 1. Fetch horse histories for ALL horses in DB missing form data (prioritizing today's runners)
 * 2. Rebuild horse_form_history from results
 * 3. Recompute speed figures
 * 4. Refresh materialized views
 *
 * Usage: npx tsx src/scripts/backfillFull.ts [--today-only] [--skip-histories] [--batch-size=100]
 */
import { traClient, type ResultRace } from '../lib/traClient.js'
import { upsert, query, execute, pool } from '../lib/database.js'
import { buildFormHistory } from '../etl/buildFormHistory.js'
import { computeSpeedFigures } from '../etl/computeSpeedFigures.js'
import pino from 'pino'

const log = pino({ name: 'backfill-full' })

// --- Helpers (same as ingestHorseHistories) ---

function toNum(val: string | number | undefined | null): number | null {
  if (val === undefined || val === null || val === '') return null
  const n = typeof val === 'string' ? parseFloat(val) : val
  return isNaN(n) ? null : n
}

function toInt(val: string | number | undefined | null): number | null {
  const n = toNum(val)
  return n !== null ? Math.round(n) : null
}

function parseBeatenLengths(btn: string | undefined | null): number | null {
  if (!btn) return null
  const specials: Record<string, number> = { nk: 0.3, hd: 0.2, shd: 0.1, snk: 0.2, dht: 0, nse: 0.05 }
  const lower = btn.toLowerCase().trim()
  if (lower in specials) return specials[lower]
  const n = parseFloat(lower)
  return isNaN(n) ? null : n
}

function parseSP(sp: string | undefined | null): number | null {
  if (!sp) return null
  const fracMatch = sp.match(/^(\d+)\/(\d+)$/)
  if (fracMatch) return parseFloat(fracMatch[1]) / parseFloat(fracMatch[2]) + 1
  const dec = parseFloat(sp)
  return isNaN(dec) ? null : dec
}

function parseDistanceM(distance?: string, distanceF?: string): number | null {
  if (distance) {
    const match = distance.match(/(\d+)/)
    if (match) return parseInt(match[1])
  }
  if (distanceF) {
    const fMatch = distanceF.match(/([\d.]+)/)
    if (fMatch) return Math.round(parseFloat(fMatch[1]) * 201.168)
  }
  return null
}

function guessState(course: string): string | null {
  const trackStates: Record<string, string> = {
    randwick: 'NSW', rosehill: 'NSW', warwick: 'NSW', canterbury: 'NSW', newcastle: 'NSW',
    kensington: 'NSW', hawkesbury: 'NSW', gosford: 'NSW', kembla: 'NSW', wyong: 'NSW',
    scone: 'NSW', mudgee: 'NSW', tamworth: 'NSW', dubbo: 'NSW', bathurst: 'NSW',
    flemington: 'VIC', caulfield: 'VIC', moonee: 'VIC', sandown: 'VIC', cranbourne: 'VIC',
    pakenham: 'VIC', ballarat: 'VIC', bendigo: 'VIC', geelong: 'VIC', mornington: 'VIC',
    eagle: 'QLD', doomben: 'QLD', sunshine: 'QLD', toowoomba: 'QLD', ipswich: 'QLD',
    gold: 'QLD', rockhampton: 'QLD', caloundra: 'QLD',
    morphettville: 'SA', murray: 'SA', gawler: 'SA',
    ascot: 'WA', belmont: 'WA', bunbury: 'WA', pinjarra: 'WA',
    hobart: 'TAS', launceston: 'TAS', devonport: 'TAS',
    canberra: 'ACT', darwin: 'NT', alice: 'NT',
  }
  const lower = course.toLowerCase()
  for (const [key, state] of Object.entries(trackStates)) {
    if (lower.includes(key)) return state
  }
  return null
}

// --- Ingest one horse's race history ---

async function ingestOneHorseHistory(horseId: string): Promise<number> {
  const response = await traClient.getHorseResults(horseId, { limit: 100 })
  const races: ResultRace[] = response?.results ?? []
  let count = 0

  for (const race of races) {
    if (!race.race_id) continue
    if (race.region && !race.region.toLowerCase().includes('aus')) continue

    const courseId = race.course_id ?? `crs_${(race.course ?? 'unknown').toLowerCase().replace(/\s+/g, '_')}`
    const state = guessState(race.course ?? '')
    await upsert('venues', ['venue_id', 'name', 'state', 'country'], [courseId, race.course ?? 'Unknown', state, 'AU'], ['venue_id'])

    const meetingDate = race.date ?? '1970-01-01'
    const meetingId = `${courseId}_${meetingDate}`
    await upsert('meetings', ['meeting_id', 'venue_id', 'meeting_date', 'source'], [meetingId, courseId, meetingDate, 'TRA_HORSE_HIST'], ['meeting_id'])

    const distanceM = parseDistanceM(race.distance, race.distance_f)
    await upsert(
      'races',
      ['race_id', 'meeting_id', 'race_name', 'distance_m', 'going', 'off_time', 'field_size'],
      [race.race_id, meetingId, race.race_name ?? null, distanceM, race.going ?? null, race.off_time ?? null, race.runners?.length ?? null],
      ['race_id']
    )

    if (!race.runners) continue

    for (const runner of race.runners) {
      if (!runner.horse_id) continue

      await upsert(
        'horses',
        ['id', 'name', 'sire', 'sire_id', 'dam', 'dam_id', 'damsire', 'damsire_id', 'age', 'sex'],
        [runner.horse_id, runner.horse ?? 'Unknown', runner.sire ?? null, runner.sire_id ?? null, runner.dam ?? null, runner.dam_id ?? null, runner.damsire ?? null, runner.damsire_id ?? null, runner.age ?? null, runner.sex ?? null],
        ['id']
      )

      if (runner.jockey_id && runner.jockey) {
        await upsert('jockeys', ['id', 'name'], [runner.jockey_id, runner.jockey], ['id'])
      }
      if (runner.trainer_id && runner.trainer) {
        await upsert('trainers', ['id', 'name'], [runner.trainer_id, runner.trainer], ['id'])
      }

      await upsert(
        'runners',
        ['race_id', 'horse_id', 'jockey_id', 'trainer_id', 'horse', 'number', 'draw', 'weight_lbs', 'rating', 'sp_decimal', 'position', 'margin', 'comment', 'scratched'],
        [
          race.race_id, runner.horse_id, runner.jockey_id ?? null, runner.trainer_id ?? null,
          runner.horse ?? null, toInt(runner.number), toInt(runner.draw), toNum(runner.weight),
          toInt(runner.or), parseSP(runner.sp), toInt(runner.position), runner.btn ?? null,
          runner.comment ?? null, false,
        ],
        ['race_id', 'horse_id']
      )

      await upsert(
        'results',
        ['race_id', 'horse_id', 'position', 'sp_decimal', 'beaten_lengths', 'race_time', 'official_rating', 'rpr', 'prize', 'comment'],
        [
          race.race_id, runner.horse_id, toInt(runner.position),
          parseSP(runner.sp), parseBeatenLengths(runner.btn),
          runner.time ?? null, toInt(runner.or), toInt(runner.rpr),
          toNum(runner.prize), runner.comment ?? null,
        ],
        ['race_id', 'horse_id']
      )

      count++
    }
  }

  return count
}

// --- Main pipeline ---

async function main() {
  const args = process.argv.slice(2)
  const todayOnly = args.includes('--today-only')
  const skipHistories = args.includes('--skip-histories')
  const batchArg = args.find(a => a.startsWith('--batch-size='))
  const concurrency = 3 // parallel API calls (conservative for rate limits)

  log.info({ todayOnly, skipHistories }, 'Starting full backfill pipeline')

  // ===== Step 1: Fetch horse histories from API =====
  if (!skipHistories) {
    log.info('=== Step 1: Fetching horse histories from TheRacingAPI ===')

    // Find horses missing form history, prioritize today's runners
    const missingHorses = await query<{ horse_id: string; is_today: boolean }>(`
      SELECT DISTINCT r.horse_id,
        EXISTS (
          SELECT 1 FROM runners r2
          JOIN races rc2 ON r2.race_id = rc2.race_id
          JOIN meetings m2 ON rc2.meeting_id = m2.meeting_id
          WHERE r2.horse_id = r.horse_id AND m2.meeting_date = CURRENT_DATE
        ) AS is_today
      FROM runners r
      LEFT JOIN horse_form_history hfh ON r.horse_id = hfh.horse_id
      WHERE hfh.horse_id IS NULL
        AND r.scratched = FALSE
      ${todayOnly ? "AND EXISTS (SELECT 1 FROM runners r2 JOIN races rc2 ON r2.race_id = rc2.race_id JOIN meetings m2 ON rc2.meeting_id = m2.meeting_id WHERE r2.horse_id = r.horse_id AND m2.meeting_date = CURRENT_DATE)" : ''}
      ORDER BY is_today DESC
    `)

    log.info({ total: missingHorses.length, todayRunners: missingHorses.filter(h => h.is_today).length }, 'Horses missing form history')

    let processed = 0
    let totalNewResults = 0
    let errors = 0
    const startTime = Date.now()

    // Process in batches with concurrency
    for (let i = 0; i < missingHorses.length; i += concurrency) {
      const batch = missingHorses.slice(i, i + concurrency)

      const results = await Promise.allSettled(
        batch.map(async (horse) => {
          try {
            return await ingestOneHorseHistory(horse.horse_id)
          } catch (err: any) {
            if (err.message?.includes('404') || err.message?.includes('Not Found')) {
              return 0 // Horse not found in API, skip silently
            }
            throw err
          }
        })
      )

      for (const result of results) {
        processed++
        if (result.status === 'fulfilled') {
          totalNewResults += result.value
        } else {
          errors++
          if (errors <= 10) {
            log.warn({ error: result.reason?.message }, 'Horse history fetch failed')
          }
        }
      }

      if (processed % 50 === 0 || processed === missingHorses.length) {
        const elapsed = (Date.now() - startTime) / 1000
        const rate = processed / elapsed
        const eta = (missingHorses.length - processed) / rate
        log.info({
          processed,
          total: missingHorses.length,
          pct: `${(processed / missingHorses.length * 100).toFixed(1)}%`,
          newResults: totalNewResults,
          errors,
          rate: `${rate.toFixed(1)}/s`,
          eta: `${Math.round(eta)}s`,
        }, 'Progress')
      }
    }

    log.info({ processed, totalNewResults, errors }, 'Step 1 complete: Horse histories fetched')
  }

  // ===== Step 2: Rebuild form history =====
  log.info('=== Step 2: Building form history ===')
  const formCount = await buildFormHistory()
  log.info({ formHistoryRows: formCount }, 'Step 2 complete')

  // ===== Step 3: Recompute speed figures =====
  log.info('=== Step 3: Computing speed figures ===')
  const speedCount = await computeSpeedFigures()
  log.info({ speedFigures: speedCount }, 'Step 3 complete')

  // ===== Step 4: Refresh materialized views =====
  log.info('=== Step 4: Refreshing materialized views ===')
  const views = [
    'mv_trainer_stats', 'mv_jockey_stats', 'mv_combo_stats',
    'mv_trainer_spell_stats', 'mv_track_bias', 'mv_barrier_stats',
  ]
  for (const view of views) {
    try {
      await execute(`REFRESH MATERIALIZED VIEW ${view}`)
      log.info({ view }, 'Refreshed')
    } catch (err: any) {
      log.warn({ view, err: err.message }, 'View refresh failed')
    }
  }

  // ===== Summary =====
  const counts = await pool.query(`
    SELECT
      (SELECT COUNT(*) FROM venues) AS venues,
      (SELECT COUNT(*) FROM meetings) AS meetings,
      (SELECT COUNT(*) FROM races) AS races,
      (SELECT COUNT(*) FROM runners) AS runners,
      (SELECT COUNT(*) FROM horses) AS horses,
      (SELECT COUNT(*) FROM results) AS results,
      (SELECT COUNT(*) FROM horse_form_history) AS form_history,
      (SELECT COUNT(*) FROM speed_figures) AS speed_figures,
      (SELECT COUNT(DISTINCT r.horse_id) FROM runners r
       JOIN races rc ON r.race_id = rc.race_id
       JOIN meetings m ON rc.meeting_id = m.meeting_id
       WHERE m.meeting_date = CURRENT_DATE AND r.scratched = FALSE) AS today_horses,
      (SELECT COUNT(DISTINCT r.horse_id) FROM runners r
       JOIN races rc ON r.race_id = rc.race_id
       JOIN meetings m ON rc.meeting_id = m.meeting_id
       LEFT JOIN horse_form_history hfh ON r.horse_id = hfh.horse_id
       WHERE m.meeting_date = CURRENT_DATE AND r.scratched = FALSE AND hfh.horse_id IS NOT NULL) AS today_with_form
  `)

  const s = counts.rows[0]
  log.info({
    venues: s.venues,
    meetings: s.meetings,
    races: s.races,
    runners: s.runners,
    horses: s.horses,
    results: s.results,
    formHistory: s.form_history,
    speedFigures: s.speed_figures,
    todayHorses: s.today_horses,
    todayWithForm: s.today_with_form,
    todayFormCoverage: `${((s.today_with_form / s.today_horses) * 100).toFixed(1)}%`,
  }, 'Backfill complete - database summary')

  await pool.end()
}

main().catch((e) => {
  log.error(e, 'Backfill failed')
  process.exit(1)
})
