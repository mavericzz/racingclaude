/**
 * Fetch horse histories for horses we've mapped from AU racecards.
 * These have canonical hrs_* IDs that work with /v1/horses/{id}/results.
 */
import { traClient, type ResultRace } from '../lib/traClient.js'
import { upsert, query, pool } from '../lib/database.js'
import pino from 'pino'

const log = pino({ name: 'fetch-mapped' })

function toNum(val: string | number | undefined | null): number | null {
  if (val === undefined || val === null || val === '') return null
  const n = typeof val === 'string' ? parseFloat(val) : val
  return isNaN(n) ? null : n
}
function toInt(val: string | number | undefined | null): number | null {
  const n = toNum(val); return n !== null ? Math.round(n) : null
}
function parseBeatenLengths(btn: string | undefined | null): number | null {
  if (!btn) return null
  const specials: Record<string, number> = { nk: 0.3, hd: 0.2, shd: 0.1, snk: 0.2, dht: 0, nse: 0.05 }
  const lower = btn.toLowerCase().trim()
  if (lower in specials) return specials[lower]
  const n = parseFloat(lower); return isNaN(n) ? null : n
}
function parseSP(sp: string | undefined | null): number | null {
  if (!sp) return null
  const fracMatch = sp.match(/^(\d+)\/(\d+)$/)
  if (fracMatch) return parseFloat(fracMatch[1]) / parseFloat(fracMatch[2]) + 1
  const dec = parseFloat(sp); return isNaN(dec) ? null : dec
}
function parseDistanceM(distance?: string, distanceF?: string): number | null {
  if (distance) { const m = distance.match(/(\d+)/); if (m) return parseInt(m[1]) }
  if (distanceF) { const m = distanceF.match(/([\d.]+)/); if (m) return Math.round(parseFloat(m[1]) * 201.168) }
  return null
}
function guessState(course: string): string | null {
  const t: Record<string, string> = {
    randwick:'NSW',rosehill:'NSW',canterbury:'NSW',newcastle:'NSW',kensington:'NSW',hawkesbury:'NSW',gosford:'NSW',kembla:'NSW',wyong:'NSW',
    flemington:'VIC',caulfield:'VIC',moonee:'VIC',sandown:'VIC',cranbourne:'VIC',pakenham:'VIC',ballarat:'VIC',bendigo:'VIC',
    eagle:'QLD',doomben:'QLD',sunshine:'QLD',toowoomba:'QLD',ipswich:'QLD',gold:'QLD',
    morphettville:'SA',ascot:'WA',belmont:'WA',hobart:'TAS',launceston:'TAS',canberra:'ACT',darwin:'NT',
  }
  const lower = course.toLowerCase()
  for (const [key, state] of Object.entries(t)) { if (lower.includes(key)) return state }
  return null
}

async function ingestOneHorse(horseId: string): Promise<number> {
  const response = await traClient.getHorseResults(horseId, { limit: 100 })
  const races: ResultRace[] = response?.results ?? []
  let count = 0
  for (const race of races) {
    if (!race.race_id) continue
    if (race.region && !race.region.toLowerCase().includes('aus')) continue
    const courseId = race.course_id ?? `crs_${(race.course ?? 'unknown').toLowerCase().replace(/\s+/g, '_')}`
    await upsert('venues', ['venue_id','name','state','country'], [courseId, race.course ?? 'Unknown', guessState(race.course ?? ''), 'AU'], ['venue_id'])
    const meetingDate = race.date ?? '1970-01-01'
    const meetingId = `${courseId}_${meetingDate}`
    await upsert('meetings', ['meeting_id','venue_id','meeting_date','source'], [meetingId, courseId, meetingDate, 'TRA_HORSE_HIST'], ['meeting_id'])
    const distanceM = parseDistanceM(race.distance, race.distance_f)
    await upsert('races', ['race_id','meeting_id','race_name','distance_m','going','off_time','field_size'],
      [race.race_id, meetingId, race.race_name ?? null, distanceM, race.going ?? null, race.off_time ?? null, race.runners?.length ?? null], ['race_id'])
    if (!race.runners) continue
    for (const runner of race.runners) {
      if (!runner.horse_id) continue
      await upsert('horses', ['id','name','sire','sire_id','dam','dam_id','damsire','damsire_id','age','sex'],
        [runner.horse_id, runner.horse ?? 'Unknown', runner.sire ?? null, runner.sire_id ?? null, runner.dam ?? null, runner.dam_id ?? null, runner.damsire ?? null, runner.damsire_id ?? null, runner.age ?? null, runner.sex ?? null], ['id'])
      if (runner.jockey_id && runner.jockey) await upsert('jockeys', ['id','name'], [runner.jockey_id, runner.jockey], ['id'])
      if (runner.trainer_id && runner.trainer) await upsert('trainers', ['id','name'], [runner.trainer_id, runner.trainer], ['id'])
      await upsert('runners', ['race_id','horse_id','jockey_id','trainer_id','horse','number','draw','weight_lbs','rating','sp_decimal','position','margin','comment','scratched'],
        [race.race_id, runner.horse_id, runner.jockey_id ?? null, runner.trainer_id ?? null, runner.horse ?? null, toInt(runner.number), toInt(runner.draw), toNum(runner.weight), toInt(runner.or), parseSP(runner.sp), toInt(runner.position), runner.btn ?? null, runner.comment ?? null, false], ['race_id','horse_id'])
      await upsert('results', ['race_id','horse_id','position','sp_decimal','beaten_lengths','race_time','official_rating','rpr','prize','comment'],
        [race.race_id, runner.horse_id, toInt(runner.position), parseSP(runner.sp), parseBeatenLengths(runner.btn), runner.time ?? null, toInt(runner.or), toInt(runner.rpr), toNum(runner.prize), runner.comment ?? null], ['race_id','horse_id'])
      count++
    }
  }
  return count
}

async function main() {
  // Get all mapped canonical IDs that don't have form history yet
  const horses = await query<{ canonical_id: string }>(
    `SELECT DISTINCT m.canonical_id FROM horse_id_map m
     LEFT JOIN horse_form_history hfh ON m.canonical_id = hfh.horse_id
     WHERE hfh.horse_id IS NULL`
  )
  log.info({ count: horses.length }, 'Mapped horses needing history')

  let processed = 0, totalResults = 0, errors = 0
  for (let i = 0; i < horses.length; i += 3) {
    const batch = horses.slice(i, i + 3)
    const results = await Promise.allSettled(batch.map(h => ingestOneHorse(h.canonical_id)))
    for (const r of results) {
      processed++
      if (r.status === 'fulfilled') totalResults += r.value
      else errors++
    }
    if (processed % 50 === 0 || processed === horses.length) {
      log.info({ processed, total: horses.length, results: totalResults, errors }, 'Progress')
    }
  }

  log.info({ processed, totalResults, errors }, 'Done')
  await pool.end()
}

main().catch(e => { log.error(e); process.exit(1) })
