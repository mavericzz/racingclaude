/**
 * Supplement results data by fetching individual horse histories.
 * The /v1/results endpoint only returns major races (~50/month),
 * but /v1/horses/{id}/results returns ALL races a horse has run in,
 * including maidens, benchmark, and provincial races.
 * This dramatically increases our training data.
 */
import { traClient, type ResultRace } from '../lib/traClient.js'
import { upsert, query, pool } from '../lib/database.js'
import pino from 'pino'

const log = pino({ name: 'ingest-horse-histories' })

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

async function ingestHorseRaces(horseId: string, races: ResultRace[]): Promise<number> {
  let count = 0

  for (const race of races) {
    if (!race.race_id) continue
    // Only AU races
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

export async function ingestHorseHistories(batchSize = 50): Promise<void> {
  // Get all unique horse IDs from our DB that we haven't fully backfilled
  const horses = await query<{ id: string; name: string }>(
    `SELECT DISTINCT h.id, h.name FROM horses h
     ORDER BY h.id
     LIMIT $1`,
    [batchSize * 10] // process up to 500 horses
  )

  log.info({ totalHorses: horses.length }, 'Starting horse history backfill')

  let totalNewResults = 0
  let processed = 0

  for (const horse of horses) {
    try {
      const response = await traClient.getHorseResults(horse.id, { limit: 100 })
      const races = response?.results ?? []

      if (races.length > 0) {
        const count = await ingestHorseRaces(horse.id, races)
        totalNewResults += count
      }

      processed++
      if (processed % 25 === 0) {
        log.info({ processed, totalHorses: horses.length, totalNewResults }, 'Progress')
      }
    } catch (err: any) {
      // Skip 404s etc
      log.debug({ horseId: horse.id, err: err.message }, 'Failed to fetch horse history')
    }
  }

  log.info({ processed, totalNewResults }, 'Horse history backfill complete')
}

// CLI entry
if (import.meta.url === `file://${process.argv[1]}`) {
  const batchSize = parseInt(process.argv[2] ?? '50')
  ingestHorseHistories(batchSize)
    .then(() => pool.end())
    .catch((e) => { log.error(e); process.exit(1) })
}
