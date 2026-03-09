import { traClient, type ResultRace, type ResultRunner } from '../lib/traClient.js'
import { upsert, query, pool } from '../lib/database.js'
import pino from 'pino'

const log = pino({ name: 'ingest-results' })

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
  // Try distance field first (e.g. "1200m" or "1200")
  if (distance) {
    const match = distance.match(/(\d+)/)
    if (match) return parseInt(match[1])
  }
  // Try distance_f field (furlongs -> metres: 1f = 201.168m)
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
    moruya: 'NSW', nowra: 'NSW', goulburn: 'NSW', queanbeyan: 'NSW', grafton: 'NSW',
    coffs: 'NSW', port: 'NSW', taree: 'NSW', muswellbrook: 'NSW', orange: 'NSW',
    flemington: 'VIC', caulfield: 'VIC', moonee: 'VIC', sandown: 'VIC', cranbourne: 'VIC',
    pakenham: 'VIC', ballarat: 'VIC', bendigo: 'VIC', geelong: 'VIC', mornington: 'VIC',
    wangaratta: 'VIC', echuca: 'VIC', swan: 'VIC', kilmore: 'VIC', seymour: 'VIC',
    sale: 'VIC', hamilton: 'VIC', warrnambool: 'VIC', stawell: 'VIC', stony: 'VIC',
    eagle: 'QLD', doomben: 'QLD', sunshine: 'QLD', toowoomba: 'QLD', ipswich: 'QLD',
    gold: 'QLD', rockhampton: 'QLD', cairns: 'QLD', townsville: 'QLD', mackay: 'QLD',
    caloundra: 'QLD', beaudesert: 'QLD', dalby: 'QLD',
    morphettville: 'SA', murray: 'SA', gawler: 'SA', strathalbyn: 'SA', balaklava: 'SA',
    ascot: 'WA', belmont: 'WA', bunbury: 'WA', northam: 'WA', geraldton: 'WA', pinjarra: 'WA',
    hobart: 'TAS', launceston: 'TAS', devonport: 'TAS',
    canberra: 'ACT',
    darwin: 'NT', alice: 'NT',
  }
  const lower = course.toLowerCase()
  for (const [key, state] of Object.entries(trackStates)) {
    if (lower.includes(key)) return state
  }
  return null
}

/**
 * Ingest historical results from /v1/results endpoint.
 * This also creates venues, meetings, races, horses, jockeys, trainers
 * since this is our primary historical data source.
 */
export async function ingestResults(dateFrom: string, dateTo: string): Promise<number> {
  log.info({ dateFrom, dateTo }, 'Fetching results')

  let totalResults = 0
  let skip = 0
  const limit = 100 // API max is 100

  while (true) {
    const response = await traClient.getResults({
      region: 'aus',
      start_date: dateFrom,
      end_date: dateTo,
      limit,
      skip,
    })

    const races: ResultRace[] = Array.isArray(response)
      ? response
      : (response as any)?.results ?? []

    if (races.length === 0) break

    for (const race of races) {
      if (!race.race_id) continue

      // Create venue from course info
      const courseId = race.course_id ?? `crs_${(race.course ?? 'unknown').toLowerCase().replace(/\s+/g, '_')}`
      const state = guessState(race.course ?? '')

      await upsert(
        'venues',
        ['venue_id', 'name', 'state', 'country'],
        [courseId, race.course ?? 'Unknown', state, 'AU'],
        ['venue_id']
      )

      // Create meeting from race date + venue
      const meetingDate = race.date ?? dateFrom
      const meetingId = `${courseId}_${meetingDate}`

      await upsert(
        'meetings',
        ['meeting_id', 'venue_id', 'meeting_date', 'source'],
        [meetingId, courseId, meetingDate, 'TRA_RESULTS'],
        ['meeting_id']
      )

      // Create race
      const distanceM = parseDistanceM(race.distance, race.distance_f)
      await upsert(
        'races',
        ['race_id', 'meeting_id', 'race_name', 'distance_m', 'going', 'off_time', 'field_size'],
        [
          race.race_id, meetingId, race.race_name ?? null, distanceM,
          race.going ?? null, race.off_time ?? null,
          race.runners?.length ?? null,
        ],
        ['race_id']
      )

      // Process runners/results
      if (!race.runners || race.runners.length === 0) continue

      for (const runner of race.runners) {
        if (!runner.horse_id) continue

        const position = toInt(runner.position)

        // Upsert horse
        await upsert(
          'horses',
          ['id', 'name', 'sire', 'sire_id', 'dam', 'dam_id', 'damsire', 'damsire_id', 'age', 'sex'],
          [runner.horse_id, runner.horse ?? 'Unknown', runner.sire ?? null, runner.sire_id ?? null, runner.dam ?? null, runner.dam_id ?? null, runner.damsire ?? null, runner.damsire_id ?? null, runner.age ?? null, runner.sex ?? null],
          ['id']
        )

        // Upsert jockey
        if (runner.jockey_id && runner.jockey) {
          await upsert('jockeys', ['id', 'name'], [runner.jockey_id, runner.jockey], ['id'])
        }

        // Upsert trainer
        if (runner.trainer_id && runner.trainer) {
          await upsert('trainers', ['id', 'name'], [runner.trainer_id, runner.trainer], ['id'])
        }

        // Upsert runner
        await upsert(
          'runners',
          ['race_id', 'horse_id', 'jockey_id', 'trainer_id', 'horse', 'number', 'draw', 'weight_lbs', 'rating', 'sp_decimal', 'position', 'margin', 'comment', 'scratched'],
          [
            race.race_id, runner.horse_id, runner.jockey_id ?? null, runner.trainer_id ?? null,
            runner.horse ?? null, toInt(runner.number), toInt(runner.draw), toNum(runner.weight),
            toInt(runner.or), parseSP(runner.sp), position, runner.btn ?? null,
            runner.comment ?? null, false,
          ],
          ['race_id', 'horse_id']
        )

        // Upsert result
        await upsert(
          'results',
          ['race_id', 'horse_id', 'position', 'sp_decimal', 'beaten_lengths', 'race_time', 'official_rating', 'rpr', 'prize', 'comment'],
          [
            race.race_id, runner.horse_id, position,
            parseSP(runner.sp), parseBeatenLengths(runner.btn),
            runner.time ?? null, toInt(runner.or), toInt(runner.rpr),
            toNum(runner.prize), runner.comment ?? null,
          ],
          ['race_id', 'horse_id']
        )

        totalResults++
      }
    }

    log.info({ batch: skip, results: totalResults, racesInBatch: races.length }, 'Batch processed')
    skip += limit

    if (races.length < limit) break
  }

  log.info({ totalResults }, 'Results ingestion complete')
  return totalResults
}

// CLI entry
if (import.meta.url === `file://${process.argv[1]}`) {
  const dateFrom = process.argv[2]
  const dateTo = process.argv[3]
  if (!dateFrom || !dateTo) {
    console.error('Usage: tsx ingestResults.ts <start_date> <end_date>')
    process.exit(1)
  }
  ingestResults(dateFrom, dateTo)
    .then((n) => { log.info({ results: n }, 'Done'); pool.end() })
    .catch((e) => { log.error(e); process.exit(1) })
}
