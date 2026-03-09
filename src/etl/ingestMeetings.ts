import { traClient, type AustraliaMeet } from '../lib/traClient.js'
import { upsert, query, execute, pool } from '../lib/database.js'
import pino from 'pino'

const log = pino({ name: 'ingest-meetings' })

function parseDistanceM(distance?: string): number | null {
  if (!distance) return null
  const match = distance.match(/(\d+)/)
  return match ? parseInt(match[1]) : null
}

function parseState(meetId: string, course: string): string | null {
  // Try to extract state from meet_id pattern: met_aus_NSW_...
  const stateMatch = meetId.match(/met_aus_([A-Z]{2,3})/)
  if (stateMatch) return stateMatch[1]

  // Fallback: well-known tracks
  const trackStates: Record<string, string> = {
    randwick: 'NSW', rosehill: 'NSW', warwick: 'NSW', canterbury: 'NSW', newcastle: 'NSW',
    kensington: 'NSW', hawkesbury: 'NSW', gosford: 'NSW', kembla: 'NSW',
    flemington: 'VIC', caulfield: 'VIC', moonee: 'VIC', sandown: 'VIC', cranbourne: 'VIC',
    pakenham: 'VIC', ballarat: 'VIC', bendigo: 'VIC', geelong: 'VIC', mornington: 'VIC',
    eagle: 'QLD', doomben: 'QLD', sunshine: 'QLD', toowoomba: 'QLD', ipswich: 'QLD',
    gold: 'QLD',
    morphettville: 'SA', murray: 'SA', gawler: 'SA',
    ascot: 'WA', belmont: 'WA',
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
 * Ingest today's (or a specific date's) Australian meetings.
 * Note: /v1/australia/meets only supports a single date, NOT a date range.
 * For historical data, use ingestResults which uses /v1/results endpoint.
 */
export async function ingestMeetings(date?: string): Promise<number> {
  log.info({ date: date ?? 'today' }, 'Fetching AU meetings')

  const response = await traClient.getAustraliaMeets(date)

  // Handle both { meets: [...] } and direct array response
  const meets: AustraliaMeet[] = Array.isArray(response)
    ? response
    : (response as any)?.meets ?? []

  log.info({ count: meets.length }, 'Meetings received')

  let racesInserted = 0

  for (const meet of meets) {
    const meetId = meet.meet_id
    const courseId = meet.course_id
    const state = parseState(meetId, meet.course)

    // Upsert venue
    await upsert(
      'venues',
      ['venue_id', 'name', 'state', 'country'],
      [courseId, meet.course, state, 'AU'],
      ['venue_id']
    )

    // Upsert meeting
    await upsert(
      'meetings',
      ['meeting_id', 'venue_id', 'meeting_date', 'source'],
      [meetId, courseId, meet.date, 'TRA_AU'],
      ['meeting_id']
    )

    // Upsert races
    for (const race of meet.races ?? []) {
      // Skip trials and jump-outs
      if (race.is_trial || race.is_jump_out) continue

      const raceNum = typeof race.race_number === 'string' ? parseInt(race.race_number) : race.race_number
      const raceId = `${meetId}_R${raceNum}`
      const distanceM = parseDistanceM(race.distance)

      await upsert(
        'races',
        ['race_id', 'meeting_id', 'race_number', 'race_name', 'class', 'race_group', 'distance_m', 'race_status', 'off_time'],
        [raceId, meetId, raceNum, race.race_name ?? null, race.class ?? null, race.race_group ?? null, distanceM, race.race_status ?? null, race.off_time ?? null],
        ['race_id']
      )
      racesInserted++
    }
  }

  log.info({ meetings: meets.length, races: racesInserted }, 'Meetings ingestion complete')
  return racesInserted
}

// CLI entry point
if (import.meta.url === `file://${process.argv[1]}`) {
  const date = process.argv[2] // optional, defaults to today
  ingestMeetings(date)
    .then((n) => { log.info({ races: n }, 'Done'); pool.end() })
    .catch((e) => { log.error(e); process.exit(1) })
}
