import { query, upsert, pool } from '../lib/database.js'
import pino from 'pino'

const log = pino({ name: 'speed-figures' })

function parseTimeToSeconds(timeStr: string | null): number | null {
  if (!timeStr) return null

  // Handle "1:23.45" or "1m 23.45s" or "83.45"
  const colonMatch = timeStr.match(/^(\d+):(\d+\.?\d*)$/)
  if (colonMatch) {
    return parseInt(colonMatch[1]) * 60 + parseFloat(colonMatch[2])
  }

  const mMatch = timeStr.match(/^(\d+)m\s*(\d+\.?\d*)s?$/)
  if (mMatch) {
    return parseInt(mMatch[1]) * 60 + parseFloat(mMatch[2])
  }

  const secs = parseFloat(timeStr)
  return isNaN(secs) ? null : secs
}

// Median helper
function median(arr: number[]): number {
  const sorted = [...arr].sort((a, b) => a - b)
  const mid = Math.floor(sorted.length / 2)
  return sorted.length % 2 !== 0 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2
}

export async function computeSpeedFigures(): Promise<number> {
  log.info('Computing speed figures from race results')

  // Step 1: Get all results with valid times
  const results = await query<{
    horse_id: string
    race_id: string
    race_time: string
    distance_m: number
    going: string
    meeting_date: string
    venue_id: string
    position: number
  }>(`
    SELECT
      res.horse_id,
      res.race_id,
      res.race_time,
      rc.distance_m,
      COALESCE(rc.going, 'unknown') AS going,
      m.meeting_date,
      m.venue_id,
      res.position
    FROM results res
    JOIN races rc ON res.race_id = rc.race_id
    JOIN meetings m ON rc.meeting_id = m.meeting_id
    WHERE res.race_time IS NOT NULL
      AND rc.distance_m IS NOT NULL
      AND rc.distance_m > 0
    ORDER BY m.venue_id, rc.distance_m, rc.going
  `)

  log.info({ totalResults: results.length }, 'Results with times found')

  // Step 2: Parse times and group by venue + distance_bucket + going
  type TimedResult = typeof results[number] & { timeSecs: number }
  const validResults: TimedResult[] = []

  for (const r of results) {
    const timeSecs = parseTimeToSeconds(r.race_time)
    if (timeSecs && timeSecs > 10 && timeSecs < 300) {
      validResults.push({ ...r, timeSecs })
    }
  }

  log.info({ validTimes: validResults.length }, 'Valid times parsed')

  // Group by venue + distance bucket (round to nearest 100m) + going
  const groups = new Map<string, TimedResult[]>()
  for (const r of validResults) {
    const distBucket = Math.round(r.distance_m / 100) * 100
    const key = `${r.venue_id}|${distBucket}|${r.going}`
    if (!groups.has(key)) groups.set(key, [])
    groups.get(key)!.push(r)
  }

  // Step 3: Calculate par times (median time for winner at each group)
  const parTimes = new Map<string, number>()
  for (const [key, runs] of groups) {
    const winnerTimes = runs.filter((r) => r.position === 1).map((r) => r.timeSecs)
    if (winnerTimes.length >= 3) {
      parTimes.set(key, median(winnerTimes))
    }
  }

  log.info({ parTimeGroups: parTimes.size }, 'Par times calculated')

  // Step 4: Calculate track variant per race day and compute speed figures
  // Group by race_id first
  const raceGroups = new Map<string, TimedResult[]>()
  for (const r of validResults) {
    if (!raceGroups.has(r.race_id)) raceGroups.set(r.race_id, [])
    raceGroups.get(r.race_id)!.push(r)
  }

  let computed = 0
  for (const [raceId, raceRunners] of raceGroups) {
    const first = raceRunners[0]
    const distBucket = Math.round(first.distance_m / 100) * 100
    const parKey = `${first.venue_id}|${distBucket}|${first.going}`
    const parTime = parTimes.get(parKey)

    if (!parTime) continue

    // Track variant: how much faster/slower was the winner compared to par
    const winnerTime = raceRunners.find((r) => r.position === 1)?.timeSecs
    const trackVariant = winnerTime ? winnerTime - parTime : 0

    for (const r of raceRunners) {
      // Speed figure: par difference adjusted for track variant
      // Higher = faster = better
      // Scale: 100 = par, each second = ~5 points, capped 0-130
      const rawDiff = parTime - r.timeSecs // positive = faster than par
      const adjusted = rawDiff - trackVariant // remove track-speed bias
      const speedFigure = Math.max(0, Math.min(130, 100 + adjusted * 5))

      await upsert(
        'speed_figures',
        ['horse_id', 'race_id', 'raw_time_secs', 'distance_m', 'going', 'track_variant', 'adjusted_speed_figure', 'par_time_secs'],
        [r.horse_id, r.race_id, r.timeSecs, r.distance_m, r.going, trackVariant, speedFigure, parTime],
        ['horse_id', 'race_id']
      )
      computed++
    }
  }

  log.info({ speedFigures: computed }, 'Speed figures computed')
  return computed
}

// CLI entry
if (import.meta.url === `file://${process.argv[1]}`) {
  computeSpeedFigures()
    .then((n) => { log.info({ computed: n }, 'Done'); pool.end() })
    .catch((e) => { log.error(e); process.exit(1) })
}
