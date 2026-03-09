import { query } from '../../lib/database.js'

export interface ExtendedFeatures {
  // API form stats
  apiCourseWinPct: number | null
  apiCourseDistanceWinPct: number | null
  apiDistanceWinPct: number | null
  apiLast10WinPct: number | null
  // RPR
  lastRpr: number | null
  avgRprLast5: number | null
  // Beaten lengths trend
  beatenLengthsTrend: number | null
  bestBeatenLengths5: number | null
  // Form string
  formStringScore: number | null
  recentWinsCount: number
  // Trainer at venue
  trainerVenueRuns: number | null
  // Sire features
  sireDistanceWinPct: number | null
  sireProgenyCount: number | null
}

function parseFormString(form: string | null): { score: number | null; wins: number } {
  if (!form) return { score: null, wins: 0 }
  const chars = form.trim().slice(-8).split('')
  const weights = [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3]
  let score = 0
  let wins = 0
  for (let i = 0; i < chars.length; i++) {
    const ch = chars[chars.length - 1 - i]
    const w = weights[i] ?? 0.3
    if (ch === '1') { score += 5 * w; wins++ }
    else if (ch === '2') score += 3 * w
    else if (ch === '3') score += 2 * w
    else if (ch >= '4' && ch <= '9') score += 1 * w
  }
  return { score: chars.length > 0 ? score : null, wins }
}

function computeBLTrend(bls: number[]): { trend: number | null; best: number | null } {
  if (bls.length === 0) return { trend: null, best: null }
  if (bls.length < 2) return { trend: null, best: bls[0] }
  // Linear regression slope: negative = improving
  const n = bls.length
  const xMean = (n - 1) / 2
  const yMean = bls.reduce((a, b) => a + b, 0) / n
  let num = 0, den = 0
  for (let i = 0; i < n; i++) {
    num += (i - xMean) * (bls[i] - yMean)
    den += (i - xMean) ** 2
  }
  return { trend: den > 0 ? num / den : 0, best: Math.min(...bls) }
}

export async function computeExtendedFeatures(
  horseId: string,
  formHorseId: string,
  raceId: string,
  raceDate: Date
): Promise<ExtendedFeatures> {
  const dateStr = raceDate.toISOString().split('T')[0]

  // Batch queries in parallel
  const [apiStats, rprResults, blResults, formResult, trainerVenue, sireResult] = await Promise.all([
    // API form stats from runner_form_stats
    query<{ stat_type: string; total: number; first: number }>(
      `SELECT stat_type, total, first FROM runner_form_stats
       WHERE horse_id = $1 AND race_id = $2
       AND stat_type IN ('course', 'course_distance', 'distance', 'last_ten')`,
      [horseId, raceId]
    ),

    // RPR from results table
    query<{ rpr: number }>(
      `SELECT res.rpr FROM results res
       JOIN runners ru ON res.race_id = ru.race_id AND res.horse_id = ru.horse_id
       JOIN races rc ON ru.race_id = rc.race_id
       JOIN meetings m ON rc.meeting_id = m.meeting_id
       WHERE res.horse_id = $1 AND m.meeting_date < $2 AND res.rpr IS NOT NULL
       ORDER BY m.meeting_date DESC LIMIT 5`,
      [formHorseId, dateStr]
    ),

    // Beaten lengths for trend
    query<{ beaten_lengths: number }>(
      `SELECT beaten_lengths FROM horse_form_history
       WHERE horse_id = $1 AND race_date < $2 AND beaten_lengths IS NOT NULL
       ORDER BY race_date DESC LIMIT 5`,
      [formHorseId, dateStr]
    ),

    // Form string from runners
    query<{ form: string | null }>(
      `SELECT form FROM runners WHERE horse_id = $1 AND race_id = $2`,
      [horseId, raceId]
    ),

    // Trainer at venue: get trainer_id and venue_id, then count
    (async () => {
      const runner = await query<{ trainer_id: string | null }>(
        `SELECT trainer_id FROM runners WHERE horse_id = $1 AND race_id = $2`,
        [horseId, raceId]
      )
      const trainerId = runner[0]?.trainer_id
      if (!trainerId) return null

      const race = await query<{ meeting_id: string }>(
        `SELECT meeting_id FROM races WHERE race_id = $1`, [raceId]
      )
      const meeting = await query<{ venue_id: string }>(
        `SELECT venue_id FROM meetings WHERE meeting_id = $1`, [race[0]?.meeting_id]
      )
      const venueId = meeting[0]?.venue_id
      if (!venueId) return null

      const stats = await query<{ total: string }>(
        `SELECT COUNT(*) as total FROM horse_form_history hfh
         JOIN runners ru ON hfh.race_id = ru.race_id AND ru.trainer_id = $1
         WHERE hfh.venue_id = $2 AND hfh.race_date < $3`,
        [trainerId, venueId, dateStr]
      )
      return parseInt(stats[0]?.total ?? '0')
    })(),

    // Sire distance affinity
    (async () => {
      const horse = await query<{ sire_id: string | null }>(
        `SELECT sire_id FROM horses WHERE id = $1`, [horseId]
      )
      const sireId = horse[0]?.sire_id
      if (!sireId) return null

      const race = await query<{ distance_m: number }>(
        `SELECT distance_m FROM races WHERE race_id = $1`, [raceId]
      )
      const dist = race[0]?.distance_m ?? 1400

      // Distance band matching
      const bandLow = dist <= 1200 ? 0 : dist <= 1400 ? 1201 : dist <= 1600 ? 1401 : dist <= 2000 ? 1601 : 2001
      const bandHigh = dist <= 1200 ? 1200 : dist <= 1400 ? 1400 : dist <= 1600 ? 1600 : dist <= 2000 ? 2000 : 9999

      const stats = await query<{ total: string; wins: string }>(
        `SELECT COUNT(*) as total, COUNT(*) FILTER (WHERE res.position = 1) as wins
         FROM results res
         JOIN runners ru ON res.race_id = ru.race_id AND res.horse_id = ru.horse_id
         JOIN horses h ON ru.horse_id = h.id
         JOIN races rc ON ru.race_id = rc.race_id
         WHERE h.sire_id = $1 AND rc.distance_m BETWEEN $2 AND $3
           AND res.position IS NOT NULL`,
        [sireId, bandLow, bandHigh]
      )
      const total = parseInt(stats[0]?.total ?? '0')
      const wins = parseInt(stats[0]?.wins ?? '0')
      return total >= 3 ? { winPct: (wins / total) * 100, count: total } : null
    })(),
  ])

  // Process API stats
  const statMap = new Map(apiStats.map(s => [s.stat_type, s]))
  const apiWinPct = (type: string) => {
    const s = statMap.get(type)
    return s && s.total > 0 ? (s.first / s.total) * 100 : null
  }

  // Process RPR
  const rprs = rprResults.map(r => Number(r.rpr))
  const lastRpr = rprs[0] ?? null
  const avgRprLast5 = rprs.length > 0 ? rprs.reduce((a, b) => a + b, 0) / rprs.length : null

  // Process beaten lengths
  const bls = blResults.map(r => Number(r.beaten_lengths))
  const { trend, best } = computeBLTrend(bls)

  // Process form string
  const { score, wins } = parseFormString(formResult[0]?.form ?? null)

  return {
    apiCourseWinPct: apiWinPct('course'),
    apiCourseDistanceWinPct: apiWinPct('course_distance'),
    apiDistanceWinPct: apiWinPct('distance'),
    apiLast10WinPct: apiWinPct('last_ten'),
    lastRpr,
    avgRprLast5,
    beatenLengthsTrend: trend,
    bestBeatenLengths5: best,
    formStringScore: score,
    recentWinsCount: wins,
    trainerVenueRuns: trainerVenue,
    sireDistanceWinPct: sireResult?.winPct ?? null,
    sireProgenyCount: sireResult?.count ?? null,
  }
}
