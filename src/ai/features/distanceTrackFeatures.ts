import { query } from '../../lib/database.js'

export interface DistanceTrackFeatures {
  lastDistanceM: number | null
  distanceChange: number | null
  distanceWinPct: number | null
  trackRuns: number
  trackWinPct: number | null
  trackDistanceWinPct: number | null
  hasTrackExperience: number
  hasTrackWin: number
  goingWinPct: number | null
  isWetTrackSpecialist: number | null
}

function statWinPct(total: number, first: number): number | null {
  return total > 0 ? (first / total) * 100 : null
}

export async function computeDistanceTrackFeatures(
  horseId: string,
  formHorseId: string,
  raceId: string,
  raceDate: Date
): Promise<DistanceTrackFeatures> {
  const dateStr = raceDate.toISOString().split('T')[0]

  // Get current race info
  const race = await query<{ distance_m: number | null; going: string | null; meeting_id: string }>(`
    SELECT distance_m, going, meeting_id FROM races WHERE race_id = $1
  `, [raceId])

  const distanceM = race[0]?.distance_m ?? null
  const going = race[0]?.going ?? null
  const meetingId = race[0]?.meeting_id

  // Get venue_id
  const meeting = await query<{ venue_id: string }>(`
    SELECT venue_id FROM meetings WHERE meeting_id = $1
  `, [meetingId])
  const venueId = meeting[0]?.venue_id

  // Last distance from form history (canonical ID)
  const lastDist = await query<{ distance_m: number | null }>(`
    SELECT distance_m FROM horse_form_history
    WHERE horse_id = $1 AND race_date < $2
    ORDER BY race_date DESC LIMIT 1
  `, [formHorseId, dateStr])
  const lastDistanceM = lastDist[0]?.distance_m ?? null
  const distanceChange = distanceM !== null && lastDistanceM !== null
    ? distanceM - lastDistanceM
    : null

  // Distance win pct from form history
  let distanceWinPct: number | null = null
  if (distanceM !== null) {
    const distStats = await query<{ total: number; wins: number }>(`
      SELECT COUNT(*) as total, COUNT(*) FILTER (WHERE position = 1) as wins
      FROM horse_form_history
      WHERE horse_id = $1 AND race_date < $2 AND ABS(distance_m - $3) <= 100
    `, [formHorseId, dateStr, distanceM])
    if (distStats[0]?.total >= 2) {
      distanceWinPct = statWinPct(distStats[0].total, distStats[0].wins)
    }
  }

  // Track experience
  let trackRuns = 0, trackWins = 0, trackDistRuns = 0, trackDistWins = 0
  if (venueId) {
    const trackStats = await query<{
      runs: number; wins: number; dist_runs: number; dist_wins: number
    }>(`
      SELECT
        COUNT(*) as runs,
        COUNT(*) FILTER (WHERE position = 1) as wins,
        COUNT(*) FILTER (WHERE ABS(distance_m - $3) <= 100) as dist_runs,
        COUNT(*) FILTER (WHERE position = 1 AND ABS(distance_m - $3) <= 100) as dist_wins
      FROM horse_form_history
      WHERE horse_id = $1 AND venue_id = $4 AND race_date < $2
    `, [formHorseId, dateStr, distanceM ?? 0, venueId])

    trackRuns = trackStats[0]?.runs ?? 0
    trackWins = trackStats[0]?.wins ?? 0
    trackDistRuns = trackStats[0]?.dist_runs ?? 0
    trackDistWins = trackStats[0]?.dist_wins ?? 0
  }

  const trackWinPct = trackRuns >= 2 ? statWinPct(trackRuns, trackWins) : null
  const trackDistanceWinPct = trackDistRuns >= 2 ? statWinPct(trackDistRuns, trackDistWins) : null

  // Going win pct from form history
  let goingWinPct: number | null = null
  if (going) {
    const goingStats = await query<{ total: number; wins: number }>(`
      SELECT COUNT(*) as total, COUNT(*) FILTER (WHERE position = 1) as wins
      FROM horse_form_history
      WHERE horse_id = $1 AND race_date < $2 AND LOWER(going) = LOWER($3)
    `, [formHorseId, dateStr, going])
    if (goingStats[0]?.total >= 2) {
      goingWinPct = statWinPct(goingStats[0].total, goingStats[0].wins)
    }
  }

  // Wet track specialist from form history
  let isWetTrackSpecialist: number | null = null
  const wetStats = await query<{
    wet_total: number; wet_wins: number; dry_total: number; dry_wins: number
  }>(`
    SELECT
      COUNT(*) FILTER (WHERE LOWER(going) IN ('soft','heavy','soft to heavy','very soft')) as wet_total,
      COUNT(*) FILTER (WHERE position = 1 AND LOWER(going) IN ('soft','heavy','soft to heavy','very soft')) as wet_wins,
      COUNT(*) FILTER (WHERE LOWER(going) IN ('good','good to firm','firm')) as dry_total,
      COUNT(*) FILTER (WHERE position = 1 AND LOWER(going) IN ('good','good to firm','firm')) as dry_wins
    FROM horse_form_history
    WHERE horse_id = $1 AND race_date < $2
    HAVING COUNT(*) >= 3
  `, [formHorseId, dateStr])

  if (wetStats[0]) {
    const wetPct = wetStats[0].wet_total > 0 ? (wetStats[0].wet_wins / wetStats[0].wet_total) * 100 : 0
    const dryPct = wetStats[0].dry_total > 0 ? (wetStats[0].dry_wins / wetStats[0].dry_total) * 100 : 0
    isWetTrackSpecialist = wetPct - dryPct
  }

  return {
    lastDistanceM,
    distanceChange,
    distanceWinPct,
    trackRuns,
    trackWinPct,
    trackDistanceWinPct,
    hasTrackExperience: trackRuns > 0 ? 1 : 0,
    hasTrackWin: trackWins > 0 ? 1 : 0,
    goingWinPct,
    isWetTrackSpecialist,
  }
}
