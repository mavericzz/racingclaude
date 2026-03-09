import { query } from '../../lib/database.js'

export interface PaceFeatures {
  runningStyle: string | null     // mode of last 5 running styles
  barrierDraw: number | null
  barrierBiasScore: number | null // deviation from average at this venue/distance
  fieldSize: number
  leaderCountInField: number      // how many frontrunners in the race
}

export async function computePaceFeatures(
  horseId: string,
  formHorseId: string,
  raceId: string,
  raceDate: Date
): Promise<PaceFeatures> {
  // Get runner draw and race info
  const runnerInfo = await query<{ draw: number | null }>(`
    SELECT draw FROM runners WHERE horse_id = $1 AND race_id = $2
  `, [horseId, raceId])

  const raceInfo = await query<{ distance_m: number | null; field_size: number | null; meeting_id: string }>(`
    SELECT distance_m, field_size, meeting_id FROM races WHERE race_id = $1
  `, [raceId])

  const barrierDraw = runnerInfo[0]?.draw ?? null
  const fieldSize = raceInfo[0]?.field_size ?? 10
  const distanceM = raceInfo[0]?.distance_m ?? 1400

  // Running style: mode of last 5 classified runs
  const styles = await query<{ running_style: string }>(`
    SELECT running_style FROM horse_form_history
    WHERE horse_id = $1 AND race_date < $2 AND running_style IS NOT NULL
    ORDER BY race_date DESC LIMIT 5
  `, [formHorseId, raceDate.toISOString().split('T')[0]])

  let runningStyle: string | null = null
  if (styles.length > 0) {
    const counts = new Map<string, number>()
    for (const s of styles) {
      counts.set(s.running_style, (counts.get(s.running_style) ?? 0) + 1)
    }
    let maxCount = 0
    for (const [style, count] of counts) {
      if (count > maxCount) {
        maxCount = count
        runningStyle = style
      }
    }
  }

  // Barrier bias score from materialized view
  let barrierBiasScore: number | null = null
  if (barrierDraw !== null) {
    const venueId = await query<{ venue_id: string }>(`
      SELECT venue_id FROM meetings WHERE meeting_id = $1
    `, [raceInfo[0]?.meeting_id])

    if (venueId[0]) {
      const distBucket = distanceM <= 1100 ? 'sprint'
        : distanceM <= 1400 ? 'short'
        : distanceM <= 1800 ? 'mile'
        : distanceM <= 2200 ? 'middle'
        : 'staying'

      const barrierGroup = barrierDraw <= 4 ? 'inside'
        : barrierDraw <= 8 ? 'middle'
        : 'outside'

      const bias = await query<{ win_pct: number }>(`
        SELECT win_pct FROM mv_barrier_stats
        WHERE venue_id = $1 AND distance_bucket = $2 AND barrier_group = $3
      `, [venueId[0].venue_id, distBucket, barrierGroup])

      const avgBias = await query<{ avg_pct: number }>(`
        SELECT AVG(win_pct) AS avg_pct FROM mv_barrier_stats
        WHERE venue_id = $1 AND distance_bucket = $2
      `, [venueId[0].venue_id, distBucket])

      if (bias[0] && avgBias[0]) {
        barrierBiasScore = bias[0].win_pct - avgBias[0].avg_pct
      }
    }
  }

  // Count frontrunners in the field (resolve canonical IDs via horse_id_map)
  const leaders = await query<{ cnt: string }>(`
    SELECT COUNT(*) AS cnt
    FROM runners ru
    LEFT JOIN horse_id_map him ON ru.horse_id = him.aus_id
    JOIN horse_form_history hfh ON COALESCE(him.canonical_id, ru.horse_id) = hfh.horse_id
    WHERE ru.race_id = $1
      AND ru.scratched = FALSE
      AND hfh.running_style = 'leader'
      AND hfh.race_date = (
        SELECT MAX(hfh2.race_date) FROM horse_form_history hfh2
        WHERE hfh2.horse_id = COALESCE(him.canonical_id, ru.horse_id) AND hfh2.race_date < $2
      )
  `, [raceId, raceDate.toISOString().split('T')[0]])

  const leaderCountInField = parseInt(leaders[0]?.cnt ?? '0')

  return {
    runningStyle,
    barrierDraw,
    barrierBiasScore,
    fieldSize,
    leaderCountInField,
  }
}
