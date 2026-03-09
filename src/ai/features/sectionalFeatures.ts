/**
 * Sectional speed features derived from punters.com.au scraper data.
 * 8 features capturing closing speed, acceleration, consistency.
 */

import { query } from '../../lib/database.js'

export interface SectionalFeatures {
  lastFinishSpeed: number | null
  avgFinishSpeedLast3: number | null
  lastAcceleration: number | null
  avgAcceleration: number | null
  closingSpeedRank: number | null
  earlySpeedRank: number | null
  sectionalConsistency: number | null
  finishSpeedVsFieldAvg: number | null
}

interface SectionalRow {
  speed_800m: number | null
  speed_200m: number | null
  speed_finish: number | null
  speed_avg: number | null
  race_id: string
}

export async function computeSectionalFeatures(
  horseId: string,
  raceId: string,
  raceDate: Date
): Promise<SectionalFeatures> {
  const dateStr = raceDate.toISOString().split('T')[0]

  // Get horse's historical sectionals (from past races only)
  const history = await query<SectionalRow>(`
    SELECT st.speed_800m::float, st.speed_200m::float, st.speed_finish::float,
           st.speed_avg::float, st.race_id
    FROM sectional_times st
    JOIN races r ON r.race_id = st.race_id
    JOIN meetings m ON m.meeting_id = r.meeting_id
    WHERE st.horse_id = $1
      AND m.meeting_date < $2
      AND st.speed_finish IS NOT NULL
    ORDER BY m.meeting_date DESC
    LIMIT 5
  `, [horseId, dateStr])

  const defaults: SectionalFeatures = {
    lastFinishSpeed: null,
    avgFinishSpeedLast3: null,
    lastAcceleration: null,
    avgAcceleration: null,
    closingSpeedRank: null,
    earlySpeedRank: null,
    sectionalConsistency: null,
    finishSpeedVsFieldAvg: null,
  }

  if (history.length === 0) return defaults

  // Last finish speed
  const lastFinishSpeed = history[0].speed_finish

  // Avg finish speed last 3
  const finishSpeeds = history.slice(0, 3).map(h => h.speed_finish).filter((v): v is number => v !== null)
  const avgFinishSpeedLast3 = finishSpeeds.length > 0
    ? finishSpeeds.reduce((a, b) => a + b, 0) / finishSpeeds.length
    : null

  // Acceleration: speed_200m - speed_800m (positive = strong closer)
  const accel = (h: SectionalRow) =>
    h.speed_200m !== null && h.speed_800m !== null ? h.speed_200m - h.speed_800m : null

  const lastAcceleration = accel(history[0])

  const accels = history.slice(0, 3).map(accel).filter((v): v is number => v !== null)
  const avgAcceleration = accels.length > 0
    ? accels.reduce((a, b) => a + b, 0) / accels.length
    : null

  // Sectional consistency: stddev of finish speeds (lower = more consistent)
  let sectionalConsistency: number | null = null
  if (finishSpeeds.length >= 2) {
    const mean = finishSpeeds.reduce((a, b) => a + b, 0) / finishSpeeds.length
    const variance = finishSpeeds.reduce((sum, v) => sum + (v - mean) ** 2, 0) / finishSpeeds.length
    sectionalConsistency = Math.sqrt(variance)
  }

  // Closing speed rank and early speed rank from most recent race
  // Compare against field in that race
  const lastRaceId = history[0].race_id
  const fieldSectionals = await query<{
    horse_id: string | null
    speed_finish: number | null
    speed_800m: number | null
  }>(`
    SELECT horse_id, speed_finish::float, speed_800m::float
    FROM sectional_times
    WHERE race_id = $1 AND speed_finish IS NOT NULL
  `, [lastRaceId])

  let closingSpeedRank: number | null = null
  let earlySpeedRank: number | null = null
  let finishSpeedVsFieldAvg: number | null = null

  if (fieldSectionals.length > 1 && lastFinishSpeed !== null) {
    // Closing speed rank (1 = fastest finisher)
    const finishRanked = fieldSectionals
      .filter(f => f.speed_finish !== null)
      .sort((a, b) => (b.speed_finish ?? 0) - (a.speed_finish ?? 0))
    const closingIdx = finishRanked.findIndex(f => f.horse_id === horseId)
    closingSpeedRank = closingIdx >= 0 ? closingIdx + 1 : null

    // Early speed rank (1 = fastest at 800m)
    const earlyRanked = fieldSectionals
      .filter(f => f.speed_800m !== null)
      .sort((a, b) => (b.speed_800m ?? 0) - (a.speed_800m ?? 0))
    const earlyIdx = earlyRanked.findIndex(f => f.horse_id === horseId)
    earlySpeedRank = earlyIdx >= 0 ? earlyIdx + 1 : null

    // Finish speed vs field average
    const fieldFinish = fieldSectionals
      .map(f => f.speed_finish)
      .filter((v): v is number => v !== null)
    if (fieldFinish.length > 0) {
      const avg = fieldFinish.reduce((a, b) => a + b, 0) / fieldFinish.length
      finishSpeedVsFieldAvg = lastFinishSpeed - avg
    }
  }

  return {
    lastFinishSpeed,
    avgFinishSpeedLast3,
    lastAcceleration,
    avgAcceleration,
    closingSpeedRank,
    earlySpeedRank,
    sectionalConsistency,
    finishSpeedVsFieldAvg,
  }
}
