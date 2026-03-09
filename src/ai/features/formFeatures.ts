import { query } from '../../lib/database.js'

export interface FormFeatures {
  lastSpeedFigure: number | null
  avgSpeedFigureLast5: number | null
  bestSpeedFigureLast5: number | null
  daysSinceLastRun: number | null
  consistencyIndex: number | null
  formMomentum: number | null
  avgBeatenLengths5: number | null
  fitnessScore: number
  spellStatus: 'first-up' | 'second-up' | 'third-up' | 'fit'
  benchmarkRating: number | null
  careerWinPct: number | null
  careerPlacePct: number | null
  careerStarts: number
}

export async function computeFormFeatures(
  horseId: string,
  formHorseId: string,
  raceId: string,
  raceDate: Date
): Promise<FormFeatures> {
  const dateStr = raceDate.toISOString().split('T')[0]

  // Get last 10 form entries for this horse prior to this race
  // Use formHorseId (canonical hrs_*) for historical data
  const history = await query<{
    position: number | null
    beaten_lengths: number | null
    field_size: number | null
    days_since_prev_run: number | null
    race_date: string
  }>(`
    SELECT position, beaten_lengths, field_size, days_since_prev_run, race_date
    FROM horse_form_history
    WHERE horse_id = $1 AND race_date < $2
    ORDER BY race_date DESC
    LIMIT 10
  `, [formHorseId, dateStr])

  // Get last 5 speed figures (canonical ID)
  const speedFigs = await query<{ adjusted_speed_figure: number }>(`
    SELECT sf.adjusted_speed_figure
    FROM speed_figures sf
    JOIN races rc ON sf.race_id = rc.race_id
    JOIN meetings m ON rc.meeting_id = m.meeting_id
    WHERE sf.horse_id = $1 AND m.meeting_date < $2
    ORDER BY m.meeting_date DESC
    LIMIT 5
  `, [formHorseId, dateStr])

  // Get runner's benchmark rating (aus ID for runners table)
  const runner = await query<{ rating: number | null }>(`
    SELECT rating FROM runners WHERE horse_id = $1 AND race_id = $2
  `, [horseId, raceId])

  // Get career stats (canonical ID for form history)
  const careerStats = await query<{ total: number; wins: number; places: number }>(`
    SELECT COUNT(*) as total,
           COUNT(*) FILTER (WHERE position = 1) as wins,
           COUNT(*) FILTER (WHERE position <= 3) as places
    FROM horse_form_history
    WHERE horse_id = $1 AND race_date < $2
  `, [formHorseId, dateStr])

  // Compute features (cast from PostgreSQL DECIMAL strings to numbers)
  const figs = speedFigs.map(s => Number(s.adjusted_speed_figure))
  const lastSpeedFigure = figs[0] ?? null
  const avgSpeedFigureLast5 = figs.length > 0 ? figs.reduce((a, b) => a + b, 0) / figs.length : null
  const bestSpeedFigureLast5 = figs.length > 0 ? Math.max(...figs) : null

  // Days since last run
  let daysSinceLastRun: number | null = null
  if (history.length > 0) {
    const lastRunDate = new Date(history[0].race_date)
    daysSinceLastRun = Math.round((raceDate.getTime() - lastRunDate.getTime()) / (1000 * 60 * 60 * 24))
  }

  // Consistency (cast from PostgreSQL integers which come as strings in some drivers)
  let consistencyIndex: number | null = null
  if (history.length >= 3) {
    const positions = history.filter(h => h.position !== null).map(h => Number(h.position))
    if (positions.length >= 3) {
      const mean = positions.reduce((a, b) => a + b, 0) / positions.length
      const variance = positions.reduce((a, b) => a + (b - mean) ** 2, 0) / positions.length
      const avgFieldSize = history.reduce((a, b) => a + Number(b.field_size ?? 10), 0) / history.length
      consistencyIndex = Math.max(0, 1 - Math.sqrt(variance) / avgFieldSize)
    }
  }

  // Form momentum
  let formMomentum: number | null = null
  if (figs.length >= 3) {
    const reversed = [...figs].reverse()
    const n = reversed.length
    const xMean = (n - 1) / 2
    const yMean = reversed.reduce((a, b) => a + b, 0) / n
    let num = 0, den = 0
    for (let i = 0; i < n; i++) {
      num += (i - xMean) * (reversed[i] - yMean)
      den += (i - xMean) ** 2
    }
    formMomentum = den > 0 ? num / den : 0
  }

  // Average beaten lengths (cast from PostgreSQL DECIMAL)
  const bl = history.slice(0, 5).filter(h => h.beaten_lengths !== null).map(h => Number(h.beaten_lengths))
  const avgBeatenLengths5 = bl.length > 0 ? bl.reduce((a, b) => a + b, 0) / bl.length : null

  // Fitness
  const ninetyDaysAgo = new Date(raceDate.getTime() - 90 * 24 * 60 * 60 * 1000)
  const recentRuns = history.filter(h => new Date(h.race_date) >= ninetyDaysAgo)
  const fitnessScore = recentRuns.length

  // Spell status
  let spellStatus: FormFeatures['spellStatus'] = 'fit'
  if (daysSinceLastRun === null || daysSinceLastRun > 60) {
    spellStatus = 'first-up'
  } else if (history.length >= 1 && history[0].days_since_prev_run !== null && history[0].days_since_prev_run > 60) {
    spellStatus = 'second-up'
  } else if (history.length >= 2 && history[1]?.days_since_prev_run !== null && history[1].days_since_prev_run! > 60) {
    spellStatus = 'third-up'
  }

  // Career stats
  const career = careerStats[0]
  const careerStarts = career?.total ?? 0
  const careerWinPct = careerStarts > 0 ? (career.wins / careerStarts) * 100 : null
  const careerPlacePct = careerStarts > 0 ? (career.places / careerStarts) * 100 : null

  return {
    lastSpeedFigure,
    avgSpeedFigureLast5,
    bestSpeedFigureLast5,
    daysSinceLastRun,
    consistencyIndex,
    formMomentum,
    avgBeatenLengths5,
    fitnessScore,
    spellStatus,
    benchmarkRating: runner[0]?.rating ?? null,
    careerWinPct,
    careerPlacePct,
    careerStarts,
  }
}
