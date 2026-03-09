import { query } from '../../lib/database.js'

export interface ClassWeightFeatures {
  classNumeric: number
  classChange: number | null
  weightCarriedKg: number | null
  weightChangeKg: number | null
  weightVsFieldAvg: number | null
  isApprentice: boolean
  apprenticeClaim: number
}

// Normalize weight to kg regardless of source unit
function normalizeWeightKg(w: number | null): number | null {
  if (w === null || w === undefined) return null
  if (w < 15) return w * 6.35029       // stone
  if (w < 75) return w                  // already kg
  if (w < 150) return w * 0.453592     // lbs
  return null
}

// Australian class ladder
const CLASS_LADDER: Record<string, number> = {
  'mdn': 1, 'maiden': 1, 'mdn-sw': 1, 'mdn sw': 1,
  '2y mdn': 1, '3y mdn': 1, '2y mdn-sw': 1, '3y mdn-sw': 1,
  'cl1': 2, 'class 1': 2, 'rst': 2,
  'cl2': 3, 'class 2': 3,
  'cl3': 4, 'class 3': 4,
  'cl4': 5, 'class 4': 5,
  'cl5': 6, 'class 5': 6,
  'cl6': 7, 'class 6': 7,
  'bm50': 2.5, 'bm54': 2.8, 'bm58': 3.0,
  'bm60': 3.2, 'bm64': 3.5, 'bm66': 3.8,
  'bm68': 4.0, 'bm70': 4.2, 'bm72': 4.5,
  'bm74': 4.8, 'bm76': 5.0, 'bm78': 5.5,
  'bm80': 6.0, 'bm82': 6.5, 'bm84': 7.0,
  'bm86': 7.5, 'bm88': 8.0, 'bm90': 8.5,
  'listed': 9, 'list': 9,
  'g3': 10, 'group 3': 10,
  'g2': 11, 'group 2': 11,
  'g1': 12, 'group 1': 12,
  'open': 8, 'hcp': 5, 'wfa': 8,
  'quality': 7, 'stakes': 9,
}

export function parseClassNumeric(classStr: string | null): number {
  if (!classStr) return 5
  const lower = classStr.toLowerCase().trim()
  if (CLASS_LADDER[lower] !== undefined) return CLASS_LADDER[lower]
  for (const [key, val] of Object.entries(CLASS_LADDER)) {
    if (lower.includes(key)) return val
  }
  const bmMatch = lower.match(/bm\s*(\d+)/)
  if (bmMatch) {
    const num = parseInt(bmMatch[1])
    return Math.min(8.5, Math.max(2.5, num / 10))
  }
  return 5
}

export async function computeClassWeightFeatures(
  horseId: string,
  formHorseId: string,
  raceId: string,
  raceDate: Date
): Promise<ClassWeightFeatures> {
  const dateStr = raceDate.toISOString().split('T')[0]

  const race = await query<{ class: string | null }>(`
    SELECT class FROM races WHERE race_id = $1
  `, [raceId])
  const currentClass = parseClassNumeric(race[0]?.class ?? null)

  const lastRace = await query<{ class: string | null }>(`
    SELECT hfh.class
    FROM horse_form_history hfh
    WHERE hfh.horse_id = $1 AND hfh.race_date < $2
    ORDER BY hfh.race_date DESC LIMIT 1
  `, [formHorseId, dateStr])
  const lastClass = lastRace.length > 0 ? parseClassNumeric(lastRace[0].class) : null
  const classChange = lastClass !== null ? currentClass - lastClass : null

  // Current runner weight + claim (aus ID for runners table)
  const runner = await query<{ weight_lbs: number | null; jockey_claim: number }>(`
    SELECT weight_lbs, jockey_claim FROM runners WHERE horse_id = $1 AND race_id = $2
  `, [horseId, raceId])

  const rawWeightKg = normalizeWeightKg(runner[0]?.weight_lbs ?? null)
  const claim = runner[0]?.jockey_claim ?? 0
  // AU claims are in kg
  const weightCarriedKg = rawWeightKg !== null ? rawWeightKg - claim : null

  // Last run weight (canonical ID for form history)
  const lastWeight = await query<{ weight_carried: number | null }>(`
    SELECT weight_carried FROM horse_form_history
    WHERE horse_id = $1 AND race_date < $2
    ORDER BY race_date DESC LIMIT 1
  `, [formHorseId, dateStr])
  const lastWeightKg = normalizeWeightKg(lastWeight[0]?.weight_carried ?? null)
  const weightChangeKg = rawWeightKg !== null && lastWeightKg !== null
    ? rawWeightKg - lastWeightKg
    : null

  // Field average weight (normalized)
  const fieldWeights = await query<{ weight_lbs: number | null; jockey_claim: number }>(`
    SELECT weight_lbs, jockey_claim FROM runners
    WHERE race_id = $1 AND scratched = FALSE
  `, [raceId])

  let weightVsFieldAvg: number | null = null
  if (weightCarriedKg !== null && fieldWeights.length > 0) {
    const fieldKg = fieldWeights
      .map(r => {
        const kg = normalizeWeightKg(r.weight_lbs)
        return kg !== null ? kg - (r.jockey_claim ?? 0) : null
      })
      .filter((v): v is number => v !== null)
    if (fieldKg.length > 0) {
      const avg = fieldKg.reduce((a, b) => a + b, 0) / fieldKg.length
      weightVsFieldAvg = weightCarriedKg - avg
    }
  }

  return {
    classNumeric: currentClass,
    classChange,
    weightCarriedKg,
    weightChangeKg,
    weightVsFieldAvg,
    isApprentice: claim > 0,
    apprenticeClaim: claim,
  }
}
