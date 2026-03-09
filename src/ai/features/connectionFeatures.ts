import { query } from '../../lib/database.js'

export interface ConnectionFeatures {
  trainerWinPct: number | null
  trainerPlacePct: number | null
  trainerFirstUpWinPct: number | null
  jockeyWinPct: number | null
  jockeyPlacePct: number | null
  comboWinPct: number | null
  sameJockey: number | null        // 1 = same as last run, 0 = changed
  jockeyUpgradeScore: number | null // current jockey win% - last jockey win%
  gearChangeSignal: number         // 1 = gear changed, 0 = no change
  blinkersFirstTime: number        // 1 = blinkers first time
  trainerAtVenueWinPct: number | null
}

export async function computeConnectionFeatures(
  horseId: string,
  formHorseId: string,
  raceId: string,
  raceDate: Date
): Promise<ConnectionFeatures> {
  const dateStr = raceDate.toISOString().split('T')[0]

  // Get runner's jockey and trainer
  const runner = await query<{
    jockey_id: string | null
    trainer_id: string | null
    headgear: string | null
  }>(`SELECT jockey_id, trainer_id, headgear FROM runners WHERE horse_id = $1 AND race_id = $2`, [horseId, raceId])

  const jockeyId = runner[0]?.jockey_id
  const trainerId = runner[0]?.trainer_id
  const currentHeadgear = runner[0]?.headgear ?? ''

  // Trainer stats
  let trainerWinPct: number | null = null
  let trainerPlacePct: number | null = null
  if (trainerId) {
    const ts = await query<{ win_pct: number; place_pct: number }>(`
      SELECT win_pct, place_pct FROM mv_trainer_stats WHERE trainer_id = $1
    `, [trainerId])
    trainerWinPct = ts[0]?.win_pct ?? null
    trainerPlacePct = ts[0]?.place_pct ?? null
  }

  // Trainer first-up win pct
  let trainerFirstUpWinPct: number | null = null
  if (trainerId) {
    const tfs = await query<{ win_pct: number }>(`
      SELECT win_pct FROM mv_trainer_spell_stats
      WHERE trainer_id = $1 AND spell_status = 'first-up'
    `, [trainerId])
    trainerFirstUpWinPct = tfs[0]?.win_pct ?? null
  }

  // Jockey stats
  let jockeyWinPct: number | null = null
  let jockeyPlacePct: number | null = null
  if (jockeyId) {
    const js = await query<{ win_pct: number; place_pct: number }>(`
      SELECT win_pct, place_pct FROM mv_jockey_stats WHERE jockey_id = $1
    `, [jockeyId])
    jockeyWinPct = js[0]?.win_pct ?? null
    jockeyPlacePct = js[0]?.place_pct ?? null
  }

  // Combo stats
  let comboWinPct: number | null = null
  if (trainerId && jockeyId) {
    const cs = await query<{ win_pct: number }>(`
      SELECT win_pct FROM mv_combo_stats
      WHERE trainer_id = $1 AND jockey_id = $2
    `, [trainerId, jockeyId])
    comboWinPct = cs[0]?.win_pct ?? null
  }

  // Same jockey & jockey upgrade score
  let sameJockey: number | null = null
  let jockeyUpgradeScore: number | null = null
  if (jockeyId) {
    const lastJockey = await query<{ jockey_id: string | null }>(`
      SELECT jockey_id FROM horse_form_history
      WHERE horse_id = $1 AND race_date < $2
      ORDER BY race_date DESC LIMIT 1
    `, [formHorseId, dateStr])

    if (lastJockey[0]?.jockey_id) {
      sameJockey = lastJockey[0].jockey_id === jockeyId ? 1 : 0
      if (sameJockey === 0) {
        const oldJs = await query<{ win_pct: number }>(`
          SELECT win_pct FROM mv_jockey_stats WHERE jockey_id = $1
        `, [lastJockey[0].jockey_id])
        const oldWinPct = oldJs[0]?.win_pct ?? 0
        jockeyUpgradeScore = (jockeyWinPct ?? 0) - oldWinPct
      } else {
        jockeyUpgradeScore = 0
      }
    }
  }

  // Gear change
  const lastHeadgear = await query<{ headgear: string | null }>(`
    SELECT headgear FROM horse_form_history
    WHERE horse_id = $1 AND race_date < $2
    ORDER BY race_date DESC LIMIT 1
  `, [formHorseId, dateStr])

  const prevHeadgear = lastHeadgear[0]?.headgear ?? ''
  let gearChangeSignal = 0
  let blinkersFirstTime = 0

  if (currentHeadgear !== prevHeadgear && currentHeadgear) {
    gearChangeSignal = 1
    const hasBlinkers = (h: string) => h.toLowerCase().includes('b')
    if (hasBlinkers(currentHeadgear) && !hasBlinkers(prevHeadgear)) {
      blinkersFirstTime = 1
    }
  }

  // Trainer at venue win pct (use overall trainer win pct as proxy)
  const trainerAtVenueWinPct = trainerWinPct

  return {
    trainerWinPct,
    trainerPlacePct,
    trainerFirstUpWinPct,
    jockeyWinPct,
    jockeyPlacePct,
    comboWinPct,
    sameJockey,
    jockeyUpgradeScore,
    gearChangeSignal,
    blinkersFirstTime,
    trainerAtVenueWinPct,
  }
}
