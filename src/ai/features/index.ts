import { query } from '../../lib/database.js'
import { computeFormFeatures, type FormFeatures } from './formFeatures.js'
import { computeClassWeightFeatures, type ClassWeightFeatures } from './classWeightFeatures.js'
import { computeDistanceTrackFeatures, type DistanceTrackFeatures } from './distanceTrackFeatures.js'
import { computePaceFeatures, type PaceFeatures } from './paceFeatures.js'
import { computeConnectionFeatures, type ConnectionFeatures } from './connectionFeatures.js'
import { computeMarketFeatures, type MarketFeatures } from './marketFeatures.js'
import { computeExtendedFeatures, type ExtendedFeatures } from './extendedFeatures.js'

export interface FeatureVector {
  form: FormFeatures
  classWeight: ClassWeightFeatures
  distanceTrack: DistanceTrackFeatures
  pace: PaceFeatures
  connections: ConnectionFeatures
  market: MarketFeatures
  extended: ExtendedFeatures
}

/**
 * Resolve the canonical horse ID (hrs_*) from an aus ID (hrs_aus_*) via horse_id_map.
 * Returns the canonical ID if mapped, otherwise falls back to the original ID.
 */
async function resolveCanonicalId(horseId: string): Promise<string> {
  if (!horseId.startsWith('hrs_aus_')) return horseId
  const mapped = await query<{ canonical_id: string }>(
    `SELECT canonical_id FROM horse_id_map WHERE aus_id = $1`,
    [horseId]
  )
  return mapped[0]?.canonical_id ?? horseId
}

export async function computeAllFeatures(
  horseId: string,
  raceId: string,
  raceDate: Date
): Promise<FeatureVector> {
  // Resolve canonical ID for historical data lookups (form history, speed figures, sectionals)
  // The original horseId (hrs_aus_*) is still needed for runners table queries
  const formHorseId = await resolveCanonicalId(horseId)

  const [form, classWeight, distanceTrack, pace, connections, market, extended] = await Promise.all([
    computeFormFeatures(horseId, formHorseId, raceId, raceDate),
    computeClassWeightFeatures(horseId, formHorseId, raceId, raceDate),
    computeDistanceTrackFeatures(horseId, formHorseId, raceId, raceDate),
    computePaceFeatures(horseId, formHorseId, raceId, raceDate),
    computeConnectionFeatures(horseId, formHorseId, raceId, raceDate),
    computeMarketFeatures(horseId, raceId),
    computeExtendedFeatures(horseId, formHorseId, raceId, raceDate),
  ])

  return { form, classWeight, distanceTrack, pace, connections, market, extended }
}

// Convert FeatureVector to flat numeric array for ML model input
// Must match config.py FEATURE_NAMES order exactly (78 features)
export function flattenFeatures(fv: FeatureVector): { names: string[]; values: (number | null)[] } {
  const spellMap: Record<string, number> = { 'first-up': 0, 'second-up': 1, 'third-up': 2, fit: 3 }
  const styleMap: Record<string, number> = { leader: 0, 'on-pace': 1, mid: 2, backmarker: 3 }

  const names: string[] = []
  const values: (number | null)[] = []

  // Form & Performance (12)
  names.push('last_speed_figure'); values.push(fv.form.lastSpeedFigure)
  names.push('avg_speed_figure_last5'); values.push(fv.form.avgSpeedFigureLast5)
  names.push('best_speed_figure_last5'); values.push(fv.form.bestSpeedFigureLast5)
  names.push('days_since_last_run'); values.push(fv.form.daysSinceLastRun)
  names.push('consistency_index'); values.push(fv.form.consistencyIndex)
  names.push('form_momentum'); values.push(fv.form.formMomentum)
  names.push('avg_beaten_lengths_5'); values.push(fv.form.avgBeatenLengths5)
  names.push('fitness_score'); values.push(fv.form.fitnessScore)
  names.push('spell_status'); values.push(spellMap[fv.form.spellStatus] ?? 3)
  names.push('benchmark_rating'); values.push(fv.form.benchmarkRating)
  names.push('career_win_pct'); values.push(fv.form.careerWinPct)
  names.push('career_place_pct'); values.push(fv.form.careerPlacePct)

  // Career experience (1)
  names.push('career_starts'); values.push(fv.form.careerStarts)

  // Class & Weight in KG (7)
  names.push('class_numeric'); values.push(fv.classWeight.classNumeric)
  names.push('class_change'); values.push(fv.classWeight.classChange)
  names.push('weight_carried_kg'); values.push(fv.classWeight.weightCarriedKg)
  names.push('weight_change_kg'); values.push(fv.classWeight.weightChangeKg)
  names.push('weight_vs_field_avg'); values.push(fv.classWeight.weightVsFieldAvg)
  names.push('is_apprentice'); values.push(fv.classWeight.isApprentice ? 1 : 0)
  names.push('apprentice_claim'); values.push(fv.classWeight.apprenticeClaim)

  // Distance (3)
  names.push('last_distance_m'); values.push(fv.distanceTrack.lastDistanceM)
  names.push('distance_change'); values.push(fv.distanceTrack.distanceChange)
  names.push('distance_win_pct'); values.push(fv.distanceTrack.distanceWinPct)

  // Track experience (6)
  names.push('track_runs'); values.push(fv.distanceTrack.trackRuns)
  names.push('track_win_pct'); values.push(fv.distanceTrack.trackWinPct)
  names.push('track_distance_win_pct'); values.push(fv.distanceTrack.trackDistanceWinPct)
  names.push('has_track_experience'); values.push(fv.distanceTrack.hasTrackExperience)
  names.push('has_track_win'); values.push(fv.distanceTrack.hasTrackWin)
  names.push('going_win_pct'); values.push(fv.distanceTrack.goingWinPct)

  // Wet track (1)
  names.push('is_wet_track_specialist'); values.push(fv.distanceTrack.isWetTrackSpecialist)

  // Pace & Barrier (5)
  const runningStyleVal = styleMap[fv.pace.runningStyle ?? ''] ?? 2
  names.push('running_style'); values.push(runningStyleVal)
  names.push('barrier_draw'); values.push(fv.pace.barrierDraw)
  names.push('barrier_bias_score'); values.push(fv.pace.barrierBiasScore)
  names.push('field_size'); values.push(fv.pace.fieldSize)
  names.push('leader_count_in_field'); values.push(fv.pace.leaderCountInField)

  // Jockey & Trainer connections (10)
  names.push('trainer_win_pct'); values.push(fv.connections.trainerWinPct)
  names.push('trainer_place_pct'); values.push(fv.connections.trainerPlacePct)
  names.push('trainer_first_up_win_pct'); values.push(fv.connections.trainerFirstUpWinPct)
  names.push('jockey_win_pct'); values.push(fv.connections.jockeyWinPct)
  names.push('jockey_place_pct'); values.push(fv.connections.jockeyPlacePct)
  names.push('combo_win_pct'); values.push(fv.connections.comboWinPct)
  names.push('same_jockey'); values.push(fv.connections.sameJockey)
  names.push('jockey_upgrade_score'); values.push(fv.connections.jockeyUpgradeScore)
  names.push('gear_change_signal'); values.push(fv.connections.gearChangeSignal)
  names.push('blinkers_first_time'); values.push(fv.connections.blinkersFirstTime)

  // Trainer at venue (1)
  names.push('trainer_at_venue_win_pct'); values.push(fv.connections.trainerAtVenueWinPct)

  // Market (8)
  names.push('current_odds'); values.push(fv.market.currentOdds)
  names.push('odds_movement'); values.push(fv.market.oddsMovement)
  names.push('market_implied_prob'); values.push(fv.market.marketImpliedProb)
  names.push('is_favourite'); values.push(fv.market.isFavourite ? 1 : 0)
  names.push('odds_movement_pct'); values.push(fv.market.oddsMovementPct)
  names.push('is_plunge'); values.push(fv.market.isPlunge ? 1 : 0)
  names.push('late_money_steam'); values.push(fv.market.lateMoneySteam)
  names.push('odds_volatility'); values.push(fv.market.oddsVolatility)

  // Interaction & derived features (7)
  const barrierDraw = fv.pace.barrierDraw
  const fieldSize = fv.pace.fieldSize || 1
  names.push('barrier_draw_ratio'); values.push(barrierDraw != null ? barrierDraw / fieldSize : null)

  // speed_vs_field_avg: computed per-race at prediction time (set null here, filled by caller)
  names.push('speed_vs_field_avg'); values.push(null)

  const jockeyWin = fv.connections.jockeyWinPct ?? 0
  const trainerWin = fv.connections.trainerWinPct ?? 0
  names.push('jockey_trainer_interaction'); values.push(jockeyWin * trainerWin / 100)

  const distWinPct = fv.distanceTrack.distanceWinPct
  const classChange = fv.classWeight.classChange
  names.push('class_distance_affinity'); values.push(
    distWinPct != null && classChange != null
      ? distWinPct * (1 + Math.max(-2, Math.min(2, classChange)) / 10)
      : null
  )

  const odds = fv.market.currentOdds
  const avgSpeed = fv.form.avgSpeedFigureLast5
  names.push('odds_x_speed'); values.push(
    odds != null && avgSpeed != null ? odds * avgSpeed : null
  )

  // Recency-weighted speed: approximate with 0.36 * last + 0.64 * avg
  const lastSpeed = fv.form.lastSpeedFigure
  names.push('recency_weighted_speed'); values.push(
    lastSpeed != null
      ? (avgSpeed != null ? 0.36 * lastSpeed + 0.64 * avgSpeed : lastSpeed)
      : null
  )

  // Career stage: 0=debut(0-5), 1=developing(6-15), 2=established(16-30), 3=veteran(31+)
  const starts = fv.form.careerStarts
  names.push('career_stage'); values.push(
    starts <= 5 ? 0 : starts <= 15 ? 1 : starts <= 30 ? 2 : 3
  )

  // Target-encoded categoricals (use approximate values from training data medians)
  // These will be overridden by the model's median imputation if null
  names.push('spell_status_te'); values.push(null)  // filled by model median
  names.push('running_style_te'); values.push(null)  // filled by model median

  // --- v5 NEW FEATURES ---

  // API form stats (4)
  names.push('api_course_win_pct'); values.push(fv.extended.apiCourseWinPct)
  names.push('api_course_distance_win_pct'); values.push(fv.extended.apiCourseDistanceWinPct)
  names.push('api_distance_win_pct'); values.push(fv.extended.apiDistanceWinPct)
  names.push('api_last10_win_pct'); values.push(fv.extended.apiLast10WinPct)

  // RPR features (3) - rpr_vs_field_avg computed per-race by caller
  names.push('last_rpr'); values.push(fv.extended.lastRpr)
  names.push('avg_rpr_last5'); values.push(fv.extended.avgRprLast5)
  names.push('rpr_vs_field_avg'); values.push(null)  // filled per-race by caller

  // Beaten lengths trend (2)
  names.push('beaten_lengths_trend'); values.push(fv.extended.beatenLengthsTrend)
  names.push('best_beaten_lengths_5'); values.push(fv.extended.bestBeatenLengths5)

  // Field strength (2) - computed per-race by caller
  names.push('field_avg_rpr'); values.push(null)  // filled per-race by caller
  names.push('field_strength_rank'); values.push(null)  // filled per-race by caller

  // Form string (2)
  names.push('form_string_score'); values.push(fv.extended.formStringScore)
  names.push('recent_wins_count'); values.push(fv.extended.recentWinsCount)

  // Missing data indicators (3)
  names.push('has_form_history'); values.push(fv.form.careerStarts > 0 ? 1 : 0)
  names.push('has_speed_figure'); values.push(fv.form.lastSpeedFigure != null ? 1 : 0)
  names.push('has_rpr'); values.push(fv.extended.lastRpr != null ? 1 : 0)

  // Trainer at venue (1)
  names.push('trainer_venue_runs'); values.push(fv.extended.trainerVenueRuns)

  // Sire features (2)
  names.push('sire_distance_win_pct'); values.push(fv.extended.sireDistanceWinPct)
  names.push('sire_progeny_count'); values.push(fv.extended.sireProgenyCount)

  return { names, values }
}

// Total: 82 features (78 v5 + 4 v6 odds movement)
export const FEATURE_COUNT = 82
export const FEATURE_NAMES = [
  'last_speed_figure', 'avg_speed_figure_last5', 'best_speed_figure_last5',
  'days_since_last_run', 'consistency_index', 'form_momentum',
  'avg_beaten_lengths_5', 'fitness_score', 'spell_status',
  'benchmark_rating', 'career_win_pct', 'career_place_pct',
  'career_starts',
  'class_numeric', 'class_change',
  'weight_carried_kg', 'weight_change_kg', 'weight_vs_field_avg',
  'is_apprentice', 'apprentice_claim',
  'last_distance_m', 'distance_change', 'distance_win_pct',
  'track_runs', 'track_win_pct', 'track_distance_win_pct',
  'has_track_experience', 'has_track_win',
  'going_win_pct',
  'is_wet_track_specialist',
  'running_style', 'barrier_draw', 'barrier_bias_score',
  'field_size', 'leader_count_in_field',
  'trainer_win_pct', 'trainer_place_pct', 'trainer_first_up_win_pct',
  'jockey_win_pct', 'jockey_place_pct', 'combo_win_pct',
  'same_jockey', 'jockey_upgrade_score',
  'gear_change_signal', 'blinkers_first_time',
  'trainer_at_venue_win_pct',
  'current_odds', 'odds_movement', 'market_implied_prob', 'is_favourite',
  'odds_movement_pct', 'is_plunge', 'late_money_steam', 'odds_volatility',
  'barrier_draw_ratio', 'speed_vs_field_avg', 'jockey_trainer_interaction',
  'class_distance_affinity', 'odds_x_speed', 'recency_weighted_speed',
  'career_stage', 'spell_status_te', 'running_style_te',
  // v5 features
  'api_course_win_pct', 'api_course_distance_win_pct', 'api_distance_win_pct', 'api_last10_win_pct',
  'last_rpr', 'avg_rpr_last5', 'rpr_vs_field_avg',
  'beaten_lengths_trend', 'best_beaten_lengths_5',
  'field_avg_rpr', 'field_strength_rank',
  'form_string_score', 'recent_wins_count',
  'has_form_history', 'has_speed_figure', 'has_rpr',
  'trainer_venue_runs',
  'sire_distance_win_pct', 'sire_progeny_count',
]
