/**
 * API client for the frontend.
 */

const BASE = '/api'

async function fetchJson<T>(url: string): Promise<T> {
  const res = await fetch(`${BASE}${url}`)
  if (!res.ok) throw new Error(`API ${res.status}: ${res.statusText}`)
  return res.json()
}

export interface RaceRunner {
  horse_id: string
  horse: string
  number: number | null
  draw: number | null
  sp_decimal: number | null
  jockey_name: string | null
}

export interface Race {
  race_id: string
  race_number: number | null
  race_name: string
  distance_m: number | null
  class: string | null
  going: string | null
  off_time: string | null
  field_size: number
  prize_total: number | null
  runners: RaceRunner[]
  winner: { horseName: string; sp: number | null } | null
  topPick: { horseName: string; winProb: number; verdict: string } | null
  valueBetCount: number
}

export interface Meeting {
  meeting_id: string
  venue_name: string
  state: string | null
  meeting_date: string
  weather: string | null
  track_condition: string | null
  races: Race[]
}

export interface Runner {
  horse_id: string
  horse: string
  number: number | null
  draw: number | null
  weight_lbs: number | null
  jockey_claim: number | null
  rating: number | null
  sp_decimal: number | null
  form: string | null
  headgear: string | null
  jockey_name: string | null
  trainer_name: string | null
  scratched: boolean
  result: { position: number; sp_decimal: number; beaten_lengths: number } | null
  lastSpeedFig: number | null
  avgSpeedFig: number | null
  careerRuns: number | null
  careerWins: number | null
  jockeyWinPct: number | null
  trainerWinPct: number | null
}

export interface RaceDetail extends Omit<Race, 'runners'> {
  meeting_id: string
  venue_name: string
  state: string | null
  meeting_date: string
  runners: Runner[]
}

export interface Prediction {
  horseId: string
  horseName: string
  rank: number
  winProb: number
  marketOdds: number
  fairOdds: number
  edgePct: number
  kellyFraction: number
  recommendedStake: number
  verdict: string
}

export interface PredictionResponse {
  raceId: string
  predictions: Prediction[]
  valueBets: Prediction[]
}

export interface DateCount {
  meeting_date: string
  count: number
}

export interface SectionalEntry {
  horse_name: string
  horse_id: string | null
  horse_number: number
  barrier: number | null
  weight_kg: number | null
  weight_diff_kg: number | null
  speed_800m: number | null
  speed_600m: number | null
  speed_400m: number | null
  speed_200m: number | null
  speed_finish: number | null
  speed_avg: number | null
  adj_speed_800m: number | null
  adj_speed_600m: number | null
  adj_speed_400m: number | null
  adj_speed_200m: number | null
  adj_speed_finish: number | null
  adj_speed_avg: number | null
  scraper_odds: number | null
}

export interface SectionalResponse {
  raceId: string
  raceContext: {
    distance_m: number | null
    going: string | null
    class: string | null
    venue: string
    trackCondition: string | null
  } | null
  avgWeight: number | null
  sectionals: SectionalEntry[]
  fieldAverages: Record<string, number | null>
  adjFieldAverages: Record<string, number | null>
}

export interface RaceAnalysisResponse {
  raceId: string
  analysis: {
    top_pick_position: number | null
    top_pick_won: boolean
    value_bets_count: number
    value_bets_won: number
    total_staked: number
    total_return: number
    race_pnl: number
    pace_scenario: string | null
  } | null
  predictions: Array<{
    horse_id: string
    predicted_win_prob: number
    predicted_rank: number
    market_odds: number
    edge_pct: number
    verdict: string
    actual_position: number | null
    prediction_correct: boolean | null
    value_bet_correct: boolean | null
    profit_loss: number | null
    jockey_changed: boolean | null
    weight_changed_kg: number | null
    distance_changed_m: number | null
    class_changed: boolean | null
    going_changed: boolean | null
  }>
}

export interface AnalysisSummary {
  summary: {
    total_races: number
    top_pick_wins: number
    top_pick_win_rate: number
    value_bets_total: number
    value_bets_won: number
    value_bet_strike_rate: number
    total_staked: number
    total_return: number
    cumulative_pnl: number
    roi_pct: number
  } | null
  recentRaces: Array<{
    race_id: string
    race_name: string
    race_number: number
    venue_name: string
    meeting_date: string
    top_pick_position: number | null
    top_pick_won: boolean
    value_bets_count: number
    value_bets_won: number
    race_pnl: number
    pace_scenario: string | null
  }>
}

export const api = {
  getMeetings: (date: string) => fetchJson<Meeting[]>(`/meetings?date=${date}`),
  getDates: () => fetchJson<DateCount[]>(`/meetings/dates`),
  getRace: (raceId: string) => fetchJson<RaceDetail>(`/races/${raceId}`),
  getPredictions: (raceId: string) => fetchJson<PredictionResponse>(`/predictions/${raceId}`),
  getSectionals: (raceId: string) => fetchJson<SectionalResponse>(`/races/${raceId}/sectionals`),
  getRaceAnalysis: (raceId: string) => fetchJson<RaceAnalysisResponse>(`/races/${raceId}/analysis`),
  getAnalysisSummary: (days?: number) => fetchJson<AnalysisSummary>(`/analysis/summary?days=${days ?? 7}`),
}
