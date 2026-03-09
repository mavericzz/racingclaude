import 'dotenv/config'
import { request } from 'undici'
import pino from 'pino'

const log = pino({ name: 'tra-client' })

const TRA_USERNAME = process.env.TRA_USERNAME
const TRA_PASSWORD = process.env.TRA_PASSWORD

if (!TRA_USERNAME || !TRA_PASSWORD) {
  throw new Error('TRA_USERNAME and TRA_PASSWORD environment variables are required')
}

const basicAuth = Buffer.from(`${TRA_USERNAME}:${TRA_PASSWORD}`).toString('base64')
const BASE_URL = 'https://api.theracingapi.com'

class TheRacingApiClient {
  private lastRequestTime = 0
  private readonly RATE_LIMIT_DELAY = 250 // 4 rps (API limit is 5/sec, keep margin)

  private async rateLimit(): Promise<void> {
    const now = Date.now()
    const elapsed = now - this.lastRequestTime
    if (elapsed < this.RATE_LIMIT_DELAY) {
      await new Promise((r) => setTimeout(r, this.RATE_LIMIT_DELAY - elapsed))
    }
    this.lastRequestTime = Date.now()
  }

  async get<T = unknown>(path: string, params?: Record<string, string | number>): Promise<T> {
    const maxRetries = 3

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      await this.rateLimit()

      const url = new URL(path, BASE_URL)
      if (params) {
        for (const [key, value] of Object.entries(params)) {
          url.searchParams.append(key, String(value))
        }
      }

      log.debug({ url: url.toString() }, 'API request')

      const res = await request(url.toString(), {
        method: 'GET',
        headers: {
          Authorization: `Basic ${basicAuth}`,
          'Content-Type': 'application/json',
        },
      })

      if (res.statusCode === 429) {
        const body = await res.body.text()
        const wait = 2000 * (attempt + 1)
        log.warn({ wait, attempt }, 'Rate limited, backing off')
        await new Promise((r) => setTimeout(r, wait))
        continue
      }

      if (res.statusCode >= 400) {
        const body = await res.body.text()
        log.error({ status: res.statusCode, body, url: url.toString() }, 'API error')
        throw new Error(`TRA API ${res.statusCode}: ${body}`)
      }

      return (await res.body.json()) as T
    }

    throw new Error(`TRA API: Max retries exceeded for ${path}`)
  }

  // Australian Meets (today only - API only accepts single 'date' param)
  async getAustraliaMeets(date?: string) {
    const params: Record<string, string> = {}
    if (date) params.date = date
    return this.get<AustraliaMeetsResponse>('/v1/australia/meets', params)
  }

  // Australian Race (single race with runners)
  async getAustraliaRace(meetId: string, raceNumber: number) {
    return this.get<AustraliaRaceResponse>(
      `/v1/australia/meets/${meetId}/races/${raceNumber}`
    )
  }

  // Results (historical - uses start_date/end_date, max 365 day range)
  async getResults(params: { region?: string; start_date?: string; end_date?: string; limit?: number; skip?: number }) {
    return this.get<ResultsResponse>('/v1/results', params as Record<string, string | number>)
  }

  // Single result
  async getResult(raceId: string) {
    return this.get<ResultResponse>(`/v1/results/${raceId}`)
  }

  // Horse results history (uses start_date/end_date)
  async getHorseResults(horseId: string, params?: { start_date?: string; end_date?: string; limit?: number }) {
    return this.get<HorseResultsResponse>(`/v1/horses/${horseId}/results`, params as Record<string, string | number> | undefined)
  }

  // Horse pro profile
  async getHorsePro(horseId: string) {
    return this.get<unknown>(`/v1/horses/${horseId}/pro`)
  }

  // Trainer analysis
  async getTrainerResults(trainerId: string, limit = 50) {
    return this.get<unknown>(`/v1/trainers/${trainerId}/results`, { limit })
  }

  // Jockey analysis
  async getJockeyResults(jockeyId: string, limit = 50) {
    return this.get<unknown>(`/v1/jockeys/${jockeyId}/results`, { limit })
  }

  // Odds
  async getOdds(raceId: string, horseId: string) {
    return this.get<unknown>(`/v1/odds/${raceId}/${horseId}`)
  }
}

export const traClient = new TheRacingApiClient()

// ---- Type Definitions ----

export interface AustraliaMeet {
  meet_id: string
  date: string
  course: string
  course_id: string
  state?: string
  country?: string
  races: AustraliaRaceSummary[]
}

export interface AustraliaRaceSummary {
  race_number: number | string
  race_name?: string
  class?: string
  race_group?: string
  distance?: string
  race_status?: string
  off_time?: string
  is_trial?: boolean
  is_jump_out?: boolean
}

export interface AustraliaMeetsResponse {
  meets: AustraliaMeet[]
}

export interface AustraliaRunner {
  horse_id: string
  horse: string
  number?: number | string
  draw?: number | string
  age?: string
  sex?: string
  colour?: string
  weight?: string | number
  sire?: string
  sire_id?: string
  dam?: string
  dam_id?: string
  damsire?: string
  damsire_id?: string
  jockey?: string
  jockey_id?: string
  jockey_claim?: number | string
  trainer?: string
  trainer_id?: string
  owner?: string
  form?: string
  headgear?: string
  headgear_run?: string
  wind_surgery?: string
  rating?: number | string
  position?: number | string
  margin?: string
  sp?: string | number
  scratched?: boolean
  silk_url?: string
  comment?: string
  stats?: RunnerStats
  odds?: RunnerOdds[]
}

export interface RunnerStats {
  // API returns both forms - handle both
  course_stats?: StatBreakdown
  course?: StatBreakdown
  course_distance_stats?: StatBreakdown
  course_distance?: StatBreakdown
  distance_stats?: StatBreakdown
  distance?: StatBreakdown
  ground_firm_stats?: StatBreakdown
  ground_firm?: StatBreakdown
  ground_good_stats?: StatBreakdown
  ground_good?: StatBreakdown
  ground_heavy_stats?: StatBreakdown
  ground_heavy?: StatBreakdown
  ground_soft_stats?: StatBreakdown
  ground_soft?: StatBreakdown
  ground_aw_stats?: StatBreakdown
  ground_aw?: StatBreakdown
  jockey_stats?: StatBreakdown
  jockey?: StatBreakdown
  last_ten_races_stats?: StatBreakdown
  last_ten?: StatBreakdown
  last_twelve_months_stats?: StatBreakdown
  last_twelve_months?: StatBreakdown
  last_raced?: string
}

export interface StatBreakdown {
  total?: number
  first?: number
  second?: number
  third?: number
}

export interface RunnerOdds {
  bookmaker?: string
  win_odds?: number | string
  place_odds?: number | string
}

export interface AustraliaRaceResponse {
  race_id?: string
  meet_id?: string
  race_number?: number | string
  race_name?: string
  class?: string
  race_group?: string
  distance?: string
  going?: string
  off_time?: string
  race_status?: string
  prize?: string | number
  runners?: AustraliaRunner[]
}

export interface ResultsResponse {
  results: ResultRace[]
}

export interface ResultRace {
  race_id: string
  course?: string
  course_id?: string
  date?: string
  off_time?: string
  race_name?: string
  distance?: string
  distance_f?: string
  region?: string
  going?: string
  runners?: ResultRunner[]
}

export interface ResultRunner {
  horse_id: string
  horse?: string
  age?: string
  sex?: string
  position?: string | number
  draw?: string | number
  weight?: string
  sp?: string
  btn?: string
  time?: string
  or?: string | number
  rpr?: string | number
  prize?: string | number
  comment?: string
  trainer_id?: string
  trainer?: string
  jockey_id?: string
  jockey?: string
  owner_id?: string
  owner?: string
  sire_id?: string
  sire?: string
  dam_id?: string
  dam?: string
  damsire_id?: string
  damsire?: string
  headgear?: string
  number?: string | number
}

export interface ResultResponse {
  race_id: string
  course?: string
  course_id?: string
  date?: string
  off_time?: string
  race_name?: string
  distance?: string
  distance_f?: string
  region?: string
  going?: string
  runners?: ResultRunner[]
}

export interface HorseResultsResponse {
  horse_id: string
  results: ResultRace[]
}
