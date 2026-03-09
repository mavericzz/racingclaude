import { traClient, type AustraliaRaceResponse, type AustraliaRunner, type RunnerStats, type StatBreakdown } from '../lib/traClient.js'
import { upsert, query, pool } from '../lib/database.js'
import pino from 'pino'

const log = pino({ name: 'ingest-runners' })

function toNum(val: string | number | undefined | null): number | null {
  if (val === undefined || val === null || val === '') return null
  const n = typeof val === 'string' ? parseFloat(val) : val
  return isNaN(n) ? null : n
}

function toInt(val: string | number | undefined | null): number | null {
  const n = toNum(val)
  return n !== null ? Math.round(n) : null
}

function parseSP(sp: string | number | undefined | null): number | null {
  if (!sp) return null
  if (typeof sp === 'number') return sp
  // Handle fractional odds like "5/1" or decimal "6.0"
  const fracMatch = sp.match(/^(\d+)\/(\d+)$/)
  if (fracMatch) return parseFloat(fracMatch[1]) / parseFloat(fracMatch[2]) + 1
  const dec = parseFloat(sp)
  return isNaN(dec) ? null : dec
}

async function upsertFormStats(horseId: string, raceId: string, stats: RunnerStats) {
  const statTypes: [string, StatBreakdown | undefined][] = [
    ['course', stats.course_stats ?? stats.course],
    ['course_distance', stats.course_distance_stats ?? stats.course_distance],
    ['distance', stats.distance_stats ?? stats.distance],
    ['ground_firm', stats.ground_firm_stats ?? stats.ground_firm],
    ['ground_good', stats.ground_good_stats ?? stats.ground_good],
    ['ground_heavy', stats.ground_heavy_stats ?? stats.ground_heavy],
    ['ground_soft', stats.ground_soft_stats ?? stats.ground_soft],
    ['ground_aw', stats.ground_aw_stats ?? stats.ground_aw],
    ['jockey', stats.jockey_stats ?? stats.jockey],
    ['last_ten', stats.last_ten_races_stats ?? stats.last_ten],
    ['last_twelve_months', stats.last_twelve_months_stats ?? stats.last_twelve_months],
  ]

  for (const [statType, breakdown] of statTypes) {
    if (!breakdown) continue
    const total = toInt(breakdown.total)
    if (!total) continue
    await upsert(
      'runner_form_stats',
      ['horse_id', 'race_id', 'stat_type', 'total', 'first', 'second', 'third'],
      [horseId, raceId, statType, total, toInt(breakdown.first) ?? 0, toInt(breakdown.second) ?? 0, toInt(breakdown.third) ?? 0],
      ['horse_id', 'race_id', 'stat_type']
    )
  }
}

async function upsertRunnerOdds(raceId: string, horseId: string, odds: AustraliaRunner['odds']) {
  if (!odds || !Array.isArray(odds)) return
  for (const o of odds) {
    if (!o.bookmaker) continue
    const winOdds = toNum(o.win_odds)
    const placeOdds = toNum(o.place_odds)
    if (winOdds === null && placeOdds === null) continue

    await upsert(
      'odds_snapshots',
      ['race_id', 'horse_id', 'bookmaker', 'win_odds', 'place_odds', 'observed_at'],
      [raceId, horseId, o.bookmaker, winOdds, placeOdds, new Date().toISOString()],
      ['race_id', 'horse_id', 'bookmaker', 'observed_at']
    )
  }
}

export async function ingestRunnersForRace(meetId: string, raceNumber: number, raceId: string): Promise<number> {
  const race = await traClient.getAustraliaRace(meetId, raceNumber)
  const runners = race?.runners ?? []

  if (runners.length === 0) {
    log.debug({ raceId }, 'No runners returned')
    return 0
  }

  // Update race with going and field_size if available
  if (race.going || runners.length > 0) {
    await pool.query(
      `UPDATE races SET going = COALESCE($1, going), field_size = $2 WHERE race_id = $3`,
      [race.going ?? null, runners.filter(r => !r.scratched).length, raceId]
    )
  }

  let count = 0
  for (const r of runners) {
    if (!r.horse_id) continue

    // Upsert horse
    await upsert(
      'horses',
      ['id', 'name', 'sire', 'sire_id', 'dam', 'dam_id', 'damsire', 'damsire_id', 'age', 'sex', 'colour'],
      [r.horse_id, r.horse ?? 'Unknown', r.sire ?? null, r.sire_id ?? null, r.dam ?? null, r.dam_id ?? null, r.damsire ?? null, r.damsire_id ?? null, r.age ?? null, r.sex ?? null, r.colour ?? null],
      ['id']
    )

    // Upsert jockey
    if (r.jockey_id && r.jockey) {
      await upsert('jockeys', ['id', 'name'], [r.jockey_id, r.jockey], ['id'])
    }

    // Upsert trainer
    if (r.trainer_id && r.trainer) {
      await upsert('trainers', ['id', 'name'], [r.trainer_id, r.trainer], ['id'])
    }

    // Upsert owner
    if (r.owner) {
      const ownerId = `own_${r.owner.replace(/\s+/g, '_').toLowerCase().substring(0, 50)}`
      await upsert('owners', ['id', 'name'], [ownerId, r.owner], ['id'])
    }

    // Upsert runner
    await upsert(
      'runners',
      ['race_id', 'horse_id', 'jockey_id', 'trainer_id', 'horse', 'number', 'draw', 'weight_lbs', 'jockey_claim', 'form', 'headgear', 'headgear_run', 'wind_surgery', 'rating', 'sp_decimal', 'position', 'margin', 'comment', 'scratched'],
      [
        raceId, r.horse_id, r.jockey_id ?? null, r.trainer_id ?? null,
        r.horse ?? null, toInt(r.number), toInt(r.draw), toNum(r.weight),
        toInt(r.jockey_claim) ?? 0, r.form ?? null, r.headgear ?? null,
        r.headgear_run ?? null, r.wind_surgery ?? null, toInt(r.rating),
        parseSP(r.sp), toInt(r.position), r.margin ?? null, r.comment ?? null,
        r.scratched ?? false,
      ],
      ['race_id', 'horse_id']
    )

    // Upsert form stats
    if (r.stats) {
      await upsertFormStats(r.horse_id, raceId, r.stats)
    }

    // Upsert odds
    if (r.odds) {
      await upsertRunnerOdds(raceId, r.horse_id, r.odds)
    }

    count++
  }

  return count
}

export async function ingestAllRunners(): Promise<void> {
  // Get all races that need runners ingested
  const races = await query<{ race_id: string; meeting_id: string; race_number: number }>(
    `SELECT r.race_id, r.meeting_id, r.race_number
     FROM races r
     LEFT JOIN runners ru ON r.race_id = ru.race_id
     WHERE ru.id IS NULL
     ORDER BY r.off_time`
  )

  log.info({ totalRaces: races.length }, 'Races needing runner ingestion')

  let total = 0
  for (let i = 0; i < races.length; i++) {
    const race = races[i]
    try {
      const count = await ingestRunnersForRace(race.meeting_id, race.race_number, race.race_id)
      total += count
      if ((i + 1) % 50 === 0) {
        log.info({ progress: `${i + 1}/${races.length}`, runnersTotal: total }, 'Progress')
      }
    } catch (err) {
      log.error({ raceId: race.race_id, err }, 'Failed to ingest runners')
    }
  }

  log.info({ totalRunners: total, totalRaces: races.length }, 'Runner ingestion complete')
}

// CLI entry
if (import.meta.url === `file://${process.argv[1]}`) {
  ingestAllRunners()
    .then(() => pool.end())
    .catch((e) => { log.error(e); process.exit(1) })
}
