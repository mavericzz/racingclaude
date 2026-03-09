/**
 * Import Webcrawler output (punters.com.au) into racingclaude database.
 * Reads from /Users/rahulsharma/Webcrawler/output/{date}/{Venue}/data.json
 * Upserts into: sectional_times, scraper_form_data
 */

import fs from 'fs'
import path from 'path'
import pino from 'pino'
import { query, batchUpsert, pool } from '../lib/database.js'
import { normalizeName, findBestMatch } from '../lib/nameMatch.js'

const log = pino({ name: 'ingest-scraper' })

const WEBCRAWLER_OUTPUT = '/Users/rahulsharma/Webcrawler/output'

// ── Types for scraper JSON ──

interface ScraperRunner {
  number: number
  name: string
  barrier: string
  jockey: string
  trainer: string
  weight: string        // "58kg", "61.5kg"
  formString: string
  career: string        // "40-3-1" (starts-wins-places) or "350-2-3"
  winPercent: string    // "0%", "25%"
  placePercent: string
  prizeMoney: string    // "$5,588"
  formComment: string
}

interface ScraperSectional {
  horseNumber: number
  horseName: string
  barrier: string
  runsCalculated: string
  speed800m: string     // "63.41km/h" or "No Data"
  speed600m: string
  speed400m: string
  speed200m: string
  speedFinish: string
  speedAvg: string
  odds: string          // "$26" or "$7.5"
}

interface ScraperRace {
  raceNumber: number
  raceName: string
  raceUrl: string
  distance: string
  raceTime: string
  runners: ScraperRunner[]
  sectionals: ScraperSectional[]
}

interface ScraperMeeting {
  meetingName: string
  meetingUrl: string
  location: string
  raceCount: number
  races: Array<{ raceNumber: number; raceName: string }>
}

// ── Parsing helpers ──

function parseSpeed(s: string): number | null {
  if (!s || s === 'No Data' || s === '-' || s === '') return null
  const num = parseFloat(s.replace('km/h', '').trim())
  return isNaN(num) ? null : num
}

function parseOdds(s: string): number | null {
  if (!s || s === '-' || s === '') return null
  const num = parseFloat(s.replace('$', '').replace(',', '').trim())
  return isNaN(num) ? null : num
}

function parseWeightKg(s: string): number | null {
  if (!s) return null
  const num = parseFloat(s.replace('kg', '').trim())
  return isNaN(num) ? null : num
}

function parseCareer(s: string): { starts: number; wins: number; places: number } | null {
  if (!s || s === '-') return null
  // Format: "40-3-1" or "350-2-3"
  // First number can be multi-digit starts, but format is starts-wins-places
  const parts = s.split('-').map(n => parseInt(n.trim()))
  if (parts.length < 3 || parts.some(isNaN)) return null
  return { starts: parts[0], wins: parts[1], places: parts[2] }
}

function parsePct(s: string): number | null {
  if (!s || s === '-') return null
  const num = parseFloat(s.replace('%', '').trim())
  return isNaN(num) ? null : num
}

/**
 * Parse form comment into structured flags.
 * Comments are concatenated tags like:
 * "Racing WellThis horse has run consistently well... | Top TrainerOne of the top..."
 */
function parseFormFlags(comment: string): Record<string, boolean> {
  if (!comment) return {}
  const flags: Record<string, boolean> = {}

  const patterns: [RegExp, string][] = [
    [/racing well/i, 'racing_well'],
    [/not racing well/i, 'not_racing_well'],
    [/top trainer/i, 'top_trainer'],
    [/top jockey/i, 'top_jockey'],
    [/jockey claim/i, 'jockey_claim'],
    [/blinkers on/i, 'blinkers_on'],
    [/blinkers off/i, 'blinkers_off'],
    [/gear change/i, 'gear_change'],
    [/drawn wide/i, 'drawn_wide'],
    [/drawn well/i, 'drawn_well'],
    [/back in distance/i, 'back_in_distance'],
    [/up in distance/i, 'up_in_distance'],
    [/quick back up/i, 'quick_backup'],
    [/first up/i, 'first_up'],
    [/second up/i, 'second_up'],
    [/third up/i, 'third_up'],
    [/lacks race experience/i, 'first_starter'],
    [/long starting price/i, 'long_price'],
    [/short starting price/i, 'short_price'],
    [/class drop/i, 'class_drop'],
    [/class rise/i, 'class_rise'],
    [/wet track/i, 'wet_track'],
    [/track specialist/i, 'track_specialist'],
    [/distance specialist/i, 'distance_specialist'],
  ]

  for (const [regex, flag] of patterns) {
    if (regex.test(comment)) flags[flag] = true
  }

  // Override: if both racing_well and not_racing_well matched, keep only not_racing_well
  if (flags.not_racing_well) delete flags.racing_well

  return flags
}

// ── Venue name matching ──

/**
 * Normalize scraper venue folder name to match DB venue names.
 * e.g. "Sunshine_Coast" → "sunshine coast", "Sunshine_Coast_Poly" → "sunshine coast poly"
 */
function normalizeVenueName(folderName: string): string {
  return folderName.replace(/_/g, ' ').toLowerCase().trim()
}

async function findMeetingForVenue(
  venueName: string,
  dateStr: string
): Promise<{ meetingId: string; venueName: string } | null> {
  // Get all meetings for this date with venue names
  const meetings = await query<{
    meeting_id: string
    venue_name: string
  }>(`
    SELECT m.meeting_id, v.name as venue_name
    FROM meetings m
    JOIN venues v ON v.venue_id = m.venue_id
    WHERE m.meeting_date = $1
  `, [dateStr])

  const normalized = normalizeVenueName(venueName)

  // Handle "Poly" suffix first (most specific): "Sunshine_Coast_Poly" → "Sunshine Coast Poly Track"
  if (normalized.endsWith(' poly')) {
    const polyTrack = normalized.replace(/ poly$/, ' poly track')
    for (const m of meetings) {
      if (m.venue_name.toLowerCase() === polyTrack) {
        return { meetingId: m.meeting_id, venueName: m.venue_name }
      }
    }
    // Also try just matching with "poly" in the name
    for (const m of meetings) {
      if (m.venue_name.toLowerCase().includes('poly') && m.venue_name.toLowerCase().includes(normalized.replace(/ poly$/, ''))) {
        return { meetingId: m.meeting_id, venueName: m.venue_name }
      }
    }
  }

  // Exact match (case-insensitive)
  for (const m of meetings) {
    if (m.venue_name.toLowerCase() === normalized) {
      return { meetingId: m.meeting_id, venueName: m.venue_name }
    }
  }

  // Fuzzy: strip common prefixes like "bet365 " from DB names
  for (const m of meetings) {
    const dbName = m.venue_name.toLowerCase().replace(/^(bet365|tab|sky)\s+/i, '')
    if (dbName === normalized) {
      return { meetingId: m.meeting_id, venueName: m.venue_name }
    }
  }

  // Fuzzy containment — but skip if folder name has "poly" (already handled above)
  if (!normalized.includes('poly')) {
    for (const m of meetings) {
      const dbName = m.venue_name.toLowerCase()
      // Only match if DB name doesn't contain "poly" either (avoid cross-matching)
      if (!dbName.includes('poly') && (dbName.includes(normalized) || normalized.includes(dbName))) {
        return { meetingId: m.meeting_id, venueName: m.venue_name }
      }
    }

    for (const m of meetings) {
      const dbName = m.venue_name.toLowerCase().replace(/^(bet365|tab|sky)\s+/i, '')
      if (!dbName.includes('poly') && (dbName.includes(normalized) || normalized.includes(dbName))) {
        return { meetingId: m.meeting_id, venueName: m.venue_name }
      }
    }
  }

  return null
}

// ── Main ingestion ──

interface IngestStats {
  venues: number
  venuesMatched: number
  racesProcessed: number
  sectionalRows: number
  formRows: number
  horsesMatched: number
  horsesUnmatched: number
}

export async function ingestScraperData(dateStr: string): Promise<IngestStats> {
  const dateDir = path.join(WEBCRAWLER_OUTPUT, dateStr)
  if (!fs.existsSync(dateDir)) {
    throw new Error(`Scraper output not found: ${dateDir}`)
  }

  const stats: IngestStats = {
    venues: 0, venuesMatched: 0, racesProcessed: 0,
    sectionalRows: 0, formRows: 0, horsesMatched: 0, horsesUnmatched: 0,
  }

  // Read all_meetings.json or scan directories
  const entries = fs.readdirSync(dateDir, { withFileTypes: true })
  const venueDirs = entries
    .filter(e => e.isDirectory())
    .map(e => e.name)

  stats.venues = venueDirs.length
  log.info({ date: dateStr, venues: venueDirs.length }, 'Starting scraper data ingestion')

  for (const venueFolder of venueDirs) {
    const dataFile = path.join(dateDir, venueFolder, 'data.json')
    if (!fs.existsSync(dataFile)) {
      log.warn({ venue: venueFolder }, 'No data.json found, skipping')
      continue
    }

    // Match venue to DB meeting
    const meeting = await findMeetingForVenue(venueFolder, dateStr)
    if (!meeting) {
      log.warn({ venue: venueFolder, date: dateStr }, 'Could not match venue to DB meeting')
      continue
    }
    stats.venuesMatched++
    log.info({ folder: venueFolder, dbVenue: meeting.venueName, meetingId: meeting.meetingId }, 'Matched venue')

    // Load scraper data
    const data: { races: ScraperRace[] } = JSON.parse(fs.readFileSync(dataFile, 'utf-8'))

    // Get all races for this meeting from DB
    const dbRaces = await query<{ race_id: string; race_number: number }>(`
      SELECT race_id, race_number FROM races WHERE meeting_id = $1
    `, [meeting.meetingId])
    const raceMap = new Map(dbRaces.map(r => [r.race_number, r.race_id]))

    for (const race of data.races) {
      const raceId = raceMap.get(race.raceNumber)
      if (!raceId) {
        log.warn({ venue: venueFolder, raceNum: race.raceNumber }, 'Race number not found in DB')
        continue
      }
      stats.racesProcessed++

      // Get DB runners for name matching
      const dbRunners = await query<{ horse: string; horse_id: string }>(`
        SELECT horse, horse_id FROM runners WHERE race_id = $1
      `, [raceId])
      const runnerNames = dbRunners.map(r => r.horse)
      const runnerMap = new Map(dbRunners.map(r => [r.horse, r.horse_id]))

      // Process sectionals
      if (race.sectionals?.length > 0) {
        const sectionalCols = [
          'race_id', 'horse_name', 'horse_id', 'horse_number', 'barrier',
          'speed_800m', 'speed_600m', 'speed_400m', 'speed_200m',
          'speed_finish', 'speed_avg', 'scraper_odds', 'source',
        ]

        const sectionalRows: unknown[][] = []
        for (const sec of race.sectionals) {
          const matchedName = findBestMatch(sec.horseName, runnerNames)
          const horseId = matchedName ? (runnerMap.get(matchedName) ?? null) : null

          if (matchedName) stats.horsesMatched++
          else stats.horsesUnmatched++

          sectionalRows.push([
            raceId,
            sec.horseName,
            horseId,
            sec.horseNumber,
            sec.barrier ? parseInt(sec.barrier) || null : null,
            parseSpeed(sec.speed800m),
            parseSpeed(sec.speed600m),
            parseSpeed(sec.speed400m),
            parseSpeed(sec.speed200m),
            parseSpeed(sec.speedFinish),
            parseSpeed(sec.speedAvg),
            parseOdds(sec.odds),
            'punters',
          ])
        }

        if (sectionalRows.length > 0) {
          const inserted = await batchUpsert(
            'sectional_times', sectionalCols, sectionalRows,
            ['race_id', 'horse_name']
          )
          stats.sectionalRows += inserted
        }
      }

      // Process runner form data
      if (race.runners?.length > 0) {
        const formCols = [
          'race_id', 'horse_name', 'horse_id', 'jockey', 'trainer',
          'weight_kg', 'form_string', 'career_starts', 'career_wins', 'career_places',
          'win_pct', 'place_pct', 'prize_money_text', 'form_comment', 'form_flags', 'source',
        ]

        const formRows: unknown[][] = []
        for (const runner of race.runners) {
          const matchedName = findBestMatch(runner.name, runnerNames)
          const horseId = matchedName ? (runnerMap.get(matchedName) ?? null) : null
          const career = parseCareer(runner.career)

          formRows.push([
            raceId,
            runner.name,
            horseId,
            runner.jockey || null,
            runner.trainer || null,
            parseWeightKg(runner.weight),
            runner.formString || null,
            career?.starts ?? null,
            career?.wins ?? null,
            career?.places ?? null,
            parsePct(runner.winPercent),
            parsePct(runner.placePercent),
            runner.prizeMoney || null,
            runner.formComment || null,
            JSON.stringify(parseFormFlags(runner.formComment)),
            'punters',
          ])
        }

        if (formRows.length > 0) {
          const inserted = await batchUpsert(
            'scraper_form_data', formCols, formRows,
            ['race_id', 'horse_name']
          )
          stats.formRows += inserted
        }
      }
    }
  }

  log.info(stats, 'Scraper data ingestion complete')
  return stats
}

// ── CLI entry point ──

async function main() {
  const dateArg = process.argv[2] || new Date().toISOString().split('T')[0]
  log.info({ date: dateArg }, 'Ingesting scraper data')

  try {
    const stats = await ingestScraperData(dateArg)
    console.log('\nIngestion Summary:')
    console.log(`  Venues found:     ${stats.venues}`)
    console.log(`  Venues matched:   ${stats.venuesMatched}`)
    console.log(`  Races processed:  ${stats.racesProcessed}`)
    console.log(`  Sectional rows:   ${stats.sectionalRows}`)
    console.log(`  Form data rows:   ${stats.formRows}`)
    console.log(`  Horses matched:   ${stats.horsesMatched}`)
    console.log(`  Horses unmatched: ${stats.horsesUnmatched}`)
  } catch (err) {
    log.error(err, 'Ingestion failed')
    process.exit(1)
  } finally {
    await pool.end()
  }
}

const isDirectExecution = process.argv[1]?.includes('ingestScraperData')
if (isDirectExecution) main()
