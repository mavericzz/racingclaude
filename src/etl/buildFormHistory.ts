import { query, upsert, pool } from '../lib/database.js'
import pino from 'pino'

const log = pino({ name: 'build-form-history' })

// Classify running style from race comment text
function classifyRunningStyle(comment: string | null): string | null {
  if (!comment) return null
  const c = comment.toLowerCase()

  // Leader patterns
  if (/\b(led\b|led all|led throughout|went forward|made all|set pace|jumped to lead|crossed to lead|began fast)/.test(c)) {
    return 'leader'
  }
  // On-pace patterns
  if (/\b(settled|tracked|handy|prominent|stalked|pressed|raced keen|near the lead|in touch|chased)/.test(c)) {
    return 'on-pace'
  }
  // Midfield patterns
  if (/\b(midfield|mid-division|mid division|middle|settled midfield|raced midfield)/.test(c)) {
    return 'mid'
  }
  // Backmarker patterns
  if (/\b(settled rear|last|rearward|tailed|back|behind|detached|at the tail|towards rear|near last)/.test(c)) {
    return 'backmarker'
  }

  return null
}

export async function buildFormHistory(): Promise<number> {
  log.info('Building horse form history from results + races')

  // Get all results with race context, ordered by horse + date
  const rows = await query<{
    horse_id: string
    race_id: string
    race_date: string
    venue_id: string | null
    distance_m: number | null
    going: string | null
    class: string | null
    position: number | null
    beaten_lengths: number | null
    sp_decimal: number | null
    weight_lbs: number | null
    jockey_id: string | null
    trainer_id: string | null
    headgear: string | null
    rating: number | null
    race_time: string | null
    field_size: number | null
    comment: string | null
  }>(`
    SELECT
      res.horse_id,
      res.race_id,
      m.meeting_date AS race_date,
      m.venue_id,
      rc.distance_m,
      rc.going,
      rc.class,
      res.position,
      res.beaten_lengths,
      res.sp_decimal,
      ru.weight_lbs,
      ru.jockey_id,
      ru.trainer_id,
      ru.headgear,
      ru.rating,
      res.race_time,
      rc.field_size,
      res.comment
    FROM results res
    JOIN races rc ON res.race_id = rc.race_id
    JOIN meetings m ON rc.meeting_id = m.meeting_id
    LEFT JOIN runners ru ON res.race_id = ru.race_id AND res.horse_id = ru.horse_id
    ORDER BY res.horse_id, m.meeting_date ASC
  `)

  log.info({ totalRows: rows.length }, 'Results fetched')

  // Group by horse to compute days_since_prev_run
  let currentHorseId = ''
  let prevDate: Date | null = null
  let inserted = 0

  for (const row of rows) {
    if (row.horse_id !== currentHorseId) {
      currentHorseId = row.horse_id
      prevDate = null
    }

    const raceDate = new Date(row.race_date)
    let daysSincePrev: number | null = null
    if (prevDate) {
      daysSincePrev = Math.round((raceDate.getTime() - prevDate.getTime()) / (1000 * 60 * 60 * 24))
    }

    const runningStyle = classifyRunningStyle(row.comment)

    await upsert(
      'horse_form_history',
      [
        'horse_id', 'race_id', 'race_date', 'venue_id', 'distance_m', 'going', 'class',
        'position', 'beaten_lengths', 'sp_decimal', 'weight_carried', 'jockey_id', 'trainer_id',
        'headgear', 'rating_before', 'race_time', 'field_size', 'days_since_prev_run', 'running_style',
      ],
      [
        row.horse_id, row.race_id, row.race_date, row.venue_id, row.distance_m, row.going, row.class,
        row.position, row.beaten_lengths, row.sp_decimal, row.weight_lbs, row.jockey_id, row.trainer_id,
        row.headgear, row.rating, row.race_time, row.field_size, daysSincePrev, runningStyle,
      ],
      ['horse_id', 'race_id']
    )

    prevDate = raceDate
    inserted++

    if (inserted % 5000 === 0) {
      log.info({ inserted }, 'Progress')
    }
  }

  log.info({ inserted }, 'Form history build complete')
  return inserted
}

// CLI entry
if (import.meta.url === `file://${process.argv[1]}`) {
  buildFormHistory()
    .then((n) => { log.info({ inserted: n }, 'Done'); pool.end() })
    .catch((e) => { log.error(e); process.exit(1) })
}
