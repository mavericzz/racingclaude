import { ingestMeetings } from '../etl/ingestMeetings.js'
import { pool } from '../lib/database.js'
import { format, subMonths, addDays } from 'date-fns'

async function main() {
  const monthsBack = parseInt(process.argv[2] ?? '12')
  const endDate = new Date()
  const startDate = subMonths(endDate, monthsBack)

  console.log(`Backfilling meetings from ${format(startDate, 'yyyy-MM-dd')} to ${format(endDate, 'yyyy-MM-dd')}`)

  let totalRaces = 0
  let cursor = new Date(startDate)
  const stepDays = 7

  while (cursor < endDate) {
    const chunkEnd = new Date(Math.min(addDays(cursor, stepDays - 1).getTime(), endDate.getTime()))
    const from = format(cursor, 'yyyy-MM-dd')
    const to = format(chunkEnd, 'yyyy-MM-dd')

    try {
      const races = await ingestMeetings(from)
      totalRaces += races
      process.stdout.write(`.`)
    } catch (err: any) {
      process.stdout.write(`X`)
    }

    cursor = addDays(chunkEnd, 1)
  }

  const counts = await pool.query(`
    SELECT COUNT(*) AS races FROM races
  `)
  console.log(`\nDone! Total races inserted: ${totalRaces}. DB total: ${counts.rows[0].races}`)
  await pool.end()
}

main().catch((e) => { console.error(e); process.exit(1) })
