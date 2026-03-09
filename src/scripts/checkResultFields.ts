import { traClient } from '../lib/traClient.js'

async function main() {
  const r = await traClient.getResults({ region: 'aus', start_date: '2026-02-28', end_date: '2026-03-06', limit: 3 })
  for (const race of (r.results ?? []).slice(0, 2)) {
    console.log('Race fields:', JSON.stringify({
      race_id: race.race_id,
      course: race.course,
      course_id: race.course_id,
      date: race.date,
      race_name: race.race_name,
      distance: race.distance,
      distance_f: race.distance_f,
      going: race.going,
      region: race.region,
      off_time: race.off_time,
    }, null, 2))
  }
}

main().catch(e => { console.error(e.message); process.exit(1) })
