/**
 * Test inference on a historical race where we have full data.
 */
import { readFileSync } from 'fs'
import { LGBMPredictor } from '../ai/models/lgbmInference.js'
import { analyzeRace } from '../ai/models/betting.js'
import { query, pool } from '../lib/database.js'

async function main() {
  const modelPath = new URL('../ai/models/modelWeights.json', import.meta.url)
  const modelJson = JSON.parse(readFileSync(modelPath, 'utf-8'))
  const predictor = new LGBMPredictor(modelJson)
  const featureNames: string[] = modelJson.feature_names

  // Get a well-populated historical race
  const races = await query<{ race_id: string; race_name: string; distance_m: number; going: string; field_size: number; meeting_date: string }>(
    `SELECT r.race_id, r.race_name, r.distance_m, r.going, r.field_size, m.meeting_date
     FROM races r
     JOIN meetings m ON r.meeting_id = m.meeting_id
     WHERE r.field_size >= 8
     AND m.meeting_date < CURRENT_DATE
     ORDER BY m.meeting_date DESC
     LIMIT 1`
  )

  if (races.length === 0) { console.log('No races'); await pool.end(); return }
  const race = races[0]
  console.log(`Race: ${race.race_name} (${race.meeting_date}, ${race.distance_m}m, ${race.going}, ${race.field_size} runners)`)

  // Get runners with features from DB
  const runners = await query<{
    horse_id: string; horse: string; draw: number; weight_lbs: number;
    jockey_claim: number; rating: number; sp_decimal: number; position: number;
  }>(
    `SELECT ru.horse_id, ru.horse, ru.draw, ru.weight_lbs, ru.jockey_claim,
            ru.rating, ru.sp_decimal, res.position
     FROM runners ru
     JOIN results res ON ru.race_id = res.race_id AND ru.horse_id = res.horse_id
     WHERE ru.race_id = $1 AND ru.scratched = FALSE
     ORDER BY ru.number`,
    [race.race_id]
  )

  // Get speed figures and form data
  const predictions = await Promise.all(runners.map(async runner => {
    const features: Record<string, number | null> = {}
    for (const name of featureNames) features[name] = null

    // Speed figures
    const sf = await query<{ adjusted_speed_figure: number }>(
      `SELECT sf.adjusted_speed_figure FROM speed_figures sf
       JOIN races r ON sf.race_id = r.race_id
       JOIN meetings m ON r.meeting_id = m.meeting_id
       WHERE sf.horse_id = $1 AND m.meeting_date < $2::date
       ORDER BY m.meeting_date DESC LIMIT 1`,
      [runner.horse_id, race.meeting_date]
    )
    if (sf.length > 0) features['last_speed_figure'] = Number(sf[0].adjusted_speed_figure)

    const sfAvg = await query<{ avg: number }>(
      `SELECT AVG(sf.adjusted_speed_figure) AS avg FROM (
        SELECT sf.adjusted_speed_figure FROM speed_figures sf
        JOIN races r ON sf.race_id = r.race_id
        JOIN meetings m ON r.meeting_id = m.meeting_id
        WHERE sf.horse_id = $1 AND m.meeting_date < $2::date
        ORDER BY m.meeting_date DESC LIMIT 5
      ) sf`,
      [runner.horse_id, race.meeting_date]
    )
    if (sfAvg.length > 0 && sfAvg[0].avg) features['avg_speed_figure_last5'] = Number(sfAvg[0].avg)

    // Form history
    const form = await query<{ days_gap: number; career_runs: number; career_wins: number }>(
      `SELECT
        ($2::date - MAX(race_date))::int AS days_gap,
        COUNT(*)::int AS career_runs,
        COUNT(*) FILTER (WHERE position = 1)::int AS career_wins
       FROM horse_form_history WHERE horse_id = $1 AND race_date < $2::date`,
      [runner.horse_id, race.meeting_date]
    )
    if (form.length > 0 && form[0].days_gap) {
      features['days_since_last_run'] = form[0].days_gap
      features['career_win_pct'] = form[0].career_runs > 0 ? (form[0].career_wins / form[0].career_runs) * 100 : null
      features['fitness_score'] = form[0].days_gap < 90 ? 1 : 0
    }

    features['barrier_draw'] = runner.draw ?? null
    features['weight_carried'] = runner.weight_lbs ? Number(runner.weight_lbs) - (runner.jockey_claim ?? 0) : null
    features['benchmark_rating'] = runner.rating ? Number(runner.rating) : null
    features['current_odds'] = runner.sp_decimal ? Number(runner.sp_decimal) : null
    features['market_implied_prob'] = runner.sp_decimal && Number(runner.sp_decimal) > 1 ? 1 / Number(runner.sp_decimal) : null
    features['field_size'] = race.field_size
    features['apprentice_claim'] = runner.jockey_claim ?? 0
    features['is_apprentice'] = (runner.jockey_claim ?? 0) > 0 ? 1 : 0

    return {
      horseId: runner.horse_id,
      horseName: runner.horse ?? 'Unknown',
      features,
      marketOdds: Number(runner.sp_decimal) || 10,
      actualPosition: Number(runner.position),
    }
  }))

  // Set favourite
  const minOdds = Math.min(...predictions.map(p => p.marketOdds))
  for (const p of predictions) {
    if (p.marketOdds === minOdds) p.features['is_favourite'] = 1
    else p.features['is_favourite'] = 0
  }

  const raceResults = predictor.predictRace(
    predictions.map(p => ({ horseId: p.horseId, features: p.features }))
  )

  console.log('\nRank | Horse                    | Win%   | Odds   | Fair   | Actual')
  console.log('-----|--------------------------|--------|--------|--------|-------')

  for (const pred of raceResults) {
    const runner = predictions.find(p => p.horseId === pred.horseId)!
    const fairOdds = pred.winProb > 0 ? (1 / pred.winProb).toFixed(1) : 'N/A'
    const actual = runner.actualPosition === 1 ? 'WON!' : `${runner.actualPosition}`
    console.log(
      `  ${pred.rank}  | ${runner.horseName.substring(0, 24).padEnd(24)} | ${(pred.winProb * 100).toFixed(1).padStart(5)}% | ${runner.marketOdds.toFixed(1).padStart(6)} | ${fairOdds.padStart(6)} | ${actual}`
    )
  }

  // Betting analysis
  const bets = analyzeRace(
    raceResults.map(pred => {
      const runner = predictions.find(p => p.horseId === pred.horseId)!
      return { horseId: pred.horseId, horseName: runner.horseName, modelProb: pred.winProb, marketOdds: runner.marketOdds }
    })
  )

  const valueBets = bets.filter(b => b.verdict === 'strong-value' || b.verdict === 'value')
  if (valueBets.length > 0) {
    console.log('\n=== Value Bets ===')
    for (const b of valueBets) {
      console.log(`  ${b.verdict.toUpperCase()} ${b.horseName} | edge: ${b.edgePct.toFixed(1)}% | kelly: ${(b.kellyFraction * 100).toFixed(2)}% | stake: $${b.recommendedStake.toFixed(0)}`)
    }
  }

  await pool.end()
}

main().catch(e => { console.error(e); process.exit(1) })
