/**
 * Test the TypeScript LightGBM inference engine with the exported model.
 * Runs predictions on a sample race from today's data.
 */
import { readFileSync } from 'fs'
import { LGBMPredictor } from '../ai/models/lgbmInference.js'
import { analyzeRace } from '../ai/models/betting.js'
import { query, pool } from '../lib/database.js'

async function main() {
  // Load the exported model
  const modelPath = new URL('../ai/models/modelWeights.json', import.meta.url)
  const modelJson = JSON.parse(readFileSync(modelPath, 'utf-8'))
  const predictor = new LGBMPredictor(modelJson)

  console.log(`Model version: ${predictor.version}`)
  console.log(`Features: ${modelJson.feature_names.length}`)
  console.log(`Calibration: ${modelJson.calibration ? 'yes' : 'no'}`)

  // Get a race from today with runners
  const races = await query<{ race_id: string; race_name: string; distance_m: number; going: string; field_size: number }>(
    `SELECT r.race_id, r.race_name, r.distance_m, r.going, r.field_size
     FROM races r
     JOIN meetings m ON r.meeting_id = m.meeting_id
     WHERE m.meeting_date = CURRENT_DATE
     AND r.field_size > 0
     ORDER BY r.off_time
     LIMIT 1`
  )

  if (races.length === 0) {
    console.log('No races found for today')
    await pool.end()
    return
  }

  const race = races[0]
  console.log(`\nTest race: ${race.race_name} (${race.distance_m}m, ${race.going}, ${race.field_size} runners)`)

  // Get runners with basic features
  const runners = await query<{
    horse_id: string; horse: string; draw: number; weight_lbs: number;
    jockey_claim: number; rating: number; sp_decimal: number;
  }>(
    `SELECT ru.horse_id, ru.horse, ru.draw, ru.weight_lbs, ru.jockey_claim,
            ru.rating, ru.sp_decimal
     FROM runners ru
     WHERE ru.race_id = $1 AND ru.scratched = FALSE
     ORDER BY ru.number`,
    [race.race_id]
  )

  console.log(`Runners: ${runners.length}`)

  // Build feature vectors for each runner
  const featureNames: string[] = modelJson.feature_names
  const predictions = runners.map(runner => {
    // Create a basic feature dict (many will be NaN, which LightGBM handles)
    const features: Record<string, number | null> = {}
    for (const name of featureNames) {
      features[name] = null // default NaN
    }

    // Fill what we have
    features['barrier_draw'] = runner.draw ?? null
    features['weight_carried'] = runner.weight_lbs ? runner.weight_lbs - (runner.jockey_claim ?? 0) : null
    features['benchmark_rating'] = runner.rating ?? null
    features['current_odds'] = runner.sp_decimal ?? null
    features['market_implied_prob'] = runner.sp_decimal && runner.sp_decimal > 1 ? 1 / runner.sp_decimal : null
    features['field_size'] = race.field_size
    features['is_favourite'] = 0
    features['apprentice_claim'] = runner.jockey_claim ?? 0
    features['is_apprentice'] = (runner.jockey_claim ?? 0) > 0 ? 1 : 0

    return {
      horseId: runner.horse_id,
      horseName: runner.horse ?? 'Unknown',
      features,
      marketOdds: Number(runner.sp_decimal) || 10,
    }
  })

  // Find favourite
  const minOdds = Math.min(...predictions.map(p => p.marketOdds))
  for (const p of predictions) {
    if (p.marketOdds === minOdds) p.features['is_favourite'] = 1
  }

  // Run predictions
  const raceResults = predictor.predictRace(
    predictions.map(p => ({ horseId: p.horseId, features: p.features }))
  )

  console.log('\n=== AI Predictions ===')
  console.log('Rank | Horse                    | Win%   | Odds   | Model Odds')
  console.log('-----|--------------------------|--------|--------|----------')

  for (const pred of raceResults) {
    const runner = predictions.find(p => p.horseId === pred.horseId)!
    const fairOdds = pred.winProb > 0 ? (1 / pred.winProb).toFixed(1) : 'N/A'
    console.log(
      `  ${pred.rank}  | ${runner.horseName.padEnd(24)} | ${(pred.winProb * 100).toFixed(1).padStart(5)}% | ${runner.marketOdds.toFixed(1).padStart(6)} | ${fairOdds.padStart(8)}`
    )
  }

  // Run betting analysis
  const bettingAnalysis = analyzeRace(
    raceResults.map(pred => {
      const runner = predictions.find(p => p.horseId === pred.horseId)!
      return {
        horseId: pred.horseId,
        horseName: runner.horseName,
        modelProb: pred.winProb,
        marketOdds: runner.marketOdds,
      }
    })
  )

  console.log('\n=== Betting Verdicts ===')
  for (const bet of bettingAnalysis.filter(b => b.verdict !== 'pass')) {
    console.log(
      `${bet.verdict.toUpperCase().padEnd(16)} ${bet.horseName.padEnd(24)} ` +
      `edge: ${bet.edgePct.toFixed(1)}% kelly: ${(bet.kellyFraction * 100).toFixed(2)}% ` +
      `stake: $${bet.recommendedStake.toFixed(0)}`
    )
  }

  await pool.end()
}

main().catch(e => { console.error(e); process.exit(1) })
