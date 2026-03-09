/**
 * Betting strategy engine.
 * Kelly criterion, dutching, value detection, and verdict classification.
 */

export type Verdict = 'strong-value' | 'value' | 'dutch-candidate' | 'fair-price' | 'oppose' | 'pass'

export interface BetRecommendation {
  horseId: string
  horseName: string
  modelProb: number
  marketOdds: number
  marketImpliedProb: number
  edgePct: number
  kellyFraction: number
  recommendedStake: number // as % of bankroll
  verdict: Verdict
  fairOdds: number
}

export interface DutchSelection {
  horseId: string
  horseName: string
  stake: number
  targetReturn: number
}

// Calculate edge: model probability vs market-implied probability
export function calculateEdge(modelProb: number, marketOdds: number): number {
  if (marketOdds <= 1) return 0
  const marketImplied = 1 / marketOdds
  return modelProb - marketImplied
}

// Kelly criterion: optimal fraction of bankroll to bet
export function kellyStake(
  modelProb: number,
  marketOdds: number,
  fraction: number = 0.25 // quarter Kelly for safety
): number {
  if (marketOdds <= 1 || modelProb <= 0) return 0

  const b = marketOdds - 1 // net odds
  const p = modelProb
  const q = 1 - p

  const fullKelly = (b * p - q) / b
  if (fullKelly <= 0) return 0

  // Apply fraction and cap at 5% max
  return Math.min(fullKelly * fraction, 0.05)
}

// Fair odds: what the odds should be based on our model
export function fairOdds(modelProb: number): number {
  if (modelProb <= 0) return 999
  return 1 / modelProb
}

// Classify verdict based on edge
export function classifyVerdict(edgePct: number, modelProb: number): Verdict {
  if (edgePct > 10 && modelProb > 0.12) return 'strong-value'
  if (edgePct > 5) return 'value'
  if (edgePct > 3) return 'dutch-candidate'
  if (edgePct > -2) return 'fair-price'
  if (edgePct < -10 && modelProb < 0.08) return 'oppose'
  return 'pass'
}

// Generate bet recommendation for a runner
export function generateRecommendation(
  horseId: string,
  horseName: string,
  modelProb: number,
  marketOdds: number,
  bankroll: number = 1000
): BetRecommendation {
  const edge = calculateEdge(modelProb, marketOdds)
  const edgePct = edge * 100
  const kelly = kellyStake(modelProb, marketOdds)
  const verdict = classifyVerdict(edgePct, modelProb)

  return {
    horseId,
    horseName,
    modelProb,
    marketOdds,
    marketImpliedProb: marketOdds > 1 ? 1 / marketOdds : 0,
    edgePct,
    kellyFraction: kelly,
    recommendedStake: kelly * bankroll,
    verdict,
    fairOdds: fairOdds(modelProb),
  }
}

// Generate recommendations for an entire race
export function analyzeRace(
  runners: Array<{
    horseId: string
    horseName: string
    modelProb: number
    marketOdds: number
  }>,
  bankroll: number = 1000
): BetRecommendation[] {
  return runners
    .map((r) => generateRecommendation(r.horseId, r.horseName, r.modelProb, r.marketOdds, bankroll))
    .sort((a, b) => b.edgePct - a.edgePct)
}

// Dutching: spread stake across multiple selections for guaranteed equal return
export function calculateDutching(
  selections: Array<{ horseId: string; horseName: string; marketOdds: number }>,
  totalStake: number
): DutchSelection[] {
  if (selections.length === 0) return []

  const totalImplied = selections.reduce((sum, s) => sum + (1 / s.marketOdds), 0)

  // No value in dutching if total implied > 1 (overround exceeds selections)
  if (totalImplied >= 1) return []

  return selections.map((s) => {
    const proportion = (1 / s.marketOdds) / totalImplied
    const stake = totalStake * proportion
    return {
      horseId: s.horseId,
      horseName: s.horseName,
      stake: Math.round(stake * 100) / 100,
      targetReturn: Math.round((totalStake / totalImplied) * 100) / 100,
    }
  })
}

// Calculate expected value of a bet
export function expectedValue(modelProb: number, marketOdds: number, stake: number = 1): number {
  const winReturn = stake * (marketOdds - 1)
  const lossReturn = -stake
  return modelProb * winReturn + (1 - modelProb) * lossReturn
}

// Bankroll management: calculate max simultaneous exposure
export function maxExposure(recommendations: BetRecommendation[], bankroll: number): {
  totalExposure: number
  exposurePct: number
  betsCount: number
} {
  const activeBets = recommendations.filter(
    (r) => r.verdict === 'strong-value' || r.verdict === 'value'
  )
  const totalExposure = activeBets.reduce((sum, r) => sum + r.recommendedStake, 0)

  return {
    totalExposure,
    exposurePct: (totalExposure / bankroll) * 100,
    betsCount: activeBets.length,
  }
}
