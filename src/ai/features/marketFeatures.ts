import { query } from '../../lib/database.js'

export interface MarketFeatures {
  currentOdds: number | null
  oddsMovement: number | null         // positive = money coming (odds shortened)
  oddsMovementPct: number | null      // percentage change from open to latest
  isPlunge: boolean                   // >20% shortening = smart money signal
  lateMoneySteam: number | null       // movement in last 2 snapshots (late market move)
  oddsVolatility: number | null       // std dev of odds changes (market uncertainty)
  marketImpliedProb: number | null    // 1/odds normalized for overround
  isFavourite: boolean
}

export async function computeMarketFeatures(
  horseId: string,
  raceId: string
): Promise<MarketFeatures> {
  // Get ALL odds snapshots for this runner (ordered by time)
  const allSnapshots = await query<{ win_odds: number; observed_at: string }>(`
    SELECT win_odds, observed_at FROM odds_snapshots
    WHERE race_id = $1 AND horse_id = $2 AND win_odds IS NOT NULL
    ORDER BY observed_at ASC
  `, [raceId, horseId])

  // Fallback: use SP from runners table
  const runnerSP = await query<{ sp_decimal: number | null }>(`
    SELECT sp_decimal FROM runners WHERE race_id = $1 AND horse_id = $2
  `, [raceId, horseId])

  const openPrice = allSnapshots.length > 0 ? Number(allSnapshots[0].win_odds) : null
  const latestPrice = allSnapshots.length > 0 ? Number(allSnapshots[allSnapshots.length - 1].win_odds) : null
  const currentOdds = latestPrice ?? (runnerSP[0]?.sp_decimal != null ? Number(runnerSP[0].sp_decimal) : null)

  // --- Odds movement (absolute): positive = shortened = money coming ---
  const oddsMovement = openPrice !== null && currentOdds !== null
    ? openPrice - currentOdds
    : null

  // --- Odds movement percentage ---
  let oddsMovementPct: number | null = null
  if (openPrice !== null && currentOdds !== null && openPrice > 1) {
    oddsMovementPct = ((openPrice - currentOdds) / openPrice) * 100
  }

  // --- Plunge detection: >20% shortening ---
  const isPlunge = oddsMovementPct !== null && oddsMovementPct > 20

  // --- Late money steam: movement in last 2 snapshots ---
  let lateMoneySteam: number | null = null
  if (allSnapshots.length >= 3) {
    const last = Number(allSnapshots[allSnapshots.length - 1].win_odds)
    const prev = Number(allSnapshots[allSnapshots.length - 2].win_odds)
    if (prev > 1) {
      lateMoneySteam = ((prev - last) / prev) * 100  // positive = still shortening
    }
  }

  // --- Odds volatility: std dev of successive changes ---
  let oddsVolatility: number | null = null
  if (allSnapshots.length >= 3) {
    const changes: number[] = []
    for (let i = 1; i < allSnapshots.length; i++) {
      const prev = Number(allSnapshots[i - 1].win_odds)
      const curr = Number(allSnapshots[i].win_odds)
      if (prev > 1) {
        changes.push(((prev - curr) / prev) * 100)
      }
    }
    if (changes.length >= 2) {
      const mean = changes.reduce((a, b) => a + b, 0) / changes.length
      const variance = changes.reduce((a, b) => a + (b - mean) ** 2, 0) / changes.length
      oddsVolatility = Math.sqrt(variance)
    }
  }

  // --- Market implied probability (remove overround by normalizing) ---
  let marketImpliedProb: number | null = null
  if (currentOdds !== null && currentOdds > 1) {
    const allOdds = await query<{ win_odds: number | null; sp_decimal: number | null }>(`
      SELECT
        (SELECT os.win_odds FROM odds_snapshots os WHERE os.race_id = ru.race_id AND os.horse_id = ru.horse_id ORDER BY os.observed_at DESC LIMIT 1) AS win_odds,
        ru.sp_decimal
      FROM runners ru
      WHERE ru.race_id = $1 AND ru.scratched = FALSE
    `, [raceId])

    let totalImplied = 0
    for (const o of allOdds) {
      const odds = o.win_odds != null ? Number(o.win_odds) : (o.sp_decimal != null ? Number(o.sp_decimal) : null)
      if (odds && odds > 1) {
        totalImplied += 1 / odds
      }
    }

    if (totalImplied > 0) {
      marketImpliedProb = (1 / currentOdds) / totalImplied
    } else {
      marketImpliedProb = 1 / currentOdds
    }
  }

  // --- Is favourite ---
  let isFavourite = false
  if (currentOdds !== null) {
    const shortest = await query<{ min_odds: number }>(`
      SELECT MIN(COALESCE(
        (SELECT os.win_odds FROM odds_snapshots os WHERE os.race_id = ru.race_id AND os.horse_id = ru.horse_id ORDER BY os.observed_at DESC LIMIT 1),
        ru.sp_decimal
      )) AS min_odds
      FROM runners ru
      WHERE ru.race_id = $1 AND ru.scratched = FALSE
    `, [raceId])

    isFavourite = shortest[0]?.min_odds !== null && Math.abs(currentOdds - Number(shortest[0].min_odds)) < 0.01
  }

  return {
    currentOdds,
    oddsMovement,
    oddsMovementPct,
    isPlunge,
    lateMoneySteam,
    oddsVolatility,
    marketImpliedProb,
    isFavourite,
  }
}
