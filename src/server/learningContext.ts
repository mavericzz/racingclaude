/**
 * Learning Context Builder
 *
 * Builds the "memory" that makes Claude smarter over time by querying
 * historical accuracy data, venue patterns, scratching impacts, and
 * past mistakes to feed back into the AI analysis prompt.
 */

import { query } from '../lib/database.js'

export interface LearningContext {
  modelAccuracy: string
  venuePatterns: string
  scratchingImpact: string
  trackConditionDrift: string
  aiPerformance: string
  modelDisagreements: string
}

/**
 * Build the full learning context for a race analysis prompt.
 */
export async function buildLearningContext(
  raceId: string,
  venueId: string,
  distanceM: number | null,
  going: string | null
): Promise<LearningContext> {
  const [modelAccuracy, venuePatterns, scratchingImpact, trackConditionDrift, aiPerformance, modelDisagreements] =
    await Promise.all([
      getModelAccuracyByCondition(going, distanceM),
      getVenuePatterns(venueId, distanceM),
      getScratchingImpact(raceId),
      getTrackConditionDrift(raceId),
      getAIPerformance(),
      getModelDisagreements(),
    ])

  return { modelAccuracy, venuePatterns, scratchingImpact, trackConditionDrift, aiPerformance, modelDisagreements }
}

/**
 * Format learning context into prompt text.
 */
export function formatLearningContext(ctx: LearningContext): string {
  const sections: string[] = []

  if (ctx.modelAccuracy) sections.push(`### Model Accuracy by Condition\n${ctx.modelAccuracy}`)
  if (ctx.venuePatterns) sections.push(`### Venue-Specific Patterns\n${ctx.venuePatterns}`)
  if (ctx.scratchingImpact) sections.push(`### Scratching Impact\n${ctx.scratchingImpact}`)
  if (ctx.trackConditionDrift) sections.push(`### Track Condition Changes\n${ctx.trackConditionDrift}`)
  if (ctx.aiPerformance) sections.push(`### Your Recent Performance (Self-Reflection)\n${ctx.aiPerformance}`)
  if (ctx.modelDisagreements) sections.push(`### Model vs Market Disagreements\n${ctx.modelDisagreements}`)

  return sections.length > 0
    ? `## YOUR LEARNING CONTEXT (Historical Performance)\n\n${sections.join('\n\n')}`
    : ''
}

// --- Individual context builders ---

async function getModelAccuracyByCondition(going: string | null, distanceM: number | null): Promise<string> {
  const lines: string[] = []

  // Overall model accuracy
  const overall = await query<{ total: number; wins: number }>(`
    SELECT COUNT(*)::int AS total, COUNT(*) FILTER (WHERE top_pick_won)::int AS wins
    FROM race_analysis
  `)
  if (overall[0]?.total > 0) {
    const rate = ((overall[0].wins / overall[0].total) * 100).toFixed(1)
    lines.push(`- Overall ML model top-pick win rate: ${rate}% (${overall[0].wins}/${overall[0].total} races)`)
  }

  // Accuracy by going/track condition
  if (going) {
    const goingKey = going.toLowerCase().includes('heavy') ? 'Heavy'
      : going.toLowerCase().includes('soft') ? 'Soft'
      : going.toLowerCase().includes('good') ? 'Good'
      : going.toLowerCase().includes('firm') ? 'Firm'
      : null

    if (goingKey) {
      const byGoing = await query<{ total: number; wins: number }>(`
        SELECT COUNT(*)::int AS total, COUNT(*) FILTER (WHERE ra.top_pick_won)::int AS wins
        FROM race_analysis ra
        JOIN races r ON r.race_id = ra.race_id
        WHERE r.going ILIKE $1
      `, [`%${goingKey}%`])
      if (byGoing[0]?.total >= 5) {
        const rate = ((byGoing[0].wins / byGoing[0].total) * 100).toFixed(1)
        lines.push(`- Model accuracy on ${goingKey} tracks: ${rate}% (${byGoing[0].wins}/${byGoing[0].total})`)
      }
    }
  }

  // Accuracy by distance band
  if (distanceM) {
    const distBand = distanceM <= 1200 ? 'sprint (≤1200m)'
      : distanceM <= 1600 ? 'middle (1201-1600m)'
      : 'staying (1601m+)'
    const distMin = distanceM <= 1200 ? 0 : distanceM <= 1600 ? 1201 : 1601
    const distMax = distanceM <= 1200 ? 1200 : distanceM <= 1600 ? 1600 : 99999

    const byDist = await query<{ total: number; wins: number }>(`
      SELECT COUNT(*)::int AS total, COUNT(*) FILTER (WHERE ra.top_pick_won)::int AS wins
      FROM race_analysis ra
      JOIN races r ON r.race_id = ra.race_id
      WHERE r.distance_m BETWEEN $1 AND $2
    `, [distMin, distMax])
    if (byDist[0]?.total >= 5) {
      const rate = ((byDist[0].wins / byDist[0].total) * 100).toFixed(1)
      lines.push(`- Model accuracy at ${distBand}: ${rate}% (${byDist[0].wins}/${byDist[0].total})`)
    }
  }

  // Accuracy by field size
  const byFieldSize = await query<{ bucket: string; total: number; wins: number }>(`
    SELECT
      CASE WHEN r.field_size <= 8 THEN 'small (≤8)'
           WHEN r.field_size <= 12 THEN 'medium (9-12)'
           ELSE 'large (13+)' END AS bucket,
      COUNT(*)::int AS total,
      COUNT(*) FILTER (WHERE ra.top_pick_won)::int AS wins
    FROM race_analysis ra
    JOIN races r ON r.race_id = ra.race_id
    GROUP BY bucket
    HAVING COUNT(*) >= 5
  `)
  for (const row of byFieldSize) {
    const rate = ((row.wins / row.total) * 100).toFixed(1)
    lines.push(`- Model accuracy in ${row.bucket} fields: ${rate}% (${row.wins}/${row.total})`)
  }

  // Check for stored learning insights about model weaknesses
  const insights = await query<{ insight_key: string; insight_data: any }>(`
    SELECT insight_key, insight_data FROM ai_learning_insights
    WHERE insight_type = 'model_weakness' AND sample_size >= 5
    ORDER BY updated_at DESC LIMIT 3
  `)
  for (const ins of insights) {
    const d = ins.insight_data
    if (d.note) lines.push(`- INSIGHT: ${d.note}`)
  }

  return lines.join('\n')
}

async function getVenuePatterns(venueId: string, distanceM: number | null): Promise<string> {
  const lines: string[] = []

  const distBucket = !distanceM ? null
    : distanceM <= 1100 ? 'sprint'
    : distanceM <= 1400 ? 'short'
    : distanceM <= 1800 ? 'mile'
    : distanceM <= 2200 ? 'middle'
    : 'staying'

  // Track bias: running style wins
  const bias = await query<{
    total_races: number; leader_wins: number; on_pace_wins: number;
    mid_wins: number; back_wins: number;
    inside_draw_wins: number; middle_draw_wins: number; outside_draw_wins: number;
  }>(`
    SELECT
      SUM(total_races)::int AS total_races,
      SUM(leader_wins)::int AS leader_wins,
      SUM(on_pace_wins)::int AS on_pace_wins,
      SUM(mid_wins)::int AS mid_wins,
      SUM(back_wins)::int AS back_wins,
      SUM(inside_draw_wins)::int AS inside_draw_wins,
      SUM(middle_draw_wins)::int AS middle_draw_wins,
      SUM(outside_draw_wins)::int AS outside_draw_wins
    FROM mv_track_bias
    WHERE venue_id = $1 ${distBucket ? 'AND distance_bucket = $2' : ''}
  `, distBucket ? [venueId, distBucket] : [venueId])

  if (bias[0]?.total_races > 5) {
    const b = bias[0]
    const total = b.total_races
    const stylePcts = [
      `Leaders ${((b.leader_wins / total) * 100).toFixed(0)}%`,
      `On-pace ${((b.on_pace_wins / total) * 100).toFixed(0)}%`,
      `Midfield ${((b.mid_wins / total) * 100).toFixed(0)}%`,
      `Backmarkers ${((b.back_wins / total) * 100).toFixed(0)}%`,
    ]
    lines.push(`- Running style win rates at this venue: ${stylePcts.join(', ')} (${total} races)`)

    const totalDrawWins = b.inside_draw_wins + b.middle_draw_wins + b.outside_draw_wins
    if (totalDrawWins > 0) {
      const drawPcts = [
        `Inside ${((b.inside_draw_wins / totalDrawWins) * 100).toFixed(0)}%`,
        `Middle ${((b.middle_draw_wins / totalDrawWins) * 100).toFixed(0)}%`,
        `Outside ${((b.outside_draw_wins / totalDrawWins) * 100).toFixed(0)}%`,
      ]
      lines.push(`- Draw bias: ${drawPcts.join(', ')}`)
    }
  }

  // Barrier stats
  const barriers = await query<{ barrier_group: string; win_pct: number; total_runs: number }>(`
    SELECT barrier_group, win_pct::float, total_runs::int
    FROM mv_barrier_stats
    WHERE venue_id = $1 ${distBucket ? 'AND distance_bucket = $2' : ''}
    ORDER BY win_pct DESC
  `, distBucket ? [venueId, distBucket] : [venueId])

  if (barriers.length > 0) {
    const barrierLines = barriers.map(b =>
      `${b.barrier_group}: ${Number(b.win_pct).toFixed(1)}% win rate (${b.total_runs} runs)`
    )
    lines.push(`- Barrier groups: ${barrierLines.join(', ')}`)
  }

  return lines.join('\n')
}

async function getScratchingImpact(raceId: string): Promise<string> {
  const lines: string[] = []

  // Get scratched runners with their form data
  const scratched = await query<{
    horse: string; draw: number; running_style: string | null;
  }>(`
    SELECT ru.horse, ru.draw,
      (SELECT fh.running_style FROM horse_form_history fh
       WHERE fh.horse_id = COALESCE(
         (SELECT canonical_id FROM horse_id_map WHERE aus_id = ru.horse_id),
         ru.horse_id
       )
       ORDER BY fh.race_date DESC LIMIT 1) AS running_style
    FROM runners ru
    WHERE ru.race_id = $1 AND ru.scratched = TRUE
  `, [raceId])

  if (scratched.length === 0) {
    return ''
  }

  lines.push(`- ${scratched.length} horse(s) scratched from this race:`)
  for (const s of scratched) {
    const style = s.running_style || 'unknown style'
    lines.push(`  - ${s.horse} (barrier ${s.draw}, ${style})`)
  }

  // Pace recount: remaining runners by style
  const paceCount = await query<{ running_style: string; cnt: number }>(`
    SELECT COALESCE(
      (SELECT fh.running_style FROM horse_form_history fh
       WHERE fh.horse_id = COALESCE(
         (SELECT canonical_id FROM horse_id_map WHERE aus_id = ru.horse_id),
         ru.horse_id
       )
       ORDER BY fh.race_date DESC LIMIT 1),
      'unknown'
    ) AS running_style,
    COUNT(*)::int AS cnt
    FROM runners ru
    WHERE ru.race_id = $1 AND ru.scratched = FALSE
    GROUP BY running_style
  `, [raceId])

  if (paceCount.length > 0) {
    const paceStr = paceCount.map(p => `${p.running_style}: ${p.cnt}`).join(', ')
    lines.push(`- Remaining field pace composition: ${paceStr}`)

    const leaderCount = paceCount.find(p => p.running_style === 'leader')?.cnt ?? 0
    if (leaderCount === 0) {
      lines.push(`- WARNING: No clear leaders remain — expect slow/contested pace`)
    } else if (leaderCount >= 3) {
      lines.push(`- NOTE: ${leaderCount} leaders in field — expect hot pace favouring closers`)
    }
  }

  return lines.join('\n')
}

async function getTrackConditionDrift(raceId: string): Promise<string> {
  const drift = await query<{
    track_condition: string | null; going: string | null;
  }>(`
    SELECT m.track_condition, r.going
    FROM races r
    JOIN meetings m ON m.meeting_id = r.meeting_id
    WHERE r.race_id = $1
  `, [raceId])

  if (drift.length === 0) return ''

  const { track_condition, going } = drift[0]
  if (!track_condition || !going) return ''

  // Normalize for comparison
  const tc = track_condition.toLowerCase()
  const g = going.toLowerCase()

  if (tc !== g && tc.length > 0 && g.length > 0) {
    return `- Track was posted as "${track_condition}" but current going is "${going}" — conditions have changed.\n` +
      `- Consider which horses benefit from this shift (e.g., wet-track specialists if deteriorating).`
  }

  return ''
}

async function getAIPerformance(): Promise<string> {
  const lines: string[] = []

  // Recent AI accuracy
  const recent = await query<{
    total: number; wins: number; pace_correct: number;
  }>(`
    SELECT
      COUNT(*)::int AS total,
      COUNT(*) FILTER (WHERE ai_top_pick_won)::int AS wins,
      COUNT(*) FILTER (WHERE pace_call_correct)::int AS pace_correct
    FROM ai_prediction_results
    WHERE analyzed_at > NOW() - INTERVAL '30 days'
  `)

  if (recent[0]?.total > 0) {
    const r = recent[0]
    const winRate = ((r.wins / r.total) * 100).toFixed(1)
    const paceRate = ((r.pace_correct / r.total) * 100).toFixed(1)
    lines.push(`- Your last 30 days: ${r.wins}/${r.total} top picks won (${winRate}%), ${r.pace_correct}/${r.total} pace calls correct (${paceRate}%)`)
  }

  // Check for systematic errors
  const errors = await query<{ insight_key: string; insight_data: any }>(`
    SELECT insight_key, insight_data FROM ai_learning_insights
    WHERE insight_type = 'systematic_error'
    ORDER BY updated_at DESC LIMIT 3
  `)
  for (const err of errors) {
    if (err.insight_data?.note) {
      lines.push(`- PAST MISTAKE: ${err.insight_data.note}`)
    }
  }

  // Compare AI vs ML accuracy
  const comparison = await query<{
    ai_wins: number; ml_wins: number; total: number;
  }>(`
    SELECT
      COUNT(*) FILTER (WHERE ap.ai_top_pick_won)::int AS ai_wins,
      COUNT(*) FILTER (WHERE ra.top_pick_won)::int AS ml_wins,
      COUNT(*)::int AS total
    FROM ai_prediction_results ap
    JOIN race_analysis ra ON ra.race_id = ap.race_id
  `)
  if (comparison[0]?.total >= 5) {
    const c = comparison[0]
    if (c.ai_wins > c.ml_wins) {
      lines.push(`- You are outperforming the ML model: AI ${c.ai_wins} wins vs ML ${c.ml_wins} wins (${c.total} shared races)`)
    } else if (c.ml_wins > c.ai_wins) {
      lines.push(`- ML model is outperforming you: ML ${c.ml_wins} wins vs AI ${c.ai_wins} wins (${c.total} shared races) — consider trusting the model more`)
    }
  }

  return lines.join('\n')
}

async function getModelDisagreements(): Promise<string> {
  // Check stored insights about model-market disagreements
  const insights = await query<{ insight_key: string; insight_data: any }>(`
    SELECT insight_key, insight_data FROM ai_learning_insights
    WHERE insight_type = 'model_market_disagreement'
    ORDER BY updated_at DESC LIMIT 1
  `)

  if (insights.length > 0 && insights[0].insight_data?.note) {
    return `- ${insights[0].insight_data.note}`
  }

  // Fallback: compute on the fly from recent data
  const disagreements = await query<{ total: number; model_right: number; market_right: number }>(`
    SELECT
      COUNT(*)::int AS total,
      COUNT(*) FILTER (WHERE pr.predicted_rank = 1 AND pr.actual_position = 1)::int AS model_right,
      COUNT(*) FILTER (WHERE pr.predicted_rank > 3 AND pr.actual_position = 1
        AND pr.market_odds_at_prediction = (
          SELECT MIN(pr2.market_odds_at_prediction)
          FROM prediction_results pr2
          WHERE pr2.race_id = pr.race_id
        ))::int AS market_right
    FROM prediction_results pr
    WHERE pr.actual_position IS NOT NULL
      AND pr.predicted_rank <= 5
  `)

  if (disagreements[0]?.total >= 10) {
    return `- When ML model disagrees with market favourite: track both signals carefully`
  }

  return ''
}
