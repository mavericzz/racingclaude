/**
 * AI Race Analysis via OpenRouter (Claude)
 *
 * Gathers all available race data from the database, constructs a rich prompt,
 * calls Claude via OpenRouter, and returns structured analysis with learning data.
 */

import { query, pool } from '../lib/database.js'
import { buildLearningContext, formatLearningContext } from './learningContext.js'

export interface AIAnalysisResponse {
  raceId: string
  analysis: string
  aiTopPicks: Array<{ horse_id: string; horse_name: string; confidence: string }> | null
  aiDangers: Array<{ horse_id: string; horse_name: string }> | null
  aiPaceCall: string | null
  keyFactor: string | null
  model: string
  tokensUsed: { prompt: number; completion: number }
  generatedAt: string
  learningBasis: number // how many past analyses inform this one
}

// --- Data gathering ---

interface RaceContext {
  race_id: string
  race_name: string
  race_number: number
  distance_m: number | null
  class: string | null
  going: string | null
  off_time: string | null
  field_size: number
  prize_total: number | null
  venue_name: string
  venue_id: string
  state: string | null
  meeting_date: string
  weather: string | null
  track_condition: string | null
}

interface RunnerProfile {
  horse_id: string
  horse: string
  number: number | null
  draw: number | null
  weight_lbs: number | null
  rating: number | null
  form: string | null
  headgear: string | null
  jockey_claim: number | null
  scratched: boolean
  // Horse info
  sire: string | null
  dam: string | null
  damsire: string | null
  age: string | null
  sex: string | null
  // Connections
  jockey_name: string | null
  trainer_name: string | null
  jockey_id: string | null
  trainer_id: string | null
  // Odds
  current_odds: number | null
  open_odds: number | null
  odds_movement: string | null
  // Stats
  jockey_win_pct: number | null
  trainer_win_pct: number | null
  combo_win_pct: number | null
  // Form stats
  course_win_pct: number | null
  course_distance_win_pct: number | null
  distance_win_pct: number | null
  last10_win_pct: number | null
}

interface FormRun {
  horse_id: string
  race_date: string
  venue: string | null
  distance_m: number | null
  going: string | null
  class: string | null
  position: number | null
  field_size: number | null
  beaten_lengths: number | null
  speed_figure: number | null
  running_style: string | null
  days_since_prev: number | null
}

interface MLPrediction {
  horseId: string
  horseName: string
  rank: number
  winProb: number
  marketOdds: number
  fairOdds: number
  edgePct: number
  verdict: string
}

async function gatherRaceIntelligence(raceId: string) {
  // 1. Race context
  const raceRows = await query<RaceContext>(`
    SELECT r.race_id, r.race_name, r.race_number, r.distance_m, r.class, r.going,
           r.off_time, r.field_size, r.prize_total,
           v.name AS venue_name, v.venue_id, v.state, m.meeting_date::text,
           m.weather, m.track_condition
    FROM races r
    JOIN meetings m ON r.meeting_id = m.meeting_id
    JOIN venues v ON m.venue_id = v.venue_id
    WHERE r.race_id = $1
  `, [raceId])

  if (raceRows.length === 0) throw new Error(`Race ${raceId} not found`)
  const race = raceRows[0]

  // 2. Runner profiles with odds + connections
  const runners = await query<RunnerProfile>(`
    SELECT
      ru.horse_id, ru.horse, ru.number, ru.draw, ru.weight_lbs, ru.rating,
      ru.form, ru.headgear, ru.jockey_claim, ru.scratched,
      h.sire, h.dam, h.damsire, h.age, h.sex,
      j.name AS jockey_name, t.name AS trainer_name,
      ru.jockey_id, ru.trainer_id,
      -- Current odds (latest snapshot or SP)
      COALESCE(
        (SELECT os.win_odds FROM odds_snapshots os
         WHERE os.race_id = ru.race_id AND os.horse_id = ru.horse_id
         ORDER BY os.observed_at DESC LIMIT 1),
        ru.sp_decimal
      )::float AS current_odds,
      -- Opening odds (earliest snapshot)
      (SELECT os.win_odds FROM odds_snapshots os
       WHERE os.race_id = ru.race_id AND os.horse_id = ru.horse_id
       ORDER BY os.observed_at ASC LIMIT 1)::float AS open_odds,
      -- Jockey/trainer stats
      js.win_pct::float AS jockey_win_pct,
      ts.win_pct::float AS trainer_win_pct,
      cs.win_pct::float AS combo_win_pct
    FROM runners ru
    LEFT JOIN horses h ON h.id = ru.horse_id
    LEFT JOIN jockeys j ON ru.jockey_id = j.id
    LEFT JOIN trainers t ON ru.trainer_id = t.id
    LEFT JOIN mv_jockey_stats js ON js.jockey_id = ru.jockey_id
    LEFT JOIN mv_trainer_stats ts ON ts.trainer_id = ru.trainer_id
    LEFT JOIN mv_combo_stats cs ON cs.jockey_id = ru.jockey_id AND cs.trainer_id = ru.trainer_id
    WHERE ru.race_id = $1
    ORDER BY ru.number
  `, [raceId])

  // Add odds movement description
  for (const r of runners) {
    if (r.open_odds && r.current_odds && r.open_odds > 1) {
      const pctChange = ((r.open_odds - r.current_odds) / r.open_odds) * 100
      r.odds_movement = pctChange > 20 ? `PLUNGE ${pctChange.toFixed(0)}% shorter`
        : pctChange > 5 ? `shortened ${pctChange.toFixed(0)}%`
        : pctChange < -20 ? `DRIFTED ${Math.abs(pctChange).toFixed(0)}% longer`
        : pctChange < -5 ? `drifted ${Math.abs(pctChange).toFixed(0)}%`
        : 'steady'
    }
  }

  // Add form stats from runner_form_stats
  const activeRunners = runners.filter(r => !r.scratched)
  for (const r of activeRunners) {
    const stats = await query<{ stat_type: string; total: number; first: number }>(`
      SELECT stat_type, total::int, first::int
      FROM runner_form_stats
      WHERE horse_id = $1 AND race_id = $2
    `, [r.horse_id, raceId])

    for (const s of stats) {
      const winPct = s.total > 0 ? (s.first / s.total) * 100 : null
      if (s.stat_type === 'course') r.course_win_pct = winPct
      else if (s.stat_type === 'course_distance') r.course_distance_win_pct = winPct
      else if (s.stat_type === 'distance') r.distance_win_pct = winPct
      else if (s.stat_type === 'last_ten') r.last10_win_pct = winPct
    }
  }

  // 3. Form history (last 5 per runner via canonical IDs)
  const horseIds = activeRunners.map(r => r.horse_id)
  const ausIds = horseIds.filter(id => id.startsWith('hrs_aus_'))
  const idMappings = ausIds.length > 0
    ? await query<{ aus_id: string; canonical_id: string }>(
        `SELECT aus_id, canonical_id FROM horse_id_map WHERE aus_id = ANY($1)`, [ausIds]
      )
    : []
  const idMap = new Map(idMappings.map(m => [m.aus_id, m.canonical_id]))

  const formHistory: Map<string, FormRun[]> = new Map()
  const maxFormRuns = activeRunners.length > 16 ? 3 : 5 // Cap for large fields

  for (const r of activeRunners) {
    const lookupId = idMap.get(r.horse_id) ?? r.horse_id
    const form = await query<FormRun>(`
      SELECT
        fh.horse_id, fh.race_date::text, fh.venue_name AS venue,
        fh.distance_m, fh.going, fh.class, fh.finish_position AS position,
        fh.field_size, fh.beaten_lengths,
        sf.adjusted_speed_figure AS speed_figure,
        fh.running_style, fh.days_since_prev_run AS days_since_prev
      FROM horse_form_history fh
      LEFT JOIN speed_figures sf ON sf.race_id = fh.race_id AND sf.horse_id = fh.horse_id
      WHERE fh.horse_id = $1
      ORDER BY fh.race_date DESC
      LIMIT $2
    `, [lookupId, maxFormRuns])
    formHistory.set(r.horse_id, form)
  }

  // 4. Speed figures summary per runner
  const speedSummaries: Map<string, { last: number | null; avg: number | null; best: number | null }> = new Map()
  for (const r of activeRunners) {
    const lookupId = idMap.get(r.horse_id) ?? r.horse_id
    const figs = await query<{ fig: number }>(`
      SELECT adjusted_speed_figure AS fig FROM speed_figures
      WHERE horse_id = $1
      ORDER BY (SELECT m.meeting_date FROM races rc JOIN meetings m ON m.meeting_id = rc.meeting_id WHERE rc.race_id = speed_figures.race_id) DESC
      LIMIT 5
    `, [lookupId])
    if (figs.length > 0) {
      const vals = figs.map(f => Number(f.fig))
      speedSummaries.set(r.horse_id, {
        last: vals[0],
        avg: Math.round((vals.reduce((a, b) => a + b, 0) / vals.length) * 10) / 10,
        best: Math.max(...vals),
      })
    }
  }

  // 5. ML predictions (from prediction_results if stored, otherwise empty)
  const mlPreds = await query<MLPrediction>(`
    SELECT pr.horse_id AS "horseId", ru.horse AS "horseName",
           pr.predicted_rank AS rank, pr.predicted_win_prob::float AS "winProb",
           pr.market_odds_at_prediction::float AS "marketOdds",
           CASE WHEN pr.predicted_win_prob > 0 THEN (1.0 / pr.predicted_win_prob::float) ELSE 999 END AS "fairOdds",
           pr.edge_pct::float AS "edgePct",
           pr.verdict
    FROM prediction_results pr
    LEFT JOIN runners ru ON pr.race_id = ru.race_id AND pr.horse_id = ru.horse_id
    WHERE pr.race_id = $1
    ORDER BY pr.predicted_rank
  `, [raceId])

  return { race, runners, formHistory, speedSummaries, mlPredictions: mlPreds }
}

// --- Prompt construction ---

function buildPrompt(
  data: Awaited<ReturnType<typeof gatherRaceIntelligence>>,
  learningContextText: string
): { system: string; user: string } {
  const { race, runners, formHistory, speedSummaries, mlPredictions } = data
  const active = runners.filter(r => !r.scratched)
  const scratched = runners.filter(r => r.scratched)

  const system = `You are an expert Australian thoroughbred horse racing analyst with decades of experience handicapping races. You have access to a statistical ML model's predictions AND your own historical accuracy data. Use both to make better predictions.

RULES:
- When the model has historically struggled in certain conditions (shown in learning context), adjust accordingly
- When you've made mistakes before (shown in learning context), learn from them and avoid repeating
- Consider scratching impacts on pace carefully — if pace leaders are scratched, closers benefit
- Always factor in track condition changes when present
- Be specific and data-driven in your analysis — reference speed figures, form, and stats
- Account for barrier draws, especially at this venue's known biases

IMPORTANT: End your analysis with a JSON block wrapped in \`\`\`json ... \`\`\` containing your structured picks so we can track your accuracy and improve over time.`

  const lines: string[] = []

  // Race header
  lines.push(`# Race: R${race.race_number} ${race.race_name || 'Unknown'} at ${race.venue_name}`)
  lines.push(`Date: ${race.meeting_date} | Distance: ${race.distance_m || '?'}m | Class: ${race.class || '?'} | Going: ${race.going || '?'}`)
  lines.push(`Prize: $${race.prize_total || '?'} | Field Size: ${active.length} runners | Weather: ${race.weather || '?'} | Track: ${race.track_condition || '?'}`)
  lines.push('')

  // Learning context
  if (learningContextText) {
    lines.push(learningContextText)
    lines.push('')
  }

  // Scratchings
  if (scratched.length > 0) {
    lines.push('## SCRATCHINGS')
    for (const s of scratched) {
      lines.push(`- ${s.horse} (barrier ${s.draw ?? '?'})`)
    }
    lines.push('')
  }

  // ML predictions
  if (mlPredictions.length > 0) {
    lines.push('## ML MODEL PREDICTIONS (3-model ensemble: LightGBM + XGBoost + CatBoost)')
    for (const p of mlPredictions) {
      lines.push(`${p.rank}. ${p.horseName} — Win: ${(p.winProb * 100).toFixed(1)}%, Market: $${p.marketOdds?.toFixed(2) ?? '?'}, Fair: $${p.fairOdds?.toFixed(2) ?? '?'}, Edge: ${p.edgePct >= 0 ? '+' : ''}${p.edgePct?.toFixed(1) ?? '?'}%, Verdict: ${p.verdict}`)
    }
    lines.push('')
  }

  // Runner profiles
  lines.push('## RUNNER PROFILES')
  for (const r of active) {
    const weightKg = r.weight_lbs ? (Number(r.weight_lbs) * 0.453592).toFixed(1) : '?'
    lines.push(`\n### #${r.number ?? '?'} ${r.horse} (Bar ${r.draw ?? '?'}, ${weightKg}kg)`)

    // Breeding
    const breeding = [r.sire, r.dam ? `x ${r.dam}` : null, r.damsire ? `by ${r.damsire}` : null]
      .filter(Boolean).join(' ')
    if (breeding) lines.push(`**Breeding**: ${breeding} | Age: ${r.age ?? '?'} | Sex: ${r.sex ?? '?'}`)

    // Connections
    const connParts = []
    if (r.jockey_name) connParts.push(`J: ${r.jockey_name} (${r.jockey_win_pct != null ? Number(r.jockey_win_pct).toFixed(1) + '%W' : '?'})`)
    if (r.trainer_name) connParts.push(`T: ${r.trainer_name} (${r.trainer_win_pct != null ? Number(r.trainer_win_pct).toFixed(1) + '%W' : '?'})`)
    if (r.combo_win_pct != null) connParts.push(`Combo: ${Number(r.combo_win_pct).toFixed(1)}%W`)
    if (connParts.length > 0) lines.push(`**Connections**: ${connParts.join(' | ')}`)

    // Gear & rating
    const extras = []
    if (r.headgear) extras.push(`Headgear: ${r.headgear}`)
    if (r.rating) extras.push(`Rating: ${r.rating}`)
    if (r.jockey_claim) extras.push(`Claim: ${r.jockey_claim}kg`)
    if (extras.length > 0) lines.push(`${extras.join(' | ')}`)

    // Form string
    if (r.form) lines.push(`**Form**: ${r.form}`)

    // Course/distance stats
    const statParts = []
    if (r.course_win_pct != null) statParts.push(`Course: ${r.course_win_pct.toFixed(0)}%`)
    if (r.course_distance_win_pct != null) statParts.push(`C+D: ${r.course_distance_win_pct.toFixed(0)}%`)
    if (r.distance_win_pct != null) statParts.push(`Distance: ${r.distance_win_pct.toFixed(0)}%`)
    if (r.last10_win_pct != null) statParts.push(`Last 10: ${r.last10_win_pct.toFixed(0)}%`)
    if (statParts.length > 0) lines.push(`**Stats**: ${statParts.join(', ')}`)

    // Market
    if (r.current_odds) {
      const mkt = `Current $${Number(r.current_odds).toFixed(2)}`
      const open = r.open_odds ? ` (opened $${Number(r.open_odds).toFixed(2)}, ${r.odds_movement || 'steady'})` : ''
      lines.push(`**Market**: ${mkt}${open}`)
    }

    // Form history
    const form = formHistory.get(r.horse_id) ?? []
    if (form.length > 0) {
      lines.push('**Last runs**:')
      for (const f of form) {
        const parts = [
          f.race_date,
          f.venue || '?',
          `${f.distance_m || '?'}m`,
          f.class || '?',
          f.going || '?',
          `Pos: ${f.position ?? '?'}/${f.field_size ?? '?'}`,
          `BL: ${f.beaten_lengths != null ? Number(f.beaten_lengths).toFixed(1) : '?'}`,
          f.speed_figure != null ? `Speed: ${Number(f.speed_figure).toFixed(0)}` : null,
          f.running_style ? `Style: ${f.running_style}` : null,
          f.days_since_prev != null ? `Spell: ${f.days_since_prev}d` : null,
        ].filter(Boolean)
        lines.push(`  ${parts.join(' | ')}`)
      }
    }

    // Speed summary
    const speed = speedSummaries.get(r.horse_id)
    if (speed) {
      lines.push(`**Speed figs**: Last ${speed.last ?? '?'}, Avg ${speed.avg ?? '?'}, Best ${speed.best ?? '?'}`)
    }
  }

  // Speed rankings
  lines.push('\n## SPEED FIGURE RANKINGS (0-130 scale)')
  const speedRanked = active
    .map(r => ({ name: r.horse, ...speedSummaries.get(r.horse_id) }))
    .filter(r => r.avg != null)
    .sort((a, b) => (b.avg ?? 0) - (a.avg ?? 0))
  for (const s of speedRanked) {
    lines.push(`- ${s.name}: Last ${s.last ?? '?'}, Avg ${s.avg ?? '?'}, Best ${s.best ?? '?'}`)
  }

  // Instructions
  lines.push(`
---

Please provide your analysis:
1. **RACE OVERVIEW** — Track/conditions assessment, how scratchings change the race dynamics
2. **PACE SCENARIO** — Who leads now (accounting for scratchings), predicted tempo, who benefits
3. **KEY CONTENDERS** (top 3-4) — Detailed case for each with strengths/weaknesses
4. **DANGERS** — Horses that could upset at odds
5. **FINAL VERDICT** — Ranked selections with confidence level
6. **BETTING STATEMENT** — One clear actionable recommendation

Then output your structured picks:
\`\`\`json
{
  "top_picks": [{"horse_id": "...", "horse_name": "...", "confidence": "high|medium|low"}],
  "dangers": [{"horse_id": "...", "horse_name": "..."}],
  "pace_call": "fast|slow|even",
  "key_factor": "one-sentence summary of the decisive factor"
}
\`\`\``)

  return { system, user: lines.join('\n') }
}

// --- OpenRouter API call ---

const OPENROUTER_URL = 'https://openrouter.ai/api/v1/chat/completions'
const MODEL = 'anthropic/claude-sonnet-4-20250514'

async function callOpenRouter(
  system: string,
  user: string
): Promise<{ content: string; model: string; tokensUsed: { prompt: number; completion: number } }> {
  const apiKey = process.env.OPENROUTER_API_KEY
  if (!apiKey) throw new Error('OPENROUTER_API_KEY not set')

  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), 30000)

  try {
    const res = await fetch(OPENROUTER_URL, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
        'HTTP-Referer': 'http://localhost:3004',
        'X-Title': 'RacingClaude',
      },
      body: JSON.stringify({
        model: MODEL,
        messages: [
          { role: 'system', content: system },
          { role: 'user', content: user },
        ],
        max_tokens: 2500,
        temperature: 0.3,
      }),
      signal: controller.signal,
    })

    if (!res.ok) {
      const errBody = await res.text().catch(() => '')
      if (res.status === 429) throw new Error('Rate limited by OpenRouter — try again shortly')
      if (res.status === 402) throw new Error('OpenRouter API credits exhausted')
      throw new Error(`OpenRouter API error ${res.status}: ${errBody.slice(0, 200)}`)
    }

    const data = await res.json() as any
    const choice = data.choices?.[0]
    if (!choice?.message?.content) throw new Error('Empty response from OpenRouter')

    return {
      content: choice.message.content,
      model: data.model ?? MODEL,
      tokensUsed: {
        prompt: data.usage?.prompt_tokens ?? 0,
        completion: data.usage?.completion_tokens ?? 0,
      },
    }
  } finally {
    clearTimeout(timeout)
  }
}

// --- Response parsing ---

function parseStructuredPicks(content: string): {
  topPicks: Array<{ horse_id: string; horse_name: string; confidence: string }> | null
  dangers: Array<{ horse_id: string; horse_name: string }> | null
  paceCall: string | null
  keyFactor: string | null
} {
  try {
    const jsonMatch = content.match(/```json\s*([\s\S]*?)```/)
    if (!jsonMatch) return { topPicks: null, dangers: null, paceCall: null, keyFactor: null }

    const parsed = JSON.parse(jsonMatch[1])
    return {
      topPicks: Array.isArray(parsed.top_picks) ? parsed.top_picks : null,
      dangers: Array.isArray(parsed.dangers) ? parsed.dangers : null,
      paceCall: typeof parsed.pace_call === 'string' ? parsed.pace_call : null,
      keyFactor: typeof parsed.key_factor === 'string' ? parsed.key_factor : null,
    }
  } catch {
    return { topPicks: null, dangers: null, paceCall: null, keyFactor: null }
  }
}

// --- Main orchestrator ---

export async function generateAIAnalysis(raceId: string): Promise<AIAnalysisResponse> {
  // Check for cached analysis (return if race hasn't changed)
  const cached = await query<{
    analysis: string; ai_top_picks: any; ai_dangers: any; ai_pace_call: string;
    model_used: string; tokens_prompt: number; tokens_completion: number; created_at: string;
  }>(`
    SELECT analysis, ai_top_picks, ai_dangers, ai_pace_call,
           model_used, tokens_prompt, tokens_completion, created_at
    FROM ai_analyses WHERE race_id = $1
  `, [raceId])

  // If cached and no new scratchings since analysis, return cached
  if (cached.length > 0) {
    const recentScratch = await query<{ cnt: number }>(`
      SELECT COUNT(*)::int AS cnt FROM runners
      WHERE race_id = $1 AND scratched = TRUE
    `, [raceId])

    // Simple cache: if analysis exists, check if field changed
    const currentField = await query<{ cnt: number }>(`
      SELECT COUNT(*)::int AS cnt FROM runners
      WHERE race_id = $1 AND scratched = FALSE
    `, [raceId])

    // Invalidate cache if we have a force flag or field size mismatch
    // For now, return cached to save API calls
    const c = cached[0]
    const learningBasis = await query<{ cnt: number }>(`
      SELECT COUNT(*)::int AS cnt FROM ai_prediction_results
    `)

    return {
      raceId,
      analysis: c.analysis,
      aiTopPicks: c.ai_top_picks,
      aiDangers: c.ai_dangers,
      aiPaceCall: c.ai_pace_call,
      keyFactor: null,
      model: c.model_used,
      tokensUsed: { prompt: c.tokens_prompt, completion: c.tokens_completion },
      generatedAt: c.created_at,
      learningBasis: learningBasis[0]?.cnt ?? 0,
    }
  }

  // Gather all data
  const data = await gatherRaceIntelligence(raceId)

  // Build learning context
  const learningCtx = await buildLearningContext(
    raceId, data.race.venue_id, data.race.distance_m, data.race.going
  )
  const learningText = formatLearningContext(learningCtx)

  // Build prompt
  const { system, user } = buildPrompt(data, learningText)

  // Call OpenRouter
  const response = await callOpenRouter(system, user)

  // Parse structured picks
  const { topPicks, dangers, paceCall, keyFactor } = parseStructuredPicks(response.content)

  // Remove JSON block from display text
  const analysisText = response.content.replace(/```json\s*[\s\S]*?```/, '').trim()

  // Store in database
  await pool.query(`
    INSERT INTO ai_analyses (race_id, analysis, ai_top_picks, ai_dangers, ai_pace_call,
                             model_used, tokens_prompt, tokens_completion)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    ON CONFLICT (race_id) DO UPDATE SET
      analysis = EXCLUDED.analysis,
      ai_top_picks = EXCLUDED.ai_top_picks,
      ai_dangers = EXCLUDED.ai_dangers,
      ai_pace_call = EXCLUDED.ai_pace_call,
      model_used = EXCLUDED.model_used,
      tokens_prompt = EXCLUDED.tokens_prompt,
      tokens_completion = EXCLUDED.tokens_completion,
      created_at = NOW()
  `, [
    raceId, analysisText,
    topPicks ? JSON.stringify(topPicks) : null,
    dangers ? JSON.stringify(dangers) : null,
    paceCall, response.model,
    response.tokensUsed.prompt, response.tokensUsed.completion,
  ])

  // Count learning basis
  const learningBasis = await query<{ cnt: number }>(`
    SELECT COUNT(*)::int AS cnt FROM ai_prediction_results
  `)

  return {
    raceId,
    analysis: analysisText,
    aiTopPicks: topPicks,
    aiDangers: dangers,
    aiPaceCall: paceCall,
    keyFactor,
    model: response.model,
    tokensUsed: response.tokensUsed,
    generatedAt: new Date().toISOString(),
    learningBasis: learningBasis[0]?.cnt ?? 0,
  }
}
