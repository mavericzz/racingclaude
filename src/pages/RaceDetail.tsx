import { useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { api, type Prediction, type Runner, type SectionalResponse } from '../lib/api'
import VerdictBadge from '../components/VerdictBadge'
import { cn } from '../lib/cn'
import { ArrowLeft, Trophy, TrendingUp, TrendingDown, Target, Gauge, ChevronRight, Zap, Info } from 'lucide-react'

function PredictionRow({ pred, runner }: { pred: Prediction; runner?: Runner }) {
  const isValue = pred.verdict === 'strong-value' || pred.verdict === 'value'
  const hasResult = runner?.result?.position != null
  const isWinner = hasResult && runner?.result?.position === 1

  return (
    <div className={cn(
      'grid grid-cols-12 gap-2 items-center px-4 py-3 border-b border-zinc-800/50 text-sm transition-colors',
      isValue && 'bg-emerald-500/5',
      isWinner && 'bg-amber-500/5'
    )}>
      <div className="col-span-1 text-center">
        <span className={cn(
          'inline-flex items-center justify-center w-7 h-7 rounded-lg text-xs font-bold',
          pred.rank === 1 ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30' :
          pred.rank === 2 ? 'bg-emerald-500/10 text-emerald-500/70 border border-emerald-500/20' :
          pred.rank === 3 ? 'bg-zinc-800 text-zinc-400 border border-zinc-700' :
          'bg-zinc-800/50 text-zinc-500 border border-zinc-800'
        )}>
          {pred.rank}
        </span>
      </div>

      <div className="col-span-3 min-w-0">
        <p className="font-medium text-zinc-200 truncate">{pred.horseName}</p>
        {runner && (
          <p className="text-xs text-zinc-500 truncate">
            {runner.jockey_name || ''}
            {runner.trainer_name ? ` · ${runner.trainer_name}` : ''}
          </p>
        )}
      </div>

      <div className="col-span-2 text-right">
        <span className="font-mono font-semibold text-zinc-200">
          {(pred.winProb * 100).toFixed(1)}%
        </span>
        <div className="w-full bg-zinc-800 rounded-full h-1.5 mt-1 overflow-hidden">
          <div
            className="bg-gradient-to-r from-emerald-600 to-emerald-400 h-1.5 rounded-full transition-all duration-500"
            style={{ width: `${Math.min(pred.winProb * 100 * 3, 100)}%` }}
          />
        </div>
      </div>

      <div className="col-span-2 text-center">
        <span className="text-zinc-400 font-mono">${pred.marketOdds.toFixed(1)}</span>
        <span className="text-zinc-700 mx-1">/</span>
        <span className={cn(
          'font-mono font-medium',
          pred.fairOdds < pred.marketOdds ? 'text-emerald-400' : 'text-red-400'
        )}>
          ${pred.fairOdds.toFixed(1)}
        </span>
      </div>

      <div className="col-span-1 text-right">
        <span className={cn(
          'font-mono text-xs font-semibold',
          pred.edgePct > 5 ? 'text-emerald-400' :
          pred.edgePct > 0 ? 'text-zinc-400' :
          'text-red-400'
        )}>
          {pred.edgePct > 0 ? '+' : ''}{pred.edgePct.toFixed(1)}%
        </span>
      </div>

      <div className="col-span-2 text-center">
        <VerdictBadge verdict={pred.verdict} />
      </div>

      <div className="col-span-1 text-center">
        {hasResult ? (
          <span className={cn(
            'text-xs font-bold px-2 py-0.5 rounded-md',
            isWinner ? 'bg-amber-500/20 text-amber-400 border border-amber-500/30' :
            (runner?.result?.position ?? 99) <= 3 ? 'bg-blue-500/15 text-blue-400' :
            'text-zinc-600'
          )}>
            {isWinner ? 'WON' : `${runner?.result?.position}`}
          </span>
        ) : (
          <span className="text-xs text-zinc-700">-</span>
        )}
      </div>
    </div>
  )
}

function RunnerTable({ runners }: { runners: Runner[] }) {
  const activeRunners = runners.filter(r => !r.scratched)

  return (
    <div className="card overflow-hidden">
      <div className="card-header">
        <h3 className="font-semibold text-zinc-200">Full Form Guide</h3>
        <span className="text-xs text-zinc-500">{activeRunners.length} runners</span>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-zinc-800/50 text-zinc-500 text-xs uppercase tracking-wider">
              <th className="px-3 py-2.5 text-left font-medium">No.</th>
              <th className="px-3 py-2.5 text-left font-medium">Horse</th>
              <th className="px-3 py-2.5 text-left font-medium">Jockey</th>
              <th className="px-3 py-2.5 text-left font-medium">Trainer</th>
              <th className="px-3 py-2.5 text-right font-medium">Wt</th>
              <th className="px-3 py-2.5 text-right font-medium">Drw</th>
              <th className="px-3 py-2.5 text-right font-medium">SP</th>
              <th className="px-3 py-2.5 text-center font-medium">Speed</th>
              <th className="px-3 py-2.5 text-center font-medium">Record</th>
              <th className="px-3 py-2.5 text-left font-medium">Form</th>
              <th className="px-3 py-2.5 text-center font-medium">Res</th>
            </tr>
          </thead>
          <tbody>
            {activeRunners.map(r => {
              const winPct = r.careerRuns && r.careerRuns > 0
                ? ((r.careerWins ?? 0) / r.careerRuns * 100) : null
              return (
                <tr key={r.horse_id} className="border-b border-zinc-800/50 hover:bg-zinc-800/30 transition-colors">
                  <td className="px-3 py-2.5 text-zinc-500 font-mono">{r.number || '-'}</td>
                  <td className="px-3 py-2.5">
                    <div>
                      <span className="font-medium text-zinc-200">
                        {r.horse?.replace(' (AUS)', '').replace(' (NZ)', '')}
                      </span>
                      {r.headgear && <span className="ml-1 text-xs text-orange-400 font-medium">{r.headgear}</span>}
                    </div>
                    {r.rating && <span className="text-xs text-zinc-600">Rtg: {r.rating}</span>}
                  </td>
                  <td className="px-3 py-2.5">
                    <div className="text-zinc-400 truncate max-w-28">
                      {r.jockey_name || '-'}
                      {r.jockey_claim ? <sup className="text-red-400 ml-0.5">({r.jockey_claim})</sup> : null}
                    </div>
                    {r.jockeyWinPct != null && (
                      <span className="text-xs text-blue-400/70">{Number(r.jockeyWinPct).toFixed(0)}% W</span>
                    )}
                  </td>
                  <td className="px-3 py-2.5">
                    <div className="text-zinc-400 truncate max-w-28">{r.trainer_name || '-'}</div>
                    {r.trainerWinPct != null && (
                      <span className="text-xs text-purple-400/70">{Number(r.trainerWinPct).toFixed(0)}% W</span>
                    )}
                  </td>
                  <td className="px-3 py-2.5 text-right text-zinc-400 font-mono text-xs">
                    {r.weight_lbs ? `${Number(r.weight_lbs).toFixed(1)}kg` : '-'}
                  </td>
                  <td className="px-3 py-2.5 text-right text-zinc-400 font-mono">{r.draw || '-'}</td>
                  <td className="px-3 py-2.5 text-right font-mono font-medium text-zinc-300">
                    {r.sp_decimal ? `$${Number(r.sp_decimal).toFixed(1)}` : '-'}
                  </td>
                  <td className="px-3 py-2.5 text-center">
                    {r.lastSpeedFig != null ? (
                      <div>
                        <span className={cn(
                          'font-mono font-semibold text-xs',
                          Number(r.lastSpeedFig) >= 100 ? 'text-emerald-400' :
                          Number(r.lastSpeedFig) >= 90 ? 'text-zinc-300' :
                          'text-red-400'
                        )}>
                          {Number(r.lastSpeedFig).toFixed(0)}
                        </span>
                        {r.avgSpeedFig != null && (
                          <span className="text-xs text-zinc-600 ml-0.5">
                            ({Number(r.avgSpeedFig).toFixed(0)})
                          </span>
                        )}
                      </div>
                    ) : (
                      <span className="text-xs text-zinc-700">-</span>
                    )}
                  </td>
                  <td className="px-3 py-2.5 text-center text-xs">
                    {r.careerRuns != null && r.careerRuns > 0 ? (
                      <span className="text-zinc-400">
                        {r.careerWins}-{(r.careerRuns ?? 0) - (r.careerWins ?? 0)}-{r.careerRuns}
                        {winPct != null && (
                          <span className={cn(
                            'ml-1',
                            winPct >= 20 ? 'text-emerald-400 font-semibold' : 'text-zinc-600'
                          )}>
                            {winPct.toFixed(0)}%
                          </span>
                        )}
                      </span>
                    ) : (
                      <span className="text-zinc-700">-</span>
                    )}
                  </td>
                  <td className="px-3 py-2.5 text-zinc-500 font-mono text-xs tracking-wide">
                    {r.form || '-'}
                  </td>
                  <td className="px-3 py-2.5 text-center">
                    {r.result ? (
                      <span className={cn(
                        'font-bold text-xs',
                        r.result.position === 1 ? 'text-amber-400' :
                        r.result.position <= 3 ? 'text-blue-400' : 'text-zinc-600'
                      )}>
                        {r.result.position === 1 ? 'WON' : r.result.position}
                      </span>
                    ) : '-'}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function SectionalTable({ data }: { data: SectionalResponse }) {
  const [showAdjusted, setShowAdjusted] = useState(false)
  const { sectionals, fieldAverages, adjFieldAverages, raceContext, avgWeight } = data

  const rawSplits = ['speed_800m', 'speed_600m', 'speed_400m', 'speed_200m', 'speed_finish', 'speed_avg'] as const
  const adjSplits = ['adj_speed_800m', 'adj_speed_600m', 'adj_speed_400m', 'adj_speed_200m', 'adj_speed_finish', 'adj_speed_avg'] as const
  const labels = ['800m', '600m', '400m', '200m', 'Finish', 'Avg']

  const activeSplits = showAdjusted ? adjSplits : rawSplits
  const activeAvgs = showAdjusted ? adjFieldAverages : fieldAverages

  function cellColor(val: number | null, avg: number | null) {
    if (val === null || avg === null) return ''
    const diff = val - avg
    if (diff > 1) return 'text-emerald-400 bg-emerald-500/10 font-semibold'
    if (diff > 0) return 'text-emerald-500/80'
    if (diff < -1) return 'text-red-400 bg-red-500/10'
    if (diff < 0) return 'text-red-500/70'
    return ''
  }

  function weightBadge(diff: number | null) {
    if (diff === null) return null
    const abs = Math.abs(diff)
    if (abs < 0.5) return <span className="text-zinc-600 text-[10px]">avg</span>
    return (
      <span className={cn(
        'text-[10px] font-medium',
        diff > 0 ? 'text-red-400' : 'text-emerald-400'
      )}>
        {diff > 0 ? '+' : ''}{diff.toFixed(1)}
      </span>
    )
  }

  return (
    <div className="card overflow-hidden mb-6">
      <div className="card-header">
        <div>
          <h3 className="font-semibold text-zinc-200 flex items-center gap-2">
            <Gauge className="w-4 h-4 text-emerald-400" />
            Sectional Speeds
            <span className="text-xs text-zinc-500 font-normal">(km/h)</span>
          </h3>
          {raceContext && (
            <p className="text-xs text-zinc-500 mt-0.5">
              {raceContext.venue} · {raceContext.distance_m}m · {raceContext.going || raceContext.trackCondition || 'N/A'}
              {raceContext.class ? ` · ${raceContext.class}` : ''}
              {avgWeight ? ` · Avg wt: ${avgWeight.toFixed(1)}kg` : ''}
            </p>
          )}
        </div>
        <button
          onClick={() => setShowAdjusted(!showAdjusted)}
          className={cn(
            'px-3 py-1.5 text-xs rounded-lg border transition-all duration-200 font-medium',
            showAdjusted
              ? 'bg-indigo-500/20 text-indigo-400 border-indigo-500/30'
              : 'bg-zinc-800 text-zinc-400 border-zinc-700 hover:bg-zinc-700'
          )}
        >
          {showAdjusted ? 'Wt-Adjusted' : 'Raw'}
        </button>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-zinc-800/50 text-zinc-500 text-xs uppercase tracking-wider">
              <th className="px-3 py-2.5 text-left font-medium">No.</th>
              <th className="px-3 py-2.5 text-left font-medium">Horse</th>
              <th className="px-3 py-2.5 text-right font-medium">Wt</th>
              <th className="px-3 py-2.5 text-right font-medium">Bar</th>
              {labels.map(l => <th key={l} className="px-3 py-2.5 text-right font-medium">{l}</th>)}
              <th className="px-3 py-2.5 text-right font-medium">Odds</th>
            </tr>
          </thead>
          <tbody>
            {sectionals.map(s => (
              <tr key={s.horse_number} className="border-b border-zinc-800/50 hover:bg-zinc-800/30 transition-colors">
                <td className="px-3 py-2 text-zinc-500 font-mono">{s.horse_number}</td>
                <td className="px-3 py-2 font-medium text-zinc-200 truncate max-w-[140px]">{s.horse_name}</td>
                <td className="px-3 py-2 text-right text-xs">
                  {s.weight_kg != null ? (
                    <div className="flex flex-col items-end">
                      <span className="font-mono text-zinc-400">{s.weight_kg.toFixed(1)}</span>
                      {weightBadge(s.weight_diff_kg)}
                    </div>
                  ) : '-'}
                </td>
                <td className="px-3 py-2 text-right font-mono text-xs text-zinc-500">
                  {s.barrier ?? '-'}
                </td>
                {activeSplits.map((split, i) => {
                  const val = s[split] as number | null
                  const avgKey = showAdjusted ? adjSplits[i] : rawSplits[i]
                  return (
                    <td key={split} className={cn('px-3 py-2 text-right font-mono text-xs', cellColor(val, activeAvgs[avgKey]))}>
                      {val !== null ? val.toFixed(2) : '-'}
                    </td>
                  )
                })}
                <td className="px-3 py-2 text-right font-mono text-xs text-zinc-500">
                  {s.scraper_odds !== null ? `$${s.scraper_odds.toFixed(1)}` : '-'}
                </td>
              </tr>
            ))}
            <tr className="bg-zinc-800/30 font-semibold text-xs">
              <td className="px-3 py-2.5"></td>
              <td className="px-3 py-2.5 text-zinc-400">Field Average</td>
              <td className="px-3 py-2.5 text-right font-mono text-zinc-500">
                {avgWeight ? avgWeight.toFixed(1) : '-'}
              </td>
              <td className="px-3 py-2.5"></td>
              {activeSplits.map(split => (
                <td key={split} className="px-3 py-2.5 text-right font-mono text-zinc-500">
                  {activeAvgs[split] !== null ? activeAvgs[split]!.toFixed(2) : '-'}
                </td>
              ))}
              <td className="px-3 py-2.5"></td>
            </tr>
          </tbody>
        </table>
      </div>
      {showAdjusted && (
        <div className="px-4 py-2.5 bg-indigo-500/10 border-t border-indigo-500/20 text-xs text-indigo-400 flex items-center gap-2">
          <Info className="w-3.5 h-3.5 shrink-0" />
          Weight-adjusted: +0.5 km/h per kg above field average weight. Heavier horses get credit for carrying more.
        </div>
      )}
    </div>
  )
}

export default function RaceDetail() {
  const { raceId } = useParams<{ raceId: string }>()

  const { data: race, isLoading: raceLoading } = useQuery({
    queryKey: ['race', raceId],
    queryFn: () => api.getRace(raceId!),
    enabled: !!raceId,
  })

  const { data: predictions, isLoading: predsLoading } = useQuery({
    queryKey: ['predictions', raceId],
    queryFn: () => api.getPredictions(raceId!),
    enabled: !!raceId,
  })

  const { data: sectionals } = useQuery({
    queryKey: ['sectionals', raceId],
    queryFn: () => api.getSectionals(raceId!),
    enabled: !!raceId,
  })

  const { data: analysis } = useQuery({
    queryKey: ['analysis', raceId],
    queryFn: () => api.getRaceAnalysis(raceId!),
    enabled: !!raceId,
  })

  if (raceLoading) return (
    <div className="text-center py-16">
      <div className="inline-flex items-center gap-3 text-zinc-500">
        <div className="w-5 h-5 border-2 border-emerald-500/30 border-t-emerald-500 rounded-full animate-spin" />
        Loading race...
      </div>
    </div>
  )
  if (!race) return <div className="text-center py-16 text-red-400">Race not found</div>

  const runnerMap = new Map(race.runners.map(r => [r.horse_id, r]))
  const hasResults = race.runners.some(r => r.result?.position != null)

  const active = race.runners.filter(r => !r.scratched)
  const withSpeed = active.filter(r => r.lastSpeedFig != null).length
  const withForm = active.filter(r => r.careerRuns != null && r.careerRuns > 0).length

  return (
    <div>
      {/* Breadcrumb */}
      <Link
        to={`/?date=${race.meeting_date}`}
        className="inline-flex items-center gap-1.5 text-sm text-zinc-500 hover:text-zinc-300 transition-colors mb-4"
      >
        <ArrowLeft className="w-4 h-4" />
        {race.venue_name} - {new Date(race.meeting_date + 'T00:00:00').toLocaleDateString('en-AU', {
          weekday: 'long', day: 'numeric', month: 'long', year: 'numeric'
        })}
      </Link>

      {/* Race header */}
      <div className="card px-6 py-5 mb-6">
        <div className="flex items-start justify-between">
          <div>
            <h1 className="text-xl font-bold text-zinc-100">
              {race.race_number ? (
                <span className="text-emerald-400 mr-2">R{race.race_number}</span>
              ) : null}
              {race.race_name || 'Race'}
            </h1>
            <div className="flex flex-wrap gap-3 mt-3">
              {race.distance_m && (
                <span className="inline-flex items-center gap-1.5 bg-zinc-800 text-zinc-300 px-2.5 py-1 rounded-lg text-sm border border-zinc-700/50">
                  {race.distance_m}m
                </span>
              )}
              {race.class && (
                <span className="inline-flex items-center bg-zinc-800 text-zinc-400 px-2.5 py-1 rounded-lg text-sm border border-zinc-700/50">
                  {race.class}
                </span>
              )}
              {race.going && (
                <span className="inline-flex items-center bg-zinc-800 text-zinc-400 px-2.5 py-1 rounded-lg text-sm border border-zinc-700/50">
                  {race.going}
                </span>
              )}
              <span className="inline-flex items-center bg-zinc-800 text-zinc-400 px-2.5 py-1 rounded-lg text-sm border border-zinc-700/50">
                {race.field_size} runners
              </span>
              {race.prize_total && (
                <span className="inline-flex items-center bg-zinc-800 text-zinc-400 px-2.5 py-1 rounded-lg text-sm border border-zinc-700/50">
                  ${race.prize_total.toLocaleString()}
                </span>
              )}
            </div>
            <div className="flex gap-4 mt-3 text-xs text-zinc-600">
              <span>Speed figs: <span className="text-zinc-400">{withSpeed}/{active.length}</span></span>
              <span>Form data: <span className="text-zinc-400">{withForm}/{active.length}</span></span>
            </div>
          </div>
          {race.off_time && (
            <span className="text-lg font-mono font-semibold text-zinc-400 bg-zinc-800 px-3 py-1.5 rounded-lg border border-zinc-700">
              {race.off_time.slice(11, 16) || race.off_time.slice(0, 5)}
            </span>
          )}
        </div>
      </div>

      {/* Post-race analysis */}
      {hasResults && analysis?.analysis && (() => {
        const a = analysis.analysis
        return (
          <div className="card px-6 py-5 mb-6">
            <h2 className="font-semibold text-zinc-200 mb-4 flex items-center gap-2">
              <Target className="w-4 h-4 text-emerald-400" />
              Post-Race Analysis
            </h2>
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
              <div className="bg-zinc-800/50 rounded-lg p-3 border border-zinc-700/50">
                <p className="text-[11px] text-zinc-500 uppercase tracking-wider mb-1">Top Pick</p>
                <p className={cn('text-xl font-bold', a.top_pick_won ? 'text-amber-400' : 'text-zinc-400')}>
                  {a.top_pick_won ? 'WON' : a.top_pick_position ? `${a.top_pick_position}${a.top_pick_position === 2 ? 'nd' : a.top_pick_position === 3 ? 'rd' : 'th'}` : '-'}
                </p>
              </div>
              <div className="bg-zinc-800/50 rounded-lg p-3 border border-zinc-700/50">
                <p className="text-[11px] text-zinc-500 uppercase tracking-wider mb-1">Value Bets</p>
                <p className={cn('text-xl font-bold', a.value_bets_won > 0 ? 'text-emerald-400' : 'text-zinc-400')}>
                  {a.value_bets_count > 0 ? `${a.value_bets_won}/${a.value_bets_count} won` : 'None'}
                </p>
              </div>
              <div className="bg-zinc-800/50 rounded-lg p-3 border border-zinc-700/50">
                <p className="text-[11px] text-zinc-500 uppercase tracking-wider mb-1">Race P&L</p>
                <p className={cn('text-xl font-bold font-mono', a.race_pnl >= 0 ? 'text-emerald-400' : 'text-red-400')}>
                  {a.race_pnl >= 0 ? '+' : ''}{a.race_pnl.toFixed(1)}
                </p>
              </div>
              {a.pace_scenario && (
                <div className="bg-zinc-800/50 rounded-lg p-3 border border-zinc-700/50">
                  <p className="text-[11px] text-zinc-500 uppercase tracking-wider mb-1">Pace</p>
                  <p className={cn(
                    'text-xl font-bold capitalize',
                    a.pace_scenario === 'fast' ? 'text-red-400' :
                    a.pace_scenario === 'slow' ? 'text-blue-400' : 'text-zinc-400'
                  )}>
                    {a.pace_scenario}
                  </p>
                </div>
              )}
            </div>
          </div>
        )
      })()}

      {/* Value bets highlight */}
      {predictions && predictions.valueBets.length > 0 && (
        <div className="card border-emerald-500/30 bg-emerald-500/5 px-6 py-5 mb-6 glow-emerald">
          <h2 className="font-semibold text-emerald-300 mb-3 flex items-center gap-2">
            <Zap className="w-4 h-4" />
            Value Bets Found
          </h2>
          <div className="grid gap-3 sm:grid-cols-2">
            {predictions.valueBets.map(vb => (
              <div key={vb.horseId} className="flex items-center justify-between bg-zinc-900/80 rounded-lg px-4 py-3 border border-emerald-500/15">
                <div>
                  <span className="font-semibold text-zinc-200">{vb.horseName}</span>
                  <span className="ml-2 text-sm text-zinc-500 font-mono">${vb.marketOdds.toFixed(1)}</span>
                </div>
                <div className="text-right">
                  <span className="text-emerald-400 font-semibold text-sm">
                    +{vb.edgePct.toFixed(1)}% edge
                  </span>
                  <p className="text-xs text-zinc-500">
                    Kelly: {(vb.kellyFraction * 100).toFixed(1)}% · ${vb.recommendedStake.toFixed(0)} stake
                  </p>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* AI Predictions */}
      <div className="card overflow-hidden mb-6">
        <div className="card-header">
          <h3 className="font-semibold text-zinc-200 flex items-center gap-2">
            <TrendingUp className="w-4 h-4 text-emerald-400" />
            AI Predictions
          </h3>
          {predsLoading && (
            <div className="flex items-center gap-2 text-xs text-zinc-500">
              <div className="w-3 h-3 border border-emerald-500/30 border-t-emerald-500 rounded-full animate-spin" />
              Loading...
            </div>
          )}
        </div>

        <div className="grid grid-cols-12 gap-2 px-4 py-2.5 bg-zinc-800/50 text-xs text-zinc-500 uppercase font-medium tracking-wider border-b border-zinc-800">
          <div className="col-span-1 text-center">#</div>
          <div className="col-span-3">Horse</div>
          <div className="col-span-2 text-right">Win%</div>
          <div className="col-span-2 text-center">Mkt / Fair</div>
          <div className="col-span-1 text-right">Edge</div>
          <div className="col-span-2 text-center">Verdict</div>
          <div className="col-span-1 text-center">Res</div>
        </div>

        {predictions?.predictions.map(pred => (
          <PredictionRow
            key={pred.horseId}
            pred={pred}
            runner={runnerMap.get(pred.horseId)}
          />
        ))}

        {!predictions && !predsLoading && (
          <div className="px-4 py-10 text-center text-zinc-600">No predictions available</div>
        )}
      </div>

      {/* Sectional Speeds */}
      {sectionals && sectionals.sectionals.length > 0 && (
        <SectionalTable data={sectionals} />
      )}

      {/* Form Guide */}
      <RunnerTable runners={race.runners} />
    </div>
  )
}
