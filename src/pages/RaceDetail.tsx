import { useState, useMemo } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { api, type Prediction, type Runner, type SectionalResponse } from '../lib/api'
import VerdictBadge from '../components/VerdictBadge'
import { cn } from '../lib/cn'
import { ArrowLeft, ChevronRight, Trophy, TrendingUp, Zap, Target, Gauge, Info, Users, BarChart3 } from 'lucide-react'

type Tab = 'runners' | 'edge' | 'speed' | 'results' | 'ai' | 'guide'

function Countdown({ offTime }: { offTime: string }) {
  const diff = new Date(offTime).getTime() - Date.now()
  if (diff <= 0) return <span className="text-red-400 text-sm font-semibold">Started</span>
  const hrs = Math.floor(diff / 3600000)
  const mins = Math.floor((diff % 3600000) / 60000)
  return (
    <div className="flex items-center gap-1.5">
      <div className="bg-zinc-800 border border-zinc-700 rounded-lg px-2.5 py-1.5 text-center min-w-[40px]">
        <div className="text-lg font-bold font-mono text-zinc-100">{hrs}</div>
        <div className="text-[9px] text-zinc-500 uppercase">Hrs</div>
      </div>
      <div className="bg-zinc-800 border border-zinc-700 rounded-lg px-2.5 py-1.5 text-center min-w-[40px]">
        <div className="text-lg font-bold font-mono text-zinc-100">{mins}</div>
        <div className="text-[9px] text-zinc-500 uppercase">Min</div>
      </div>
      <div className="text-[10px] text-zinc-500 ml-1">Until Jump</div>
    </div>
  )
}

function RaceTabs({ races, currentRaceNum }: {
  races: { race_id: string; race_number: number }[]
  currentRaceNum: number | null
}) {
  return (
    <div className="flex items-center gap-1 bg-zinc-900/80 rounded-xl p-1 border border-zinc-800 overflow-x-auto">
      {races.map(r => (
        <Link key={r.race_id} to={`/race/${r.race_id}`}
          className={cn('px-3.5 py-2 rounded-lg text-sm font-semibold transition-all shrink-0',
            r.race_number === currentRaceNum ? 'bg-zinc-700 text-white shadow-sm' : 'text-zinc-500 hover:text-zinc-300 hover:bg-zinc-800'
          )}>
          R{r.race_number}
        </Link>
      ))}
    </div>
  )
}

function RunnerCard({ runner, prediction }: { runner: Runner; prediction?: Prediction }) {
  const odds = runner.sp_decimal ? Number(runner.sp_decimal) : null
  const isValue = prediction && (prediction.verdict === 'strong-value' || prediction.verdict === 'value')
  const winPct = runner.careerRuns && runner.careerRuns > 0 ? ((runner.careerWins ?? 0) / runner.careerRuns * 100) : null

  return (
    <div className={cn('border-b border-zinc-800/50 hover:bg-zinc-800/30 transition-colors', isValue && 'bg-emerald-500/5 hover:bg-emerald-500/10')}>
      <div className="px-4 py-3.5">
        <div className="flex items-start justify-between gap-4">
          <div className="flex items-start gap-3 min-w-0">
            <span className={cn('flex items-center justify-center w-8 h-8 rounded-lg text-sm font-bold shrink-0 mt-0.5',
              prediction?.rank === 1 ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30' :
              prediction?.rank === 2 ? 'bg-emerald-500/10 text-emerald-500/70 border border-emerald-500/20' :
              prediction?.rank === 3 ? 'bg-zinc-800 text-zinc-400 border border-zinc-700' :
              'bg-zinc-800/50 text-zinc-500 border border-zinc-800'
            )}>
              {runner.number || '-'}
            </span>
            <div className="min-w-0">
              <div className="flex items-center gap-2 flex-wrap">
                <h4 className="font-semibold text-zinc-100 text-[15px]">
                  {runner.horse?.replace(' (AUS)', '').replace(' (NZ)', '')}
                </h4>
                {runner.weight_lbs && <span className="text-xs text-zinc-500">({Number(runner.weight_lbs).toFixed(0)}kg)</span>}
                {prediction && <VerdictBadge verdict={prediction.verdict} />}
              </div>
              <div className="flex items-center gap-3 mt-1 text-xs text-zinc-500">
                <span>J: <span className="text-zinc-400">{runner.jockey_name || '-'}</span></span>
                <span>T: <span className="text-zinc-400">{runner.trainer_name || '-'}</span></span>
              </div>
              <div className="flex items-center gap-3 mt-2 flex-wrap">
                {runner.form && (
                  <span className="font-mono text-xs text-zinc-400 bg-zinc-800 px-2 py-0.5 rounded-md border border-zinc-700/50">{runner.form}</span>
                )}
                {runner.lastSpeedFig != null && (
                  <span className={cn('text-xs font-mono px-2 py-0.5 rounded-md border',
                    Number(runner.lastSpeedFig) >= 100 ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20' :
                    Number(runner.lastSpeedFig) >= 90 ? 'bg-zinc-800 text-zinc-400 border-zinc-700/50' :
                    'bg-red-500/10 text-red-400 border-red-500/20'
                  )}>
                    Spd {Number(runner.lastSpeedFig).toFixed(0)}
                  </span>
                )}
                {winPct != null && <span className="text-xs text-zinc-500">{runner.careerWins}-{(runner.careerRuns ?? 0) - (runner.careerWins ?? 0)} ({winPct.toFixed(0)}%)</span>}
              </div>
            </div>
          </div>
          <div className="text-right shrink-0">
            {odds ? (
              <span className={cn('inline-block px-3 py-1.5 rounded-lg font-bold font-mono text-sm border',
                isValue ? 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30' : 'bg-zinc-800 text-zinc-200 border-zinc-700'
              )}>${odds.toFixed(2)}</span>
            ) : <span className="text-zinc-600 text-sm">-</span>}
            {prediction && prediction.edgePct > 0 && (
              <div className="text-xs text-emerald-400 font-semibold mt-1">+{prediction.edgePct.toFixed(1)}% edge</div>
            )}
            {runner.result?.position != null && (
              <div className={cn('mt-1 text-xs font-bold px-2 py-0.5 rounded-md inline-block',
                runner.result.position === 1 ? 'bg-amber-500/20 text-amber-400' :
                runner.result.position <= 3 ? 'bg-blue-500/15 text-blue-400' : 'text-zinc-600'
              )}>
                {runner.result.position === 1 ? 'WON' : `Fin: ${runner.result.position}`}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

function EdgeTable({ predictions, runners }: { predictions: Prediction[]; runners: Runner[] }) {
  const runnerMap = new Map(runners.map(r => [r.horse_id, r]))
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="bg-zinc-800/50 text-zinc-500 text-xs uppercase tracking-wider border-b border-zinc-800">
            <th className="px-4 py-3 text-center font-medium w-12">#</th>
            <th className="px-4 py-3 text-left font-medium">Horse</th>
            <th className="px-4 py-3 text-right font-medium">Win%</th>
            <th className="px-4 py-3 text-right font-medium">Market</th>
            <th className="px-4 py-3 text-right font-medium">Fair</th>
            <th className="px-4 py-3 text-right font-medium">Edge</th>
            <th className="px-4 py-3 text-center font-medium">Verdict</th>
            <th className="px-4 py-3 text-right font-medium">Kelly</th>
            <th className="px-4 py-3 text-center font-medium">Res</th>
          </tr>
        </thead>
        <tbody>
          {predictions.map(pred => {
            const runner = runnerMap.get(pred.horseId)
            const isValue = pred.verdict === 'strong-value' || pred.verdict === 'value'
            const isWinner = runner?.result?.position === 1
            return (
              <tr key={pred.horseId} className={cn('border-b border-zinc-800/50', isValue && 'bg-emerald-500/5', isWinner && 'bg-amber-500/5')}>
                <td className="px-4 py-3 text-center">
                  <span className={cn('inline-flex items-center justify-center w-7 h-7 rounded-lg text-xs font-bold',
                    pred.rank === 1 ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30' :
                    pred.rank === 2 ? 'bg-emerald-500/10 text-emerald-500/70 border border-emerald-500/20' :
                    pred.rank === 3 ? 'bg-zinc-800 text-zinc-400 border border-zinc-700' : 'bg-zinc-800/50 text-zinc-500'
                  )}>{pred.rank}</span>
                </td>
                <td className="px-4 py-3">
                  <p className="font-medium text-zinc-200">{pred.horseName}</p>
                  {runner && <p className="text-xs text-zinc-500">{runner.jockey_name} · {runner.trainer_name}</p>}
                </td>
                <td className="px-4 py-3 text-right">
                  <span className="font-mono font-semibold text-zinc-200">{(pred.winProb * 100).toFixed(1)}%</span>
                  <div className="w-full bg-zinc-800 rounded-full h-1 mt-1 overflow-hidden">
                    <div className="bg-emerald-500 h-1 rounded-full" style={{ width: `${Math.min(pred.winProb * 100 * 3, 100)}%` }} />
                  </div>
                </td>
                <td className="px-4 py-3 text-right font-mono text-zinc-400">${pred.marketOdds.toFixed(2)}</td>
                <td className={cn('px-4 py-3 text-right font-mono font-medium', pred.fairOdds < pred.marketOdds ? 'text-emerald-400' : 'text-red-400')}>
                  ${pred.fairOdds.toFixed(2)}
                </td>
                <td className={cn('px-4 py-3 text-right font-mono font-semibold text-xs',
                  pred.edgePct > 5 ? 'text-emerald-400' : pred.edgePct > 0 ? 'text-zinc-400' : 'text-red-400'
                )}>
                  {pred.edgePct > 0 ? '+' : ''}{pred.edgePct.toFixed(1)}%
                </td>
                <td className="px-4 py-3 text-center"><VerdictBadge verdict={pred.verdict} /></td>
                <td className="px-4 py-3 text-right font-mono text-xs text-zinc-500">
                  {pred.kellyFraction > 0 ? `${(pred.kellyFraction * 100).toFixed(1)}%` : '-'}
                </td>
                <td className="px-4 py-3 text-center">
                  {runner?.result?.position != null ? (
                    <span className={cn('text-xs font-bold px-2 py-0.5 rounded-md',
                      isWinner ? 'bg-amber-500/20 text-amber-400' : (runner.result?.position ?? 99) <= 3 ? 'bg-blue-500/15 text-blue-400' : 'text-zinc-600'
                    )}>{isWinner ? 'WON' : runner.result?.position}</span>
                  ) : <span className="text-zinc-700">-</span>}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function AllRunnersTable({ runners }: { runners: Runner[] }) {
  const active = runners.filter(r => !r.scratched)
  return (
    <div className="overflow-x-auto">
      <div className="px-4 py-3 border-b border-zinc-800/50 flex items-center gap-2">
        <Users className="w-4 h-4 text-zinc-500" />
        <span className="text-sm text-zinc-400">All Runners - Details</span>
        <span className="text-xs text-zinc-600">{active.length} Runners</span>
      </div>
      <table className="w-full text-sm">
        <thead>
          <tr className="bg-zinc-800/50 text-zinc-500 text-xs uppercase tracking-wider border-b border-zinc-800">
            <th className="px-3 py-2.5 text-center font-medium">Bar</th>
            <th className="px-3 py-2.5 text-left font-medium">Horse</th>
            <th className="px-3 py-2.5 text-left font-medium">Jockey</th>
            <th className="px-3 py-2.5 text-left font-medium">Trainer</th>
            <th className="px-3 py-2.5 text-right font-medium">Wt</th>
            <th className="px-3 py-2.5 text-center font-medium">Form</th>
            <th className="px-3 py-2.5 text-center font-medium">Speed</th>
            <th className="px-3 py-2.5 text-center font-medium">Win%</th>
            <th className="px-3 py-2.5 text-right font-medium">Odds</th>
          </tr>
        </thead>
        <tbody>
          {active.map(r => {
            const winPct = r.careerRuns && r.careerRuns > 0 ? ((r.careerWins ?? 0) / r.careerRuns * 100) : null
            return (
              <tr key={r.horse_id} className="border-b border-zinc-800/50 hover:bg-zinc-800/30">
                <td className="px-3 py-2.5 text-center text-zinc-500 font-mono">{r.draw || '-'}</td>
                <td className="px-3 py-2.5">
                  <span className="font-medium text-zinc-200">{r.horse?.replace(' (AUS)', '').replace(' (NZ)', '')}</span>
                  {r.headgear && <span className="ml-1 text-xs text-orange-400">{r.headgear}</span>}
                </td>
                <td className="px-3 py-2.5 text-zinc-400 text-xs">{r.jockey_name || '-'}</td>
                <td className="px-3 py-2.5 text-zinc-400 text-xs">{r.trainer_name || '-'}</td>
                <td className="px-3 py-2.5 text-right text-zinc-400 font-mono text-xs">{r.weight_lbs ? `${Number(r.weight_lbs).toFixed(0)}` : '-'}</td>
                <td className="px-3 py-2.5 text-center font-mono text-xs text-zinc-500">{r.form || '-'}</td>
                <td className="px-3 py-2.5 text-center">
                  {r.lastSpeedFig != null ? (
                    <span className={cn('font-mono text-xs font-semibold',
                      Number(r.lastSpeedFig) >= 100 ? 'text-emerald-400' : Number(r.lastSpeedFig) >= 90 ? 'text-zinc-300' : 'text-red-400'
                    )}>{Number(r.lastSpeedFig).toFixed(0)}</span>
                  ) : <span className="text-zinc-700 text-xs">-</span>}
                </td>
                <td className="px-3 py-2.5 text-center text-xs">
                  {winPct != null ? <span className={cn(winPct >= 20 ? 'text-emerald-400 font-semibold' : 'text-zinc-500')}>{winPct.toFixed(0)}%</span> : <span className="text-zinc-700">-</span>}
                </td>
                <td className="px-3 py-2.5 text-right font-mono font-medium text-zinc-300">{r.sp_decimal ? `$${Number(r.sp_decimal).toFixed(2)}` : '-'}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function SectionalTable({ data }: { data: SectionalResponse }) {
  const [showAdjusted, setShowAdjusted] = useState(false)
  const { sectionals, fieldAverages, adjFieldAverages, avgWeight } = data
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

  return (
    <div>
      <div className="px-4 py-3 border-b border-zinc-800/50 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Gauge className="w-4 h-4 text-emerald-400" />
          <span className="text-sm font-medium text-zinc-200">Sectional Speeds (km/h)</span>
        </div>
        <button onClick={() => setShowAdjusted(!showAdjusted)}
          className={cn('px-3 py-1 text-xs rounded-lg border transition-all font-medium',
            showAdjusted ? 'bg-indigo-500/20 text-indigo-400 border-indigo-500/30' : 'bg-zinc-800 text-zinc-400 border-zinc-700 hover:bg-zinc-700'
          )}>
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
              {labels.map(l => <th key={l} className="px-3 py-2.5 text-right font-medium">{l}</th>)}
            </tr>
          </thead>
          <tbody>
            {sectionals.map(s => (
              <tr key={s.horse_number} className="border-b border-zinc-800/50 hover:bg-zinc-800/30">
                <td className="px-3 py-2 text-zinc-500 font-mono">{s.horse_number}</td>
                <td className="px-3 py-2 font-medium text-zinc-200 truncate max-w-[140px]">{s.horse_name}</td>
                <td className="px-3 py-2 text-right text-xs font-mono text-zinc-400">{s.weight_kg != null ? s.weight_kg.toFixed(1) : '-'}</td>
                {activeSplits.map((split, i) => {
                  const val = s[split] as number | null
                  const avgKey = showAdjusted ? adjSplits[i] : rawSplits[i]
                  return <td key={split} className={cn('px-3 py-2 text-right font-mono text-xs', cellColor(val, activeAvgs[avgKey]))}>{val !== null ? val.toFixed(2) : '-'}</td>
                })}
              </tr>
            ))}
            <tr className="bg-zinc-800/30 font-semibold text-xs">
              <td className="px-3 py-2.5"></td>
              <td className="px-3 py-2.5 text-zinc-400">Field Average</td>
              <td className="px-3 py-2.5 text-right font-mono text-zinc-500">{avgWeight ? avgWeight.toFixed(1) : '-'}</td>
              {activeSplits.map(split => (
                <td key={split} className="px-3 py-2.5 text-right font-mono text-zinc-500">{activeAvgs[split] !== null ? activeAvgs[split]!.toFixed(2) : '-'}</td>
              ))}
            </tr>
          </tbody>
        </table>
      </div>
      {showAdjusted && (
        <div className="px-4 py-2.5 bg-indigo-500/10 border-t border-indigo-500/20 text-xs text-indigo-400 flex items-center gap-2">
          <Info className="w-3.5 h-3.5 shrink-0" />
          Weight-adjusted: +0.5 km/h per kg above field average weight.
        </div>
      )}
    </div>
  )
}

export default function RaceDetail() {
  const { raceId } = useParams<{ raceId: string }>()
  const [activeTab, setActiveTab] = useState<Tab>('runners')

  const { data: race, isLoading: raceLoading } = useQuery({ queryKey: ['race', raceId], queryFn: () => api.getRace(raceId!), enabled: !!raceId })
  const { data: predictions, isLoading: predsLoading } = useQuery({ queryKey: ['predictions', raceId], queryFn: () => api.getPredictions(raceId!), enabled: !!raceId })
  const { data: sectionals } = useQuery({ queryKey: ['sectionals', raceId], queryFn: () => api.getSectionals(raceId!), enabled: !!raceId })
  const { data: analysis } = useQuery({ queryKey: ['analysis', raceId], queryFn: () => api.getRaceAnalysis(raceId!), enabled: !!raceId })

  const { data: meetingRaces } = useQuery({
    queryKey: ['meetings', race?.meeting_date],
    queryFn: () => api.getMeetings(race!.meeting_date),
    enabled: !!race?.meeting_date,
    select: (meetings) => {
      const m = meetings.find(m => m.meeting_id === race?.meeting_id)
      return m?.races.map(r => ({ race_id: r.race_id, race_number: r.race_number ?? 0 })).sort((a, b) => a.race_number - b.race_number) ?? []
    }
  })

  const predMap = useMemo(() => {
    if (!predictions) return new Map<string, Prediction>()
    return new Map(predictions.predictions.map(p => [p.horseId, p]))
  }, [predictions])

  if (raceLoading) return (
    <div className="text-center py-16">
      <div className="inline-flex items-center gap-3 text-zinc-500">
        <div className="w-5 h-5 border-2 border-emerald-500/30 border-t-emerald-500 rounded-full animate-spin" />
        Loading race...
      </div>
    </div>
  )
  if (!race) return <div className="text-center py-16 text-red-400">Race not found</div>

  const active = race.runners.filter(r => !r.scratched)
  const hasResults = race.runners.some(r => r.result?.position != null)
  const topPred = predictions?.predictions[0]
  const valueBets = predictions?.valueBets ?? []
  const currentIdx = meetingRaces?.findIndex(r => r.race_id === raceId) ?? -1
  const nextRace = meetingRaces && currentIdx >= 0 && currentIdx < meetingRaces.length - 1 ? meetingRaces[currentIdx + 1] : null

  const tabs: { key: Tab; label: string; icon?: React.ReactNode }[] = [
    { key: 'runners', label: 'Runners' },
    { key: 'edge', label: 'Edge', icon: <TrendingUp className="w-3.5 h-3.5" /> },
    { key: 'speed', label: 'Speed', icon: <Gauge className="w-3.5 h-3.5" /> },
    { key: 'results', label: 'Results', icon: <Trophy className="w-3.5 h-3.5" /> },
    { key: 'ai', label: 'AI', icon: <BarChart3 className="w-3.5 h-3.5" /> },
    { key: 'guide', label: 'Guide' },
  ]

  return (
    <div>
      {/* Back + Race Tabs */}
      <div className="flex items-center justify-between mb-4 gap-4">
        <Link to={`/?date=${race.meeting_date}`} className="flex items-center gap-1.5 text-sm text-zinc-500 hover:text-zinc-300 transition-colors shrink-0">
          <ArrowLeft className="w-4 h-4" /> Back to Dashboard
        </Link>
        {meetingRaces && meetingRaces.length > 1 && <RaceTabs races={meetingRaces} currentRaceNum={race.race_number} />}
        {nextRace && (
          <Link to={`/race/${nextRace.race_id}`} className="flex items-center gap-1.5 px-3 py-2 bg-emerald-600 text-white rounded-lg text-sm font-medium hover:bg-emerald-500 transition-colors shrink-0">
            Next: R{nextRace.race_number} <ChevronRight className="w-4 h-4" />
          </Link>
        )}
      </div>

      {/* Race Header */}
      <div className="card overflow-hidden mb-6">
        <div className="bg-gradient-to-r from-zinc-900 via-zinc-900 to-zinc-800 px-6 py-5">
          <div className="flex items-start justify-between">
            <div>
              <div className="flex items-center gap-3 mb-2">
                <span className="inline-flex items-center justify-center w-12 h-12 rounded-xl bg-emerald-600 text-white text-xl font-bold">R{race.race_number}</span>
                <div>
                  <h1 className="text-xl font-bold text-zinc-100">{race.venue_name}</h1>
                  <p className="text-sm text-zinc-500">{race.race_name || 'Race'}</p>
                </div>
              </div>
              <div className="flex flex-wrap gap-2 mt-3">
                {race.distance_m && <span className="inline-flex items-center bg-emerald-500/15 text-emerald-400 px-2.5 py-1 rounded-lg text-xs font-semibold border border-emerald-500/20">{race.distance_m}m</span>}
                {race.class && <span className="inline-flex items-center bg-zinc-800 text-zinc-300 px-2.5 py-1 rounded-lg text-xs border border-zinc-700/50">{race.class}</span>}
                {race.going && <span className="inline-flex items-center bg-zinc-800 text-zinc-400 px-2.5 py-1 rounded-lg text-xs border border-zinc-700/50">{race.going}</span>}
                {race.prize_total && <span className="inline-flex items-center bg-amber-500/15 text-amber-400 px-2.5 py-1 rounded-lg text-xs font-semibold border border-amber-500/20">${race.prize_total.toLocaleString()}</span>}
              </div>
            </div>
            {race.off_time && <Countdown offTime={race.off_time} />}
          </div>
        </div>
        <div className="grid grid-cols-4 divide-x divide-zinc-800 border-t border-zinc-800">
          <div className="px-4 py-3 text-center">
            <div className="text-xl font-bold text-zinc-100">{active.length}</div>
            <div className="text-[11px] text-zinc-500">Runners</div>
          </div>
          <div className="px-4 py-3 text-center">
            <div className="text-xl font-bold text-zinc-100">{race.prize_total ? `$${(race.prize_total / 1000).toFixed(0)}k` : '-'}</div>
            <div className="text-[11px] text-zinc-500">Prize</div>
          </div>
          <div className="px-4 py-3 text-center">
            <div className="text-xl font-bold text-zinc-100">{race.field_size}</div>
            <div className="text-[11px] text-zinc-500">Field Size</div>
          </div>
          <div className="px-4 py-3 text-center">
            <div className={cn('text-xl font-bold', hasResults ? 'text-zinc-400' : 'text-emerald-400')}>{hasResults ? 'Final' : 'Upcoming'}</div>
            <div className="text-[11px] text-zinc-500">Status</div>
          </div>
        </div>
      </div>

      {/* Verdict + Confidence */}
      {topPred && (
        <div className="flex items-center gap-4 mb-6 flex-wrap">
          <VerdictBadge verdict={topPred.verdict} />
          <span className="text-sm text-zinc-400">Top Pick: <span className="text-zinc-200 font-semibold">{topPred.horseName}</span></span>
          <div className="flex items-center gap-2 text-sm">
            <span className="text-zinc-500">Confidence</span>
            <div className="w-24 bg-zinc-800 rounded-full h-2 overflow-hidden">
              <div className="bg-emerald-500 h-2 rounded-full" style={{ width: `${Math.min(topPred.winProb * 100 * 3, 100)}%` }} />
            </div>
            <span className="text-zinc-400 font-mono text-xs">{(topPred.winProb * 100).toFixed(0)}%</span>
          </div>
        </div>
      )}

      {/* Value Bets */}
      {valueBets.length > 0 && (
        <div className="card border-emerald-500/30 bg-emerald-500/5 px-5 py-4 mb-6">
          <div className="flex items-center gap-2 mb-3">
            <Zap className="w-4 h-4 text-emerald-400" />
            <h3 className="font-semibold text-emerald-300 text-sm">Value Bets Found</h3>
          </div>
          <div className="grid gap-2 sm:grid-cols-2">
            {valueBets.map(vb => (
              <div key={vb.horseId} className="flex items-center justify-between bg-zinc-900/80 rounded-lg px-4 py-2.5 border border-emerald-500/15">
                <div>
                  <span className="font-semibold text-zinc-200 text-sm">{vb.horseName}</span>
                  <span className="ml-2 text-xs text-zinc-500 font-mono">${vb.marketOdds.toFixed(2)}</span>
                </div>
                <span className="text-emerald-400 font-semibold text-xs">+{vb.edgePct.toFixed(1)}% edge</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Post-race Analysis */}
      {hasResults && analysis?.analysis && (() => {
        const a = analysis.analysis
        return (
          <div className="card px-5 py-4 mb-6">
            <div className="flex items-center gap-2 mb-3"><Target className="w-4 h-4 text-emerald-400" /><h3 className="font-semibold text-zinc-200 text-sm">Post-Race Analysis</h3></div>
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
              <div className="bg-zinc-800/50 rounded-lg p-3 border border-zinc-700/50">
                <p className="text-[10px] text-zinc-500 uppercase mb-0.5">Top Pick</p>
                <p className={cn('text-lg font-bold', a.top_pick_won ? 'text-amber-400' : 'text-zinc-400')}>{a.top_pick_won ? 'WON' : a.top_pick_position ? `${a.top_pick_position}th` : '-'}</p>
              </div>
              <div className="bg-zinc-800/50 rounded-lg p-3 border border-zinc-700/50">
                <p className="text-[10px] text-zinc-500 uppercase mb-0.5">Value Bets</p>
                <p className={cn('text-lg font-bold', a.value_bets_won > 0 ? 'text-emerald-400' : 'text-zinc-400')}>{a.value_bets_count > 0 ? `${a.value_bets_won}/${a.value_bets_count}` : 'None'}</p>
              </div>
              <div className="bg-zinc-800/50 rounded-lg p-3 border border-zinc-700/50">
                <p className="text-[10px] text-zinc-500 uppercase mb-0.5">P&L</p>
                <p className={cn('text-lg font-bold font-mono', a.race_pnl >= 0 ? 'text-emerald-400' : 'text-red-400')}>{a.race_pnl >= 0 ? '+' : ''}{a.race_pnl.toFixed(1)}</p>
              </div>
              {a.pace_scenario && (
                <div className="bg-zinc-800/50 rounded-lg p-3 border border-zinc-700/50">
                  <p className="text-[10px] text-zinc-500 uppercase mb-0.5">Pace</p>
                  <p className="text-lg font-bold text-zinc-400 capitalize">{a.pace_scenario}</p>
                </div>
              )}
            </div>
          </div>
        )
      })()}

      {/* Content Tabs */}
      <div className="card overflow-hidden">
        <div className="flex items-center gap-1 px-4 py-2 bg-zinc-900/50 border-b border-zinc-800 overflow-x-auto">
          {tabs.map(tab => (
            <button key={tab.key} onClick={() => setActiveTab(tab.key)}
              className={cn('flex items-center gap-1.5 px-4 py-2 rounded-lg text-sm font-medium transition-all shrink-0',
                activeTab === tab.key ? 'bg-emerald-600 text-white shadow-sm' : 'text-zinc-500 hover:text-zinc-300 hover:bg-zinc-800'
              )}>
              {tab.icon}{tab.label}
            </button>
          ))}
        </div>

        <div>
          {activeTab === 'runners' && (
            <div>
              <div className="px-4 py-3 border-b border-zinc-800/50 flex items-center justify-between">
                <span className="text-sm text-zinc-300 font-medium">Field ({active.length} starters)</span>
                {predsLoading && (
                  <div className="flex items-center gap-2 text-xs text-zinc-500">
                    <div className="w-3 h-3 border border-emerald-500/30 border-t-emerald-500 rounded-full animate-spin" />
                    Loading predictions...
                  </div>
                )}
              </div>
              {active.map(runner => <RunnerCard key={runner.horse_id} runner={runner} prediction={predMap.get(runner.horse_id)} />)}
            </div>
          )}
          {activeTab === 'edge' && predictions && <EdgeTable predictions={predictions.predictions} runners={race.runners} />}
          {activeTab === 'edge' && !predictions && <div className="px-4 py-10 text-center text-zinc-600">{predsLoading ? 'Loading...' : 'No predictions'}</div>}
          {activeTab === 'speed' && sectionals && sectionals.sectionals.length > 0 && <SectionalTable data={sectionals} />}
          {activeTab === 'speed' && (!sectionals || sectionals.sectionals.length === 0) && <div className="px-4 py-10 text-center text-zinc-600">No sectional data</div>}
          {activeTab === 'results' && hasResults && <AllRunnersTable runners={race.runners} />}
          {activeTab === 'results' && !hasResults && <div className="px-4 py-10 text-center text-zinc-600">Race not yet run</div>}
          {activeTab === 'ai' && predictions && (
            <div className="p-4">
              <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
                {predictions.predictions.slice(0, 6).map(pred => (
                  <div key={pred.horseId} className="bg-zinc-800/50 rounded-lg p-4 border border-zinc-700/50">
                    <div className="flex items-center justify-between mb-2">
                      <span className="font-semibold text-zinc-200">{pred.horseName}</span>
                      <VerdictBadge verdict={pred.verdict} />
                    </div>
                    <div className="space-y-1.5 text-xs">
                      <div className="flex justify-between"><span className="text-zinc-500">Win Prob</span><span className="font-mono text-zinc-300">{(pred.winProb * 100).toFixed(1)}%</span></div>
                      <div className="flex justify-between"><span className="text-zinc-500">Market / Fair</span>
                        <span className="font-mono"><span className="text-zinc-400">${pred.marketOdds.toFixed(2)}</span><span className="text-zinc-600 mx-1">/</span><span className={pred.fairOdds < pred.marketOdds ? 'text-emerald-400' : 'text-red-400'}>${pred.fairOdds.toFixed(2)}</span></span>
                      </div>
                      <div className="flex justify-between"><span className="text-zinc-500">Edge</span><span className={cn('font-mono font-semibold', pred.edgePct > 0 ? 'text-emerald-400' : 'text-red-400')}>{pred.edgePct > 0 ? '+' : ''}{pred.edgePct.toFixed(1)}%</span></div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
          {activeTab === 'guide' && <AllRunnersTable runners={race.runners} />}
        </div>
      </div>
    </div>
  )
}
