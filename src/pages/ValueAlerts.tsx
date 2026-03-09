import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { api, type PredictionResponse } from '../lib/api'
import VerdictBadge from '../components/VerdictBadge'
import { cn } from '../lib/cn'
import { Zap, Calendar, ChevronRight, DollarSign, TrendingUp, Target } from 'lucide-react'

function formatDate(d: Date): string {
  return d.toISOString().slice(0, 10)
}

export default function ValueAlerts() {
  const [date, setDate] = useState(formatDate(new Date()))

  const { data: meetings, isLoading: meetingsLoading } = useQuery({
    queryKey: ['meetings', date],
    queryFn: () => api.getMeetings(date),
  })

  const allRaceIds = meetings?.flatMap(m => m.races.map(r => r.race_id)) ?? []

  const { data: allPredictions, isLoading: predsLoading } = useQuery({
    queryKey: ['all-predictions', date, allRaceIds.length],
    queryFn: async () => {
      const results: Array<PredictionResponse & { raceName: string; venueName: string; raceNumber: number | null }> = []
      const raceInfo = meetings!.flatMap(m =>
        m.races.map(r => ({
          raceId: r.race_id,
          raceName: r.race_name,
          venueName: m.venue_name,
          raceNumber: r.race_number,
        }))
      )
      const batchSize = 5
      for (let i = 0; i < raceInfo.length; i += batchSize) {
        const batch = raceInfo.slice(i, i + batchSize)
        const batchResults = await Promise.all(
          batch.map(async info => {
            try {
              const preds = await api.getPredictions(info.raceId)
              return { ...preds, raceName: info.raceName, venueName: info.venueName, raceNumber: info.raceNumber }
            } catch {
              return null
            }
          })
        )
        results.push(...batchResults.filter(Boolean) as typeof results)
      }
      return results
    },
    enabled: allRaceIds.length > 0,
  })

  const valueBetRaces = allPredictions?.filter(p => p.valueBets.length > 0) ?? []
  const totalValueBets = valueBetRaces.reduce((sum, r) => sum + r.valueBets.length, 0)

  return (
    <div>
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-6">
        <div>
          <h1 className="text-2xl font-bold text-zinc-100 flex items-center gap-2">
            <Zap className="w-6 h-6 text-emerald-400" />
            Value Alerts
          </h1>
          <p className="text-sm text-zinc-500 mt-1">
            {predsLoading ? (
              <span className="flex items-center gap-2">
                <div className="w-3 h-3 border border-emerald-500/30 border-t-emerald-500 rounded-full animate-spin" />
                Scanning races...
              </span>
            ) : (
              <span>
                <span className="text-emerald-400 font-semibold">{totalValueBets}</span> value bets across{' '}
                <span className="text-zinc-300">{valueBetRaces.length}</span> races
              </span>
            )}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <div className="relative">
            <Calendar className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-500 pointer-events-none" />
            <input
              type="date"
              value={date}
              onChange={e => setDate(e.target.value)}
              className="pl-9 pr-3 py-2 text-sm bg-zinc-800 border border-zinc-700 rounded-lg text-zinc-200 focus:outline-none focus:border-emerald-500/50 focus:ring-1 focus:ring-emerald-500/20"
            />
          </div>
          <button
            onClick={() => setDate(formatDate(new Date()))}
            className="btn-primary"
          >
            Today
          </button>
        </div>
      </div>

      {/* Loading states */}
      {meetingsLoading && (
        <div className="text-center py-16">
          <div className="inline-flex items-center gap-3 text-zinc-500">
            <div className="w-5 h-5 border-2 border-emerald-500/30 border-t-emerald-500 rounded-full animate-spin" />
            Loading meetings...
          </div>
        </div>
      )}
      {predsLoading && meetings && (
        <div className="text-center py-16">
          <div className="inline-flex items-center gap-3 text-zinc-500">
            <div className="w-5 h-5 border-2 border-emerald-500/30 border-t-emerald-500 rounded-full animate-spin" />
            Analyzing {allRaceIds.length} races...
          </div>
        </div>
      )}

      {valueBetRaces.length === 0 && !predsLoading && !meetingsLoading && (
        <div className="text-center py-16 text-zinc-600">
          <Zap className="w-10 h-10 mx-auto mb-3 text-zinc-700" />
          No value bets found for {date}
        </div>
      )}

      {/* Value bet cards */}
      <div className="space-y-4">
        {valueBetRaces.map(race => (
          <div key={race.raceId} className="card overflow-hidden animate-slide-up">
            <Link
              to={`/race/${race.raceId}`}
              className="group block px-5 py-3.5 bg-zinc-800/50 border-b border-zinc-800 hover:bg-zinc-800/80 transition-colors"
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span className="text-sm font-semibold text-zinc-200">
                    {race.venueName}
                  </span>
                  <span className="text-sm font-mono text-emerald-400">R{race.raceNumber || '?'}</span>
                  <span className="text-sm text-zinc-500">{race.raceName}</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-xs text-emerald-400 font-medium bg-emerald-500/10 px-2 py-0.5 rounded-md border border-emerald-500/20">
                    {race.valueBets.length} value bet{race.valueBets.length > 1 ? 's' : ''}
                  </span>
                  <ChevronRight className="w-4 h-4 text-zinc-600 group-hover:text-zinc-400 transition-colors" />
                </div>
              </div>
            </Link>
            <div className="divide-y divide-zinc-800/50">
              {race.valueBets.map(vb => (
                <div key={vb.horseId} className="flex items-center justify-between px-5 py-3.5 hover:bg-zinc-800/20 transition-colors">
                  <div className="flex items-center gap-3">
                    <VerdictBadge verdict={vb.verdict} />
                    <div>
                      <span className="font-medium text-zinc-200">{vb.horseName}</span>
                      <span className="ml-2 text-sm text-zinc-500">
                        Win: <span className="text-zinc-300 font-mono">{(vb.winProb * 100).toFixed(1)}%</span>
                      </span>
                    </div>
                  </div>
                  <div className="flex items-center gap-5 text-sm">
                    <div className="text-right">
                      <span className="text-[11px] text-zinc-600 uppercase tracking-wider">Odds</span>
                      <p className="font-mono font-semibold text-zinc-300">${vb.marketOdds.toFixed(1)}</p>
                    </div>
                    <div className="text-right">
                      <span className="text-[11px] text-zinc-600 uppercase tracking-wider">Fair</span>
                      <p className="font-mono font-semibold text-emerald-400">${vb.fairOdds.toFixed(1)}</p>
                    </div>
                    <div className="text-right">
                      <span className="text-[11px] text-zinc-600 uppercase tracking-wider">Edge</span>
                      <p className="font-mono font-semibold text-emerald-400">+{vb.edgePct.toFixed(1)}%</p>
                    </div>
                    <div className="text-right">
                      <span className="text-[11px] text-zinc-600 uppercase tracking-wider">Stake</span>
                      <p className="font-mono font-semibold text-zinc-200">${vb.recommendedStake.toFixed(0)}</p>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
