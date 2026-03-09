import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { api } from '../lib/api'
import { cn } from '../lib/cn'
import { BarChart3, Trophy, Target, DollarSign, TrendingUp, ArrowUpRight, ArrowDownRight } from 'lucide-react'

function StatCard({ label, value, sub, color, icon: Icon }: {
  label: string; value: string | number; sub?: string; color?: string; icon?: typeof Trophy
}) {
  return (
    <div className="card p-5 group hover:border-zinc-700 transition-colors">
      <div className="flex items-center justify-between mb-3">
        <p className="text-xs text-zinc-500 uppercase tracking-wider font-medium">{label}</p>
        {Icon && <Icon className="w-4 h-4 text-zinc-700 group-hover:text-zinc-600 transition-colors" />}
      </div>
      <p className={cn('text-2xl font-bold', color || 'text-zinc-100')}>{value}</p>
      {sub && <p className="text-xs text-zinc-600 mt-1.5">{sub}</p>}
    </div>
  )
}

export default function Analysis() {
  const [days, setDays] = useState(7)

  const { data, isLoading } = useQuery({
    queryKey: ['analysis-summary', days],
    queryFn: () => api.getAnalysisSummary(days),
  })

  const s = data?.summary

  return (
    <div>
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-6">
        <div>
          <h1 className="text-2xl font-bold text-zinc-100 flex items-center gap-2">
            <BarChart3 className="w-6 h-6 text-emerald-400" />
            Model Performance
          </h1>
          <p className="text-sm text-zinc-500 mt-1">Track prediction accuracy and betting returns</p>
        </div>
        <div className="flex items-center bg-zinc-800/50 rounded-lg border border-zinc-700/50 p-1">
          {[1, 7, 14, 30].map(d => (
            <button
              key={d}
              onClick={() => setDays(d)}
              className={cn(
                'px-4 py-1.5 text-sm rounded-md font-medium transition-all duration-200',
                days === d
                  ? 'bg-emerald-600 text-white shadow-md shadow-emerald-900/30'
                  : 'text-zinc-500 hover:text-zinc-300'
              )}
            >
              {d}d
            </button>
          ))}
        </div>
      </div>

      {isLoading && (
        <div className="text-center py-16">
          <div className="inline-flex items-center gap-3 text-zinc-500">
            <div className="w-5 h-5 border-2 border-emerald-500/30 border-t-emerald-500 rounded-full animate-spin" />
            Loading...
          </div>
        </div>
      )}

      {s && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
          <StatCard
            label="Top Pick Win Rate"
            value={s.top_pick_win_rate ? `${s.top_pick_win_rate}%` : '-'}
            sub={`${s.top_pick_wins} / ${s.total_races} races`}
            color={s.top_pick_win_rate >= 20 ? 'text-emerald-400' : 'text-zinc-200'}
            icon={Trophy}
          />
          <StatCard
            label="Value Bet Strike"
            value={s.value_bet_strike_rate ? `${s.value_bet_strike_rate}%` : '-'}
            sub={`${s.value_bets_won} / ${s.value_bets_total} bets`}
            color={s.value_bet_strike_rate >= 20 ? 'text-emerald-400' : 'text-zinc-200'}
            icon={Target}
          />
          <StatCard
            label="Cumulative P&L"
            value={s.cumulative_pnl >= 0 ? `+$${s.cumulative_pnl.toFixed(0)}` : `-$${Math.abs(s.cumulative_pnl).toFixed(0)}`}
            sub={`$${s.total_staked.toFixed(0)} staked`}
            color={s.cumulative_pnl >= 0 ? 'text-emerald-400' : 'text-red-400'}
            icon={DollarSign}
          />
          <StatCard
            label="ROI"
            value={s.roi_pct ? `${s.roi_pct > 0 ? '+' : ''}${s.roi_pct}%` : '-'}
            sub={`$${s.total_return.toFixed(0)} return`}
            color={s.roi_pct && s.roi_pct >= 0 ? 'text-emerald-400' : 'text-red-400'}
            icon={TrendingUp}
          />
        </div>
      )}

      {/* Recent races table */}
      {data?.recentRaces && data.recentRaces.length > 0 && (
        <div className="card overflow-hidden">
          <div className="card-header">
            <h3 className="font-semibold text-zinc-200">Recent Race Results</h3>
            <span className="text-xs text-zinc-500">{data.recentRaces.length} races</span>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-zinc-800/50 text-zinc-500 text-xs uppercase tracking-wider">
                  <th className="px-4 py-3 text-left font-medium">Date</th>
                  <th className="px-4 py-3 text-left font-medium">Venue</th>
                  <th className="px-4 py-3 text-left font-medium">Race</th>
                  <th className="px-4 py-3 text-center font-medium">Top Pick</th>
                  <th className="px-4 py-3 text-center font-medium">Value Bets</th>
                  <th className="px-4 py-3 text-right font-medium">P&L</th>
                  <th className="px-4 py-3 text-center font-medium">Pace</th>
                </tr>
              </thead>
              <tbody>
                {data.recentRaces.map(r => (
                  <tr key={r.race_id} className="border-b border-zinc-800/50 hover:bg-zinc-800/30 transition-colors">
                    <td className="px-4 py-3 text-zinc-500 text-xs font-mono">{r.meeting_date}</td>
                    <td className="px-4 py-3 font-medium text-zinc-300">{r.venue_name}</td>
                    <td className="px-4 py-3">
                      <Link to={`/race/${r.race_id}`} className="text-emerald-400 hover:text-emerald-300 transition-colors">
                        R{r.race_number} {r.race_name}
                      </Link>
                    </td>
                    <td className="px-4 py-3 text-center">
                      {r.top_pick_won ? (
                        <span className="inline-flex items-center gap-1 text-amber-400 font-bold text-xs bg-amber-500/15 px-2 py-0.5 rounded-md border border-amber-500/20">
                          <Trophy className="w-3 h-3" /> WON
                        </span>
                      ) : r.top_pick_position ? (
                        <span className="text-zinc-500 text-xs">{r.top_pick_position}th</span>
                      ) : '-'}
                    </td>
                    <td className="px-4 py-3 text-center text-xs">
                      {r.value_bets_count > 0 ? (
                        <span className={cn(
                          'font-mono',
                          r.value_bets_won > 0 ? 'text-emerald-400 font-semibold' : 'text-zinc-500'
                        )}>
                          {r.value_bets_won}/{r.value_bets_count}
                        </span>
                      ) : <span className="text-zinc-700">-</span>}
                    </td>
                    <td className="px-4 py-3 text-right">
                      <span className={cn(
                        'inline-flex items-center gap-0.5 font-mono text-xs font-semibold',
                        r.race_pnl >= 0 ? 'text-emerald-400' : 'text-red-400'
                      )}>
                        {r.race_pnl >= 0 ? <ArrowUpRight className="w-3 h-3" /> : <ArrowDownRight className="w-3 h-3" />}
                        {r.race_pnl >= 0 ? '+' : ''}{r.race_pnl.toFixed(1)}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-center">
                      {r.pace_scenario ? (
                        <span className={cn(
                          'text-xs px-2 py-0.5 rounded-md border font-medium',
                          r.pace_scenario === 'fast' ? 'bg-red-500/15 text-red-400 border-red-500/20' :
                          r.pace_scenario === 'slow' ? 'bg-blue-500/15 text-blue-400 border-blue-500/20' :
                          'bg-zinc-800 text-zinc-400 border-zinc-700'
                        )}>
                          {r.pace_scenario}
                        </span>
                      ) : <span className="text-zinc-700">-</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {data?.recentRaces?.length === 0 && !isLoading && (
        <div className="text-center py-16 text-zinc-600">
          <BarChart3 className="w-10 h-10 mx-auto mb-3 text-zinc-700" />
          No analyzed races yet. Results will appear after races are completed and auto-updated.
        </div>
      )}
    </div>
  )
}
