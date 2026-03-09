import { useState, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link, useSearchParams } from 'react-router-dom'
import { api, type Meeting, type Race } from '../lib/api'
import { cn } from '../lib/cn'
import { ChevronLeft, ChevronRight, Calendar, Download } from 'lucide-react'

function formatDate(d: Date): string {
  return d.toISOString().slice(0, 10)
}

const today = formatDate(new Date())

function relativeLabel(dateStr: string): string {
  const diff = Math.round((new Date(dateStr + 'T00:00:00').getTime() - new Date(today + 'T00:00:00').getTime()) / 86400000)
  if (diff === 0) return 'Today'
  if (diff === 1) return 'Tomorrow'
  if (diff === -1) return 'Yesterday'
  return ''
}

function RaceCell({ race }: { race: Race }) {
  const hasWinner = !!race.winner
  const hasValue = race.valueBetCount > 0
  const topPick = race.topPick
  const topPickWon = hasWinner && topPick && topPick.horseName === race.winner?.horseName

  return (
    <Link
      to={`/race/${race.race_id}`}
      className={cn(
        'group flex items-center justify-center w-10 h-10 rounded-lg transition-all duration-200 border text-xs font-semibold',
        hasWinner
          ? topPickWon
            ? 'bg-emerald-500/20 border-emerald-500/40 text-emerald-400 hover:bg-emerald-500/30'
            : 'bg-zinc-800 border-zinc-700 text-zinc-400 hover:bg-zinc-700'
          : hasValue
            ? 'bg-amber-500/15 border-amber-500/30 text-amber-400 hover:bg-amber-500/25 animate-pulse-slow'
            : 'bg-zinc-800/60 border-zinc-700/50 text-zinc-500 hover:bg-zinc-800 hover:border-zinc-600'
      )}
      title={`R${race.race_number} - ${race.race_name || ''} (${race.field_size} runners)`}
    >
      {hasWinner ? (
        topPickWon ? '✓' : <span className="text-[10px]">{race.winner?.horseName?.slice(0, 2)}</span>
      ) : (
        <svg className="w-4 h-4 opacity-60" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <circle cx="12" cy="12" r="10" />
          <path d="M12 6v6l4 2" />
        </svg>
      )}
    </Link>
  )
}

function MeetingRow({ meeting, maxRaces }: { meeting: Meeting; maxRaces: number }) {
  const raceMap = new Map(meeting.races.map(r => [r.race_number, r]))

  return (
    <tr className="border-b border-zinc-800/50 hover:bg-zinc-800/20 transition-colors">
      <td className="px-4 py-3">
        <div className="font-semibold text-zinc-200 text-sm">{meeting.venue_name}</div>
        <div className="text-[11px] text-zinc-500">
          {meeting.state || 'AUS'}
          {meeting.track_condition ? ` · ${meeting.track_condition}` : ''}
        </div>
      </td>
      {Array.from({ length: maxRaces }, (_, i) => {
        const race = raceMap.get(i + 1)
        return (
          <td key={i} className="px-1.5 py-3 text-center">
            {race ? <RaceCell race={race} /> : <span className="text-zinc-800">-</span>}
          </td>
        )
      })}
    </tr>
  )
}

export default function Racecards() {
  const [searchParams, setSearchParams] = useSearchParams()
  const urlDate = searchParams.get('date')
  const [date, setDate] = useState(urlDate || today)
  const [region, setRegion] = useState<'au' | 'uk'>('au')

  useEffect(() => {
    if (urlDate && urlDate !== date) setDate(urlDate)
  }, [urlDate])

  const changeDate = (newDate: string) => {
    setDate(newDate)
    setSearchParams({ date: newDate })
  }

  const { data: meetings, isLoading, error } = useQuery({
    queryKey: ['meetings', date],
    queryFn: () => api.getMeetings(date),
  })

  const { data: availableDates } = useQuery({
    queryKey: ['dates'],
    queryFn: () => api.getDates(),
  })

  const shiftDate = (days: number) => {
    const d = new Date(date + 'T00:00:00')
    d.setDate(d.getDate() + days)
    changeDate(formatDate(d))
  }

  useEffect(() => {
    if (meetings && meetings.length === 0 && availableDates && availableDates.length > 0 && date === today) {
      const futureDate = availableDates.find(d => d.meeting_date >= today)
      if (futureDate) changeDate(futureDate.meeting_date)
    }
  }, [meetings, availableDates])

  // Split meetings by region (AU vs UK/IRE)
  const auMeetings = meetings?.filter(m =>
    !m.venue_name.match(/\b(Ascot|Cheltenham|Newmarket|York|Doncaster|Leopardstown|Curragh|Kempton|Sandown|Lingfield|Wolverhampton|Newcastle|Dundalk)\b/i)
  ) ?? []
  const ukMeetings = meetings?.filter(m =>
    m.venue_name.match(/\b(Ascot|Cheltenham|Newmarket|York|Doncaster|Leopardstown|Curragh|Kempton|Sandown|Lingfield|Wolverhampton|Newcastle|Dundalk)\b/i)
  ) ?? []

  const activeMeetings = region === 'au' ? auMeetings : ukMeetings
  const maxRaces = Math.max(...(activeMeetings.map(m => Math.max(...m.races.map(r => r.race_number ?? 0), 0))), 0)

  const label = relativeLabel(date)
  const dateDisplay = new Date(date + 'T00:00:00')

  return (
    <div>
      {/* Header */}
      <div className="flex flex-col gap-6 mb-8">
        <div className="flex flex-col sm:flex-row sm:items-end justify-between gap-4">
          <div>
            <h1 className="text-3xl font-bold text-zinc-100">
              {label ? `${label}'s` : ''} Race Meetings
            </h1>
            <p className="text-sm text-zinc-500 mt-1">
              Browse race schedules and basic information
            </p>
          </div>
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-1 bg-zinc-800/80 rounded-xl p-1 border border-zinc-700/50">
              <button
                onClick={() => shiftDate(-1)}
                className="p-2 rounded-lg hover:bg-zinc-700 transition-colors"
                aria-label="Previous day"
              >
                <ChevronLeft className="w-4 h-4 text-zinc-400" />
              </button>
              <div className="relative">
                <Calendar className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-500 pointer-events-none" />
                <input
                  type="date"
                  value={date}
                  onChange={e => changeDate(e.target.value)}
                  className="pl-9 pr-3 py-1.5 text-sm bg-transparent text-zinc-200 focus:outline-none w-[160px]"
                />
              </div>
              <button
                onClick={() => shiftDate(1)}
                className="p-2 rounded-lg hover:bg-zinc-700 transition-colors"
                aria-label="Next day"
              >
                <ChevronRight className="w-4 h-4 text-zinc-400" />
              </button>
            </div>
            <button className="flex items-center gap-2 px-4 py-2.5 bg-zinc-800 border border-zinc-700/50 rounded-xl text-sm text-zinc-400 hover:text-zinc-200 hover:bg-zinc-700 transition-all">
              <Download className="w-4 h-4" />
              Download PDF Racecard
            </button>
          </div>
        </div>

        {/* Region tabs */}
        <div className="flex gap-2">
          <button
            onClick={() => setRegion('au')}
            className={cn(
              'px-4 py-2 rounded-xl text-sm font-medium transition-all border',
              region === 'au'
                ? 'bg-emerald-600 text-white border-emerald-500 shadow-lg shadow-emerald-900/30'
                : 'bg-zinc-800/60 text-zinc-400 border-zinc-700/50 hover:bg-zinc-800 hover:text-zinc-200'
            )}
          >
            Australia ({auMeetings.length})
          </button>
          <button
            onClick={() => setRegion('uk')}
            className={cn(
              'px-4 py-2 rounded-xl text-sm font-medium transition-all border',
              region === 'uk'
                ? 'bg-emerald-600 text-white border-emerald-500 shadow-lg shadow-emerald-900/30'
                : 'bg-zinc-800/60 text-zinc-400 border-zinc-700/50 hover:bg-zinc-800 hover:text-zinc-200'
            )}
          >
            UK & Ireland ({ukMeetings.length})
          </button>
        </div>
      </div>

      {/* Loading/Error states */}
      {isLoading && (
        <div className="text-center py-16">
          <div className="inline-flex items-center gap-3 text-zinc-500">
            <div className="w-5 h-5 border-2 border-emerald-500/30 border-t-emerald-500 rounded-full animate-spin" />
            Loading meetings...
          </div>
        </div>
      )}
      {error && (
        <div className="text-center py-16">
          <p className="text-red-400">Failed to load meetings. Is the API server running?</p>
        </div>
      )}
      {meetings && meetings.length === 0 && (
        <div className="text-center py-16 text-zinc-600">No meetings found for {date}</div>
      )}

      {/* Race Grid Table */}
      {activeMeetings.length > 0 && maxRaces > 0 && (
        <div className="card overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-zinc-800 bg-zinc-900/50">
                  <th className="px-4 py-3 text-left text-xs font-semibold text-zinc-500 uppercase tracking-wider w-48">
                    Venue
                  </th>
                  {Array.from({ length: maxRaces }, (_, i) => (
                    <th key={i} className="px-1.5 py-3 text-center text-xs font-semibold text-zinc-500 uppercase tracking-wider w-14">
                      Race {i + 1}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {activeMeetings.map(meeting => (
                  <MeetingRow key={meeting.meeting_id} meeting={meeting} maxRaces={maxRaces} />
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Quick date chips */}
      {availableDates && availableDates.length > 0 && (
        <div className="mt-6 flex flex-wrap gap-1.5">
          {availableDates
            .sort((a, b) => a.meeting_date.localeCompare(b.meeting_date))
            .slice(0, 14)
            .map(d => {
              const rel = relativeLabel(d.meeting_date)
              return (
                <button
                  key={d.meeting_date}
                  onClick={() => changeDate(d.meeting_date)}
                  className={cn(
                    'px-2.5 py-1 text-xs rounded-lg transition-all duration-200',
                    d.meeting_date === date
                      ? 'bg-emerald-600 text-white shadow-md'
                      : 'bg-zinc-800/50 text-zinc-500 hover:bg-zinc-800 hover:text-zinc-400 border border-zinc-800'
                  )}
                >
                  {rel || new Date(d.meeting_date + 'T00:00:00').toLocaleDateString('en-AU', {
                    weekday: 'short', day: 'numeric', month: 'short'
                  })}
                </button>
              )
            })}
        </div>
      )}
    </div>
  )
}
