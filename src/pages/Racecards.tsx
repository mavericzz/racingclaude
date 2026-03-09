import { useState, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link, useSearchParams } from 'react-router-dom'
import { api, type Meeting, type Race } from '../lib/api'
import { cn } from '../lib/cn'
import { ChevronLeft, ChevronRight, Calendar, MapPin, Trophy, Users, Clock, ArrowRight, CheckCircle2, CircleDot } from 'lucide-react'

function formatDate(d: Date): string {
  return d.toISOString().slice(0, 10)
}

const today = formatDate(new Date())

function isUpcoming(dateStr: string): boolean {
  return dateStr >= today
}

function relativeLabel(dateStr: string): string {
  const diff = Math.round((new Date(dateStr + 'T00:00:00').getTime() - new Date(today + 'T00:00:00').getTime()) / 86400000)
  if (diff === 0) return 'Today'
  if (diff === 1) return 'Tomorrow'
  if (diff === -1) return 'Yesterday'
  return ''
}

function RaceRow({ race }: { race: Race }) {
  const activeRunners = race.runners || []
  const topPickCorrect = race.winner && race.topPick && race.topPick.horseName === race.winner.horseName

  return (
    <Link
      to={`/race/${race.race_id}`}
      className="group block px-4 py-3 hover:bg-zinc-800/50 transition-all duration-200 border-b border-zinc-800/50 last:border-b-0"
    >
      <div className="flex items-center justify-between mb-1.5">
        <div className="flex items-center gap-2.5 min-w-0">
          <span className="inline-flex items-center justify-center w-7 h-7 text-xs font-bold text-emerald-400 bg-emerald-500/15 border border-emerald-500/20 rounded-lg shrink-0">
            R{race.race_number || '?'}
          </span>
          <span className="text-sm font-semibold text-zinc-200 truncate group-hover:text-zinc-50 transition-colors">
            {race.race_name || 'Race'}
          </span>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          {race.off_time && (
            <span className="flex items-center gap-1 text-xs font-mono text-zinc-500">
              <Clock className="w-3 h-3" />
              {race.off_time.slice(11, 16) || race.off_time.slice(0, 5)}
            </span>
          )}
          <ArrowRight className="w-3.5 h-3.5 text-zinc-700 group-hover:text-zinc-500 transition-colors" />
        </div>
      </div>

      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 text-xs text-zinc-500">
          {race.distance_m && <span className="font-medium text-zinc-400">{race.distance_m}m</span>}
          {race.class && <><span className="text-zinc-700">|</span><span>{race.class}</span></>}
          {race.going && <><span className="text-zinc-700">|</span><span>{race.going}</span></>}
          <span className="text-zinc-700">|</span>
          <span className="flex items-center gap-0.5">
            <Users className="w-3 h-3" /> {race.field_size}
          </span>
        </div>
        <div className="flex items-center gap-1.5">
          {race.topPick && (
            <span className="inline-flex items-center gap-1.5 bg-zinc-800 text-zinc-300 px-2 py-0.5 rounded-md text-[11px] border border-zinc-700/50">
              <CircleDot className="w-3 h-3 text-emerald-500" />
              <span className="font-medium">{race.topPick.horseName}</span>
              <span className="text-zinc-500 font-mono">{(race.topPick.winProb * 100).toFixed(0)}%</span>
            </span>
          )}
          {race.valueBetCount > 0 && (
            <span className="inline-flex items-center bg-emerald-500/15 text-emerald-400 px-2 py-0.5 rounded-md text-[11px] font-semibold border border-emerald-500/20">
              {race.valueBetCount} Value
            </span>
          )}
        </div>
      </div>

      {/* Winner or runners preview */}
      {race.winner ? (
        <div className="mt-2.5 flex items-center gap-2">
          <Trophy className="w-3.5 h-3.5 text-amber-400" />
          <span className="text-xs font-semibold text-zinc-200">{race.winner.horseName}</span>
          {race.winner.sp && <span className="text-xs text-zinc-500 font-mono">${race.winner.sp.toFixed(1)}</span>}
          {race.topPick && (
            <span className={cn(
              'text-[10px] ml-1 flex items-center gap-1',
              topPickCorrect ? 'text-emerald-400 font-bold' : 'text-zinc-500'
            )}>
              {topPickCorrect ? (
                <><CheckCircle2 className="w-3 h-3" /> Correct</>
              ) : (
                `Pick: ${race.topPick.horseName}`
              )}
            </span>
          )}
        </div>
      ) : activeRunners.length > 0 ? (
        <div className="mt-2.5 flex flex-wrap gap-x-3 gap-y-1">
          {activeRunners.slice(0, 5).map((r, i) => (
            <span key={r.horse_id} className="text-xs">
              <span className={cn(i === 0 ? 'font-medium text-zinc-300' : 'text-zinc-500')}>
                {r.horse?.replace(' (AUS)', '').replace(' (NZ)', '')}
              </span>
              {r.sp_decimal && (
                <span className="text-zinc-600 font-mono ml-0.5">${Number(r.sp_decimal).toFixed(1)}</span>
              )}
            </span>
          ))}
          {activeRunners.length > 5 && (
            <span className="text-xs text-zinc-600">+{race.field_size - 5} more</span>
          )}
        </div>
      ) : null}
    </Link>
  )
}

function MeetingCard({ meeting }: { meeting: Meeting }) {
  const totalRunners = meeting.races.reduce((sum, r) => sum + r.field_size, 0)

  return (
    <div className="card overflow-hidden animate-slide-up">
      <div className="px-4 py-3 bg-gradient-to-r from-zinc-800 to-zinc-900 flex items-center justify-between">
        <div className="flex items-center gap-2.5">
          <MapPin className="w-4 h-4 text-emerald-400" />
          <div>
            <h3 className="font-semibold text-zinc-100">{meeting.venue_name}</h3>
            <p className="text-[11px] text-zinc-500">
              {meeting.state || ''}
              {meeting.track_condition ? ` · ${meeting.track_condition}` : ''}
              {meeting.weather ? ` · ${meeting.weather}` : ''}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2 text-xs text-zinc-500">
          <span className="bg-zinc-700/50 px-2 py-0.5 rounded-md">{meeting.races.length}R</span>
          <span className="bg-zinc-700/50 px-2 py-0.5 rounded-md">{totalRunners} runners</span>
        </div>
      </div>
      <div>
        {meeting.races.map(race => (
          <RaceRow key={race.race_id} race={race} />
        ))}
      </div>
    </div>
  )
}

export default function Racecards() {
  const [searchParams, setSearchParams] = useSearchParams()
  const urlDate = searchParams.get('date')
  const [date, setDate] = useState(urlDate || today)

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

  const totalRaces = meetings?.reduce((sum, m) => sum + m.races.length, 0) ?? 0
  const totalRunners = meetings?.reduce((sum, m) => sum + m.races.reduce((s, r) => s + r.field_size, 0), 0) ?? 0

  const upcomingDates = availableDates?.filter(d => isUpcoming(d.meeting_date))
    .sort((a, b) => a.meeting_date.localeCompare(b.meeting_date)) ?? []
  const pastDates = availableDates?.filter(d => !isUpcoming(d.meeting_date))
    .sort((a, b) => b.meeting_date.localeCompare(a.meeting_date)) ?? []

  const label = relativeLabel(date)

  return (
    <div>
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-6">
        <div>
          <h1 className="text-2xl font-bold text-zinc-100">Racecards</h1>
          <p className="text-sm text-zinc-500 mt-1">
            {label && <span className="font-medium text-emerald-400 mr-1.5">{label}</span>}
            {new Date(date + 'T00:00:00').toLocaleDateString('en-AU', {
              weekday: 'long', day: 'numeric', month: 'long', year: 'numeric'
            })}
            {meetings && (
              <span className="text-zinc-600 ml-2">
                {meetings.length} venues · {totalRaces} races · {totalRunners} runners
              </span>
            )}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => shiftDate(-1)}
            className="btn-secondary !px-2.5"
            aria-label="Previous day"
          >
            <ChevronLeft className="w-4 h-4" />
          </button>
          <div className="relative">
            <Calendar className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-500 pointer-events-none" />
            <input
              type="date"
              value={date}
              onChange={e => changeDate(e.target.value)}
              className="pl-9 pr-3 py-2 text-sm bg-zinc-800 border border-zinc-700 rounded-lg text-zinc-200 focus:outline-none focus:border-emerald-500/50 focus:ring-1 focus:ring-emerald-500/20"
            />
          </div>
          <button
            onClick={() => shiftDate(1)}
            className="btn-secondary !px-2.5"
            aria-label="Next day"
          >
            <ChevronRight className="w-4 h-4" />
          </button>
          <button
            onClick={() => changeDate(today)}
            className="btn-primary"
          >
            Today
          </button>
        </div>
      </div>

      {/* Date chips */}
      {upcomingDates.length > 0 && (
        <div className="mb-3 flex items-center gap-2 flex-wrap">
          <span className="text-[11px] font-semibold text-emerald-500 uppercase tracking-wider">Upcoming</span>
          <div className="flex flex-wrap gap-1.5">
            {upcomingDates.map(d => {
              const rel = relativeLabel(d.meeting_date)
              return (
                <button
                  key={d.meeting_date}
                  onClick={() => changeDate(d.meeting_date)}
                  className={cn(
                    'px-2.5 py-1 text-xs rounded-lg transition-all duration-200',
                    d.meeting_date === date
                      ? 'bg-emerald-600 text-white shadow-md shadow-emerald-900/30'
                      : 'bg-emerald-500/10 text-emerald-400 hover:bg-emerald-500/20 border border-emerald-500/20'
                  )}
                >
                  {rel || new Date(d.meeting_date + 'T00:00:00').toLocaleDateString('en-AU', {
                    weekday: 'short', day: 'numeric', month: 'short'
                  })}
                  <span className="ml-1 opacity-50">({d.count})</span>
                </button>
              )
            })}
          </div>
        </div>
      )}

      {pastDates.length > 0 && (
        <div className="mb-5 flex items-center gap-2 flex-wrap">
          <span className="text-[11px] font-semibold text-zinc-600 uppercase tracking-wider">Recent</span>
          <div className="flex flex-wrap gap-1.5">
            {pastDates.slice(0, 10).map(d => {
              const rel = relativeLabel(d.meeting_date)
              return (
                <button
                  key={d.meeting_date}
                  onClick={() => changeDate(d.meeting_date)}
                  className={cn(
                    'px-2.5 py-1 text-xs rounded-lg transition-all duration-200',
                    d.meeting_date === date
                      ? 'bg-zinc-700 text-zinc-100'
                      : 'bg-zinc-800/50 text-zinc-500 hover:bg-zinc-800 hover:text-zinc-400 border border-zinc-800'
                  )}
                >
                  {rel || new Date(d.meeting_date + 'T00:00:00').toLocaleDateString('en-AU', {
                    weekday: 'short', day: 'numeric', month: 'short'
                  })}
                  <span className="ml-1 opacity-50">({d.count})</span>
                </button>
              )
            })}
          </div>
        </div>
      )}

      {/* States */}
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

      {/* Meeting cards */}
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        {meetings?.map(meeting => (
          <MeetingCard key={meeting.meeting_id} meeting={meeting} />
        ))}
      </div>
    </div>
  )
}
