import { Link, useLocation } from 'react-router-dom'
import { LayoutGrid, TrendingUp, Zap, BarChart3 } from 'lucide-react'
import { cn } from '../lib/cn'

const navItems = [
  { path: '/', label: 'Racecards', icon: LayoutGrid },
  { path: '/value', label: 'Value Alerts', icon: Zap },
  { path: '/analysis', label: 'Analysis', icon: BarChart3 },
]

export default function Layout({ children }: { children: React.ReactNode }) {
  const location = useLocation()

  return (
    <div className="min-h-screen bg-zinc-950">
      {/* Top navigation */}
      <header className="sticky top-0 z-50 border-b border-zinc-800/80 bg-zinc-950/80 backdrop-blur-xl">
        <div className="max-w-[1400px] mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            {/* Logo */}
            <Link to="/" className="flex items-center gap-3 group">
              <div className="flex items-center justify-center w-9 h-9 rounded-lg bg-emerald-600/20 border border-emerald-500/30 group-hover:bg-emerald-600/30 transition-colors">
                <TrendingUp className="w-5 h-5 text-emerald-400" />
              </div>
              <div className="flex items-center gap-2">
                <span className="text-lg font-bold text-zinc-100 tracking-tight">RacingClaude</span>
                <span className="text-[10px] font-semibold bg-emerald-500/20 text-emerald-400 px-1.5 py-0.5 rounded-md border border-emerald-500/30">
                  AI
                </span>
              </div>
            </Link>

            {/* Navigation */}
            <nav className="flex items-center gap-1">
              {navItems.map(item => {
                const isActive = item.path === '/'
                  ? location.pathname === '/'
                  : location.pathname.startsWith(item.path)
                const Icon = item.icon

                return (
                  <Link
                    key={item.path}
                    to={item.path}
                    className={cn(
                      'flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all duration-200',
                      isActive
                        ? 'bg-zinc-800 text-zinc-100 shadow-sm'
                        : 'text-zinc-400 hover:text-zinc-200 hover:bg-zinc-800/50'
                    )}
                  >
                    <Icon className={cn('w-4 h-4', isActive ? 'text-emerald-400' : '')} />
                    <span className="hidden sm:inline">{item.label}</span>
                  </Link>
                )
              })}
            </nav>
          </div>
        </div>
      </header>

      {/* Main content */}
      <main className="max-w-[1400px] mx-auto px-4 sm:px-6 lg:px-8 py-6">
        <div className="animate-fade-in">
          {children}
        </div>
      </main>
    </div>
  )
}
