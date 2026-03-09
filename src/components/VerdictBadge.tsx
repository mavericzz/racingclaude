import { cn } from '../lib/cn'

const verdictConfig: Record<string, { bg: string; text: string; border: string; label: string }> = {
  'strong-value': {
    bg: 'bg-emerald-500/15',
    text: 'text-emerald-400',
    border: 'border-emerald-500/30',
    label: 'STRONG VALUE',
  },
  'value': {
    bg: 'bg-green-500/15',
    text: 'text-green-400',
    border: 'border-green-500/30',
    label: 'VALUE',
  },
  'dutch-candidate': {
    bg: 'bg-blue-500/15',
    text: 'text-blue-400',
    border: 'border-blue-500/30',
    label: 'DUTCH',
  },
  'fair-price': {
    bg: 'bg-zinc-500/15',
    text: 'text-zinc-400',
    border: 'border-zinc-500/30',
    label: 'FAIR',
  },
  'oppose': {
    bg: 'bg-red-500/15',
    text: 'text-red-400',
    border: 'border-red-500/30',
    label: 'OPPOSE',
  },
  'pass': {
    bg: 'bg-zinc-800/50',
    text: 'text-zinc-500',
    border: 'border-zinc-700/50',
    label: 'PASS',
  },
}

export default function VerdictBadge({ verdict }: { verdict: string }) {
  const config = verdictConfig[verdict] || verdictConfig['pass']
  return (
    <span className={cn(
      'inline-flex items-center px-2 py-0.5 rounded-md text-[11px] font-semibold tracking-wide border',
      config.bg, config.text, config.border
    )}>
      {config.label}
    </span>
  )
}
