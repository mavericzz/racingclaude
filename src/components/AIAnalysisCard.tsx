import { useState } from 'react'
import { Sparkles, RefreshCw, Brain, Zap, AlertTriangle, Clock } from 'lucide-react'
import { api, type AIAnalysisResponse } from '../lib/api'

interface Props {
  raceId: string
}

function renderAnalysis(text: string) {
  // Minimal markdown: bold, section headers, line breaks
  return text.split('\n\n').map((block, i) => {
    const lines = block.split('\n').map((line, j) => {
      // Bold
      let processed = line.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')

      // Section headers (numbered or all-caps lines)
      if (/^\d+\.\s+\*?\*?[A-Z]/.test(line) || /^#{1,3}\s/.test(line)) {
        const clean = line.replace(/^#{1,3}\s*/, '').replace(/\*\*/g, '')
        return (
          <h4 key={j} className="text-emerald-400 font-semibold text-sm mt-3 mb-1">
            {clean}
          </h4>
        )
      }

      return (
        <span key={j} className="block" dangerouslySetInnerHTML={{ __html: processed }} />
      )
    })

    return (
      <div key={i} className="mb-2">
        {lines}
      </div>
    )
  })
}

function ConfidenceBadge({ confidence }: { confidence: string }) {
  const colors = {
    high: 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30',
    medium: 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30',
    low: 'bg-zinc-500/20 text-zinc-400 border-zinc-500/30',
  }
  return (
    <span className={`text-xs px-2 py-0.5 rounded-full border ${colors[confidence as keyof typeof colors] || colors.low}`}>
      {confidence}
    </span>
  )
}

export default function AIAnalysisCard({ raceId }: Props) {
  const [result, setResult] = useState<AIAnalysisResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function runAnalysis(force?: boolean) {
    setLoading(true)
    setError(null)
    try {
      const data = await api.getAIAnalysis(raceId, force)
      setResult(data)
    } catch (err: any) {
      setError(err.message || 'Analysis failed')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="mt-6">
      {/* Button */}
      {!result && !loading && (
        <button
          onClick={() => runAnalysis()}
          disabled={loading}
          className="flex items-center gap-2 px-5 py-2.5 bg-gradient-to-r from-emerald-600 to-emerald-500 hover:from-emerald-500 hover:to-emerald-400 text-white font-medium rounded-lg transition-all shadow-lg shadow-emerald-500/20 hover:shadow-emerald-500/30 disabled:opacity-50"
        >
          <Sparkles className="w-4 h-4" />
          Analyse with AI
        </button>
      )}

      {/* Loading */}
      {loading && (
        <div className="flex items-center gap-3 px-5 py-4 bg-zinc-900/50 border border-zinc-800 rounded-lg">
          <div className="animate-spin">
            <RefreshCw className="w-5 h-5 text-emerald-400" />
          </div>
          <div>
            <p className="text-sm text-zinc-300 font-medium">Analysing race...</p>
            <p className="text-xs text-zinc-500 mt-0.5">Claude is reviewing form, pace, and market data</p>
          </div>
        </div>
      )}

      {/* Error */}
      {error && (
        <div className="flex items-start gap-3 px-4 py-3 bg-red-500/10 border border-red-500/20 rounded-lg">
          <AlertTriangle className="w-4 h-4 text-red-400 mt-0.5 shrink-0" />
          <div>
            <p className="text-sm text-red-400">{error}</p>
            <button
              onClick={() => runAnalysis()}
              className="text-xs text-red-400/70 hover:text-red-400 mt-1 underline"
            >
              Try again
            </button>
          </div>
        </div>
      )}

      {/* Result */}
      {result && (
        <div className="bg-zinc-900/80 border border-zinc-800 rounded-xl overflow-hidden">
          {/* Header */}
          <div className="flex items-center justify-between px-5 py-3 border-b border-zinc-800 bg-zinc-900/50">
            <div className="flex items-center gap-2">
              <Brain className="w-4 h-4 text-emerald-400" />
              <h3 className="text-sm font-semibold text-zinc-200">AI Race Analysis</h3>
              {result.learningBasis > 0 && (
                <span className="text-xs px-2 py-0.5 bg-emerald-500/10 text-emerald-400 rounded-full border border-emerald-500/20">
                  Learning from {result.learningBasis} past races
                </span>
              )}
            </div>
            <div className="flex items-center gap-3">
              <button
                onClick={() => runAnalysis(true)}
                disabled={loading}
                className="text-xs text-zinc-500 hover:text-emerald-400 transition-colors flex items-center gap-1"
                title="Re-analyse (fresh)"
              >
                <RefreshCw className="w-3 h-3" />
                Refresh
              </button>
            </div>
          </div>

          {/* Structured picks summary */}
          {(result.aiTopPicks || result.aiPaceCall) && (
            <div className="px-5 py-3 border-b border-zinc-800 bg-zinc-900/30">
              <div className="flex flex-wrap items-center gap-3">
                {result.aiPaceCall && (
                  <div className="flex items-center gap-1.5">
                    <Zap className="w-3.5 h-3.5 text-yellow-400" />
                    <span className="text-xs text-zinc-400">Pace:</span>
                    <span className="text-xs font-medium text-zinc-200 capitalize">{result.aiPaceCall}</span>
                  </div>
                )}
                {result.aiTopPicks && result.aiTopPicks.length > 0 && (
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-zinc-400">Picks:</span>
                    {result.aiTopPicks.map((pick, i) => (
                      <span key={i} className="flex items-center gap-1.5">
                        <span className="text-xs font-medium text-zinc-200">{pick.horse_name}</span>
                        <ConfidenceBadge confidence={pick.confidence} />
                      </span>
                    ))}
                  </div>
                )}
                {result.aiDangers && result.aiDangers.length > 0 && (
                  <div className="flex items-center gap-1.5">
                    <AlertTriangle className="w-3 h-3 text-orange-400" />
                    <span className="text-xs text-zinc-400">Dangers:</span>
                    <span className="text-xs text-orange-300">
                      {result.aiDangers.map(d => d.horse_name).join(', ')}
                    </span>
                  </div>
                )}
              </div>
              {result.keyFactor && (
                <p className="text-xs text-zinc-500 mt-1.5 italic">{result.keyFactor}</p>
              )}
            </div>
          )}

          {/* Analysis body */}
          <div className="px-5 py-4 text-sm text-zinc-300 leading-relaxed">
            {renderAnalysis(result.analysis)}
          </div>

          {/* Footer */}
          <div className="flex items-center justify-between px-5 py-2 border-t border-zinc-800 bg-zinc-900/50">
            <div className="flex items-center gap-1.5 text-xs text-zinc-600">
              <Clock className="w-3 h-3" />
              {new Date(result.generatedAt).toLocaleString()}
            </div>
            <div className="text-xs text-zinc-600">
              {result.model.split('/').pop()} | {result.tokensUsed.prompt + result.tokensUsed.completion} tokens
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
