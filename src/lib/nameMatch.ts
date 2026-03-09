/**
 * Shared horse name normalization for matching across data sources.
 * Used by: linkHorses.ts, ingestScraperData.ts
 */

export function normalizeName(name: string): string {
  return name.toLowerCase()
    .replace(/\s*\((aus|nz|ire|gb|usa|fr|ger|jpn|hk|saf|sgp|mac)\)\s*/gi, '')
    .replace(/[''`]/g, "'")  // normalize apostrophes
    .replace(/[^a-z0-9' ]/g, '')  // strip non-alpha except apostrophe
    .replace(/\s+/g, ' ')
    .trim()
}

/**
 * Match a scraper horse name against a list of runner names, returning the best match.
 * Returns the matched runner name or null if no match found.
 */
export function findBestMatch(
  scraperName: string,
  runnerNames: string[]
): string | null {
  const normalized = normalizeName(scraperName)

  // Exact normalized match
  for (const name of runnerNames) {
    if (normalizeName(name) === normalized) return name
  }

  // Partial match (scraper name contained in runner name or vice versa)
  for (const name of runnerNames) {
    const norm = normalizeName(name)
    if (norm.includes(normalized) || normalized.includes(norm)) return name
  }

  return null
}
