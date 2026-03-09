import 'dotenv/config'
import pg from 'pg'
import pino from 'pino'

const { Pool } = pg
const log = pino({ name: 'database' })

const connectionString = process.env.DATABASE_URL
if (!connectionString) {
  throw new Error('DATABASE_URL environment variable is required')
}

const isLocalhost = connectionString.includes('localhost') || connectionString.includes('127.0.0.1')
export const pool = new Pool({
  connectionString,
  ssl: isLocalhost ? false : { rejectUnauthorized: false },
  max: 10,
})

// Generic upsert helper
export async function upsert(
  table: string,
  columns: string[],
  values: unknown[],
  conflictColumns: string[],
  updateColumns?: string[]
): Promise<void> {
  const placeholders = columns.map((_, i) => `$${i + 1}`).join(', ')
  const conflictClause = conflictColumns.join(', ')
  const updates = (updateColumns ?? columns.filter((c) => !conflictColumns.includes(c)))
    .map((c) => `${c} = EXCLUDED.${c}`)
    .join(', ')

  const sql = `
    INSERT INTO ${table} (${columns.join(', ')})
    VALUES (${placeholders})
    ON CONFLICT (${conflictClause})
    DO UPDATE SET ${updates}
  `

  try {
    await pool.query(sql, values)
  } catch (err) {
    log.error({ table, err }, 'Upsert failed')
    throw err
  }
}

// Batch upsert for efficiency
export async function batchUpsert(
  table: string,
  columns: string[],
  rows: unknown[][],
  conflictColumns: string[],
  updateColumns?: string[]
): Promise<number> {
  if (rows.length === 0) return 0

  const client = await pool.connect()
  let inserted = 0
  try {
    await client.query('BEGIN')

    for (const values of rows) {
      const placeholders = columns.map((_, i) => `$${i + 1}`).join(', ')
      const conflictClause = conflictColumns.join(', ')
      const updates = (updateColumns ?? columns.filter((c) => !conflictColumns.includes(c)))
        .map((c) => `${c} = EXCLUDED.${c}`)
        .join(', ')

      const sql = `
        INSERT INTO ${table} (${columns.join(', ')})
        VALUES (${placeholders})
        ON CONFLICT (${conflictClause}) DO UPDATE SET ${updates}
      `
      await client.query(sql, values)
      inserted++
    }

    await client.query('COMMIT')
  } catch (err) {
    await client.query('ROLLBACK')
    log.error({ table, err }, 'Batch upsert failed')
    throw err
  } finally {
    client.release()
  }

  return inserted
}

// Query helper
export async function query<T = Record<string, unknown>>(
  sql: string,
  params?: unknown[]
): Promise<T[]> {
  const result = await pool.query(sql, params)
  return result.rows as T[]
}

// Single row query
export async function queryOne<T = Record<string, unknown>>(
  sql: string,
  params?: unknown[]
): Promise<T | null> {
  const rows = await query<T>(sql, params)
  return rows[0] ?? null
}

export async function execute(sql: string, params?: unknown[]): Promise<number> {
  const result = await pool.query(sql, params)
  return result.rowCount ?? 0
}
