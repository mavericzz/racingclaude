import 'dotenv/config'
import { readFileSync, readdirSync } from 'fs'
import { join } from 'path'
import { pool } from '../lib/database.js'
import pino from 'pino'

const log = pino({ name: 'migrate' })

async function migrate() {
  const migrationsDir = join(import.meta.dirname, '../../db/migrations')
  const files = readdirSync(migrationsDir)
    .filter((f) => f.endsWith('.sql'))
    .sort()

  log.info({ count: files.length }, 'Running migrations')

  for (const file of files) {
    const sql = readFileSync(join(migrationsDir, file), 'utf-8')
    try {
      await pool.query(sql)
      log.info({ file }, 'Migration applied')
    } catch (err) {
      log.error({ file, err }, 'Migration failed')
      throw err
    }
  }

  log.info('All migrations complete')
  await pool.end()
}

migrate().catch((err) => {
  log.error(err, 'Migration runner failed')
  process.exit(1)
})
