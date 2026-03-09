import { traClient } from '../lib/traClient.js'
import { pool } from '../lib/database.js'

async function main() {
  // Test various search endpoints
  const endpoints = [
    '/v1/horses/search?name=Crickwood',
    '/v1/search/horses?name=Crickwood',
    '/v1/horses?name=Crickwood',
  ]
  for (const path of endpoints) {
    try {
      const resp = await traClient.get(path)
      console.log(`${path} -> OK:`, JSON.stringify(resp).slice(0, 200))
    } catch (e: any) {
      console.log(`${path} -> ${e.message?.slice(0, 100)}`)
    }
  }
  await pool.end()
}
main()
