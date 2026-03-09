import { traClient } from '../lib/traClient.js'

async function main() {
  const result = await traClient.getResult('rac_11894129')
  console.log('Single result fields:', JSON.stringify(result, null, 2).slice(0, 2000))
}

main().catch(e => { console.error(e.message); process.exit(1) })
