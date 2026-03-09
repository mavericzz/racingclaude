import { traClient } from '../lib/traClient.js'

async function main() {
  // Test 1: Get a day of AU meets
  console.log('=== Testing AU Meets ===')
  const meets = await traClient.getAustraliaMeets('2025-03-01')
  console.log(JSON.stringify(meets, null, 2).substring(0, 3000))

  // Test 2: Get a single race with runners
  const firstMeet = (meets as any)?.meets?.[0] ?? (Array.isArray(meets) ? meets[0] : null)
  if (firstMeet) {
    console.log('\n=== Testing Single Race ===')
    const meetId = firstMeet.meet_id ?? firstMeet.id
    const raceNum = firstMeet.races?.[0]?.race_number ?? 1
    console.log(`Fetching: meet=${meetId} race=${raceNum}`)
    const race = await traClient.getAustraliaRace(meetId, Number(raceNum))
    console.log(JSON.stringify(race, null, 2).substring(0, 5000))
  }
}

main().catch(console.error).finally(() => process.exit(0))
