import { Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import Racecards from './pages/Racecards'
import RaceDetail from './pages/RaceDetail'
import ValueAlerts from './pages/ValueAlerts'
import Analysis from './pages/Analysis'

export default function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<Racecards />} />
        <Route path="/race/:raceId" element={<RaceDetail />} />
        <Route path="/value" element={<ValueAlerts />} />
        <Route path="/analysis" element={<Analysis />} />
      </Routes>
    </Layout>
  )
}
