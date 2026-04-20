import './App.css'
import { useEffect } from 'react'

import { getStars } from './lib/http-client'

function App() {
  useEffect(() => {
    void getStars()
      .then((stars) => {
        console.info('[stl-verify/ui] /v1/stars response', stars)
      })
      .catch((error: unknown) => {
        console.error('[stl-verify/ui] /v1/stars request failed', error)
      })
  }, [])

  return (
    <main className="app-shell">
      <section className="hero" aria-label="STL Verify">
        <img className="hero-logo" src="/assets/archon-logo.svg" alt="Archon" />
        <h1>STL Verify</h1>
      </section>
      <div className="footer-logo-pattern" aria-hidden="true" />
    </main>
  )
}

export default App
