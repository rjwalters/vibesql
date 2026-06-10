/**
 * VibeSQL Homepage
 *
 * Product landing page. Loads dashboard.json to populate performance stats.
 */

import './styles/main.css'
import { initTheme } from './theme'
import { initLocale } from './locale'
import { NavigationComponent } from './components/Navigation'
import { FooterComponent } from './components/Footer'
import { initI18n, updateDOM, setI18nLocale } from './i18n'

interface DashboardData {
  summary: {
    conformance: {
      pass_rate: number
      tests_passing: number
      tests_total: number
    }
    tpch: {
      queries_passing: number
      queries_total: number
    }
    tpcds: {
      queries_passing: number
      queries_total: number
    }
  }
}

function updateStats(data: DashboardData): void {
  const { summary } = data

  const tpchEl = document.getElementById('stat-tpch')
  if (tpchEl) {
    tpchEl.textContent = `${summary.tpch.queries_passing}/${summary.tpch.queries_total}`
  }

  const conformanceEl = document.getElementById('stat-conformance')
  if (conformanceEl) {
    conformanceEl.textContent = `${summary.conformance.pass_rate}%`
  }

  const tpcdsEl = document.getElementById('stat-tpcds')
  if (tpcdsEl) {
    tpcdsEl.textContent = `${summary.tpcds.queries_passing}/${summary.tpcds.queries_total}`
  }
}

async function init(): Promise<void> {
  const theme = initTheme()
  const locale = initLocale()

  initI18n(locale.current)
  updateDOM()

  locale.onChange(newLocale => {
    setI18nLocale(newLocale)
    updateDOM()
  })

  new NavigationComponent('home', theme, locale)
  new FooterComponent(true)

  // Load dashboard stats
  try {
    const response = await fetch('./data/dashboard.json')
    if (response.ok) {
      const data: DashboardData = await response.json()
      updateStats(data)
    }
  } catch (error) {
    console.error('Error loading dashboard data:', error)
  }
}

document.addEventListener('DOMContentLoaded', init)
