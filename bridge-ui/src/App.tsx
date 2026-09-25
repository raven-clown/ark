import { useCallback, useEffect, useState } from 'react'

import { api, ApiError, getToken, setToken, type Health, type Overview } from './api'
import { Canvas, type TailFocus } from './components/Canvas'
import { Wordmark } from './components/Logo'
import { MetricsView } from './components/Metrics'
import { PipelinePanel } from './components/PipelinePanel'
import { ClusterView, EventsView, NewPipelineModal, SettingsView, TopicsView } from './components/Views'
import { detectLang, LangContext, translate, type Key, type Lang } from './i18n'

type View = 'pipelines' | 'metrics' | 'events' | 'cluster' | 'topics' | 'settings'

const icons: Record<View, string> = {
  pipelines: 'M4 17c2-6 5-10 8-12 3 2 6 6 8 12M3 21h18',
  metrics: 'M3 17l5-6 4 3 5-7 4 4M3 21h18',
  events: 'M4 6h16M4 12h16M4 18h10',
  cluster: 'M6 6m-2.5 0a2.5 2.5 0 1 0 5 0a2.5 2.5 0 1 0-5 0M18 6m-2.5 0a2.5 2.5 0 1 0 5 0a2.5 2.5 0 1 0-5 0M12 18m-2.5 0a2.5 2.5 0 1 0 5 0a2.5 2.5 0 1 0-5 0M8 7.5l3 8M16 7.5l-3 8',
  topics: 'M4 7h16M4 12h16M4 17h16',
  settings: 'M12 15a3 3 0 1 0 0-6 3 3 0 0 0 0 6zM19 12a7 7 0 0 0-.1-1.2l2-1.6-2-3.4-2.4 1a7 7 0 0 0-2-1.2L14 3h-4l-.5 2.6a7 7 0 0 0-2 1.2l-2.4-1-2 3.4 2 1.6A7 7 0 0 0 5 12c0 .4 0 .8.1 1.2l-2 1.6 2 3.4 2.4-1a7 7 0 0 0 2 1.2L10 21h4l.5-2.6a7 7 0 0 0 2-1.2l2.4 1 2-3.4-2-1.6c.1-.4.1-.8.1-1.2z',
}

function readPref(key: string, fallback: string) {
  try {
    return localStorage.getItem(key) ?? fallback
  } catch {
    return fallback
  }
}

function writePref(key: string, value: string) {
  try {
    localStorage.setItem(key, value)
  } catch {
    // storage blocked
  }
}

export function App() {
  const [lang, setLangState] = useState<Lang>(detectLang)
  const [motion, setMotionState] = useState(() => readPref('ark.motion', 'on') === 'on')
  const [authed, setAuthed] = useState(false)
  const [checking, setChecking] = useState(true)
  const [view, setView] = useState<View>('pipelines')
  const [overview, setOverview] = useState<Overview | null>(null)
  const [selected, setSelected] = useState<{ name: string; tab?: string; focus?: TailFocus } | null>(null)
  const [creating, setCreating] = useState(false)
  const [refreshKey, setRefreshKey] = useState(0)
  const [toastMsg, setToastMsg] = useState<{ text: string; error?: boolean } | null>(null)
  const t = (k: Key) => translate(lang, k)

  const setLang = (l: Lang) => {
    setLangState(l)
    writePref('ark.lang', l)
  }
  const setMotion = (m: boolean) => {
    setMotionState(m)
    writePref('ark.motion', m ? 'on' : 'off')
  }
  const toast = useCallback((text: string, error?: boolean) => {
    setToastMsg({ text, error })
    setTimeout(() => setToastMsg(null), 3500)
  }, [])

  const loadOverview = useCallback(async () => {
    try {
      setOverview(await api<Overview>('/overview'))
      setAuthed(true)
    } catch (e) {
      if (e instanceof ApiError && (e.status === 401 || e.status === 403)) setAuthed(false)
    } finally {
      setChecking(false)
    }
  }, [])

  useEffect(() => {
    loadOverview()
  }, [loadOverview])

  useEffect(() => {
    if (!authed) return
    const id = setInterval(loadOverview, 5000)
    return () => clearInterval(id)
  }, [authed, loadOverview])

  useEffect(() => {
    document.documentElement.lang = lang
  }, [lang])

  if (checking) return null
  if (!authed)
    return (
      <LangContext.Provider value={lang}>
        <Login
          onDone={() => {
            setChecking(false)
            loadOverview()
          }}
        />
      </LangContext.Provider>
    )

  const health: Record<string, Health> = {}
  for (const p of overview?.pipelines ?? []) health[p.name] = p.health
  const counts = { healthy: 0, degraded: 0, down: 0, paused: 0 }
  for (const h of Object.values(health)) counts[h]++
  const names = (overview?.pipelines ?? []).map((p) => p.name).sort()

  return (
    <LangContext.Provider value={lang}>
      <div className="shell">
        <header className="topbar">
          <div className="brand">
            <Wordmark />
            <span>Console</span>
          </div>
          <div className="spacer" />
          <div className="summary">
            {(Object.keys(counts) as Health[])
              .filter((h) => counts[h] > 0)
              .map((h) => (
                <span key={h} className={`chip ${h}`}>
                  <i />
                  {counts[h]} {t(`health.${h}`)}
                </span>
              ))}
          </div>
          <span className="small dim mono">{overview?.timezone}</span>
        </header>
        <nav className="sidebar">
          {(['pipelines', 'metrics', 'events', 'topics', 'cluster'] as View[]).map((v) => (
            <button key={v} className={view === v ? 'active' : ''} onClick={() => setView(v)}>
              <svg viewBox="0 0 24 24">
                <path d={icons[v]} />
              </svg>
              {t(`nav.${v}`)}
            </button>
          ))}
          <div className="grow" />
          <button className={view === 'settings' ? 'active' : ''} onClick={() => setView('settings')}>
            <svg viewBox="0 0 24 24">
              <path d={icons.settings} />
            </svg>
            {t('nav.settings')}
          </button>
        </nav>
        <main className="main">
          {view === 'pipelines' && (
            <>
              <Canvas
                health={health}
                motion={motion}
                selected={selected?.name}
                refreshKey={refreshKey}
                onSelect={(name, tab, focus) => setSelected({ name, tab, focus })}
                onNew={() => setCreating(true)}
              />
              {selected && (
                <PipelinePanel
                  name={selected.name}
                  tab={selected.tab}
                  focus={selected.focus}
                  onClose={() => setSelected(null)}
                  onChanged={() => {
                    setRefreshKey((k) => k + 1)
                    loadOverview()
                  }}
                  toast={toast}
                />
              )}
            </>
          )}
          {view === 'metrics' && <MetricsView pipelines={names} motion={motion} />}
          {view === 'events' && <EventsView pipelines={names} />}
          {view === 'cluster' && <ClusterView />}
          {view === 'topics' && <TopicsView />}
          {view === 'settings' && (
            <SettingsView
              lang={lang}
              setLang={setLang}
              motion={motion}
              setMotion={setMotion}
              timezone={overview?.timezone ?? ''}
              onSignOut={() => {
                setToken('')
                setAuthed(false)
              }}
            />
          )}
        </main>
      </div>
      {creating && (
        <NewPipelineModal
          onClose={() => setCreating(false)}
          onApplied={() => {
            setRefreshKey((k) => k + 1)
            loadOverview()
          }}
        />
      )}
      {toastMsg && <div className={`toast ${toastMsg.error ? 'error' : ''}`}>{toastMsg.text}</div>}
    </LangContext.Provider>
  )
}

function Login({ onDone }: { onDone: () => void }) {
  const [lang] = useState<Lang>(detectLang)
  const t = (k: Key) => translate(lang, k)
  const [value, setValue] = useState(getToken())
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  return (
    <div className="login">
      <form
        onSubmit={async (e) => {
          e.preventDefault()
          setBusy(true)
          setError('')
          setToken(value.trim())
          try {
            await api('/overview')
            onDone()
          } catch {
            setToken('')
            setError(t('login.bad'))
          } finally {
            setBusy(false)
          }
        }}
      >
        <Wordmark className="word" />
        <h1>{t('login.title')}</h1>
        <p>{t('login.hint')}</p>
        <input className="input mono" type="password" autoComplete="off" placeholder={t('login.token')} value={value} onChange={(e) => setValue(e.target.value)} autoFocus />
        {error && <p className="err">{error}</p>}
        <button className="btn primary" disabled={busy || !value.trim()}>
          {t('login.submit')}
        </button>
      </form>
    </div>
  )
}
