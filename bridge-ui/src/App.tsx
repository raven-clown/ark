import { useCallback, useEffect, useState } from 'react'

import { api, ApiError, getToken, setToken, type Health, type Overview, type PipelineConfig } from './api'
import { Canvas, type TailFocus } from './components/Canvas'
import { Icon, type IconName } from './components/Icon'
import { CountUp } from './components/fx'
import { Mark, Wordmark } from './components/Logo'
import type { Sample } from './components/Metrics'
import { MetricsView } from './components/Metrics'
import { ProjectsView } from './components/ProjectsView'
import { AssistantPanel } from './components/Assistant'
import { PipelinePanel } from './components/PipelinePanel'
import { Designer } from './components/Designer'
import { ClusterView, EventsView, NewPipelineModal, SettingsView, TopicsView } from './components/Views'
import { detectLang, LangContext, translate, type Key, type Lang } from './i18n'

type View = 'pipelines' | 'projects' | 'metrics' | 'events' | 'cluster' | 'topics' | 'settings'


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
  const [creating, setCreating] = useState<'' | 'designer' | 'simple'>('')
  const [editing, setEditing] = useState<{ name: string; config: Record<string, unknown> } | null>(null)
  const [refreshKey, setRefreshKey] = useState(0)
  const [search, setSearch] = useState('')
  const [chatOpen, setChatOpen] = useState(false)
  const [node, setNode] = useState<NodeInfo | null>(null)
  const [scope, setScope] = useState('')
  const [io, setIo] = useState({ rate: 0, lag: 0, peak: 0 })
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

  useEffect(() => {
    if (!authed) return
    const load = async () => {
      try {
        setNode(await api<NodeInfo>('/node'))
        setScope((await api<{ scope: string }>('/whoami')).scope)
        const h = await api<{ pipelines: Record<string, Sample[]> }>('/history?minutes=15')
        let rate = 0
        let lag = 0
        let peak = 0
        const totals: Record<string, number> = {}
        for (const series of Object.values(h.pipelines)) {
          const recent = series.slice(-3)
          if (recent.length) {
            rate += recent.reduce((a, x) => a + x.processed_per_sec + x.rejected_per_sec + x.dead_lettered_per_sec, 0) / recent.length
            lag += recent[recent.length - 1].lag
          }
          for (const s of series) totals[s.time] = (totals[s.time] ?? 0) + s.processed_per_sec + s.rejected_per_sec + s.dead_lettered_per_sec
        }
        for (const v of Object.values(totals)) peak = Math.max(peak, v)
        setIo({ rate, lag, peak })
      } catch {
        // shown elsewhere
      }
    }
    load()
    const id = setInterval(load, 5000)
    return () => clearInterval(id)
  }, [authed])

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === '/' && !(e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement)) {
        e.preventDefault()
        document.querySelector<HTMLInputElement>('.search input')?.focus()
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [])

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

  const alerts = counts.degraded + counts.down

  const nav: { id: View; icon: IconName; count?: number }[] = [
    { id: 'pipelines', icon: 'pipelines', count: names.length },
    { id: 'projects', icon: 'folder' },
    { id: 'metrics', icon: 'metrics' },
    { id: 'events', icon: 'events' },
    { id: 'topics', icon: 'topics' },
    { id: 'cluster', icon: 'cluster' },
  ]

  return (
    <LangContext.Provider value={lang}>
      <div className="shell">
        <nav className="rail">
          <div className="brand">
            <div className="logo">
              <Mark />
            </div>
            <div className="name">
              <b>ARK</b>
              <small>CONSOLE</small>
            </div>
            {node?.version && <span className="badge-mono ver">{node.version}</span>}
          </div>
          <div className="label">{t('nav.workspaces')}</div>
          {nav.map((n) => (
            <button key={n.id} className={`nav ${view === n.id ? 'active' : ''}`} onClick={() => setView(n.id)}>
              <Icon name={n.icon} />
              {t(`nav.${n.id}`)}
              {n.count !== undefined && <span className="count">{n.count}</span>}
            </button>
          ))}
          <div className="grow" />
          <div className="io">
            <div className="top">
              <span className="micro">{t('io.title')}</span>
              <b>
                <CountUp value={io.rate} decimals={1} /> msg/s
              </b>
            </div>
            <div className="bar">
              <i style={{ width: `${Math.min(100, io.peak > 0 ? (io.rate / io.peak) * 100 : 0)}%` }} />
            </div>
            <div className="foot">
              <span>
                {t('io.lag')}: {io.lag}
              </span>
              <span>
                {t('io.pipes')}: {names.length}
              </span>
            </div>
          </div>
          <button className={`nav ${view === 'settings' ? 'active' : ''}`} onClick={() => setView('settings')}>
            <Icon name="settings" />
            {t('nav.settings')}
          </button>
        </nav>
        <header className="topbar">
          <label className="search">
            <Icon name="search" className="" />
            <input className="input" placeholder={t('top.search')} value={search} onChange={(e) => setSearch(e.target.value)} />
            <kbd>/</kbd>
          </label>
          <div className="spacer" />
          <span className="sc ok hide-md">
            <i className="dot healthy" />
            {node?.live_nodes ?? 1} {(node?.live_nodes ?? 1) === 1 ? t('top.node') : t('top.nodes')} {t('top.healthy')}
          </span>
          {alerts > 0 ? (
            <span className={`sc ${counts.down > 0 ? 'alert' : 'warn'}`}>
              <i className={`dot ${counts.down > 0 ? 'down' : 'degraded'}`} />
              {alerts} {alerts === 1 ? t('top.alert') : t('top.alerts')}
            </span>
          ) : (
            <span className="sc ok">{t('top.allClear')}</span>
          )}
          <span className="sc">
            <i className="live-dot" />
            {t('top.live')} 5s
          </span>
          <span className="sc hide-md">
            <Clock tz={overview?.timezone ?? 'UTC'} />
          </span>
          {scope && <span className="sc hide-md">{scope}</span>}
          <button className="btn primary sm ask-btn" onClick={() => setChatOpen(!chatOpen)}>
            <Icon name="spark" className="" />
            {t('chat.ask')}
          </button>
          <span className="avatar" title={scope}>
            {(scope || '?').slice(0, 1).toUpperCase()}
          </span>
        </header>
        <main className="stage-area">
          {view === 'pipelines' && (
            <>
              <Canvas
                search={search}
                health={health}
                motion={motion}
                selected={selected?.name}
                refreshKey={refreshKey}
                onSelect={(name, tab, focus) => setSelected({ name, tab, focus })}
                onNew={() => setCreating('designer')}
                toast={toast}
                onEdit={async (name) => {
                  try {
                    const cfg = await api<PipelineConfig>(`/config/pipelines/${encodeURIComponent(name)}`)
                    if (cfg.config) setEditing({ name, config: cfg.config })
                  } catch (e) {
                    toast((e as Error).message, true)
                  }
                }}
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
          {view === 'projects' && <ProjectsView toast={toast} />}
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
              toast={toast}
            />
          )}
          {chatOpen && <AssistantPanel onClose={() => setChatOpen(false)} />}
        </main>
      </div>
      {creating === 'designer' && (
        <Designer
          onClose={() => setCreating('')}
          onSimple={() => setCreating('simple')}
          onApplied={() => {
            setRefreshKey((k) => k + 1)
            loadOverview()
          }}
        />
      )}
      {editing && (
        <Designer
          key={editing.name}
          existing={editing}
          onClose={() => setEditing(null)}
          onSimple={() => {}}
          onApplied={() => {
            setRefreshKey((k) => k + 1)
            loadOverview()
          }}
        />
      )}
      {creating === 'simple' && (
        <NewPipelineModal
          onClose={() => setCreating('')}
          onDesigner={() => setCreating('designer')}
          onApplied={() => {
            setRefreshKey((k) => k + 1)
            loadOverview()
          }}
        />
      )}
      {toastMsg && (
        <div className={`toast ${toastMsg.error ? 'error' : ''}`}>
          <i className={`dot ${toastMsg.error ? 'down' : 'healthy'}`} />
          {toastMsg.text}
        </div>
      )}
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

interface NodeInfo {
  version: string
  live_nodes?: number
}

function Clock({ tz }: { tz: string }) {
  const [now, setNow] = useState(() => new Date())
  useEffect(() => {
    const id = setInterval(() => setNow(new Date()), 1000)
    return () => clearInterval(id)
  }, [])
  let text: string
  try {
    text = new Intl.DateTimeFormat('en-GB', { timeZone: tz, hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }).format(now)
  } catch {
    text = now.toISOString().slice(11, 19)
  }
  return (
    <>
      {text} <span className="dim">{tz}</span>
    </>
  )
}
