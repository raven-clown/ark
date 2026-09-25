import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import { api, ApiError, streamEvents, type Diagnosis, type DLQEntry, type PipelineConfig, type TapRecord, type WorkerStatus } from '../api'
import { useT, type Key } from '../i18n'
import type { TailFocus } from './Canvas'
import { ConfigEditor } from './ConfigEditor'
import { RulesTab } from './RulesTab'
import { FindingCard, Icon, type IconName } from './Icon'
import { CountUp } from './fx'
import { RateChart, useHistory } from './Metrics'

type Tab = 'health' | 'tail' | 'rules' | 'dlq' | 'reject' | 'config' | 'actions'

interface Props {
  name: string
  tab?: string
  focus?: TailFocus
  onClose: () => void
  onChanged: () => void
  toast: (msg: string, error?: boolean) => void
}

export function PipelinePanel({ name, tab, focus, onClose, onChanged, toast }: Props) {
  const t = useT()
  const [active, setActive] = useState<Tab>((tab as Tab) ?? 'health')
  useEffect(() => setActive((tab as Tab) ?? 'health'), [name, tab, focus])

  const tabs: [Tab, Key][] = [
    ['health', 'panel.health'],
    ['tail', 'panel.tail'],
    ['rules', 'panel.rules'],
    ['dlq', 'panel.dlq'],
    ['reject', 'panel.rejects'],
    ['config', 'panel.config'],
    ['actions', 'panel.actions'],
  ]
  return (
    <aside className="drawer">
      <header>
        <div className="t">
          <h2>{name}</h2>
          <p>{t('nav.pipelines')}</p>
        </div>
        <button className="btn ghost icon-btn" aria-label={t('common.close')} onClick={onClose}>
          <Icon name="close" className="" />
        </button>
      </header>
      <div className="tabbar">
        <div className="seg">
          {tabs.map(([id, label]) => (
            <button key={id} className={active === id ? 'active' : ''} onClick={() => setActive(id)}>
              {t(label)}
            </button>
          ))}
        </div>
      </div>
      <div className="body">
        {active === 'health' && <HealthTab name={name} onOpen={setActive} toast={toast} />}
        {active === 'tail' && <TailTab key={name + JSON.stringify(focus)} name={name} focus={focus} />}
        {active === 'rules' && <RulesTab name={name} onChanged={onChanged} />}
        {active === 'dlq' && <EntriesTab name={name} kind="dlq" toast={toast} />}
        {active === 'reject' && <EntriesTab name={name} kind="reject" toast={toast} />}
        {active === 'config' && <ConfigTab name={name} onChanged={onChanged} />}
        {active === 'actions' && <ActionsTab name={name} onChanged={onChanged} onDeleted={onClose} toast={toast} />}
      </div>
    </aside>
  )
}

function usePoll<T>(path: string, every: number) {
  const [data, setData] = useState<T | null>(null)
  const [error, setError] = useState('')
  const load = useCallback(async () => {
    try {
      setData(await api<T>(path))
      setError('')
    } catch (e) {
      setError((e as Error).message)
    }
  }, [path])
  useEffect(() => {
    load()
    if (!every) return
    const id = setInterval(load, every)
    return () => clearInterval(id)
  }, [load, every])
  return { data, error, reload: load }
}

function pctChange(now: number, base: number) {
  if (base <= 0) return null
  return ((now - base) / base) * 100
}

function traceLine(r: TapRecord): { cls: string; tag: string; text: string } {
  if (r.stage === 'in') return { cls: 'call', tag: 'IN', text: `${r.topic ?? ''} p${r.partition ?? '?'}@${r.offset ?? '?'} ${r.key ? `key=${r.key}` : ''}` }
  if (r.stage === 'callback') {
    const bad = !r.status || r.status >= 400
    return { cls: bad ? 'err' : 'call', tag: bad ? 'ERR' : 'CALL', text: `#${r.attempt} ${r.status ?? ''} ${r.duration_ms?.toFixed(1) ?? ''}ms ${r.reason ?? ''}` }
  }
  if (r.to === 'dlq') return { cls: 'err', tag: 'DLQ', text: r.reason ?? `→ ${r.topic}` }
  if (r.to === 'reject') return { cls: 'rej', tag: 'REJ', text: r.reason ?? `→ ${r.topic}` }
  return { cls: 'ok', tag: 'OK', text: `→ ${r.topic ?? r.target ?? r.to}${r.rule ? ` (rule ${r.rule})` : ''}` }
}

function HealthTab({ name, onOpen, toast }: { name: string; onOpen: (tab: Tab) => void; toast: (m: string, e?: boolean) => void }) {
  const t = useT()
  const enc = encodeURIComponent(name)
  const { data, error } = usePoll<Diagnosis>(`/pipelines/${enc}/diagnosis`, 5000)
  const workers = usePoll<WorkerStatus[]>(`/pipelines/${enc}`, 3000)
  const hist = useHistory(name, 60)
  const [trace, setTrace] = useState<(TapRecord & { n: number })[]>([])
  const [busy, setBusy] = useState(false)
  const n = useRef(0)

  useEffect(() => {
    const ctl = new AbortController()
    streamEvents(`/pipelines/${enc}/tail?max_per_sec=20`, ctl.signal, (event, raw) => {
      if (event !== 'record') return
      const rec = JSON.parse(raw) as TapRecord
      if (rec.stage === 'in') return
      const k = n.current++
      setTrace((cur) => [...cur, { ...rec, n: k }].slice(-9))
    }).catch(() => {})
    return () => ctl.abort()
  }, [enc])

  if (error) return <p className="err">{error}</p>
  if (!data) return <p className="muted">{t('common.loading')}</p>
  const samples = hist.data?.pipelines[name] ?? []
  const recent = samples.slice(-3)
  const rateNow = recent.length ? recent.reduce((a, x) => a + x.processed_per_sec + x.rejected_per_sec + x.dead_lettered_per_sec, 0) / recent.length : 0
  const rateHour = samples.length ? samples.reduce((a, x) => a + x.processed_per_sec + x.rejected_per_sec + x.dead_lettered_per_sec, 0) / samples.length : 0
  const change = pctChange(rateNow, rateHour)
  const withCalls = samples.filter((x) => x.p99_ms > 0)
  const lastLat = withCalls[withCalls.length - 1]
  const ws = Array.isArray(workers.data) ? workers.data : []
  const running = ws.filter((w) => w.running).length
  const top = (data.findings ?? [])[0]
  const rest = (data.findings ?? []).slice(1)
  const last15 = samples.filter((x) => Date.parse(x.time) >= Date.now() - 15 * 60 * 1000)

  const retryAll = async () => {
    setBusy(true)
    try {
      const entries = await api<DLQEntry[]>(`/pipelines/${enc}/dlq`)
      let ok = 0
      for (const e of entries) {
        try {
          await api(`/pipelines/${enc}/dlq/${encodeURIComponent(e.id)}/retry`, { method: 'POST', body: {} })
          ok++
        } catch {
          // counted below
        }
      }
      toast(`${ok}/${entries.length} ${t('dlq.retry')} · ${t('act.done')}`, ok < entries.length)
    } catch (e) {
      toast(e instanceof ApiError && e.status === 403 ? t('common.noAccess') : (e as Error).message, true)
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="stack">
      <div className={`hbox ${data.health}`}>
        <div className="top">
          <span className="micro">{t('health.status')}</span>
          <span className={`st ${data.health}`}>{t(`health.${data.health}`)}</span>
        </div>
        <p>{top ? top.what : data.summary}</p>
        {top?.why && <p className="small muted">{top.why}</p>}
      </div>

      <div className="kpis rise-in">
        <div className="kpi">
          <span className="micro">{t('kpi.throughput')}</span>
          <b>
            <CountUp value={rateNow} decimals={1} />
            <small>msg/s</small>
          </b>
          <span className="sub">
            {change === null ? '·' : <span className={`delta ${change >= 0 ? 'up' : 'down'}`}>{change >= 0 ? '▲' : '▼'} {Math.abs(change).toFixed(1)}%</span>} {t('kpi.vs1h')}
          </span>
        </div>
        <div className="kpi">
          <span className="micro">{t('kpi.lag')}</span>
          <b className={data.numbers.lag > 0 ? 'warn' : ''}>
            <CountUp value={data.numbers.lag} />
            <small>msg</small>
          </b>
          <span className="sub">{data.numbers.lag > 0 ? t('kpi.backlog') : t('kpi.nominal')}</span>
        </div>
        <div className="kpi">
          <span className="micro">{t('kpi.p99')}</span>
          <b>
            {lastLat ? lastLat.p99_ms.toFixed(1) : data.numbers.avg_callback_ms.toFixed(1)}
            <small>ms</small>
          </b>
          <span className="sub">{lastLat ? `p50 ${lastLat.p50_ms.toFixed(1)} · p95 ${lastLat.p95_ms.toFixed(1)}` : 'avg'}</span>
        </div>
        <div className="kpi">
          <span className="micro">{t('kpi.workers')}</span>
          <b>
            {running} / {ws.length}
          </b>
          <span className="sub">{ws[0]?.consumer_group ?? ''}</span>
        </div>
      </div>

      <div className="wbar">
        <div className="row between">
          <span className="micro">{t('workers.title')}</span>
          <span className="micro" style={{ color: 'var(--frost)' }}>
            {running}/{ws.length} {t('workers.running')}
          </span>
        </div>
        <div className="segs">
          {ws.map((w) => (
            <i key={w.worker} title={`worker ${w.worker}`} className={!w.running ? 'stopped' : w.paused ? 'paused' : w.breaker_state === 'open' ? 'open' : ''} />
          ))}
        </div>
      </div>

      <div className="panel-card">
        <div className="ph">
          <span className="micro">{t('rate.title')}</span>
          <span className="live">
            <i />
            {t('top.live')}
          </span>
        </div>
        {last15.length > 1 ? <RateChart samples={last15} /> : <div className="empty small">{t('metrics.noData')}</div>}
      </div>

      <div>
        <div className="row between" style={{ marginBottom: 7 }}>
          <span className="micro">{t('trace.title')}</span>
          <button className="btn ghost sm" onClick={() => onOpen('tail')}>
            {t('trace.open')} →
          </button>
        </div>
        <div className="terminal">
          {trace.length === 0 && <div className="dim">{t('trace.waiting')}</div>}
          {trace.map((r) => {
            const l = traceLine(r)
            return (
              <div key={r.n} className="l">
                <span className="dim">{r.time.slice(11, 19)}</span>
                <span className={l.cls}>[{l.tag}]</span>
                <span>{l.text}</span>
              </div>
            )
          })}
        </div>
      </div>

      <div className="btn-row">
        <button className="btn" disabled={busy} onClick={async () => {
          try {
            await api(`/pipelines/${enc}/restart`, { method: 'POST', body: {} })
            toast(t('act.done'))
          } catch (e) {
            toast(e instanceof ApiError && e.status === 403 ? t('common.noAccess') : (e as Error).message, true)
          }
        }}>
          <Icon name="restart" className="" />
          {t('act.restart')}
        </button>
        <button className="btn" onClick={() => onOpen('tail')}>
          <Icon name="search" className="" />
          {t('panel.tail')}
        </button>
      </div>
      {data.numbers.pending_dlq_entries > 0 && (
        <button className="btn danger block" disabled={busy} onClick={retryAll}>
          <Icon name="restart" className="" />
          {t('act.retryAll')} ({data.numbers.pending_dlq_entries})
        </button>
      )}

      {rest.map((f, i) => (
        <FindingCard key={i} severity={f.severity} what={f.what} why={f.why} actions={f.suggested_actions} />
      ))}
    </div>
  )
}

const MAX_RECORDS = 500

function TailTab({ name, focus }: { name: string; focus?: TailFocus }) {
  const t = useT()
  const [records, setRecords] = useState<(TapRecord & { n: number })[]>([])
  const [paused, setPaused] = useState(false)
  const [stage, setStage] = useState<string>(focus?.stage ?? '')
  const [to] = useState<string>(focus?.to ?? '')
  const [filter, setFilter] = useState('')
  const [open, setOpen] = useState<number | null>(null)
  const [skipped, setSkipped] = useState(0)
  const [error, setError] = useState('')
  const [info, setInfo] = useState<{ local_workers: number } | null>(null)
  const pausedRef = useRef(paused)
  pausedRef.current = paused
  const counter = useRef(0)

  useEffect(() => {
    const ctl = new AbortController()
    const q = new URLSearchParams({ max_per_sec: '100' })
    if (stage) q.set('stage', stage)
    if (to && stage === 'out') q.set('to', to)
    streamEvents(`/pipelines/${encodeURIComponent(name)}/tail?${q}`, ctl.signal, (event, data) => {
      if (event === 'hello') setInfo(JSON.parse(data))
      if (event === 'ping') {
        const p = JSON.parse(data)
        setSkipped((s) => s + (p.skipped ?? 0) + (p.dropped ?? 0))
      }
      if (event === 'record' && !pausedRef.current) {
        const rec = JSON.parse(data) as TapRecord
        const n = counter.current++
        setRecords((cur) => [{ ...rec, n }, ...cur].slice(0, MAX_RECORDS))
      }
    }).catch((e) => {
      if (!ctl.signal.aborted) setError((e as Error).message)
    })
    return () => ctl.abort()
  }, [name, stage, to])

  const shown = useMemo(() => {
    const f = filter.trim().toLowerCase()
    if (!f) return records
    return records.filter((r) => [r.key, r.correlation_id, r.value, r.reason].some((v) => v?.toLowerCase().includes(f)))
  }, [records, filter])

  return (
    <div>
      <div className="tail-bar">
        <select className="select" style={{ width: 150 }} value={stage} onChange={(e) => setStage(e.target.value)}>
          <option value="">{t('tail.all')}</option>
          <option value="in">{t('tail.in')}</option>
          <option value="callback">{t('tail.callback')}</option>
          <option value="out">{t('tail.out')}</option>
        </select>
        <input className="input" placeholder={t('tail.filter')} value={filter} onChange={(e) => setFilter(e.target.value)} />
        <div className="row" style={{ flexWrap: 'nowrap' }}>
          <button className="btn icon-btn" title={paused ? t('tail.resume') : t('tail.pause')} onClick={() => setPaused(!paused)}>
            <Icon name={paused ? 'play' : 'pause'} className="" />
          </button>
          <button className="btn icon-btn" title={t('tail.clear')} onClick={() => setRecords([])}>
            <Icon name="trash" className="" />
          </button>
        </div>
      </div>
      {error && <p className="err">{error}</p>}
      <div className="tail-meta">
        <span className={`live ${paused || error ? 'off' : ''}`}>
          <i />
          {paused ? t('tail.pause') : 'Live'}
          {to && stage === 'out' ? ` · ${to}` : ''}
        </span>
        <span>
          {info ? `${info.local_workers} ${t('stat.workers')} · ` : ''}
          {shown.length}
          {skipped > 0 ? ` · ${skipped} ${t('tail.skipped')}` : ''}
        </span>
      </div>
      {shown.length === 0 && !error && (
        <div className="empty">
          <Icon name="search" className="" />
          {t('tail.waiting')}
        </div>
      )}
      {shown.length > 0 && <div className="tail-list">
        {shown.map((r) => {
          const label = r.stage === 'out' ? r.to ?? 'out' : r.stage
          return (
            <div key={r.n} className={`tail-row ${open === r.n ? 'open' : ''}`} onClick={() => setOpen(open === r.n ? null : r.n)}>
              <div className="line">
                <span className="t">{r.time.slice(11, 23)}</span>
                <span className={`tag ${label}`}>{label}</span>
                <span className="v">
                  {r.stage === 'callback' ? `#${r.attempt} ${r.target ?? ''}` : r.reason ? r.reason : r.value}
                </span>
                <span className={`status ${r.status && r.status >= 400 ? 'bad' : ''}`}>
                  {r.status ? r.status : ''}
                  {r.duration_ms ? ` ${r.duration_ms.toFixed(1)}ms` : ''}
                </span>
              </div>
              {open === r.n && (
                <>
                  <pre>{JSON.stringify(r, (k, v) => (k === 'n' ? undefined : v), 2)}</pre>
                  {r.correlation_id && (
                    <div style={{ padding: '0 12px 12px' }}>
                      <button
                        className="btn sm"
                        onClick={(e) => {
                          e.stopPropagation()
                          setFilter(r.correlation_id!)
                        }}
                      >
                        {t('tail.follow')}
                      </button>
                    </div>
                  )}
                </>
              )}
            </div>
          )
        })}
      </div>}
    </div>
  )
}

function EntriesTab({ name, kind, toast }: { name: string; kind: 'dlq' | 'reject'; toast: (m: string, e?: boolean) => void }) {
  const t = useT()
  const path = `/pipelines/${encodeURIComponent(name)}/${kind}`
  const { data, error, reload } = usePoll<DLQEntry[]>(path, 5000)
  const act = async (id: string, action: 'retry' | 'discard') => {
    try {
      await api(`${path}/${encodeURIComponent(id)}/${action}`, { method: 'POST', body: {} })
      toast(`${id}: ${action === 'retry' ? t('dlq.retry') : t('dlq.discard')} · ${t('act.done')}`)
      reload()
    } catch (e) {
      toast(e instanceof ApiError && e.status === 403 ? t('common.noAccess') : (e as Error).message, true)
    }
  }
  if (error) return <p className="err">{error}</p>
  if (!data) return <p className="muted">{t('common.loading')}</p>
  if (data.length === 0)
    return (
      <div className="empty">
        <Icon name="inbox" className="" />
        {t('dlq.empty')}
      </div>
    )
  return (
    <div className="stack">
      {data.map((e) => (
        <div key={e.id} className="entry">
          <div className="row" style={{ justifyContent: 'space-between' }}>
            <span className="mono small dim">
              {e.id} · {e.failed_at ?? e.timestamp} {e.redrives > 0 ? `· ${e.redrives} ${t('dlq.redrives')}` : ''}
            </span>
            <span className="row">
              <button className="btn sm" onClick={() => act(e.id, 'retry')}>
                {t('dlq.retry')}
              </button>
              <button className="btn sm danger" onClick={() => act(e.id, 'discard')}>
                {t('dlq.discard')}
              </button>
            </span>
          </div>
          {e.reason && (
            <div className={`reason ${kind === 'dlq' ? 'err' : 'warn'}`}>
              <i className={`dot ${kind === 'dlq' ? 'down' : 'degraded'}`} style={{ marginTop: 6, animation: 'none' }} />
              {e.reason}
            </div>
          )}
          <pre>{e.value}</pre>
        </div>
      ))}
    </div>
  )
}

function ConfigTab({ name, onChanged }: { name: string; onChanged: () => void }) {
  const t = useT()
  const { data, error } = usePoll<PipelineConfig>(`/config/pipelines/${encodeURIComponent(name)}`, 0)
  if (error) return <p className="err">{error}</p>
  if (!data) return <p className="muted">{t('common.loading')}</p>
  return <ConfigEditor initial={data.yaml} onApplied={onChanged} />
}

function ActionsTab({ name, onChanged, onDeleted, toast }: { name: string; onChanged: () => void; onDeleted: () => void; toast: (m: string, e?: boolean) => void }) {
  const t = useT()
  const { data, reload } = usePoll<WorkerStatus[]>(`/pipelines/${encodeURIComponent(name)}`, 3000)
  const cfg = usePoll<PipelineConfig>(`/config/pipelines/${encodeURIComponent(name)}`, 0)
  const current = Number(/^workers:\s*(\d+)/m.exec(cfg.data?.yaml ?? '')?.[1] ?? 1)
  const [workers, setWorkers] = useState<number | null>(null)
  const [busy, setBusy] = useState(false)
  const [confirmDelete, setConfirmDelete] = useState<{ token: string; warnings: string[] } | null>(null)
  const paused = Array.isArray(data) && data.some((w) => w.paused)

  const run = async (fn: () => Promise<unknown>, done: string) => {
    setBusy(true)
    try {
      await fn()
      toast(done)
      reload()
      onChanged()
    } catch (e) {
      toast(e instanceof ApiError && e.status === 403 ? t('common.noAccess') : (e as Error).message, true)
    } finally {
      setBusy(false)
    }
  }
  const p = `/pipelines/${encodeURIComponent(name)}`
  const Row = ({ icon, title, hint, danger, children }: { icon: IconName; title: string; hint: string; danger?: boolean; children: React.ReactNode }) => (
    <div className={`action ${danger ? 'danger' : ''}`}>
      <div className="ico">
        <Icon name={icon} className="" />
      </div>
      <div>
        <b className={danger ? 'err' : ''}>{title}</b>
        <p>{hint}</p>
      </div>
      <div className="row" style={{ flexWrap: 'nowrap' }}>
        {children}
      </div>
    </div>
  )
  return (
    <div className="stack">
      <Row icon={paused ? 'play' : 'pause'} title={paused ? t('act.resume') : t('act.pause')} hint={t('act.pauseHint')}>
        <button className="btn" disabled={busy} onClick={() => run(() => api(`${p}/${paused ? 'resume' : 'pause'}`, { method: 'POST', body: {} }), t('act.done'))}>
          {paused ? t('act.resume') : t('act.pause')}
        </button>
      </Row>
      <Row icon="restart" title={t('act.restart')} hint={t('act.restartHint')}>
        <button className="btn" disabled={busy} onClick={() => run(() => api(`${p}/restart`, { method: 'POST', body: {} }), t('act.done'))}>
          {t('act.restart')}
        </button>
      </Row>
      <Row icon="scale" title={t('act.scale')} hint={t('act.scaleHint')}>
        <input className="input" type="number" min={1} max={1024} style={{ width: 72 }} value={workers ?? current} onChange={(e) => setWorkers(Number(e.target.value))} />
        <button className="btn" disabled={busy || workers === null || workers === current} onClick={() => run(() => api(`${p}/scale`, { body: { workers } }), t('act.done'))}>
          {t('act.scale')}
        </button>
      </Row>
      <Row icon="trash" danger title={t('act.delete')} hint={confirmDelete ? confirmDelete.warnings.join(' ') : t('act.deleteHint')}>
        {!confirmDelete ? (
          <button
            className="btn danger"
            disabled={busy}
            onClick={async () => {
              try {
                const out = await api<{ confirm_token: string; preview: { warnings?: { what: string }[] } }>('/config/preview', { body: { delete: name } })
                setConfirmDelete({ token: out.confirm_token, warnings: (out.preview.warnings ?? []).map((w) => w.what) })
              } catch (e) {
                toast(e instanceof ApiError && e.status === 403 ? t('common.noAccess') : (e as Error).message, true)
              }
            }}
          >
            {t('act.delete')}
          </button>
        ) : (
          <>
            <button className="btn ghost sm" onClick={() => setConfirmDelete(null)}>
              {t('common.no')}
            </button>
            <button
              className="btn danger sm"
              onClick={() =>
                run(async () => {
                  await api('/config/confirm', { body: { confirm_token: confirmDelete.token } })
                  onDeleted()
                }, t('act.done'))
              }
            >
              {t('common.sure')} {t('common.yes')}
            </button>
          </>
        )}
      </Row>
    </div>
  )
}
