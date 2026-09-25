import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import { api, ApiError, streamEvents, type Diagnosis, type DLQEntry, type PipelineConfig, type TapRecord, type WorkerStatus } from '../api'
import { useT, type Key } from '../i18n'
import type { TailFocus } from './Canvas'
import { ConfigEditor } from './ConfigEditor'
import { ThroughputChart, useHistory } from './Metrics'

type Tab = 'health' | 'tail' | 'dlq' | 'reject' | 'config' | 'actions'

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
    ['dlq', 'panel.dlq'],
    ['reject', 'panel.rejects'],
    ['config', 'panel.config'],
    ['actions', 'panel.actions'],
  ]
  return (
    <aside className="drawer">
      <header>
        <h2>{name}</h2>
        <button className="btn ghost sm" onClick={onClose}>
          {t('common.close')}
        </button>
      </header>
      <nav className="tabs">
        {tabs.map(([id, label]) => (
          <button key={id} className={active === id ? 'active' : ''} onClick={() => setActive(id)}>
            {t(label)}
          </button>
        ))}
      </nav>
      <div className="body">
        {active === 'health' && <HealthTab name={name} />}
        {active === 'tail' && <TailTab key={name + JSON.stringify(focus)} name={name} focus={focus} />}
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

function HealthTab({ name }: { name: string }) {
  const t = useT()
  const { data, error } = usePoll<Diagnosis>(`/pipelines/${encodeURIComponent(name)}/diagnosis`, 5000)
  const hist = useHistory(name, 15)
  const samples = hist.data?.pipelines[name] ?? []
  if (error) return <p className="err">{error}</p>
  if (!data) return <p className="muted">{t('common.loading')}</p>
  const n = data.numbers
  return (
    <div className="stack">
      <div className="row">
        <span className={`chip ${data.health}`}>
          <i />
          {t(`health.${data.health}`)}
        </span>
        <span className="muted">{data.summary}</span>
      </div>
      <div className="numbers">
        <div>
          <b>{n.processed}</b>
          <span>{t('stat.processed')}</span>
        </div>
        <div>
          <b className={n.lag > 0 ? 'warn' : ''}>{n.lag}</b>
          <span>{t('stat.lag')}</span>
        </div>
        <div>
          <b>{n.avg_callback_ms.toFixed(1)}</b>
          <span>{t('stat.latency')}</span>
        </div>
        <div>
          <b className={n.rejected > 0 ? 'warn' : ''}>{n.rejected}</b>
          <span>{t('stat.rejected')}</span>
        </div>
        <div>
          <b className={n.pending_dlq_entries > 0 ? 'err' : ''}>{n.pending_dlq_entries}</b>
          <span>{t('stat.dlq')}</span>
        </div>
        <div>
          <b>{n.local_workers}</b>
          <span>{t('stat.workers')}</span>
        </div>
      </div>
      {samples.length > 1 && (
        <div className="card chart-card">
          <h3>{t('metrics.throughput')} · 15 min</h3>
          <ThroughputChart samples={samples} height={180} motion />
        </div>
      )}
      {(data.findings ?? []).map((f, i) => (
        <div key={i} className={`finding ${f.severity}`}>
          <div className="what">{f.what}</div>
          {f.why && <div className="why">{f.why}</div>}
          {f.suggested_actions && (
            <ul>
              {f.suggested_actions.map((a, j) => (
                <li key={j}>{a}</li>
              ))}
            </ul>
          )}
        </div>
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
      <div className="tail-controls">
        <select className="select" value={stage} onChange={(e) => setStage(e.target.value)}>
          <option value="">{t('tail.all')}</option>
          <option value="in">{t('tail.in')}</option>
          <option value="callback">{t('tail.callback')}</option>
          <option value="out">{t('tail.out')}</option>
        </select>
        <input className="input" placeholder={t('tail.filter')} value={filter} onChange={(e) => setFilter(e.target.value)} />
        <button className="btn" onClick={() => setPaused(!paused)}>
          {paused ? t('tail.resume') : t('tail.pause')}
        </button>
        <button className="btn ghost" onClick={() => setRecords([])}>
          {t('tail.clear')}
        </button>
      </div>
      {error && <p className="err">{error}</p>}
      <p className="small dim">
        {to && stage === 'out' ? `→ ${to} · ` : ''}
        {info ? `${info.local_workers} ${t('stat.workers')} · ` : ''}
        {shown.length} · {skipped > 0 ? `${skipped} ${t('tail.skipped')}` : ''}
      </p>
      {shown.length === 0 && !error && <div className="empty">{t('tail.waiting')}</div>}
      <div className="tail-list">
        {shown.map((r) => {
          const label = r.stage === 'out' ? r.to ?? 'out' : r.stage
          return (
            <div key={r.n} className="tail-row" onClick={() => setOpen(open === r.n ? null : r.n)}>
              <div className="line">
                <span className="t">{r.time.slice(11, 23)}</span>
                <span className={`stage ${label}`}>{label}</span>
                <span className="v">
                  {r.stage === 'callback' ? `#${r.attempt} ${r.target ?? ''}` : r.reason ? r.reason : r.value}
                </span>
                <span className={`status ${r.status && r.status >= 400 ? 'err' : 'dim'}`}>
                  {r.status ? r.status : ''}
                  {r.duration_ms ? ` ${r.duration_ms.toFixed(1)}ms` : ''}
                </span>
              </div>
              {open === r.n && (
                <>
                  <pre>{JSON.stringify(r, (k, v) => (k === 'n' ? undefined : v), 2)}</pre>
                  {r.correlation_id && (
                    <div style={{ padding: '0 10px 10px' }}>
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
      </div>
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
  if (data.length === 0) return <div className="empty">{t('dlq.empty')}</div>
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
          {e.reason && <div className={kind === 'dlq' ? 'err' : 'warn'}>{e.reason}</div>}
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
  return (
    <div className="stack">
      <div className="card stack">
        <div className="row" style={{ justifyContent: 'space-between' }}>
          <b>{paused ? t('act.resume') : t('act.pause')}</b>
          <button className="btn" disabled={busy} onClick={() => run(() => api(`${p}/${paused ? 'resume' : 'pause'}`, { method: 'POST', body: {} }), t('act.done'))}>
            {paused ? t('act.resume') : t('act.pause')}
          </button>
        </div>
        <span className="muted small">{t('act.pauseHint')}</span>
      </div>
      <div className="card stack">
        <div className="row" style={{ justifyContent: 'space-between' }}>
          <b>{t('act.restart')}</b>
          <button className="btn" disabled={busy} onClick={() => run(() => api(`${p}/restart`, { method: 'POST', body: {} }), t('act.done'))}>
            {t('act.restart')}
          </button>
        </div>
        <span className="muted small">{t('act.restartHint')}</span>
      </div>
      <div className="card stack">
        <div className="row" style={{ justifyContent: 'space-between' }}>
          <b>{t('act.scale')}</b>
          <span className="row">
            <input className="input" type="number" min={1} max={1024} style={{ width: 90 }} value={workers ?? current} onChange={(e) => setWorkers(Number(e.target.value))} />
            <button className="btn" disabled={busy || workers === null || workers === current} onClick={() => run(() => api(`${p}/scale`, { body: { workers } }), t('act.done'))}>
              {t('act.scale')}
            </button>
          </span>
        </div>
        <span className="muted small">{t('act.scaleHint')}</span>
      </div>
      <div className="card stack">
        <div className="row" style={{ justifyContent: 'space-between' }}>
          <b className="err">{t('act.delete')}</b>
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
            <span className="row">
              <span className="warn small">{t('common.sure')}</span>
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
                {t('common.yes')}
              </button>
            </span>
          )}
        </div>
        <span className="muted small">{confirmDelete ? confirmDelete.warnings.join(' ') : t('act.deleteHint')}</span>
      </div>
    </div>
  )
}
