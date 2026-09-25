import { useCallback, useEffect, useMemo, useState } from 'react'

import { api } from '../api'
import { useT } from '../i18n'
import { TimeChart, type Series } from './Chart'
import { Sparkline } from './Icon'
import { CountUp } from './fx'
import { Mark } from './Logo'

export interface Sample {
  time: string
  processed_per_sec: number
  rejected_per_sec: number
  dead_lettered_per_sec: number
  failed_per_sec: number
  lag: number
  avg_callback_ms: number
  p50_ms: number
  p95_ms: number
  p99_ms: number
  workers: number
  running: number
}

interface HistoryOut {
  timezone: string
  every_seconds: number
  pipelines: Record<string, Sample[]>
}

export function useHistory(pipeline: string, minutes: number) {
  const [data, setData] = useState<HistoryOut | null>(null)
  const [error, setError] = useState('')
  const load = useCallback(async () => {
    try {
      const q = new URLSearchParams({ minutes: String(minutes) })
      if (pipeline) q.set('pipeline', pipeline)
      setData(await api<HistoryOut>(`/history?${q}`))
      setError('')
    } catch (e) {
      setError((e as Error).message)
    }
  }, [pipeline, minutes])
  useEffect(() => {
    load()
    const id = setInterval(load, 5000)
    return () => clearInterval(id)
  }, [load])
  return { data, error }
}

export function RateChart({ samples }: { samples: Sample[] }) {
  const series = useMemo<Series[]>(() => [{ name: 'msg/s', kind: 'single', points: samples.map((s) => [s.time, s.processed_per_sec + s.rejected_per_sec + s.dead_lettered_per_sec]) }], [samples])
  return <TimeChart series={series} unit="msg/s" height={150} />
}

export function ThroughputChart({ samples, height, motion }: { samples: Sample[]; height?: number; motion: boolean }) {
  const t = useT()
  const series = useMemo<Series[]>(
    () => [
      { name: t('metrics.processed'), kind: 'processed', points: samples.map((s) => [s.time, s.processed_per_sec]) },
      { name: t('metrics.rejected'), kind: 'rejected', points: samples.map((s) => [s.time, s.rejected_per_sec]) },
      { name: t('metrics.dlq'), kind: 'dlq', points: samples.map((s) => [s.time, s.dead_lettered_per_sec]) },
    ],
    [samples, t],
  )
  return <TimeChart series={series} unit="msg/s" height={height} motion={motion} />
}

function avg(samples: Sample[], f: (s: Sample) => number) {
  return samples.length ? samples.reduce((a, s) => a + f(s), 0) / samples.length : 0
}

const total = (s: Sample) => s.processed_per_sec + s.rejected_per_sec + s.dead_lettered_per_sec

function Delta({ now, before }: { now: number; before: number }) {
  if (before <= 0) return null
  const pct = ((now - before) / before) * 100
  return <span className={`badge-mono delta ${pct >= 0 ? 'up' : 'down'}`}>{`${pct >= 0 ? '↗ +' : '↘ '}${pct.toFixed(1)}%`}</span>
}

interface NodeOut {
  version: string
  go_version: string
  uptime_seconds: number
  goroutines: number
  heap_alloc_bytes: number
  sys_bytes: number
  num_cpu: number
  gomaxprocs: number
  gc_cycles: number
  node_id?: string
  cluster?: string
}

function uptime(s: number) {
  const d = Math.floor(s / 86400)
  const h = Math.floor((s % 86400) / 3600)
  const m = Math.floor((s % 3600) / 60)
  return d ? `${d}d ${h}h` : h ? `${h}h ${m}m` : `${m}m ${s % 60}s`
}

export function MetricsView({ pipelines, motion }: { pipelines: string[]; motion: boolean }) {
  const t = useT()
  const [pipeline, setPipeline] = useState(pipelines[0] ?? '')
  const [minutes, setMinutes] = useState(15)
  const [node, setNode] = useState<NodeOut | null>(null)
  useEffect(() => {
    if (!pipeline && pipelines.length) setPipeline(pipelines[0])
  }, [pipelines, pipeline])
  useEffect(() => {
    const load = () => api<NodeOut>('/node').then(setNode, () => {})
    load()
    const id = setInterval(load, 5000)
    return () => clearInterval(id)
  }, [])
  const { data, error } = useHistory(pipeline, 60)
  const all = (pipeline && data?.pipelines[pipeline]) || []
  const cut = Date.now() - minutes * 60 * 1000
  const samples = all.filter((s) => Date.parse(s.time) >= cut)
  const before = all.filter((s) => Date.parse(s.time) >= cut - minutes * 60 * 1000 && Date.parse(s.time) < cut)
  const recent = samples.slice(-12)
  const rateNow = avg(samples.slice(-3), total)
  const peak = Math.max(0, ...samples.map(total))
  const errNow = avg(recent, (s) => s.rejected_per_sec + s.dead_lettered_per_sec)
  const errRate = avg(recent, total) > 0 ? (errNow / avg(recent, total)) * 100 : 0
  const withCalls = samples.filter((s) => s.p50_ms > 0)
  const lat = withCalls[withCalls.length - 1]
  const lagNow = samples[samples.length - 1]?.lag ?? 0
  const lagPeak = Math.max(0, ...samples.map((s) => s.lag))

  const lag = useMemo<Series[]>(() => [{ name: t('stat.lag'), kind: 'single', points: samples.map((s) => [s.time, s.lag]) }], [samples, t])
  const pct = useMemo<Series[]>(
    () => [
      { name: 'p50', kind: 'p50', points: withCalls.map((s) => [s.time, s.p50_ms]) },
      { name: 'p95', kind: 'p95', points: withCalls.map((s) => [s.time, s.p95_ms]) },
      { name: 'p99', kind: 'p99', points: withCalls.map((s) => [s.time, s.p99_ms]) },
    ],
    [withCalls],
  )

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <div className="eyebrow">
            <span className="tagbox">{t('metrics.stream').toUpperCase()}</span>
            <span className="live">
              <i />
              {t('metrics.sync')}
            </span>
          </div>
          <h1 className="title-grad">{t('nav.metrics')}</h1>
          <p className="lead">{t('metrics.lead')}</p>
        </div>
        <div className="row">
          <select className="select" style={{ width: 200 }} value={pipeline} onChange={(e) => setPipeline(e.target.value)}>
            {pipelines.map((p) => (
              <option key={p}>{p}</option>
            ))}
          </select>
          <div className="seg">
            {[5, 15, 30, 60].map((m) => (
              <button key={m} className={minutes === m ? 'active' : ''} onClick={() => setMinutes(m)}>
                {m}m
              </button>
            ))}
          </div>
          <span className="sc">
            <i className="live-dot" />
            5s
          </span>
          <span className="sc">{data?.timezone}</span>
        </div>
      </div>
      {error && <p className="err">{error}</p>}
      <div className="tiles rise-in">
        <div className="tile spot">
          <div className="th">
            <span className="micro">{t('metrics.throughputNow')}</span>
            <Delta now={rateNow} before={avg(before, total)} />
          </div>
          <b>
            <CountUp value={rateNow} decimals={1} />
            <small>msg/s</small>
          </b>
          <div className="row between">
            <Sparkline values={samples.slice(-30).map(total)} width={120} height={26} />
            <span className="small dim mono">
              {t('metrics.peak')}: {peak.toFixed(1)}
            </span>
          </div>
        </div>
        <div className="tile spot">
          <div className="th">
            <span className="micro">{t('kpi.lag')}</span>
            <span className={`badge-mono ${lagNow === 0 ? 'delta up' : 'warn'}`}>{lagNow === 0 ? t('kpi.nominal') : t('kpi.backlog')}</span>
          </div>
          <b>
            <CountUp value={lagNow} />
            <small>{t('metrics.messages')}</small>
          </b>
          <div className="progress">
            <i style={{ width: `${lagPeak > 0 ? (lagNow / lagPeak) * 100 : 0}%` }} />
          </div>
          <span className="small dim mono">
            {t('metrics.peak')}: {lagPeak}
          </span>
        </div>
        <div className="tile spot">
          <div className="th">
            <span className="micro">{t('metrics.latency')} p50</span>
          </div>
          <b>
            {lat ? lat.p50_ms.toFixed(1) : '0.0'}
            <small>ms</small>
          </b>
          <span className="small dim mono">{lat ? `p95 ${lat.p95_ms.toFixed(1)} · p99 ${lat.p99_ms.toFixed(1)}` : '·'}</span>
        </div>
        <div className="tile spot">
          <div className="th">
            <span className="micro">{t('metrics.errorRate')}</span>
            {errRate === 0 && <span className="badge-mono delta up">{t('metrics.clean')}</span>}
          </div>
          <b className={errRate > 5 ? 'err' : errRate > 0 ? 'warn' : ''}>
            <CountUp value={errRate} decimals={1} />
            <small>%</small>
          </b>
          <span className="small dim mono">{errNow.toFixed(2)} msg/s</span>
        </div>
      </div>
      {samples.length < 2 ? (
        <div className="empty card">{t('metrics.noData')}</div>
      ) : (
        <div className="charts">
          <section className="card chart-card wide spot">
            <h3>{t('metrics.throughput')}</h3>
            <ThroughputChart samples={samples} height={280} motion={motion} />
          </section>
          <section className="card chart-card">
            <h3>{t('metrics.lagTitle')}</h3>
            <TimeChart series={lag} unit={t('metrics.messages')} motion={motion} />
          </section>
          <section className="card chart-card">
            <h3>{t('metrics.pctTitle')}</h3>
            {withCalls.length > 1 ? <TimeChart series={pct} unit="ms" motion={motion} /> : <div className="empty small">{t('metrics.noData')}</div>}
          </section>
        </div>
      )}
      {node && (
        <div className="card instance">
          <div className="who">
            <span className="lg">
              <Mark />
            </span>
            <div>
              <b>
                {t('metrics.instance')}: {node.node_id ?? 'ark'}
              </b>
              <div className="small dim mono">
                {node.version} · {node.go_version}
                {node.cluster ? ` · ${node.cluster}` : ''}
              </div>
            </div>
          </div>
          <div className="m">
            <span className="micro">{t('metrics.heap')}</span>
            <b>{(node.heap_alloc_bytes / 1048576).toFixed(1)} MB</b>
            <div className="meter">
              <i style={{ width: `${Math.min(100, (node.heap_alloc_bytes / node.sys_bytes) * 100)}%` }} />
            </div>
          </div>
          <div className="m">
            <span className="micro">{t('metrics.goroutines')}</span>
            <b>{node.goroutines}</b>
          </div>
          <div className="m">
            <span className="micro">{t('metrics.cpus')}</span>
            <b>
              {node.gomaxprocs}/{node.num_cpu}
            </b>
          </div>
          <div className="m">
            <span className="micro">{t('metrics.gc')}</span>
            <b>{node.gc_cycles}</b>
          </div>
          <div className="m">
            <span className="micro">{t('metrics.uptime')}</span>
            <b>{uptime(node.uptime_seconds)}</b>
          </div>
        </div>
      )}
      <details className="small" style={{ marginTop: 16 }}>
        <summary className="muted" style={{ cursor: 'pointer' }}>
          {t('metrics.table')}
        </summary>
        <div className="table-wrap" style={{ marginTop: 10 }}><table className="list">
          <thead>
            <tr>
              <th>time</th>
              <th>{t('metrics.processed')}</th>
              <th>{t('metrics.rejected')}</th>
              <th>{t('metrics.dlq')}</th>
              <th>{t('stat.lag')}</th>
              <th>ms</th>
            </tr>
          </thead>
          <tbody>
            {samples
              .slice(-60)
              .reverse()
              .map((s) => (
                <tr key={s.time}>
                  <td className="mono">{s.time}</td>
                  <td className="mono">{s.processed_per_sec.toFixed(2)}</td>
                  <td className="mono">{s.rejected_per_sec.toFixed(2)}</td>
                  <td className="mono">{s.dead_lettered_per_sec.toFixed(2)}</td>
                  <td className="mono">{s.lag}</td>
                  <td className="mono">{s.avg_callback_ms.toFixed(1)}</td>
                </tr>
              ))}
          </tbody>
        </table></div>
      </details>
    </div>
  )
}
