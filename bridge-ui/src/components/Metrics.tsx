import { useCallback, useEffect, useMemo, useState } from 'react'

import { api } from '../api'
import { useT } from '../i18n'
import { TimeChart, type Series } from './Chart'

export interface Sample {
  time: string
  processed_per_sec: number
  rejected_per_sec: number
  dead_lettered_per_sec: number
  failed_per_sec: number
  lag: number
  avg_callback_ms: number
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

function last(samples: Sample[]) {
  return samples.length ? samples[samples.length - 1] : undefined
}

function avg(samples: Sample[], f: (s: Sample) => number) {
  return samples.length ? samples.reduce((a, s) => a + f(s), 0) / samples.length : 0
}

export function MetricsView({ pipelines, motion }: { pipelines: string[]; motion: boolean }) {
  const t = useT()
  const [pipeline, setPipeline] = useState(pipelines[0] ?? '')
  const [minutes, setMinutes] = useState(15)
  useEffect(() => {
    if (!pipeline && pipelines.length) setPipeline(pipelines[0])
  }, [pipelines, pipeline])
  const { data, error } = useHistory(pipeline, minutes)
  const samples = (pipeline && data?.pipelines[pipeline]) || []
  const now = last(samples)
  const recent = samples.slice(-12)
  const errorRate = (() => {
    const total = avg(recent, (s) => s.processed_per_sec + s.rejected_per_sec + s.dead_lettered_per_sec)
    return total > 0 ? (avg(recent, (s) => s.rejected_per_sec + s.dead_lettered_per_sec) / total) * 100 : 0
  })()
  const lag = useMemo<Series[]>(() => [{ name: t('stat.lag'), kind: 'single', points: samples.map((s) => [s.time, s.lag]) }], [samples, t])
  const latency = useMemo<Series[]>(() => [{ name: t('stat.latency'), kind: 'single', points: samples.map((s) => [s.time, s.avg_callback_ms]) }], [samples, t])

  return (
    <div className="page">
      <h1>{t('nav.metrics')}</h1>
      <p className="lead">{t('metrics.lead')}</p>
      <div className="row" style={{ marginBottom: 18 }}>
        <select className="select" style={{ width: 240 }} value={pipeline} onChange={(e) => setPipeline(e.target.value)}>
          {pipelines.map((p) => (
            <option key={p}>{p}</option>
          ))}
        </select>
        <div className="seg">
          {[15, 30, 60].map((m) => (
            <button key={m} className={minutes === m ? 'active' : ''} onClick={() => setMinutes(m)}>
              {m} min
            </button>
          ))}
        </div>
        <span className="small dim mono">{data?.timezone}</span>
      </div>
      {error && <p className="err">{error}</p>}
      <div className="tiles">
        <div className="tile">
          <span>{t('metrics.throughputNow')}</span>
          <b>{(now?.processed_per_sec ?? 0).toFixed(1)}</b>
          <small>msg/s</small>
        </div>
        <div className="tile">
          <span>{t('stat.lag')}</span>
          <b>{now?.lag ?? 0}</b>
          <small>{t('metrics.messages')}</small>
        </div>
        <div className="tile">
          <span>{t('metrics.latency')}</span>
          <b>{(now?.avg_callback_ms ?? 0).toFixed(1)}</b>
          <small>ms</small>
        </div>
        <div className="tile">
          <span>{t('metrics.errorRate')}</span>
          <b>{errorRate.toFixed(1)}</b>
          <small>%</small>
        </div>
      </div>
      {samples.length < 2 ? (
        <div className="empty">{t('metrics.noData')}</div>
      ) : (
        <div className="charts">
          <section className="card chart-card wide">
            <h3>{t('metrics.throughput')}</h3>
            <ThroughputChart samples={samples} height={260} motion={motion} />
          </section>
          <section className="card chart-card">
            <h3>{t('metrics.lagTitle')}</h3>
            <TimeChart series={lag} unit={t('metrics.messages')} motion={motion} />
          </section>
          <section className="card chart-card">
            <h3>{t('metrics.latencyTitle')}</h3>
            <TimeChart series={latency} unit="ms" motion={motion} />
          </section>
        </div>
      )}
      <details className="small" style={{ marginTop: 16 }}>
        <summary className="muted" style={{ cursor: 'pointer' }}>
          {t('metrics.table')}
        </summary>
        <table className="list" style={{ marginTop: 8 }}>
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
        </table>
      </details>
    </div>
  )
}
