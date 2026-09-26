import { useCallback, useEffect, useState } from 'react'

import { api, type ArkEvent } from '../api'
import { LANGS, useT, type Lang } from '../i18n'
import { ConfigEditor } from './ConfigEditor'
import { EngineSettings } from './EngineSettings'
import { Icon } from './Icon'

function useLoad<T>(path: string, every = 0) {
  const [data, setData] = useState<T | null>(null)
  const [error, setError] = useState('')
  const load = useCallback(async () => {
    if (!path) return
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

const kindTone: Record<string, string> = {
  breaker_opened: 'err',
  pipeline_start_failed: 'err',
  message_dead_lettered: 'err',
  worker_restarted: 'warn',
  message_rejected: 'warn',
  data_rule_violation: 'warn',
  target_rate_limited: 'warn',
  paused: 'warn',
  breaker_closed: 'ok',
  resumed: 'ok',
  pipeline_started: 'ok',
}

export function EventsView({ pipelines }: { pipelines: string[] }) {
  const t = useT()
  const [pipeline, setPipeline] = useState('')
  const q = new URLSearchParams({ limit: '200' })
  if (pipeline) q.set('pipeline', pipeline)
  const { data, error } = useLoad<{ events: ArkEvent[] | null; note: string }>(`/events?${q}`, 4000)
  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>{t('nav.events')}</h1>
          <p className="lead">{t('events.title')}</p>
        </div>
        <select className="select" style={{ width: 220 }} value={pipeline} onChange={(e) => setPipeline(e.target.value)}>
          <option value="">{t('events.all')}</option>
          {pipelines.map((p) => (
            <option key={p}>{p}</option>
          ))}
        </select>
      </div>
      {error && <p className="err">{error}</p>}
      {data && (data.events ?? []).length === 0 && (
        <div className="empty">
          <Icon name="events" className="" />
          {t('events.empty')}
        </div>
      )}
      {data && (data.events ?? []).length > 0 && (
        <div className="table-wrap">
        <table className="list">
          <tbody>
            {(data.events ?? []).map((e, i) => (
              <tr key={i}>
                <td className="mono">{e.time}</td>
                <td className="mono">{e.pipeline}</td>
                <td>
                  <span className={`tag ${kindTone[e.kind] === 'err' ? 'dlq' : kindTone[e.kind] === 'warn' ? 'reject' : kindTone[e.kind] === 'ok' ? 'destination' : ''}`}>{e.kind.replace(/_/g, ' ')}</span>
                </td>
                <td>
                  {e.message}
                  {e.details && (
                    <div className="small dim mono">
                      {Object.entries(e.details)
                        .filter(([, v]) => v)
                        .map(([k, v]) => `${k}=${v}`)
                        .join(' · ')}
                    </div>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        </div>
      )}
      {data && <p className="small dim">{data.note}</p>}
    </div>
  )
}

interface ClusterOut {
  enabled: boolean
  cluster?: string
  node_id?: string
  leader?: boolean
  live_nodes?: string[]
  config_version?: number
  node_config_versions?: Record<string, number>
  leader_node?: string
  node_labels?: Record<string, Record<string, string>>
}

type ClusterPipelines = Record<string, { nodes: Record<string, { workers: number }> }>

export function ClusterView() {
  const t = useT()
  const { data, error } = useLoad<ClusterOut>('/cluster', 4000)
  const placed = useLoad<ClusterPipelines>(data?.enabled ? '/cluster/pipelines' : '', 4000)
  const workersOn = (node: string) =>
    Object.entries(placed.data ?? {})
      .map(([name, p]) => [name, p.nodes?.[node]?.workers ?? 0] as const)
      .filter(([, w]) => w > 0)
  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>{t('nav.cluster')}</h1>
          {data?.enabled && (
            <p className="lead">
              {data.cluster} · {t('cluster.version')} {data.config_version}
            </p>
          )}
        </div>
      </div>
      {error && <p className="err">{error}</p>}
      {data && !data.enabled && (
        <div className="empty card">
          <Icon name="cluster" className="" />
          {t('cluster.off')}
        </div>
      )}
      {data?.enabled && (
        <>
          <div className="table-wrap">
          <table className="list">
            <thead>
              <tr>
                <th>{t('cluster.nodes')}</th>
                <th>{t('cluster.labels')}</th>
                <th>{t('cluster.workers')}</th>
                <th>{t('cluster.version')}</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {(data.live_nodes ?? []).map((n) => (
                <tr key={n}>
                  <td className="mono">{n}</td>
                  <td>
                    <span className="row">
                      {Object.entries(data.node_labels?.[n] ?? {}).map(([k, val]) => (
                        <span key={k} className="pill mono">
                          {k}={val}
                        </span>
                      ))}
                    </span>
                  </td>
                  <td className="mono">
                    {workersOn(n).map(([name, w]) => (
                      <div key={name}>
                        {name} × {w}
                      </div>
                    ))}
                  </td>
                  <td className="mono">{data.node_config_versions?.[n] ?? ''}</td>
                  <td>
                    <span className="row">
                      {n === data.node_id && <span className="pill">{t('cluster.thisNode')}</span>}
                      {n === data.leader_node && <span className="pill healthy">{t('cluster.leader')}</span>}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          </div>
        </>
      )}
    </div>
  )
}

export function TopicsView() {
  const t = useT()
  const { data, error } = useLoad<{ name: string; partitions: number; used_by?: string[] }[]>('/topics', 10000)
  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>{t('nav.topics')}</h1>
        </div>
      </div>
      {error && <p className="err">{error}</p>}
      {data && (
        <div className="table-wrap">
        <table className="list">
          <thead>
            <tr>
              <th>Topic</th>
              <th>{t('topics.partitions')}</th>
              <th>{t('topics.usedBy')}</th>
            </tr>
          </thead>
          <tbody>
            {data.map((tp) => (
              <tr key={tp.name}>
                <td className="mono" style={{ color: 'var(--frost)' }}>
                  {tp.name}
                </td>
                <td className="mono">{tp.partitions}</td>
                <td className="muted">{(tp.used_by ?? []).join(', ')}</td>
              </tr>
            ))}
          </tbody>
        </table>
        </div>
      )}
    </div>
  )
}

export function SettingsView(props: { lang: Lang; setLang: (l: Lang) => void; motion: boolean; setMotion: (m: boolean) => void; timezone: string; onSignOut: () => void; toast: (m: string, e?: boolean) => void }) {
  const t = useT()
  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>{t('nav.settings')}</h1>
        </div>
      </div>
      <div className="card stack" style={{ maxWidth: 520, gap: 18 }}>
        <div className="field">
          <label>{t('settings.lang')}</label>
          <select className="select" value={props.lang} onChange={(e) => props.setLang(e.target.value as Lang)}>
            {LANGS.map((l) => (
              <option key={l.id} value={l.id}>
                {l.label}
              </option>
            ))}
          </select>
        </div>
        <label className="check">
          <input type="checkbox" checked={props.motion} onChange={(e) => props.setMotion(e.target.checked)} />
          {t('settings.motion')}
        </label>
        <div className="field">
          <label>{t('settings.tz')}</label>
          <div className="mono">{props.timezone || 'UTC'}</div>
        </div>
        <div>
          <button className="btn danger" onClick={props.onSignOut}>
            <Icon name="signout" className="" />
            {t('signout')}
          </button>
        </div>
      </div>
      <div style={{ marginTop: 16 }}>
        <EngineSettings toast={props.toast} />
      </div>
    </div>
  )
}

export function NewPipelineModal({ onClose, onApplied, onDesigner }: { onClose: () => void; onApplied: () => void; onDesigner: () => void }) {
  const t = useT()
  const [f, setF] = useState({ name: '', source: '', target: '', destination: '', dlq: '', reject: '', group: '' })
  const [yaml, setYaml] = useState<string | null>(null)
  const set = (k: keyof typeof f) => (e: React.ChangeEvent<HTMLInputElement>) => setF({ ...f, [k]: e.target.value })
  const name = f.name.trim()
  const build = () =>
    [
      `name: ${name}`,
      `source_topic: ${f.source || `${name}.in`}`,
      `destination_topic: ${f.destination || `${name}.out`}`,
      `dead_letter_topic: ${f.dlq || `${name}.dlq`}`,
      f.reject ? `reject_topic: ${f.reject}` : '',
      `consumer_group: ${f.group || `ark-${name}`}`,
      'workers: 1',
      'target:',
      `  url: ${f.target || 'http://your-app:8080/process'}`,
      'retry:',
      '  max_attempts: 3',
      '  backoff_ms: 1000',
      '',
    ]
      .filter((l) => l !== '')
      .join('\n') + '\n'
  return (
    <div className="scrim" onClick={onClose}>
      <div className="modal stack" onClick={(e) => e.stopPropagation()}>
        <div>
          <h2>{t('cfg.newTitle')}</h2>
          <p className="muted" style={{ margin: 0 }}>
            {t('cfg.newHint')}
          </p>
        </div>
        {yaml === null ? (
          <>
            <div className="grid2">
              <div className="field">
                <label>{t('cfg.name')}</label>
                <input className="input" value={f.name} onChange={set('name')} placeholder="orders" autoFocus />
              </div>
              <div className="field">
                <label>{t('cfg.target')}</label>
                <input className="input" value={f.target} onChange={set('target')} placeholder="http://app:8080/process" />
              </div>
              <div className="field">
                <label>{t('cfg.source')}</label>
                <input className="input" value={f.source} onChange={set('source')} placeholder={name ? `${name}.in` : 'orders.raw'} />
              </div>
              <div className="field">
                <label>{t('cfg.destination')}</label>
                <input className="input" value={f.destination} onChange={set('destination')} placeholder={name ? `${name}.out` : 'orders.processed'} />
              </div>
              <div className="field">
                <label>{t('cfg.dlq')}</label>
                <input className="input" value={f.dlq} onChange={set('dlq')} placeholder={name ? `${name}.dlq` : 'orders.dlq'} />
              </div>
              <div className="field">
                <label>{t('cfg.reject')}</label>
                <input className="input" value={f.reject} onChange={set('reject')} placeholder="orders.rejected" />
              </div>
            </div>
            <div className="row" style={{ justifyContent: 'flex-end' }}>
              <button className="btn ghost" style={{ marginRight: 'auto' }} onClick={onDesigner}>
                {t('dz.designer')}
              </button>
              <button className="btn ghost" onClick={onClose}>
                {t('cfg.cancel')}
              </button>
              <button className="btn primary" disabled={!/^[a-z0-9][a-z0-9._-]*$/i.test(name)} onClick={() => setYaml(build())}>
                {t('cfg.toYaml')}
              </button>
            </div>
          </>
        ) : (
          <ConfigEditor
            initial={yaml}
            onApplied={() => {
              onApplied()
              setTimeout(onClose, 900)
            }}
          />
        )}
      </div>
    </div>
  )
}
