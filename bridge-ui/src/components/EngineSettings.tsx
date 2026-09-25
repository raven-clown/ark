import { useEffect, useState } from 'react'

import { api, ApiError } from '../api'
import { useT } from '../i18n'
import type { Model } from './ProjectsView'

interface Knob {
  name: string
  value: number
  default: number
  unit: string
  restart: boolean
}

interface SettingsOut {
  settings: { timezone: string; assistant_model?: Model; tuning: Record<string, number> }
  knobs: Knob[]
  mode: string
  editable: boolean
}

// The tuning struct's JSON keys are its Go field names; map the yaml names
// the knobs use onto them.
const FIELD: Record<string, string> = {
  retry_backoff_cap_seconds: 'RetryBackoffCapSeconds',
  retry_after_cap_seconds: 'RetryAfterCapSeconds',
  final_commit_timeout_seconds: 'FinalCommitTimeoutSeconds',
  producer_batch_timeout_ms: 'ProducerBatchTimeoutMs',
  dlq_browser_entries: 'DLQBrowserEntries',
  dlq_prune_minutes: 'DLQPruneMinutes',
  redrive_check_seconds: 'RedriveCheckSeconds',
  diagnosis_stuck_seconds: 'DiagnosisStuckSeconds',
  diagnosis_stalled_seconds: 'DiagnosisStalledSeconds',
  diagnosis_window_minutes: 'DiagnosisWindowMinutes',
  history_sample_seconds: 'HistorySampleSeconds',
  history_keep_minutes: 'HistoryKeepMinutes',
  event_log_entries: 'EventLogEntries',
  tail_value_bytes: 'TailValueBytes',
  confirm_token_minutes: 'ConfirmTokenMinutes',
  assistant_session_minutes: 'AssistantSessionMinutes',
  cluster_catch_up_seconds: 'ClusterCatchUpSeconds',
  compacted_idle_seconds: 'CompactedIdleSeconds',
}

export function EngineSettings({ toast }: { toast: (m: string, e?: boolean) => void }) {
  const t = useT()
  const [data, setData] = useState<SettingsOut | null>(null)
  const [tz, setTz] = useState('')
  const [model, setModel] = useState<Model | undefined>()
  const [values, setValues] = useState<Record<string, number>>({})
  const [error, setError] = useState('')

  const load = () =>
    api<SettingsOut>('/config/settings').then(
      (d) => {
        setData(d)
        setTz(d.settings.timezone)
        setModel(d.settings.assistant_model)
        setValues(Object.fromEntries(d.knobs.map((k) => [k.name, k.value])))
      },
      (e) => setError((e as Error).message),
    )
  useEffect(() => {
    load()
  }, [])

  if (error) return <p className="err">{error}</p>
  if (!data) return <p className="muted">{t('common.loading')}</p>

  const save = async () => {
    const tuning: Record<string, number> = {}
    for (const [k, v] of Object.entries(values)) if (v > 0) tuning[FIELD[k]] = v
    try {
      const out = await api<{ restart_needed: boolean }>('/config/settings', { method: 'PUT', body: { timezone: tz, assistant_model: model?.provider ? model : undefined, tuning } })
      toast(out.restart_needed ? t('eng.savedRestart') : t('cfg.applied'))
      load()
    } catch (e) {
      toast(e instanceof ApiError && e.status === 403 ? t('common.noAccess') : (e as Error).message, true)
    }
  }

  const ro = !data.editable
  return (
    <div className="card stack" style={{ gap: 18, maxWidth: 820 }}>
      <div>
        <b>{t('eng.title')}</b>
        <div className="small dim">{ro ? t('eng.clusterNote') : t('eng.fileNote')}</div>
      </div>
      <div className="grid2">
        <div className="field">
          <label>{t('eng.timezone')}</label>
          <input className="input mono" disabled={ro} value={tz} onChange={(e) => setTz(e.target.value)} placeholder="Asia/Bangkok" list="tz-list" />
          <datalist id="tz-list">
            {['UTC', 'Asia/Bangkok', 'Asia/Singapore', 'Asia/Shanghai', 'Asia/Taipei', 'Asia/Tokyo', 'Europe/London', 'Europe/Berlin', 'America/New_York', 'America/Los_Angeles'].map((z) => (
              <option key={z} value={z} />
            ))}
          </datalist>
          <span className="small dim">{t('eng.tzHint')}</span>
        </div>
        <div className="field">
          <label>{t('eng.defaultModel')}</label>
          <div className="row">
            <select className="select" style={{ width: 200 }} disabled={ro} value={model?.provider ?? ''} onChange={(e) => setModel(e.target.value ? { provider: e.target.value, model: model?.model ?? '', base_url: model?.base_url, api_key_env: model?.api_key_env } : undefined)}>
              <option value="">{t('proj.noModel')}</option>
              <option value="anthropic">Anthropic</option>
              <option value="openai">OpenAI</option>
              <option value="gemini">Google Gemini</option>
              <option value="openai_compatible">OpenAI-compatible</option>
            </select>
            {model && <input className="input" style={{ width: 170 }} disabled={ro} placeholder={t('proj.modelName')} value={model.model} onChange={(e) => setModel({ ...model, model: e.target.value })} />}
          </div>
          {model && (
            <div className="row">
              <input className="input" style={{ width: 200 }} disabled={ro} placeholder={t('proj.baseUrl')} value={model.base_url ?? ''} onChange={(e) => setModel({ ...model, base_url: e.target.value || undefined })} />
              <input className="input mono" style={{ width: 170 }} disabled={ro} placeholder={t('proj.keyEnv')} value={model.api_key_env ?? ''} onChange={(e) => setModel({ ...model, api_key_env: e.target.value.toUpperCase() || undefined })} />
            </div>
          )}
        </div>
      </div>
      <div className="field">
        <label>{t('eng.tuning')}</label>
        <div className="table-wrap" style={{ background: 'var(--s2)' }}>
          <table className="list compact">
            <tbody>
              {data.knobs.map((k) => (
                <tr key={k.name}>
                  <td>
                    <div>{t(`knob.${k.name}` as never) || k.name}</div>
                    <div className="small dim mono">{k.name}</div>
                  </td>
                  <td style={{ width: 150 }}>
                    <input
                      className="input"
                      type="number"
                      min={0}
                      disabled={ro}
                      placeholder={`${k.default}`}
                      value={values[k.name] || ''}
                      onChange={(e) => setValues({ ...values, [k.name]: Number(e.target.value) || 0 })}
                    />
                  </td>
                  <td className="small dim" style={{ width: 60 }}>
                    {k.unit}
                  </td>
                  <td className="small dim" style={{ width: 130 }}>
                    {k.restart ? t('eng.restart') : t('eng.live')}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <span className="small dim">{t('eng.defaultHint')}</span>
      </div>
      {!ro && (
        <div>
          <button className="btn primary" onClick={save}>
            {t('proj.save')}
          </button>
        </div>
      )}
    </div>
  )
}
