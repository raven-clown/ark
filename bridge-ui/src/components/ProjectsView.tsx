import { useCallback, useEffect, useState } from 'react'

import { api, ApiError } from '../api'
import { useT } from '../i18n'
import { Icon } from './Icon'

export type AIAccess = 'none' | 'read_only' | 'operate' | 'configure'

export interface Model {
  provider: string
  model: string
  base_url?: string
  api_key_env?: string
  max_steps?: number
}

export interface Endpoint {
  name: string
  access: AIAccess
  tokens_env: string
  tools?: string[]
  path?: string
  effective_access?: AIAccess
  tokens_configured?: boolean
}

export interface Project {
  name: string
  description?: string
  ai_access: AIAccess
  mcp_endpoints?: Endpoint[]
  assistant?: Model
  pipelines?: string[] | null
  endpoints?: Endpoint[]
  assistant_key_configured?: boolean
  effective_assistant?: Model
}

export interface ProjectsOut {
  projects: Project[]
  unassigned: string[] | null
  default_assistant?: Model
  default_assistant_key: boolean
  tools: string[]
  applies_to: string
}

const ACCESS: AIAccess[] = ['none', 'read_only', 'operate', 'configure']

export function useProjects(every = 0) {
  const [data, setData] = useState<ProjectsOut | null>(null)
  const [error, setError] = useState('')
  const load = useCallback(async () => {
    try {
      setData(await api<ProjectsOut>('/projects'))
      setError('')
    } catch (e) {
      setError((e as Error).message)
    }
  }, [])
  useEffect(() => {
    load()
    if (!every) return
    const id = setInterval(load, every)
    return () => clearInterval(id)
  }, [load, every])
  return { data, error, reload: load }
}

function envName(project: string, endpoint: string) {
  return `ARK_MCP_${project}_${endpoint}_TOKENS`.toUpperCase().replace(/[^A-Z0-9_]/g, '_')
}

// strip removes the read-only fields the API adds, leaving what it accepts.
function strip(p: Project): Project {
  return {
    name: p.name,
    description: p.description,
    ai_access: p.ai_access,
    mcp_endpoints: (p.mcp_endpoints ?? []).map((e) => ({ name: e.name, access: e.access, tokens_env: e.tokens_env, tools: e.tools?.length ? e.tools : undefined })),
    assistant: p.assistant,
  }
}

export function ProjectsView({ toast }: { toast: (m: string, e?: boolean) => void }) {
  const t = useT()
  const { data, error, reload } = useProjects()
  const [creating, setCreating] = useState('')

  const save = async (p: Project) => {
    try {
      await api(`/config/projects/${encodeURIComponent(p.name)}`, { method: 'PUT', body: strip(p) })
      toast(t('cfg.applied'))
      reload()
    } catch (e) {
      toast(e instanceof ApiError && e.status === 403 ? t('common.noAccess') : (e as Error).message, true)
    }
  }
  const remove = async (name: string) => {
    try {
      await api(`/config/projects/${encodeURIComponent(name)}`, { method: 'DELETE' })
      toast(t('act.done'))
      reload()
    } catch (e) {
      toast(e instanceof ApiError && e.status === 403 ? t('common.noAccess') : (e as Error).message, true)
    }
  }

  return (
    <div className="page">
      <div className="page-head">
        <div>
          <h1>{t('nav.projects')}</h1>
          <p className="lead">{t('proj.lead')}</p>
        </div>
        <div className="row">
          <input className="input" style={{ width: 200 }} placeholder={t('proj.newName')} value={creating} onChange={(e) => setCreating(e.target.value.toLowerCase())} />
          <button className="btn primary" disabled={!/^[a-z0-9][a-z0-9_-]*$/.test(creating)} onClick={() => save({ name: creating, ai_access: 'read_only' }).then(() => setCreating(''))}>
            <Icon name="plus" className="" />
            {t('proj.create')}
          </button>
        </div>
      </div>
      {error && <p className="err">{error}</p>}
      {data && data.projects.length === 0 && (
        <div className="empty card">
          <Icon name="topics" className="" />
          {t('proj.empty')}
        </div>
      )}
      <div className="stack" style={{ gap: 16 }}>
        {data?.projects.map((p) => (
          <ProjectCard key={p.name} p={p} tools={data.tools} defaultModel={data.default_assistant} onSave={save} onDelete={() => remove(p.name)} />
        ))}
      </div>
      {data && (data.unassigned ?? []).length > 0 && (
        <p className="small dim" style={{ marginTop: 16 }}>
          {t('proj.unassigned')}: {(data.unassigned ?? []).join(', ')}. {t('proj.assignHint')}
        </p>
      )}
      {data && <p className="small dim">{t('cfg.appliesTo')}: {data.applies_to}</p>}
    </div>
  )
}

function ProjectCard({ p, tools, defaultModel, onSave, onDelete }: { p: Project; tools: string[]; defaultModel?: Model; onSave: (p: Project) => Promise<void>; onDelete: () => void }) {
  const t = useT()
  const [draft, setDraft] = useState<Project>(p)
  const [newEp, setNewEp] = useState('')
  const [openTools, setOpenTools] = useState<string | null>(null)
  const [sure, setSure] = useState(false)
  useEffect(() => setDraft(p), [p])
  const dirty = JSON.stringify(strip(draft)) !== JSON.stringify(strip(p))
  const eps = draft.mcp_endpoints ?? []
  const setEp = (i: number, e: Endpoint) => setDraft({ ...draft, mcp_endpoints: eps.map((x, j) => (j === i ? e : x)) })
  const live = new Map((p.endpoints ?? []).map((e) => [e.name, e]))
  const model = draft.assistant

  return (
    <section className="card stack" style={{ gap: 18 }}>
      <div className="row between">
        <div>
          <h3 style={{ margin: 0, fontSize: 16, fontWeight: 600 }}>{p.name}</h3>
          <input className="input" style={{ marginTop: 6, width: 420, maxWidth: '100%' }} placeholder={t('proj.description')} value={draft.description ?? ''} onChange={(e) => setDraft({ ...draft, description: e.target.value })} />
        </div>
        <div className="row">
          {dirty && (
            <button className="btn ghost" onClick={() => setDraft(p)}>
              {t('cfg.cancel')}
            </button>
          )}
          <button className="btn primary" disabled={!dirty} onClick={() => onSave(draft)}>
            {t('proj.save')}
          </button>
        </div>
      </div>

      <div className="grid2">
        <div className="field">
          <label>{t('proj.aiAccess')}</label>
          <div className="seg">
            {ACCESS.map((a) => (
              <button key={a} className={draft.ai_access === a ? 'active' : ''} onClick={() => setDraft({ ...draft, ai_access: a })}>
                {t(`access.${a}`)}
              </button>
            ))}
          </div>
          <span className="small dim">{t(`access.${draft.ai_access}Hint`)}</span>
        </div>
        <div className="field">
          <label>{t('nav.pipelines')}</label>
          <div className="row">
            {(p.pipelines ?? []).length === 0 && <span className="small dim">{t('proj.noPipelines')}</span>}
            {(p.pipelines ?? []).map((name) => (
              <span key={name} className="pill">
                {name}
              </span>
            ))}
          </div>
        </div>
      </div>

      <div className="field">
        <label>{t('proj.endpoints')}</label>
        {eps.length > 0 && (
          <div className="table-wrap" style={{ background: 'var(--s2)' }}>
            <table className="list">
              <thead>
                <tr>
                  <th>{t('cfg.name')}</th>
                  <th>{t('proj.access')}</th>
                  <th>{t('proj.tokens')}</th>
                  <th>{t('proj.tools')}</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {eps.map((e, i) => {
                  const l = live.get(e.name)
                  return (
                    <tr key={i}>
                      <td>
                        <div>{e.name}</div>
                        <div className="small dim mono">/mcp/{p.name}/{e.name}</div>
                      </td>
                      <td>
                        <select className="select" style={{ width: 130 }} value={e.access} onChange={(ev) => setEp(i, { ...e, access: ev.target.value as AIAccess })}>
                          {ACCESS.filter((a) => a !== 'none').map((a) => (
                            <option key={a} value={a}>
                              {t(`access.${a}`)}
                            </option>
                          ))}
                        </select>
                        {l?.effective_access && l.effective_access !== e.access && <div className="small warn">→ {t(`access.${l.effective_access}`)}</div>}
                      </td>
                      <td>
                        <div className="mono small">{e.tokens_env}</div>
                        {l && <span className={`small ${l.tokens_configured ? 'ok' : 'warn'}`}>{l.tokens_configured ? t('proj.tokensSet') : t('proj.tokensMissing')}</span>}
                      </td>
                      <td>
                        <button className="btn sm" onClick={() => setOpenTools(openTools === e.name ? null : e.name)}>
                          {e.tools?.length ? `${e.tools.length} ${t('proj.tools').toLowerCase()}` : t('proj.allTools')}
                        </button>
                        {openTools === e.name && (
                          <div className="stack" style={{ gap: 4, marginTop: 8, maxHeight: 220, overflow: 'auto' }}>
                            {tools.map((tool) => (
                              <label key={tool} className="check small">
                                <input
                                  type="checkbox"
                                  checked={(e.tools ?? []).includes(tool)}
                                  onChange={(ev) => {
                                    const set = new Set(e.tools ?? [])
                                    if (ev.target.checked) set.add(tool)
                                    else set.delete(tool)
                                    setEp(i, { ...e, tools: [...set] })
                                  }}
                                />
                                <span className="mono">{tool}</span>
                              </label>
                            ))}
                          </div>
                        )}
                      </td>
                      <td style={{ textAlign: 'right' }}>
                        <button className="btn ghost sm icon-btn" title={t('proj.remove')} onClick={() => setDraft({ ...draft, mcp_endpoints: eps.filter((_, j) => j !== i) })}>
                          <Icon name="trash" className="" />
                        </button>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
        <div className="row">
          <input className="input" style={{ width: 200 }} placeholder={t('proj.endpointName')} value={newEp} onChange={(e) => setNewEp(e.target.value.toLowerCase())} />
          <button
            className="btn"
            disabled={!/^[a-z0-9][a-z0-9_-]*$/.test(newEp) || eps.some((e) => e.name === newEp)}
            onClick={() => {
              setDraft({ ...draft, mcp_endpoints: [...eps, { name: newEp, access: 'read_only', tokens_env: envName(p.name, newEp) }] })
              setNewEp('')
            }}
          >
            <Icon name="plus" className="" />
            {t('proj.addEndpoint')}
          </button>
        </div>
      </div>

      <div className="field">
        <label>{t('proj.model')}</label>
        <div className="row" style={{ alignItems: 'flex-start' }}>
          <select
            className="select"
            style={{ width: 190 }}
            value={model?.provider ?? ''}
            onChange={(e) => setDraft({ ...draft, assistant: e.target.value ? { provider: e.target.value, model: model?.model ?? '', base_url: model?.base_url, api_key_env: model?.api_key_env } : undefined })}
          >
            <option value="">{defaultModel ? `${t('proj.useDefault')} (${defaultModel.provider}/${defaultModel.model})` : t('proj.noModel')}</option>
            <option value="anthropic">Anthropic</option>
            <option value="openai">OpenAI</option>
            <option value="gemini">Google Gemini</option>
            <option value="openai_compatible">{t('proj.compatible')}</option>
          </select>
          {model && (
            <>
              <input className="input" style={{ width: 180 }} placeholder={t('proj.modelName')} value={model.model} onChange={(e) => setDraft({ ...draft, assistant: { ...model, model: e.target.value } })} />
              <input className="input" style={{ width: 220 }} placeholder={model.provider === 'openai_compatible' ? 'http://localhost:11434/v1' : t('proj.baseUrl')} value={model.base_url ?? ''} onChange={(e) => setDraft({ ...draft, assistant: { ...model, base_url: e.target.value || undefined } })} />
              <input className="input mono" style={{ width: 200 }} placeholder={t('proj.keyEnv')} value={model.api_key_env ?? ''} onChange={(e) => setDraft({ ...draft, assistant: { ...model, api_key_env: e.target.value.toUpperCase() || undefined } })} />
            </>
          )}
        </div>
        <span className={`small ${p.assistant_key_configured ? 'ok' : 'dim'}`}>
          {p.effective_assistant ? `${p.effective_assistant.provider}/${p.effective_assistant.model} · ${p.assistant_key_configured ? t('proj.keyReady') : t('proj.keyMissing')}` : t('proj.noModel')}
        </span>
      </div>

      <div className="row between" style={{ borderTop: '1px solid var(--line)', paddingTop: 14 }}>
        <span className="small dim">{t('proj.deleteHint')}</span>
        {!sure ? (
          <button className="btn danger sm" onClick={() => setSure(true)}>
            {t('proj.delete')}
          </button>
        ) : (
          <span className="row">
            <button className="btn ghost sm" onClick={() => setSure(false)}>
              {t('common.no')}
            </button>
            <button className="btn danger sm" onClick={onDelete}>
              {t('common.sure')} {t('common.yes')}
            </button>
          </span>
        )}
      </div>
    </section>
  )
}
