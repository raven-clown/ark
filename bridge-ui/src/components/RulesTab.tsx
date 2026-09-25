import { useEffect, useMemo, useState } from 'react'

import { api, ApiError, type ApplyOut } from '../api'
import { useT } from '../i18n'
import { FindingCard, Icon } from './Icon'

interface Rule {
  name: string
  condition: string
  action: string
  destination_override?: string
  webhook_override?: string
}

interface FieldRule {
  path: string
  required?: boolean
  type?: string
  min?: number
  max?: number
  pattern?: string
  enum?: string[]
  format?: string
}

interface DataRules {
  on_violation?: string
  allow_unknown_fields?: boolean
  max_bytes?: number
  fields?: FieldRule[]
}

interface Rules {
  fast_path_rules: Rule[] | null
  post_callback_rules: Rule[] | null
  data_rules: DataRules | null
}

interface Profile {
  path: string
  types: Record<string, number>
  examples?: string[]
}

type Stage = 'fast_path' | 'post_callback'
type Op = '==' | '!=' | '>' | '>=' | '<' | '<=' | 'contains' | 'exists' | 'missing'

interface Cond {
  field: string
  op: Op
  value: string
}

const ACTIONS = ['pass_through', 'reject', 'drop', 'dead_letter', 'transform_route']
const TYPES = ['', 'string', 'number', 'integer', 'boolean', 'object', 'array']
const FORMATS = ['', 'email', 'uuid', 'date-time', 'date', 'url', 'ipv4']

function literal(value: string, numeric: boolean) {
  const v = value.trim()
  if (v === 'true' || v === 'false' || v === 'nil') return v
  if (numeric && v !== '' && !Number.isNaN(Number(v))) return v
  return JSON.stringify(v)
}

// toExpr turns builder rows into an expr-lang condition.
function toExpr(conds: Cond[], join: '&&' | '||', numericFields: Set<string>) {
  return conds
    .filter((c) => c.field)
    .map((c) => {
      const f = c.field
      switch (c.op) {
        case 'exists':
          return `${f} != nil`
        case 'missing':
          return `${f} == nil`
        case 'contains':
          return `${f} contains ${JSON.stringify(c.value)}`
        default:
          return `${f} ${c.op} ${literal(c.value, numericFields.has(f))}`
      }
    })
    .join(` ${join} `)
}

export function RulesTab({ name, onChanged }: { name: string; onChanged: () => void }) {
  const t = useT()
  const enc = encodeURIComponent(name)
  const [rules, setRules] = useState<Rules | null>(null)
  const [fields, setFields] = useState<Profile[]>([])
  const [error, setError] = useState('')
  const [editing, setEditing] = useState<{ stage: Stage; index: number } | null>(null)
  const [pending, setPending] = useState<ApplyOut | null>(null)
  const [message, setMessage] = useState<{ text: string; error?: boolean } | null>(null)

  const load = async () => {
    try {
      setRules(await api<Rules>(`/pipelines/${enc}/rules`))
    } catch (e) {
      setError((e as Error).message)
    }
  }
  useEffect(() => {
    load()
    api<{ fields: Profile[] | null }>(`/pipelines/${enc}/data-check?sample=50`).then(
      (d) => setFields(d.fields ?? []),
      () => setFields([]),
    )
  }, [enc])

  if (error) return <p className="err">{error}</p>
  if (!rules) return <p className="muted">{t('common.loading')}</p>
  const fast = rules.fast_path_rules ?? []
  const post = rules.post_callback_rules ?? []
  const data: DataRules = rules.data_rules ?? {}

  const setList = (stage: Stage, list: Rule[]) => setRules({ ...rules, [stage === 'fast_path' ? 'fast_path_rules' : 'post_callback_rules']: list })
  const listOf = (stage: Stage) => (stage === 'fast_path' ? fast : post)

  const preview = async () => {
    setMessage(null)
    try {
      const out = await api<ApplyOut>(`/pipelines/${enc}/rules/preview`, { body: { fast_path_rules: fast, post_callback_rules: post, data_rules: data } })
      if (out.state === 'awaiting_confirmation') setPending(out)
      else if (out.state === 'unchanged') setMessage({ text: t('cfg.unchanged') })
      else setPending(out)
    } catch (e) {
      setMessage({ text: e instanceof ApiError && e.status === 403 ? t('common.noAccess') : (e as Error).message, error: true })
    }
  }
  const confirm = async () => {
    try {
      await api('/config/confirm', { body: { confirm_token: pending?.confirm_token } })
      setPending(null)
      setMessage({ text: t('cfg.applied') })
      onChanged()
      setTimeout(load, 1500)
    } catch (e) {
      setMessage({ text: (e as Error).message, error: true })
    }
  }

  const ruleList = (stage: Stage) => {
    const list = listOf(stage)
    return (
      <div className="stack" style={{ gap: 8 }}>
        <div className="row between">
          <div>
            <b>{stage === 'fast_path' ? t('rules.before') : t('rules.after')}</b>
            <div className="small dim">{stage === 'fast_path' ? t('rules.beforeHint') : t('rules.afterHint')}</div>
          </div>
          <button className="btn sm" onClick={() => setEditing({ stage, index: -1 })}>
            <Icon name="plus" className="" />
            {t('rules.add')}
          </button>
        </div>
        {list.length === 0 && <div className="small dim">{t('rules.none')}</div>}
        {list.map((r, i) => (
          <div key={i} className="rule-row">
            <span className="order">{i + 1}</span>
            <div style={{ minWidth: 0 }}>
              <div className="row" style={{ gap: 6 }}>
                <b>{r.name}</b>
                <span className={`pill ${r.action === 'dead_letter' ? 'down' : r.action === 'reject' ? 'degraded' : r.action === 'drop' ? '' : 'healthy'}`}>{r.action}</span>
                {r.destination_override && <span className="small dim mono">→ {r.destination_override}</span>}
                {r.webhook_override && <span className="small dim mono">→ {r.webhook_override}</span>}
              </div>
              <code className="small">{r.condition}</code>
            </div>
            <div className="row" style={{ flexWrap: 'nowrap', gap: 2 }}>
              <button className="btn ghost sm icon-btn" disabled={i === 0} title="↑" onClick={() => setList(stage, list.map((x, j) => (j === i - 1 ? list[i] : j === i ? list[i - 1] : x)))}>
                ↑
              </button>
              <button className="btn ghost sm" onClick={() => setEditing({ stage, index: i })}>
                {t('rules.edit')}
              </button>
              <button className="btn ghost sm icon-btn" title={t('proj.remove')} onClick={() => setList(stage, list.filter((_, j) => j !== i))}>
                <Icon name="trash" className="" />
              </button>
            </div>
          </div>
        ))}
      </div>
    )
  }

  const setData = (d: DataRules) => setRules({ ...rules, data_rules: d })
  const dfields = data.fields ?? []
  const setField = (i: number, f: FieldRule) => setData({ ...data, fields: dfields.map((x, j) => (j === i ? f : x)) })

  return (
    <div className="stack" style={{ gap: 20 }}>
      {editing ? (
        <RuleBuilder
          name={name}
          stage={editing.stage}
          fields={fields}
          initial={editing.index >= 0 ? listOf(editing.stage)[editing.index] : undefined}
          onCancel={() => setEditing(null)}
          onDone={(rule) => {
            const list = listOf(editing.stage)
            setList(editing.stage, editing.index >= 0 ? list.map((x, j) => (j === editing.index ? rule : x)) : [...list, rule])
            setEditing(null)
          }}
        />
      ) : (
        <>
          {ruleList('fast_path')}
          {ruleList('post_callback')}

          <div className="stack" style={{ gap: 10 }}>
            <div className="row between">
              <div>
                <b>{t('rules.data')}</b>
                <div className="small dim">{t('rules.dataHint')}</div>
              </div>
              <button
                className="btn sm"
                disabled={fields.length === 0}
                onClick={() => {
                  const have = new Set(dfields.map((f) => f.path))
                  const add = fields
                    .filter((f) => !have.has(f.path))
                    .map((f) => {
                      const type = Object.keys(f.types).sort((a, b) => f.types[b] - f.types[a])[0]
                      return { path: f.path, type: type === 'null' ? undefined : type }
                    })
                  setData({ on_violation: data.on_violation ?? 'tag', ...data, fields: [...dfields, ...add] })
                }}
              >
                {t('rules.suggest')}
              </button>
            </div>
            <div className="row">
              <label className="small muted">{t('rules.onViolation')}</label>
              <div className="seg">
                {['reject', 'dead_letter', 'tag'].map((v) => (
                  <button key={v} className={(data.on_violation ?? 'reject') === v ? 'active' : ''} onClick={() => setData({ ...data, on_violation: v })}>
                    {v}
                  </button>
                ))}
              </div>
              <label className="check small">
                <input type="checkbox" checked={data.allow_unknown_fields === false} onChange={(e) => setData({ ...data, allow_unknown_fields: e.target.checked ? false : undefined })} />
                {t('rules.strict')}
              </label>
            </div>
            {dfields.length > 0 && (
              <div className="table-wrap" style={{ background: 'var(--s2)' }}>
                <table className="list compact">
                  <thead>
                    <tr>
                      <th>{t('rules.field')}</th>
                      <th>{t('rules.required')}</th>
                      <th>{t('rules.type')}</th>
                      <th>min</th>
                      <th>max</th>
                      <th>{t('rules.format')}</th>
                      <th>{t('rules.allowed')}</th>
                      <th />
                    </tr>
                  </thead>
                  <tbody>
                    {dfields.map((f, i) => (
                      <tr key={i}>
                        <td>
                          <input className="input mono" value={f.path} onChange={(e) => setField(i, { ...f, path: e.target.value })} />
                        </td>
                        <td style={{ textAlign: 'center' }}>
                          <input type="checkbox" checked={!!f.required} onChange={(e) => setField(i, { ...f, required: e.target.checked || undefined })} />
                        </td>
                        <td>
                          <select className="select" value={f.type ?? ''} onChange={(e) => setField(i, { ...f, type: e.target.value || undefined })}>
                            {TYPES.map((x) => (
                              <option key={x} value={x}>
                                {x || '·'}
                              </option>
                            ))}
                          </select>
                        </td>
                        <td>
                          <input className="input" style={{ width: 70 }} value={f.min ?? ''} onChange={(e) => setField(i, { ...f, min: e.target.value === '' ? undefined : Number(e.target.value) })} />
                        </td>
                        <td>
                          <input className="input" style={{ width: 70 }} value={f.max ?? ''} onChange={(e) => setField(i, { ...f, max: e.target.value === '' ? undefined : Number(e.target.value) })} />
                        </td>
                        <td>
                          <select className="select" value={f.format ?? ''} onChange={(e) => setField(i, { ...f, format: e.target.value || undefined })}>
                            {FORMATS.map((x) => (
                              <option key={x} value={x}>
                                {x || '·'}
                              </option>
                            ))}
                          </select>
                        </td>
                        <td>
                          <input
                            className="input"
                            placeholder="a, b, c"
                            value={(f.enum ?? []).join(', ')}
                            onChange={(e) => setField(i, { ...f, enum: e.target.value.trim() ? e.target.value.split(',').map((x) => x.trim()).filter(Boolean) : undefined })}
                          />
                        </td>
                        <td>
                          <button className="btn ghost sm icon-btn" onClick={() => setData({ ...data, fields: dfields.filter((_, j) => j !== i) })}>
                            <Icon name="trash" className="" />
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
            <button className="btn sm" style={{ justifySelf: 'start' }} onClick={() => setData({ on_violation: data.on_violation ?? 'tag', ...data, fields: [...dfields, { path: '' }] })}>
              <Icon name="plus" className="" />
              {t('rules.addField')}
            </button>
          </div>

          <div className="row" style={{ borderTop: '1px solid var(--line)', paddingTop: 14 }}>
            <button className="btn" onClick={preview}>
              {t('cfg.preview')}
            </button>
            {pending?.state === 'awaiting_confirmation' && (
              <>
                <button className="btn primary" onClick={confirm}>
                  {t('cfg.confirm')}
                </button>
                <button className="btn ghost" onClick={() => setPending(null)}>
                  {t('cfg.cancel')}
                </button>
              </>
            )}
          </div>
          {message && <p className={message.error ? 'err' : 'ok'}>{message.text}</p>}
          {pending?.preview && (
            <div className="preview stack">
              {(pending.preview.errors ?? []).map((e, i) => (
                <div key={i} className="err small">
                  {e}
                </div>
              ))}
              {pending.preview.diff && pending.preview.diff.length > 0 && (
                <ul className="diff">
                  {pending.preview.diff.map((d, i) => (
                    <li key={i}>{d}</li>
                  ))}
                </ul>
              )}
              {(pending.preview.warnings ?? []).map((w, i) => (
                <FindingCard key={i} severity={w.severity} what={w.what} why={w.why} />
              ))}
            </div>
          )}
        </>
      )}
    </div>
  )
}

function RuleBuilder({ name, stage, fields, initial, onCancel, onDone }: { name: string; stage: Stage; fields: Profile[]; initial?: Rule; onCancel: () => void; onDone: (r: Rule) => void }) {
  const t = useT()
  const [rule, setRule] = useState<Rule>(initial ?? { name: '', condition: '', action: stage === 'fast_path' ? 'pass_through' : 'transform_route' })
  const [conds, setConds] = useState<Cond[]>([{ field: '', op: '==', value: '' }])
  const [join, setJoin] = useState<'&&' | '||'>('&&')
  const [manual, setManual] = useState(!!initial)
  const [test, setTest] = useState<{ sampled: number; matched: number; examples: string[]; error?: string; note?: string } | null>(null)

  const options = useMemo(() => {
    const base = fields.map((f) => ({ path: `data.${f.path}`, types: f.types, examples: f.examples ?? [] }))
    if (stage === 'post_callback') return [{ path: 'response.status', types: { integer: 1 }, examples: ['200'] }, ...base.map((b) => ({ ...b, path: b.path.replace(/^data\./, 'response.body.') })), ...base]
    return base
  }, [fields, stage])
  const numeric = useMemo(() => new Set(options.filter((o) => 'number' in o.types || 'integer' in o.types).map((o) => o.path)), [options])
  const generated = toExpr(conds, join, numeric)
  const condition = manual ? rule.condition : generated

  useEffect(() => {
    if (!condition.trim()) {
      setTest(null)
      return
    }
    const id = setTimeout(() => {
      api<typeof test>(`/pipelines/${encodeURIComponent(name)}/rules/test`, { body: { condition, stage, sample: 100 } }).then(setTest, (e) => setTest({ sampled: 0, matched: 0, examples: [], error: (e as Error).message }))
    }, 450)
    return () => clearTimeout(id)
  }, [condition, stage, name])

  const valid = rule.name.trim() !== '' && condition.trim() !== '' && !test?.error && (rule.action !== 'transform_route' || !!rule.destination_override || !!rule.webhook_override)

  return (
    <div className="stack builder" style={{ gap: 14 }}>
      <div className="row between">
        <b>{stage === 'fast_path' ? t('rules.before') : t('rules.after')}</b>
        <button className="btn ghost sm" onClick={onCancel}>
          {t('cfg.cancel')}
        </button>
      </div>
      <div className="field">
        <label>{t('cfg.name')}</label>
        <input className="input" value={rule.name} placeholder="auto-approve-small" onChange={(e) => setRule({ ...rule, name: e.target.value })} />
      </div>

      <div className="field">
        <div className="row between">
          <label>{t('rules.when')}</label>
          <div className="seg">
            <button className={!manual ? 'active' : ''} onClick={() => setManual(false)}>
              {t('rules.visual')}
            </button>
            <button
              className={manual ? 'active' : ''}
              onClick={() => {
                setRule({ ...rule, condition: rule.condition || generated })
                setManual(true)
              }}
            >
              {t('rules.expression')}
            </button>
          </div>
        </div>
        {manual ? (
          <textarea className="textarea" style={{ minHeight: 80 }} value={rule.condition} onChange={(e) => setRule({ ...rule, condition: e.target.value })} />
        ) : (
          <div className="stack" style={{ gap: 6 }}>
            {conds.map((c, i) => {
              const opt = options.find((o) => o.path === c.field)
              return (
                <div key={i} className="cond">
                  {i > 0 ? (
                    <button className="btn sm join" onClick={() => setJoin(join === '&&' ? '||' : '&&')}>
                      {join === '&&' ? t('rules.and') : t('rules.or')}
                    </button>
                  ) : (
                    <span className="join small dim">{t('rules.if')}</span>
                  )}
                  <select className="select" value={c.field} onChange={(e) => setConds(conds.map((x, j) => (j === i ? { ...x, field: e.target.value } : x)))}>
                    <option value="">{t('rules.pickField')}</option>
                    {options.map((o) => (
                      <option key={o.path} value={o.path}>
                        {o.path}
                      </option>
                    ))}
                  </select>
                  <select className="select" value={c.op} onChange={(e) => setConds(conds.map((x, j) => (j === i ? { ...x, op: e.target.value as Op } : x)))}>
                    {(['==', '!=', '>', '>=', '<', '<=', 'contains', 'exists', 'missing'] as Op[]).map((o) => (
                      <option key={o} value={o}>
                        {t(`op.${o}` as never) || o}
                      </option>
                    ))}
                  </select>
                  {c.op !== 'exists' && c.op !== 'missing' ? (
                    <input className="input" list={`ex-${i}`} placeholder={opt?.examples[0] ?? 'value'} value={c.value} onChange={(e) => setConds(conds.map((x, j) => (j === i ? { ...x, value: e.target.value } : x)))} />
                  ) : (
                    <span />
                  )}
                  <datalist id={`ex-${i}`}>
                    {opt?.examples.map((ex) => (
                      <option key={ex} value={ex} />
                    ))}
                  </datalist>
                  <button className="btn ghost sm icon-btn" disabled={conds.length === 1} onClick={() => setConds(conds.filter((_, j) => j !== i))}>
                    <Icon name="close" className="" />
                  </button>
                </div>
              )
            })}
            <button className="btn sm" style={{ justifySelf: 'start' }} onClick={() => setConds([...conds, { field: '', op: '==', value: '' }])}>
              <Icon name="plus" className="" />
              {t('rules.addCondition')}
            </button>
            {generated && <code className="small expr">{generated}</code>}
          </div>
        )}
      </div>

      <div className={`match ${test?.error ? 'bad' : ''}`}>
        {!test && <span className="dim small">{t('rules.previewHint')}</span>}
        {test?.error && <span className="err small">{test.error}</span>}
        {test && !test.error && (
          <>
            <div className="row between">
              <span>
                <b>{test.matched}</b> / {test.sampled} {t('rules.recentMatch')}
              </span>
              <div className="meter" style={{ width: 120 }}>
                <i style={{ width: `${test.sampled ? (test.matched / test.sampled) * 100 : 0}%` }} />
              </div>
            </div>
            {test.note && <div className="small dim">{test.note}</div>}
            {test.examples.map((ex, i) => (
              <code key={i} className="small ex">
                {ex}
              </code>
            ))}
          </>
        )}
      </div>

      <div className="field">
        <label>{t('rules.then')}</label>
        <div className="seg" style={{ flexWrap: 'wrap' }}>
          {ACTIONS.map((a) => (
            <button key={a} className={rule.action === a ? 'active' : ''} onClick={() => setRule({ ...rule, action: a })}>
              {t(`act.${a}` as never) || a}
            </button>
          ))}
        </div>
        <span className="small dim">{t(`act.${rule.action}Hint` as never)}</span>
        {rule.action === 'transform_route' && (
          <div className="grid2">
            <input className="input mono" placeholder={t('rules.toTopic')} value={rule.destination_override ?? ''} onChange={(e) => setRule({ ...rule, destination_override: e.target.value || undefined, webhook_override: undefined })} />
            <input className="input mono" placeholder={t('rules.toWebhook')} value={rule.webhook_override ?? ''} onChange={(e) => setRule({ ...rule, webhook_override: e.target.value || undefined, destination_override: undefined })} />
          </div>
        )}
      </div>

      <div className="row">
        <button className="btn primary" disabled={!valid} onClick={() => onDone({ ...rule, condition })}>
          {t('rules.done')}
        </button>
        <span className="small dim">{t('rules.doneHint')}</span>
      </div>
    </div>
  )
}
