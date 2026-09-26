import { useEffect, useMemo, useState } from 'react'
import {
  Background,
  Handle,
  Position,
  ReactFlow,
  ReactFlowProvider,
  applyNodeChanges,
  useReactFlow,
  type Connection,
  type Edge,
  type Node,
  type NodeChange,
  type NodeProps,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'

import { useT, type Key } from '../i18n'
import { ConfigEditor } from './ConfigEditor'
import { Icon, type IconName } from './Icon'
import { useProjects } from './ProjectsView'

// Fields is a pipeline, or part of one, as its config file keys.
type Fields = Record<string, unknown>

export type StepType = 'call' | 'condition' | 'data_check' | 'topic' | 'webhook' | 'reject' | 'dead_letter' | 'drop'

interface BranchCfg {
  name?: string
  when: string
  next: string[]
}

export interface Step {
  id: string
  type: StepType
  name?: string
  next?: string[]
  on_reject?: string[]
  on_failure?: string[]
  on_fail?: string[]
  otherwise?: string[]
  branches?: BranchCfg[]
  match?: string
  target?: Fields
  retry?: Fields
  circuit_breaker?: Fields
  rules?: Fields
  topic?: string
  url?: string
  reason?: string
}

interface FlowCfg {
  start: string[]
  steps: Step[]
}

const SOURCE = '__source'
const MIME = 'application/x-ark-step'

const TYPES: { type: StepType; icon: IconName }[] = [
  { type: 'condition', icon: 'spark' },
  { type: 'call', icon: 'globe' },
  { type: 'webhook', icon: 'signout' },
  { type: 'topic', icon: 'stream' },
  { type: 'data_check', icon: 'check' },
  { type: 'reject', icon: 'inbox' },
  { type: 'dead_letter', icon: 'alert' },
  { type: 'drop', icon: 'trash' },
]
const iconOf = (t: StepType) => TYPES.find((x) => x.type === t)!.icon
const TERMINAL: StepType[] = ['drop']

const obj = (v: unknown): Fields => (v && typeof v === 'object' && !Array.isArray(v) ? (v as Fields) : {})
const list = (v: unknown): Fields[] => (Array.isArray(v) ? v.map(obj) : [])
const str = (v: unknown) => (v === undefined || v === null ? '' : String(v))
const num = (s: unknown, def: number) => (/^\d+$/.test(str(s).trim()) ? Number(s) : def)

// handlesOf lists a step's ways out: the key in the step that holds where
// each leads, and the label drawn next to it.
function handlesOf(s: Step): { key: string; label: string }[] {
  switch (s.type) {
    case 'call':
      return [
        { key: 'next', label: 'fz.h.ok' },
        { key: 'on_reject', label: 'fz.h.reject' },
        { key: 'on_failure', label: 'fz.h.failed' },
      ]
    case 'condition':
      return [...(s.branches ?? []).map((b, i) => ({ key: `b${i}`, label: b.name || b.when || `#${i + 1}` })), { key: 'otherwise', label: 'fz.h.otherwise' }]
    case 'data_check':
      return [
        { key: 'next', label: 'fz.h.pass' },
        { key: 'on_fail', label: 'fz.h.fail' },
      ]
    case 'webhook':
      return [
        { key: 'next', label: 'fz.h.then' },
        { key: 'on_failure', label: 'fz.h.failed' },
      ]
    case 'topic':
    case 'reject':
    case 'dead_letter':
      return [{ key: 'next', label: 'fz.h.then' }]
    default:
      return []
  }
}

function outputsAt(s: Step, key: string): string[] {
  if (key.startsWith('b')) return s.branches?.[Number(key.slice(1))]?.next ?? []
  return ((s as unknown as Record<string, string[] | undefined>)[key] ?? []) as string[]
}

function withOutputs(s: Step, key: string, ids: string[]): Step {
  if (key.startsWith('b')) {
    const i = Number(key.slice(1))
    return { ...s, branches: (s.branches ?? []).map((b, j) => (j === i ? { ...b, next: ids } : b)) }
  }
  return { ...s, [key]: ids }
}

function allOutputs(s: Step): string[] {
  return handlesOf(s).flatMap((h) => outputsAt(s, h.key))
}

// fromLegacy turns a fixed-path pipeline into a flow that does the same:
// data check, rules before the call as one first-match condition, the call,
// rules after it, and the result, reject and dead-letter topics.
export function fromLegacy(cfg: Fields): FlowCfg {
  const steps: Step[] = []
  const add = (s: Step) => (steps.push(s), s.id)
  const dest = str(cfg.destination_topic)
  const result = dest ? add({ id: 'result', type: 'topic', topic: dest }) : ''
  const reject = add({ id: 'reject', type: 'reject' })
  const dlq = add({ id: 'dead-letter', type: 'dead_letter' })
  let drop = ''
  let routes = 0
  const action = (r: Fields): string => {
    switch (str(r.action)) {
      case 'pass_through':
        return result
      case 'reject':
        return reject
      case 'dead_letter':
        return dlq
      case 'drop':
        return (drop ||= add({ id: 'drop', type: 'drop' }))
      default:
        routes++
        return r.webhook_override
          ? add({ id: `route-${routes}`, type: 'webhook', url: str(r.webhook_override) })
          : add({ id: `route-${routes}`, type: 'topic', topic: str(r.destination_override) })
    }
  }
  const rules = (key: string, id: string, otherwise: string) => {
    const rs = list(cfg[key])
    if (!rs.length) return otherwise
    return add({ id, type: 'condition', branches: rs.map((r) => ({ name: str(r.name), when: str(r.condition), next: [action(r)].filter(Boolean) })), otherwise: otherwise ? [otherwise] : [] })
  }
  const after = rules('post_callback_rules', 'after-call', result)
  const app = add({
    id: 'app',
    type: 'call',
    target: obj(cfg.target),
    retry: obj(cfg.retry),
    circuit_breaker: obj(cfg.circuit_breaker),
    next: after ? [after] : [],
    on_reject: [reject],
  })
  let first = rules('fast_path_rules', 'before-call', app)
  const dr = obj(cfg.data_rules)
  if (list(dr.fields).length || dr.key || list(dr.headers).length || dr.max_bytes || dr.allow_unknown_fields === false) {
    const onViolation = str(dr.on_violation) || 'reject'
    first = add({ id: 'check', type: 'data_check', rules: dr, next: [first], on_fail: onViolation === 'tag' ? [] : [onViolation === 'dead_letter' ? dlq : reject] })
  }
  const used = new Set([first, ...steps.flatMap(allOutputs)])
  return { start: [first], steps: steps.filter((s) => used.has(s.id)) }
}

function readFlow(cfg: Fields): FlowCfg {
  const f = obj(cfg.flow)
  if (!Array.isArray(f.steps)) return fromLegacy(cfg)
  const start = Array.isArray(f.start) ? f.start.map(str) : [str(f.start)].filter(Boolean)
  return { start, steps: (f.steps as Step[]).map((s) => ({ ...s })) }
}

// layout places steps in columns by how far they are from the source, and
// within a column in the order their ways out are drawn, so lines from one
// step fan out without crossing lines from another.
function layout(flow: FlowCfg): Record<string, { x: number; y: number }> {
  const depth: Record<string, number> = {}
  const order: string[] = []
  const walk = (id: string, d: number, seen: Set<string>) => {
    if (seen.has(id)) return
    depth[id] = Math.max(depth[id] ?? 0, d)
    if (!order.includes(id)) order.push(id)
    const s = flow.steps.find((x) => x.id === id)
    if (!s) return
    const next = new Set(seen).add(id)
    for (const o of allOutputs(s)) walk(o, d + 1, next)
  }
  for (const id of flow.start) walk(id, 1, new Set())
  for (const s of flow.steps) if (!order.includes(s.id)) order.push(s.id)
  const cols: Record<number, string[]> = {}
  for (const id of order) (cols[depth[id] ?? 1] ??= []).push(id)
  const tallest = Math.max(1, ...Object.values(cols).map((c) => c.length))
  const pos: Record<string, { x: number; y: number }> = { [SOURCE]: { x: 0, y: ((tallest - 1) * 130) / 2 } }
  for (const [d, ids] of Object.entries(cols)) {
    const top = ((tallest - ids.length) * 130) / 2
    ids.forEach((id, row) => (pos[id] = { x: Number(d) * 270, y: top + row * 130 }))
  }
  return pos
}

function summary(s: Step): string {
  switch (s.type) {
    case 'call':
      return str(obj(s.target).url) || str(list(obj(s.target).urls)[0]) || 'http://…'
    case 'condition':
      return s.match === 'all' ? 'match: all' : 'match: first'
    case 'data_check':
      return list(obj(s.rules).fields)
        .map((f) => str(f.path))
        .join(', ') || '…'
    case 'topic':
      return s.topic || '…'
    case 'webhook':
      return s.url || 'http://…'
    case 'reject':
    case 'dead_letter':
      return s.topic || s.reason || ''
    default:
      return ''
  }
}

type NodeData = { step?: Step; source?: string; issue: boolean }

function StepNode({ data, selected }: NodeProps<Node<NodeData>>) {
  const t = useT()
  if (data.source !== undefined) {
    return (
      <div className={`n-block n-flow k-source ${selected ? 'selected' : ''}`}>
        <div className="bh">
          <span className="ico">
            <Icon name="stream" className="" />
          </span>
          <b>{t('fz.source')}</b>
        </div>
        <div className="bs">{data.source || '…'}</div>
        <Handle type="source" position={Position.Right} id="start" className="h-out" />
      </div>
    )
  }
  const s = data.step!
  const hs = handlesOf(s)
  return (
    <div className={`n-block n-flow k-${s.type} ${selected ? 'selected' : ''} ${data.issue ? 'issue' : ''}`}>
      <Handle type="target" position={Position.Left} className="h-in" />
      <div className="bh">
        <span className="ico">
          <Icon name={iconOf(s.type)} className="" />
        </span>
        <b>{s.name || t(`fz.t.${s.type}` as Key)}</b>
      </div>
      <div className="bs">{summary(s)}</div>
      {hs.length > 0 && (
        <div className="outs">
          {hs.map((h) => (
            <div key={h.key} className={`out o-${h.key.startsWith('b') ? 'branch' : h.key}`}>
              <span>{h.label.startsWith('fz.') ? t(h.label as Key) : h.label}</span>
              <Handle type="source" position={Position.Right} id={h.key} className="h-out" />
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

const nodeTypes = { step: StepNode }
const edgeColor = (handle: string) =>
  handle === 'on_reject' || handle === 'on_fail' ? '#E4A951' : handle === 'on_failure' ? '#EC6B77' : handle === 'otherwise' ? '#7C848D' : handle.startsWith('b') ? '#7DB6F5' : '#34D399'

function newStep(type: StepType, id: string): Step {
  switch (type) {
    case 'call':
      return { id, type, target: { url: '' }, next: [] }
    case 'condition':
      return { id, type, branches: [{ name: '', when: '', next: [] }], otherwise: [] }
    case 'data_check':
      return { id, type, rules: { fields: [] }, next: [] }
    case 'topic':
      return { id, type, topic: '', next: [] }
    case 'webhook':
      return { id, type, url: '', next: [] }
    default:
      return { id, type }
  }
}

function uniqueId(steps: Step[], type: StepType) {
  const base = type.replace('_', '-')
  for (let n = 1; ; n++) {
    const id = `${base}-${n}`
    if (!steps.some((s) => s.id === id)) return id
  }
}

function hasLoop(flow: FlowCfg): boolean {
  const state: Record<string, number> = {}
  const visit = (id: string): boolean => {
    if (state[id] === 1) return true
    if (state[id] === 2) return false
    state[id] = 1
    const s = flow.steps.find((x) => x.id === id)
    if (s) for (const o of allOutputs(s)) if (visit(o)) return true
    state[id] = 2
    return false
  }
  return flow.steps.some((s) => visit(s.id))
}

interface Issue {
  key: Key
  id?: string
}

function issuesOf(name: string, pipe: Fields, flow: FlowCfg): Issue[] {
  const out: Issue[] = []
  if (!/^[a-z0-9][a-z0-9._-]*$/i.test(name)) out.push({ key: 'dz.i.name' })
  if (!str(pipe.source_topic)) out.push({ key: 'fz.i.source' })
  if (!flow.start.length) out.push({ key: 'fz.i.start' })
  const reached = new Set([...flow.start, ...flow.steps.flatMap(allOutputs)])
  for (const s of flow.steps) {
    if (!reached.has(s.id)) out.push({ key: 'fz.i.orphan', id: s.id })
    if (s.type === 'call' && !/^https?:\/\/\S+$/.test(str(obj(s.target).url)) && !list(obj(s.target).urls).length) out.push({ key: 'dz.i.url', id: s.id })
    if (s.type === 'webhook' && !/^https?:\/\/\S+$/.test(s.url ?? '')) out.push({ key: 'dz.i.url', id: s.id })
    if (s.type === 'condition' && (!s.branches?.length || s.branches.some((b) => !b.when.trim()))) out.push({ key: 'dz.i.condition', id: s.id })
    if (s.type === 'topic' && !s.topic?.trim()) out.push({ key: 'fz.i.topic', id: s.id })
  }
  if (hasLoop(flow)) out.push({ key: 'fz.i.loop' })
  return out
}

// toConfig writes the flow over the pipeline's config. The fixed path's
// fields are dropped since the steps carry them now.
export function toConfig(base: Fields, name: string, project: string, pipe: Fields, flow: FlowCfg): Fields {
  const out: Fields = structuredClone(base)
  for (const k of ['target', 'destination_topic', 'fast_path_rules', 'post_callback_rules', 'data_rules']) delete out[k]
  out.name = name
  if (project) out.project = project
  else delete out.project
  out.source_topic = str(pipe.source_topic)
  out.consumer_group = str(pipe.consumer_group) || `ark-${name}`
  out.workers = num(pipe.workers, 1)
  out.reject_topic = str(pipe.reject_topic)
  out.dead_letter_topic = str(pipe.dead_letter_topic)
  const clean = (s: Step): Step => {
    const c: Record<string, unknown> = { ...s }
    for (const k of Object.keys(c)) {
      const v = c[k]
      if (v === '' || v === undefined || (Array.isArray(v) && v.length === 0)) delete c[k]
    }
    if (TERMINAL.includes(s.type)) delete c.next
    return c as unknown as Step
  }
  out.flow = { start: flow.start, steps: flow.steps.map(clean) }
  return out
}

const KEY_ORDER = ['name', 'id', 'type', 'project', 'tenant', 'mcp_access', 'enabled', 'source_topic', 'consumer_group', 'workers', 'reject_topic', 'dead_letter_topic', 'ordering', 'when', 'next', 'start', 'steps', 'flow', 'path', 'url']
const rank = (k: string) => (KEY_ORDER.includes(k) ? KEY_ORDER.indexOf(k) : KEY_ORDER.length)

// yamlOf writes plain data as YAML, quoting every string, with the keys a
// person looks for first at the top.
export function yamlOf(v: unknown, indent = ''): string {
  const scalar = (x: unknown) => (x === null || x === undefined ? 'null' : typeof x === 'string' ? JSON.stringify(x) : String(x))
  const isEmpty = (x: unknown) => (Array.isArray(x) ? x.length === 0 : x !== null && typeof x === 'object' ? Object.keys(x).length === 0 : false)
  const inline = (x: unknown) => (Array.isArray(x) ? '[]' : '{}')
  const nested = (x: unknown) => x !== null && typeof x === 'object' && !isEmpty(x)
  if (Array.isArray(v)) {
    return v
      .map((item) => {
        if (!nested(item)) return `${indent}- ${isEmpty(item) ? inline(item) : scalar(item)}\n`
        const body = yamlOf(item, indent + '  ')
        return `${indent}- ${body.slice(indent.length + 2)}`
      })
      .join('')
  }
  return Object.entries(obj(v))
    .sort(([a], [b]) => rank(a) - rank(b))
    .map(([k, x]) => (nested(x) ? `${indent}${k}:\n${yamlOf(x, indent + '  ')}` : `${indent}${k}: ${isEmpty(x) ? inline(x) : scalar(x)}\n`))
    .join('')
}

function Input({ label, value, onChange, placeholder, mono = true }: { label: Key; value: string; onChange: (v: string) => void; placeholder?: string; mono?: boolean }) {
  const t = useT()
  return (
    <div className="field">
      <label>{t(label)}</label>
      <input className={`input ${mono ? 'mono' : ''}`} value={value} placeholder={placeholder} onChange={(e) => onChange(e.target.value)} />
    </div>
  )
}

function StepFields({ step, pipe, onChange }: { step: Step; pipe: Fields; onChange: (s: Step) => void }) {
  const t = useT()
  const set = (patch: Partial<Step>) => onChange({ ...step, ...patch })
  const name = <Input label="cfg.name" value={step.name ?? ''} mono={false} onChange={(v) => set({ name: v })} />
  switch (step.type) {
    case 'call': {
      const target = obj(step.target)
      const retry = obj(step.retry)
      const setT = (k: string, v: unknown) => set({ target: { ...target, [k]: v } })
      return (
        <>
          {name}
          <Input label="cfg.target" value={str(target.url)} placeholder="http://app:8080/process" onChange={(v) => setT('url', v)} />
          <Input label="fz.f.health" value={str(target.health_check_url)} placeholder="http://app:8080/health" onChange={(v) => setT('health_check_url', v)} />
          <Input label="dz.f.timeout" value={str(target.timeout_ms)} placeholder="30000" onChange={(v) => setT('timeout_ms', num(v, 0) || undefined)} />
          <div className="grid2">
            <Input label="dz.f.attempts" value={str(retry.max_attempts)} placeholder="3" onChange={(v) => set({ retry: { ...retry, max_attempts: num(v, 0) || undefined } })} />
            <Input label="dz.f.backoff" value={str(retry.backoff_ms)} placeholder="1000" onChange={(v) => set({ retry: { ...retry, backoff_ms: num(v, 0) || undefined } })} />
          </div>
          <span className="small dim">{t('fz.f.callHint')}</span>
        </>
      )
    }
    case 'condition': {
      const branches = step.branches ?? []
      const setB = (i: number, patch: Partial<BranchCfg>) => set({ branches: branches.map((b, j) => (j === i ? { ...b, ...patch } : b)) })
      return (
        <>
          {name}
          <div className="field">
            <label>{t('fz.f.match')}</label>
            <select className="select" value={step.match || 'first'} onChange={(e) => set({ match: e.target.value === 'first' ? undefined : e.target.value })}>
              <option value="first">{t('fz.f.matchFirst')}</option>
              <option value="all">{t('fz.f.matchAll')}</option>
            </select>
          </div>
          {branches.map((b, i) => (
            <div key={i} className="fz-branch stack">
              <div className="row" style={{ justifyContent: 'space-between' }}>
                <b className="small">
                  {t('fz.f.branch')} {i + 1}
                </b>
                {branches.length > 1 && (
                  <button className="btn ghost small" onClick={() => set({ branches: branches.filter((_, j) => j !== i) })} aria-label={t('dz.remove')}>
                    <Icon name="trash" className="" />
                  </button>
                )}
              </div>
              <input className="input" value={b.name ?? ''} placeholder={t('cfg.name')} onChange={(e) => setB(i, { name: e.target.value })} />
              <input className="input mono" value={b.when} placeholder="data.amount > 1000" onChange={(e) => setB(i, { when: e.target.value })} />
            </div>
          ))}
          <button className="btn small" onClick={() => set({ branches: [...branches, { name: '', when: '', next: [] }] })}>
            <Icon name="plus" className="" /> {t('fz.f.addBranch')}
          </button>
          <span className="small dim">{t('fz.f.condHint')}</span>
        </>
      )
    }
    case 'data_check': {
      const rules = obj(step.rules)
      const fields = list(rules.fields)
      const required = fields.filter((f) => f.required).map((f) => str(f.path))
      const setRequired = (v: string) => {
        const want = v
          .split(',')
          .map((x) => x.trim())
          .filter(Boolean)
        const kept = fields.map((f): Fields => ({ ...f, required: want.includes(str(f.path)) })).filter((f) => f.required || Object.keys(f).some((k) => k !== 'path' && k !== 'required'))
        for (const p of want) if (!kept.some((f) => f.path === p)) kept.push({ path: p, required: true })
        set({ rules: { ...rules, fields: kept } })
      }
      return (
        <>
          {name}
          <Input label="dz.f.required" value={required.join(', ')} placeholder="order_id, amount" onChange={setRequired} />
          <div className="field">
            <label>{t('dz.f.onViolation')}</label>
            <select className="select" value={str(rules.on_violation) === 'tag' ? 'tag' : 'route'} onChange={(e) => set({ rules: { ...rules, on_violation: e.target.value === 'tag' ? 'tag' : 'reject' } })}>
              <option value="route">{t('fz.f.failPath')}</option>
              <option value="tag">tag</option>
            </select>
          </div>
        </>
      )
    }
    case 'topic':
      return (
        <>
          {name}
          <Input label="dz.f.topic" value={step.topic ?? ''} placeholder="orders.processed" onChange={(v) => set({ topic: v })} />
        </>
      )
    case 'webhook':
      return (
        <>
          {name}
          <Input label="fz.f.url" value={step.url ?? ''} placeholder="http://crm:8080/notify" onChange={(v) => set({ url: v })} />
        </>
      )
    case 'reject':
    case 'dead_letter':
      return (
        <>
          {name}
          <Input label="dz.f.topic" value={step.topic ?? ''} placeholder={str(step.type === 'reject' ? pipe.reject_topic || pipe.dead_letter_topic : pipe.dead_letter_topic)} onChange={(v) => set({ topic: v })} />
          <Input label="fz.f.reason" value={step.reason ?? ''} mono={false} onChange={(v) => set({ reason: v })} />
        </>
      )
    default:
      return name
  }
}

interface DesignerProps {
  onClose: () => void
  onApplied: () => void
  onSimple: () => void
  // existing opens a running pipeline for editing instead of a new one.
  existing?: { name: string; config: Fields }
}

function Board({ onClose, onApplied, onSimple, existing }: DesignerProps) {
  const t = useT()
  const flowApi = useReactFlow()
  const projects = useProjects()
  const initial = useMemo<FlowCfg>(() => {
    if (existing) return readFlow(existing.config)
    return {
      start: ['app'],
      steps: [
        { id: 'app', type: 'call', target: { url: '' }, next: ['result'] },
        { id: 'result', type: 'topic', topic: '' },
      ],
    }
  }, [existing])
  const [name, setName] = useState(existing?.name ?? '')
  const [project, setProject] = useState(str(existing?.config.project))
  const [pipe, setPipe] = useState<Fields>(() => {
    const c = existing?.config ?? {}
    return { source_topic: str(c.source_topic), consumer_group: str(c.consumer_group), workers: str(c.workers || 1), reject_topic: str(c.reject_topic), dead_letter_topic: str(c.dead_letter_topic) }
  })
  const [touched, setTouched] = useState(!!existing)
  const [flow, setFlow] = useState<FlowCfg>(initial)
  const [pos, setPos] = useState<Record<string, { x: number; y: number }>>(() => layout(initial))
  const [dims, setDims] = useState<Record<string, { width: number; height: number }>>({})
  const [sel, setSel] = useState<string | null>(null)
  const [yaml, setYaml] = useState<string | null>(null)

  // A new pipeline's topics follow its name until someone types their own.
  useEffect(() => {
    if (touched) return
    const n = name || 'pipeline'
    setPipe((p) => ({ ...p, source_topic: `${n}.in`, dead_letter_topic: `${n}.dlq`, reject_topic: `${n}.rejected` }))
    setFlow((f) => ({ ...f, steps: f.steps.map((s) => (s.id === 'result' && s.type === 'topic' ? { ...s, topic: `${n}.out` } : s)) }))
  }, [name, touched])

  const issues = useMemo(() => issuesOf(name, pipe, flow), [name, pipe, flow])
  const nodes: Node<NodeData>[] = useMemo(
    () => [
      { id: SOURCE, type: 'step', position: pos[SOURCE] ?? { x: 0, y: 0 }, measured: dims[SOURCE], selected: sel === SOURCE, deletable: false, data: { source: str(pipe.source_topic), issue: false } },
      ...flow.steps.map((s) => ({ id: s.id, type: 'step', position: pos[s.id] ?? { x: 260, y: 0 }, measured: dims[s.id], selected: sel === s.id, data: { step: s, issue: issues.some((i) => i.id === s.id) } })),
    ],
    [flow, pos, dims, sel, pipe.source_topic, issues],
  )
  const edges: Edge[] = useMemo(() => {
    const line = (id: string, source: string, handle: string, target: string): Edge => ({
      id,
      source,
      sourceHandle: handle,
      target,
      type: 'smoothstep',
      style: { stroke: edgeColor(handle === 'start' ? 'next' : handle), strokeWidth: 1.6 },
    })
    const out: Edge[] = flow.start.map((id) => line(`${SOURCE}:start>${id}`, SOURCE, 'start', id))
    for (const s of flow.steps) for (const h of handlesOf(s)) for (const to of outputsAt(s, h.key)) out.push(line(`${s.id}:${h.key}>${to}`, s.id, h.key, to))
    return out
  }, [flow])

  const connect = (c: Connection) => {
    const from = c.source
    const to = c.target
    if (!from || !to || to === SOURCE || from === to) return
    if (from === SOURCE) {
      setFlow((f) => (f.start.includes(to) ? f : { ...f, start: [...f.start, to] }))
      return
    }
    const key = c.sourceHandle ?? 'next'
    setFlow((f) => ({ ...f, steps: f.steps.map((s) => (s.id !== from || outputsAt(s, key).includes(to) ? s : withOutputs(s, key, [...outputsAt(s, key), to]))) }))
  }
  const removeEdges = (gone: Edge[]) =>
    setFlow((f) => {
      let next = f
      for (const e of gone) {
        const key = e.sourceHandle ?? 'next'
        if (e.source === SOURCE) next = { ...next, start: next.start.filter((id) => id !== e.target) }
        else next = { ...next, steps: next.steps.map((s) => (s.id === e.source ? withOutputs(s, key, outputsAt(s, key).filter((id) => id !== e.target)) : s)) }
      }
      return next
    })
  const removeStep = (id: string) => {
    setFlow((f) => ({
      start: f.start.filter((x) => x !== id),
      steps: f.steps.filter((s) => s.id !== id).map((s) => handlesOf(s).reduce((acc, h) => withOutputs(acc, h.key, outputsAt(acc, h.key).filter((x) => x !== id)), s)),
    }))
    setSel(null)
  }
  const add = (type: StepType, at?: { x: number; y: number }) => {
    const id = uniqueId(flow.steps, type)
    setFlow((f) => ({ ...f, steps: [...f.steps, newStep(type, id)] }))
    setPos((p) => ({ ...p, [id]: at ?? { x: Math.max(0, ...Object.values(p).map((v) => v.x)) + 260, y: 0 } }))
    setSel(id)
  }
  const selected = flow.steps.find((s) => s.id === sel)
  const allSized = flow.steps.every((s) => dims[s.id]) && !!dims[SOURCE]
  useEffect(() => {
    if (!allSized) return
    const id = setTimeout(() => flowApi.fitView({ padding: 0.15, maxZoom: 1, duration: 250 }), 40)
    return () => clearTimeout(id)
  }, [allSized, flowApi])

  if (yaml !== null) {
    return (
      <div className="dz-review stack">
        <div className="row">
          <button className="btn ghost" onClick={() => setYaml(null)}>
            ← {t('dz.back')}
          </button>
          <span className="small dim">{t('dz.reviewHint')}</span>
        </div>
        <ConfigEditor
          initial={yaml}
          onApplied={() => {
            onApplied()
            setTimeout(onClose, 900)
          }}
        />
      </div>
    )
  }

  return (
    <div className="dz">
      <aside className="dz-palette">
        <div className="small dim">{t('fz.palette')}</div>
        {TYPES.map((k) => (
          <button
            key={k.type}
            className={`dz-chip k-${k.type}`}
            draggable
            onDragStart={(e) => {
              e.dataTransfer.setData(MIME, k.type)
              e.dataTransfer.effectAllowed = 'move'
            }}
            onClick={() => add(k.type)}
            title={t(`fz.d.${k.type}` as Key)}
          >
            <span className="ico">
              <Icon name={k.icon} className="" />
            </span>
            <span className="lbl">
              <b>{t(`fz.t.${k.type}` as Key)}</b>
              <span>{t(`fz.d.${k.type}` as Key)}</span>
            </span>
          </button>
        ))}
        <div className="small dim" style={{ marginTop: 'auto' }}>
          {t('fz.dragHint')}
        </div>
      </aside>
      <div
        className="dz-board"
        onDragOver={(e) => {
          if (e.dataTransfer.types.includes(MIME)) {
            e.preventDefault()
            e.dataTransfer.dropEffect = 'move'
          }
        }}
        onDrop={(e) => {
          const type = e.dataTransfer.getData(MIME) as StepType
          if (!type) return
          e.preventDefault()
          const p = flowApi.screenToFlowPosition({ x: e.clientX, y: e.clientY })
          add(type, { x: p.x - 100, y: p.y - 30 })
        }}
      >
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          onNodesChange={(ch: NodeChange<Node<NodeData>>[]) => {
            const moved = applyNodeChanges(
              ch.filter((c) => c.type === 'position'),
              nodes,
            )
            setPos((p) => ({ ...p, ...Object.fromEntries(moved.map((n) => [n.id, n.position])) }))
            const sized = ch.flatMap((c) => (c.type === 'dimensions' && c.dimensions ? [[c.id, c.dimensions] as const] : []))
            if (sized.length) setDims((d) => ({ ...d, ...Object.fromEntries(sized) }))
            for (const c of ch) if (c.type === 'remove' && c.id !== SOURCE) removeStep(c.id)
          }}
          onConnect={connect}
          onEdgesDelete={removeEdges}
          onEdgeDoubleClick={(_, e) => removeEdges([e])}
          onNodeClick={(_, n) => setSel(n.id)}
          onPaneClick={() => setSel(null)}
          deleteKeyCode={['Delete', 'Backspace']}
          minZoom={0.3}
          maxZoom={1.5}
          proOptions={{ hideAttribution: true }}
        >
          <Background gap={22} size={1} color="#23272d" />
        </ReactFlow>
      </div>
      <aside className="dz-props stack">
        {selected ? (
          <>
            <div className="row" style={{ justifyContent: 'space-between' }}>
              <b>{t(`fz.t.${selected.type}` as Key)}</b>
              <button className="btn ghost small" onClick={() => removeStep(selected.id)}>
                <Icon name="trash" className="" /> {t('dz.remove')}
              </button>
            </div>
            <span className="small dim mono">{selected.id}</span>
            <span className="small dim">{t(`fz.d.${selected.type}` as Key)}</span>
            <StepFields step={selected} pipe={pipe} onChange={(s) => setFlow((f) => ({ ...f, steps: f.steps.map((x) => (x.id === s.id ? s : x)) }))} />
          </>
        ) : (
          <>
            <b>{t('dz.pipeline')}</b>
            <div className="field">
              <label>{t('cfg.name')}</label>
              <input className="input mono" value={name} onChange={(e) => setName(e.target.value)} placeholder="orders" autoFocus={!existing} disabled={!!existing} />
            </div>
            <div className="field">
              <label>{t('dz.project')}</label>
              <select className="select" value={project} onChange={(e) => setProject(e.target.value)}>
                <option value="">{t('dz.noProject')}</option>
                {(projects.data?.projects ?? []).map((p) => (
                  <option key={p.name}>{p.name}</option>
                ))}
              </select>
            </div>
            {(
              [
                ['source_topic', 'cfg.source'],
                ['consumer_group', 'cfg.group'],
                ['workers', 'dz.f.workers'],
                ['reject_topic', 'fz.f.rejectTopic'],
                ['dead_letter_topic', 'fz.f.dlqTopic'],
              ] as [string, Key][]
            ).map(([k, label]) => (
              <Input
                key={k}
                label={label}
                value={str(pipe[k])}
                placeholder={k === 'consumer_group' ? `ark-${name || 'pipeline'}` : ''}
                onChange={(v) => {
                  setTouched(true)
                  setPipe((p) => ({ ...p, [k]: v }))
                }}
              />
            ))}
            <span className="small dim">{t('fz.selectHint')}</span>
          </>
        )}
        <div className="dz-check">
          <div className="small dim">{t('dz.checklist')}</div>
          {issues.length === 0 ? (
            <div className="ok small">
              <Icon name="check" className="" /> {t('dz.ready')}
            </div>
          ) : (
            issues.map((i, n) => (
              <button key={n} className="dz-issue small" onClick={() => i.id && setSel(i.id)}>
                <Icon name="alert" className="" />
                {t(i.key)}
                {i.id ? ` (${i.id})` : ''}
              </button>
            ))
          )}
        </div>
        <div className="row" style={{ justifyContent: 'space-between', marginTop: 'auto' }}>
          {existing ? (
            <span />
          ) : (
            <button className="btn ghost small" onClick={onSimple}>
              {t('dz.simple')}
            </button>
          )}
          <button className="btn primary" disabled={issues.length > 0} onClick={() => setYaml(yamlOf(toConfig(existing?.config ?? {}, name, project, pipe, flow)))}>
            {t('dz.review')}
          </button>
        </div>
      </aside>
    </div>
  )
}

export function Designer(props: DesignerProps) {
  const t = useT()
  return (
    <div className="scrim" onClick={props.onClose}>
      <div className="modal dz-modal" onClick={(e) => e.stopPropagation()}>
        <header className="row" style={{ justifyContent: 'space-between' }}>
          <div>
            <h2>{props.existing ? `${t('dz.editTitle')} ${props.existing.name}` : t('cfg.newTitle')}</h2>
            <p className="muted small" style={{ margin: 0 }}>
              {t('fz.hint')}
            </p>
          </div>
          <button className="btn ghost small" onClick={props.onClose} aria-label={t('cfg.cancel')}>
            <Icon name="close" className="" />
          </button>
        </header>
        <ReactFlowProvider>
          <Board {...props} />
        </ReactFlowProvider>
      </div>
    </div>
  )
}
