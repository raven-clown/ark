import { useEffect, useMemo, useState } from 'react'
import {
  Background,
  Handle,
  Position,
  ReactFlow,
  ReactFlowProvider,
  applyNodeChanges,
  useReactFlow,
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

export type Kind = 'source' | 'check' | 'before' | 'target' | 'after' | 'destination' | 'reject' | 'dlq'

type Props = Record<string, string>
type BlockData = { kind: Kind; props: Props; issue: boolean; name: string }

const KINDS: { kind: Kind; icon: IconName; single: boolean; required: boolean }[] = [
  { kind: 'source', icon: 'stream', single: true, required: true },
  { kind: 'check', icon: 'check', single: true, required: false },
  { kind: 'before', icon: 'spark', single: false, required: false },
  { kind: 'target', icon: 'globe', single: true, required: true },
  { kind: 'after', icon: 'spark', single: false, required: false },
  { kind: 'destination', icon: 'stream', single: true, required: true },
  { kind: 'reject', icon: 'inbox', single: true, required: false },
  { kind: 'dlq', icon: 'alert', single: true, required: true },
]
const STEP_X = 250
const STEP_Y = 92
const ACTIONS = ['pass_through', 'reject', 'drop', 'dead_letter', 'transform_route']
const MIME = 'application/x-ark-block'

let seq = 0
const newId = (k: Kind) => `${k}-${++seq}`

// arrange lays the blocks out top to bottom in the order a message passes
// them, with reject and dead letter to the sides of your app.
function arrange(nodes: Node<BlockData>[]): Node<BlockData>[] {
  const pos = new Map<string, { x: number; y: number }>()
  let row = 0
  let targetRow = 0
  for (const k of ['source', 'check', 'before', 'target', 'after', 'destination'] as Kind[]) {
    const list = ordered(nodes, k)
    for (const n of k === 'before' || k === 'after' ? list : list.slice(0, 1)) {
      if (k === 'target') targetRow = row
      pos.set(n.id, { x: 0, y: row++ * STEP_Y })
    }
  }
  const side = (targetRow + 1) * STEP_Y
  const reject = ordered(nodes, 'reject')[0]
  const dlq = ordered(nodes, 'dlq')[0]
  if (reject) pos.set(reject.id, { x: -STEP_X, y: side })
  if (dlq) pos.set(dlq.id, { x: STEP_X, y: side })
  return nodes.map((n) => ({ ...n, position: pos.get(n.id) ?? n.position }))
}

const starter = (): Node<BlockData>[] =>
  arrange(
    (['source', 'target', 'destination', 'dlq'] as Kind[]).map((k) => ({
      id: newId(k),
      type: 'block',
      position: { x: 0, y: 0 },
      data: { kind: k, props: {}, issue: false, name: '' },
    })),
  )

// defaults are what an empty field falls back to, derived from the name.
function defaults(kind: Kind, name: string): Props {
  const n = name || 'pipeline'
  switch (kind) {
    case 'source':
      return { topic: `${n}.in`, group: `ark-${n}`, workers: '1' }
    case 'destination':
      return { topic: `${n}.out` }
    case 'dlq':
      return { topic: `${n}.dlq` }
    case 'reject':
      return { topic: `${n}.rejected` }
    case 'target':
      return { url: '', timeout: '30000', attempts: '3', backoff: '1000' }
    case 'check':
      return { on_violation: 'reject', required: '', max_bytes: '' }
    default:
      return { name: '', condition: '', action: 'pass_through', route: '' }
  }
}

const val = (d: BlockData, k: string) => d.props[k] || defaults(d.kind, d.name)[k] || ''

function summary(d: BlockData): string {
  switch (d.kind) {
    case 'target':
      return val(d, 'url') || 'http://…'
    case 'check':
      return val(d, 'required') ? `required: ${val(d, 'required')}` : val(d, 'on_violation')
    case 'before':
    case 'after':
      return val(d, 'condition') ? `${val(d, 'condition')} → ${val(d, 'action')}` : val(d, 'action')
    default:
      return val(d, 'topic')
  }
}

function BlockNode({ data, selected }: NodeProps<Node<BlockData>>) {
  const t = useT()
  const k = KINDS.find((x) => x.kind === data.kind)!
  return (
    <div className={`n-block k-${data.kind} ${selected ? 'selected' : ''} ${data.issue ? 'issue' : ''}`}>
      {data.kind !== 'source' && <Handle type="target" position={Position.Top} />}
      <div className="bh">
        <span className="ico">
          <Icon name={k.icon} className="" />
        </span>
        <b>{t(`dz.k.${data.kind}` as Key)}</b>
      </div>
      <div className="bs">{summary(data)}</div>
      <Handle type="source" position={Position.Bottom} />
      {data.kind === 'target' && (
        <>
          <Handle type="source" id="l" position={Position.Left} />
          <Handle type="source" id="r" position={Position.Right} />
        </>
      )}
    </div>
  )
}

const nodeTypes = { block: BlockNode }

// ordered returns the blocks of a kind top to bottom, which is also the
// order rules are evaluated in.
function ordered(nodes: Node<BlockData>[], kind: Kind) {
  return nodes.filter((n) => n.data.kind === kind).sort((a, b) => a.position.y - b.position.y || a.position.x - b.position.x)
}

function edgesFor(nodes: Node<BlockData>[]): Edge[] {
  const one = (k: Kind) => ordered(nodes, k)[0]
  const chain = [one('source'), one('check'), ...ordered(nodes, 'before'), one('target'), ...ordered(nodes, 'after'), one('destination')].filter(Boolean)
  const edges: Edge[] = []
  const link = (a: Node, b: Node, color: string, handle?: string) =>
    edges.push({ id: `${a.id}>${b.id}`, source: a.id, target: b.id, sourceHandle: handle, type: 'default', style: { stroke: color, strokeWidth: 1.6, strokeOpacity: 0.7 } })
  for (let i = 1; i < chain.length; i++) link(chain[i - 1], chain[i], '#34D399')
  const target = one('target')
  if (target && one('reject')) link(target, one('reject'), '#E4A951', 'l')
  if (target && one('dlq')) link(target, one('dlq'), '#EC6B77', 'r')
  return edges
}

const q = (s: string) => JSON.stringify(s)
const num = (s: string, def: number) => (/^\d+$/.test(s.trim()) ? Number(s) : def)

export function toYaml(name: string, project: string, blocks: Node<BlockData>[]): string {
  const nodes = blocks.map((n) => ({ ...n, data: { ...n.data, name } }))
  const one = (k: Kind) => ordered(nodes, k)[0]?.data
  const src = one('source')
  const tgt = one('target')
  const L: string[] = [`name: ${q(name)}`]
  if (project) L.push(`project: ${q(project)}`)
  if (src) {
    L.push(`source_topic: ${q(val(src, 'topic'))}`, `consumer_group: ${q(val(src, 'group'))}`, `workers: ${num(val(src, 'workers'), 1)}`)
  }
  for (const [k, key] of [
    ['destination', 'destination_topic'],
    ['dlq', 'dead_letter_topic'],
    ['reject', 'reject_topic'],
  ] as [Kind, string][]) {
    const d = one(k)
    if (d) L.push(`${key}: ${q(val(d, 'topic'))}`)
  }
  if (tgt) {
    L.push('target:', `  url: ${q(val(tgt, 'url'))}`, `  timeout_ms: ${num(val(tgt, 'timeout'), 30000)}`)
    L.push('retry:', `  max_attempts: ${num(val(tgt, 'attempts'), 3)}`, `  backoff_ms: ${num(val(tgt, 'backoff'), 1000)}`)
  }
  const chk = one('check')
  if (chk) {
    L.push('data_rules:', `  on_violation: ${q(val(chk, 'on_violation'))}`)
    if (num(val(chk, 'max_bytes'), 0) > 0) L.push(`  max_bytes: ${num(val(chk, 'max_bytes'), 0)}`)
    const req = val(chk, 'required')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
    if (req.length) {
      L.push('  fields:')
      for (const p of req) L.push(`    - path: ${q(p)}`, '      required: true')
    }
  }
  for (const [k, key] of [
    ['before', 'fast_path_rules'],
    ['after', 'post_callback_rules'],
  ] as [Kind, string][]) {
    const rules = ordered(nodes, k)
    if (!rules.length) continue
    L.push(`${key}:`)
    rules.forEach((r, i) => {
      const d = r.data
      L.push(`  - name: ${q(val(d, 'name') || `${k}-${i + 1}`)}`, `    condition: ${q(val(d, 'condition'))}`, `    action: ${val(d, 'action')}`)
      if (val(d, 'action') === 'transform_route' && val(d, 'route')) L.push(`    destination_override: ${q(val(d, 'route'))}`)
    })
  }
  return L.join('\n') + '\n'
}

function issuesOf(name: string, nodes: Node<BlockData>[]): { key: Key; arg?: string; id?: string }[] {
  const out: { key: Key; arg?: string; id?: string }[] = []
  if (!/^[a-z0-9][a-z0-9._-]*$/i.test(name)) out.push({ key: 'dz.i.name' })
  for (const k of KINDS) if (k.required && !nodes.some((n) => n.data.kind === k.kind)) out.push({ key: 'dz.i.missing', arg: k.kind })
  for (const n of nodes) {
    const d = n.data
    if (d.kind === 'target' && !/^https?:\/\/\S+$/.test(val(d, 'url'))) out.push({ key: 'dz.i.url', id: n.id })
    if ((d.kind === 'before' || d.kind === 'after') && !val(d, 'condition').trim()) out.push({ key: 'dz.i.condition', id: n.id })
    if ((d.kind === 'before' || d.kind === 'after') && val(d, 'action') === 'transform_route' && !val(d, 'route').trim()) out.push({ key: 'dz.i.route', id: n.id })
  }
  return out
}

function Fields({ node, onChange }: { node: Node<BlockData>; onChange: (p: Props) => void }) {
  const t = useT()
  const d = node.data
  const def = defaults(d.kind, d.name)
  const input = (k: string, label: Key, mono = true) => (
    <div className="field" key={k}>
      <label>{t(label)}</label>
      <input className={`input ${mono ? 'mono' : ''}`} value={d.props[k] ?? ''} placeholder={def[k]} onChange={(e) => onChange({ ...d.props, [k]: e.target.value })} />
    </div>
  )
  switch (d.kind) {
    case 'source':
      return (
        <>
          {input('topic', 'cfg.source')}
          {input('group', 'cfg.group')}
          {input('workers', 'dz.f.workers')}
        </>
      )
    case 'target':
      return (
        <>
          {input('url', 'cfg.target')}
          {input('timeout', 'dz.f.timeout')}
          <div className="grid2">
            {input('attempts', 'dz.f.attempts')}
            {input('backoff', 'dz.f.backoff')}
          </div>
        </>
      )
    case 'check':
      return (
        <>
          {input('required', 'dz.f.required')}
          {input('max_bytes', 'dz.f.maxBytes')}
          <div className="field">
            <label>{t('dz.f.onViolation')}</label>
            <select className="select" value={val(d, 'on_violation')} onChange={(e) => onChange({ ...d.props, on_violation: e.target.value })}>
              {['reject', 'dead_letter', 'tag'].map((v) => (
                <option key={v}>{v}</option>
              ))}
            </select>
          </div>
        </>
      )
    case 'before':
    case 'after':
      return (
        <>
          {input('name', 'cfg.name', false)}
          {input('condition', 'dz.f.condition')}
          <span className="small dim">{t(d.kind === 'before' ? 'dz.f.condBefore' : 'dz.f.condAfter')}</span>
          <div className="field">
            <label>{t('dz.f.action')}</label>
            <select className="select" value={val(d, 'action')} onChange={(e) => onChange({ ...d.props, action: e.target.value })}>
              {ACTIONS.map((a) => (
                <option key={a}>{a}</option>
              ))}
            </select>
          </div>
          {val(d, 'action') === 'transform_route' && input('route', 'dz.f.route')}
        </>
      )
    default:
      return input('topic', 'dz.f.topic')
  }
}

function Board({ onClose, onApplied, onSimple }: { onClose: () => void; onApplied: () => void; onSimple: () => void }) {
  const t = useT()
  const flow = useReactFlow()
  const projects = useProjects()
  const [name, setName] = useState('')
  const [project, setProject] = useState('')
  const [nodes, setNodes] = useState<Node<BlockData>[]>(starter)
  const [sel, setSel] = useState<string | null>(null)
  const [yaml, setYaml] = useState<string | null>(null)

  const issues = useMemo(() => issuesOf(name, nodes), [name, nodes])
  const shown = useMemo(
    () => nodes.map((n) => ({ ...n, selected: n.id === sel, data: { ...n.data, name, issue: issues.some((i) => i.id === n.id) } })),
    [nodes, sel, name, issues],
  )
  const edges = useMemo(() => edgesFor(nodes), [nodes])
  const measured = nodes.filter((n) => n.measured?.width).length
  useEffect(() => {
    if (measured !== nodes.length) return
    const id = setTimeout(() => flow.fitView({ padding: 0.15, maxZoom: 1, duration: 250 }), 40)
    return () => clearTimeout(id)
  }, [measured, nodes.length, flow])
  const selected = nodes.find((n) => n.id === sel)

  const add = (kind: Kind, at?: { x: number; y: number }) => {
    const k = KINDS.find((x) => x.kind === kind)!
    if (k.single && nodes.some((n) => n.data.kind === kind)) return
    const id = newId(kind)
    const node: Node<BlockData> = { id, type: 'block', position: at ?? { x: 0, y: 0 }, data: { kind, props: {}, issue: false, name: '' } }
    setNodes((ns) => arrange([...ns, node]))
    setSel(id)
  }
  const remove = (id: string) => {
    setNodes((ns) => arrange(ns.filter((n) => n.id !== id)))
    setSel(null)
  }
  const update = (id: string, props: Props) => setNodes((ns) => ns.map((n) => (n.id === id ? { ...n, data: { ...n.data, props } } : n)))

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
        <div className="small dim">{t('dz.palette')}</div>
        {KINDS.map((k) => {
          const used = k.single && nodes.some((n) => n.data.kind === k.kind)
          return (
            <button
              key={k.kind}
              className={`dz-chip k-${k.kind}`}
              disabled={used}
              draggable={!used}
              onDragStart={(e) => {
                e.dataTransfer.setData(MIME, k.kind)
                e.dataTransfer.effectAllowed = 'move'
              }}
              onClick={() => add(k.kind)}
              title={t(`dz.d.${k.kind}` as Key)}
            >
              <span className="ico">
                <Icon name={k.icon} className="" />
              </span>
              <span className="lbl">
                <b>{t(`dz.k.${k.kind}` as Key)}</b>
                <span>{t(`dz.d.${k.kind}` as Key)}</span>
              </span>
            </button>
          )
        })}
        <div className="small dim" style={{ marginTop: 'auto' }}>
          {t('dz.dragHint')}
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
          const kind = e.dataTransfer.getData(MIME) as Kind
          if (!kind) return
          e.preventDefault()
          const p = flow.screenToFlowPosition({ x: e.clientX, y: e.clientY })
          add(kind, { x: p.x - 90, y: p.y - 28 })
        }}
      >
        <ReactFlow
          nodes={shown}
          edges={edges}
          nodeTypes={nodeTypes}
          onNodesChange={(ch: NodeChange<Node<BlockData>>[]) => setNodes((ns) => applyNodeChanges(ch.filter((c) => c.type === 'position' || c.type === 'dimensions'), ns))}
          onNodeClick={(_, n) => setSel(n.id)}
          onNodeDragStop={() => setNodes(arrange)}
          onPaneClick={() => setSel(null)}
          nodesConnectable={false}
          deleteKeyCode={null}
          fitView
          fitViewOptions={{ padding: 0.2, maxZoom: 1 }}
          minZoom={0.4}
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
              <b>{t(`dz.k.${selected.data.kind}` as Key)}</b>
              <button className="btn ghost small" onClick={() => remove(selected.id)}>
                <Icon name="trash" className="" /> {t('dz.remove')}
              </button>
            </div>
            <span className="small dim">{t(`dz.d.${selected.data.kind}` as Key)}</span>
            <Fields node={{ ...selected, data: { ...selected.data, name } }} onChange={(p) => update(selected.id, p)} />
          </>
        ) : (
          <>
            <b>{t('dz.pipeline')}</b>
            <div className="field">
              <label>{t('cfg.name')}</label>
              <input className="input mono" value={name} onChange={(e) => setName(e.target.value)} placeholder="orders" autoFocus />
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
            <span className="small dim">{t('dz.selectHint')}</span>
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
                {t(i.key).replace('{}', i.arg ? t(`dz.k.${i.arg}` as Key) : '')}
              </button>
            ))
          )}
        </div>
        <div className="row" style={{ justifyContent: 'space-between', marginTop: 'auto' }}>
          <button className="btn ghost small" onClick={onSimple}>
            {t('dz.simple')}
          </button>
          <button className="btn primary" disabled={issues.length > 0} onClick={() => setYaml(toYaml(name, project, nodes))}>
            {t('dz.review')}
          </button>
        </div>
      </aside>
    </div>
  )
}

export function Designer(props: { onClose: () => void; onApplied: () => void; onSimple: () => void }) {
  const t = useT()
  return (
    <div className="scrim" onClick={props.onClose}>
      <div className="modal dz-modal" onClick={(e) => e.stopPropagation()}>
        <header className="row" style={{ justifyContent: 'space-between' }}>
          <div>
            <h2>{t('cfg.newTitle')}</h2>
            <p className="muted small" style={{ margin: 0 }}>
              {t('dz.hint')}
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
