import { memo, useCallback, useEffect, useMemo, useRef, useState, type RefObject } from 'react'
import {
  Background,
  BaseEdge,
  Controls,
  MiniMap,
  Handle,
  Panel,
  Position,
  ReactFlow,
  getBezierPath,
  useEdgesState,
  useNodesState,
  type Edge,
  type EdgeProps,
  type Node,
  type NodeProps,
  type ReactFlowInstance,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'

import { api, type Health, type PipelineStats, type Topology, type TopologyEdge } from '../api'
import { useT } from '../i18n'
import { layout } from './layout'
import { Icon, Sparkline } from './Icon'
import { useProjects } from './ProjectsView'
import { CountUp } from './fx'
import { Mark } from './Logo'

export interface TailFocus {
  stage?: 'in' | 'callback' | 'out'
  to?: string
}

interface Props {
  search: string
  health: Record<string, Health>
  motion: boolean
  selected?: string
  onSelect: (pipeline: string, tab?: string, focus?: TailFocus) => void
  onNew: () => void
  refreshKey: number
}

interface Rates {
  calls: number
  in: number
  out: number
  reject: number
  dlq: number
}

type PipeData = { label: string; stats?: PipelineStats; health?: Health; rates?: Rates; spark?: number[]; selected: boolean; dimmed?: boolean }
type TopicData = { label: string; role?: string; dimmed?: boolean }
type TargetData = { label: string; kind: string; dimmed?: boolean }
type FlowData = { role: TopologyEdge['role']; rate: number; perDot: number; alert?: 'amber' | 'coral'; motion: boolean; rule?: string }

const roleColor: Record<string, string> = {
  consume: '#34D399',
  call: '#7C848D',
  destination: '#34D399',
  override: '#34D399',
  webhook: '#34D399',
  reject: '#E4A951',
  dead_letter: '#EC6B77',
}


function PipelineNode({ data }: NodeProps<Node<PipeData>>) {
  const t = useT()
  const s = data.stats
  const h = data.health ?? (s?.paused ? 'paused' : 'healthy')
  const rate = data.rates?.in ?? 0
  return (
    <div className={`n-pipe ${h} ${data.selected ? 'selected' : ''} ${data.dimmed ? 'dimmed' : ''} ${rate > 0 && h === 'healthy' ? 'flowing' : ''}`}>
      <Handle type="target" position={Position.Left} />
      <div className="head">
        <span className="lg">
          <Mark flow={h === 'down' ? '#FF6B81' : h === 'degraded' ? '#F5B84B' : h === 'paused' ? '#5F6975' : '#2EE6A6'} />
        </span>
        <div className="name">{data.label}</div>
        <span className={`st ${h}`}>{t(`health.${h}`)}</span>
      </div>
      <div className="nums">
        <div>
          <b>
            <CountUp value={rate} decimals={rate >= 100 ? 0 : 1} />
          </b>
          <span>{t('stat.rate')}</span>
        </div>
        <div>
          <b className={s && s.lag > 0 ? 'warn' : ''}>{s?.lag ?? 0}</b>
          <span>{t('stat.lag')}</span>
        </div>
        <div>
          <b className={s && s.dead_lettered > 0 ? 'err' : ''}>{s?.dead_lettered ?? 0}</b>
          <span>{t('stat.dlq')}</span>
        </div>
      </div>
      <div className="foot">
        <span>
          {s?.running ?? 0}/{s?.local_workers ?? 0} {t('stat.workers')}
        </span>
        <Sparkline values={data.spark ?? []} width={64} height={16} color={h === 'down' ? '#EC6B77' : h === 'degraded' ? '#E4A951' : '#34D399'} />
        <span>{(s?.avg_callback_ms ?? 0).toFixed(1)} ms</span>
      </div>
      <Handle type="source" position={Position.Right} />
      <Handle type="source" id="top" position={Position.Top} />
    </div>
  )
}

function TopicNode({ data }: NodeProps<Node<TopicData>>) {
  return (
    <div className={`n-topic ${data.role ?? ''} ${data.dimmed ? 'dimmed' : ''}`}>
      <Handle type="target" position={Position.Left} />
      <span className="ico">
        <Icon name="stream" className="" />
      </span>
      {data.label}
      <Handle type="source" position={Position.Right} />
    </div>
  )
}

function TargetNode({ data }: NodeProps<Node<TargetData>>) {
  return (
    <div className={`n-target ${data.dimmed ? 'dimmed' : ''}`} title={data.label}>
      <Handle type="target" position={Position.Bottom} />
      <Handle type="target" id="left" position={Position.Left} />
      <span className="ico">
        <Icon name="globe" className="" />
      </span>
      <span className="u">{data.label}</span>
    </div>
  )
}

const MAX_DOTS = 32
const MAX_EMIT = 6
const PER_DOT_STEPS = [10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000]

// messagesPerDot picks how many messages one dot stands for, the same on
// every line so they can be compared: 10, or more when the busiest line
// would otherwise release dots faster than the eye can follow.
function messagesPerDot(maxRate: number) {
  return PER_DOT_STEPS.find((n) => maxRate / n <= MAX_EMIT) ?? PER_DOT_STEPS[PER_DOT_STEPS.length - 1]
}

// dotSpeed is how many pixels a second a dot travels on a line carrying
// rate messages a second: busier lines run faster, slowly enough to follow.
function dotSpeed(rate: number) {
  return Math.min(120, 25 + 18 * Math.log2(1 + rate))
}

type Live = { rate: number; perDot: number; enabled: boolean }

// FlowDots releases one dot per perDot messages crossing the line and moves
// each dot to the end of the
// line at a speed set by that line's own rate. New numbers only change how
// often dots are released and, gradually, how fast they move; dots already
// on the line keep going, so a refresh never resets the animation. It only
// re-renders when the line itself or its color changes.
const FlowDots = memo(function FlowDots({ path, color, live }: { path: string; color: string; live: RefObject<Live> }) {
  const pathRef = useRef<SVGPathElement>(null)
  const dotRefs = useRef<(SVGGElement | null)[]>([])

  useEffect(() => {
    let raf = 0
    let last = performance.now()
    let due = 0
    let speed = 0
    let seeded = false
    let shape = ''
    let len = 0
    const dots: number[] = []
    const tick = (now: number) => {
      const dt = Math.min(1 / 30, (now - last) / 1000)
      last = now
      const el = pathRef.current
      const d = el?.getAttribute('d') ?? ''
      if (el && d !== shape) {
        shape = d
        len = el.getTotalLength()
      }
      const { rate: r, perDot, enabled: on } = live.current
      const emit = on ? Math.min(r / perDot, MAX_EMIT) : 0
      speed += (dotSpeed(r) - speed) * Math.min(1, dt / 3)
      if (el && len > 0) {
        if (!seeded && emit > 0) {
          seeded = true
          speed = dotSpeed(r)
          const gap = speed / emit / len
          for (let p = gap / 2; p < 1 && dots.length < MAX_DOTS; p += gap) dots.push(p)
        }
        if (emit > 0) {
          due += emit * dt
          while (due >= 1 && dots.length < MAX_DOTS) {
            due -= 1
            dots.push((due / emit) * (speed / len))
          }
          due = Math.min(due, 1)
        } else {
          due = 0
        }
        const step = (speed * dt) / len
        for (let i = dots.length - 1; i >= 0; i--) {
          dots[i] += step
          if (dots[i] >= 1) dots.splice(i, 1)
        }
      }
      for (let i = 0; i < MAX_DOTS; i++) {
        const g = dotRefs.current[i]
        if (!g) continue
        if (i >= dots.length || !el) {
          g.setAttribute('opacity', '0')
          continue
        }
        const pt = el.getPointAtLength(dots[i] * len)
        g.setAttribute('transform', `translate(${pt.x},${pt.y})`)
        g.setAttribute('opacity', '1')
      }
      raf = requestAnimationFrame(tick)
    }
    raf = requestAnimationFrame(tick)
    return () => cancelAnimationFrame(raf)
  }, [live])

  return (
    <>
      <path ref={pathRef} d={path} fill="none" stroke="none" />
      {Array.from({ length: MAX_DOTS }, (_, i) => (
        <g
          key={i}
          ref={(el) => {
            dotRefs.current[i] = el
          }}
          opacity="0"
          style={{ pointerEvents: 'none' }}
        >
          <circle r="5" fill={color} opacity="0.14" />
          <circle r="2.6" fill={color} />
        </g>
      ))}
    </>
  )
})

// FlowEdge draws a connection with dots moving along it at a speed and
// density that follow the real message rate.
function FlowEdge({ id, sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, data }: EdgeProps<Edge<FlowData>>) {
  const [path] = getBezierPath({ sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition })
  const d = data!
  const color = d.alert === 'coral' ? '#EC6B77' : d.alert === 'amber' ? '#E4A951' : roleColor[d.role] ?? '#8B96A3'
  const active = d.rate > 0
  const live = useRef<Live>({ rate: d.rate, perDot: d.perDot, enabled: d.motion })
  live.current = { rate: d.rate, perDot: d.perDot, enabled: d.motion }
  return (
    <>
      <BaseEdge
        id={id}
        path={path}
        style={{
          stroke: active || d.alert ? color : '#3A4047',
          strokeOpacity: active ? 0.55 : d.alert ? 0.8 : 1,
          strokeWidth: 1.6,
        }}
      />
      <FlowDots path={path} color={color} live={live} />
    </>
  )
}

// splitTargets gives every pipeline its own copy of the HTTP targets it
// calls, so each call line is a short straight link above its pipeline
// instead of a long bent line to one shared node.
function splitTargets(t: Topology): Topology {
  const nodes = (t.nodes ?? []).filter((n) => n.kind !== 'target')
  const byId = new Map((t.nodes ?? []).map((n) => [n.id, n]))
  const edges = (t.edges ?? []).map((e) => {
    if (e.role !== 'call') return e
    const id = `${e.to}@${e.from}`
    const orig = byId.get(e.to)
    if (orig && !nodes.some((n) => n.id === id)) nodes.push({ ...orig, id })
    return { ...e, to: id }
  })
  return { nodes, edges }
}

const nodeTypes = { pipeline: PipelineNode, topic: TopicNode, target: TargetNode }
const edgeTypes = { flow: FlowEdge }

export function Canvas({ search, health, motion, selected, onSelect, onNew, refreshKey }: Props) {
  const t = useT()
  const [topo, setTopo] = useState<Topology | null>(null)
  const [error, setError] = useState('')
  const [rates, setRates] = useState<Record<string, Rates>>({})
  const prev = useRef<Record<string, { at: number; s: PipelineStats }>>({})
  const spark = useRef<Record<string, number[]>>({})
  const recentRates = useRef<Record<string, Rates[]>>({})
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const shape = useRef('')
  const pendingFit = useRef(false)
  const fitTimer = useRef(0)
  const flow = useRef<ReactFlowInstance | null>(null)
  const [showMap, setShowMap] = useState(true)
  const [perDot, setPerDot] = useState(PER_DOT_STEPS[0])
  const [projectFilter, setProjectFilter] = useState('')
  const projects = useProjects(15000)

  const load = useCallback(async () => {
    try {
      const next = splitTargets(await api<Topology>('/topology'))
      const now = performance.now()
      const r: Record<string, Rates> = {}
      for (const n of next.nodes ?? []) {
        if (n.kind !== 'pipeline' || !n.stats) continue
        const p = prev.current[n.id]
        if (p) {
          const dt = Math.max(0.5, (now - p.at) / 1000)
          const d = (a: number, b: number) => Math.max(0, a - b) / dt
          const out = d(n.stats.processed, p.s.processed)
          const reject = d(n.stats.rejected, p.s.rejected)
          const dlq = d(n.stats.dead_lettered, p.s.dead_lettered)
          const calls = d(n.stats.callback_calls, p.s.callback_calls)
          // Average the last three polls (about 6s) so a pause between
          // bursts doesn't make the lines flicker off and on.
          const h = [...(recentRates.current[n.id] ?? []), { calls, in: out + reject + dlq, out, reject, dlq }].slice(-3)
          recentRates.current[n.id] = h
          const avg = (k: keyof Rates) => h.reduce((a, x) => a + x[k], 0) / h.length
          r[n.id] = { calls: avg('calls'), in: avg('in'), out: avg('out'), reject: avg('reject'), dlq: avg('dlq') }
          spark.current[n.id] = [...(spark.current[n.id] ?? []), r[n.id].in].slice(-24)
        }
        prev.current[n.id] = { at: now, s: n.stats }
      }
      setRates(r)
      setTopo(next)
      setError('')
    } catch (e) {
      setError((e as Error).message)
    }
  }, [])

  useEffect(() => {
    load()
    const id = setInterval(load, 2000)
    return () => clearInterval(id)
  }, [load, refreshKey])

  const placed = useMemo(() => (topo ? layout(topo) : new Map()), [topo])

  useEffect(() => {
    if (!topo) return
    const sig = JSON.stringify([(topo.nodes ?? []).map((n) => n.id), (topo.edges ?? []).map((e) => e.from + e.to + e.role)])
    const roleOf = new Map<string, string>()
    for (const e of topo.edges ?? []) if (e.role === 'reject' || e.role === 'dead_letter') roleOf.set(e.to, e.role)
    const q = search.trim().toLowerCase()
    const inProject = new Set(projects.data?.projects.find((p) => p.name === projectFilter)?.pipelines ?? [])
    const touches = (id: string) =>
      (topo.edges ?? []).some((e) => (e.from === id && inProject.has(e.to.replace(/^pipeline:/, ''))) || (e.to === id && inProject.has(e.from.replace(/^pipeline:/, ''))))
    const outside = (id: string, label: string, kind: string) => projectFilter !== '' && !(kind === 'pipeline' ? inProject.has(label) : touches(id))
    const dimmedNode = (id: string, label: string, kind: string) => (q !== '' && !label.toLowerCase().includes(q)) || outside(id, label, kind)
    const build = (n: NonNullable<Topology['nodes']>[number]): Node => {
      const base = { id: n.id, position: { x: placed.get(n.id)?.x ?? 0, y: placed.get(n.id)?.y ?? 0 } }
      if (n.kind === 'pipeline')
        return { ...base, type: 'pipeline', data: { label: n.label, stats: n.stats, health: health[n.label], rates: rates[n.id], spark: spark.current[n.id], selected: selected === n.label, dimmed: dimmedNode(n.id, n.label, 'pipeline') } }
      if (n.kind === 'topic') return { ...base, type: 'topic', data: { label: n.label, role: roleOf.get(n.id), dimmed: dimmedNode(n.id, n.label, 'topic') } }
      return { ...base, type: 'target', data: { label: n.label, kind: n.kind, dimmed: dimmedNode(n.id, n.label, n.kind) } }
    }
    if (sig !== shape.current) {
      shape.current = sig
      pendingFit.current = true
      setNodes((topo.nodes ?? []).map(build))
    } else {
      setNodes((cur) =>
        cur.map((node) => {
          const n = (topo.nodes ?? []).find((x) => x.id === node.id)
          return n ? { ...node, data: build(n).data } : node
        }),
      )
    }
    const stats = new Map((topo.nodes ?? []).map((n) => [n.id, n.stats]))
    const perDot = messagesPerDot(Math.max(0, ...Object.values(rates).flatMap((r) => [r.in, r.calls])))
    setPerDot(perDot)
    setEdges(
      (topo.edges ?? []).map((e, i): Edge => {
        const pid = e.role === 'consume' ? e.to : e.from
        const r = rates[pid]
        const s = stats.get(pid)
        const rate =
          e.role === 'consume' ? r?.in ?? 0 : e.role === 'call' ? r?.calls ?? 0 : e.role === 'destination' || e.role === 'override' || e.role === 'webhook' ? r?.out ?? 0 : e.role === 'reject' ? r?.reject ?? 0 : r?.dlq ?? 0
        const breakerOpen = s?.breaker_state === 'open'
        return {
          id: `${e.from}>${e.to}>${e.role}>${i}`,
          source: e.from,
          target: e.to,
          sourceHandle: e.role === 'call' ? 'top' : undefined,
          targetHandle: e.role === 'webhook' ? 'left' : undefined,
          type: 'flow',
          data: { role: e.role, rate, perDot, motion, rule: e.rule, alert: e.role === 'call' && breakerOpen ? 'coral' : s?.paused && e.role === 'consume' ? 'amber' : undefined },
        }
      }),
    )
  }, [topo, placed, rates, health, selected, motion, search, projectFilter, projects.data, setNodes, setEdges])

  const onEdgeClick = useCallback(
    (_: unknown, edge: Edge) => {
      const role = (edge.data as FlowData).role
      const pipeline = (role === 'consume' ? edge.target : edge.source).replace(/^pipeline:/, '')
      const focus: TailFocus =
        role === 'consume' ? { stage: 'in' } : role === 'call' ? { stage: 'callback' } : { stage: 'out', to: role === 'dead_letter' ? 'dlq' : role }
      onSelect(pipeline, 'tail', focus)
    },
    [onSelect],
  )

  const empty = topo && !(topo.nodes ?? []).some((n) => n.kind === 'pipeline')

  return (
    <div className="canvas">
      <div className="canvas-bar">
        <div className="title">
          <b>{t('nav.pipelines')}</b>
          <span>{error ? <span className="err">{error}</span> : t('canvas.hint')}</span>
        </div>
        <div className="spacer" />
        {(projects.data?.projects.length ?? 0) > 0 && (
          <select className="select" style={{ width: 170, height: 32 }} value={projectFilter} onChange={(e) => setProjectFilter(e.target.value)}>
            <option value="">{t('canvas.allProjects')}</option>
            {projects.data?.projects.map((p) => (
              <option key={p.name} value={p.name}>
                {p.name}
              </option>
            ))}
          </select>
        )}
        <div className="tools">
          <button
            className="btn sm"
            onClick={() => {
              setNodes((cur) => cur.map((n) => ({ ...n, position: { x: placed.get(n.id)?.x ?? n.position.x, y: placed.get(n.id)?.y ?? n.position.y } })))
              setTimeout(() => flow.current?.fitView({ padding: 0.18, maxZoom: 1.15, duration: 450 }), 30)
            }}
          >
            <Icon name="scale" className="" />
            {t('canvas.autolayout')}
          </button>
          <button className={`btn sm ${showMap ? 'on' : ''}`} onClick={() => setShowMap(!showMap)}>
            <Icon name="cluster" className="" />
            {t('canvas.minimap')}
          </button>
          <button
            className="btn sm icon-btn"
            title={t('canvas.fullscreen')}
            onClick={() => {
              const el = document.querySelector('.stage-area')
              if (document.fullscreenElement) document.exitFullscreen()
              else el?.requestFullscreen?.()
            }}
          >
            <Icon name="expand" className="" />
          </button>
        </div>
        <button className="btn primary" onClick={onNew}>
          <Icon name="plus" className="" />
          {t('canvas.new')}
        </button>
      </div>
      {empty && (
        <div className="empty" style={{ position: 'absolute', inset: 0, zIndex: 2, pointerEvents: 'none' }}>
          <Icon name="pipelines" className="" />
          {t('canvas.empty')}
        </div>
      )}
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={(changes) => {
          onNodesChange(changes)
          // Fit once the new nodes have been measured; fitting earlier uses
          // zero sizes and leaves part of the graph off screen.
          if (pendingFit.current && changes.some((c) => c.type === 'dimensions')) {
            clearTimeout(fitTimer.current)
            fitTimer.current = window.setTimeout(() => {
              pendingFit.current = false
              flow.current?.fitView({ padding: 0.12, maxZoom: 1.1, minZoom: 0.8, duration: 400 })
            }, 50)
          }
        }}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        onNodeClick={(_, n) => n.type === 'pipeline' && onSelect((n.data as PipeData).label)}
        onEdgeClick={onEdgeClick}
        onInit={(inst) => (flow.current = inst)}
        fitView
        fitViewOptions={{ padding: 0.12, maxZoom: 1.1, minZoom: 0.8 }}
        minZoom={0.2}
        proOptions={{ hideAttribution: true }}
        nodesConnectable={false}
        colorMode="dark"
      >
        <Background color="#1a1d21" gap={28} size={1.2} />
        {motion && (
          <Panel position="bottom-center" className="dot-scale">
            ● = {perDot.toLocaleString()} {t('canvas.dotMessages')}
          </Panel>
        )}
        <Controls showInteractive={false} position="bottom-left" />
        {showMap && (
          <MiniMap
            pannable
            zoomable
            position="bottom-right"
            bgColor="#121417"
            maskColor="rgba(12,13,15,0.7)"
            nodeColor={(n) => (n.type === 'pipeline' ? ((n.data as PipeData).health === 'down' ? '#EC6B77' : (n.data as PipeData).health === 'degraded' ? '#E4A951' : '#3B4148') : n.type === 'topic' ? '#2A2F35' : '#1F2328')}
            nodeBorderRadius={6}
            style={{ width: 180, height: 110 }}
          />
        )}
      </ReactFlow>
    </div>
  )
}
