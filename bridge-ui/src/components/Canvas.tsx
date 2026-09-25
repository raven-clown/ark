import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  Background,
  BaseEdge,
  Controls,
  MiniMap,
  Handle,
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
  in: number
  out: number
  reject: number
  dlq: number
}

type PipeData = { label: string; stats?: PipelineStats; health?: Health; rates?: Rates; spark?: number[]; selected: boolean; dimmed?: boolean }
type TopicData = { label: string; role?: string; dimmed?: boolean }
type TargetData = { label: string; kind: string; dimmed?: boolean }
type FlowData = { role: TopologyEdge['role']; rate: number; alert?: 'amber' | 'coral'; motion: boolean; rule?: string }

const roleColor: Record<string, string> = {
  consume: '#8FB8FF',
  call: '#8B96A3',
  destination: '#2EE6A6',
  override: '#2EE6A6',
  webhook: '#2EE6A6',
  reject: '#F5B84B',
  dead_letter: '#FF6B81',
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
        <Sparkline values={data.spark ?? []} width={64} height={16} color={h === 'down' ? '#FF6B81' : h === 'degraded' ? '#F5B84B' : '#2EE6A6'} />
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

// FlowEdge draws a connection with dots moving along it at a speed and
// density that follow the real message rate.
function FlowEdge({ id, sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, data }: EdgeProps<Edge<FlowData>>) {
  const [path] = getBezierPath({ sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition })
  const d = data!
  const color = d.alert === 'coral' ? '#FF6B81' : d.alert === 'amber' ? '#F5B84B' : roleColor[d.role] ?? '#8B96A3'
  const active = d.rate > 0
  const dots = d.motion && active ? Math.min(6, Math.max(1, Math.ceil(Math.log2(d.rate + 1)))) : 0
  const dur = Math.max(0.7, Math.min(3, 2.6 - Math.log10(d.rate + 1) * 0.9))
  return (
    <>
      <BaseEdge
        id={id}
        path={path}
        style={{
          stroke: active || d.alert ? color : '#2A323C',
          strokeOpacity: active ? 0.55 : d.alert ? 0.8 : 1,
          strokeWidth: active ? 1.8 : 1.4,
          strokeDasharray: d.role === 'call' || d.role === 'webhook' ? '4 5' : undefined,
        }}
      />
      {Array.from({ length: dots }, (_, i) => (
        <g key={i}>
          <circle r="7" fill={color} opacity="0.18">
            <animateMotion dur={`${dur}s`} repeatCount="indefinite" begin={`-${(i * dur) / dots}s`} path={path} />
          </circle>
          <circle r="3.2" fill={color}>
            <animateMotion dur={`${dur}s`} repeatCount="indefinite" begin={`-${(i * dur) / dots}s`} path={path} />
          </circle>
        </g>
      ))}
    </>
  )
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
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const shape = useRef('')
  const flow = useRef<ReactFlowInstance | null>(null)
  const [showMap, setShowMap] = useState(true)

  const load = useCallback(async () => {
    try {
      const next = await api<Topology>('/topology')
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
          r[n.id] = { in: out + reject + dlq, out, reject, dlq }
          spark.current[n.id] = [...(spark.current[n.id] ?? []), out + reject + dlq].slice(-24)
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
    const dimmed = (label: string) => q !== '' && !label.toLowerCase().includes(q)
    const build = (n: NonNullable<Topology['nodes']>[number]): Node => {
      const base = { id: n.id, position: { x: placed.get(n.id)?.x ?? 0, y: placed.get(n.id)?.y ?? 0 } }
      if (n.kind === 'pipeline')
        return { ...base, type: 'pipeline', data: { label: n.label, stats: n.stats, health: health[n.label], rates: rates[n.id], spark: spark.current[n.id], selected: selected === n.label, dimmed: dimmed(n.label) } }
      if (n.kind === 'topic') return { ...base, type: 'topic', data: { label: n.label, role: roleOf.get(n.id), dimmed: dimmed(n.label) } }
      return { ...base, type: 'target', data: { label: n.label, kind: n.kind, dimmed: dimmed(n.label) } }
    }
    if (sig !== shape.current) {
      shape.current = sig
      setNodes((topo.nodes ?? []).map(build))
      for (const ms of [60, 400, 1200]) setTimeout(() => flow.current?.fitView({ padding: 0.14, maxZoom: 1.2, duration: 400 }), ms)
    } else {
      setNodes((cur) =>
        cur.map((node) => {
          const n = (topo.nodes ?? []).find((x) => x.id === node.id)
          return n ? { ...build(n), position: node.position } : node
        }),
      )
    }
    const stats = new Map((topo.nodes ?? []).map((n) => [n.id, n.stats]))
    setEdges(
      (topo.edges ?? []).map((e, i): Edge => {
        const pid = e.role === 'consume' ? e.to : e.from
        const r = rates[pid]
        const s = stats.get(pid)
        const rate =
          e.role === 'consume' || e.role === 'call' ? r?.in ?? 0 : e.role === 'destination' || e.role === 'override' || e.role === 'webhook' ? r?.out ?? 0 : e.role === 'reject' ? r?.reject ?? 0 : r?.dlq ?? 0
        const breakerOpen = s?.breaker_state === 'open'
        return {
          id: `${e.from}>${e.to}>${e.role}>${i}`,
          source: e.from,
          target: e.to,
          sourceHandle: e.role === 'call' || e.role === 'webhook' ? 'top' : undefined,
          type: 'flow',
          data: { role: e.role, rate, motion, rule: e.rule, alert: e.role === 'call' && breakerOpen ? 'coral' : s?.paused && e.role === 'consume' ? 'amber' : undefined },
        }
      }),
    )
  }, [topo, placed, rates, health, selected, motion, search, setNodes, setEdges])

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
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        onNodeClick={(_, n) => n.type === 'pipeline' && onSelect((n.data as PipeData).label)}
        onEdgeClick={onEdgeClick}
        onInit={(inst) => (flow.current = inst)}
        fitView
        fitViewOptions={{ padding: 0.14, maxZoom: 1.2 }}
        minZoom={0.2}
        proOptions={{ hideAttribution: true }}
        nodesConnectable={false}
        colorMode="dark"
      >
        <Background color="#161b21" gap={26} size={1.3} />
        <Controls showInteractive={false} position="bottom-left" />
        {showMap && (
          <MiniMap
            pannable
            zoomable
            position="bottom-right"
            bgColor="rgba(10,12,15,0.92)"
            maskColor="rgba(6,7,8,0.6)"
            nodeColor={(n) => (n.type === 'pipeline' ? ((n.data as PipeData).health === 'down' ? '#FF6B81' : (n.data as PipeData).health === 'degraded' ? '#F5B84B' : '#2EE6A6') : n.type === 'topic' ? '#3A4550' : '#252C34')}
            nodeBorderRadius={6}
            style={{ width: 180, height: 110 }}
          />
        )}
      </ReactFlow>
    </div>
  )
}
