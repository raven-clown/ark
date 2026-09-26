import type { Topology, TopologyEdge } from '../api'

export interface Placed {
  id: string
  x: number
  y: number
}

const COL_W = 310
const PIPE_ROW = 280
const TOPIC_GAP = 64
const ROLE_ORDER = ['destination', 'override', 'webhook', 'reject', 'dead_letter']

// layout places a topology left to right: source topics, then the
// pipelines reading them, then what they write to, so a chain of pipelines
// reads as one line. Each pipeline's outputs stay together beside it, in
// the same order every time, so lines from different pipelines don't cross.
// Targets sit above their pipeline.
export function layout(topo: Topology): Map<string, Placed> {
  const nodes = topo.nodes ?? []
  const edges = topo.edges ?? []
  const kind = new Map(nodes.map((n) => [n.id, n.kind]))
  const col = new Map<string, number>()
  const outs = (id: string) => edges.filter((e) => e.from === id)
  const ins = (id: string) => edges.filter((e) => e.to === id)

  for (const n of nodes) if (n.kind === 'topic' && !ins(n.id).some((e) => kind.get(e.from) === 'pipeline')) col.set(n.id, 0)
  for (const n of nodes) if (!col.has(n.id) && n.kind === 'topic') col.set(n.id, 0)

  for (let pass = 0; pass < nodes.length + 2; pass++) {
    let changed = false
    for (const e of edges) {
      const from = col.get(e.from)
      if (from === undefined) continue
      if (e.role !== 'consume' && !isOutput(e)) continue
      const want = from + 1
      if ((col.get(e.to) ?? -1) < want && want < 2 * nodes.length) {
        col.set(e.to, want)
        changed = true
      }
    }
    if (!changed) break
  }

  // Each output belongs to the first pipeline that writes to it.
  const owner = new Map<string, string>()
  const pipelines = nodes.filter((n) => n.kind === 'pipeline')
  const blocks = new Map<string, TopologyEdge[]>()
  for (const p of pipelines) {
    const mine = outs(p.id)
      .filter(isOutput)
      .filter((e) => !owner.has(e.to) && (owner.set(e.to, p.id), true))
      .sort((a, b) => ROLE_ORDER.indexOf(a.role) - ROLE_ORDER.indexOf(b.role))
    blocks.set(p.id, mine)
  }

  const placed = new Map<string, Placed>()
  const nextY = new Map<number, number>()
  let prevHalf = 0
  for (const p of pipelines) {
    const c = col.get(p.id) ?? 1
    const half = Math.max(PIPE_ROW / 2, ((blocks.get(p.id)?.length ?? 0) * TOPIC_GAP) / 2)
    const last = nextY.get(c)
    const y = last === undefined ? 90 : last + prevHalf + half
    nextY.set(c, y)
    prevHalf = half
    placed.set(p.id, { id: p.id, x: c * COL_W, y })
  }

  // Outputs: one block per pipeline, centred on it, pushed down only if the
  // block above reaches into it.
  const bottom = new Map<number, number>()
  for (const p of pipelines) {
    const at = placed.get(p.id)!
    const mine = blocks.get(p.id) ?? []
    mine.forEach((e, i) => {
      const c = col.get(e.to) ?? (col.get(p.id) ?? 1) + 1
      const want = at.y + 40 + (i - (mine.length - 1) / 2) * TOPIC_GAP
      const y = Math.max(want, (bottom.get(c) ?? -Infinity) + TOPIC_GAP)
      bottom.set(c, y)
      placed.set(e.to, { id: e.to, x: c * COL_W + 20, y })
    })
  }

  // Source topics: level with the pipelines reading them.
  const sources = nodes.filter((n) => n.kind === 'topic' && !placed.has(n.id))
  const byCol = new Map<number, { id: string; want: number }[]>()
  for (const n of sources) {
    const c = col.get(n.id) ?? 0
    const ys = outs(n.id).flatMap((e) => (placed.get(e.to) ? [placed.get(e.to)!.y + 40] : []))
    const list = byCol.get(c) ?? []
    list.push({ id: n.id, want: ys.length ? ys.reduce((a, b) => a + b, 0) / ys.length : 0 })
    byCol.set(c, list)
  }
  for (const [c, list] of byCol) {
    list.sort((a, b) => a.want - b.want)
    let last = bottom.get(c) ?? -Infinity
    for (const t of list) {
      const y = Math.max(t.want, last + TOPIC_GAP)
      placed.set(t.id, { id: t.id, x: c * COL_W + 20, y })
      last = y
    }
  }

  // Targets: above the pipeline that calls them.
  for (const p of pipelines) {
    const at = placed.get(p.id)!
    outs(p.id)
      .filter((e) => e.role === 'call')
      .forEach((e, i) => {
        if (!placed.has(e.to)) placed.set(e.to, { id: e.to, x: at.x + 16, y: at.y - 96 - i * 40 })
      })
  }
  for (const n of nodes) if (!placed.has(n.id)) placed.set(n.id, { id: n.id, x: 0, y: 0 })
  return placed
}

function isOutput(e: TopologyEdge) {
  return ROLE_ORDER.includes(e.role)
}
