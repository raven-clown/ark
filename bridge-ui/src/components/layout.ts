import type { Topology, TopologyEdge } from '../api'

export interface Placed {
  id: string
  x: number
  y: number
}

const COL_W = 310
const PIPE_ROW = 280
const TOPIC_GAP = 64

// layout places a topology left to right: source topics, then the
// pipelines reading them, then the topics they write to, so a chain of
// pipelines reads as one line. Targets sit above their pipeline.
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
      const flows = e.role === 'consume' || isOutput(e)
      if (!flows) continue
      const want = from + 1
      if ((col.get(e.to) ?? -1) < want && want < 2 * nodes.length) {
        col.set(e.to, want)
        changed = true
      }
    }
    if (!changed) break
  }

  const placed = new Map<string, Placed>()
  const pipelines = nodes.filter((n) => n.kind === 'pipeline')
  const rowsByCol = new Map<number, number>()
  for (const p of pipelines) {
    const c = col.get(p.id) ?? 1
    const row = rowsByCol.get(c) ?? 0
    rowsByCol.set(c, row + 1)
    placed.set(p.id, { id: p.id, x: c * COL_W, y: row * PIPE_ROW + 90 })
  }

  // Topics: next to the pipelines that touch them, then pushed apart.
  const topicsByCol = new Map<number, { id: string; want: number }[]>()
  for (const n of nodes.filter((n) => n.kind === 'topic')) {
    const c = col.get(n.id) ?? 0
    const ys: number[] = []
    for (const e of ins(n.id)) {
      const p = placed.get(e.from)
      if (p) ys.push(p.y + outputOffset(e, outs(e.from)))
    }
    for (const e of outs(n.id)) {
      const p = placed.get(e.to)
      if (p) ys.push(p.y + 40)
    }
    const want = ys.length ? ys.reduce((a, b) => a + b, 0) / ys.length : 0
    const list = topicsByCol.get(c) ?? []
    list.push({ id: n.id, want })
    topicsByCol.set(c, list)
  }
  for (const [c, list] of topicsByCol) {
    list.sort((a, b) => a.want - b.want)
    let last = -Infinity
    for (const t of list) {
      const y = Math.max(t.want, last + TOPIC_GAP)
      placed.set(t.id, { id: t.id, x: c * COL_W + 20, y })
      last = y
    }
  }

  // Targets and webhooks: above or beside the pipeline that calls them.
  for (const p of pipelines) {
    const at = placed.get(p.id)!
    outs(p.id)
      .filter((e) => e.role === 'call')
      .forEach((e, i) => {
        if (!placed.has(e.to)) placed.set(e.to, { id: e.to, x: at.x + i * 30, y: at.y - 76 - i * 44 })
      })
    outs(p.id)
      .filter((e) => e.role === 'webhook')
      .forEach((e, i) => {
        if (!placed.has(e.to)) placed.set(e.to, { id: e.to, x: at.x + COL_W, y: at.y - 60 - i * 44 })
      })
  }
  for (const n of nodes) if (!placed.has(n.id)) placed.set(n.id, { id: n.id, x: 0, y: 0 })
  return placed
}

function isOutput(e: TopologyEdge) {
  return e.role === 'destination' || e.role === 'reject' || e.role === 'dead_letter' || e.role === 'override'
}

function outputOffset(e: TopologyEdge, siblings: TopologyEdge[]) {
  const order = ['destination', 'override', 'reject', 'dead_letter']
  const outputs = siblings.filter(isOutput).sort((a, b) => order.indexOf(a.role) - order.indexOf(b.role))
  const i = outputs.indexOf(e)
  return 40 + (i - (outputs.length - 1) / 2) * TOPIC_GAP
}
