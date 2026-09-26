// Client for the ARK engine REST API. The console server proxies /api to
// the engine, so every call is same-origin; the token is kept for this
// browser tab only.

const TOKEN_KEY = 'ark.token'

function readToken(): string {
  try {
    return sessionStorage.getItem(TOKEN_KEY) ?? ''
  } catch {
    return ''
  }
}

let token = readToken()

export function getToken() {
  return token
}

export function setToken(t: string) {
  token = t
  try {
    if (t) sessionStorage.setItem(TOKEN_KEY, t)
    else sessionStorage.removeItem(TOKEN_KEY)
  } catch {
    // storage blocked: keep it in memory only
  }
}

export class ApiError extends Error {
  status: number
  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

function headers(json: boolean): HeadersInit {
  const h: Record<string, string> = {}
  if (token) h.Authorization = `Bearer ${token}`
  if (json) h['Content-Type'] = 'application/json'
  return h
}

export async function api<T>(path: string, opts: { method?: string; body?: unknown; signal?: AbortSignal } = {}): Promise<T> {
  const res = await fetch(`/api/v1${path}`, {
    method: opts.method ?? (opts.body !== undefined ? 'POST' : 'GET'),
    headers: headers(opts.body !== undefined),
    body: opts.body !== undefined ? JSON.stringify(opts.body) : undefined,
    signal: opts.signal,
  })
  const text = await res.text()
  let data: unknown = undefined
  try {
    data = text ? JSON.parse(text) : undefined
  } catch {
    data = text
  }
  if (!res.ok) {
    const msg = (data && typeof data === 'object' && 'error' in data ? String((data as { error: unknown }).error) : '') || `${res.status} ${res.statusText}`
    throw new ApiError(res.status, msg)
  }
  return data as T
}

// streamEvents reads a Server-Sent Events response with the auth header
// (EventSource can't send one) and calls onEvent for each event.
export async function streamEvents(path: string, signal: AbortSignal, onEvent: (event: string, data: string) => void): Promise<void> {
  const res = await fetch(`/api/v1${path}`, { headers: headers(false), signal })
  if (!res.ok || !res.body) {
    throw new ApiError(res.status, (await res.text()) || res.statusText)
  }
  const reader = res.body.pipeThrough(new TextDecoderStream()).getReader()
  let buf = ''
  for (;;) {
    const { value, done } = await reader.read()
    if (done) return
    buf += value
    let sep: number
    while ((sep = buf.indexOf('\n\n')) >= 0) {
      const block = buf.slice(0, sep)
      buf = buf.slice(sep + 2)
      let event = 'message'
      const data: string[] = []
      for (const line of block.split('\n')) {
        if (line.startsWith('event: ')) event = line.slice(7)
        else if (line.startsWith('data: ')) data.push(line.slice(6))
      }
      onEvent(event, data.join('\n'))
    }
  }
}

// clock returns the wall-clock part of an ISO 8601 time as sent by the
// engine, which drops trailing zeros from the fraction.
export function clock(iso: string, millis = false): string {
  const m = /T(\d{2}:\d{2}:\d{2})(?:\.(\d+))?/.exec(iso)
  if (!m) return iso
  return millis ? `${m[1]}.${(m[2] ?? '').padEnd(3, '0').slice(0, 3)}` : m[1]
}

export type Health = 'healthy' | 'degraded' | 'down' | 'paused'

export interface PipelineStats {
  enabled: boolean
  local_workers: number
  running: number
  paused: boolean
  breaker_state?: string
  processed: number
  rejected: number
  dead_lettered: number
  failed: number
  lag: number
  avg_callback_ms: number
}

export interface TopologyNode {
  id: string
  kind: 'topic' | 'pipeline' | 'target' | 'webhook'
  label: string
  stats?: PipelineStats
}

export interface TopologyEdge {
  from: string
  to: string
  role: 'consume' | 'call' | 'destination' | 'reject' | 'dead_letter' | 'override' | 'webhook'
  rule?: string
}

export interface Topology {
  nodes: TopologyNode[] | null
  edges: TopologyEdge[] | null
}

export interface Overview {
  time: string
  timezone: string
  pipelines: { name: string; health: Health; summary: string; lag: number; pending_dlq: number }[] | null
  needs_attention: string[] | null
  cluster?: ClusterStatus
}

export interface ClusterStatus {
  node_id: string
  cluster: string
  leader: boolean
  live_nodes: string[]
  config_version: number
  node_config_versions: Record<string, number>
}

export interface Finding {
  severity: 'ok' | 'info' | 'warning' | 'critical'
  what: string
  why?: string
  suggested_actions?: string[]
}

export interface Diagnosis {
  pipeline: string
  health: Health
  summary: string
  findings: Finding[] | null
  numbers: {
    local_workers: number
    processed: number
    rejected: number
    dead_lettered: number
    failed_attempts: number
    lag: number
    oldest_uncommitted_seconds: number
    avg_callback_ms: number
    breaker_state?: string
    paused: boolean
    last_activity_at?: string
    pending_dlq_entries: number
    pending_reject_entries: number
  }
}

export interface ArkEvent {
  time: string
  pipeline?: string
  kind: string
  message: string
  details?: Record<string, string>
}

export interface DLQEntry {
  id: string
  redrives: number
  reason?: string
  failed_at?: string
  correlation_id?: string
  key?: string
  value: string
  timestamp: string
  partition: number
  offset: number
}

export interface TapRecord {
  time: string
  pipeline: string
  stage: 'in' | 'callback' | 'out'
  to?: string
  topic?: string
  partition?: number
  offset?: number
  correlation_id?: string
  key?: string
  headers?: Record<string, string>
  value?: string
  truncated?: boolean
  target?: string
  status?: number
  attempt?: number
  duration_ms?: number
  rule?: string
  reason?: string
}

export interface ValidationOut {
  valid: boolean
  change: string
  errors?: string[]
  warnings?: Finding[]
  diff?: string[]
  normalized_yaml?: string
  applies_to: string
}

export interface ApplyOut {
  state: 'awaiting_confirmation' | 'invalid' | 'unchanged' | 'applied'
  preview?: ValidationOut
  confirm_token?: string
}

export interface PipelineConfig {
  name: string
  yaml: string
  applies_to: string
}

export interface WorkerStatus {
  pipeline: string
  worker: number
  running: boolean
  paused: boolean
  breaker_state: string
  processed: number
  lag: number
  consumer_group: string
}
