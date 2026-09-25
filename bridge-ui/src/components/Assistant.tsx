import { Fragment, useEffect, useRef, useState, type ReactNode } from 'react'

import { api } from '../api'
import { useT } from '../i18n'
import { Icon } from './Icon'
import { Mark } from './Logo'
import { useProjects } from './ProjectsView'

interface Step {
  tool: string
  error?: string
}

interface Turn {
  role: 'user' | 'ark'
  text: string
  understood?: string
  asked?: boolean
  steps?: Step[]
  error?: boolean
}

interface Status {
  ready: boolean
  model?: string
  hint?: string
  ai_access?: string
}

// inline renders **bold** and `code` without injecting HTML.
function inline(text: string): ReactNode[] {
  const out: ReactNode[] = []
  const re = /(\*\*[^*]+\*\*|`[^`]+`)/g
  let last = 0
  let m: RegExpExecArray | null
  let i = 0
  while ((m = re.exec(text))) {
    if (m.index > last) out.push(text.slice(last, m.index))
    const tok = m[0]
    out.push(tok.startsWith('**') ? <b key={i++}>{tok.slice(2, -2)}</b> : <code key={i++}>{tok.slice(1, -1)}</code>)
    last = m.index + tok.length
  }
  if (last < text.length) out.push(text.slice(last))
  return out
}

// Markdown renders the small subset of Markdown answers use.
function Markdown({ text }: { text: string }) {
  const blocks: ReactNode[] = []
  const lines = text.split('\n')
  let list: string[] = []
  let code: string[] | null = null
  const flush = () => {
    if (list.length) {
      blocks.push(
        <ul key={blocks.length}>
          {list.map((l, i) => (
            <li key={i}>{inline(l)}</li>
          ))}
        </ul>,
      )
      list = []
    }
  }
  for (const line of lines) {
    if (line.trim().startsWith('```')) {
      if (code) {
        blocks.push(<pre key={blocks.length}>{code.join('\n')}</pre>)
        code = null
      } else {
        flush()
        code = []
      }
      continue
    }
    if (code) {
      code.push(line)
      continue
    }
    const item = /^\s*(?:[-*•]|\d+\.)\s+(.*)$/.exec(line)
    if (item) {
      list.push(item[1])
      continue
    }
    flush()
    const h = /^#{1,4}\s+(.*)$/.exec(line)
    if (h) blocks.push(<b key={blocks.length} style={{ display: 'block', marginTop: 6 }}>{inline(h[1])}</b>)
    else if (line.trim()) blocks.push(<p key={blocks.length}>{inline(line)}</p>)
  }
  flush()
  if (code) blocks.push(<pre key={blocks.length}>{(code as string[]).join('\n')}</pre>)
  return <Fragment>{blocks}</Fragment>
}

export function AssistantPanel({ onClose }: { onClose: () => void }) {
  const t = useT()
  const projects = useProjects()
  const [project, setProject] = useState('')
  const [status, setStatus] = useState<Status | null>(null)
  const [turns, setTurns] = useState<Turn[]>([])
  const [conversation, setConversation] = useState('')
  const [input, setInput] = useState('')
  const [busy, setBusy] = useState(false)
  const end = useRef<HTMLDivElement>(null)

  useEffect(() => {
    api<Status>(`/assistant/status${project ? `?project=${encodeURIComponent(project)}` : ''}`).then(setStatus, () => setStatus(null))
    setTurns([])
    setConversation('')
  }, [project])
  useEffect(() => end.current?.scrollIntoView({ behavior: 'smooth', block: 'end' }), [turns, busy])

  const send = async (text: string) => {
    if (!text.trim() || busy) return
    setTurns((cur) => [...cur, { role: 'user', text }])
    setInput('')
    setBusy(true)
    try {
      const out = await api<{ conversation_id: string; answer: string; understood?: string; asked_back: boolean; steps: Step[] }>('/assistant/chat', {
        body: { conversation_id: conversation, project, message: text },
      })
      setConversation(out.conversation_id)
      setTurns((cur) => [...cur, { role: 'ark', text: out.answer, understood: out.understood, asked: out.asked_back, steps: out.steps }])
    } catch (e) {
      setTurns((cur) => [...cur, { role: 'ark', text: (e as Error).message, error: true }])
    } finally {
      setBusy(false)
    }
  }

  const suggestions = [t('chat.s1'), t('chat.s2'), t('chat.s3'), t('chat.s4')]

  return (
    <aside className="drawer chat">
      <header>
        <div className="t">
          <h2>{t('chat.title')}</h2>
          <p>{status?.model ? `${status.model} · ${status.ai_access ?? ''}` : t('chat.sub')}</p>
        </div>
        <select className="select" style={{ width: 170 }} value={project} onChange={(e) => setProject(e.target.value)}>
          <option value="">{t('chat.allProjects')}</option>
          {projects.data?.projects.map((p) => (
            <option key={p.name} value={p.name}>
              {p.name}
            </option>
          ))}
        </select>
        <button className="btn ghost icon-btn" aria-label={t('common.close')} onClick={onClose}>
          <Icon name="close" className="" />
        </button>
      </header>
      <div className="body chat-body">
        {status && !status.ready && (
          <div className="hbox degraded">
            <p>{t('chat.notReady')}</p>
            <p className="small muted">{status.hint}</p>
          </div>
        )}
        {turns.length === 0 && (
          <div className="chat-empty">
            <div className="logo">
              <Mark />
            </div>
            <p className="muted">{t('chat.hello')}</p>
            <div className="stack" style={{ gap: 6 }}>
              {suggestions.map((s) => (
                <button key={s} className="btn ghost chat-suggest" onClick={() => send(s)}>
                  {s}
                </button>
              ))}
            </div>
          </div>
        )}
        {turns.map((turn, i) => (
          <div key={i} className={`bubble ${turn.role} ${turn.error ? 'error' : ''}`}>
            {turn.role === 'ark' && turn.understood && !turn.error && (
              <div className="understood">
                {t('chat.understood')}: {turn.understood}
              </div>
            )}
            {turn.role === 'ark' ? <Markdown text={turn.text} /> : <p>{turn.text}</p>}
            {turn.steps && turn.steps.length > 0 && (
              <div className="steps">
                {turn.steps.map((s, j) => (
                  <span key={j} className={`step ${s.error ? 'bad' : ''}`} title={s.error}>
                    {s.tool}
                  </span>
                ))}
              </div>
            )}
          </div>
        ))}
        {busy && (
          <div className="bubble ark typing">
            <i />
            <i />
            <i />
          </div>
        )}
        <div ref={end} />
      </div>
      <form
        className="chat-input"
        onSubmit={(e) => {
          e.preventDefault()
          send(input)
        }}
      >
        <textarea
          className="input"
          rows={2}
          placeholder={t('chat.placeholder')}
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
              e.preventDefault()
              send(input)
            }
          }}
        />
        <button className="btn primary" disabled={busy || !input.trim()}>
          {t('chat.send')}
        </button>
      </form>
    </aside>
  )
}
