import { useState } from 'react'

import { api, ApiError, type ApplyOut, type ValidationOut } from '../api'
import { useT } from '../i18n'

// ConfigEditor edits one pipeline as YAML with the same two-step flow as
// the MCP config tools: preview shows the diff and warnings, and only an
// explicit confirm applies it.
export function ConfigEditor({ initial, onApplied }: { initial: string; onApplied: () => void }) {
  const t = useT()
  const [yaml, setYaml] = useState(initial)
  const [result, setResult] = useState<ValidationOut | null>(null)
  const [pending, setPending] = useState<ApplyOut | null>(null)
  const [message, setMessage] = useState<{ text: string; error?: boolean } | null>(null)
  const [busy, setBusy] = useState(false)

  const fail = (e: unknown) => setMessage({ text: e instanceof ApiError && e.status === 403 ? t('common.noAccess') : (e as Error).message, error: true })

  const validate = async () => {
    setBusy(true)
    setMessage(null)
    setPending(null)
    try {
      setResult(await api<ValidationOut>('/config/validate', { body: { yaml } }))
    } catch (e) {
      fail(e)
    } finally {
      setBusy(false)
    }
  }

  const preview = async () => {
    setBusy(true)
    setMessage(null)
    try {
      const out = await api<ApplyOut>('/config/preview', { body: { yaml } })
      setResult(out.preview ?? null)
      if (out.state === 'awaiting_confirmation') setPending(out)
      else if (out.state === 'unchanged') setMessage({ text: t('cfg.unchanged') })
    } catch (e) {
      fail(e)
    } finally {
      setBusy(false)
    }
  }

  const confirm = async () => {
    if (!pending?.confirm_token) return
    setBusy(true)
    try {
      await api('/config/confirm', { body: { confirm_token: pending.confirm_token } })
      setPending(null)
      setMessage({ text: t('cfg.applied') })
      onApplied()
    } catch (e) {
      fail(e)
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="stack">
      <textarea
        className="textarea"
        spellCheck={false}
        value={yaml}
        onChange={(e) => {
          setYaml(e.target.value)
          setPending(null)
        }}
        onKeyDown={(e) => {
          if (e.key === 'Tab') {
            e.preventDefault()
            const el = e.currentTarget
            const { selectionStart: s, selectionEnd: end } = el
            setYaml(yaml.slice(0, s) + '  ' + yaml.slice(end))
            requestAnimationFrame(() => el.setSelectionRange(s + 2, s + 2))
          }
        }}
      />
      <div className="row">
        <button className="btn" disabled={busy} onClick={validate}>
          {t('cfg.validate')}
        </button>
        <button className="btn" disabled={busy} onClick={preview}>
          {t('cfg.preview')}
        </button>
        {pending && (
          <>
            <button className="btn primary" disabled={busy} onClick={confirm}>
              {t('cfg.confirm')}
            </button>
            <button className="btn ghost" disabled={busy} onClick={() => setPending(null)}>
              {t('cfg.cancel')}
            </button>
          </>
        )}
      </div>
      {message && <p className={message.error ? 'err' : 'ok'}>{message.text}</p>}
      {result && (
        <div className="preview stack">
          <div className="row">
            <b className={result.valid ? 'ok' : 'err'}>{result.valid ? t('cfg.valid') : t('cfg.invalid')}</b>
            <span className="muted small">
              {result.change} · {t('cfg.appliesTo')}: {result.applies_to}
            </span>
          </div>
          {(result.errors ?? []).map((e, i) => (
            <div key={i} className="err small">
              {e}
            </div>
          ))}
          {result.diff && result.diff.length > 0 && (
            <div>
              <div className="small dim">{t('cfg.diff')}</div>
              <ul className="diff">
                {result.diff.map((d, i) => (
                  <li key={i}>{d}</li>
                ))}
              </ul>
            </div>
          )}
          {result.warnings && result.warnings.length > 0 && (
            <div className="stack">
              <div className="small dim">{t('cfg.warnings')}</div>
              {result.warnings.map((w, i) => (
                <div key={i} className={`finding ${w.severity}`}>
                  <div className="what">{w.what}</div>
                  {w.why && <div className="why">{w.why}</div>}
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
