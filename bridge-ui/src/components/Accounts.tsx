import { useCallback, useEffect, useState } from 'react'

import { api } from '../api'
import { useT } from '../i18n'

interface Account {
  username: string
  scope: string
}

const SCOPES = ['viewer', 'operator', 'admin']

export function Accounts({ toast }: { toast: (m: string, e?: boolean) => void }) {
  const t = useT()
  const [list, setList] = useState<Account[] | null>(null)
  const [name, setName] = useState('')
  const [pass, setPass] = useState('')
  const [scope, setScope] = useState('viewer')
  const [reset, setReset] = useState<Record<string, string>>({})

  const load = useCallback(() => {
    api<Account[]>('/config/users').then(setList, () => setList(null))
  }, [])
  useEffect(load, [load])

  const save = async (username: string, body: { scope: string; password?: string }, done: string) => {
    try {
      await api(`/config/users/${encodeURIComponent(username)}`, { method: 'PUT', body })
      toast(done)
      load()
      return true
    } catch (e) {
      toast((e as Error).message, true)
      return false
    }
  }

  if (!list) return null
  return (
    <div className="card stack accounts" style={{ maxWidth: 720, gap: 14 }}>
      <div>
        <h3 style={{ margin: 0 }}>{t('acct.title')}</h3>
        <p className="small dim" style={{ margin: '4px 0 0' }}>
          {t('acct.lead')}
        </p>
      </div>
      {list.length === 0 && <div className="small dim">{t('acct.none')}</div>}
      {list.length > 0 && (
        <table className="list">
          <thead>
            <tr>
              <th>{t('acct.username')}</th>
              <th>{t('acct.scope')}</th>
              <th>{t('acct.newPassword')}</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {list.map((a) => (
              <tr key={a.username}>
                <td className="mono">{a.username}</td>
                <td>
                  <select className="select" value={a.scope} onChange={(e) => save(a.username, { scope: e.target.value }, `${a.username} · ${e.target.value}`)}>
                    {SCOPES.map((s) => (
                      <option key={s}>{s}</option>
                    ))}
                  </select>
                </td>
                <td>
                  <div className="row" style={{ gap: 6 }}>
                    <input className="input" type="password" autoComplete="new-password" value={reset[a.username] ?? ''} onChange={(e) => setReset({ ...reset, [a.username]: e.target.value })} />
                    <button
                      className="btn sm"
                      disabled={(reset[a.username] ?? '').length < 8}
                      onClick={async () => {
                        if (await save(a.username, { scope: a.scope, password: reset[a.username] }, t('acct.passwordSet'))) setReset({ ...reset, [a.username]: '' })
                      }}
                    >
                      {t('acct.setPassword')}
                    </button>
                  </div>
                </td>
                <td>
                  <button
                    className="btn ghost sm"
                    onClick={async () => {
                      if (!confirm(`${t('acct.confirmDelete')} ${a.username}?`)) return
                      try {
                        await api(`/config/users/${encodeURIComponent(a.username)}`, { method: 'DELETE' })
                        toast(`${a.username} · ${t('acct.deleted')}`)
                        load()
                      } catch (e) {
                        toast((e as Error).message, true)
                      }
                    }}
                  >
                    {t('acct.delete')}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <form
        className="row"
        style={{ gap: 8, flexWrap: 'wrap' }}
        onSubmit={async (e) => {
          e.preventDefault()
          if (await save(name.trim(), { scope, password: pass }, `${name.trim()} · ${t('acct.created')}`)) {
            setName('')
            setPass('')
          }
        }}
      >
        <input className="input" style={{ width: 180 }} placeholder={t('acct.username')} value={name} onChange={(e) => setName(e.target.value)} />
        <input className="input" style={{ width: 180 }} type="password" autoComplete="new-password" placeholder={t('acct.password')} value={pass} onChange={(e) => setPass(e.target.value)} />
        <select className="select" style={{ width: 120 }} value={scope} onChange={(e) => setScope(e.target.value)}>
          {SCOPES.map((s) => (
            <option key={s}>{s}</option>
          ))}
        </select>
        <button className="btn primary" disabled={!name.trim() || pass.length < 8}>
          {t('acct.add')}
        </button>
      </form>
    </div>
  )
}
