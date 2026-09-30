import { useEffect, useState } from 'react'
import type { IconType } from 'react-icons'
import { FaAws, FaMicrosoft, FaWindows } from 'react-icons/fa'
import { SiAuth0, SiBitbucket, SiGithub, SiGitlab, SiGoogle, SiKeycloak, SiOkta } from 'react-icons/si'
import { TbShieldLock } from 'react-icons/tb'

import { api, setToken } from '../api'
import { translate, type Key, type Lang } from '../i18n'
import { Wordmark } from './Logo'

export interface AuthInfo {
  tokens: boolean
  password: boolean
  jwt: boolean
  anonymous?: string
  sign_in_with: { kind: string; name: string; url: string }[]
}

const PROVIDERS: Record<string, { label: string; logo: IconType }> = {
  azure: { label: 'Microsoft Entra ID', logo: FaMicrosoft },
  adfs: { label: 'ADFS', logo: FaWindows },
  google: { label: 'Google', logo: SiGoogle },
  keycloak: { label: 'Keycloak', logo: SiKeycloak },
  okta: { label: 'Okta', logo: SiOkta },
  auth0: { label: 'Auth0', logo: SiAuth0 },
  cognito: { label: 'AWS Cognito', logo: FaAws },
  github: { label: 'GitHub', logo: SiGithub },
  gitlab: { label: 'GitLab', logo: SiGitlab },
  bitbucket: { label: 'Bitbucket', logo: SiBitbucket },
  saml: { label: 'SAML', logo: TbShieldLock },
  sso: { label: 'SSO', logo: TbShieldLock },
}

export function SignIn({ lang, onDone }: { lang: Lang; onDone: () => void }) {
  const t = (k: Key) => translate(lang, k)
  const [info, setInfo] = useState<AuthInfo | null>(null)
  const [user, setUser] = useState('')
  const [pass, setPass] = useState('')
  const [token, setTok] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState(() => new URLSearchParams(location.search).get('signin_error') ?? '')

  useEffect(() => {
    api<AuthInfo>('/auth/info').then(setInfo, () => setInfo({ tokens: true, password: false, jwt: false, sign_in_with: [] }))
    if (location.search.includes('signin_error')) history.replaceState(null, '', '/')
  }, [])

  const run = async (fn: () => Promise<unknown>, bad: Key) => {
    setBusy(true)
    setError('')
    try {
      await fn()
      await api('/overview')
      onDone()
    } catch (e) {
      setError(e instanceof Error && e.message ? e.message : t(bad))
    } finally {
      setBusy(false)
    }
  }

  const nothing = info && !info.tokens && !info.password && !info.jwt && info.sign_in_with.length === 0
  return (
    <div className="login">
      <div className="login-box stack">
        <Wordmark className="word" />
        <h1>{t('login.title')}</h1>
        {error && <p className="err">{error}</p>}
        {info?.sign_in_with.map((p) => {
          const meta = PROVIDERS[p.name] ?? { label: p.name, logo: TbShieldLock }
          const Logo = meta.logo
          return (
            <a key={p.url} className="btn sso" href={p.url}>
              <Logo aria-hidden /> {t('login.with')} {meta.label}
            </a>
          )
        })}
        {info?.password && (
          <form
            className="stack"
            onSubmit={(e) => {
              e.preventDefault()
              run(() => api('/auth/login', { body: { username: user.trim(), password: pass } }), 'login.badPassword')
            }}
          >
            {(info.sign_in_with.length > 0) && <div className="login-or">{t('login.or')}</div>}
            <input className="input" autoComplete="username" placeholder={t('login.username')} value={user} onChange={(e) => setUser(e.target.value)} />
            <input className="input" type="password" autoComplete="current-password" placeholder={t('login.password')} value={pass} onChange={(e) => setPass(e.target.value)} />
            <button className="btn primary" disabled={busy || !user.trim() || !pass}>
              {t('login.submit')}
            </button>
          </form>
        )}
        {(info?.tokens || info?.jwt) && (
          <form
            className="stack"
            onSubmit={(e) => {
              e.preventDefault()
              run(async () => setToken(token.trim()), 'login.bad')
            }}
          >
            {(info.password || info.sign_in_with.length > 0) && <div className="login-or">{t('login.orToken')}</div>}
            <input className="input mono" type="password" autoComplete="off" placeholder={t('login.token')} value={token} onChange={(e) => setTok(e.target.value)} />
            <button className="btn" disabled={busy || !token.trim()}>
              {t('login.useToken')}
            </button>
          </form>
        )}
        {nothing && <p className="small dim">{t('login.localOnly')}</p>}
      </div>
    </div>
  )
}
