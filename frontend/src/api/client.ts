export type Bootstrap = {
  ok: boolean
  csrf_token: string
  mount: string
  audit_enabled: boolean
  fairness_types: string[]
  version: string
}

declare global {
  interface Window {
    __KB_MOUNT__?: string
    __KB_CSRF__?: string
  }
}

let csrfToken = typeof window !== 'undefined' ? window.__KB_CSRF__ || '' : ''

export function mountBase(): string {
  const m = typeof window !== 'undefined' ? window.__KB_MOUNT__ || '' : ''
  return m.replace(/\/$/, '')
}

export function apiUrl(path: string): string {
  const base = mountBase()
  const p = path.startsWith('/') ? path : `/${path}`
  return `${base}${p}`
}

export function setCsrf(token: string) {
  csrfToken = token
}

export function getCsrf(): string {
  return csrfToken || window.__KB_CSRF__ || ''
}

async function parseJson<T>(res: Response): Promise<T> {
  const text = await res.text()
  try {
    return JSON.parse(text) as T
  } catch {
    throw new Error(text || res.statusText)
  }
}

export async function apiGet<T>(path: string): Promise<T> {
  const res = await fetch(apiUrl(path), {
    credentials: 'same-origin',
    headers: { Accept: 'application/json' },
  })
  if (!res.ok) {
    const body = await parseJson<{ error?: string }>(res).catch(() => ({}))
    throw new Error(body.error || `HTTP ${res.status}`)
  }
  return parseJson<T>(res)
}

export async function apiMutate<T>(
  method: 'POST' | 'PUT' | 'DELETE',
  path: string,
  body?: unknown,
): Promise<T> {
  const res = await fetch(apiUrl(path), {
    method,
    credentials: 'same-origin',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
      'X-CSRF-Token': getCsrf(),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  const data = await parseJson<T & { error?: string; ok?: boolean }>(res)
  if (!res.ok) {
    throw new Error(data.error || `HTTP ${res.status}`)
  }
  return data
}

export async function loadBootstrap(): Promise<Bootstrap> {
  const data = await apiGet<Bootstrap>('/api/bootstrap')
  if (data.csrf_token) setCsrf(data.csrf_token)
  return data
}
