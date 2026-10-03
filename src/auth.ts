export interface Access {
  userId: string
  name: string
  role: 'OWNER' | 'EDITOR' | 'VIEWER'
  clientId: number
  expiresAt: number
}

export function createAuthorizer(apiUrl: string, secret: string) {
  if (secret.length < 32) throw new Error('NEXTJS_INTERNAL_SECRET must contain at least 32 random characters')
  return async (roomId: string, token: string): Promise<Access | null> => {
    try {
      const response = await fetch(`${apiUrl.replace(/\/$/, '')}/api/rooms/${encodeURIComponent(roomId)}/collab/authorize`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-internal-secret': secret },
        body: JSON.stringify({ token }),
        signal: AbortSignal.timeout(3000),
        redirect: 'error',
      })
      if (!response.ok) return null
      const access = await response.json() as Access
      if (!access || typeof access.userId !== 'string' || !access.userId || typeof access.name !== 'string' ||
        !['OWNER', 'EDITOR', 'VIEWER'].includes(access.role) || !Number.isSafeInteger(access.clientId) ||
        access.clientId < 0 || access.clientId > 0xffffffff || !Number.isSafeInteger(access.expiresAt) ||
        access.expiresAt <= Date.now()) return null
      return access
    } catch {
      // Auth service failures never fall back to anonymous access or a stale role.
      return null
    }
  }
}
