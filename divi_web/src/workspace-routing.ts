export const workspacePaths = { home: '/inicio', insights: '/finanzas/analisis', balance: '/finanzas/balance', panorama: '/finanzas/proyeccion', groups: '/grupos', connections: '/conexiones', profile: '/perfil', settings: '/ajustes' } as const
export function legacyWorkspaceView(path: string) {
  const aliases: Record<string, 'home' | 'balance' | 'panorama' | 'groups' | 'settings'> = { '/app': 'home', '/balance': 'balance', '/panorama': 'panorama', '/actividad': 'balance', '/billetera': 'balance', '/regalos': 'groups', '/settings': 'settings' }
  return aliases[path.replace(/\/$/, '')] || null
}
export function loginDestination(requested: string | null) {
  const allowed = ['home', 'insights', 'balance', 'panorama', 'groups', 'connections', 'profile', 'settings', 'splitter', 'etl']
  return requested && allowed.includes(requested) ? requested : 'home'
}
