import { createContext, useContext, type AnchorHTMLAttributes, type ReactNode } from 'react'

const Navigation = createContext<(destination: string) => void>(destination => window.location.assign(destination))

export function NavigationProvider({ navigate, children }: { navigate: (destination: string) => void; children: ReactNode }) {
  return <Navigation.Provider value={navigate}>{children}</Navigation.Provider>
}

export function Link({ to, children, onClick, ...props }: AnchorHTMLAttributes<HTMLAnchorElement> & { to: string }) {
  const navigate = useContext(Navigation)
  return <a {...props} href={to} onClick={event => {
    onClick?.(event)
    if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return
    event.preventDefault()
    navigate(to)
  }}>{children}</a>
}
