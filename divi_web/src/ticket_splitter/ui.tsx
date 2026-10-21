import { cloneElement, isValidElement, type ButtonHTMLAttributes, type HTMLAttributes, type ReactElement } from 'react'

type ButtonProps = ButtonHTMLAttributes<HTMLButtonElement> & {
  asChild?: boolean
  variant?: 'default' | 'outline' | 'ghost'
  size?: 'default' | 'lg' | 'icon'
}

export function Button({ asChild, children, variant = 'default', size = 'default', className = '', type = 'button', ...props }: ButtonProps) {
  const classes = `classic-button classic-button-${variant} classic-button-${size} ${className}`
  if (asChild && isValidElement(children)) {
    const child = children as ReactElement<{ className?: string }>
    return cloneElement(child, { ...props, className: `${classes} ${child.props.className || ''}` })
  }
  return <button type={type} className={classes} {...props}>{children}</button>
}

export function Card({ className = '', ...props }: HTMLAttributes<HTMLDivElement>) {
  return <div className={`classic-card ${className}`} {...props} />
}
export function CardContent({ className = '', ...props }: HTMLAttributes<HTMLDivElement>) {
  return <div className={`classic-card-content ${className}`} {...props} />
}
