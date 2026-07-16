import Link from '@mui/material/Link'
import { Link as RouterLink } from 'react-router-dom'

export function MonoLink({ to, children }: { to: string; children: React.ReactNode }) {
  return (
    <Link
      component={RouterLink}
      to={to}
      underline="hover"
      sx={{
        display: 'inline-block',
        fontFamily: 'Roboto Mono, monospace',
        fontWeight: 500,
        fontSize: '0.8125rem',
      }}
    >
      {children}
    </Link>
  )
}

export const monoSx = {
  fontFamily: 'Roboto Mono, ui-monospace, Menlo, Monaco, Consolas, monospace',
  fontSize: '0.8125rem',
} as const
