import { createTheme } from '@mui/material/styles'

export const theme = createTheme({
  palette: {
    mode: 'light',
    primary: { main: '#0f766e', dark: '#115e59', light: '#14b8a6', contrastText: '#fff' },
    secondary: { main: '#0369a1', dark: '#0c4a6e', light: '#0ea5e9' },
    background: { default: '#f0f4f5', paper: '#ffffff' },
    success: { main: '#059669' },
    warning: { main: '#d97706' },
    error: { main: '#dc2626' },
    info: { main: '#0284c7' },
    divider: 'rgba(15, 23, 42, 0.08)',
  },
  typography: {
    fontFamily: '"DM Sans", "Segoe UI", Roboto, Helvetica, Arial, sans-serif',
    h4: { fontWeight: 700, letterSpacing: '-0.02em' },
    h5: { fontWeight: 700, letterSpacing: '-0.02em' },
    h6: { fontWeight: 650 },
    button: { textTransform: 'none', fontWeight: 600 },
  },
  shape: { borderRadius: 12 },
  components: {
    MuiCssBaseline: {
      styleOverrides: {
        body: {
          backgroundImage:
            'radial-gradient(ellipse 80% 50% at 10% -10%, rgba(15,118,110,0.10), transparent), radial-gradient(ellipse 60% 40% at 90% 0%, rgba(3,105,161,0.08), transparent)',
          minHeight: '100vh',
        },
        code: {
          fontFamily: '"JetBrains Mono", ui-monospace, Menlo, monospace',
          fontSize: '0.85em',
        },
      },
    },
    MuiPaper: {
      defaultProps: { elevation: 0 },
      styleOverrides: {
        root: {
          border: '1px solid rgba(15, 23, 42, 0.08)',
          backgroundImage: 'none',
        },
      },
    },
    MuiTableCell: {
      styleOverrides: {
        head: {
          fontWeight: 700,
          fontSize: '0.72rem',
          textTransform: 'uppercase',
          letterSpacing: '0.06em',
          color: '#64748b',
        },
        root: { borderColor: 'rgba(15, 23, 42, 0.06)' },
      },
    },
    MuiButton: {
      styleOverrides: {
        root: { borderRadius: 10 },
      },
    },
    MuiChip: {
      styleOverrides: {
        root: { fontWeight: 600 },
      },
    },
  },
})

export const STATUS_COLORS: Record<string, string> = {
  running: '#2563eb',
  success: '#059669',
  complete: '#d97706',
  cancelled: '#64748b',
  pending: '#7c3aed',
  retrying: '#d97706',
  failed: '#dc2626',
  ok: '#059669',
  error: '#dc2626',
}
