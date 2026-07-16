import { createTheme, type PaletteMode, type Theme } from '@mui/material/styles'

export const STATUS_COLORS: Record<string, 'default' | 'primary' | 'secondary' | 'error' | 'info' | 'success' | 'warning'> = {
  running: 'info',
  success: 'success',
  complete: 'warning',
  cancelled: 'default',
  pending: 'secondary',
  retrying: 'warning',
  failed: 'error',
  ok: 'success',
  error: 'error',
}

const THEME_KEY = 'kafka_batch_theme'

export function readStoredMode(): PaletteMode {
  try {
    const v = localStorage.getItem(THEME_KEY)
    if (v === 'dark' || v === 'light') return v
  } catch {
    /* ignore */
  }
  if (typeof window !== 'undefined' && window.matchMedia?.('(prefers-color-scheme: dark)').matches) {
    return 'dark'
  }
  return 'light'
}

export function storeMode(mode: PaletteMode) {
  try {
    localStorage.setItem(THEME_KEY, mode)
  } catch {
    /* ignore */
  }
}

export function createAppTheme(mode: PaletteMode): Theme {
  const dark = mode === 'dark'

  return createTheme({
    palette: {
      mode,
      primary: {
        main: dark ? '#8ab4f8' : '#1a73e8',
        dark: dark ? '#aecbfa' : '#174ea6',
        light: dark ? '#d2e3fc' : '#8ab4f8',
        contrastText: dark ? '#202124' : '#ffffff',
      },
      secondary: {
        main: dark ? '#9aa0a6' : '#5f6368',
      },
      background: {
        default: dark ? '#202124' : '#f8f9fa',
        paper: dark ? '#292a2d' : '#ffffff',
      },
      text: {
        primary: dark ? '#e8eaed' : '#202124',
        secondary: dark ? '#9aa0a6' : '#5f6368',
      },
      divider: dark ? '#3c4043' : '#dadce0',
      success: { main: dark ? '#81c995' : '#188038' },
      warning: { main: dark ? '#fdd663' : '#f9ab00' },
      error: { main: dark ? '#f28b82' : '#d93025' },
      info: { main: dark ? '#8ab4f8' : '#1a73e8' },
    },
    typography: {
      fontFamily: '"Roboto", "Helvetica", "Arial", sans-serif',
      h4: { fontSize: '1.75rem', fontWeight: 400, letterSpacing: 0 },
      h5: { fontSize: '1.375rem', fontWeight: 400, letterSpacing: 0 },
      h6: { fontSize: '1.125rem', fontWeight: 500, letterSpacing: '0.0075em' },
      subtitle1: { fontWeight: 500 },
      subtitle2: { fontWeight: 500 },
      body1: { fontSize: '0.9375rem', lineHeight: 1.5 },
      body2: { fontSize: '0.875rem', lineHeight: 1.43 },
      button: { textTransform: 'none', fontWeight: 500 },
      overline: { letterSpacing: '0.08em', fontWeight: 500 },
    },
    shape: { borderRadius: 8 },
    components: {
      MuiCssBaseline: {
        styleOverrides: {
          body: { backgroundColor: dark ? '#202124' : '#f8f9fa' },
          code: {
            fontFamily: 'Roboto Mono, ui-monospace, Menlo, Monaco, Consolas, monospace',
            fontSize: '0.8125rem',
            backgroundColor: dark ? '#3c4043' : '#f1f3f4',
            padding: '1px 4px',
            borderRadius: 4,
          },
          pre: {
            fontFamily: 'Roboto Mono, ui-monospace, Menlo, Monaco, Consolas, monospace',
            fontSize: '0.8125rem',
            margin: 0,
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-word',
          },
        },
      },
      MuiAppBar: {
        defaultProps: { color: 'default', elevation: 0 },
        styleOverrides: {
          root: {
            backgroundColor: dark ? '#292a2d' : '#ffffff',
            color: dark ? '#e8eaed' : '#202124',
            borderBottom: `1px solid ${dark ? '#3c4043' : '#dadce0'}`,
          },
        },
      },
      MuiDrawer: {
        styleOverrides: {
          paper: {
            backgroundColor: dark ? '#292a2d' : '#ffffff',
            borderRight: `1px solid ${dark ? '#3c4043' : '#dadce0'}`,
          },
        },
      },
      MuiPaper: {
        defaultProps: { elevation: 0 },
        styleOverrides: {
          root: { backgroundImage: 'none' },
          outlined: { borderColor: dark ? '#3c4043' : '#dadce0' },
        },
      },
      MuiCard: {
        defaultProps: { variant: 'outlined' },
        styleOverrides: {
          root: { borderColor: dark ? '#3c4043' : '#dadce0' },
        },
      },
      MuiButton: {
        defaultProps: { disableElevation: true },
        styleOverrides: {
          root: { borderRadius: 4, textTransform: 'none', fontWeight: 500 },
          sizeSmall: { padding: '4px 12px' },
        },
      },
      MuiChip: {
        styleOverrides: {
          root: { fontWeight: 500 },
          sizeSmall: { height: 24 },
        },
      },
      MuiTableHead: {
        styleOverrides: {
          root: {
            backgroundColor: dark ? '#202124' : '#f8f9fa',
          },
        },
      },
      MuiTableCell: {
        styleOverrides: {
          head: {
            fontWeight: 500,
            fontSize: '0.75rem',
            color: dark ? '#9aa0a6' : '#5f6368',
            whiteSpace: 'nowrap',
            borderBottom: `1px solid ${dark ? '#3c4043' : '#dadce0'}`,
          },
          root: {
            borderBottom: `1px solid ${dark ? '#3c4043' : '#e8eaed'}`,
            fontSize: '0.875rem',
          },
          sizeSmall: { paddingTop: 10, paddingBottom: 10 },
        },
      },
      MuiListItemButton: {
        styleOverrides: {
          root: {
            borderRadius: 0,
            '&.Mui-selected': {
              backgroundColor: dark ? 'rgba(138,180,248,0.16)' : '#e8f0fe',
              color: dark ? '#8ab4f8' : '#174ea6',
              '& .MuiListItemIcon-root': { color: dark ? '#8ab4f8' : '#1a73e8' },
              '&:hover': { backgroundColor: dark ? 'rgba(138,180,248,0.20)' : '#e8f0fe' },
            },
          },
        },
      },
      MuiTextField: {
        defaultProps: { size: 'small', variant: 'outlined' },
      },
      MuiAlert: {
        styleOverrides: { root: { borderRadius: 8 } },
      },
      MuiToolbar: {
        styleOverrides: { root: { minHeight: 56 } },
      },
    },
  })
}

/** @deprecated use createAppTheme — kept for any stray imports during transition */
export const theme = createAppTheme('light')
