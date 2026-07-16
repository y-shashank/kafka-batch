import { useMemo, useState } from 'react'
import { Link as RouterLink, Outlet, useLocation } from 'react-router-dom'
import AppBar from '@mui/material/AppBar'
import Box from '@mui/material/Box'
import Drawer from '@mui/material/Drawer'
import IconButton from '@mui/material/IconButton'
import List from '@mui/material/List'
import ListItemButton from '@mui/material/ListItemButton'
import ListItemIcon from '@mui/material/ListItemIcon'
import ListItemText from '@mui/material/ListItemText'
import Toolbar from '@mui/material/Toolbar'
import Typography from '@mui/material/Typography'
import Button from '@mui/material/Button'
import Divider from '@mui/material/Divider'
import useMediaQuery from '@mui/material/useMediaQuery'
import { useTheme } from '@mui/material/styles'
import MenuIcon from '@mui/icons-material/Menu'
import ViewListIcon from '@mui/icons-material/ViewList'
import WarningAmberIcon from '@mui/icons-material/WarningAmber'
import DangerousIcon from '@mui/icons-material/Dangerous'
import PlayCircleIcon from '@mui/icons-material/PlayCircle'
import SpeedIcon from '@mui/icons-material/Speed'
import ScheduleIcon from '@mui/icons-material/Schedule'
import SyncIcon from '@mui/icons-material/Sync'
import HistoryIcon from '@mui/icons-material/History'
import TimerIcon from '@mui/icons-material/Timer'
import BoltIcon from '@mui/icons-material/Bolt'
import BalanceIcon from '@mui/icons-material/Balance'
import SettingsIcon from '@mui/icons-material/Settings'
import type { Bootstrap } from '../api/client'

const DRAWER_WIDTH = 260

type NavItem = { to: string; label: string; icon: React.ReactNode; auditOnly?: boolean }

export function AppLayout({
  bootstrap,
  live,
  onToggleLive,
}: {
  bootstrap: Bootstrap | null
  live: boolean
  onToggleLive: () => void
}) {
  const theme = useTheme()
  const mobile = useMediaQuery(theme.breakpoints.down('md'))
  const [open, setOpen] = useState(false)
  const location = useLocation()

  const nav = useMemo<NavItem[]>(
    () => [
      { to: '/', label: 'Batches', icon: <ViewListIcon /> },
      { to: '/failures', label: 'Failures', icon: <WarningAmberIcon /> },
      { to: '/dead_letter', label: 'Dead letter', icon: <DangerousIcon /> },
      { to: '/live', label: 'Consumer Process', icon: <PlayCircleIcon /> },
      { to: '/lag', label: 'Kafka Lag', icon: <SpeedIcon /> },
      { to: '/scheduled', label: 'Scheduled', icon: <ScheduleIcon /> },
      { to: '/reconciler', label: 'Reconciler', icon: <SyncIcon /> },
      { to: '/audit', label: 'Audit', icon: <HistoryIcon />, auditOnly: true },
      { to: '/fairness/time', label: 'Time Fairness', icon: <TimerIcon /> },
      { to: '/weights/time', label: 'Time Weights', icon: <BalanceIcon /> },
      { to: '/fairness/throughput', label: 'Throughput Fairness', icon: <BoltIcon /> },
      { to: '/weights/throughput', label: 'Throughput Weights', icon: <BalanceIcon /> },
      { to: '/system', label: 'System', icon: <SettingsIcon /> },
    ],
    [],
  )

  const items = nav.filter((n) => !n.auditOnly || bootstrap?.audit_enabled)

  const drawer = (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column', bgcolor: '#0b1220', color: '#e2e8f0' }}>
      <Box sx={{ px: 2.5, py: 2.25, display: 'flex', alignItems: 'center', gap: 1.25 }}>
        <Box
          sx={{
            width: 28,
            height: 28,
            borderRadius: 1.5,
            background: 'linear-gradient(135deg, #0f766e, #0369a1)',
            boxShadow: '0 0 0 1px rgba(255,255,255,.12)',
          }}
        />
        <Box>
          <Typography sx={{ fontWeight: 800, letterSpacing: '-0.02em', lineHeight: 1.1 }}>KafkaBatch</Typography>
          <Typography variant="caption" sx={{ color: '#94a3b8' }}>
            v{bootstrap?.version || '—'}
          </Typography>
        </Box>
      </Box>
      <Divider sx={{ borderColor: 'rgba(148,163,184,.15)' }} />
      <List dense sx={{ flex: 1, py: 1, overflow: 'auto' }}>
        {items.map((item) => {
          const active =
            item.to === '/'
              ? location.pathname === '/' || location.pathname.startsWith('/batches/')
              : location.pathname === item.to || location.pathname.startsWith(`${item.to}/`)
          return (
            <ListItemButton
              key={item.to}
              component={RouterLink}
              to={item.to}
              selected={active}
              onClick={() => setOpen(false)}
              sx={{
                mx: 1,
                mb: 0.25,
                borderRadius: 2,
                color: active ? '#fff' : '#cbd5e1',
                '&.Mui-selected': { bgcolor: 'rgba(20,184,166,.18)' },
                '&:hover': { bgcolor: 'rgba(148,163,184,.10)' },
              }}
            >
              <ListItemIcon sx={{ minWidth: 36, color: active ? '#5eead4' : '#94a3b8' }}>{item.icon}</ListItemIcon>
              <ListItemText primary={item.label} primaryTypographyProps={{ fontSize: 13.5, fontWeight: active ? 700 : 500 }} />
            </ListItemButton>
          )
        })}
      </List>
    </Box>
  )

  return (
    <Box sx={{ display: 'flex', minHeight: '100vh' }}>
      <AppBar
        position="fixed"
        color="inherit"
        elevation={0}
        sx={{
          width: { md: `calc(100% - ${DRAWER_WIDTH}px)` },
          ml: { md: `${DRAWER_WIDTH}px` },
          bgcolor: 'rgba(255,255,255,.78)',
          backdropFilter: 'blur(12px)',
          borderBottom: '1px solid',
          borderColor: 'divider',
        }}
      >
        <Toolbar sx={{ gap: 1 }}>
          {mobile ? (
            <IconButton edge="start" onClick={() => setOpen(true)}>
              <MenuIcon />
            </IconButton>
          ) : null}
          <Typography variant="subtitle1" sx={{ flex: 1, fontWeight: 700 }}>
            Control plane
          </Typography>
          <Button
            variant={live ? 'contained' : 'outlined'}
            color={live ? 'success' : 'inherit'}
            onClick={onToggleLive}
            size="small"
          >
            {live ? '● Live' : '○ Live'}
          </Button>
        </Toolbar>
      </AppBar>

      <Box component="nav" sx={{ width: { md: DRAWER_WIDTH }, flexShrink: { md: 0 } }}>
        {mobile ? (
          <Drawer open={open} onClose={() => setOpen(false)} ModalProps={{ keepMounted: true }} sx={{ '& .MuiDrawer-paper': { width: DRAWER_WIDTH } }}>
            {drawer}
          </Drawer>
        ) : (
          <Drawer
            variant="permanent"
            open
            sx={{
              '& .MuiDrawer-paper': {
                width: DRAWER_WIDTH,
                boxSizing: 'border-box',
                borderRight: 'none',
              },
            }}
          >
            {drawer}
          </Drawer>
        )}
      </Box>

      <Box component="main" sx={{ flexGrow: 1, width: { md: `calc(100% - ${DRAWER_WIDTH}px)` }, p: { xs: 2, md: 3 }, pt: { xs: 10, md: 11 } }}>
        <Outlet />
      </Box>
    </Box>
  )
}
