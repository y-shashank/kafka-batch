import { useMemo, useState, type ReactNode } from 'react'
import { Link as RouterLink, Outlet, useLocation } from 'react-router-dom'
import AppBar from '@mui/material/AppBar'
import Box from '@mui/material/Box'
import Drawer from '@mui/material/Drawer'
import IconButton from '@mui/material/IconButton'
import List from '@mui/material/List'
import ListItemButton from '@mui/material/ListItemButton'
import ListItemIcon from '@mui/material/ListItemIcon'
import ListItemText from '@mui/material/ListItemText'
import ListSubheader from '@mui/material/ListSubheader'
import Toolbar from '@mui/material/Toolbar'
import Typography from '@mui/material/Typography'
import Button from '@mui/material/Button'
import Divider from '@mui/material/Divider'
import Stack from '@mui/material/Stack'
import useMediaQuery from '@mui/material/useMediaQuery'
import { useTheme } from '@mui/material/styles'
import MenuIcon from '@mui/icons-material/Menu'
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft'
import ChevronRightIcon from '@mui/icons-material/ChevronRight'
import ViewListOutlinedIcon from '@mui/icons-material/ViewListOutlined'
import WarningAmberOutlinedIcon from '@mui/icons-material/WarningAmberOutlined'
import ReportOutlinedIcon from '@mui/icons-material/ReportOutlined'
import SensorsOutlinedIcon from '@mui/icons-material/SensorsOutlined'
import SpeedOutlinedIcon from '@mui/icons-material/SpeedOutlined'
import ScheduleOutlinedIcon from '@mui/icons-material/ScheduleOutlined'
import EventRepeatOutlinedIcon from '@mui/icons-material/EventRepeatOutlined'
import SyncOutlinedIcon from '@mui/icons-material/SyncOutlined'
import HistoryOutlinedIcon from '@mui/icons-material/HistoryOutlined'
import InsightsOutlinedIcon from '@mui/icons-material/InsightsOutlined'
import TimerOutlinedIcon from '@mui/icons-material/TimerOutlined'
import BoltOutlinedIcon from '@mui/icons-material/BoltOutlined'
import TuneOutlinedIcon from '@mui/icons-material/TuneOutlined'
import SettingsOutlinedIcon from '@mui/icons-material/SettingsOutlined'
import SmartToyOutlinedIcon from '@mui/icons-material/SmartToyOutlined'
import FiberManualRecordIcon from '@mui/icons-material/FiberManualRecord'
import DarkModeOutlinedIcon from '@mui/icons-material/DarkModeOutlined'
import LightModeOutlinedIcon from '@mui/icons-material/LightModeOutlined'
import Tooltip from '@mui/material/Tooltip'
import type { PaletteMode } from '@mui/material/styles'
import type { Bootstrap } from '../api/client'
import { BrandMark } from './BrandMark'
import { ChatBubble } from './ChatBubble'

const DRAWER_WIDTH = 256
const DRAWER_WIDTH_COLLAPSED = 72
const NAV_COLLAPSED_KEY = 'kafka_batch_nav_collapsed'

type NavItem = {
  to: string
  label: string
  icon: ReactNode
  auditOnly?: boolean
  performanceOnly?: boolean
  aiOnly?: boolean
}

function readCollapsed(): boolean {
  try {
    return localStorage.getItem(NAV_COLLAPSED_KEY) === '1'
  } catch {
    return false
  }
}

export function AppLayout({
  bootstrap,
  live,
  onToggleLive,
  mode,
  onToggleMode,
}: {
  bootstrap: Bootstrap | null
  live: boolean
  onToggleLive: () => void
  mode: PaletteMode
  onToggleMode: () => void
}) {
  const theme = useTheme()
  const mobile = useMediaQuery(theme.breakpoints.down('md'))
  const [open, setOpen] = useState(false)
  const [collapsed, setCollapsed] = useState(() => readCollapsed())
  const location = useLocation()

  const drawerWidth = collapsed && !mobile ? DRAWER_WIDTH_COLLAPSED : DRAWER_WIDTH

  const toggleCollapsed = () => {
    setCollapsed((prev) => {
      const next = !prev
      try {
        localStorage.setItem(NAV_COLLAPSED_KEY, next ? '1' : '0')
      } catch {
        /* ignore */
      }
      return next
    })
  }

  const groups = useMemo(
    () => [
      {
        title: 'Operations',
        items: [
          { to: '/', label: 'Batches', icon: <ViewListOutlinedIcon fontSize="small" /> },
          { to: '/failures', label: 'Retries', icon: <WarningAmberOutlinedIcon fontSize="small" /> },
          { to: '/dead_letter', label: 'Dead letter', icon: <ReportOutlinedIcon fontSize="small" /> },
          { to: '/live', label: 'Consumers', icon: <SensorsOutlinedIcon fontSize="small" /> },
          { to: '/lag', label: 'Kafka lag', icon: <SpeedOutlinedIcon fontSize="small" /> },
          { to: '/scheduled', label: 'Scheduled', icon: <ScheduleOutlinedIcon fontSize="small" /> },
          { to: '/recurring', label: 'Recurring', icon: <EventRepeatOutlinedIcon fontSize="small" /> },
          { to: '/performance', label: 'Performance', icon: <InsightsOutlinedIcon fontSize="small" />, performanceOnly: true },
        ] as NavItem[],
      },
      {
        title: 'Fairness',
        items: [
          { to: '/fairness/time', label: 'Time fairness', icon: <TimerOutlinedIcon fontSize="small" /> },
          { to: '/weights/time', label: 'Time weights', icon: <TuneOutlinedIcon fontSize="small" /> },
          { to: '/fairness/throughput', label: 'Throughput fairness', icon: <BoltOutlinedIcon fontSize="small" /> },
          { to: '/weights/throughput', label: 'Throughput weights', icon: <TuneOutlinedIcon fontSize="small" /> },
        ] as NavItem[],
      },
      {
        title: 'Platform',
        items: [
          { to: '/reconciler', label: 'Reconciler', icon: <SyncOutlinedIcon fontSize="small" /> },
          { to: '/audit', label: 'Audit log', icon: <HistoryOutlinedIcon fontSize="small" />, auditOnly: true },
          { to: '/ai', label: 'AI Settings', icon: <SmartToyOutlinedIcon fontSize="small" />, aiOnly: true },
          { to: '/system', label: 'System', icon: <SettingsOutlinedIcon fontSize="small" /> },
        ] as NavItem[],
      },
    ],
    [],
  )

  const isActive = (to: string) => {
    if (to === '/') return location.pathname === '/' || location.pathname.startsWith('/batches/')
    return location.pathname === to || location.pathname.startsWith(`${to}/`)
  }

  const narrow = collapsed && !mobile

  const drawer = (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <Box sx={{ px: narrow ? 1 : 2, py: 1.5 }}>
        <Stack direction="row" spacing={narrow ? 0 : 1.5} alignItems="center" justifyContent={narrow ? 'center' : 'flex-start'}>
          <Box sx={{ width: 32, height: 32, flexShrink: 0, lineHeight: 0 }}>
            <BrandMark size={32} />
          </Box>
          {!narrow ? (
            <Box sx={{ minWidth: 0 }}>
              <Typography variant="subtitle1" noWrap sx={{ lineHeight: 1.2 }}>
                KafkaBatch
              </Typography>
              <Typography variant="caption" color="text.secondary" sx={{ display: 'block', lineHeight: 1.2, mt: 0.25 }}>
                v{bootstrap?.version || '—'}
              </Typography>
            </Box>
          ) : null}
        </Stack>
      </Box>
      <Divider />
      <Box sx={{ flex: 1, overflow: 'auto', py: 1 }}>
        {groups.map((group) => {
          const items = group.items.filter(
            (n) =>
              (!n.auditOnly || bootstrap?.audit_enabled) &&
              (!n.performanceOnly || bootstrap?.performance_metrics_enabled) &&
              (!n.aiOnly || bootstrap?.ai_enabled),
          )
          if (!items.length) return null
          return (
            <List
              key={group.title}
              dense
              subheader={
                narrow ? undefined : (
                  <ListSubheader
                    disableSticky
                    sx={{
                      bgcolor: 'transparent',
                      lineHeight: '32px',
                      typography: 'overline',
                      color: 'text.secondary',
                    }}
                  >
                    {group.title}
                  </ListSubheader>
                )
              }
            >
              {items.map((item) => {
                const button = (
                  <ListItemButton
                    key={item.to}
                    component={RouterLink}
                    to={item.to}
                    selected={isActive(item.to)}
                    onClick={() => setOpen(false)}
                    sx={{
                      mx: narrow ? 0.75 : 1,
                      mb: 0.25,
                      borderRadius: 1,
                      minHeight: 40,
                      justifyContent: narrow ? 'center' : 'flex-start',
                      px: narrow ? 1 : 2,
                    }}
                  >
                    <ListItemIcon sx={{ minWidth: narrow ? 0 : 36, justifyContent: 'center' }}>{item.icon}</ListItemIcon>
                    {!narrow ? (
                      <ListItemText
                        primary={item.label}
                        primaryTypographyProps={{ variant: 'body2', fontWeight: isActive(item.to) ? 500 : 400 }}
                      />
                    ) : null}
                  </ListItemButton>
                )
                return narrow ? (
                  <Tooltip key={item.to} title={item.label} placement="right">
                    {button}
                  </Tooltip>
                ) : (
                  button
                )
              })}
            </List>
          )
        })}
      </Box>
      {!mobile ? (
        <>
          <Divider />
          <Box sx={{ p: 1, display: 'flex', justifyContent: narrow ? 'center' : 'flex-end' }}>
            <Tooltip title={collapsed ? 'Expand navigation' : 'Shrink navigation'} placement="right">
              <IconButton
                size="small"
                onClick={toggleCollapsed}
                aria-label={collapsed ? 'Expand navigation' : 'Shrink navigation'}
              >
                {collapsed ? <ChevronRightIcon fontSize="small" /> : <ChevronLeftIcon fontSize="small" />}
              </IconButton>
            </Tooltip>
          </Box>
        </>
      ) : null}
    </Box>
  )

  return (
    <Box sx={{ display: 'flex', minHeight: '100vh', bgcolor: 'background.default' }}>
      <AppBar
        position="fixed"
        sx={{
          width: { md: `calc(100% - ${drawerWidth}px)` },
          ml: { md: `${drawerWidth}px` },
          transition: theme.transitions.create(['width', 'margin'], {
            easing: theme.transitions.easing.sharp,
            duration: theme.transitions.duration.enteringScreen,
          }),
        }}
      >
        <Toolbar>
          {mobile ? (
            <IconButton edge="start" onClick={() => setOpen(true)} sx={{ mr: 1 }} aria-label="Open navigation">
              <MenuIcon />
            </IconButton>
          ) : null}
          <Typography variant="subtitle1" sx={{ flex: 1 }} color="text.secondary">
            Operations console
          </Typography>
          <Tooltip title={mode === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}>
            <IconButton onClick={onToggleMode} aria-label="Toggle dark mode" size="small" sx={{ mr: 1 }}>
              {mode === 'dark' ? <LightModeOutlinedIcon fontSize="small" /> : <DarkModeOutlinedIcon fontSize="small" />}
            </IconButton>
          </Tooltip>
          <Button
            variant={live ? 'contained' : 'outlined'}
            color={live ? 'success' : 'inherit'}
            onClick={onToggleLive}
            size="small"
            startIcon={<FiberManualRecordIcon sx={{ fontSize: 12, color: live ? undefined : 'text.disabled' }} />}
          >
            {live ? 'Live' : 'Live off'}
          </Button>
        </Toolbar>
      </AppBar>

      <Box
        component="nav"
        sx={{
          width: { md: drawerWidth },
          flexShrink: { md: 0 },
          transition: theme.transitions.create('width', {
            easing: theme.transitions.easing.sharp,
            duration: theme.transitions.duration.enteringScreen,
          }),
        }}
      >
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
                width: drawerWidth,
                boxSizing: 'border-box',
                overflowX: 'hidden',
                transition: theme.transitions.create('width', {
                  easing: theme.transitions.easing.sharp,
                  duration: theme.transitions.duration.enteringScreen,
                }),
              },
            }}
          >
            {drawer}
          </Drawer>
        )}
      </Box>

      <Box
        component="main"
        sx={{
          flexGrow: 1,
          width: { md: `calc(100% - ${drawerWidth}px)` },
          maxWidth: '100%',
          minWidth: 0,
          px: { xs: 2, md: 3 },
          py: { xs: 2, md: 3 },
          mt: 7,
          transition: theme.transitions.create('width', {
            easing: theme.transitions.easing.sharp,
            duration: theme.transitions.duration.enteringScreen,
          }),
        }}
      >
        <Outlet />
      </Box>
      <ChatBubble enabled={!!bootstrap?.ai_enabled} />
    </Box>
  )
}
