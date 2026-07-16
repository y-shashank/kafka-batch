import { useCallback, useEffect, useState } from 'react'
import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import { CssBaseline, ThemeProvider } from '@mui/material'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import CircularProgress from '@mui/material/CircularProgress'
import { loadBootstrap, mountBase, type Bootstrap } from './api/client'
import { AppLayout } from './components/AppLayout'
import { theme } from './theme'
import { BatchesPage } from './pages/BatchesPage'
import { BatchDetailPage } from './pages/BatchDetailPage'
import { FailuresPage } from './pages/FailuresPage'
import { LivePage } from './pages/LivePage'
import { LagPage } from './pages/LagPage'
import { FairnessPage } from './pages/FairnessPage'
import { WeightsPage } from './pages/WeightsPage'
import { ScheduledPage } from './pages/ScheduledPage'
import { SystemPage } from './pages/SystemPage'
import { ReconcilerPage } from './pages/ReconcilerPage'
import { DeadLetterPage } from './pages/DeadLetterPage'
import { AuditPage } from './pages/AuditPage'

const LIVE_KEY = 'kafka_batch_live'

function AppRoutes({ bootstrap }: { bootstrap: Bootstrap }) {
  const [live, setLive] = useState(() => localStorage.getItem(LIVE_KEY) === '1')
  const toggleLive = useCallback(() => {
    setLive((prev) => {
      const next = !prev
      localStorage.setItem(LIVE_KEY, next ? '1' : '0')
      return next
    })
  }, [])

  return (
    <Routes>
      <Route element={<AppLayout bootstrap={bootstrap} live={live} onToggleLive={toggleLive} />}>
        <Route index element={<BatchesPage />} />
        <Route path="batches/:id" element={<BatchDetailPage />} />
        <Route path="failures" element={<FailuresPage />} />
        <Route path="live" element={<LivePage />} />
        <Route path="lag" element={<LagPage />} />
        <Route path="scheduled" element={<ScheduledPage />} />
        <Route path="reconciler" element={<ReconcilerPage />} />
        <Route path="dead_letter" element={<DeadLetterPage />} />
        <Route path="audit" element={<AuditPage />} />
        <Route path="fairness/:type" element={<FairnessPage />} />
        <Route path="fairness" element={<Navigate to="/fairness/time" replace />} />
        <Route path="weights/:type" element={<WeightsPage />} />
        <Route path="weights" element={<Navigate to="/weights/time" replace />} />
        <Route path="system" element={<SystemPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  )
}

export default function App() {
  const [bootstrap, setBootstrap] = useState<Bootstrap | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    loadBootstrap()
      .then(setBootstrap)
      .catch((e) => setError(e instanceof Error ? e.message : 'Failed to bootstrap'))
  }, [])

  if (error) {
    return (
      <ThemeProvider theme={theme}>
        <CssBaseline />
        <Box sx={{ p: 4 }}>
          <Alert severity="error">{error}</Alert>
        </Box>
      </ThemeProvider>
    )
  }

  if (!bootstrap) {
    return (
      <ThemeProvider theme={theme}>
        <CssBaseline />
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 12 }}>
          <CircularProgress />
        </Box>
      </ThemeProvider>
    )
  }

  const basename = mountBase() || '/'

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <BrowserRouter basename={basename === '/' ? undefined : basename}>
        <AppRoutes bootstrap={bootstrap} />
      </BrowserRouter>
    </ThemeProvider>
  )
}
