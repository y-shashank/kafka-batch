import { useCallback, useEffect, useState } from 'react'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import Typography from '@mui/material/Typography'
import Chip from '@mui/material/Chip'
import { apiGet } from '../api/client'
import { LoadingBlock } from '../components/LoadingBlock'
import { PageHeader } from '../components/PageHeader'
import { useLiveRefresh } from '../hooks/useLiveRefresh'

export function SystemPage() {
  const [data, setData] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)
  const load = useCallback(async () => {
    try {
      setData(await apiGet('/api/system'))
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load')
    }
  }, [])
  useEffect(() => {
    void load()
  }, [load])
  useLiveRefresh(load)

  if (!data && !error) return <LoadingBlock />
  if (error) return <Alert severity="error">{error}</Alert>

  return (
    <Box>
      <PageHeader title="System" subtitle="Read-only view of the active KafkaBatch configuration. Passwords and secrets are masked." />
      {data.reconciler_last_ran_at ? (
        <Alert severity="info" sx={{ mb: 2 }}>
          Reconciler last ran {data.reconciler_age} ({data.reconciler_last_ran_at})
        </Alert>
      ) : null}
      <Box
        sx={{
          display: 'grid',
          gap: 2,
          gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' },
        }}
      >
        {data.sections.map((section: any) => (
          <Paper
            key={section.id}
            sx={{
              p: 2,
              gridColumn: section.wide ? { md: '1 / -1' } : undefined,
              borderLeft: `4px solid ${section.accent || '#0f766e'}`,
            }}
          >
            <Stack direction="row" spacing={1} alignItems="center" sx={{ mb: 1.5 }}>
              <Typography aria-hidden>{section.icon}</Typography>
              <Typography variant="h6">{section.title}</Typography>
            </Stack>
            <Stack spacing={1}>
              {section.rows.map((r: any) => (
                <Stack key={r.label} direction={{ xs: 'column', sm: 'row' }} justifyContent="space-between" spacing={0.5}>
                  <Typography variant="body2" color="text.secondary" sx={{ minWidth: 160 }}>
                    {r.label}
                  </Typography>
                  <Typography
                    variant="body2"
                    sx={{
                      fontFamily: 'JetBrains Mono, monospace',
                      textAlign: { sm: 'right' },
                      wordBreak: 'break-all',
                      color: r.masked ? 'warning.main' : 'text.primary',
                    }}
                  >
                    {r.masked ? <Chip size="small" color="warning" label={r.value} /> : r.value}
                  </Typography>
                </Stack>
              ))}
            </Stack>
          </Paper>
        ))}
      </Box>
    </Box>
  )
}
