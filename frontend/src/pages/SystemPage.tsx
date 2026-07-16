import { useCallback, useEffect, useState } from 'react'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Chip from '@mui/material/Chip'
import Divider from '@mui/material/Divider'
import Grid from '@mui/material/Grid'
import Stack from '@mui/material/Stack'
import Typography from '@mui/material/Typography'
import { apiGet } from '../api/client'
import { LoadingBlock } from '../components/LoadingBlock'
import { PageHeader } from '../components/PageHeader'
import { SectionCard } from '../components/SectionCard'
import { monoSx } from '../components/MonoLink'
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
      <PageHeader title="System" subtitle="Read-only configuration snapshot. Passwords and secrets are masked." />
      {data.reconciler_last_ran_at ? (
        <Alert severity="info" sx={{ mb: 2 }}>
          Reconciler last ran {data.reconciler_age} ({data.reconciler_last_ran_at})
        </Alert>
      ) : null}
      <Grid container spacing={2}>
        {data.sections.map((section: any) => (
          <Grid key={section.id} size={{ xs: 12, md: section.wide ? 12 : 6 }}>
            <SectionCard title={section.title}>
              <Stack divider={<Divider flexItem />} spacing={1.25}>
                {section.rows.map((r: any) => (
                  <Stack
                    key={r.label}
                    direction={{ xs: 'column', sm: 'row' }}
                    justifyContent="space-between"
                    spacing={0.75}
                    alignItems={{ sm: 'flex-start' }}
                  >
                    <Typography variant="body2" color="text.secondary" sx={{ minWidth: 140, flexShrink: 0 }}>
                      {r.label}
                    </Typography>
                    <Box sx={{ textAlign: { sm: 'right' }, minWidth: 0, maxWidth: '100%' }}>
                      {r.masked ? (
                        <Chip size="small" color="warning" variant="outlined" label={r.value} />
                      ) : (
                        <Typography variant="body2" sx={{ ...monoSx, wordBreak: 'break-word' }}>
                          {r.value}
                        </Typography>
                      )}
                    </Box>
                  </Stack>
                ))}
              </Stack>
            </SectionCard>
          </Grid>
        ))}
      </Grid>
    </Box>
  )
}
