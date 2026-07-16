import { useCallback, useEffect, useState } from 'react'
import { Link as RouterLink, useSearchParams } from 'react-router-dom'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Chip from '@mui/material/Chip'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import Typography from '@mui/material/Typography'
import { apiGet } from '../api/client'
import { EmptyState } from '../components/EmptyState'
import { LoadingBlock } from '../components/LoadingBlock'
import { MetricCards } from '../components/MetricCards'
import { PageHeader } from '../components/PageHeader'
import { useLiveRefresh } from '../hooks/useLiveRefresh'

export function DeadLetterPage() {
  const [params, setParams] = useSearchParams()
  const type = params.get('type') || ''
  const before = params.get('before') || ''
  const [data, setData] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    try {
      const qs = new URLSearchParams()
      if (type) qs.set('type', type)
      if (before) qs.set('before', before)
      setData(await apiGet(`/api/dead_letter?${qs}`))
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load')
    }
  }, [type, before])

  useEffect(() => {
    void load()
  }, [load])
  useLiveRefresh(load)

  if (!data && !error) return <LoadingBlock />
  if (error) return <Alert severity="error">{error}</Alert>
  if (!data.available) return <EmptyState title="Dead letter topic" message={data.message} />

  const byType = data.stats.by_type || {}
  const chips = [
    { label: `All (${Object.values(byType).reduce((a: number, b: any) => a + Number(b || 0), 0)})`, value: '' },
    ...Object.entries(byType)
      .filter(([t, n]) => Number(n) > 0 || type === t)
      .map(([t, n]) => ({ label: `${t} (${n})`, value: t })),
  ]

  return (
    <Box>
      <Button component={RouterLink} to="/" sx={{ mb: 1 }}>
        ← All batches
      </Button>
      <PageHeader title="Dead letter" subtitle={`Topic ${data.stats.topic}. Newest first.`} />
      <MetricCards
        metrics={[
          { label: 'Total sampled', value: data.stats.total },
          { label: 'Partitions', value: data.stats.partitions },
          { label: 'Sample size', value: data.stats.sample_size },
        ]}
      />
      <Stack direction="row" spacing={1} sx={{ mb: 2 }} flexWrap="wrap" useFlexGap>
        {chips.map((c) => (
          <Chip
            key={c.label}
            label={c.label}
            clickable
            color={type === c.value ? 'primary' : 'default'}
            onClick={() => setParams(c.value ? { type: c.value } : {})}
          />
        ))}
      </Stack>
      <Paper sx={{ overflow: 'auto' }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>When</TableCell>
              <TableCell>Type</TableCell>
              <TableCell>Worker</TableCell>
              <TableCell>Batch / Job</TableCell>
              <TableCell>Source</TableCell>
              <TableCell>Error</TableCell>
              <TableCell>Part:Off</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {data.messages.length === 0 ? (
              <TableRow>
                <TableCell colSpan={7} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No dead-letter messages.
                </TableCell>
              </TableRow>
            ) : (
              data.messages.map((m: any, i: number) => (
                <TableRow key={`${m.partition}:${m.offset}:${i}`}>
                  <TableCell>{m.dlt_at_label}</TableCell>
                  <TableCell>
                    <Chip size="small" label={m.dlt_type} />
                  </TableCell>
                  <TableCell sx={{ fontFamily: 'JetBrains Mono, monospace' }}>{m.worker_class}</TableCell>
                  <TableCell>
                    {m.batch_id ? (
                      <Typography component={RouterLink} to={`/batches/${m.batch_id}`} sx={{ fontFamily: 'JetBrains Mono, monospace', textDecoration: 'none' }}>
                        {String(m.batch_id).slice(0, 8)}
                      </Typography>
                    ) : (
                      '—'
                    )}{' '}
                    / <span style={{ fontFamily: 'JetBrains Mono, monospace' }}>{m.job_id ? String(m.job_id).slice(0, 8) : '—'}</span>
                  </TableCell>
                  <TableCell sx={{ fontFamily: 'JetBrains Mono, monospace' }}>{m.source_topic}</TableCell>
                  <TableCell sx={{ fontFamily: 'JetBrains Mono, monospace' }}>{m.error}</TableCell>
                  <TableCell sx={{ fontFamily: 'JetBrains Mono, monospace' }}>
                    {m.partition}:{m.offset}
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </Paper>
      {data.has_older ? (
        <Stack direction="row" justifyContent="center" sx={{ mt: 2 }}>
          <Button
            onClick={() => {
              const next: Record<string, string> = { before: data.cursor_older }
              if (type) next.type = type
              setParams(next)
            }}
          >
            Older →
          </Button>
        </Stack>
      ) : null}
    </Box>
  )
}
