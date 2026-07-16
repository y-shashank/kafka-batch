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
import { LoadingBlock } from '../components/LoadingBlock'
import { MetricCards } from '../components/MetricCards'
import { PageHeader } from '../components/PageHeader'
import { StatusChip } from '../components/StatusChip'
import { useLiveRefresh } from '../hooks/useLiveRefresh'

export function FailuresPage() {
  const [params, setParams] = useSearchParams()
  const status = params.get('status') || 'retrying'
  const page = Math.max(1, Number(params.get('page') || 1))
  const [data, setData] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    try {
      const qs = new URLSearchParams({ status, page: String(page) })
      setData(await apiGet(`/api/failures?${qs}`))
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load')
    }
  }, [status, page])

  useEffect(() => {
    void load()
  }, [load])
  useLiveRefresh(load)

  if (!data && !error) return <LoadingBlock />
  if (error) return <Alert severity="error">{error}</Alert>

  const tiers = data.retry_lag_by_tier || {}
  const metrics = [
    { label: 'Pending retries (all)', value: data.retry_lag_total ?? '—' },
    ...Object.entries(tiers).map(([tier, lag]) => ({ label: `${tier} tier`, value: lag as number })),
  ]

  return (
    <Box>
      <Button component={RouterLink} to="/" sx={{ mb: 1 }}>
        ← All batches
      </Button>
      <PageHeader title="Failures" subtitle="Retrying and exhausted job failures across all batches." />
      {data.retry_lag_by_tier ? <MetricCards metrics={metrics} /> : null}
      <Stack direction="row" spacing={1} sx={{ mb: 2 }}>
        {['retrying', 'failed'].map((s) => (
          <Chip
            key={s}
            label={s === 'retrying' ? 'Retrying' : 'Failed'}
            clickable
            color={status === s ? 'primary' : 'default'}
            onClick={() => setParams({ status: s })}
          />
        ))}
      </Stack>
      <Paper sx={{ overflow: 'auto' }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Batch</TableCell>
              <TableCell>Job</TableCell>
              <TableCell>Worker</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Attempt</TableCell>
              <TableCell>Next retry</TableCell>
              <TableCell>Error</TableCell>
              <TableCell>Message</TableCell>
              <TableCell>Failed at</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {data.failures.length === 0 ? (
              <TableRow>
                <TableCell colSpan={9} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No failures recorded.
                </TableCell>
              </TableRow>
            ) : (
              data.failures.map((f: any) => (
                <TableRow key={`${f.batch_id}-${f.job_id}`}>
                  <TableCell>
                    <Typography component={RouterLink} to={`/batches/${f.batch_id}`} sx={{ fontFamily: 'JetBrains Mono, monospace', textDecoration: 'none' }}>
                      {String(f.batch_id).slice(0, 8)}
                    </Typography>
                  </TableCell>
                  <TableCell sx={{ fontFamily: 'JetBrains Mono, monospace' }}>{String(f.job_id).slice(0, 8)}</TableCell>
                  <TableCell>{f.worker_class}</TableCell>
                  <TableCell>
                    <StatusChip status={f.status} />
                  </TableCell>
                  <TableCell>{f.attempt}</TableCell>
                  <TableCell>{f.next_retry_eta || '—'}</TableCell>
                  <TableCell sx={{ color: 'error.main', fontWeight: 600 }}>{f.error_class}</TableCell>
                  <TableCell>{f.error_message}</TableCell>
                  <TableCell>{f.failed_at_label}</TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </Paper>
      <Stack direction="row" spacing={1} justifyContent="center" sx={{ mt: 2 }}>
        <Button disabled={page <= 1} onClick={() => setParams({ status, page: String(page - 1) })}>
          ← Prev
        </Button>
        <Typography variant="body2" color="text.secondary" sx={{ alignSelf: 'center' }}>
          Page {page}
        </Typography>
        <Button disabled={!data.has_next} onClick={() => setParams({ status, page: String(page + 1) })}>
          Next →
        </Button>
      </Stack>
    </Box>
  )
}
