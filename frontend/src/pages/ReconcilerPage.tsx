import { useCallback, useEffect, useState } from 'react'
import { Link as RouterLink } from 'react-router-dom'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Paper from '@mui/material/Paper'
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
import { useLiveRefresh } from '../hooks/useLiveRefresh'

export function ReconcilerPage() {
  const [data, setData] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)
  const load = useCallback(async () => {
    try {
      setData(await apiGet('/api/reconciler'))
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
  const last = data.last

  return (
    <Box>
      <Button component={RouterLink} to="/" sx={{ mb: 1 }}>
        ← All batches
      </Button>
      <PageHeader title="Reconciler" subtitle="Last sweep that recovered stuck batches and refired lost callbacks." />
      {!last ? (
        <Alert severity="info" sx={{ mb: 2 }}>
          No reconciler run recorded yet. Runs automatically every {data.reconciliation_interval}s or via rake kafka_batch:reconcile.
        </Alert>
      ) : (
        <MetricCards
          metrics={[
            { label: 'Last run', value: String(last.ran_at || '—') },
            { label: 'Triggered by', value: String(last.triggered_by || '—') },
            { label: 'Duration', value: `${last.duration ?? '—'}s` },
            { label: 'Stuck recovered', value: last.recovered_stale ?? 0 },
            { label: 'Callbacks refired', value: last.refired_lost ?? 0 },
            { label: 'Produce failures', value: last.produce_failed ?? 0 },
          ]}
        />
      )}
      {data.skip?.at ? (
        <Alert severity="warning" sx={{ mb: 2 }}>
          Last lock skip: {data.skip.at} ({data.skip.reason})
        </Alert>
      ) : null}
      {last ? (
        <Paper sx={{ p: 2, mb: 2 }}>
          <Typography variant="h6" sx={{ mb: 1 }}>
            Last sweep
          </Typography>
          <Table size="small">
            <TableBody>
              <TableRow>
                <TableCell sx={{ width: 280, color: 'text.secondary' }}>Stuck-running found / processed</TableCell>
                <TableCell>
                  {last.found_stale}/{last.processed_stale}
                  {String(last.capped_stale) === '1' ? ' (capped)' : ''}
                </TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ color: 'text.secondary' }}>Lost-callback found / processed</TableCell>
                <TableCell>
                  {last.found_lost}/{last.processed_lost}
                  {String(last.capped_lost) === '1' ? ' (capped)' : ''}
                </TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ color: 'text.secondary' }}>Skipped</TableCell>
                <TableCell>{last.skipped_stale}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ color: 'text.secondary' }}>Interval / max per run</TableCell>
                <TableCell>
                  {data.reconciliation_interval}s / {data.max_reconcile_per_run}
                </TableCell>
              </TableRow>
            </TableBody>
          </Table>
        </Paper>
      ) : null}
      <Paper sx={{ p: 2, overflow: 'auto' }}>
        <Typography variant="h6" sx={{ mb: 1 }}>
          Last run detail
        </Typography>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Batch</TableCell>
              <TableCell>Action</TableCell>
              <TableCell>Outcome</TableCell>
              <TableCell>Total</TableCell>
              <TableCell>Failed</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {(data.details || []).length === 0 ? (
              <TableRow>
                <TableCell colSpan={5} align="center" sx={{ py: 3, color: 'text.secondary' }}>
                  No per-batch actions on the last run.
                </TableCell>
              </TableRow>
            ) : (
              data.details.map((d: any, i: number) => (
                <TableRow key={i}>
                  <TableCell>
                    {d.batch_id ? (
                      <Typography component={RouterLink} to={`/batches/${d.batch_id}`} sx={{ fontFamily: 'JetBrains Mono, monospace', textDecoration: 'none' }}>
                        {String(d.batch_id).slice(0, 8)}…
                      </Typography>
                    ) : (
                      '—'
                    )}
                  </TableCell>
                  <TableCell>{d.action}</TableCell>
                  <TableCell>{d.outcome}</TableCell>
                  <TableCell>{d.total_jobs}</TableCell>
                  <TableCell>{d.failed_count}</TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </Paper>
    </Box>
  )
}
