import { useCallback, useEffect, useState } from 'react'
import { Link as RouterLink, useNavigate, useParams, useSearchParams } from 'react-router-dom'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import Typography from '@mui/material/Typography'
import { apiGet, apiMutate } from '../api/client'
import { LoadingBlock } from '../components/LoadingBlock'
import { PageHeader } from '../components/PageHeader'
import { ProgressBar } from '../components/ProgressBar'
import { StatusChip } from '../components/StatusChip'
import { TenantChip } from '../components/TenantChip'
import { useLiveRefresh } from '../hooks/useLiveRefresh'

export function BatchDetailPage() {
  const { id = '' } = useParams()
  const [params, setParams] = useSearchParams()
  const fp = Math.max(1, Number(params.get('fp') || 1))
  const navigate = useNavigate()
  const [data, setData] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    try {
      const res = await apiGet<any>(`/api/batches/${id}?fp=${fp}`)
      setData(res)
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load')
    }
  }, [id, fp])

  useEffect(() => {
    void load()
  }, [load])
  useLiveRefresh(load)

  if (!data && !error) return <LoadingBlock />
  if (error) return <Alert severity="error">{error}</Alert>
  const b = data.batch

  return (
    <Box>
      <Button component={RouterLink} to="/" sx={{ mb: 1 }}>
        ← All batches
      </Button>
      <PageHeader title={`Batch ${b.short_id}`} subtitle={b.description || undefined} />
      <Paper sx={{ p: 2.5, mb: 2 }}>
        <Box sx={{ mb: 2 }}>
          <ProgressBar donePct={b.progress.done_pct} failPct={b.progress.fail_pct} />
        </Box>
        <Table size="small">
          <TableBody>
            {[
              ['ID', <code key="id">{b.id}</code>],
              ['Description', b.description || '—'],
              ['Status', <StatusChip key="s" status={b.status} />],
              ['Tenant', <TenantChip key="t" tenantId={b.tenant_id} />],
              ['Total jobs', b.total_jobs],
              ['Completed', b.completed_count],
              ['Failed', b.failed_count],
              ['Pending', b.pending],
              ['on_success', b.on_success || '—'],
              ['on_complete', b.on_complete || '—'],
              ['Created at', b.created_at_label],
              ['Finished at', b.finished_at_label || '—'],
              ['Callback fired', b.callback_dispatched_at_label || 'no'],
              ['Callback ran on', b.callback_dispatched_by || '—'],
              ['Meta', b.meta ? <pre key="m" style={{ margin: 0, whiteSpace: 'pre-wrap' }}>{JSON.stringify(b.meta, null, 2)}</pre> : '—'],
            ].map(([label, value]) => (
              <TableRow key={String(label)}>
                <TableCell sx={{ width: 180, color: 'text.secondary', fontWeight: 600 }}>{label}</TableCell>
                <TableCell>{value}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
        <Stack direction="row" spacing={1} sx={{ mt: 2 }}>
          {b.status === 'running' ? (
            <Button
              color="warning"
              variant="outlined"
              onClick={async () => {
                if (!confirm('Cancel this batch? Remaining jobs will not run.')) return
                await apiMutate('POST', `/api/batches/${b.id}/cancel`)
                void load()
              }}
            >
              Cancel
            </Button>
          ) : null}
          <Button
            color="error"
            variant="outlined"
            onClick={async () => {
              if (!confirm('Delete this batch record permanently?')) return
              await apiMutate('DELETE', `/api/batches/${b.id}`)
              navigate('/')
            }}
          >
            Delete
          </Button>
        </Stack>
      </Paper>

      {(data.failures?.length > 0 || fp > 1) && (
        <Paper sx={{ p: 2.5 }}>
          <Typography variant="h6" sx={{ mb: 1 }}>
            Job failures
          </Typography>
          <Table size="small">
            <TableHead>
              <TableRow>
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
              {data.failures.map((f: any) => (
                <TableRow key={f.job_id}>
                  <TableCell sx={{ fontFamily: 'JetBrains Mono, monospace' }}>{String(f.job_id).slice(0, 8)}</TableCell>
                  <TableCell>{f.worker_class}</TableCell>
                  <TableCell>
                    <StatusChip status={f.status} />
                  </TableCell>
                  <TableCell>{f.attempt}</TableCell>
                  <TableCell>
                    {f.next_retry_eta || '—'}
                    {f.next_retry_at_label ? (
                      <Typography variant="caption" display="block" color="text.secondary">
                        {f.next_retry_at_label}
                      </Typography>
                    ) : null}
                  </TableCell>
                  <TableCell sx={{ color: 'error.main', fontWeight: 600 }}>{f.error_class}</TableCell>
                  <TableCell>{f.error_message}</TableCell>
                  <TableCell>{f.failed_at_label}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
          <Stack direction="row" spacing={1} justifyContent="center" sx={{ mt: 2 }}>
            <Button disabled={fp <= 1} onClick={() => setParams({ fp: String(fp - 1) })}>
              ← Prev
            </Button>
            <Typography variant="body2" color="text.secondary" sx={{ alignSelf: 'center' }}>
              Page {fp}
            </Typography>
            <Button disabled={!data.failures_has_next} onClick={() => setParams({ fp: String(fp + 1) })}>
              Next →
            </Button>
          </Stack>
        </Paper>
      )}
    </Box>
  )
}
