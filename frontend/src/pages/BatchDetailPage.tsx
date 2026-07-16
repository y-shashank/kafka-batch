import { useCallback, useEffect, useState } from 'react'
import { Link as RouterLink, useNavigate, useParams, useSearchParams } from 'react-router-dom'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Stack from '@mui/material/Stack'
import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableContainer from '@mui/material/TableContainer'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import Typography from '@mui/material/Typography'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import { apiGet, apiMutate } from '../api/client'
import { LoadingBlock } from '../components/LoadingBlock'
import { PageHeader } from '../components/PageHeader'
import { ProgressBar } from '../components/ProgressBar'
import { StatusChip } from '../components/StatusChip'
import { TenantChip } from '../components/TenantChip'
import { SectionCard } from '../components/SectionCard'
import { monoSx } from '../components/MonoLink'
import { useLiveRefresh } from '../hooks/useLiveRefresh'
import { PaginationBar } from '../components/PaginationBar'

export function BatchDetailPage() {
  const { id = '' } = useParams()
  const [params, setParams] = useSearchParams()
  const fp = Math.max(1, Number(params.get('fp') || 1))
  const navigate = useNavigate()
  const [data, setData] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    try {
      setData(await apiGet(`/api/batches/${id}?fp=${fp}`))
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

  const rows: [string, React.ReactNode][] = [
    ['ID', <Typography key="id" component="span" sx={monoSx}>{b.id}</Typography>],
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
    ['Meta', b.meta ? <pre key="m">{JSON.stringify(b.meta, null, 2)}</pre> : '—'],
  ]

  return (
    <Box>
      <Button component={RouterLink} to="/" startIcon={<ArrowBackIcon />} sx={{ mb: 1.5 }}>
        All batches
      </Button>
      <PageHeader
        title={`Batch ${b.short_id}`}
        subtitle={b.description || undefined}
        actions={
          <Stack direction="row" spacing={1}>
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
        }
      />

      <SectionCard title="Overview">
        <Box sx={{ mb: 2, maxWidth: 320 }}>
          <ProgressBar donePct={b.progress.done_pct} failPct={b.progress.fail_pct} />
        </Box>
        <TableContainer>
          <Table size="small">
            <TableBody>
              {rows.map(([label, value]) => (
                <TableRow key={String(label)}>
                  <TableCell sx={{ width: 200, color: 'text.secondary', fontWeight: 500, verticalAlign: 'top' }}>{label}</TableCell>
                  <TableCell sx={{ wordBreak: 'break-word' }}>{value}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </SectionCard>

      {(data.failures?.length > 0 || fp > 1) && (
        <SectionCard title="Job failures" subheader="Recorded on the first failed attempt." noPadding>
          <TableContainer>
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
                  <TableRow key={f.job_id} hover>
                    <TableCell sx={monoSx}>{String(f.job_id).slice(0, 8)}</TableCell>
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
                    <TableCell sx={{ color: 'error.main', fontWeight: 500 }}>{f.error_class}</TableCell>
                    <TableCell sx={{ maxWidth: 280 }}>{f.error_message}</TableCell>
                    <TableCell sx={{ whiteSpace: 'nowrap' }}>{f.failed_at_label}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
          <Box sx={{ p: 2 }}>
            <PaginationBar
              page={fp}
              hasNext={!!data.failures_has_next}
              onPrev={() => setParams({ fp: String(fp - 1) })}
              onNext={() => setParams({ fp: String(fp + 1) })}
            />
          </Box>
        </SectionCard>
      )}
    </Box>
  )
}
