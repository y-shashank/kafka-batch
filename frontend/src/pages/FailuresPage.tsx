import { useCallback, useEffect, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Checkbox from '@mui/material/Checkbox'
import Chip from '@mui/material/Chip'
import Stack from '@mui/material/Stack'
import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableContainer from '@mui/material/TableContainer'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import Typography from '@mui/material/Typography'
import Snackbar from '@mui/material/Snackbar'
import { apiGet, apiMutate } from '../api/client'
import { LoadingBlock } from '../components/LoadingBlock'
import { MetricCards } from '../components/MetricCards'
import { PageHeader } from '../components/PageHeader'
import { StatusChip } from '../components/StatusChip'
import { SectionCard } from '../components/SectionCard'
import { MonoLink, monoSx } from '../components/MonoLink'
import { useLiveRefresh } from '../hooks/useLiveRefresh'
import { PaginationBar } from '../components/PaginationBar'

export function FailuresPage() {
  const [params, setParams] = useSearchParams()
  const status = params.get('status') || 'retrying'
  const page = Math.max(1, Number(params.get('page') || 1))
  const cursor = params.get('cursor') || ''
  const [data, setData] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)
  const [selected, setSelected] = useState<string[]>([])
  const [toast, setToast] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [cursorStack, setCursorStack] = useState<string[]>([])

  const load = useCallback(async () => {
    try {
      const qs = new URLSearchParams({ status })
      if (status === 'retrying') {
        if (cursor) qs.set('cursor', cursor)
      } else {
        qs.set('page', String(page))
      }
      setData(await apiGet(`/api/failures?${qs}`))
      setError(null)
      setSelected([])
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load')
    }
  }, [status, page, cursor])

  useEffect(() => {
    void load()
  }, [load])
  useLiveRefresh(load)

  const mutate = async (fn: () => Promise<unknown>, msg: string) => {
    setBusy(true)
    try {
      await fn()
      setToast(msg)
      await load()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Action failed')
    } finally {
      setBusy(false)
    }
  }

  if (!data && !error) return <LoadingBlock />
  if (error && !data) return <Alert severity="error">{error}</Alert>

  const tiers = data?.retry_lag_by_tier || {}
  const failures: any[] = data?.failures || []
  const isRetrying = status === 'retrying'
  const allIds = failures.map((f) => f.job_id).filter(Boolean)

  return (
    <Box>
      <PageHeader
        title="Failures"
        subtitle={
          isRetrying
            ? 'Pending retries loaded from Kafka retry topics (50 per tier). Delete skips execution; delete-all advances past a watermark.'
            : 'Exhausted job failures across all batches.'
        }
      />
      {data?.retry_lag_by_tier ? (
        <MetricCards
          metrics={[
            { label: 'Pending retries', value: data.retry_lag_total ?? '—' },
            ...Object.entries(tiers).map(([tier, lag]) => ({ label: `${tier} tier`, value: lag as number })),
          ]}
        />
      ) : null}

      {error ? (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      ) : null}

      {isRetrying && data?.available === false ? (
        <Alert severity="warning" sx={{ mb: 2 }}>
          {data.message || 'Retry listing unavailable.'}
        </Alert>
      ) : null}

      <SectionCard noPadding>
        <Stack
          direction={{ xs: 'column', sm: 'row' }}
          spacing={1}
          sx={{ px: 2, pt: 2, pb: 1 }}
          alignItems={{ sm: 'center' }}
          justifyContent="space-between"
          flexWrap="wrap"
          useFlexGap
        >
          <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
            {['retrying', 'failed'].map((s) => (
              <Chip
                key={s}
                label={s === 'retrying' ? 'Retrying' : 'Failed'}
                clickable
                color={status === s ? 'primary' : 'default'}
                variant={status === s ? 'filled' : 'outlined'}
                onClick={() => {
                  setCursorStack([])
                  setParams(s === 'retrying' ? { status: s } : { status: s, page: '1' })
                }}
              />
            ))}
          </Stack>
          {isRetrying ? (
            <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
              <Button
                size="small"
                color="error"
                disabled={busy || !selected.length}
                onClick={() => {
                  if (!confirm(`Skip ${selected.length} retrying job(s)?`)) return
                  void mutate(
                    () => apiMutate('POST', '/api/retries/delete', { job_ids: selected }),
                    'Selected retries cancelled',
                  )
                }}
              >
                Delete selected
              </Button>
              <Button
                size="small"
                color="error"
                variant="outlined"
                disabled={busy}
                onClick={() => {
                  if (!confirm('Skip all pending retries up to the current Kafka watermarks?')) return
                  void mutate(() => apiMutate('POST', '/api/retries/delete_all', {}), 'All pending retries marked to skip')
                }}
              >
                Delete all
              </Button>
            </Stack>
          ) : null}
        </Stack>
        <TableContainer>
          <Table size="small">
            <TableHead>
              <TableRow>
                {isRetrying ? <TableCell padding="checkbox" /> : null}
                <TableCell>Batch</TableCell>
                <TableCell>Job</TableCell>
                <TableCell>Worker</TableCell>
                <TableCell>Status</TableCell>
                <TableCell>Attempt</TableCell>
                <TableCell>Next retry</TableCell>
                {isRetrying ? <TableCell>Location</TableCell> : null}
                <TableCell>Error</TableCell>
                <TableCell>Message</TableCell>
                <TableCell>Failed at</TableCell>
                {isRetrying ? <TableCell align="right">Actions</TableCell> : null}
              </TableRow>
            </TableHead>
            <TableBody>
              {failures.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={isRetrying ? 11 : 9} align="center" sx={{ py: 6 }}>
                    <Typography color="text.secondary">
                      {isRetrying ? 'No pending retries in Kafka.' : 'No failures recorded.'}
                    </Typography>
                  </TableCell>
                </TableRow>
              ) : (
                failures.map((f: any) => (
                  <TableRow key={`${f.topic || f.batch_id}-${f.partition ?? ''}-${f.offset ?? f.job_id}`} hover>
                    {isRetrying ? (
                      <TableCell padding="checkbox">
                        <Checkbox
                          checked={selected.includes(f.job_id)}
                          onChange={(e) =>
                            setSelected((prev) =>
                              e.target.checked ? [...prev, f.job_id] : prev.filter((id) => id !== f.job_id),
                            )
                          }
                        />
                      </TableCell>
                    ) : null}
                    <TableCell>
                      {f.batch_id ? (
                        <MonoLink to={`/batches/${f.batch_id}`}>{String(f.batch_id).slice(0, 8)}</MonoLink>
                      ) : (
                        '—'
                      )}
                    </TableCell>
                    <TableCell sx={monoSx}>{String(f.job_id).slice(0, 8)}</TableCell>
                    <TableCell>{f.worker_class || '—'}</TableCell>
                    <TableCell>
                      <StatusChip status={f.status} />
                    </TableCell>
                    <TableCell>{f.attempt}</TableCell>
                    <TableCell>{f.next_retry_eta || '—'}</TableCell>
                    {isRetrying ? (
                      <TableCell sx={{ ...monoSx, whiteSpace: 'nowrap' }}>
                        {f.tier ? `${f.tier} ` : ''}
                        {f.topic != null ? `${f.partition}@${f.offset}` : '—'}
                      </TableCell>
                    ) : null}
                    <TableCell sx={{ color: 'error.main', fontWeight: 500 }}>{f.error_class || '—'}</TableCell>
                    <TableCell sx={{ maxWidth: 260 }}>{f.error_message || '—'}</TableCell>
                    <TableCell sx={{ whiteSpace: 'nowrap' }}>{f.failed_at_label || '—'}</TableCell>
                    {isRetrying ? (
                      <TableCell align="right">
                        <Button
                          size="small"
                          color="error"
                          disabled={busy}
                          onClick={() => {
                            if (!confirm('Skip this retrying job?')) return
                            void mutate(
                              () => apiMutate('POST', '/api/retries/delete', { job_ids: [f.job_id] }),
                              'Retry cancelled',
                            )
                          }}
                        >
                          Delete
                        </Button>
                      </TableCell>
                    ) : null}
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </TableContainer>
        <Box sx={{ p: 2 }}>
          {isRetrying ? (
            <Stack direction="row" spacing={1} alignItems="center" justifyContent="center">
              <Button
                size="small"
                disabled={!cursorStack.length || busy}
                onClick={() => {
                  const prev = [...cursorStack]
                  const back = prev.pop() || ''
                  setCursorStack(prev)
                  setParams(back ? { status: 'retrying', cursor: back } : { status: 'retrying' })
                }}
              >
                Previous
              </Button>
              <Typography variant="body2" color="text.secondary" sx={{ px: 1 }}>
                Page {cursorStack.length + 1}
              </Typography>
              <Button
                size="small"
                disabled={!data?.has_next || busy}
                onClick={() => {
                  if (!data?.cursor) return
                  setCursorStack((s) => [...s, cursor])
                  setParams({ status: 'retrying', cursor: data.cursor })
                }}
              >
                Next
              </Button>
            </Stack>
          ) : (
            <PaginationBar
              page={page}
              hasNext={!!data?.has_next}
              onPrev={() => setParams({ status, page: String(page - 1) })}
              onNext={() => setParams({ status, page: String(page + 1) })}
            />
          )}
        </Box>
        {isRetrying && allIds.length > 0 ? (
          <Box sx={{ px: 2, pb: 2 }}>
            <Button size="small" onClick={() => setSelected(allIds)} disabled={busy}>
              Select page
            </Button>
          </Box>
        ) : null}
      </SectionCard>

      <Snackbar open={!!toast} autoHideDuration={3000} onClose={() => setToast(null)} message={toast} />
    </Box>
  )
}
