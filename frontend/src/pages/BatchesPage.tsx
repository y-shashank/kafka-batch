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
import TextField from '@mui/material/TextField'
import Typography from '@mui/material/Typography'
import Snackbar from '@mui/material/Snackbar'
import InputAdornment from '@mui/material/InputAdornment'
import SearchIcon from '@mui/icons-material/Search'
import { apiGet, apiMutate } from '../api/client'
import { MetricCards } from '../components/MetricCards'
import { PageHeader } from '../components/PageHeader'
import { StatusChip } from '../components/StatusChip'
import { monoSx } from '../components/MonoLink'
import { ProgressBar } from '../components/ProgressBar'
import { LoadingBlock } from '../components/LoadingBlock'
import { SectionCard } from '../components/SectionCard'
import { MonoLink } from '../components/MonoLink'
import { useLiveRefresh } from '../hooks/useLiveRefresh'
import { PaginationBar } from '../components/PaginationBar'

type Batch = {
  id: string
  short_id: string
  description?: string
  status: string
  tenant_id?: string
  total_jobs: number
  completed_count: number
  failed_count: number
  pending: number
  created_at_label: string
  progress: { done_pct: number; fail_pct: number }
}

type Dashboard = {
  counts: Record<string, number>
  total: number
  pending_jobs: number | null
  liveness: { consumers: number; running_jobs: number } | null
}

export function BatchesPage() {
  const [params, setParams] = useSearchParams()
  const status = params.get('status') || ''
  const q = params.get('q') || ''
  const page = Math.max(1, Number(params.get('page') || 1))
  const [search, setSearch] = useState(q)
  const [dashboard, setDashboard] = useState<Dashboard | null>(null)
  const [batches, setBatches] = useState<Batch[]>([])
  const [hasNext, setHasNext] = useState(false)
  const [loading, setLoading] = useState(true)
  const [selected, setSelected] = useState<string[]>([])
  const [toast, setToast] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    try {
      const qs = new URLSearchParams()
      if (status) qs.set('status', status)
      if (q) qs.set('q', q)
      if (page > 1) qs.set('page', String(page))
      const [dash, list] = await Promise.all([
        apiGet<Dashboard>('/api/dashboard'),
        apiGet<{ batches: Batch[]; has_next: boolean }>(`/api/batches?${qs}`),
      ])
      setDashboard(dash)
      setBatches(list.batches)
      setHasNext(list.has_next)
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load')
    } finally {
      setLoading(false)
    }
  }, [status, q, page])

  useEffect(() => {
    setLoading(true)
    void load()
  }, [load])

  useLiveRefresh(load)

  const setFilter = (next: { status?: string; q?: string; page?: number }) => {
    const p = new URLSearchParams()
    const s = next.status !== undefined ? next.status : status
    const query = next.q !== undefined ? next.q : q
    const pg = next.page !== undefined ? next.page : 1
    if (s) p.set('status', s)
    if (query) p.set('q', query)
    if (pg > 1) p.set('page', String(pg))
    setParams(p)
    setSelected([])
  }

  const mutate = async (fn: () => Promise<unknown>, okMsg: string) => {
    try {
      await fn()
      setToast(okMsg)
      await load()
      setSelected([])
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Action failed')
    }
  }

  if (loading && !dashboard) return <LoadingBlock />

  const counts = dashboard?.counts || {}
  const chips = [
    { label: 'All', value: '' },
    ...['running', 'success', 'complete', 'cancelled'].map((s) => ({
      label: `${s[0].toUpperCase()}${s.slice(1)} (${counts[s] || 0})`,
      value: s,
    })),
  ]

  return (
    <Box>
      <PageHeader title="Batches" subtitle="Inspect progress, cancel running work, or clean up records." />
      {error ? (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      ) : null}

      <MetricCards
        metrics={[
          { label: 'Total', value: dashboard?.total ?? 0 },
          { label: 'Running', value: counts.running || 0, color: 'info.main' },
          { label: 'Success', value: counts.success || 0, color: 'success.main' },
          { label: 'Complete', value: counts.complete || 0, color: 'warning.main' },
          { label: 'Cancelled', value: counts.cancelled || 0 },
          ...(dashboard?.pending_jobs != null ? [{ label: 'Pending jobs', value: dashboard.pending_jobs }] : []),
          ...(dashboard?.liveness
            ? [
                { label: 'Consumers', value: dashboard.liveness.consumers, to: '/live' },
                { label: 'Running jobs', value: dashboard.liveness.running_jobs, to: '/live' },
              ]
            : []),
        ]}
      />

      <SectionCard>
        <Stack spacing={2}>
          <Stack direction={{ xs: 'column', md: 'row' }} spacing={1.5} alignItems={{ md: 'center' }}>
            <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap sx={{ flex: 1 }}>
              {chips.map((c) => (
                <Chip
                  key={c.label}
                  label={c.label}
                  clickable
                  color={status === c.value ? 'primary' : 'default'}
                  variant={status === c.value ? 'filled' : 'outlined'}
                  onClick={() => setFilter({ status: c.value, page: 1 })}
                />
              ))}
            </Stack>
            <TextField
              placeholder="Search batch ID or description"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') setFilter({ q: search, page: 1 })
              }}
              sx={{ minWidth: { md: 280 } }}
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <SearchIcon fontSize="small" />
                  </InputAdornment>
                ),
              }}
            />
            <Button variant="contained" onClick={() => setFilter({ q: search, page: 1 })}>
              Search
            </Button>
            {q ? (
              <Button
                onClick={() => {
                  setSearch('')
                  setFilter({ q: '', page: 1 })
                }}
              >
                Clear
              </Button>
            ) : null}
          </Stack>

          <TableContainer sx={{ maxWidth: '100%' }}>
            <Table size="small" stickyHeader>
              <TableHead>
                <TableRow>
                  <TableCell padding="checkbox">
                    <Checkbox
                      checked={batches.length > 0 && selected.length === batches.length}
                      indeterminate={selected.length > 0 && selected.length < batches.length}
                      onChange={(e) => setSelected(e.target.checked ? batches.map((b) => b.id) : [])}
                    />
                  </TableCell>
                  <TableCell>Batch</TableCell>
                  <TableCell>Tenant</TableCell>
                  <TableCell>Status</TableCell>
                  <TableCell align="right">Total</TableCell>
                  <TableCell align="right">Done</TableCell>
                  <TableCell align="right">Failed</TableCell>
                  <TableCell align="right">Pending</TableCell>
                  <TableCell>Progress</TableCell>
                  <TableCell>Created</TableCell>
                  <TableCell align="right">Actions</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {batches.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={11} align="center" sx={{ py: 6 }}>
                      <Typography color="text.secondary">{q ? `No batches match “${q}”.` : 'No batches found.'}</Typography>
                    </TableCell>
                  </TableRow>
                ) : (
                  batches.map((b) => (
                    <TableRow key={b.id} hover selected={selected.includes(b.id)}>
                      <TableCell padding="checkbox">
                        <Checkbox
                          checked={selected.includes(b.id)}
                          onChange={(e) =>
                            setSelected((prev) => (e.target.checked ? [...prev, b.id] : prev.filter((id) => id !== b.id)))
                          }
                        />
                      </TableCell>
                      <TableCell sx={{ maxWidth: 240 }}>
                        <Stack spacing={0.25} sx={{ minWidth: 0 }}>
                          <MonoLink to={`/batches/${b.id}`}>{b.short_id}</MonoLink>
                          {b.description ? (
                            <Typography
                              variant="caption"
                              color="text.secondary"
                              noWrap
                              title={b.description}
                              sx={{ display: 'block', lineHeight: 1.3 }}
                            >
                              {b.description}
                            </Typography>
                          ) : null}
                        </Stack>
                      </TableCell>
                      <TableCell sx={{ ...monoSx, maxWidth: 160 }} title={b.tenant_id || undefined}>
                        {b.tenant_id || (
                          <Typography component="span" variant="body2" color="text.disabled">
                            —
                          </Typography>
                        )}
                      </TableCell>
                      <TableCell>
                        <StatusChip status={b.status} />
                      </TableCell>
                      <TableCell align="right">{b.total_jobs}</TableCell>
                      <TableCell align="right">{b.completed_count}</TableCell>
                      <TableCell align="right" sx={{ color: b.failed_count > 0 ? 'error.main' : undefined, fontWeight: b.failed_count > 0 ? 500 : 400 }}>
                        {b.failed_count}
                      </TableCell>
                      <TableCell align="right">{b.pending}</TableCell>
                      <TableCell>
                        <ProgressBar donePct={b.progress.done_pct} failPct={b.progress.fail_pct} />
                      </TableCell>
                      <TableCell sx={{ whiteSpace: 'nowrap' }}>
                        <Typography variant="caption" color="text.secondary">
                          {b.created_at_label}
                        </Typography>
                      </TableCell>
                      <TableCell align="right">
                        <Stack direction="row" spacing={0.5} justifyContent="flex-end">
                          {b.status === 'running' ? (
                            <Button
                              size="small"
                              color="warning"
                              onClick={() => {
                                if (confirm('Cancel this batch? Remaining jobs will not run.')) {
                                  void mutate(() => apiMutate('POST', `/api/batches/${b.id}/cancel`), 'Batch cancelled')
                                }
                              }}
                            >
                              Cancel
                            </Button>
                          ) : null}
                          <Button
                            size="small"
                            color="error"
                            onClick={() => {
                              if (confirm('Delete this batch record permanently?')) {
                                void mutate(() => apiMutate('DELETE', `/api/batches/${b.id}`), 'Batch deleted')
                              }
                            }}
                          >
                            Delete
                          </Button>
                        </Stack>
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </TableContainer>

          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1.5} alignItems="center" justifyContent="space-between">
            <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
              <Button
                variant="outlined"
                color="warning"
                disabled={!selected.length}
                onClick={() => {
                  if (confirm(`Cancel ${selected.length} batch(es)?`)) {
                    void mutate(
                      () => apiMutate('POST', '/api/batches/bulk', { bulk_action: 'cancel', batch_ids: selected }),
                      'Selected batches cancelled',
                    )
                  }
                }}
              >
                Cancel selected
              </Button>
              <Button
                variant="outlined"
                color="error"
                disabled={!selected.length}
                onClick={() => {
                  if (confirm(`Delete ${selected.length} batch record(s)?`)) {
                    void mutate(
                      () => apiMutate('POST', '/api/batches/bulk', { bulk_action: 'delete', batch_ids: selected }),
                      'Selected batches deleted',
                    )
                  }
                }}
              >
                Delete selected
              </Button>
            </Stack>
            <PaginationBar
              page={page}
              hasNext={hasNext}
              onPrev={() => setFilter({ page: page - 1 })}
              onNext={() => setFilter({ page: page + 1 })}
            />
            <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
              <Button
                color="warning"
                onClick={() => {
                  if (confirm(`Cancel all${status ? ` ${status}` : ''} batches?`)) {
                    void mutate(
                      () =>
                        apiMutate('POST', '/api/batches/bulk', {
                          bulk_action: 'cancel_all',
                          scope_status: status || undefined,
                          scope_search: q || undefined,
                        }),
                      'Cancel all submitted',
                    )
                  }
                }}
              >
                Cancel all
              </Button>
              <Button
                color="error"
                onClick={() => {
                  if (confirm(`Delete all${status ? ` ${status}` : ''} batches?`)) {
                    void mutate(
                      () =>
                        apiMutate('POST', '/api/batches/bulk', {
                          bulk_action: 'delete_all',
                          scope_status: status || undefined,
                          scope_search: q || undefined,
                        }),
                      'Delete all submitted',
                    )
                  }
                }}
              >
                Delete all
              </Button>
            </Stack>
          </Stack>
        </Stack>
      </SectionCard>

      <Snackbar open={!!toast} autoHideDuration={3000} onClose={() => setToast(null)} anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}>
        <Alert severity="success" onClose={() => setToast(null)} variant="filled">
          {toast}
        </Alert>
      </Snackbar>
    </Box>
  )
}
