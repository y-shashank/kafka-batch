import { useCallback, useEffect, useState } from 'react'
import { Link as RouterLink, useSearchParams } from 'react-router-dom'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Checkbox from '@mui/material/Checkbox'
import Chip from '@mui/material/Chip'
import Paper from '@mui/material/Paper'
import Stack from '@mui/material/Stack'
import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import TextField from '@mui/material/TextField'
import Typography from '@mui/material/Typography'
import Snackbar from '@mui/material/Snackbar'
import Alert from '@mui/material/Alert'
import { apiGet, apiMutate } from '../api/client'
import { MetricCards } from '../components/MetricCards'
import { PageHeader } from '../components/PageHeader'
import { StatusChip } from '../components/StatusChip'
import { TenantChip } from '../components/TenantChip'
import { ProgressBar } from '../components/ProgressBar'
import { LoadingBlock } from '../components/LoadingBlock'
import { useLiveRefresh } from '../hooks/useLiveRefresh'
import { STATUS_COLORS } from '../theme'

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
      <PageHeader title="Batches" subtitle="Inspect batch progress, cancel running work, or clean up records." />
      {error ? (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      ) : null}
      <MetricCards
        metrics={[
          { label: 'Total', value: dashboard?.total ?? 0, color: '#0f172a' },
          { label: 'Running', value: counts.running || 0, color: STATUS_COLORS.running },
          { label: 'Success', value: counts.success || 0, color: STATUS_COLORS.success },
          { label: 'Complete', value: counts.complete || 0, color: STATUS_COLORS.complete },
          { label: 'Cancelled', value: counts.cancelled || 0, color: STATUS_COLORS.cancelled },
          ...(dashboard?.pending_jobs != null
            ? [{ label: 'Pending jobs', value: dashboard.pending_jobs, color: STATUS_COLORS.pending }]
            : []),
          ...(dashboard?.liveness
            ? [
                { label: 'Consumers', value: dashboard.liveness.consumers, color: '#0284c7', to: '/live' },
                { label: 'Running jobs', value: dashboard.liveness.running_jobs, color: '#6366f1', to: '/live' },
              ]
            : []),
        ]}
      />

      <Stack direction={{ xs: 'column', md: 'row' }} spacing={1.5} sx={{ mb: 2 }} alignItems={{ md: 'center' }}>
        <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
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
        <Box sx={{ flex: 1 }} />
        <Stack direction="row" spacing={1}>
          <TextField
            size="small"
            placeholder="Search by batch ID or description…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') setFilter({ q: search, page: 1 })
            }}
            sx={{ minWidth: 280 }}
          />
          <Button variant="outlined" onClick={() => setFilter({ q: search, page: 1 })}>
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
      </Stack>

      <Paper sx={{ overflow: 'auto' }}>
        <Table size="small">
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
              <TableCell>Total</TableCell>
              <TableCell>Done</TableCell>
              <TableCell>Failed</TableCell>
              <TableCell>Pending</TableCell>
              <TableCell>Progress</TableCell>
              <TableCell>Created</TableCell>
              <TableCell>Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {batches.length === 0 ? (
              <TableRow>
                <TableCell colSpan={11} align="center" sx={{ py: 5, color: 'text.secondary' }}>
                  {q ? `No batches match “${q}”.` : 'No batches found.'}
                </TableCell>
              </TableRow>
            ) : (
              batches.map((b) => (
                <TableRow key={b.id} hover>
                  <TableCell padding="checkbox">
                    <Checkbox
                      checked={selected.includes(b.id)}
                      onChange={(e) =>
                        setSelected((prev) => (e.target.checked ? [...prev, b.id] : prev.filter((id) => id !== b.id)))
                      }
                    />
                  </TableCell>
                  <TableCell>
                    <Typography
                      component={RouterLink}
                      to={`/batches/${b.id}`}
                      sx={{ fontFamily: 'JetBrains Mono, monospace', textDecoration: 'none', color: 'primary.main', fontWeight: 700 }}
                    >
                      {b.short_id}
                    </Typography>
                    {b.description ? (
                      <Typography variant="caption" display="block" color="text.secondary">
                        {b.description.split(/\s+/).slice(0, q ? 10 : 3).join(' ')}
                      </Typography>
                    ) : null}
                  </TableCell>
                  <TableCell>
                    <TenantChip tenantId={b.tenant_id} />
                  </TableCell>
                  <TableCell>
                    <StatusChip status={b.status} />
                  </TableCell>
                  <TableCell>{b.total_jobs}</TableCell>
                  <TableCell>{b.completed_count}</TableCell>
                  <TableCell sx={{ color: b.failed_count > 0 ? 'error.main' : undefined, fontWeight: b.failed_count > 0 ? 700 : 400 }}>
                    {b.failed_count}
                  </TableCell>
                  <TableCell>{b.pending}</TableCell>
                  <TableCell>
                    <ProgressBar donePct={b.progress.done_pct} failPct={b.progress.fail_pct} />
                  </TableCell>
                  <TableCell sx={{ whiteSpace: 'nowrap', fontSize: 12, color: 'text.secondary' }}>{b.created_at_label}</TableCell>
                  <TableCell>
                    <Stack direction="row" spacing={0.5}>
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
      </Paper>

      <Stack direction={{ xs: 'column', md: 'row' }} spacing={1.5} sx={{ mt: 2 }} alignItems="center" justifyContent="space-between">
        <Stack direction="row" spacing={1}>
          <Button
            variant="outlined"
            color="warning"
            disabled={!selected.length}
            onClick={() => {
              if (confirm(`Cancel ${selected.length} batch(es)? Remaining jobs will not run.`)) {
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
              if (confirm(`Delete ${selected.length} batch record(s) permanently?`)) {
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
        <Stack direction="row" spacing={1} alignItems="center">
          <Button disabled={page <= 1} onClick={() => setFilter({ page: page - 1 })}>
            ← Prev
          </Button>
          <Typography variant="body2" color="text.secondary">
            Page {page}
          </Typography>
          <Button disabled={!hasNext} onClick={() => setFilter({ page: page + 1 })}>
            Next →
          </Button>
        </Stack>
        <Stack direction="row" spacing={1}>
          <Button
            color="warning"
            onClick={() => {
              if (confirm(`Cancel all${status ? ` ${status}` : ''} batches? Remaining jobs will not run.`)) {
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
              if (confirm(`Delete all${status ? ` ${status}` : ''} batches permanently?`)) {
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

      <Snackbar open={!!toast} autoHideDuration={3000} onClose={() => setToast(null)}>
        <Alert severity="success" onClose={() => setToast(null)}>
          {toast}
        </Alert>
      </Snackbar>
    </Box>
  )
}
