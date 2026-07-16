import { useCallback, useEffect, useState } from 'react'
import { Link as RouterLink, useParams } from 'react-router-dom'
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
import TextField from '@mui/material/TextField'
import Typography from '@mui/material/Typography'
import { apiGet, apiMutate } from '../api/client'
import { EmptyState } from '../components/EmptyState'
import { LoadingBlock } from '../components/LoadingBlock'
import { MetricCards } from '../components/MetricCards'
import { PageHeader } from '../components/PageHeader'
import { useLiveRefresh } from '../hooks/useLiveRefresh'

export function WeightsPage() {
  const { type = 'time' } = useParams()
  const lane = type === 'throughput' ? 'throughput' : 'time'
  const [data, setData] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)
  const [drafts, setDrafts] = useState<Record<string, string>>({})
  const [newTenant, setNewTenant] = useState('')
  const [newWeight, setNewWeight] = useState('1')

  const load = useCallback(async () => {
    try {
      const res = await apiGet<any>(`/api/weights/${lane}`)
      setData(res)
      const d: Record<string, string> = {}
      for (const t of res.tenants || []) d[t.tenant_id] = String(t.weight)
      setDrafts(d)
      if (res.default_weight != null) setNewWeight(String(res.default_weight))
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load')
    }
  }, [lane])

  useEffect(() => {
    void load()
  }, [load])
  useLiveRefresh(load)

  if (!data && !error) return <LoadingBlock />
  if (error) return <Alert severity="error">{error}</Alert>
  if (!data.available) return <EmptyState title="Tenant Weights" message={data.message} />

  const title = lane === 'throughput' ? 'Throughput Fairness Weights' : 'Time Fairness Weights'
  const shareBy = Object.fromEntries((data.shares || []).map((s: any) => [s.tenant_id, s]))

  return (
    <Box>
      <Button component={RouterLink} to="/" sx={{ mb: 1 }}>
        ← All batches
      </Button>
      <PageHeader title={title} subtitle="Higher weight = proportionally more capacity under contention." />
      {!data.weighted_concurrency ? (
        <Alert severity="warning" sx={{ mb: 2 }}>
          Weights only affect ordering right now. Set <code>config.fairness_weighted_concurrency = true</code> to enforce capacity shares.
        </Alert>
      ) : null}
      <MetricCards
        metrics={[
          { label: 'Known tenants', value: data.total_tenants },
          { label: 'Custom weights', value: data.tenants.filter((t: any) => t.has_custom_weight).length },
          { label: 'In-flight now', value: data.tenants.filter((t: any) => t.inflight > 0).length },
          { label: 'Queued', value: data.tenants.filter((t: any) => t.queued).length },
        ]}
      />
      {data.shares?.length ? (
        <Paper sx={{ p: 2, mb: 2 }}>
          <Typography variant="h6" sx={{ mb: 1 }}>
            Capacity distribution
          </Typography>
          <Box sx={{ display: 'flex', height: 28, borderRadius: 999, overflow: 'hidden', border: '1px solid', borderColor: 'divider', mb: 1.5 }}>
            {data.shares.map((s: any) => (
              <Box
                key={s.tenant_id}
                title={`${s.tenant_id}: ${s.share_pct_label}`}
                sx={{ width: `${s.share_pct}%`, bgcolor: s.color_bg, color: s.color_fg, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 11, fontWeight: 700 }}
              >
                {s.share_pct >= 8 ? s.share_pct_label : ''}
              </Box>
            ))}
          </Box>
          <Stack direction="row" spacing={2} flexWrap="wrap" useFlexGap>
            {data.shares.map((s: any) => (
              <Stack key={s.tenant_id} direction="row" spacing={0.75} alignItems="center">
                <Box sx={{ width: 12, height: 12, borderRadius: 0.5, bgcolor: s.color_bg, border: '1px solid rgba(0,0,0,.06)' }} />
                <Typography variant="body2" sx={{ fontFamily: 'JetBrains Mono, monospace' }}>
                  {s.tenant_id}
                </Typography>
                <Typography variant="body2" fontWeight={700}>
                  {s.share_pct_label}
                </Typography>
              </Stack>
            ))}
          </Stack>
        </Paper>
      ) : null}

      <Paper sx={{ p: 2, mb: 2, overflow: 'auto' }}>
        <Typography variant="h6" sx={{ mb: 1 }}>
          Tenant weights
        </Typography>
        {data.truncated ? (
          <Alert severity="warning" sx={{ mb: 1 }}>
            Showing the first {data.weights_max} of {data.total_tenants} tenants.
          </Alert>
        ) : null}
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Tenant ID</TableCell>
              <TableCell align="right">Weight</TableCell>
              <TableCell align="right">Capacity share</TableCell>
              <TableCell>Set</TableCell>
              <TableCell>Override</TableCell>
              <TableCell>In-flight</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Vtime</TableCell>
              <TableCell>Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {data.tenants.length === 0 ? (
              <TableRow>
                <TableCell colSpan={9} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No active tenants yet.
                </TableCell>
              </TableRow>
            ) : (
              data.tenants.map((t: any) => {
                const share = shareBy[t.tenant_id]
                return (
                  <TableRow key={t.tenant_id}>
                    <TableCell sx={{ fontFamily: 'JetBrains Mono, monospace' }}>{t.tenant_id}</TableCell>
                    <TableCell align="right">
                      <strong>{t.weight}</strong>
                    </TableCell>
                    <TableCell align="right">{share?.share_pct_label || '—'}</TableCell>
                    <TableCell>
                      <Stack direction="row" spacing={0.5}>
                        <TextField
                          size="small"
                          type="number"
                          value={drafts[t.tenant_id] ?? t.weight}
                          onChange={(e) => setDrafts((d) => ({ ...d, [t.tenant_id]: e.target.value }))}
                          sx={{ width: 80 }}
                          inputProps={{ step: 0.1, min: 0.1 }}
                        />
                        <Button
                          size="small"
                          onClick={async () => {
                            await apiMutate('PUT', `/api/weights/${lane}`, {
                              tenant_id: t.tenant_id,
                              weight: Number(drafts[t.tenant_id]),
                            })
                            void load()
                          }}
                        >
                          Set
                        </Button>
                      </Stack>
                    </TableCell>
                    <TableCell>
                      {t.has_custom_weight ? <Chip size="small" color="primary" label="custom" /> : <Typography color="text.secondary">default</Typography>}
                    </TableCell>
                    <TableCell>{t.inflight}</TableCell>
                    <TableCell>{t.queued ? <Chip size="small" color="success" label="queued" /> : 'idle'}</TableCell>
                    <TableCell sx={{ fontFamily: 'JetBrains Mono, monospace' }}>{t.vtime > 0 ? `${Number(t.vtime).toFixed(1)}s` : '—'}</TableCell>
                    <TableCell>
                      {t.has_custom_weight ? (
                        <Button
                          size="small"
                          onClick={async () => {
                            await apiMutate('DELETE', `/api/weights/${lane}/${encodeURIComponent(t.tenant_id)}`)
                            void load()
                          }}
                        >
                          Reset
                        </Button>
                      ) : null}
                    </TableCell>
                  </TableRow>
                )
              })
            )}
          </TableBody>
        </Table>
      </Paper>

      <Paper sx={{ p: 2 }}>
        <Typography variant="h6" sx={{ mb: 1 }}>
          Add / pre-configure tenant weight
        </Typography>
        <Stack direction="row" spacing={1}>
          <TextField size="small" label="Tenant ID" value={newTenant} onChange={(e) => setNewTenant(e.target.value)} />
          <TextField size="small" type="number" label="Weight" value={newWeight} onChange={(e) => setNewWeight(e.target.value)} sx={{ width: 100 }} />
          <Button
            variant="contained"
            onClick={async () => {
              if (!newTenant.trim()) return
              await apiMutate('PUT', `/api/weights/${lane}`, { tenant_id: newTenant.trim(), weight: Number(newWeight) })
              setNewTenant('')
              void load()
            }}
          >
            Set weight
          </Button>
        </Stack>
      </Paper>
    </Box>
  )
}
