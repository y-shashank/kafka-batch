import { useCallback, useEffect, useState } from 'react'
import { useParams } from 'react-router-dom'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
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
import { apiGet, apiMutate } from '../api/client'
import { EmptyState } from '../components/EmptyState'
import { LoadingBlock } from '../components/LoadingBlock'
import { MetricCards } from '../components/MetricCards'
import { PageHeader } from '../components/PageHeader'
import { SectionCard } from '../components/SectionCard'
import { monoSx } from '../components/MonoLink'
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
  if (!data.available) return <EmptyState title="Tenant weights" message={data.message} />

  const title = lane === 'throughput' ? 'Throughput weights' : 'Time weights'
  const shareBy = Object.fromEntries((data.shares || []).map((s: any) => [s.tenant_id, s]))

  return (
    <Box>
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
        <SectionCard title="Capacity distribution" subheader="Normalized to 100% across tenant weights.">
          <Box sx={{ display: 'flex', height: 24, borderRadius: 1, overflow: 'hidden', bgcolor: 'action.hover', mb: 2 }}>
            {data.shares.map((s: any) => (
              <Box
                key={s.tenant_id}
                title={`${s.tenant_id}: ${s.share_pct_label}`}
                sx={{
                  width: `${s.share_pct}%`,
                  bgcolor: s.color_bg || 'primary.light',
                  borderRight: '1px solid',
                  borderColor: 'background.paper',
                }}
              />
            ))}
          </Box>
          <Stack direction="row" spacing={2} flexWrap="wrap" useFlexGap>
            {data.shares.map((s: any) => (
              <Stack key={s.tenant_id} direction="row" spacing={0.75} alignItems="center">
                <Box sx={{ width: 10, height: 10, borderRadius: 0.5, bgcolor: s.color_bg || 'primary.light' }} />
                <Typography variant="body2" sx={monoSx}>
                  {s.tenant_id}
                </Typography>
                <Typography variant="body2" fontWeight={500}>
                  {s.share_pct_label}
                </Typography>
              </Stack>
            ))}
          </Stack>
        </SectionCard>
      ) : null}

      <SectionCard title="Tenant weights" noPadding>
        {data.truncated ? (
          <Alert severity="warning" sx={{ m: 2 }}>
            Showing the first {data.weights_max} of {data.total_tenants} tenants.
          </Alert>
        ) : null}
        <TableContainer>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Tenant ID</TableCell>
                <TableCell align="right">Weight</TableCell>
                <TableCell align="right">Capacity share</TableCell>
                <TableCell>Set</TableCell>
                <TableCell>Override</TableCell>
                <TableCell align="right">In-flight</TableCell>
                <TableCell>Status</TableCell>
                <TableCell>Vtime</TableCell>
                <TableCell align="right">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {data.tenants.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={9} align="center" sx={{ py: 5 }}>
                    <Typography color="text.secondary">No active tenants yet.</Typography>
                  </TableCell>
                </TableRow>
              ) : (
                data.tenants.map((t: any) => {
                  const share = shareBy[t.tenant_id]
                  return (
                    <TableRow key={t.tenant_id} hover>
                      <TableCell sx={monoSx}>{t.tenant_id}</TableCell>
                      <TableCell align="right">{t.weight}</TableCell>
                      <TableCell align="right">{share?.share_pct_label || '—'}</TableCell>
                      <TableCell>
                        <Stack direction="row" spacing={0.75} alignItems="center">
                          <TextField
                            type="number"
                            value={drafts[t.tenant_id] ?? t.weight}
                            onChange={(e) => setDrafts((d) => ({ ...d, [t.tenant_id]: e.target.value }))}
                            sx={{ width: 88 }}
                            inputProps={{ step: 0.1, min: 0.1 }}
                          />
                          <Button
                            size="small"
                            variant="outlined"
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
                      <TableCell align="right">{t.inflight}</TableCell>
                      <TableCell>{t.queued ? <Chip size="small" color="success" label="queued" /> : 'idle'}</TableCell>
                      <TableCell sx={monoSx}>{t.vtime > 0 ? `${Number(t.vtime).toFixed(1)}s` : '—'}</TableCell>
                      <TableCell align="right">
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
        </TableContainer>
      </SectionCard>

      <SectionCard title="Add / pre-configure tenant weight">
        <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1.5} alignItems={{ sm: 'center' }}>
          <TextField label="Tenant ID" value={newTenant} onChange={(e) => setNewTenant(e.target.value)} />
          <TextField type="number" label="Weight" value={newWeight} onChange={(e) => setNewWeight(e.target.value)} sx={{ width: 120 }} />
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
      </SectionCard>
    </Box>
  )
}
