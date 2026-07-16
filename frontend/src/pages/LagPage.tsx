import { useCallback, useEffect, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
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
import Tooltip from '@mui/material/Tooltip'
import { apiGet, apiMutate } from '../api/client'
import { EmptyState } from '../components/EmptyState'
import { LoadingBlock } from '../components/LoadingBlock'
import { MetricCards } from '../components/MetricCards'
import { PageHeader } from '../components/PageHeader'
import { SectionCard } from '../components/SectionCard'
import { monoSx } from '../components/MonoLink'
import { useLiveRefresh } from '../hooks/useLiveRefresh'

function statusChip(status: string) {
  const map: Record<string, { label: string; color: 'default' | 'success' | 'error' | 'warning' | 'info' }> = {
    running: { label: 'Running', color: 'success' },
    paused_topic: { label: 'Paused (topic)', color: 'error' },
    topic_paused: { label: 'Topic paused', color: 'warning' },
    paused: { label: 'Paused', color: 'error' },
    payload_log: { label: 'Payload log', color: 'info' },
  }
  const m = map[status] || { label: status, color: 'default' as const }
  return <Chip size="small" color={m.color} label={m.label} variant={m.color === 'default' ? 'outlined' : 'filled'} />
}

export function LagPage() {
  const [params, setParams] = useSearchParams()
  const tenantId = params.get('tenant_id') || ''
  const [tenantInput, setTenantInput] = useState(tenantId)
  const [data, setData] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)
  const [toast, setToast] = useState<string | null>(null)

  const load = useCallback(async () => {
    try {
      const qs = tenantId ? `?tenant_id=${encodeURIComponent(tenantId)}` : ''
      setData(await apiGet(`/api/lag${qs}`))
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load')
    }
  }, [tenantId])

  useEffect(() => {
    void load()
  }, [load])
  useLiveRefresh(load)

  const control = async (action: 'pause' | 'resume', body: Record<string, unknown>) => {
    try {
      await apiMutate('POST', `/api/lag/${action}`, body)
      setToast(`${action === 'pause' ? 'Paused' : 'Resumed'} successfully`)
      await load()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Control failed')
    }
  }

  if (!data && !error) return <LoadingBlock />
  if (error && !data) return <Alert severity="error">{error}</Alert>

  return (
    <Box>
      <PageHeader title="Kafka lag" subtitle="Per-group and topic lag with pause / resume controls." />
      {error ? (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      ) : null}
      {toast ? (
        <Alert severity="success" sx={{ mb: 2 }} onClose={() => setToast(null)}>
          {toast}
        </Alert>
      ) : null}

      <SectionCard title="Ingest partition lookup" subheader="Resolve which ingest partition a tenant_id hashes to.">
        <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1.5} alignItems={{ sm: 'center' }}>
          <TextField label="Tenant ID" value={tenantInput} onChange={(e) => setTenantInput(e.target.value)} sx={{ minWidth: 240 }} />
          <Button variant="contained" onClick={() => setParams(tenantInput ? { tenant_id: tenantInput } : {})}>
            Lookup
          </Button>
        </Stack>
        {data?.tenant_lookup?.partition != null ? (
          <Typography variant="body2" sx={{ mt: 2 }}>
            Tenant <code>{data.tenant_lookup.tenant_id}</code> → partition <strong>{data.tenant_lookup.partition}</strong> of{' '}
            {data.tenant_lookup.partition_count} on <code>{data.tenant_lookup.topic}</code>
            {data.tenant_lookup.source ? ` (${data.tenant_lookup.source})` : ''}
            {data.tenant_lookup.lag != null ? ` — lag ${data.tenant_lookup.lag}` : ''}
          </Typography>
        ) : null}
      </SectionCard>

      {!data.available ? (
        <EmptyState message={data.message} />
      ) : (
        <>
          <MetricCards
            metrics={[
              { label: 'Total pending', value: data.total },
              { label: 'Topics', value: data.topics.length },
              { label: 'Consumer groups', value: data.groups },
            ]}
          />
          <Alert severity="info" sx={{ mb: 2 }}>
            {data.pause_tooltip}
          </Alert>
          <SectionCard title="Pending by topic" noPadding>
            <TableContainer>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Group</TableCell>
                    <TableCell>Topic</TableCell>
                    <TableCell align="right">Partitions</TableCell>
                    <TableCell align="right">Lag</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell align="right">Control</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {data.topics.map((t: any) => (
                    <TableRow key={`${t.group}-${t.topic}`} hover>
                      <TableCell sx={monoSx}>{t.group}</TableCell>
                      <TableCell sx={monoSx}>{t.topic}</TableCell>
                      <TableCell align="right">{t.partitions}</TableCell>
                      <TableCell align="right">{t.lag}</TableCell>
                      <TableCell>{statusChip(t.status)}</TableCell>
                      <TableCell align="right">
                        {t.can_control ? (
                          <Tooltip title={data.pause_tooltip}>
                            <Button
                              size="small"
                              color={t.status === 'paused_topic' ? 'success' : 'error'}
                              onClick={() =>
                                void control(t.status === 'paused_topic' ? 'resume' : 'pause', {
                                  scope: 'topic',
                                  group: t.group,
                                  topic: t.topic,
                                  tenant_id: tenantId || undefined,
                                })
                              }
                            >
                              {t.status === 'paused_topic' ? 'Resume' : 'Pause'}
                            </Button>
                          </Tooltip>
                        ) : (
                          '—'
                        )}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </SectionCard>
          <SectionCard title="Pending by partition" noPadding>
            <TableContainer>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Group</TableCell>
                    <TableCell>Topic</TableCell>
                    <TableCell align="right">Partition</TableCell>
                    <TableCell>Committed</TableCell>
                    <TableCell align="right">End</TableCell>
                    <TableCell align="right">Lag</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell align="right">Control</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {data.partitions.map((r: any) => (
                    <TableRow key={`${r.group}-${r.topic}-${r.partition}`} hover>
                      <TableCell sx={monoSx}>{r.group}</TableCell>
                      <TableCell sx={monoSx}>{r.topic}</TableCell>
                      <TableCell align="right">{r.partition}</TableCell>
                      <TableCell>{r.never_consumed ? 'never consumed' : r.committed}</TableCell>
                      <TableCell align="right">{r.end_offset ?? '—'}</TableCell>
                      <TableCell align="right">{r.lag}</TableCell>
                      <TableCell>{statusChip(r.status)}</TableCell>
                      <TableCell align="right">
                        {r.can_control ? (
                          <Button
                            size="small"
                            color={r.status === 'paused' ? 'success' : 'error'}
                            onClick={() =>
                              void control(r.status === 'paused' ? 'resume' : 'pause', {
                                scope: 'partition',
                                group: r.group,
                                topic: r.topic,
                                partition: r.partition,
                                tenant_id: tenantId || undefined,
                              })
                            }
                          >
                            {r.status === 'paused' ? 'Resume' : 'Pause'}
                          </Button>
                        ) : (
                          '—'
                        )}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </SectionCard>
        </>
      )}
    </Box>
  )
}
