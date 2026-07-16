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
import TextField from '@mui/material/TextField'
import Typography from '@mui/material/Typography'
import Tooltip from '@mui/material/Tooltip'
import { apiGet, apiMutate } from '../api/client'
import { EmptyState } from '../components/EmptyState'
import { LoadingBlock } from '../components/LoadingBlock'
import { MetricCards } from '../components/MetricCards'
import { PageHeader } from '../components/PageHeader'
import { useLiveRefresh } from '../hooks/useLiveRefresh'

function statusChip(status: string) {
  const map: Record<string, { label: string; color: 'default' | 'success' | 'error' | 'warning' | 'info' }> = {
    running: { label: 'Running', color: 'success' },
    paused_topic: { label: 'Paused (topic)', color: 'error' },
    topic_paused: { label: 'Topic paused', color: 'warning' },
    paused: { label: 'Paused', color: 'error' },
    payload_log: { label: 'payload log', color: 'info' },
  }
  const m = map[status] || { label: status, color: 'default' as const }
  return <Chip size="small" color={m.color} label={m.label} />
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
      setToast(`${action} ok`)
      await load()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Control failed')
    }
  }

  if (!data && !error) return <LoadingBlock />
  if (error && !data) return <Alert severity="error">{error}</Alert>

  return (
    <Box>
      <Button component={RouterLink} to="/" sx={{ mb: 1 }}>
        ← All batches
      </Button>
      <PageHeader title="Kafka Lag" subtitle="Per-group/topic lag with pause/resume controls." />
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

      <Paper sx={{ p: 2, mb: 2 }}>
        <Typography variant="h6" sx={{ mb: 1 }}>
          Ingest partition lookup
        </Typography>
        <Stack direction="row" spacing={1}>
          <TextField
            size="small"
            label="tenant_id"
            value={tenantInput}
            onChange={(e) => setTenantInput(e.target.value)}
            sx={{ minWidth: 240 }}
          />
          <Button variant="contained" onClick={() => setParams(tenantInput ? { tenant_id: tenantInput } : {})}>
            Lookup
          </Button>
        </Stack>
        {data?.tenant_lookup?.partition != null ? (
          <Typography variant="body2" sx={{ mt: 1.5 }}>
            Tenant <code>{data.tenant_lookup.tenant_id}</code> → partition <strong>{data.tenant_lookup.partition}</strong> of{' '}
            {data.tenant_lookup.partition_count} on <code>{data.tenant_lookup.topic}</code>
            {data.tenant_lookup.source ? ` (${data.tenant_lookup.source})` : ''}
            {data.tenant_lookup.lag != null ? ` — lag ${data.tenant_lookup.lag}` : ''}
          </Typography>
        ) : null}
      </Paper>

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
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            {data.pause_tooltip}
          </Typography>
          <Paper sx={{ p: 2, mb: 2, overflow: 'auto' }}>
            <Typography variant="h6" sx={{ mb: 1 }}>
              Pending by topic
            </Typography>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Group</TableCell>
                  <TableCell>Topic</TableCell>
                  <TableCell>Partitions</TableCell>
                  <TableCell>Lag</TableCell>
                  <TableCell>Status</TableCell>
                  <TableCell>Control</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {data.topics.map((t: any) => (
                  <TableRow key={`${t.group}-${t.topic}`}>
                    <TableCell sx={{ fontFamily: 'JetBrains Mono, monospace' }}>{t.group}</TableCell>
                    <TableCell sx={{ fontFamily: 'JetBrains Mono, monospace' }}>{t.topic}</TableCell>
                    <TableCell>{t.partitions}</TableCell>
                    <TableCell>{t.lag}</TableCell>
                    <TableCell>{statusChip(t.status)}</TableCell>
                    <TableCell>
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
          </Paper>
          <Paper sx={{ p: 2, overflow: 'auto' }}>
            <Typography variant="h6" sx={{ mb: 1 }}>
              Pending by partition
            </Typography>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Group</TableCell>
                  <TableCell>Topic</TableCell>
                  <TableCell>Partition</TableCell>
                  <TableCell>Committed</TableCell>
                  <TableCell>End</TableCell>
                  <TableCell>Lag</TableCell>
                  <TableCell>Status</TableCell>
                  <TableCell>Control</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {data.partitions.map((r: any) => (
                  <TableRow key={`${r.group}-${r.topic}-${r.partition}`}>
                    <TableCell sx={{ fontFamily: 'JetBrains Mono, monospace' }}>{r.group}</TableCell>
                    <TableCell sx={{ fontFamily: 'JetBrains Mono, monospace' }}>{r.topic}</TableCell>
                    <TableCell>{r.partition}</TableCell>
                    <TableCell>{r.never_consumed ? 'never consumed' : r.committed}</TableCell>
                    <TableCell>{r.end_offset ?? '—'}</TableCell>
                    <TableCell>{r.lag}</TableCell>
                    <TableCell>{statusChip(r.status)}</TableCell>
                    <TableCell>
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
          </Paper>
        </>
      )}
    </Box>
  )
}
