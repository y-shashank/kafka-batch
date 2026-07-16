import { useCallback, useEffect, useState } from 'react'
import { Link as RouterLink, useParams, useSearchParams } from 'react-router-dom'
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
import TextField from '@mui/material/TextField'
import Typography from '@mui/material/Typography'
import Chip from '@mui/material/Chip'
import { apiGet } from '../api/client'
import { EmptyState } from '../components/EmptyState'
import { LoadingBlock } from '../components/LoadingBlock'
import { MetricCards } from '../components/MetricCards'
import { PageHeader } from '../components/PageHeader'
import { useLiveRefresh } from '../hooks/useLiveRefresh'

export function FairnessPage() {
  const { type = 'time' } = useParams()
  const lane = type === 'throughput' ? 'throughput' : 'time'
  const [params, setParams] = useSearchParams()
  const tenantId = params.get('tenant_id') || ''
  const [tenantInput, setTenantInput] = useState(tenantId)
  const [data, setData] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    try {
      const qs = tenantId ? `?tenant_id=${encodeURIComponent(tenantId)}` : ''
      setData(await apiGet(`/api/fairness/${lane}${qs}`))
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load')
    }
  }, [lane, tenantId])

  useEffect(() => {
    void load()
  }, [load])
  useLiveRefresh(load)

  if (!data && !error) return <LoadingBlock />
  if (error) return <Alert severity="error">{error}</Alert>

  const title = lane === 'throughput' ? 'Throughput Fairness' : 'Time Fairness'

  return (
    <Box>
      <Button component={RouterLink} to="/" sx={{ mb: 1 }}>
        ← All batches
      </Button>
      <PageHeader title={title} subtitle={data.ready_topics_description || undefined} />
      {!data.lane_active ? (
        <Alert severity="warning" sx={{ mb: 2 }}>
          This fairness lane has no registered workers in this process.
        </Alert>
      ) : null}
      <Paper sx={{ p: 2, mb: 2 }}>
        <Typography variant="h6" sx={{ mb: 1 }}>
          Ingest partition lookup
        </Typography>
        <Stack direction="row" spacing={1}>
          <TextField size="small" label="tenant_id" value={tenantInput} onChange={(e) => setTenantInput(e.target.value)} />
          <Button variant="contained" onClick={() => setParams(tenantInput ? { tenant_id: tenantInput } : {})}>
            Lookup
          </Button>
        </Stack>
        {data.tenant_lookup?.partition != null ? (
          <Typography variant="body2" sx={{ mt: 1.5 }}>
            Tenant <code>{data.tenant_lookup.tenant_id}</code> → partition <strong>{data.tenant_lookup.partition}</strong> on{' '}
            <code>{data.tenant_lookup.topic}</code>
          </Typography>
        ) : null}
      </Paper>
      {!data.available ? (
        <EmptyState message={data.message} />
      ) : (
        <>
          <MetricCards
            metrics={[
              { label: 'Active lanes', value: data.active_lanes },
              { label: 'Ingest lag', value: data.ingest_total },
              { label: 'Ready lag', value: data.ready_total },
              {
                label: 'Dispatcher',
                value: data.throttled ? 'Throttled' : 'Flowing',
                color: data.throttled ? '#d97706' : '#059669',
              },
            ]}
          />
          <Paper sx={{ p: 2, mb: 2, overflow: 'auto' }}>
            <Typography variant="h6" sx={{ mb: 1 }}>
              Ingest ({data.ingest_topic})
            </Typography>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Partition</TableCell>
                  <TableCell>Lag</TableCell>
                  <TableCell>Status</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {data.ingest.map((p: any) => (
                  <TableRow key={p.partition}>
                    <TableCell>{p.partition}</TableCell>
                    <TableCell>{p.lag}</TableCell>
                    <TableCell>{p.never_consumed ? <Chip size="small" color="warning" label="Never consumed" /> : '—'}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </Paper>
          <Paper sx={{ p: 2, overflow: 'auto' }}>
            <Typography variant="h6" sx={{ mb: 1 }}>
              Ready topics
            </Typography>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Partition</TableCell>
                  <TableCell>Topic</TableCell>
                  <TableCell>Runtime</TableCell>
                  <TableCell>Lag</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {data.ready.map((p: any, i: number) => (
                  <TableRow key={`${p.topic}-${p.partition}-${i}`}>
                    <TableCell>{p.partition}</TableCell>
                    <TableCell sx={{ fontFamily: 'JetBrains Mono, monospace' }}>{p.topic}</TableCell>
                    <TableCell>{p.runtime || '—'}</TableCell>
                    <TableCell>{p.lag}</TableCell>
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
