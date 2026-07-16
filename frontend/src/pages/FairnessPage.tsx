import { useCallback, useEffect, useState } from 'react'
import { useParams, useSearchParams } from 'react-router-dom'
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
import { apiGet } from '../api/client'
import { EmptyState } from '../components/EmptyState'
import { LoadingBlock } from '../components/LoadingBlock'
import { MetricCards } from '../components/MetricCards'
import { PageHeader } from '../components/PageHeader'
import { SectionCard } from '../components/SectionCard'
import { monoSx } from '../components/MonoLink'
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

  const title = lane === 'throughput' ? 'Throughput fairness' : 'Time fairness'

  return (
    <Box>
      <PageHeader title={title} subtitle={data.ready_topics_description || undefined} />
      {!data.lane_active ? (
        <Alert severity="warning" sx={{ mb: 2 }}>
          This fairness lane has no registered workers in this process.
        </Alert>
      ) : null}

      <SectionCard title="Ingest partition lookup">
        <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1.5}>
          <TextField label="Tenant ID" value={tenantInput} onChange={(e) => setTenantInput(e.target.value)} />
          <Button variant="contained" onClick={() => setParams(tenantInput ? { tenant_id: tenantInput } : {})}>
            Lookup
          </Button>
        </Stack>
        {data.tenant_lookup?.partition != null ? (
          <Typography variant="body2" sx={{ mt: 2 }}>
            Tenant <code>{data.tenant_lookup.tenant_id}</code> → partition <strong>{data.tenant_lookup.partition}</strong> on{' '}
            <code>{data.tenant_lookup.topic}</code>
          </Typography>
        ) : null}
      </SectionCard>

      {!data.available ? (
        <EmptyState message={data.message} />
      ) : (
        <>
          <MetricCards
            metrics={[
              { label: 'Active lanes', value: data.active_lanes },
              { label: 'Ingest lag', value: data.ingest_total },
              { label: 'Ready lag', value: data.ready_total },
              { label: 'Dispatcher', value: data.throttled ? 'Throttled' : 'Flowing', color: data.throttled ? 'warning.main' : 'success.main' },
            ]}
          />
          <SectionCard title={`Ingest (${data.ingest_topic})`} noPadding>
            <TableContainer>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Partition</TableCell>
                    <TableCell align="right">Lag</TableCell>
                    <TableCell>Status</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {data.ingest.map((p: any) => (
                    <TableRow key={p.partition} hover>
                      <TableCell>{p.partition}</TableCell>
                      <TableCell align="right">{p.lag}</TableCell>
                      <TableCell>{p.never_consumed ? <Chip size="small" color="warning" label="Never consumed" /> : '—'}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </SectionCard>
          <SectionCard title="Ready topics" noPadding>
            <TableContainer>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Partition</TableCell>
                    <TableCell>Topic</TableCell>
                    <TableCell>Runtime</TableCell>
                    <TableCell align="right">Lag</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {data.ready.map((p: any, i: number) => (
                    <TableRow key={`${p.topic}-${p.partition}-${i}`} hover>
                      <TableCell>{p.partition}</TableCell>
                      <TableCell sx={monoSx}>{p.topic}</TableCell>
                      <TableCell>{p.runtime || '—'}</TableCell>
                      <TableCell align="right">{p.lag}</TableCell>
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
