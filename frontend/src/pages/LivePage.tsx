import { useCallback, useEffect, useState } from 'react'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableContainer from '@mui/material/TableContainer'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import { apiGet } from '../api/client'
import { EmptyState } from '../components/EmptyState'
import { LoadingBlock } from '../components/LoadingBlock'
import { MetricCards } from '../components/MetricCards'
import { PageHeader } from '../components/PageHeader'
import { SectionCard } from '../components/SectionCard'
import { MonoLink, monoSx } from '../components/MonoLink'
import { useLiveRefresh } from '../hooks/useLiveRefresh'

export function LivePage() {
  const [data, setData] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)
  const load = useCallback(async () => {
    try {
      setData(await apiGet('/api/live'))
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load')
    }
  }, [])
  useEffect(() => {
    void load()
  }, [load])
  useLiveRefresh(load)

  if (!data && !error) return <LoadingBlock />
  if (error) return <Alert severity="error">{error}</Alert>
  if (!data.available) return <EmptyState title="Consumers" message={data.message} />

  return (
    <Box>
      <PageHeader title="Consumers" subtitle={`Backend: ${data.backend}. Stats sampled every ${data.stats_interval}s.`} />
      <MetricCards
        metrics={[
          { label: 'Consumers', value: data.consumers.length },
          { label: 'Running jobs', value: data.running_jobs.length },
        ]}
      />
      <SectionCard title="Active consumers" noPadding>
        <TableContainer>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Consumer</TableCell>
                <TableCell>Host</TableCell>
                <TableCell>PID</TableCell>
                <TableCell>RSS</TableCell>
                <TableCell>CPU</TableCell>
                <TableCell>Topic</TableCell>
                <TableCell>Last seen</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {data.consumers.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={7} align="center" sx={{ py: 4 }}>
                    No consumers reporting.
                  </TableCell>
                </TableRow>
              ) : (
                data.consumers.map((c: any) => (
                  <TableRow key={c.consumer_id} hover>
                    <TableCell sx={monoSx}>{c.consumer_id}</TableCell>
                    <TableCell>{c.hostname}</TableCell>
                    <TableCell>{c.pid}</TableCell>
                    <TableCell>{c.rss_label}</TableCell>
                    <TableCell>{c.cpu_pct != null ? `${Number(c.cpu_pct).toFixed(1)}%` : '—'}</TableCell>
                    <TableCell>{c.topic}</TableCell>
                    <TableCell sx={{ whiteSpace: 'nowrap' }}>{c.last_seen_label}</TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </TableContainer>
      </SectionCard>
      <SectionCard title="Running jobs" noPadding>
        <TableContainer>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Job</TableCell>
                <TableCell>Batch</TableCell>
                <TableCell>Worker</TableCell>
                <TableCell>Consumer</TableCell>
                <TableCell>Topic / part</TableCell>
                <TableCell>Started</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {data.running_jobs.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={6} align="center" sx={{ py: 4 }}>
                    No jobs running.
                  </TableCell>
                </TableRow>
              ) : (
                data.running_jobs.map((j: any) => (
                  <TableRow key={j.job_id} hover>
                    <TableCell sx={monoSx}>{String(j.job_id).slice(0, 8)}</TableCell>
                    <TableCell>
                      {j.batch_id ? <MonoLink to={`/batches/${j.batch_id}`}>{String(j.batch_id).slice(0, 8)}</MonoLink> : '—'}
                    </TableCell>
                    <TableCell>{j.worker_class}</TableCell>
                    <TableCell sx={monoSx}>{j.consumer_id}</TableCell>
                    <TableCell>
                      {j.topic}/{j.partition}
                    </TableCell>
                    <TableCell sx={{ whiteSpace: 'nowrap' }}>{j.started_at_label}</TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </TableContainer>
      </SectionCard>
    </Box>
  )
}
