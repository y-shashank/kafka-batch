import { useCallback, useEffect, useState } from 'react'
import { Link as RouterLink } from 'react-router-dom'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Paper from '@mui/material/Paper'
import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import Typography from '@mui/material/Typography'
import { apiGet } from '../api/client'
import { EmptyState } from '../components/EmptyState'
import { LoadingBlock } from '../components/LoadingBlock'
import { MetricCards } from '../components/MetricCards'
import { PageHeader } from '../components/PageHeader'
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
  if (!data.available) return <EmptyState title="Consumer Process" message={data.message} />

  return (
    <Box>
      <Button component={RouterLink} to="/" sx={{ mb: 1 }}>
        ← All batches
      </Button>
      <PageHeader title="Consumer Process" subtitle={`Backend: ${data.backend}. Stats sampled every ${data.stats_interval}s.`} />
      <MetricCards
        metrics={[
          { label: 'Consumers', value: data.consumers.length },
          { label: 'Running jobs', value: data.running_jobs.length },
        ]}
      />
      <Paper sx={{ p: 2, mb: 2, overflow: 'auto' }}>
        <Typography variant="h6" sx={{ mb: 1 }}>
          Consumers
        </Typography>
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
            {data.consumers.map((c: any) => (
              <TableRow key={c.consumer_id}>
                <TableCell sx={{ fontFamily: 'JetBrains Mono, monospace' }}>{c.consumer_id}</TableCell>
                <TableCell>{c.hostname}</TableCell>
                <TableCell>{c.pid}</TableCell>
                <TableCell>{c.rss_label}</TableCell>
                <TableCell>{c.cpu_pct != null ? `${Number(c.cpu_pct).toFixed(1)}%` : '—'}</TableCell>
                <TableCell>{c.topic}</TableCell>
                <TableCell>{c.last_seen_label}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </Paper>
      <Paper sx={{ p: 2, overflow: 'auto' }}>
        <Typography variant="h6" sx={{ mb: 1 }}>
          Running jobs
        </Typography>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Job</TableCell>
              <TableCell>Batch</TableCell>
              <TableCell>Worker</TableCell>
              <TableCell>Consumer</TableCell>
              <TableCell>Topic/Part</TableCell>
              <TableCell>Started</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {data.running_jobs.map((j: any) => (
              <TableRow key={j.job_id}>
                <TableCell sx={{ fontFamily: 'JetBrains Mono, monospace' }}>{String(j.job_id).slice(0, 8)}</TableCell>
                <TableCell>
                  {j.batch_id ? (
                    <Typography component={RouterLink} to={`/batches/${j.batch_id}`} sx={{ fontFamily: 'JetBrains Mono, monospace', textDecoration: 'none' }}>
                      {String(j.batch_id).slice(0, 8)}
                    </Typography>
                  ) : (
                    '—'
                  )}
                </TableCell>
                <TableCell>{j.worker_class}</TableCell>
                <TableCell sx={{ fontFamily: 'JetBrains Mono, monospace' }}>{j.consumer_id}</TableCell>
                <TableCell>
                  {j.topic}/{j.partition}
                </TableCell>
                <TableCell>{j.started_at_label}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </Paper>
    </Box>
  )
}
