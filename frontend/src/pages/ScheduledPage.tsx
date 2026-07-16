import { useCallback, useEffect, useState } from 'react'
import { Link as RouterLink, useSearchParams } from 'react-router-dom'
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
import { apiGet } from '../api/client'
import { EmptyState } from '../components/EmptyState'
import { LoadingBlock } from '../components/LoadingBlock'
import { MetricCards } from '../components/MetricCards'
import { PageHeader } from '../components/PageHeader'
import { useLiveRefresh } from '../hooks/useLiveRefresh'

export function ScheduledPage() {
  const [params, setParams] = useSearchParams()
  const q = params.get('q') || ''
  const [search, setSearch] = useState(q)
  const [data, setData] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    try {
      const qs = q ? `?q=${encodeURIComponent(q)}` : ''
      setData(await apiGet(`/api/scheduled${qs}`))
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load')
    }
  }, [q])

  useEffect(() => {
    void load()
  }, [load])
  useLiveRefresh(load)

  if (!data && !error) return <LoadingBlock />
  if (error) return <Alert severity="error">{error}</Alert>
  if (!data.available) return <EmptyState title="Scheduled jobs" message={data.message} />

  return (
    <Box>
      <Button component={RouterLink} to="/" sx={{ mb: 1 }}>
        ← All batches
      </Button>
      <PageHeader title="Scheduled" subtitle="Delayed jobs waiting in the schedule index." />
      <MetricCards
        metrics={[
          { label: 'Pending scheduled', value: data.size },
          { label: 'Backend', value: String(data.backend) },
        ]}
      />
      <Stack direction="row" spacing={1} sx={{ mb: 2 }}>
        <TextField size="small" placeholder="Search by job ID…" value={search} onChange={(e) => setSearch(e.target.value)} sx={{ minWidth: 280 }} />
        <Button variant="outlined" onClick={() => setParams(search ? { q: search } : {})}>
          Search
        </Button>
        {q ? <Button onClick={() => { setSearch(''); setParams({}) }}>Clear</Button> : null}
      </Stack>
      <Paper sx={{ overflow: 'auto' }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Pointer</TableCell>
              <TableCell>Batch</TableCell>
              <TableCell>Run at</TableCell>
              <TableCell>State</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {data.jobs.length === 0 ? (
              <TableRow>
                <TableCell colSpan={4} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No scheduled jobs.
                </TableCell>
              </TableRow>
            ) : (
              data.jobs.map((j: any) => (
                <TableRow key={j.pointer}>
                  <TableCell sx={{ fontFamily: 'JetBrains Mono, monospace' }}>{j.pointer}</TableCell>
                  <TableCell>
                    {j.batch_id ? (
                      <Typography component={RouterLink} to={`/batches/${j.batch_id}`} sx={{ fontFamily: 'JetBrains Mono, monospace', textDecoration: 'none' }}>
                        {String(j.batch_id).slice(0, 8)}
                      </Typography>
                    ) : (
                      '—'
                    )}
                  </TableCell>
                  <TableCell>
                    {j.run_at_eta}
                    <Typography variant="caption" display="block" color="text.secondary">
                      {j.run_at_label}
                    </Typography>
                  </TableCell>
                  <TableCell>{j.state}</TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </Paper>
    </Box>
  )
}
