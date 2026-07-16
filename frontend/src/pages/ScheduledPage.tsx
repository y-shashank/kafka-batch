import { useCallback, useEffect, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import InputAdornment from '@mui/material/InputAdornment'
import Stack from '@mui/material/Stack'
import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableContainer from '@mui/material/TableContainer'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import TextField from '@mui/material/TextField'
import Typography from '@mui/material/Typography'
import SearchIcon from '@mui/icons-material/Search'
import { apiGet } from '../api/client'
import { EmptyState } from '../components/EmptyState'
import { LoadingBlock } from '../components/LoadingBlock'
import { MetricCards } from '../components/MetricCards'
import { PageHeader } from '../components/PageHeader'
import { SectionCard } from '../components/SectionCard'
import { MonoLink, monoSx } from '../components/MonoLink'
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
      <PageHeader title="Scheduled" subtitle="Delayed jobs waiting in the schedule index." />
      <MetricCards
        metrics={[
          { label: 'Pending scheduled', value: data.size },
          { label: 'Backend', value: String(data.backend) },
        ]}
      />
      <SectionCard noPadding>
        <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1.5} sx={{ p: 2 }}>
          <TextField
            placeholder="Search by job ID"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') setParams(search ? { q: search } : {})
            }}
            sx={{ minWidth: { sm: 280 } }}
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <SearchIcon fontSize="small" />
                </InputAdornment>
              ),
            }}
          />
          <Button variant="contained" onClick={() => setParams(search ? { q: search } : {})}>
            Search
          </Button>
          {q ? (
            <Button
              onClick={() => {
                setSearch('')
                setParams({})
              }}
            >
              Clear
            </Button>
          ) : null}
        </Stack>
        <TableContainer>
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
                  <TableCell colSpan={4} align="center" sx={{ py: 5 }}>
                    <Typography color="text.secondary">No scheduled jobs.</Typography>
                  </TableCell>
                </TableRow>
              ) : (
                data.jobs.map((j: any) => (
                  <TableRow key={j.pointer} hover>
                    <TableCell sx={monoSx}>{j.pointer}</TableCell>
                    <TableCell>
                      {j.batch_id ? <MonoLink to={`/batches/${j.batch_id}`}>{String(j.batch_id).slice(0, 8)}</MonoLink> : '—'}
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
        </TableContainer>
      </SectionCard>
    </Box>
  )
}
