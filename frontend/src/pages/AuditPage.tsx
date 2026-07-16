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
import Typography from '@mui/material/Typography'
import { apiGet } from '../api/client'
import { EmptyState } from '../components/EmptyState'
import { LoadingBlock } from '../components/LoadingBlock'
import { PageHeader } from '../components/PageHeader'
import { StatusChip } from '../components/StatusChip'
import { useLiveRefresh } from '../hooks/useLiveRefresh'

export function AuditPage() {
  const [params, setParams] = useSearchParams()
  const action = params.get('action') || ''
  const page = Math.max(1, Number(params.get('page') || 1))
  const [data, setData] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    try {
      const qs = new URLSearchParams()
      if (action) qs.set('action', action)
      if (page > 1) qs.set('page', String(page))
      setData(await apiGet(`/api/audit?${qs}`))
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load')
    }
  }, [action, page])

  useEffect(() => {
    void load()
  }, [load])
  useLiveRefresh(load)

  if (!data && !error) return <LoadingBlock />
  if (error) return <Alert severity="error">{error}</Alert>
  if (!data.enabled) return <EmptyState title="Audit log" message={data.message} />

  return (
    <Box>
      <Button component={RouterLink} to="/" sx={{ mb: 1 }}>
        ← All batches
      </Button>
      <PageHeader title="Audit log" subtitle="Mutating dashboard actions, newest first." />
      <Stack direction="row" spacing={1} sx={{ mb: 2 }} flexWrap="wrap" useFlexGap>
        <Chip label="All" clickable color={!action ? 'primary' : 'default'} onClick={() => setParams({})} />
        {(data.actions || []).map((a: string) => (
          <Chip key={a} label={a} clickable color={action === a ? 'primary' : 'default'} onClick={() => setParams({ action: a })} />
        ))}
      </Stack>
      <Paper sx={{ overflow: 'auto' }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>When (UTC)</TableCell>
              <TableCell>Actor</TableCell>
              <TableCell>Action</TableCell>
              <TableCell>Request</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Node</TableCell>
              <TableCell>Metadata</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {data.entries.length === 0 ? (
              <TableRow>
                <TableCell colSpan={7} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No audit entries recorded yet.
                </TableCell>
              </TableRow>
            ) : (
              data.entries.map((r: any) => (
                <TableRow key={r.id || `${r.created_at}-${r.action}`}>
                  <TableCell>{r.created_at_label}</TableCell>
                  <TableCell>{r.actor || '—'}</TableCell>
                  <TableCell sx={{ fontFamily: 'JetBrains Mono, monospace' }}>{r.action}</TableCell>
                  <TableCell sx={{ fontFamily: 'JetBrains Mono, monospace' }}>
                    {r.method} {r.path}
                  </TableCell>
                  <TableCell>
                    <StatusChip status={r.status} />
                  </TableCell>
                  <TableCell sx={{ fontFamily: 'JetBrains Mono, monospace' }}>{String(r.node_id || '').slice(0, 8)}</TableCell>
                  <TableCell>
                    <code>{r.metadata_preview}</code>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </Paper>
      <Stack direction="row" spacing={1} justifyContent="center" sx={{ mt: 2 }}>
        <Button
          disabled={page <= 1}
          onClick={() => {
            const next: Record<string, string> = { page: String(page - 1) }
            if (action) next.action = action
            setParams(next)
          }}
        >
          ← Prev
        </Button>
        <Typography variant="body2" color="text.secondary" sx={{ alignSelf: 'center' }}>
          Page {page}
        </Typography>
        <Button
          disabled={!data.has_next}
          onClick={() => {
            const next: Record<string, string> = { page: String(page + 1) }
            if (action) next.action = action
            setParams(next)
          }}
        >
          Next →
        </Button>
      </Stack>
    </Box>
  )
}
