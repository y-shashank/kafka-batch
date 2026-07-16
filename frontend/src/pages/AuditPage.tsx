import { useCallback, useEffect, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Chip from '@mui/material/Chip'
import Stack from '@mui/material/Stack'
import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableContainer from '@mui/material/TableContainer'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import Typography from '@mui/material/Typography'
import { apiGet } from '../api/client'
import { EmptyState } from '../components/EmptyState'
import { LoadingBlock } from '../components/LoadingBlock'
import { PageHeader } from '../components/PageHeader'
import { StatusChip } from '../components/StatusChip'
import { SectionCard } from '../components/SectionCard'
import { monoSx } from '../components/MonoLink'
import { useLiveRefresh } from '../hooks/useLiveRefresh'
import { PaginationBar } from '../components/PaginationBar'

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
      <PageHeader title="Audit log" subtitle="Mutating dashboard actions, newest first." />
      <SectionCard noPadding>
        <Stack direction="row" spacing={1} sx={{ px: 2, pt: 2, pb: 1 }} flexWrap="wrap" useFlexGap>
          <Chip label="All" clickable color={!action ? 'primary' : 'default'} variant={!action ? 'filled' : 'outlined'} onClick={() => setParams({})} />
          {(data.actions || []).map((a: string) => (
            <Chip
              key={a}
              label={a}
              clickable
              color={action === a ? 'primary' : 'default'}
              variant={action === a ? 'filled' : 'outlined'}
              onClick={() => setParams({ action: a })}
            />
          ))}
        </Stack>
        <TableContainer>
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
                  <TableCell colSpan={7} align="center" sx={{ py: 5 }}>
                    <Typography color="text.secondary">No audit entries recorded yet.</Typography>
                  </TableCell>
                </TableRow>
              ) : (
                data.entries.map((r: any) => (
                  <TableRow key={r.id || `${r.created_at}-${r.action}`} hover>
                    <TableCell sx={{ whiteSpace: 'nowrap' }}>{r.created_at_label}</TableCell>
                    <TableCell>{r.actor || '—'}</TableCell>
                    <TableCell sx={monoSx}>{r.action}</TableCell>
                    <TableCell sx={monoSx}>
                      {r.method} {r.path}
                    </TableCell>
                    <TableCell>
                      <StatusChip status={r.status} />
                    </TableCell>
                    <TableCell sx={monoSx}>{String(r.node_id || '').slice(0, 8)}</TableCell>
                    <TableCell sx={{ maxWidth: 280 }}>
                      <Typography component="code" sx={{ ...monoSx, wordBreak: 'break-all' }}>
                        {r.metadata_preview}
                      </Typography>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </TableContainer>
        <Box sx={{ p: 2 }}>
          <PaginationBar
            page={page}
            hasNext={!!data.has_next}
            onPrev={() => {
              const next: Record<string, string> = { page: String(page - 1) }
              if (action) next.action = action
              setParams(next)
            }}
            onNext={() => {
              const next: Record<string, string> = { page: String(page + 1) }
              if (action) next.action = action
              setParams(next)
            }}
          />
        </Box>
      </SectionCard>
    </Box>
  )
}
