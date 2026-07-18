import { useCallback, useEffect, useState } from 'react'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Chip from '@mui/material/Chip'
import Dialog from '@mui/material/Dialog'
import DialogActions from '@mui/material/DialogActions'
import DialogContent from '@mui/material/DialogContent'
import DialogTitle from '@mui/material/DialogTitle'
import IconButton from '@mui/material/IconButton'
import MenuItem from '@mui/material/MenuItem'
import Snackbar from '@mui/material/Snackbar'
import Stack from '@mui/material/Stack'
import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableContainer from '@mui/material/TableContainer'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import TextField from '@mui/material/TextField'
import Tooltip from '@mui/material/Tooltip'
import Typography from '@mui/material/Typography'
import AddIcon from '@mui/icons-material/Add'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import PauseIcon from '@mui/icons-material/Pause'
import ResumeIcon from '@mui/icons-material/NotStartedOutlined'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { apiGet, apiMutate } from '../api/client'
import { EmptyState } from '../components/EmptyState'
import { LoadingBlock } from '../components/LoadingBlock'
import { MetricCards } from '../components/MetricCards'
import { PageHeader } from '../components/PageHeader'
import { SectionCard } from '../components/SectionCard'
import { monoSx } from '../components/MonoLink'
import { useLiveRefresh } from '../hooks/useLiveRefresh'

const MISFIRE_OPTIONS = ['fire_once', 'skip', 'backfill']

const EMPTY_FORM = {
  name: '',
  cron: '',
  job_type: '',
  timezone: 'UTC',
  tenant_id: '',
  misfire_policy: 'fire_once',
  args: '{}',
}

function healthChip(health: string) {
  if (health === 'stale') return <Chip size="small" color="error" label="stale" />
  if (health === 'paused') return <Chip size="small" variant="outlined" label="paused" />
  return <Chip size="small" color="success" label="ok" />
}

function fmtSeconds(s: number | null | undefined) {
  if (s == null) return '—'
  if (s < 60) return `${s}s`
  if (s < 3600) return `${Math.round(s / 60)}m`
  if (s < 86400) return `${Math.round(s / 3600)}h`
  return `${Math.round(s / 86400)}d`
}

export function RecurringPage() {
  const [data, setData] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)
  const [dialogOpen, setDialogOpen] = useState(false)
  const [form, setForm] = useState(EMPTY_FORM)
  const [editing, setEditing] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const [formError, setFormError] = useState<string | null>(null)
  const [toast, setToast] = useState<string | null>(null)

  const load = useCallback(async () => {
    try {
      setData(await apiGet('/api/recurring'))
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load')
    }
  }, [])

  useEffect(() => {
    void load()
  }, [load])
  useLiveRefresh(load)

  const openCreate = () => {
    setForm(EMPTY_FORM)
    setEditing(false)
    setFormError(null)
    setDialogOpen(true)
  }

  const openEdit = (s: any) => {
    setForm({
      name: s.name,
      cron: s.cron,
      job_type: s.job_type,
      timezone: s.timezone || 'UTC',
      tenant_id: s.tenant_id || '',
      misfire_policy: s.misfire_policy || 'fire_once',
      args: JSON.stringify(s.args ?? {}, null, 0),
    })
    setEditing(true)
    setFormError(null)
    setDialogOpen(true)
  }

  const submit = async () => {
    setSubmitting(true)
    setFormError(null)
    try {
      // Validate args JSON client-side for a friendlier message.
      if (form.args.trim()) JSON.parse(form.args)
      await apiMutate('POST', '/api/recurring', {
        ...form,
        args: form.args.trim() || '{}',
        enabled: true,
      })
      setDialogOpen(false)
      setToast(editing ? `Updated ${form.name}` : `Registered ${form.name}`)
      await load()
    } catch (e) {
      setFormError(e instanceof Error ? e.message : 'Failed to save')
    } finally {
      setSubmitting(false)
    }
  }

  const runNow = async (name: string) => {
    try {
      const res: any = await apiMutate('POST', `/api/recurring/${encodeURIComponent(name)}/run`)
      setToast(`Enqueued ${name} → ${res.job_id}`)
      await load()
    } catch (e) {
      setToast(e instanceof Error ? e.message : 'Run failed')
    }
  }

  const toggleEnabled = async (s: any) => {
    try {
      await apiMutate('POST', `/api/recurring/${encodeURIComponent(s.name)}`, { enabled: !s.enabled })
      setToast(`${s.enabled ? 'Paused' : 'Resumed'} ${s.name}`)
      await load()
    } catch (e) {
      setToast(e instanceof Error ? e.message : 'Toggle failed')
    }
  }

  const remove = async (name: string) => {
    if (!window.confirm(`Delete schedule "${name}"? This cannot be undone.`)) return
    try {
      await apiMutate('DELETE', `/api/recurring/${encodeURIComponent(name)}`)
      setToast(`Deleted ${name}`)
      await load()
    } catch (e) {
      setToast(e instanceof Error ? e.message : 'Delete failed')
    }
  }

  if (!data && !error) return <LoadingBlock />
  if (error) return <Alert severity="error">{error}</Alert>

  const available = data?.available
  const summary = data?.summary || { total: 0, enabled: 0, stale: 0 }
  const schedules: any[] = data?.schedules || []

  return (
    <Box>
      <PageHeader
        title="Recurring"
        subtitle="Cron schedules fired by the Go control plane."
        actions={
          <Button variant="contained" startIcon={<AddIcon />} onClick={openCreate}>
            New schedule
          </Button>
        }
      />

      {!available ? (
        <EmptyState title="Recurring schedules" message={data?.message} />
      ) : (
        <>
          <MetricCards
            metrics={[
              { label: 'Schedules', value: summary.total },
              { label: 'Enabled', value: summary.enabled },
              { label: 'Stale', value: summary.stale },
            ]}
          />
          <SectionCard noPadding>
            <TableContainer>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Health</TableCell>
                    <TableCell>Name</TableCell>
                    <TableCell>Cron</TableCell>
                    <TableCell>Job type</TableCell>
                    <TableCell>Misfire</TableCell>
                    <TableCell>Next run</TableCell>
                    <TableCell>Last fire</TableCell>
                    <TableCell>Idle</TableCell>
                    <TableCell align="right">Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {schedules.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={9} align="center" sx={{ py: 5 }}>
                        <Typography color="text.secondary">No recurring schedules.</Typography>
                      </TableCell>
                    </TableRow>
                  ) : (
                    schedules.map((s) => (
                      <TableRow key={s.id} hover>
                        <TableCell>{healthChip(s.health)}</TableCell>
                        <TableCell>
                          <Box
                            component="button"
                            onClick={() => openEdit(s)}
                            sx={{
                              border: 0,
                              background: 'none',
                              p: 0,
                              cursor: 'pointer',
                              color: 'primary.main',
                              font: 'inherit',
                              textAlign: 'left',
                            }}
                          >
                            {s.name}
                          </Box>
                          {s.tenant_id ? (
                            <Typography variant="caption" display="block" color="text.secondary">
                              tenant: {s.tenant_id}
                            </Typography>
                          ) : null}
                        </TableCell>
                        <TableCell sx={monoSx}>
                          {s.cron}
                          <Typography variant="caption" display="block" color="text.secondary">
                            {s.timezone}
                          </Typography>
                        </TableCell>
                        <TableCell sx={monoSx}>{s.job_type}</TableCell>
                        <TableCell>{s.misfire_policy}</TableCell>
                        <TableCell>
                          {s.next_run_eta}
                          <Typography variant="caption" display="block" color="text.secondary">
                            {s.next_run_label}
                          </Typography>
                        </TableCell>
                        <TableCell>{s.last_fire_label || '—'}</TableCell>
                        <TableCell>
                          <Tooltip
                            title={
                              s.stale_threshold_seconds
                                ? `stale threshold ${fmtSeconds(s.stale_threshold_seconds)}`
                                : 'interval unknown'
                            }
                          >
                            <span>{fmtSeconds(s.idle_seconds)}</span>
                          </Tooltip>
                        </TableCell>
                        <TableCell align="right">
                          <Tooltip title="Run now">
                            <IconButton size="small" onClick={() => runNow(s.name)}>
                              <PlayArrowIcon fontSize="small" />
                            </IconButton>
                          </Tooltip>
                          <Tooltip title={s.enabled ? 'Pause' : 'Resume'}>
                            <IconButton size="small" onClick={() => toggleEnabled(s)}>
                              {s.enabled ? <PauseIcon fontSize="small" /> : <ResumeIcon fontSize="small" />}
                            </IconButton>
                          </Tooltip>
                          <Tooltip title="Delete">
                            <IconButton size="small" color="error" onClick={() => remove(s.name)}>
                              <DeleteOutlineIcon fontSize="small" />
                            </IconButton>
                          </Tooltip>
                        </TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </TableContainer>
          </SectionCard>
        </>
      )}

      <Dialog open={dialogOpen} onClose={() => setDialogOpen(false)} fullWidth maxWidth="sm">
        <DialogTitle>{editing ? `Edit ${form.name}` : 'New recurring schedule'}</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 1 }}>
            {formError ? <Alert severity="error">{formError}</Alert> : null}
            <TextField
              label="Name"
              value={form.name}
              disabled={editing}
              onChange={(e) => setForm({ ...form, name: e.target.value })}
              helperText={editing ? 'Name is the identity key and cannot be changed.' : 'Unique identifier'}
              fullWidth
            />
            <TextField
              label="Cron expression"
              value={form.cron}
              onChange={(e) => setForm({ ...form, cron: e.target.value })}
              placeholder="*/5 * * * *  or  @daily"
              sx={{ '& input': monoSx }}
              fullWidth
            />
            <TextField
              label="Job type"
              value={form.job_type}
              onChange={(e) => setForm({ ...form, job_type: e.target.value })}
              helperText="Must resolve to a handler in the manifest"
              sx={{ '& input': monoSx }}
              fullWidth
            />
            <Stack direction="row" spacing={2}>
              <TextField
                label="Timezone"
                value={form.timezone}
                onChange={(e) => setForm({ ...form, timezone: e.target.value })}
                placeholder="UTC"
                fullWidth
              />
              <TextField
                label="Tenant ID"
                value={form.tenant_id}
                onChange={(e) => setForm({ ...form, tenant_id: e.target.value })}
                helperText="For fair.* handlers"
                fullWidth
              />
            </Stack>
            <TextField
              select
              label="Misfire policy"
              value={form.misfire_policy}
              onChange={(e) => setForm({ ...form, misfire_policy: e.target.value })}
              fullWidth
            >
              {MISFIRE_OPTIONS.map((o) => (
                <MenuItem key={o} value={o}>
                  {o}
                </MenuItem>
              ))}
            </TextField>
            <TextField
              label="Args (JSON)"
              value={form.args}
              onChange={(e) => setForm({ ...form, args: e.target.value })}
              multiline
              minRows={2}
              sx={{ '& textarea': monoSx }}
              fullWidth
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDialogOpen(false)}>Cancel</Button>
          <Button variant="contained" onClick={submit} disabled={submitting}>
            {editing ? 'Save' : 'Register'}
          </Button>
        </DialogActions>
      </Dialog>

      <Snackbar
        open={!!toast}
        autoHideDuration={4000}
        onClose={() => setToast(null)}
        message={toast}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      />
    </Box>
  )
}
