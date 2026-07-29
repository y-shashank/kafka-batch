import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Chip from '@mui/material/Chip'
import Divider from '@mui/material/Divider'
import FormControlLabel from '@mui/material/FormControlLabel'
import MenuItem from '@mui/material/MenuItem'
import Stack from '@mui/material/Stack'
import Switch from '@mui/material/Switch'
import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableContainer from '@mui/material/TableContainer'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import TextField from '@mui/material/TextField'
import Typography from '@mui/material/Typography'
import { apiGet, apiMutate } from '../api/client'
import { EmptyState } from '../components/EmptyState'
import { LoadingBlock } from '../components/LoadingBlock'
import { MetricCards } from '../components/MetricCards'
import { PageHeader } from '../components/PageHeader'
import { SectionCard } from '../components/SectionCard'
import { monoSx } from '../components/MonoLink'
import { useLiveRefresh } from '../hooks/useLiveRefresh'

type Settings = {
  enabled: boolean
  dry_run: boolean
  mitigation: string
  window_seconds: number
  min_samples: number
  error_rate_pct: number
  include_retries: boolean
  throttle_weight: number
  auto_release_seconds: number | null
  grace_ticks: number
  reconcile_interval: number
}

type Record = {
  tenant_id: string
  state: string
  lane: string
  action: string
  source: string
  reason?: string
  partition?: string
  original_weight?: string
  effective_weight?: string
  created_at?: string
  until?: string
}

type Action = {
  id?: string
  tenant_id?: string
  action?: string
  source?: string
  outcome?: string
  reason?: string
  created_at?: string
  released_at?: string
  released_by?: string
}

type Status = {
  ok: boolean
  available: boolean
  enabled: boolean
  message?: string
  settings: Settings
  active: Record[]
  actions: Action[]
  server_time: number
}

const MITIGATIONS = ['none', 'throttle', 'pause', 'throttle_then_pause']

function fmtTs(epoch?: string): string {
  const n = Number(epoch)
  if (!n) return '—'
  return new Date(n * 1000).toLocaleString()
}

export function TenantGuardPage() {
  const [data, setData] = useState<Status | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [notice, setNotice] = useState<string | null>(null)
  const [draft, setDraft] = useState<Partial<Settings> | null>(null)
  const [saving, setSaving] = useState(false)

  // Manual control form.
  const [mTenant, setMTenant] = useState('')
  const [mLane, setMLane] = useState('time')
  const [mAction, setMAction] = useState('throttle')
  const [mWeight, setMWeight] = useState('0.1')
  const [mDuration, setMDuration] = useState('')
  const [mReason, setMReason] = useState('')

  // Skew between server clock and this browser, captured at fetch time, so the
  // countdown does not depend on the client clock being correct.
  const skewRef = useRef(0)
  const [nowTick, setNowTick] = useState(() => Math.floor(Date.now() / 1000))

  const load = useCallback(async () => {
    try {
      const res = await apiGet<Status>('/api/tenant_guard')
      skewRef.current = res.server_time - Math.floor(Date.now() / 1000)
      setData(res)
      setDraft((prev) => prev ?? res.settings)
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load tenant guard')
    }
  }, [])

  useEffect(() => {
    void load()
  }, [load])

  const { live, toggle } = useLiveRefresh(load)

  // 1s ticker for the countdown display.
  useEffect(() => {
    const id = window.setInterval(() => setNowTick(Math.floor(Date.now() / 1000)), 1000)
    return () => window.clearInterval(id)
  }, [])

  const serverNow = nowTick + skewRef.current

  const metrics = useMemo(() => {
    const active = data?.active ?? []
    const paused = active.filter((r) => r.state === 'paused').length
    const throttled = active.filter((r) => r.state === 'throttled').length
    return [
      { label: 'Active controls', value: String(active.length) },
      { label: 'Paused', value: String(paused) },
      { label: 'Throttled', value: String(throttled) },
      { label: 'Guard', value: data?.enabled ? 'on' : 'off' },
    ]
  }, [data])

  const flash = (msg: string) => {
    setNotice(msg)
    window.setTimeout(() => setNotice(null), 4000)
  }

  const saveSettings = async () => {
    if (!draft) return
    setSaving(true)
    try {
      const res = await apiMutate<{ settings: Settings }>('PUT', '/api/tenant_guard/settings', draft)
      setDraft(res.settings)
      flash('Settings saved.')
      await load()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to save settings')
    } finally {
      setSaving(false)
    }
  }

  const applyManual = async () => {
    if (!mTenant.trim()) {
      setError('tenant_id is required')
      return
    }
    try {
      const path = mAction === 'pause' ? '/api/tenant_guard/pause' : '/api/tenant_guard/throttle'
      await apiMutate('POST', path, {
        tenant_id: mTenant.trim(),
        lane: mLane,
        weight: mAction === 'throttle' ? mWeight : undefined,
        until_seconds: mDuration,
        reason: mReason || 'manual (dashboard)',
      })
      flash(`${mAction === 'pause' ? 'Paused' : 'Throttled'} ${mTenant.trim()}.`)
      setMTenant('')
      setMReason('')
      await load()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to apply control')
    }
  }

  const reset = async (tenant: string) => {
    try {
      await apiMutate('POST', '/api/tenant_guard/reset', { tenant_id: tenant })
      flash(`Reset ${tenant}.`)
      await load()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to reset')
    }
  }

  const resetAll = async () => {
    try {
      const res = await apiMutate<{ reset: number }>('POST', '/api/tenant_guard/reset_all', {})
      flash(`Reset ${res.reset} tenant(s).`)
      await load()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to reset all')
    }
  }

  if (!data) {
    return (
      <>
        <PageHeader title="Tenant guard" subtitle="Per-tenant error-rate pause & throttle (fairness lanes)" />
        {error ? <Alert severity="error">{error}</Alert> : <LoadingBlock />}
      </>
    )
  }

  const s = draft ?? data.settings
  const setS = (patch: Partial<Settings>) => setDraft((prev) => ({ ...(prev ?? data.settings), ...patch }))

  const countdown = (until?: string): string => {
    const u = Number(until)
    if (!u) return 'manual — no expiry'
    const rem = u - serverNow
    if (rem <= 0) return 'releasing…'
    const m = Math.floor(rem / 60)
    const sec = rem % 60
    return m > 0 ? `${m}m ${sec}s` : `${sec}s`
  }

  return (
    <>
      <PageHeader
        title="Tenant guard"
        subtitle="Per-tenant error-rate pause & throttle (fairness lanes only). Pause reuses the ingest-partition pause; throttle reuses the fairness weight."
        actions={
          <FormControlLabel
            control={<Switch checked={live} onChange={toggle} size="small" />}
            label="Live"
          />
        }
      />

      {!data.available ? <Alert severity="warning" sx={{ mb: 2 }}>{data.message}</Alert> : null}
      {error ? <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>{error}</Alert> : null}
      {notice ? <Alert severity="success" sx={{ mb: 2 }}>{notice}</Alert> : null}
      {s.dry_run ? <Alert severity="info" sx={{ mb: 2 }}>Dry-run is ON — the guard evaluates and fires callbacks but takes no action.</Alert> : null}

      <MetricCards metrics={metrics} />

      <SectionCard title="Global triggers" subheader="Applies to all tenants. Saved to Redis; effective across the fleet without a redeploy.">
        <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', sm: '1fr 1fr' }, gap: 2 }}>
          <FormControlLabel
            control={<Switch checked={!!s.enabled} onChange={(e) => setS({ enabled: e.target.checked })} />}
            label="Guard enabled"
          />
          <FormControlLabel
            control={<Switch checked={!!s.dry_run} onChange={(e) => setS({ dry_run: e.target.checked })} />}
            label="Dry-run (evaluate + callback, no action)"
          />
          <TextField
            select label="Mitigation" size="small" value={s.mitigation}
            onChange={(e) => setS({ mitigation: e.target.value })}
          >
            {MITIGATIONS.map((m) => <MenuItem key={m} value={m}>{m}</MenuItem>)}
          </TextField>
          <TextField
            label="Error rate threshold (%)" size="small" type="number" value={s.error_rate_pct}
            onChange={(e) => setS({ error_rate_pct: Number(e.target.value) })}
          />
          <TextField
            label="Window (seconds)" size="small" type="number" value={s.window_seconds}
            onChange={(e) => setS({ window_seconds: Number(e.target.value) })}
          />
          <TextField
            label="Min samples" size="small" type="number" value={s.min_samples}
            onChange={(e) => setS({ min_samples: Number(e.target.value) })}
          />
          <TextField
            label="Throttle weight" size="small" type="number" value={s.throttle_weight}
            onChange={(e) => setS({ throttle_weight: Number(e.target.value) })}
          />
          <TextField
            label="Auto-release (seconds, blank = manual)" size="small" type="number"
            value={s.auto_release_seconds ?? ''}
            onChange={(e) => setS({ auto_release_seconds: e.target.value === '' ? null : Number(e.target.value) })}
          />
          <TextField
            label="Grace ticks (warn before acting)" size="small" type="number" value={s.grace_ticks}
            onChange={(e) => setS({ grace_ticks: Number(e.target.value) })}
          />
          <FormControlLabel
            control={<Switch checked={!!s.include_retries} onChange={(e) => setS({ include_retries: e.target.checked })} />}
            label="Count retries as failures"
          />
        </Box>
        <Divider sx={{ my: 2 }} />
        <Button variant="contained" onClick={saveSettings} disabled={saving || !data.available}>
          {saving ? 'Saving…' : 'Save settings'}
        </Button>
      </SectionCard>

      <SectionCard
        title="Active controls"
        subheader="Paused / throttled tenants (manual or automatic). Reset resumes the partition / restores the weight."
        action={
          <Button size="small" color="warning" variant="outlined" onClick={resetAll} disabled={!data.active.length}>
            Reset all
          </Button>
        }
      >
        {data.active.length === 0 ? (
          <EmptyState message="No tenants under a guard control right now." />
        ) : (
          <TableContainer>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Tenant</TableCell>
                  <TableCell>State</TableCell>
                  <TableCell>Lane</TableCell>
                  <TableCell>Source</TableCell>
                  <TableCell>Detail</TableCell>
                  <TableCell>Since</TableCell>
                  <TableCell>Auto-reset</TableCell>
                  <TableCell align="right">Action</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {data.active.map((r) => (
                  <TableRow key={r.tenant_id}>
                    <TableCell sx={monoSx}>{r.tenant_id}</TableCell>
                    <TableCell>
                      <Chip
                        size="small"
                        label={r.state}
                        color={r.state === 'paused' ? 'error' : r.state === 'throttled' ? 'warning' : 'default'}
                      />
                    </TableCell>
                    <TableCell>{r.lane}</TableCell>
                    <TableCell>
                      <Chip size="small" variant="outlined" label={r.source === 'error_rate_guard' ? 'auto' : r.source} />
                    </TableCell>
                    <TableCell sx={monoSx}>
                      {r.state === 'paused'
                        ? `partition ${r.partition}`
                        : `weight ${r.effective_weight} (was ${r.original_weight || 'default'})`}
                    </TableCell>
                    <TableCell>{fmtTs(r.created_at)}</TableCell>
                    <TableCell>{countdown(r.until)}</TableCell>
                    <TableCell align="right">
                      <Button size="small" onClick={() => reset(r.tenant_id)}>Reset</Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </SectionCard>

      <SectionCard title="Manual control" subheader="Pause or throttle a specific tenant now. Leave duration blank for manual-only (no auto-reset).">
        <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: 'repeat(6, 1fr)' }, gap: 1.5, alignItems: 'center' }}>
          <TextField label="Tenant" size="small" value={mTenant} onChange={(e) => setMTenant(e.target.value)} />
          <TextField select label="Lane" size="small" value={mLane} onChange={(e) => setMLane(e.target.value)}>
            <MenuItem value="time">time</MenuItem>
            <MenuItem value="throughput">throughput</MenuItem>
          </TextField>
          <TextField select label="Action" size="small" value={mAction} onChange={(e) => setMAction(e.target.value)}>
            <MenuItem value="throttle">throttle</MenuItem>
            <MenuItem value="pause">pause</MenuItem>
          </TextField>
          <TextField
            label="Weight" size="small" type="number" value={mWeight}
            disabled={mAction !== 'throttle'} onChange={(e) => setMWeight(e.target.value)}
          />
          <TextField label="Duration (s)" size="small" type="number" value={mDuration} onChange={(e) => setMDuration(e.target.value)} />
          <Button variant="contained" onClick={applyManual} disabled={!data.available}>Apply</Button>
        </Box>
        <TextField
          label="Reason" size="small" fullWidth sx={{ mt: 1.5 }} value={mReason}
          onChange={(e) => setMReason(e.target.value)}
        />
      </SectionCard>

      <SectionCard title="Recent actions" subheader="Audit log of guard actions (manual and automatic).">
        {data.actions.length === 0 ? (
          <EmptyState message="No actions recorded yet." />
        ) : (
          <TableContainer>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>When</TableCell>
                  <TableCell>Tenant</TableCell>
                  <TableCell>Action</TableCell>
                  <TableCell>Source</TableCell>
                  <TableCell>Outcome</TableCell>
                  <TableCell>Reason</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {data.actions.map((a, i) => (
                  <TableRow key={a.id || i}>
                    <TableCell>{fmtTs(a.created_at)}</TableCell>
                    <TableCell sx={monoSx}>{a.tenant_id}</TableCell>
                    <TableCell>{a.action}</TableCell>
                    <TableCell>{a.source === 'error_rate_guard' ? 'auto' : a.source}</TableCell>
                    <TableCell>
                      <Chip
                        size="small"
                        variant="outlined"
                        label={a.outcome || 'active'}
                        color={a.outcome === 'active' ? 'primary' : 'default'}
                      />
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2" color="text.secondary" noWrap sx={{ maxWidth: 360 }}>
                        {a.reason}
                      </Typography>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        )}
      </SectionCard>
    </>
  )
}
