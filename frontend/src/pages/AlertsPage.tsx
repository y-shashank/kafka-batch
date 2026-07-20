import { useCallback, useEffect, useMemo, useState } from 'react'
import { Link as RouterLink } from 'react-router-dom'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Chip from '@mui/material/Chip'
import FormControlLabel from '@mui/material/FormControlLabel'
import Link from '@mui/material/Link'
import Stack from '@mui/material/Stack'
import Switch from '@mui/material/Switch'
import Table from '@mui/material/Table'
import TableBody from '@mui/material/TableBody'
import TableCell from '@mui/material/TableCell'
import TableHead from '@mui/material/TableHead'
import TableRow from '@mui/material/TableRow'
import TextField from '@mui/material/TextField'
import Typography from '@mui/material/Typography'
import Tooltip from '@mui/material/Tooltip'
import Collapse from '@mui/material/Collapse'
import { apiGet, apiMutate } from '../api/client'
import { LoadingBlock } from '../components/LoadingBlock'
import { PageHeader } from '../components/PageHeader'
import { SectionCard } from '../components/SectionCard'

type Channel = {
  id: string
  label: string
  enabled: boolean
  available: boolean
  ready_to_send: boolean
  unavailable_reason?: string | null
}

type RuleSetting = {
  key: string
  label: string
  default?: number | string
  meaning?: string
}

type RuleMeta = {
  id: string
  title: string
  description: string
  detail?: string | null
  remediation?: string | null
  link?: string | null
  settings?: RuleSetting[]
  enabled: boolean
  severity: string
  available: boolean
  unavailable_reason?: string | null
}

const SETTING_HELP: Record<string, string> = {
  lag_threshold: 'Min Kafka lag (messages) before lag_stuck_growing considers a topic.',
  lag_growth_min: 'Min lag increase between ticks (or end-offset growth) to treat as worsening.',
  rtt_avg_ms: 'Fire redis_rtt_high when latest average Redis RTT reaches this (ms).',
  rtt_max_ms: 'Fire redis_rtt_high when latest max Redis RTT reaches this (ms).',
  schedule_pending_max: 'Max ZCARD of delayed-job sched:pending before schedule_depth_high fires.',
  dlt_per_minute: 'Max dead-letter publishes in a rolling one-minute window.',
  fairness_ingest_lag: 'Min fair ingest lag that counts as backed up for a lane.',
  reconciler_max_age: 'Max seconds since last reconciler summary before reconciler_stale fires.',
  interval: 'Seconds between evaluator ticks (control plane).',
  for_ticks: 'Consecutive breach ticks required before opening/firing an incident.',
  resolve_ticks: 'Consecutive healthy ticks required before resolving an open incident.',
  cooldown_seconds: 'Legacy knob (UI advanced). Notify is once per open + once per resolve; no reminder spam.',
}

type Settings = {
  encryption_configured: boolean
  enabled: boolean
  interval: number
  for_ticks: number
  resolve_ticks: number
  cooldown_seconds: number
  lag_threshold: number
  lag_growth_min: number
  rtt_avg_ms: number
  rtt_max_ms: number
  rtt_error_rate: number
  reconciler_max_age: number
  schedule_pending_max: number
  dlt_per_minute: number
  fairness_ingest_lag: number
  fairness_ready_max_when_stuck: number
  channel_slack: boolean
  channel_webhook: boolean
  channel_email: boolean
  channel_metrics: boolean
  email_to: string
  email_from: string
  email_smtp_address: string
  email_smtp_port: number
  email_smtp_user: string
  rules: Record<string, { enabled: boolean; severity: string }>
  secrets: Record<string, { set: boolean; masked: string | null }>
}

type OpenIncident = {
  fingerprint: string
  rule_id: string
  title: string
  summary: string
  severity: string
  link?: string
  fired_at?: string
}

type SettingsResponse = {
  ok: boolean
  available?: boolean
  message?: string
  settings: Settings
  channels: Channel[]
  rules: RuleMeta[]
  status: {
    enabled: boolean
    running: boolean
    open: OpenIncident[]
    last_evaluation?: { ran_at?: string; open?: number } | null
  }
}

const emptyForm = (): Partial<Settings> & {
  slack_webhook_url?: string
  webhook_urls?: string
  email_smtp_password?: string
} => ({})

export function AlertsPage() {
  const [data, setData] = useState<SettingsResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [notice, setNotice] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)
  const [testing, setTesting] = useState<string | null>(null)
  const [advanced, setAdvanced] = useState(false)
  const [form, setForm] = useState(emptyForm())

  const load = useCallback(async () => {
    try {
      const res = await apiGet<SettingsResponse>('/api/alerts/settings')
      setData(res)
      setForm({
        enabled: res.settings.enabled,
        interval: res.settings.interval,
        for_ticks: res.settings.for_ticks,
        resolve_ticks: res.settings.resolve_ticks,
        cooldown_seconds: res.settings.cooldown_seconds,
        lag_threshold: res.settings.lag_threshold,
        lag_growth_min: res.settings.lag_growth_min,
        rtt_avg_ms: res.settings.rtt_avg_ms,
        rtt_max_ms: res.settings.rtt_max_ms,
        rtt_error_rate: res.settings.rtt_error_rate,
        reconciler_max_age: res.settings.reconciler_max_age,
        schedule_pending_max: res.settings.schedule_pending_max,
        dlt_per_minute: res.settings.dlt_per_minute,
        fairness_ingest_lag: res.settings.fairness_ingest_lag,
        fairness_ready_max_when_stuck: res.settings.fairness_ready_max_when_stuck,
        channel_slack: res.settings.channel_slack,
        channel_webhook: res.settings.channel_webhook,
        channel_email: res.settings.channel_email,
        channel_metrics: res.settings.channel_metrics,
        email_to: res.settings.email_to,
        email_from: res.settings.email_from,
        email_smtp_address: res.settings.email_smtp_address,
        email_smtp_port: res.settings.email_smtp_port,
        email_smtp_user: res.settings.email_smtp_user,
        rules: res.settings.rules || {},
        slack_webhook_url: '',
        webhook_urls: '',
        email_smtp_password: '',
      })
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load alerts settings')
    }
  }, [])

  useEffect(() => {
    void load()
  }, [load])

  const open = data?.status?.open || []
  const channels = data?.channels || []
  const rules = data?.rules || []

  const setField = <K extends string>(key: K, value: unknown) => {
    setForm((prev) => ({ ...prev, [key]: value }))
  }

  const setRule = (id: string, patch: Partial<{ enabled: boolean; severity: string }>) => {
    setForm((prev) => {
      const rulesMap = { ...(prev.rules || {}) }
      rulesMap[id] = { enabled: true, severity: 'warning', ...(rulesMap[id] || {}), ...patch }
      return { ...prev, rules: rulesMap }
    })
  }

  const save = async () => {
    setSaving(true)
    setNotice(null)
    try {
      const body: Record<string, unknown> = { ...form }
      if (!String(form.slack_webhook_url || '').trim()) delete body.slack_webhook_url
      if (!String(form.webhook_urls || '').trim()) delete body.webhook_urls
      if (!String(form.email_smtp_password || '').trim()) delete body.email_smtp_password
      const res = await apiMutate<SettingsResponse>('PUT', '/api/alerts/settings', body)
      setData(res)
      setForm((prev) => ({
        ...prev,
        ...res.settings,
        rules: res.settings.rules,
        slack_webhook_url: '',
        webhook_urls: '',
        email_smtp_password: '',
      }))
      setNotice('Settings saved — evaluator picks up changes on the next tick.')
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Save failed')
    } finally {
      setSaving(false)
    }
  }

  const testChannel = async (channel: string) => {
    setTesting(channel)
    setNotice(null)
    try {
      await apiMutate('POST', '/api/alerts/test', { channel })
      setNotice(`Test sent to ${channel}.`)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Test failed')
    } finally {
      setTesting(null)
    }
  }

  const channelById = useMemo(() => {
    const m = new Map<string, Channel>()
    channels.forEach((c) => m.set(c.id, c))
    return m
  }, [channels])

  if (!data && !error) return <LoadingBlock />

  const redisAvailable = data?.available !== false
  const alertsOn = !!form.enabled

  return (
    <Box>
      <PageHeader
        title="Health alerts"
        subtitle="Monitor lag, Redis RTT, reconciler, fairness, DLT, and more. Settings persist in Redis and apply on the next evaluator tick."
      />

      {error ? (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      ) : null}
      {notice ? (
        <Alert severity="success" sx={{ mb: 2 }} onClose={() => setNotice(null)}>
          {notice}
        </Alert>
      ) : null}

      {!redisAvailable ? (
        <Alert severity="warning" sx={{ mb: 2 }}>
          <Typography variant="subtitle2" sx={{ mb: 0.5 }}>
            Alerts are unavailable
          </Typography>
          {data?.message ||
            'Alerts require Redis. Set config.redis_url (or REDIS_URL) and ensure Redis is reachable, then reload.'}
        </Alert>
      ) : !alertsOn ? (
        <Alert severity="info" sx={{ mb: 2 }}>
          <Typography variant="subtitle2" sx={{ mb: 0.5 }}>
            Alerts are disabled
          </Typography>
          The evaluator is not firing notifications. To enable:
          <Box component="ol" sx={{ m: 0, pl: 2.5, mt: 0.75 }}>
            <li>
              Turn on <strong>Alerts enabled</strong> below and click <strong>Save settings</strong>.
            </li>
            <li>Configure at least one channel (Slack, Webhook, Email, or Metrics).</li>
            <li>
              The control-plane (Karafka) picks up changes on the next tick (~
              {form.interval ?? data?.settings?.interval ?? 60}s). UI pods do not run the evaluator unless{' '}
              <code>alerts_run_on_ui</code> is true.
            </li>
          </Box>
          Optional bootstrap before first save: <code>config.alerts_enabled = true</code> or{' '}
          <code>KAFKA_BATCH_ALERTS_ENABLED=true</code>.
        </Alert>
      ) : null}

      {!data?.settings?.encryption_configured ? (
        <Alert severity="warning" sx={{ mb: 2 }}>
          Set <code>config.ai_encryption_salt</code> to store Slack/webhook/email secrets (same salt as AI Settings).
          Thresholds and toggles still save.
        </Alert>
      ) : null}

      <Stack spacing={2} sx={{ opacity: redisAvailable ? 1 : 0.7 }}>
        <SectionCard title="Status">
          <Stack
            direction="row"
            alignItems="center"
            flexWrap="wrap"
            useFlexGap
            spacing={1}
            sx={{ mb: 2 }}
          >
            <AlignedToggle
              checked={alertsOn}
              disabled={!redisAvailable}
              onChange={(v) => setField('enabled', v)}
              label={alertsOn ? 'Alerts enabled' : 'Alerts disabled'}
            />
            <Chip
              size="small"
              label={alertsOn ? 'Enabled' : 'Disabled'}
              color={alertsOn ? 'success' : 'default'}
              variant={alertsOn ? 'filled' : 'outlined'}
            />
            <Chip
              size="small"
              label={data?.status?.running ? 'Evaluator running' : 'Evaluator idle'}
              color={data?.status?.running ? 'success' : 'default'}
              variant="outlined"
            />
            <Chip size="small" label={`${open.length} open`} variant="outlined" />
            {data?.status?.last_evaluation?.ran_at ? (
              <Chip size="small" label={`Last tick ${data.status.last_evaluation.ran_at}`} variant="outlined" />
            ) : null}
          </Stack>

          {open.length === 0 ? (
            <Typography variant="body2" color="text.secondary">
              No open alerts.
            </Typography>
          ) : (
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Severity</TableCell>
                  <TableCell>Rule</TableCell>
                  <TableCell>Summary</TableCell>
                  <TableCell>Since</TableCell>
                  <TableCell />
                </TableRow>
              </TableHead>
              <TableBody>
                {open.map((inc) => (
                  <TableRow key={inc.fingerprint}>
                    <TableCell>
                      <Chip
                        size="small"
                        label={inc.severity}
                        color={inc.severity === 'critical' ? 'error' : 'warning'}
                      />
                    </TableCell>
                    <TableCell>{inc.title || inc.rule_id}</TableCell>
                    <TableCell sx={{ maxWidth: 360 }}>{inc.summary}</TableCell>
                    <TableCell>{inc.fired_at || '—'}</TableCell>
                    <TableCell>
                      {inc.link ? (
                        <Link component={RouterLink} to={inc.link}>
                          Open
                        </Link>
                      ) : null}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </SectionCard>

        <SectionCard title="Channels">
          <Stack spacing={2}>
            <ChannelCard
              title="Slack"
              channel={channelById.get('slack')}
              enabled={!!form.channel_slack}
              onEnabled={(v) => setField('channel_slack', v)}
              secretSet={!!data?.settings?.secrets?.slack_webhook_url?.set}
              secretMasked={data?.settings?.secrets?.slack_webhook_url?.masked}
              secretLabel="Webhook URL"
              secretValue={form.slack_webhook_url || ''}
              onSecret={(v) => setField('slack_webhook_url', v)}
              onTest={() => void testChannel('slack')}
              testing={testing === 'slack'}
              encryptionOk={!!data?.settings?.encryption_configured}
            />
            <ChannelCard
              title="Webhook (NR / PagerDuty / custom)"
              channel={channelById.get('webhook')}
              enabled={!!form.channel_webhook}
              onEnabled={(v) => setField('channel_webhook', v)}
              secretSet={!!data?.settings?.secrets?.webhook_urls?.set}
              secretMasked={data?.settings?.secrets?.webhook_urls?.masked}
              secretLabel="Webhook URLs (comma or newline separated)"
              secretValue={form.webhook_urls || ''}
              onSecret={(v) => setField('webhook_urls', v)}
              onTest={() => void testChannel('webhook')}
              testing={testing === 'webhook'}
              encryptionOk={!!data?.settings?.encryption_configured}
              multiline
            />
            <Box sx={{ opacity: channelById.get('email')?.available || form.channel_email ? 1 : 0.75 }}>
              <Stack
                direction="row"
                alignItems="center"
                flexWrap="wrap"
                useFlexGap
                spacing={1}
                sx={{ mb: 1 }}
              >
                <Typography variant="subtitle2">Email</Typography>
                {channelById.get('email')?.unavailable_reason ? (
                  <Tooltip title={channelById.get('email')!.unavailable_reason!}>
                    <Chip size="small" label="not ready" variant="outlined" />
                  </Tooltip>
                ) : null}
                <AlignedToggle
                  checked={!!form.channel_email}
                  onChange={(v) => setField('channel_email', v)}
                  label="Enabled"
                />
                <Button
                  size="small"
                  disabled={!channelById.get('email')?.ready_to_send || testing === 'email'}
                  onClick={() => void testChannel('email')}
                >
                  {testing === 'email' ? 'Sending…' : 'Send test'}
                </Button>
              </Stack>
              <Stack spacing={1.25}>
                <TextField
                  size="small"
                  label="To"
                  value={form.email_to || ''}
                  onChange={(e) => setField('email_to', e.target.value)}
                  fullWidth
                />
                <TextField
                  size="small"
                  label="From"
                  value={form.email_from || ''}
                  onChange={(e) => setField('email_from', e.target.value)}
                  fullWidth
                />
                <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1}>
                  <TextField
                    size="small"
                    label="SMTP host"
                    value={form.email_smtp_address || ''}
                    onChange={(e) => setField('email_smtp_address', e.target.value)}
                    fullWidth
                  />
                  <TextField
                    size="small"
                    label="Port"
                    type="number"
                    value={form.email_smtp_port ?? 587}
                    onChange={(e) => setField('email_smtp_port', Number(e.target.value))}
                    sx={{ width: { sm: 120 } }}
                  />
                </Stack>
                <TextField
                  size="small"
                  label="SMTP user"
                  value={form.email_smtp_user || ''}
                  onChange={(e) => setField('email_smtp_user', e.target.value)}
                  fullWidth
                />
                <TextField
                  size="small"
                  type="password"
                  label={
                    data?.settings?.secrets?.email_smtp_password?.set
                      ? `SMTP password (${data.settings.secrets.email_smtp_password.masked})`
                      : 'SMTP password'
                  }
                  placeholder="Leave blank to keep existing"
                  value={form.email_smtp_password || ''}
                  onChange={(e) => setField('email_smtp_password', e.target.value)}
                  fullWidth
                  disabled={!data?.settings?.encryption_configured}
                />
              </Stack>
            </Box>
            <Stack direction="row" alignItems="center" flexWrap="wrap" useFlexGap spacing={1}>
              <Typography variant="subtitle2">Metrics</Typography>
              {channelById.get('metrics')?.unavailable_reason ? (
                <Chip size="small" label={channelById.get('metrics')!.unavailable_reason!} variant="outlined" />
              ) : null}
              <AlignedToggle
                checked={!!form.channel_metrics}
                onChange={(v) => setField('channel_metrics', v)}
                label="Emit alert.fired / alert.resolved"
              />
            </Stack>
          </Stack>
        </SectionCard>

        <SectionCard title="Rules & thresholds">
          <Stack spacing={1.5}>
            {rules.map((rule) => {
              const conf = form.rules?.[rule.id] || { enabled: rule.enabled, severity: rule.severity }
              return (
                <Box
                  key={rule.id}
                  sx={{
                    border: 1,
                    borderColor: 'divider',
                    borderRadius: 1,
                    p: 1.25,
                    opacity: rule.available ? 1 : 0.65,
                  }}
                >
                  <Stack direction="row" alignItems="center" spacing={1} flexWrap="wrap" useFlexGap>
                    <AlignedToggle
                      checked={!!conf.enabled && rule.available}
                      disabled={!rule.available}
                      onChange={(v) => setRule(rule.id, { enabled: v })}
                      label={rule.title}
                    />
                    {!rule.available ? (
                      <Tooltip title={rule.unavailable_reason || 'Unavailable'}>
                        <Chip size="small" label="unavailable" variant="outlined" />
                      </Tooltip>
                    ) : null}
                    <Chip size="small" label={conf.severity} variant="outlined" />
                    {rule.link ? (
                      <Link component={RouterLink} to={rule.link} variant="body2">
                        Open page
                      </Link>
                    ) : null}
                    <Button
                      size="small"
                      onClick={() =>
                        setRule(rule.id, {
                          severity: conf.severity === 'critical' ? 'warning' : 'critical',
                        })
                      }
                    >
                      Toggle severity
                    </Button>
                  </Stack>
                  <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
                    {rule.description}
                  </Typography>
                  {rule.detail ? (
                    <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 0.5 }}>
                      {rule.detail}
                    </Typography>
                  ) : null}
                  {rule.settings && rule.settings.length > 0 ? (
                    <Stack direction="row" flexWrap="wrap" useFlexGap spacing={0.75} sx={{ mt: 0.75 }}>
                      {rule.settings.map((s) => (
                        <Tooltip key={s.key} title={s.meaning || s.label}>
                          <Chip
                            size="small"
                            variant="outlined"
                            label={`${s.label}: ${String(
                              (form as Partial<Record<string, unknown>>)[s.key] ?? s.default ?? '—',
                            )}`}
                          />
                        </Tooltip>
                      ))}
                    </Stack>
                  ) : null}
                  {rule.remediation ? (
                    <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 0.75 }}>
                      If firing: {rule.remediation}
                    </Typography>
                  ) : null}
                </Box>
              )
            })}

            <Typography variant="subtitle2" sx={{ mt: 1 }}>
              Shared thresholds
            </Typography>
            <Stack direction={{ xs: 'column', md: 'row' }} spacing={1.25}>
              <NumField
                label="Lag threshold"
                value={form.lag_threshold}
                onChange={(v) => setField('lag_threshold', v)}
                help={SETTING_HELP.lag_threshold}
              />
              <NumField
                label="Lag growth min"
                value={form.lag_growth_min}
                onChange={(v) => setField('lag_growth_min', v)}
                help={SETTING_HELP.lag_growth_min}
              />
              <NumField
                label="RTT avg ms"
                value={form.rtt_avg_ms}
                onChange={(v) => setField('rtt_avg_ms', v)}
                float
                help={SETTING_HELP.rtt_avg_ms}
              />
              <NumField
                label="RTT max ms"
                value={form.rtt_max_ms}
                onChange={(v) => setField('rtt_max_ms', v)}
                float
                help={SETTING_HELP.rtt_max_ms}
              />
            </Stack>
            <Stack direction={{ xs: 'column', md: 'row' }} spacing={1.25}>
              <NumField
                label="Schedule pending max"
                value={form.schedule_pending_max}
                onChange={(v) => setField('schedule_pending_max', v)}
                help={SETTING_HELP.schedule_pending_max}
              />
              <NumField
                label="DLT / minute"
                value={form.dlt_per_minute}
                onChange={(v) => setField('dlt_per_minute', v)}
                help={SETTING_HELP.dlt_per_minute}
              />
              <NumField
                label="Fair ingest lag"
                value={form.fairness_ingest_lag}
                onChange={(v) => setField('fairness_ingest_lag', v)}
                help={SETTING_HELP.fairness_ingest_lag}
              />
              <NumField
                label="Reconciler max age (s)"
                value={form.reconciler_max_age}
                onChange={(v) => setField('reconciler_max_age', v)}
                help={SETTING_HELP.reconciler_max_age}
              />
            </Stack>

            <Button size="small" onClick={() => setAdvanced((v) => !v)}>
              {advanced ? 'Hide advanced' : 'Show advanced'}
            </Button>
            <Collapse in={advanced}>
              <Stack direction={{ xs: 'column', md: 'row' }} spacing={1.25} sx={{ mt: 1 }}>
                <NumField
                  label="Interval (s)"
                  value={form.interval}
                  onChange={(v) => setField('interval', v)}
                  help={SETTING_HELP.interval}
                />
                <NumField
                  label="For ticks"
                  value={form.for_ticks}
                  onChange={(v) => setField('for_ticks', v)}
                  help={SETTING_HELP.for_ticks}
                />
                <NumField
                  label="Resolve ticks"
                  value={form.resolve_ticks}
                  onChange={(v) => setField('resolve_ticks', v)}
                  help={SETTING_HELP.resolve_ticks}
                />
                <NumField
                  label="Cooldown (s)"
                  value={form.cooldown_seconds}
                  onChange={(v) => setField('cooldown_seconds', v)}
                  help={SETTING_HELP.cooldown_seconds}
                />
              </Stack>
            </Collapse>
          </Stack>
        </SectionCard>

        <Stack direction="row" spacing={1} sx={{ pt: 1, pb: 2 }}>
          <Button variant="contained" onClick={() => void save()} disabled={saving || !redisAvailable}>
            {saving ? 'Saving…' : 'Save settings'}
          </Button>
          <Button variant="outlined" onClick={() => void load()} disabled={saving}>
            Discard
          </Button>
        </Stack>
      </Stack>
    </Box>
  )
}

function AlignedToggle({
  checked,
  onChange,
  label,
  disabled,
}: {
  checked: boolean
  onChange: (v: boolean) => void
  label: string
  disabled?: boolean
}) {
  return (
    <FormControlLabel
      sx={{
        m: 0,
        alignItems: 'center',
        gap: 0.5,
        '& .MuiFormControlLabel-label': { lineHeight: 1.25 },
      }}
      control={
        <Switch size="small" checked={checked} disabled={disabled} onChange={(e) => onChange(e.target.checked)} />
      }
      label={label}
    />
  )
}

function NumField({
  label,
  value,
  onChange,
  float,
  help,
}: {
  label: string
  value: number | undefined
  onChange: (v: number) => void
  float?: boolean
  help?: string
}) {
  return (
    <TextField
      size="small"
      label={label}
      type="number"
      value={value ?? ''}
      onChange={(e) => onChange(float ? Number(e.target.value) : parseInt(e.target.value, 10) || 0)}
      fullWidth
      helperText={help}
    />
  )
}

function ChannelCard({
  title,
  channel,
  enabled,
  onEnabled,
  secretSet,
  secretMasked,
  secretLabel,
  secretValue,
  onSecret,
  onTest,
  testing,
  encryptionOk,
  multiline,
}: {
  title: string
  channel?: Channel
  enabled: boolean
  onEnabled: (v: boolean) => void
  secretSet: boolean
  secretMasked?: string | null
  secretLabel: string
  secretValue: string
  onSecret: (v: string) => void
  onTest: () => void
  testing: boolean
  encryptionOk: boolean
  multiline?: boolean
}) {
  return (
    <Box sx={{ opacity: channel?.available || enabled ? 1 : 0.75 }}>
      <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1 }} flexWrap="wrap" useFlexGap>
        <Typography variant="subtitle2">{title}</Typography>
        {channel?.unavailable_reason ? (
          <Chip size="small" label={channel.unavailable_reason} variant="outlined" />
        ) : null}
        {secretSet ? <Chip size="small" label={secretMasked || 'set'} color="success" variant="outlined" /> : null}
        <AlignedToggle checked={enabled} onChange={onEnabled} label="Enabled" />
        <Button size="small" disabled={!channel?.ready_to_send || testing} onClick={onTest}>
          {testing ? 'Sending…' : 'Send test'}
        </Button>
      </Stack>
      <TextField
        size="small"
        type={multiline ? 'text' : 'password'}
        label={secretSet ? `${secretLabel} (set)` : secretLabel}
        placeholder="Leave blank to keep existing"
        value={secretValue}
        onChange={(e) => onSecret(e.target.value)}
        fullWidth
        disabled={!encryptionOk}
        multiline={multiline}
        minRows={multiline ? 2 : undefined}
      />
    </Box>
  )
}
