import { useCallback, useEffect, useMemo, useState } from 'react'
import Alert from '@mui/material/Alert'
import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import FormControl from '@mui/material/FormControl'
import InputLabel from '@mui/material/InputLabel'
import MenuItem from '@mui/material/MenuItem'
import Select from '@mui/material/Select'
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
import { apiGet } from '../api/client'
import { EmptyState } from '../components/EmptyState'
import { LoadingBlock } from '../components/LoadingBlock'
import { MetricCards } from '../components/MetricCards'
import { PageHeader } from '../components/PageHeader'
import { PaginationBar } from '../components/PaginationBar'
import { SectionCard } from '../components/SectionCard'
import { monoSx } from '../components/MonoLink'

type PartitionMeta = {
  partition: number
  committed: number | null
  end_offset: number | null
  lag: number
}

type BrowseTopic = {
  group: string
  topic: string
  partitions: number
  lag: number
  partition_meta: PartitionMeta[]
}

type BrowseMessage = {
  topic: string
  partition: number
  offset: number
  timestamp: number | null
  timestamp_label: string | null
  job_id: string
  batch_id: string | null
  job_type: string | null
  worker_class: string | null
  tenant_id: string | null
  attempt: number | null
  payload_preview: string
  payload_bytes: number
}

function topicKey(t: BrowseTopic) {
  return `${t.group}::${t.topic}`
}

export function BrowsePage() {
  const [topics, setTopics] = useState<BrowseTopic[] | null>(null)
  const [topicsAvailable, setTopicsAvailable] = useState(true)
  const [topicsMessage, setTopicsMessage] = useState<string | null>(null)
  const [selectedKey, setSelectedKey] = useState('')
  const [partition, setPartition] = useState('')
  const [startOffset, setStartOffset] = useState('')
  const [appliedPartition, setAppliedPartition] = useState('')
  const [appliedOffset, setAppliedOffset] = useState('')
  const [cursor, setCursor] = useState('')
  const [cursorStack, setCursorStack] = useState<string[]>([])
  const [pageData, setPageData] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)
  const [loadingTopics, setLoadingTopics] = useState(true)
  const [loadingMessages, setLoadingMessages] = useState(false)

  const selected = useMemo(
    () => (topics || []).find((t) => topicKey(t) === selectedKey) || null,
    [topics, selectedKey],
  )

  const loadTopics = useCallback(async () => {
    try {
      setLoadingTopics(true)
      const data = await apiGet<{
        available?: boolean
        message?: string
        topics?: BrowseTopic[]
      }>('/api/browse/topics')
      setTopics(data.topics || [])
      setTopicsAvailable(data.available !== false)
      setTopicsMessage(data.message || null)
      setError(null)
      setSelectedKey((prev) => prev || (data.topics?.length ? topicKey(data.topics[0]) : ''))
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load topics')
      setTopics([])
    } finally {
      setLoadingTopics(false)
    }
  }, [])

  const loadMessages = useCallback(async () => {
    if (!selected) {
      setPageData(null)
      return
    }
    try {
      setLoadingMessages(true)
      const qs = new URLSearchParams({
        topic: selected.topic,
        group: selected.group,
        limit: '50',
      })
      if (appliedPartition !== '') qs.set('partition', appliedPartition)
      if (appliedOffset !== '') qs.set('start_offset', appliedOffset)
      if (cursor) qs.set('cursor', cursor)
      setPageData(await apiGet(`/api/browse/messages?${qs}`))
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load messages')
      setPageData(null)
    } finally {
      setLoadingMessages(false)
    }
  }, [selected, appliedPartition, appliedOffset, cursor])

  useEffect(() => {
    void loadTopics()
  }, [loadTopics])

  useEffect(() => {
    void loadMessages()
  }, [loadMessages])

  const applyFilters = () => {
    setCursor('')
    setCursorStack([])
    setAppliedPartition(partition)
    setAppliedOffset(startOffset.trim())
  }

  const resetFilters = () => {
    setPartition('')
    setStartOffset('')
    setAppliedPartition('')
    setAppliedOffset('')
    setCursor('')
    setCursorStack([])
  }

  const onSelectTopic = (key: string) => {
    setSelectedKey(key)
    setPartition('')
    setStartOffset('')
    setAppliedPartition('')
    setAppliedOffset('')
    setCursor('')
    setCursorStack([])
  }

  if (loadingTopics && !topics) return <LoadingBlock />
  if (error && !topics) return <Alert severity="error">{error}</Alert>

  const messages: BrowseMessage[] = pageData?.messages || []
  const page = cursorStack.length + 1
  const partitionOptions = selected?.partition_meta || []

  return (
    <Box>
      <PageHeader
        title="Browse jobs"
        subtitle="Unprocessed jobs only (same consumer-group lag as Kafka lag). Messages at or after the committed offset, 50 per page from the broker."
      />

      {error ? (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      ) : null}

      {!topicsAvailable ? (
        <EmptyState message={topicsMessage || 'Browse unavailable — Karafka admin / broker access required.'} />
      ) : (
        <>
          <SectionCard title="Topic">
            <Stack
              direction={{ xs: 'column', sm: 'row' }}
              spacing={1.5}
              alignItems={{ xs: 'stretch', sm: 'center' }}
              flexWrap="wrap"
              useFlexGap
            >
              <FormControl size="small" sx={{ flex: '1 1 280px', minWidth: 200, maxWidth: 560 }}>
                <InputLabel id="browse-topic-label">Topic</InputLabel>
                <Select
                  labelId="browse-topic-label"
                  label="Topic"
                  value={selectedKey}
                  onChange={(e) => onSelectTopic(e.target.value)}
                  renderValue={(key) => {
                    const t = (topics || []).find((row) => topicKey(row) === key)
                    if (!t) return key
                    return `${t.topic} (${t.group} · lag ${t.lag})`
                  }}
                >
                  {(topics || []).map((t) => (
                    <MenuItem key={topicKey(t)} value={topicKey(t)}>
                      {t.topic}
                      <Typography component="span" variant="body2" color="text.secondary" sx={{ ml: 1 }}>
                        ({t.group} · lag {t.lag})
                      </Typography>
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
              <FormControl size="small" sx={{ width: { xs: '100%', sm: 140 } }}>
                <InputLabel id="browse-part-label">Partition</InputLabel>
                <Select
                  labelId="browse-part-label"
                  label="Partition"
                  value={partition}
                  onChange={(e) => setPartition(e.target.value)}
                >
                  <MenuItem value="">All</MenuItem>
                  {partitionOptions.map((p) => (
                    <MenuItem key={p.partition} value={String(p.partition)}>
                      {p.partition}
                      {p.lag != null ? ` (lag ${p.lag})` : ''}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
              <TextField
                size="small"
                label="Start offset"
                placeholder="default: committed"
                value={startOffset}
                onChange={(e) => setStartOffset(e.target.value.replace(/[^\d]/g, ''))}
                sx={{ width: { xs: '100%', sm: 168 } }}
              />
              <Stack direction="row" spacing={1} sx={{ flexShrink: 0 }}>
                <Button
                  size="small"
                  variant="contained"
                  onClick={applyFilters}
                  disabled={!selected || loadingMessages}
                  sx={{ height: 40, px: 2 }}
                >
                  Apply
                </Button>
                <Button
                  size="small"
                  variant="outlined"
                  onClick={resetFilters}
                  disabled={loadingMessages}
                  sx={{ height: 40, px: 2 }}
                >
                  Reset
                </Button>
              </Stack>
            </Stack>
          </SectionCard>

          {selected ? (
            <MetricCards
              metrics={[
                { label: 'Topic lag', value: selected.lag },
                { label: 'Partitions', value: selected.partitions },
                {
                  label: 'Filter',
                  value:
                    appliedPartition !== '' || appliedOffset !== ''
                      ? [
                          appliedPartition !== '' ? `p${appliedPartition}` : null,
                          appliedOffset !== '' ? `off ${appliedOffset}` : null,
                        ]
                          .filter(Boolean)
                          .join(' · ')
                      : 'pending only',
                },
                { label: 'Page size', value: pageData?.limit ?? 50 },
              ]}
            />
          ) : null}

          <SectionCard
            title="Messages"
            action={
              <Button size="small" onClick={() => void loadMessages()} disabled={!selected || loadingMessages}>
                Refresh
              </Button>
            }
            noPadding
          >
            {loadingMessages && !pageData ? (
              <Box sx={{ p: 2 }}>
                <LoadingBlock />
              </Box>
            ) : pageData?.available === false ? (
              <Box sx={{ p: 2 }}>
                <EmptyState message={pageData.message || 'Messages unavailable.'} />
              </Box>
            ) : messages.length === 0 ? (
              <Box sx={{ p: 2 }}>
                <EmptyState message="No messages in range (caught up, or filters exclude remaining jobs)." />
              </Box>
            ) : (
              <TableContainer>
                <Table size="small">
                  <TableHead>
                    <TableRow>
                      <TableCell>Partition</TableCell>
                      <TableCell>Offset</TableCell>
                      <TableCell>Time</TableCell>
                      <TableCell>Job</TableCell>
                      <TableCell>Worker</TableCell>
                      <TableCell>Tenant</TableCell>
                      <TableCell>Attempt</TableCell>
                      <TableCell>Payload</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {messages.map((m) => (
                      <TableRow key={`${m.partition}:${m.offset}`} hover>
                        <TableCell sx={monoSx}>{m.partition}</TableCell>
                        <TableCell sx={monoSx}>{m.offset}</TableCell>
                        <TableCell>{m.timestamp_label || '—'}</TableCell>
                        <TableCell sx={{ ...monoSx, maxWidth: 160 }} title={m.job_id || undefined}>
                          {m.job_id || '—'}
                        </TableCell>
                        <TableCell sx={{ maxWidth: 200 }} title={m.worker_class || m.job_type || undefined}>
                          {m.worker_class || m.job_type || '—'}
                        </TableCell>
                        <TableCell sx={monoSx}>{m.tenant_id || '—'}</TableCell>
                        <TableCell>{m.attempt ?? '—'}</TableCell>
                        <TableCell sx={{ maxWidth: 320 }}>
                          <Tooltip title={m.payload_preview} enterDelay={400}>
                            <Typography
                              variant="body2"
                              sx={{
                                ...monoSx,
                                overflow: 'hidden',
                                textOverflow: 'ellipsis',
                                whiteSpace: 'nowrap',
                                display: 'block',
                              }}
                            >
                              {m.payload_preview}
                            </Typography>
                          </Tooltip>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            )}

            <Box sx={{ p: 1.5, borderTop: 1, borderColor: 'divider' }}>
              <PaginationBar
                page={page}
                hasNext={!!pageData?.has_next}
                disabled={loadingMessages || !selected}
                onPrev={() => {
                  const prev = [...cursorStack]
                  const back = prev.pop() || ''
                  setCursorStack(prev)
                  setCursor(back)
                }}
                onNext={() => {
                  if (!pageData?.cursor) return
                  setCursorStack((s) => [...s, cursor])
                  setCursor(pageData.cursor)
                }}
              />
            </Box>
          </SectionCard>
        </>
      )}
    </Box>
  )
}
