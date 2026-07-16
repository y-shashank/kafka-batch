import Box from '@mui/material/Box'
import LinearProgress from '@mui/material/LinearProgress'
import Stack from '@mui/material/Stack'
import Tooltip from '@mui/material/Tooltip'
import Typography from '@mui/material/Typography'

export function ProgressBar({
  donePct,
  failPct,
  title,
}: {
  donePct: number
  failPct: number
  title?: string
}) {
  const done = Math.max(0, Math.min(100, donePct))
  const fail = Math.max(0, Math.min(100 - done, failPct))
  return (
    <Tooltip title={title || `${done}% done, ${fail}% failed`}>
      <Stack spacing={0.5} sx={{ minWidth: 96, maxWidth: 160 }}>
        <Box sx={{ position: 'relative', height: 6, borderRadius: 1, bgcolor: 'action.hover', overflow: 'hidden' }}>
          <Box sx={{ position: 'absolute', left: 0, top: 0, bottom: 0, width: `${done}%`, bgcolor: 'success.main' }} />
          <Box sx={{ position: 'absolute', left: `${done}%`, top: 0, bottom: 0, width: `${fail}%`, bgcolor: 'error.main' }} />
        </Box>
        <Typography variant="caption" color="text.secondary">
          {done.toFixed(0)}%
        </Typography>
      </Stack>
    </Tooltip>
  )
}

/** Simple single-value progress using default Material LinearProgress. */
export function SimpleProgress({ value }: { value: number }) {
  return <LinearProgress variant="determinate" value={Math.max(0, Math.min(100, value))} sx={{ height: 6, borderRadius: 1 }} />
}
