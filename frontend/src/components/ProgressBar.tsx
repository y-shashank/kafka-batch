import Box from '@mui/material/Box'
import Tooltip from '@mui/material/Tooltip'

export function ProgressBar({
  donePct,
  failPct,
  title,
}: {
  donePct: number
  failPct: number
  title?: string
}) {
  return (
    <Tooltip title={title || `${donePct}% done, ${failPct}% failed`}>
      <Box
        sx={{
          height: 8,
          borderRadius: 999,
          bgcolor: '#e2e8f0',
          overflow: 'hidden',
          display: 'flex',
          minWidth: 100,
        }}
      >
        <Box sx={{ width: `${donePct}%`, bgcolor: '#059669', transition: 'width .3s ease' }} />
        <Box sx={{ width: `${failPct}%`, bgcolor: '#dc2626', transition: 'width .3s ease' }} />
      </Box>
    </Tooltip>
  )
}
