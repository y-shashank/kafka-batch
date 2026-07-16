import Box from '@mui/material/Box'
import CircularProgress from '@mui/material/CircularProgress'

export function LoadingBlock() {
  return (
    <Box sx={{ display: 'flex', justifyContent: 'center', py: 8 }}>
      <CircularProgress size={36} />
    </Box>
  )
}
