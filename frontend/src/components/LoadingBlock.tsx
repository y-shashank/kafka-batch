import Box from '@mui/material/Box'
import CircularProgress from '@mui/material/CircularProgress'
import Typography from '@mui/material/Typography'

export function LoadingBlock() {
  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', py: 10, gap: 2 }}>
      <CircularProgress size={36} />
      <Typography variant="body2" color="text.secondary">
        Loading…
      </Typography>
    </Box>
  )
}
