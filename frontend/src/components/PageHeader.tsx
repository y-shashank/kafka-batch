import Box from '@mui/material/Box'
import Typography from '@mui/material/Typography'

export function PageHeader({ title, subtitle }: { title: string; subtitle?: string }) {
  return (
    <Box sx={{ mb: 2.5 }}>
      <Typography variant="h5" sx={{ mb: 0.5 }}>
        {title}
      </Typography>
      {subtitle ? (
        <Typography variant="body2" color="text.secondary">
          {subtitle}
        </Typography>
      ) : null}
    </Box>
  )
}
