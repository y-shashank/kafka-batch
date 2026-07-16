import Alert from '@mui/material/Alert'
import Paper from '@mui/material/Paper'
import Typography from '@mui/material/Typography'

export function EmptyState({ title, message }: { title?: string; message: string }) {
  return (
    <Paper sx={{ p: 3 }}>
      {title ? (
        <Typography variant="h6" sx={{ mb: 1 }}>
          {title}
        </Typography>
      ) : null}
      <Alert severity="info" variant="outlined">
        {message}
      </Alert>
    </Paper>
  )
}
