import Alert from '@mui/material/Alert'
import AlertTitle from '@mui/material/AlertTitle'
import Card from '@mui/material/Card'
import CardContent from '@mui/material/CardContent'

export function EmptyState({ title, message }: { title?: string; message: string }) {
  return (
    <Card variant="outlined">
      <CardContent>
        <Alert severity="info" variant="outlined">
          {title ? <AlertTitle>{title}</AlertTitle> : null}
          {message}
        </Alert>
      </CardContent>
    </Card>
  )
}
