import Card from '@mui/material/Card'
import CardActionArea from '@mui/material/CardActionArea'
import CardContent from '@mui/material/CardContent'
import Grid from '@mui/material/Grid'
import Typography from '@mui/material/Typography'
import { Link as RouterLink } from 'react-router-dom'

export type Metric = {
  label: string
  value: string | number
  color?: string
  to?: string
}

export function MetricCards({ metrics }: { metrics: Metric[] }) {
  return (
    <Grid container spacing={1.5} sx={{ mb: 2.5 }}>
      {metrics.map((m) => {
        const content = (
          <CardContent sx={{ py: 1.75, '&:last-child': { pb: 1.75 } }}>
            <Typography variant="h5" sx={{ color: m.color || 'text.primary', fontWeight: 400, lineHeight: 1.2 }}>
              {m.value}
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
              {m.label}
            </Typography>
          </CardContent>
        )

        return (
          <Grid key={m.label} size={{ xs: 6, sm: 4, md: 3, lg: 2 }}>
            <Card variant="outlined" sx={{ height: '100%' }}>
              {m.to ? (
                <CardActionArea component={RouterLink} to={m.to} sx={{ height: '100%' }}>
                  {content}
                </CardActionArea>
              ) : (
                content
              )}
            </Card>
          </Grid>
        )
      })}
    </Grid>
  )
}
