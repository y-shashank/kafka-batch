import Box from '@mui/material/Box'
import Paper from '@mui/material/Paper'
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
    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1.5, mb: 2 }}>
      {metrics.map((m) => {
        const inner = (
          <Paper
            key={m.label}
            sx={{
              px: 2,
              py: 1.5,
              minWidth: 120,
              transition: 'transform .15s ease, box-shadow .15s ease',
              '&:hover': m.to ? { transform: 'translateY(-2px)', boxShadow: '0 8px 24px rgba(15,23,42,.08)' } : undefined,
            }}
          >
            <Typography variant="h5" sx={{ color: m.color || 'text.primary', fontWeight: 800, lineHeight: 1.1 }}>
              {m.value}
            </Typography>
            <Typography variant="caption" color="text.secondary" sx={{ textTransform: 'uppercase', letterSpacing: '.06em', fontWeight: 600 }}>
              {m.label}
            </Typography>
          </Paper>
        )
        return m.to ? (
          <Box key={m.label} component={RouterLink} to={m.to} sx={{ textDecoration: 'none', color: 'inherit' }}>
            {inner}
          </Box>
        ) : (
          <Box key={m.label}>{inner}</Box>
        )
      })}
    </Box>
  )
}
