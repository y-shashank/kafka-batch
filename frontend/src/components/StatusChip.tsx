import Chip from '@mui/material/Chip'
import { STATUS_COLORS } from '../theme'

export function StatusChip({ status }: { status?: string | null }) {
  const s = (status || 'unknown').toLowerCase()
  const color = STATUS_COLORS[s] || '#64748b'
  return (
    <Chip
      size="small"
      label={s}
      sx={{
        bgcolor: color,
        color: '#fff',
        fontWeight: 700,
        textTransform: 'capitalize',
        height: 24,
      }}
    />
  )
}
