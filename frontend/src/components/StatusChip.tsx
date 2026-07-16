import Chip from '@mui/material/Chip'
import { STATUS_COLORS } from '../theme'

export function StatusChip({ status }: { status?: string | null }) {
  const s = (status || 'unknown').toLowerCase()
  const color = STATUS_COLORS[s] || 'default'
  return <Chip size="small" color={color} label={s} variant={color === 'default' ? 'outlined' : 'filled'} />
}
