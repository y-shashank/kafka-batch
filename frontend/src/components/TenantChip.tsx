import Chip from '@mui/material/Chip'
import Typography from '@mui/material/Typography'

export function TenantChip({ tenantId }: { tenantId?: string | null }) {
  if (!tenantId) {
    return (
      <Typography variant="body2" color="text.disabled">
        —
      </Typography>
    )
  }
  return <Chip size="small" variant="outlined" label={tenantId} sx={{ fontFamily: 'Roboto Mono, monospace', maxWidth: 180 }} />
}
