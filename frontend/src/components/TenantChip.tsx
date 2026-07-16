import Chip from '@mui/material/Chip'

const COLORS: [string, string][] = [
  ['#1d4ed8', '#dbeafe'],
  ['#0f766e', '#ccfbf1'],
  ['#b45309', '#fef3c7'],
  ['#be185d', '#fce7f3'],
  ['#047857', '#d1fae5'],
  ['#4338ca', '#e0e7ff'],
  ['#b91c1c', '#fee2e2'],
  ['#0369a1', '#e0f2fe'],
  ['#6d28d9', '#ede9fe'],
  ['#9a3412', '#ffedd5'],
]

function colorsFor(id: string): [string, string] {
  let sum = 0
  for (let i = 0; i < id.length; i++) sum += id.charCodeAt(i)
  return COLORS[sum % COLORS.length]
}

export function TenantChip({ tenantId }: { tenantId?: string | null }) {
  if (!tenantId) return <span style={{ color: '#94a3b8' }}>—</span>
  const [fg, bg] = colorsFor(tenantId)
  return (
    <Chip
      size="small"
      label={tenantId}
      sx={{ bgcolor: bg, color: fg, fontWeight: 700, fontFamily: 'JetBrains Mono, monospace', height: 22 }}
    />
  )
}
