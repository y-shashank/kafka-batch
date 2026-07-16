import Card from '@mui/material/Card'
import CardContent from '@mui/material/CardContent'
import CardHeader from '@mui/material/CardHeader'
import type { ReactNode } from 'react'

export function SectionCard({
  title,
  subheader,
  action,
  children,
  noPadding,
}: {
  title?: string
  subheader?: string
  action?: ReactNode
  children: ReactNode
  noPadding?: boolean
}) {
  return (
    <Card variant="outlined" sx={{ mb: 2 }}>
      {title ? <CardHeader title={title} subheader={subheader} action={action} titleTypographyProps={{ variant: 'h6' }} sx={{ pb: 0 }} /> : null}
      <CardContent sx={noPadding ? { p: 0, '&:last-child': { pb: 0 } } : { pt: title ? 2 : undefined }}>{children}</CardContent>
    </Card>
  )
}
