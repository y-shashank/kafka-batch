import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Stack from '@mui/material/Stack'
import Typography from '@mui/material/Typography'

/** Previous / page label / Next with consistent vertical alignment. */
export function PaginationBar({
  page,
  hasNext,
  onPrev,
  onNext,
  disabled,
}: {
  page: number
  hasNext: boolean
  onPrev: () => void
  onNext: () => void
  disabled?: boolean
}) {
  return (
    <Stack direction="row" spacing={1} alignItems="center" justifyContent="center" sx={{ minHeight: 40 }}>
      <Button size="small" disabled={disabled || page <= 1} onClick={onPrev} sx={{ minHeight: 36 }}>
        Previous
      </Button>
      <Box
        sx={{
          display: 'inline-flex',
          alignItems: 'center',
          justifyContent: 'center',
          minHeight: 36,
          px: 1,
        }}
      >
        <Typography variant="body2" color="text.secondary" component="span" sx={{ lineHeight: 1 }}>
          Page {page}
        </Typography>
      </Box>
      <Button size="small" disabled={disabled || !hasNext} onClick={onNext} sx={{ minHeight: 36 }}>
        Next
      </Button>
    </Stack>
  )
}
