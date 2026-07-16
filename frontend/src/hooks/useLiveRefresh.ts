import { useCallback, useEffect, useState } from 'react'

const KEY = 'kafka_batch_live'
const INTERVAL_MS = 5000

export function useLiveRefresh(refetch: () => void | Promise<void>) {
  const [live, setLive] = useState(() => localStorage.getItem(KEY) === '1')

  const toggle = useCallback(() => {
    setLive((prev) => {
      const next = !prev
      localStorage.setItem(KEY, next ? '1' : '0')
      return next
    })
  }, [])

  useEffect(() => {
    if (!live) return
    const id = window.setInterval(() => {
      void refetch()
    }, INTERVAL_MS)
    return () => window.clearInterval(id)
  }, [live, refetch])

  return { live, toggle }
}
