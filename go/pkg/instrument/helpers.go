package instrument

import "time"

func Since(start time.Time) float64 {
	if start.IsZero() {
		return 0
	}
	return float64(time.Since(start).Milliseconds())
}
