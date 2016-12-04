package stat

import "math"

// Mean returns the mean of the data.
func Mean(data []float64) float64 {
	cnt := Count(data)

	if cnt < 1 {
		return math.NaN()
	}

	return Sum(data) / float64(cnt)
}
