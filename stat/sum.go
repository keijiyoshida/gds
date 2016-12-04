package stat

import "math"

// Sum returns the sum of the data.
func Sum(data []float64) float64 {
	ch := make(chan float64, numConcurrency)

	n := len(data)
	d := divUp(n, numConcurrency)

	for i := 0; i < numConcurrency; i++ {
		from := d * i
		to := min(d*(i+1), n)

		go sum(data, from, to, ch)
	}

	s := 0.0

	for i := 0; i < numConcurrency; i++ {
		s += <-ch
	}

	return s
}

// sum calculates the sum of the data.
func sum(data []float64, from int, to int, ch chan<- float64) {
	s := 0.0

	for i := from; i < to; i++ {
		if math.IsNaN(data[i]) {
			continue
		}

		s += data[i]
	}

	ch <- s
}
