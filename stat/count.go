package stat

import "math"

// Count returns the number of non-nan elements of the data.
func Count(data []float64) int {
	ch := make(chan int, numConcurrency)

	n := len(data)
	d := divUp(n, numConcurrency)

	for i := 0; i < numConcurrency; i++ {
		from := d * i
		to := min(d*(i+1), n)

		go count(data, from, to, ch)
	}

	cnt := 0

	for i := 0; i < numConcurrency; i++ {
		cnt += <-ch
	}

	return cnt
}

// count calculates the number of non-nan elements of the data.
func count(data []float64, from int, to int, ch chan<- int) {
	cnt := 0

	for i := from; i < to; i++ {
		if math.IsNaN(data[i]) {
			continue
		}

		cnt++
	}

	ch <- cnt
}
