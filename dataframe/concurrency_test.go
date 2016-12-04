package dataframe

import "testing"

func TestSetNumConcurrency(t *testing.T) {
	ns := []int{1, 2, 3, 4, 5, 6, 7, 8}

	for _, n := range ns {
		SetNumConcurrency(n)

		if numConcurrency != n {
			t.Errorf("numConcurrency => %d, want %d", numConcurrency, n)
		}
	}
}
