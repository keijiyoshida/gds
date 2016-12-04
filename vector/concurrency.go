package vector

var numConcurrency int

// SetNumConcurrency sets the number of concurrency of the processing.
// The default value of this parameter is the maximum number of CPUs
// that can be executing simultaneously.
func SetNumConcurrency(n int) {
	numConcurrency = n
}
