package ml

import "runtime"

func init() {
	SetNumConcurrency(runtime.GOMAXPROCS(0))
}
