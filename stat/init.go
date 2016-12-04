package stat

import "runtime"

func init() {
	SetNumConcurrency(runtime.GOMAXPROCS(0))
}
