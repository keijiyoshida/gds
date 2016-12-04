package vector

func divUp(x, y int) int {
	return (x + y - 1) / y
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
