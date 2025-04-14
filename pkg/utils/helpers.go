package utils

import "time"

func Sleep(duration time.Duration) {
	time.Sleep(duration)
}

func IsEmpty(s string) bool {
	return len(s) == 0
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
