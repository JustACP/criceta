package utils

import "cmp"

func More[T cmp.Ordered](a T, b T) T {
	if a >= b {
		return a
	}

	return b
}

func Less[T cmp.Ordered](a T, b T) T {
	if a >= b {
		return b
	}

	return a
}
