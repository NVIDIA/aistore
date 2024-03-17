// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cos

const maxl = 16

func BHead(b []byte) string {
	if len(b) > maxl {
		return string(b[:maxl]) + "..."
	}
	return string(b)
}

func SHead(s string) string {
	if len(s) > maxl {
		return s[:maxl] + "..."
	}
	return s
}

func IsLastB(s string, b byte) bool { return s[len(s)-1] == b }

func TrimLastB(s string, b byte) string {
	if l := len(s); s[l-1] == b {
		return s[:l-1]
	}
	return s
}

// return non-empty
func Either(lhs, rhs string) string {
	if lhs != "" {
		return lhs
	}
	return rhs
}

// (common use)
func Plural(num int) (s string) {
	if num != 1 {
		s = "s"
	}
	return
}
