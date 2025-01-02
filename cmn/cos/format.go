// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"fmt"
	"strconv"
)

// NOTE usage: bench and CLI only

func FormatBigInt(n int) string { return FormatBigI64(int64(n)) }

func FormatBigI64(n int64) (s string) {
	if n < 1000 {
		return strconv.FormatInt(n, 10)
	}
	for n > 0 {
		rem := n % 1000
		n = (n - rem) / 1000
		if s == "" {
			s = fmt.Sprintf("%03d", rem)
			continue
		}
		if n == 0 {
			s = strconv.FormatInt(rem, 10) + "," + s
		} else {
			s = fmt.Sprintf("%03d", rem) + "," + s
		}
	}
	return s
}
