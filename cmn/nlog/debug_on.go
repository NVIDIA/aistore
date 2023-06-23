//go:build debug

// Package nlog
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package nlog

import (
	"fmt"
	"os"
)

func assert(cond bool, a ...any) {
	if !cond {
		msg := "DEBUG PANIC: "
		if len(a) > 0 {
			msg += fmt.Sprint(a...)
		}
		os.Stderr.WriteString(msg)
		os.Exit(1)
	}
}
