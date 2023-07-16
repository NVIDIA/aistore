//go:build debug

// Package nlog
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package nlog

import (
	"fmt"
	"os"
	rdebug "runtime/debug"
)

func assert(cond bool, a ...any) {
	if !cond {
		msg := "nlog assertion failed"
		if len(a) > 0 {
			msg += ": " + fmt.Sprint(a...)
		}
		os.Stderr.WriteString(msg + "\n")
		rdebug.PrintStack()
		os.Exit(1)
	}
}
