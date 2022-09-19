// Package tlog provides common logf and logln primitives for devtools and tutils
/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION. All rights reserved.
 */
package tlog

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func prependTime(msg string) string {
	return fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05.000000"), msg)
}

func Logln(msg string) {
	if testing.Verbose() {
		fmt.Fprintln(os.Stdout, prependTime(msg))
	}
}

func Logf(f string, a ...any) {
	if testing.Verbose() {
		fmt.Fprintf(os.Stdout, prependTime(f), a...)
	}
}

func LogfCond(cond bool, f string, a ...any) {
	if cond {
		Logf(f, a...)
	}
}
