// Package tlog provides common logf and logln primitives for dev tools
/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION. All rights reserved.
 */
package tlog

import (
	"fmt"
	"os"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
)

func prependTime(msg string) string {
	return fmt.Sprintf("[%s] %s", cos.FormatNowStamp(), msg)
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
