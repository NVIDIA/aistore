// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"flag"
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/cmn/nlog"
)

func Infoln(a ...any) {
	if flag.Parsed() {
		nlog.InfoDepth(1, a...)
	} else {
		fmt.Println(a...)
	}
}

func Errorln(a ...any) {
	if flag.Parsed() {
		nlog.ErrorDepth(1, a...)
	} else {
		fmt.Fprintln(os.Stderr, a...)
	}
}

func Errorf(format string, a ...any) {
	if flag.Parsed() {
		nlog.ErrorDepth(1, fmt.Sprintf(format, a...))
	} else {
		fmt.Fprintf(os.Stderr, format+"\n", a...)
	}
}
