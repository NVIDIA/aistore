// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"flag"
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/cmn/nlog"
)

func Infof(format string, a ...any) {
	if flag.Parsed() {
		nlog.InfoDepth(1, fmt.Sprintf(format, a...))
	} else {
		fmt.Printf(format+"\n", a...)
	}
}

func Errorf(format string, a ...any) {
	if flag.Parsed() {
		nlog.ErrorDepth(1, fmt.Sprintf(format, a...))
	} else {
		fmt.Fprintf(os.Stderr, format+"\n", a...)
	}
}
