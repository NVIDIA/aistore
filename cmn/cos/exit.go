// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"flag"
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/cmn/nlog"
)

const fatalPrefix = "FATAL ERROR: "

func _exit(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}

func Exitf(f string, a ...any) {
	msg := fmt.Sprintf(fatalPrefix+f, a...)
	_exit(msg)
}

// +log
func ExitLogf(f string, a ...any) {
	msg := fmt.Sprintf(fatalPrefix+f, a...)
	if flag.Parsed() {
		nlog.ErrorDepth(1, msg+"\n")
		nlog.Flush(nlog.ActExit)
		os.Exit(1)
	}
	_exit(msg)
}

func ExitLog(a ...any) {
	msg := fatalPrefix + fmt.Sprint(a...)
	if flag.Parsed() {
		nlog.ErrorDepth(1, msg+"\n")
		nlog.Flush(nlog.ActExit)
		os.Exit(1)
	}
	_exit(msg)
}

// +condition
func ExitAssertLog(cond bool, a ...any) {
	if !cond {
		ExitLog(a...)
	}
}
