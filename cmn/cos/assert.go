// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn/nlog"
)

const assertMsg = "assertion failed"

// NOTE: Not to be used in the datapath - consider instead one of the flavors below.
func Assertf(cond bool, f string, a ...any) {
	if !cond {
		AssertMsg(cond, fmt.Sprintf(f, a...))
	}
}

func Assert(cond bool) {
	if !cond {
		nlog.Flush(true)
		panic(assertMsg)
	}
}

// NOTE: when using Sprintf and such, `if (!cond) { AssertMsg(false, msg) }` is the preferable usage.
func AssertMsg(cond bool, msg string) {
	if !cond {
		nlog.Flush(true)
		panic(assertMsg + ": " + msg)
	}
}

func AssertNoErr(err error) {
	if err != nil {
		nlog.Flush(true)
		panic(err)
	}
}
