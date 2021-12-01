// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"fmt"

	"github.com/NVIDIA/aistore/3rdparty/glog"
)

const assertMsg = "assertion failed"

// NOTE: Not to be used in the datapath - consider instead one of the other flavors below.
func Assertf(cond bool, f string, a ...interface{}) {
	if !cond {
		AssertMsg(cond, fmt.Sprintf(f, a...))
	}
}

// NOTE: This and the other asserts below get inlined and optimized.
func Assert(cond bool) {
	if !cond {
		glog.Flush()
		panic(assertMsg)
	}
}

// NOTE: `if (!cond) { AssertMsg(false, msg) }` is preferable usage.
//  Otherwise the message (e.g. `fmt.Sprintf`) may get evaluated every time.
func AssertMsg(cond bool, msg string) {
	if !cond {
		glog.Flush()
		panic(assertMsg + ": " + msg)
	}
}

func AssertNoErr(err error) {
	if err != nil {
		glog.Flush()
		panic(err)
	}
}
