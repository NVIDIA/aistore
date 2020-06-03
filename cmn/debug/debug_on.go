// +build debug

// Package provides debug utilities
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package debug

import (
	"fmt"

	"github.com/NVIDIA/aistore/3rdparty/glog"
)

const (
	Enabled = true
)

func Errorf(f string, a ...interface{}) {
	glog.ErrorDepth(1, fmt.Sprintf("[DEBUG] "+f, a...))
}

func Infof(f string, a ...interface{}) {
	glog.InfoDepth(1, fmt.Sprintf("[DEBUG] "+f, a...))
}

func Assert(cond bool) {
	if !cond {
		glog.Flush()
		panic("DEBUG PANIC")
	}
}

func AssertMsg(cond bool, msg string) {
	if !cond {
		glog.Flush()
		panic("DEBUG PANIC: " + msg)
	}
}

func AssertNoErr(err error) {
	if err != nil {
		glog.Flush()
		panic(err)
	}
}

func Assertf(cond bool, f string, a ...interface{}) { AssertMsg(cond, fmt.Sprintf(f, a...)) }
