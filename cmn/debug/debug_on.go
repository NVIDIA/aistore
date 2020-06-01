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

func Logf(a ...interface{}) {
	a = append([]interface{}{"[DEBUG] "}, a...)
	glog.ErrorDepth(1, a...)
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
