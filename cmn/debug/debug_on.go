// +build debug

// Package provides debug utilities
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package debug

import (
	"fmt"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
)

const (
	Enabled = true
)

func Logf(a ...interface{}) {
	a = append([]interface{}{"[DEBUG] "}, a...)
	glog.ErrorDepth(1, a...)
}

func Assert(cond bool)                              { cmn.Assert(cond) }
func AssertMsg(cond bool, msg string)               { cmn.AssertMsg(cond, msg) }
func AssertNoErr(err error)                         { cmn.AssertNoErr(err) }
func Assertf(cond bool, f string, a ...interface{}) { cmn.AssertMsg(cond, fmt.Sprintf(f, a...)) }
