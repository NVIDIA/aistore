// +build !debug

// Package provides debug utilities
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package debug

import "sync"

const (
	MtxLocked  = 0
	MtxRLocked = 0

	Enabled = false
)

func Errorln(a ...interface{})          {}
func Errorf(f string, a ...interface{}) {}
func Infof(f string, a ...interface{})  {}

func Assert(cond bool, a ...interface{})            {}
func AssertFunc(f func() bool, a ...interface{})    {}
func AssertMsg(cond bool, msg string)               {}
func AssertNoErr(err error)                         {}
func Assertf(cond bool, f string, a ...interface{}) {}

func AssertMutexLocked(m *sync.Mutex)         {}
func AssertRWMutex(m *sync.RWMutex, flag int) {}
