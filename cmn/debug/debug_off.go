// +build !debug

// Package provides debug utilities
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package debug

import (
	"net/http"
	"sync"
)

func NewExpvar(smodule uint8)                         {}
func SetExpvar(smodule uint8, name string, val int64) {}

func Errorln(a ...interface{})          {}
func Errorf(f string, a ...interface{}) {}
func Infof(f string, a ...interface{})  {}

func Func(f func()) {}

func Assert(cond bool, a ...interface{})            {}
func AssertFunc(f func() bool, a ...interface{})    {}
func AssertMsg(cond bool, msg string)               {}
func AssertNoErr(err error)                         {}
func Assertf(cond bool, f string, a ...interface{}) {}

func AssertMutexLocked(m *sync.Mutex)      {}
func AssertRWMutexLocked(m *sync.RWMutex)  {}
func AssertRWMutexRLocked(m *sync.RWMutex) {}

func Handlers() map[string]http.HandlerFunc {
	return nil
}
