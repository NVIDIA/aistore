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

func NewExpvar(_ uint8)                    {}
func SetExpvar(_ uint8, _ string, _ int64) {}

func Errorln(_ ...interface{})          {}
func Errorf(_ string, _ ...interface{}) {}
func Infof(_ string, _ ...interface{})  {}

func Func(_ func()) {}

func Assert(_ bool, _ ...interface{})            {}
func AssertFunc(_ func() bool, _ ...interface{}) {}
func AssertMsg(_ bool, _ string)                 {}
func AssertNoErr(_ error)                        {}
func Assertf(_ bool, _ string, _ ...interface{}) {}

func AssertMutexLocked(_ *sync.Mutex)      {}
func AssertRWMutexLocked(_ *sync.RWMutex)  {}
func AssertRWMutexRLocked(_ *sync.RWMutex) {}

func Handlers() map[string]http.HandlerFunc {
	return nil
}
