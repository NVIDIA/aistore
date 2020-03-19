// Package tassert provides common asserts for tests
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package tassert

import (
	"runtime/debug"
	"testing"
)

func CheckFatal(t *testing.T, err error) {
	if err != nil {
		debug.PrintStack()
		t.Fatalf(err.Error())
	}
}

func CheckError(t *testing.T, err error) {
	if err != nil {
		debug.PrintStack()
		t.Errorf(err.Error())
	}
}

func Fatalf(t *testing.T, cond bool, msg string, args ...interface{}) {
	if !cond {
		debug.PrintStack()
		t.Fatalf(msg, args...)
	}
}

func Errorf(t *testing.T, cond bool, msg string, args ...interface{}) {
	if !cond {
		debug.PrintStack()
		t.Errorf(msg, args...)
	}
}

func SelectErr(t *testing.T, errCh chan error, verb string, errIsFatal bool) {
	if num := len(errCh); num > 0 {
		err := <-errCh
		f := t.Errorf
		if errIsFatal {
			f = t.Fatalf
		}
		if num > 1 {
			f("Failed to %s %d objects, e.g. error:\n%v", verb, num, err)
		} else {
			f("Failed to %s object: %v", verb, err)
		}
	}
}
