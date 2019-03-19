// Package tassert provides common asserts for tests
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package tassert

import "testing"

func Fatal(t *testing.T, cond bool, msg string, args ...interface{}) {
	if !cond {
		t.Fatalf(msg, args...)
	}
}

func Error(t *testing.T, cond bool, msg string, args ...interface{}) {
	if !cond {
		t.Errorf(msg, args...)
	}
}
