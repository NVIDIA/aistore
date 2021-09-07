// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"fmt"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/devtools/tassert"
)

func TestAbortedErrorAs(t *testing.T) {
	mockDetails := "mock details"
	mockWhat := "mock what"

	abortedError := cmn.NewErrAborted(mockWhat, mockDetails, nil)
	tassert.Fatalf(t, cmn.IsErrAborted(abortedError), "expected errors.As to return true on the same error type")

	mockError := fmt.Errorf("wrapping aborted error %w", abortedError)
	tassert.Fatalf(t, cmn.IsErrAborted(mockError), "expected errors.As to return true on a wrapped error")
}
