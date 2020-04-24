// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"errors"
	"fmt"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func TestAbortedErrorAs(t *testing.T) {
	mockDetails := "mock details"
	mockWhat := "mock what"

	abortedError := cmn.NewAbortedErrorDetails(mockWhat, mockDetails)
	tassert.Fatalf(t, errors.As(abortedError, &cmn.AbortedError{}), "expected errors.As to return true on the same error type")

	mockError := fmt.Errorf("wrapping aborted error %w", abortedError)
	tassert.Fatalf(t, errors.As(mockError, &cmn.AbortedError{}), "expected errors.As to return true on a wrapped error")
}
