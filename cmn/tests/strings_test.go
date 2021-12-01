// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/tassert"
)

func TestWordsDistance(t *testing.T) {
	testCases := []struct{ expected, actual int }{
		{0, cos.DamerauLevenstheinDistance("test", "test")},
		{1, cos.DamerauLevenstheinDistance("tests", "test")},
		{2, cos.DamerauLevenstheinDistance("cp", "copy")},
		{1, cos.DamerauLevenstheinDistance("teet", "test")},
		{1, cos.DamerauLevenstheinDistance("test", "tset")},
	}

	for _, tc := range testCases {
		tassert.Errorf(t, tc.expected == tc.actual, "expected Levensthein distance %d, got %d", tc.expected, tc.actual)
	}
}
