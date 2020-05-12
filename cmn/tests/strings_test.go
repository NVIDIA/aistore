// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package tests

import (
	"testing"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func TestWordsDistance(t *testing.T) {
	testCases := []struct{ expected, actual int }{
		{0, cmn.DamerauLevenstheinDistance("test", "test")},
		{1, cmn.DamerauLevenstheinDistance("tests", "test")},
		{2, cmn.DamerauLevenstheinDistance("cp", "copy")},
		{1, cmn.DamerauLevenstheinDistance("teet", "test")},
		{1, cmn.DamerauLevenstheinDistance("test", "tset")},
	}

	for _, tc := range testCases {
		tassert.Errorf(t, tc.expected == tc.actual, "expected Levensthein distance %d, got %d", tc.expected, tc.actual)
	}
}
