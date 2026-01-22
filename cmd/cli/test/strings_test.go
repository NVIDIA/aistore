// Package test_test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package test_test

import (
	"testing"

	"github.com/NVIDIA/aistore/cmd/cli/cli"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestWordsDistance(t *testing.T) {
	testCases := []struct{ expected, actual int }{
		{0, cli.DamerauLevenstheinDistance("test", "test")},
		{1, cli.DamerauLevenstheinDistance("tests", "test")},
		{2, cli.DamerauLevenstheinDistance("cp", "copy")},
		{1, cli.DamerauLevenstheinDistance("teet", "test")},
		{1, cli.DamerauLevenstheinDistance("test", "tset")},
		{3, cli.DamerauLevenstheinDistance("kitten", "sitting")},
	}

	for _, tc := range testCases {
		tassert.Errorf(t, tc.expected == tc.actual,
			"expected Damerau–Levenshtein distance %d, got %d", tc.expected, tc.actual)
	}
}
