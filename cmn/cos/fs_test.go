// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package cos_test

import (
	"math"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"
	jsoniter "github.com/json-iterator/go"
)

func TestFsIDMarshal(t *testing.T) {
	if testing.Short() {
		t.Skipf("skipping %s in short mode", t.Name())
	}
	tests := []struct {
		fsID cos.FsID
	}{
		{fsID: [2]int32{24, 42}},

		{fsID: [2]int32{0, 1}},
		{fsID: [2]int32{1, 0}},
		{fsID: [2]int32{0, -1}},
		{fsID: [2]int32{-1, 0}},
		{fsID: [2]int32{-1, 1}},
		{fsID: [2]int32{1, -1}},

		{fsID: [2]int32{math.MaxInt32, math.MaxInt32}},
		{fsID: [2]int32{math.MinInt32, math.MinInt32}},
		{fsID: [2]int32{math.MinInt32, math.MaxInt32}},
		{fsID: [2]int32{math.MaxInt32, math.MinInt32}},

		{fsID: [2]int32{0, math.MinInt32}},
		{fsID: [2]int32{0, math.MaxInt32}},
		{fsID: [2]int32{math.MaxInt32, 0}},
		{fsID: [2]int32{math.MinInt32, 0}},

		{fsID: [2]int32{1, math.MinInt32}},
		{fsID: [2]int32{1, math.MaxInt32}},
		{fsID: [2]int32{math.MaxInt32, 1}},
		{fsID: [2]int32{math.MinInt32, 1}},

		{fsID: [2]int32{-1, math.MinInt32}},
		{fsID: [2]int32{-1, math.MaxInt32}},
		{fsID: [2]int32{math.MaxInt32, -1}},
		{fsID: [2]int32{math.MinInt32, -1}},
	}

	for _, test := range tests {
		t.Run(test.fsID.String(), func(t *testing.T) {
			b, err := jsoniter.Marshal(test.fsID)
			tassert.Errorf(t, err == nil, "marshal failed on input: %s, err: %v", test.fsID, err)

			var tmp cos.FsID
			err = jsoniter.Unmarshal(b, &tmp)
			tassert.Fatalf(t, err == nil, "unmarshal failed on input: %s, err: %v", test.fsID, err)

			equal := tmp[0] == test.fsID[0] && tmp[1] == test.fsID[1]
			tassert.Fatalf(t, equal, "fs ids are not equal after marshal and unmarshal (expected: %s, got: %s)", test.fsID, tmp)
		})
	}
}
