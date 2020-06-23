// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"testing"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

type (
	mockDiffResolverCtx struct{}
)

func (*mockDiffResolverCtx) CompareObjects(*cluster.LOM, *DstElement) (bool, error) { return true, nil }
func (*mockDiffResolverCtx) IsObjFromCloud(*cluster.LOM) (bool, error)              { return false, nil }

func TestDiffResolver(t *testing.T) {
	tests := []struct {
		name     string
		src      []string
		dst      []string
		expected []DiffResolverResult
	}{
		{
			name:     "empty",
			src:      []string{},
			dst:      []string{},
			expected: []DiffResolverResult{{Action: DiffResolverEOF}},
		},
		{
			name: "all_send",
			src:  []string{"a", "b", "c"},
			dst:  []string{},
			expected: []DiffResolverResult{
				{Action: DiffResolverSend},
				{Action: DiffResolverSend},
				{Action: DiffResolverSend},
				{Action: DiffResolverEOF},
			},
		},
		{
			name: "all_recv",
			src:  []string{},
			dst:  []string{"a", "b", "c"},
			expected: []DiffResolverResult{
				{Action: DiffResolverRecv},
				{Action: DiffResolverRecv},
				{Action: DiffResolverRecv},
				{Action: DiffResolverEOF},
			},
		},
		{
			name: "mixed_send_recv",
			src:  []string{"a", "c"},
			dst:  []string{"b", "d"},
			expected: []DiffResolverResult{
				{Action: DiffResolverSend},
				{Action: DiffResolverRecv},
				{Action: DiffResolverSend},
				{Action: DiffResolverRecv},
				{Action: DiffResolverEOF},
			},
		},
		{
			name: "all_send_then_all_recv",
			src:  []string{"a", "b", "c"},
			dst:  []string{"d", "e"},
			expected: []DiffResolverResult{
				{Action: DiffResolverSend},
				{Action: DiffResolverSend},
				{Action: DiffResolverSend},
				{Action: DiffResolverRecv},
				{Action: DiffResolverRecv},
				{Action: DiffResolverEOF},
			},
		},
		{
			name: "all_recv_then_all_send",
			src:  []string{"d", "e"},
			dst:  []string{"a", "b", "c"},
			expected: []DiffResolverResult{
				{Action: DiffResolverRecv},
				{Action: DiffResolverRecv},
				{Action: DiffResolverRecv},
				{Action: DiffResolverSend},
				{Action: DiffResolverSend},
				{Action: DiffResolverEOF},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := &mockDiffResolverCtx{}
			dr := NewDiffResolver(ctx)
			dr.Start()
			for _, s := range test.src {
				dr.PushSrc(&cluster.LOM{ObjName: s})
			}
			dr.CloseSrc()
			for _, d := range test.dst {
				dr.PushDst(&CloudResource{ObjName: d})
			}
			dr.CloseDst()

			for i := 0; i < len(test.expected); i++ {
				result, err := dr.Next()
				tassert.CheckFatal(t, err)

				expectedResult := test.expected[i]
				tassert.Errorf(
					t, result.Action == expectedResult.Action,
					"actions differ: (got: %d, expected: %d)", result.Action, expectedResult.Action,
				)
				tassert.Fatalf(t, result.Err == nil, "error has been set")
				if result.Action == DiffResolverRecv {
					tassert.Errorf(t, result.Dst != nil, "destination has not been set for recv")
				} else if result.Action == DiffResolverSend {
					tassert.Errorf(t, result.Src != nil, "source has not been set for send")
				}
			}

			// Check that EOF is followed by EOF.
			for i := 0; i < 2; i++ {
				result, err := dr.Next()
				tassert.CheckFatal(t, err)
				tassert.Errorf(t, result.Action == DiffResolverEOF, "eof not followed by eof")
			}
		})
	}
}
