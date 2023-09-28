// Package dloader_test is a unit test
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package dload_test

import (
	"testing"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ext/dload"
	"github.com/NVIDIA/aistore/tools/tassert"
)

const (
	fromRemoteFQN = "remote"
)

type (
	mockDiffResolverCtx struct{}

	obj struct {
		name   string
		remote bool
	}

	testCase struct {
		name     string
		src      []obj
		dst      []obj
		expected []dload.DiffResolverResult
	}
)

func (*mockDiffResolverCtx) CompareObjects(*cluster.LOM, *dload.DstElement) (bool, error) {
	return true, nil
}

func (*mockDiffResolverCtx) IsObjFromRemote(lom *cluster.LOM) (bool, error) {
	return lom.FQN == fromRemoteFQN, nil
}

func TestDiffResolver(t *testing.T) {
	tests := []testCase{
		{
			name:     "empty",
			src:      []obj{},
			dst:      []obj{},
			expected: []dload.DiffResolverResult{{Action: dload.DiffResolverEOF}},
		},
		{
			name: "all_send",
			src:  []obj{{name: "a"}, {name: "b"}, {name: "c"}},
			dst:  []obj{},
			expected: []dload.DiffResolverResult{
				{Action: dload.DiffResolverSend},
				{Action: dload.DiffResolverSend},
				{Action: dload.DiffResolverSend},
				{Action: dload.DiffResolverEOF},
			},
		},
		{
			name: "all_recv",
			src:  []obj{},
			dst:  []obj{{name: "a"}, {name: "b"}, {name: "c"}},
			expected: []dload.DiffResolverResult{
				{Action: dload.DiffResolverRecv},
				{Action: dload.DiffResolverRecv},
				{Action: dload.DiffResolverRecv},
				{Action: dload.DiffResolverEOF},
			},
		},
		{
			name: "mixed_send_recv",
			src:  []obj{{name: "a"}, {name: "c"}},
			dst:  []obj{{name: "b"}, {name: "d"}},
			expected: []dload.DiffResolverResult{
				{Action: dload.DiffResolverSend},
				{Action: dload.DiffResolverRecv},
				{Action: dload.DiffResolverSend},
				{Action: dload.DiffResolverRecv},
				{Action: dload.DiffResolverEOF},
			},
		},
		{
			name: "all_send_then_all_recv",
			src:  []obj{{name: "a"}, {name: "b"}, {name: "c"}},
			dst:  []obj{{name: "d"}, {name: "e"}},
			expected: []dload.DiffResolverResult{
				{Action: dload.DiffResolverSend},
				{Action: dload.DiffResolverSend},
				{Action: dload.DiffResolverSend},
				{Action: dload.DiffResolverRecv},
				{Action: dload.DiffResolverRecv},
				{Action: dload.DiffResolverEOF},
			},
		},
		{
			name: "all_recv_then_all_send",
			src:  []obj{{name: "d"}, {name: "e"}},
			dst:  []obj{{name: "a"}, {name: "b"}, {name: "c"}},
			expected: []dload.DiffResolverResult{
				{Action: dload.DiffResolverRecv},
				{Action: dload.DiffResolverRecv},
				{Action: dload.DiffResolverRecv},
				{Action: dload.DiffResolverSend},
				{Action: dload.DiffResolverSend},
				{Action: dload.DiffResolverEOF},
			},
		},
		{
			name: "all_delete",
			src:  []obj{{name: "a", remote: true}, {name: "b", remote: true}},
			dst:  []obj{},
			expected: []dload.DiffResolverResult{
				{Action: dload.DiffResolverDelete},
				{Action: dload.DiffResolverDelete},
				{Action: dload.DiffResolverEOF},
			},
		},
		{
			name: "mixed_send_delete",
			src:  []obj{{name: "a"}, {name: "b", remote: true}, {name: "c"}, {name: "d", remote: true}},
			dst:  []obj{},
			expected: []dload.DiffResolverResult{
				{Action: dload.DiffResolverSend},
				{Action: dload.DiffResolverDelete},
				{Action: dload.DiffResolverSend},
				{Action: dload.DiffResolverDelete},
				{Action: dload.DiffResolverEOF},
			},
		},
		{
			name: "all_skip_then_all_recv",
			src:  []obj{{name: "a", remote: true}, {name: "b", remote: true}},
			dst:  []obj{{name: "a"}, {name: "b"}, {name: "c"}, {name: "d"}},
			expected: []dload.DiffResolverResult{
				{Action: dload.DiffResolverSkip},
				{Action: dload.DiffResolverSkip},
				{Action: dload.DiffResolverRecv},
				{Action: dload.DiffResolverRecv},
				{Action: dload.DiffResolverEOF},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := &mockDiffResolverCtx{}
			dr := dload.NewDiffResolver(ctx)
			go dr.Start()
			for _, s := range test.src {
				lom := &cluster.LOM{ObjName: s.name}
				if s.remote {
					lom.FQN = fromRemoteFQN
				}
				dr.PushSrc(lom)
			}
			dr.CloseSrc()
			for _, d := range test.dst {
				dr.PushDst(&dload.BackendResource{ObjName: d.name})
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
				switch result.Action {
				case dload.DiffResolverRecv:
					tassert.Errorf(t, result.Dst != nil, "destination has not been set for recv")
				case dload.DiffResolverSend:
					tassert.Errorf(t, result.Src != nil, "source has not been set for send")
				case dload.DiffResolverDelete:
					tassert.Errorf(t, result.Src != nil, "source has not been set for delete")
				case dload.DiffResolverSkip:
					tassert.Errorf(t, result.Src != nil, "source has not been set for skip")
					tassert.Errorf(t, result.Dst != nil, "destination has not been set for skip")
				case dload.DiffResolverEOF:
				default:
					debug.Assertf(false, "invalid diff-resolver action %d", result.Action)
				}
			}

			// Check that EOF is followed by EOF.
			for i := 0; i < 2; i++ {
				result, err := dr.Next()
				tassert.CheckFatal(t, err)
				tassert.Errorf(t, result.Action == dload.DiffResolverEOF, "eof not followed by eof")
			}
		})
	}
}
