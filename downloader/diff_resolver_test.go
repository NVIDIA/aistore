// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package downloader_test

import (
	"testing"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/downloader"
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
		expected []downloader.DiffResolverResult
	}
)

func (*mockDiffResolverCtx) CompareObjects(*cluster.LOM, *downloader.DstElement) (bool, error) {
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
			expected: []downloader.DiffResolverResult{{Action: downloader.DiffResolverEOF}},
		},
		{
			name: "all_send",
			src:  []obj{{name: "a"}, {name: "b"}, {name: "c"}},
			dst:  []obj{},
			expected: []downloader.DiffResolverResult{
				{Action: downloader.DiffResolverSend},
				{Action: downloader.DiffResolverSend},
				{Action: downloader.DiffResolverSend},
				{Action: downloader.DiffResolverEOF},
			},
		},
		{
			name: "all_recv",
			src:  []obj{},
			dst:  []obj{{name: "a"}, {name: "b"}, {name: "c"}},
			expected: []downloader.DiffResolverResult{
				{Action: downloader.DiffResolverRecv},
				{Action: downloader.DiffResolverRecv},
				{Action: downloader.DiffResolverRecv},
				{Action: downloader.DiffResolverEOF},
			},
		},
		{
			name: "mixed_send_recv",
			src:  []obj{{name: "a"}, {name: "c"}},
			dst:  []obj{{name: "b"}, {name: "d"}},
			expected: []downloader.DiffResolverResult{
				{Action: downloader.DiffResolverSend},
				{Action: downloader.DiffResolverRecv},
				{Action: downloader.DiffResolverSend},
				{Action: downloader.DiffResolverRecv},
				{Action: downloader.DiffResolverEOF},
			},
		},
		{
			name: "all_send_then_all_recv",
			src:  []obj{{name: "a"}, {name: "b"}, {name: "c"}},
			dst:  []obj{{name: "d"}, {name: "e"}},
			expected: []downloader.DiffResolverResult{
				{Action: downloader.DiffResolverSend},
				{Action: downloader.DiffResolverSend},
				{Action: downloader.DiffResolverSend},
				{Action: downloader.DiffResolverRecv},
				{Action: downloader.DiffResolverRecv},
				{Action: downloader.DiffResolverEOF},
			},
		},
		{
			name: "all_recv_then_all_send",
			src:  []obj{{name: "d"}, {name: "e"}},
			dst:  []obj{{name: "a"}, {name: "b"}, {name: "c"}},
			expected: []downloader.DiffResolverResult{
				{Action: downloader.DiffResolverRecv},
				{Action: downloader.DiffResolverRecv},
				{Action: downloader.DiffResolverRecv},
				{Action: downloader.DiffResolverSend},
				{Action: downloader.DiffResolverSend},
				{Action: downloader.DiffResolverEOF},
			},
		},
		{
			name: "all_delete",
			src:  []obj{{name: "a", remote: true}, {name: "b", remote: true}},
			dst:  []obj{},
			expected: []downloader.DiffResolverResult{
				{Action: downloader.DiffResolverDelete},
				{Action: downloader.DiffResolverDelete},
				{Action: downloader.DiffResolverEOF},
			},
		},
		{
			name: "mixed_send_delete",
			src:  []obj{{name: "a"}, {name: "b", remote: true}, {name: "c"}, {name: "d", remote: true}},
			dst:  []obj{},
			expected: []downloader.DiffResolverResult{
				{Action: downloader.DiffResolverSend},
				{Action: downloader.DiffResolverDelete},
				{Action: downloader.DiffResolverSend},
				{Action: downloader.DiffResolverDelete},
				{Action: downloader.DiffResolverEOF},
			},
		},
		{
			name: "all_skip_then_all_recv",
			src:  []obj{{name: "a", remote: true}, {name: "b", remote: true}},
			dst:  []obj{{name: "a"}, {name: "b"}, {name: "c"}, {name: "d"}},
			expected: []downloader.DiffResolverResult{
				{Action: downloader.DiffResolverSkip},
				{Action: downloader.DiffResolverSkip},
				{Action: downloader.DiffResolverRecv},
				{Action: downloader.DiffResolverRecv},
				{Action: downloader.DiffResolverEOF},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := &mockDiffResolverCtx{}
			dr := downloader.NewDiffResolver(ctx)
			dr.Start()
			for _, s := range test.src {
				lom := &cluster.LOM{ObjName: s.name}
				if s.remote {
					lom.FQN = fromRemoteFQN
				}
				dr.PushSrc(lom)
			}
			dr.CloseSrc()
			for _, d := range test.dst {
				dr.PushDst(&downloader.BackendResource{ObjName: d.name})
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
				case downloader.DiffResolverRecv:
					tassert.Errorf(t, result.Dst != nil, "destination has not been set for recv")
				case downloader.DiffResolverSend:
					tassert.Errorf(t, result.Src != nil, "source has not been set for send")
				case downloader.DiffResolverDelete:
					tassert.Errorf(t, result.Src != nil, "source has not been set for delete")
				case downloader.DiffResolverSkip:
					tassert.Errorf(t, result.Src != nil, "source has not been set for skip")
					tassert.Errorf(t, result.Dst != nil, "destination has not been set for skip")
				case downloader.DiffResolverEOF:
				default:
					cos.Assertf(false, "invalid diff-resolver action %d", result.Action)
				}
			}

			// Check that EOF is followed by EOF.
			for i := 0; i < 2; i++ {
				result, err := dr.Next()
				tassert.CheckFatal(t, err)
				tassert.Errorf(t, result.Action == downloader.DiffResolverEOF, "eof not followed by eof")
			}
		})
	}
}
