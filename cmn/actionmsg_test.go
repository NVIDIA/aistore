// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */

package cmn_test

import (
	"fmt"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"

	jsoniter "github.com/json-iterator/go"
)

type actmsgTestConf struct {
	action       string
	vals         []string
	isSelectObjs bool
}

func testRawUnmarshal(t *testing.T, tc actmsgTestConf) {
	t.Run(tc.action, func(t *testing.T) {
		for _, val := range tc.vals {
			msg := &apc.ActMsg{}
			raw := fmt.Sprintf(val, tc.action)
			err := jsoniter.Unmarshal([]byte(raw), &msg)
			tassert.CheckError(t, err)
			tassert.Errorf(t, tc.action == msg.Action, "actions do not match (%q vs %q)", tc.action, msg.Action)

			// parse the template
			if tc.isSelectObjs {
				lrMsg := &apc.ListRange{}
				err = cos.MorphMarshal(msg.Value, lrMsg)
				tassert.CheckError(t, err)
			}
		}
	})
}

func TestActMsgRawUnmarshal(t *testing.T) {
	tests := []actmsgTestConf{
		{
			action: apc.ActEvictObjects,
			vals: []string{
				`{"action":"%s","value":{"template":"__tst/test-{1000..2000}"}}`,
				`{"action":"%s","value":{"objnames":["o1","o2","o3"]}}`,
			},
			isSelectObjs: true,
		},
		{
			action: apc.ActPrefetchObjects,
			vals: []string{
				`{"action":"%s","value":{"template":"__tst/test-{1000..2000}"}}`,
				`{"action":"%s","value":{"objnames":["o1","o2","o3"]}}`,
			},
			isSelectObjs: true,
		},
		{
			action: apc.ActDeleteObjects,
			vals: []string{
				`{"action":"%s","value":{"template":"__tst/test-{1000..2000}"}}`,
				`{"action":"%s","value":{"objnames":["o1","o2","o3"]}}`,
			},
			isSelectObjs: true,
		},
		{
			action: apc.ActSetBprops,
			vals: []string{
				`{"action":"%s","value":{"checksum": {"type": "sha256"}, "mirror": {"enable": true}}}`,
			},
		},
		{
			action: apc.ActCreateBck,
			vals: []string{
				`{"action":"%s","value":{"checksum": {"type": "sha256"}, "mirror": {"enable": true}}}`,
			},
		},
		{
			action: apc.ActXactStart,
			vals: []string{
				`{"action":"%s","value":{"kind": "rebalance"}}`,
			},
		},
		{
			action: apc.ActXactStop,
			vals: []string{
				`{"action":"%s","value":{"kind": "rebalance"}}`,
			},
		},
		{
			action: apc.ActList,
			vals: []string{
				`{"action":"%s","value":{"props": "size"}}`,
			},
		},
		{
			action: apc.ActPromote,
			vals: []string{
				`{"action":"%s","value":{"target": "234ed78", "recurs": true, "keep": false}}`,
			},
		},
	}
	for _, test := range tests {
		testRawUnmarshal(t, test)
	}
}
