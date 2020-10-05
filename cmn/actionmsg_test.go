// Package provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */

package cmn

import (
	"testing"

	jsoniter "github.com/json-iterator/go"
)

type actmsgTestConf struct {
	action string
	vals   []string
}

func testRawUnmarshal(t *testing.T, tc actmsgTestConf) {
	t.Run(tc.action, func(t *testing.T) {
		for _, val := range tc.vals {
			acmsg := &ActionMsg{}
			err := jsoniter.Unmarshal([]byte(val), &acmsg)
			if err != nil {
				t.Errorf("actionMsg unmarshaled failed for action: %s, val: %s, err: %v", tc.action, val, err)
			}
		}
	})
}

func TestActMsgRawUnmarshal(t *testing.T) {
	tests := []actmsgTestConf{
		{
			action: ActEvictObjects,
			vals: []string{
				`{"action":"evictobj","value":{"template":"__tst/test-{1000..2000}"}}`,
				`{"action":"evictobj","value":{"objnames":["o1","o2","o3"]}}`,
			},
		},
		{
			action: ActPrefetch,
			vals: []string{
				`{"action":"prefetch","value":{"template":"__tst/test-{1000..2000}"}}`,
				`{"action":"prefetch","value":{"objnames":["o1","o2","o3"]}}`,
			},
		},
		{
			action: ActDelete,
			vals: []string{
				`{"action":"delete","value":{"template":"__tst/test-{1000..2000}"}}`,
				`{"action":"delete","value":{"objnames":["o1","o2","o3"]}}`,
			},
		},
		{
			action: ActSetBprops,
			vals: []string{
				`{"action":"setbprops","value":{"checksum": {"type": "sha256"}, "mirror": {"enable": true}}}`,
			},
		},
		{
			action: ActCreateLB,
			vals: []string{
				`{"action":"createlb","value":{"checksum": {"type": "sha256"}, "mirror": {"enable": true}}}`,
			},
		},
		{
			action: ActXactStart,
			vals: []string{
				`{"action":"start","value":{"kind": "rebalance"}}`,
			},
		},
		{
			action: ActXactStop,
			vals: []string{
				`{"action":"stop","value":{"kind": "rebalance"}}`,
			},
		},
		{
			action: ActListObjects,
			vals: []string{
				`{"action":"listobj","value":{"props": "size"}}`,
			},
		},
		{
			action: ActPromote,
			vals: []string{
				`{"action":"promote","value":{"target": "234ed78", "recurs": true, "keep": false}}`,
			},
		},
	}
	for _, test := range tests {
		testRawUnmarshal(t, test)
	}
}
