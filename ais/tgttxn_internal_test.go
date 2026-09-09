// Package ais: internal unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/url"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/tools/tassert"
)

// must reject anything but a positive nanosecond timestamp:
// 0 would drop the time component of the xid, negative would wrap to uint64
func TestTxnPtime(t *testing.T) {
	const ns = int64(1_700_000_000_000_000_000)

	tests := []struct {
		name    string
		set     bool
		val     string
		expect  uint64
		wantErr bool
	}{
		{name: "valid", set: true, val: unixNano2S(ns), expect: uint64(ns)},
		{name: "missing", set: false},
		{name: "empty", set: true, val: "", wantErr: true},
		{name: "malformed", set: true, val: "!!!", wantErr: true},
		{name: "negative", set: true, val: unixNano2S(-ns), wantErr: true},
		{name: "zero", set: true, val: unixNano2S(0), wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query := url.Values{}
			if test.set {
				query.Set(apc.QparamUnixTime, test.val)
			}
			c := &txnSrv{query: query}

			ptime, err := c.ptime()
			if test.wantErr || !test.set {
				tassert.Errorf(t, err != nil, "expected an error, got ptime=%d", ptime)
				return
			}
			tassert.CheckFatal(t, err)
			tassert.Errorf(t, ptime == test.expect, "expected %d, got %d", test.expect, ptime)
		})
	}
}
