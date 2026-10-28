// Package aisloader
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package aisloader

import (
	"net/http"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// rangeObjSize: verifies the full object size is parsed from 206 and 416 Content-Range headers.
func TestRangeObjSize(t *testing.T) {
	tests := []struct {
		name string
		hdr  string // Content-Range value; empty means header not set
		want int64
	}{
		{name: "206 partial", hdr: "bytes 0-99/1000", want: 1000},
		{name: "206 tail", hdr: "bytes 900-999/1000", want: 1000},
		{name: "416 unsatisfiable", hdr: "bytes */500", want: 500},
		{name: "missing header", want: 0},
		{name: "unknown size", hdr: "bytes 0-99/*", want: 0},
		{name: "malformed", hdr: "garbage", want: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hdr := http.Header{}
			if test.hdr != "" {
				hdr.Set(cos.HdrContentRange, test.hdr)
			}
			if got := rangeObjSize(hdr); got != test.want {
				t.Fatalf("rangeObjSize(%q) = %d, want %d", test.hdr, got, test.want)
			}
		})
	}
}
