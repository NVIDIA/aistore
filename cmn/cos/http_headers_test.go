// Package cos_test: unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos_test

import (
	"net/http"
	"testing"

	"github.com/NVIDIA/aistore/cmn/cos"
)

func TestIsCondWrite(t *testing.T) {
	for _, tc := range []struct {
		name   string
		header http.Header
		want   bool
	}{
		{"nil", nil, false},
		{"empty", http.Header{}, false},
		{"unrelated", http.Header{"Content-Type": {"application/json"}}, false},
		{"match", http.Header{"If-Match": {`"etag"`}}, true},
		{"create-only", http.Header{"If-None-Match": {"*"}}, true},
		{"empty-match", http.Header{"If-Match": {""}}, true},
		{"empty-none-match", http.Header{"If-None-Match": {""}}, true},
		{"malformed", http.Header{"If-Match": {"invalid"}}, true},
		{"multiple-values", http.Header{"If-Match": {`"one"`, `"two"`}}, true},
		{"both", http.Header{"If-Match": {`"etag"`}, "If-None-Match": {"*"}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := cos.IsCondWrite(tc.header); got != tc.want {
				t.Fatalf("IsCondWrite(%v) = %t, want %t", tc.header, got, tc.want)
			}
		})
	}
}
