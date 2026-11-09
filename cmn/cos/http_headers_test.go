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

func TestParseCondWrite(t *testing.T) {
	for _, tc := range []struct {
		name   string
		header http.Header
		want   cos.CondWrite
		fail   bool
	}{
		{"none", http.Header{}, cos.CondWrite{}, false},
		{"quoted", http.Header{"If-Match": {`"etag-1"`}}, cos.CondWrite{IfMatch: "etag-1", MatchETag: true}, false},
		{"bare", http.Header{"If-Match": {"etag-1"}}, cos.CondWrite{IfMatch: "etag-1", MatchETag: true}, false},
		{"bare-trimmed", http.Header{"If-Match": {"  etag-1 "}}, cos.CondWrite{IfMatch: "etag-1", MatchETag: true}, false},
		{"multipart", http.Header{"If-Match": {`"d41d8cd98f00b204e9800998ecf8427e-3"`}}, cos.CondWrite{IfMatch: "d41d8cd98f00b204e9800998ecf8427e-3", MatchETag: true}, false},
		{"create-only", http.Header{"If-None-Match": {"*"}}, cos.CondWrite{IfNoneMatch: "*"}, false},
		{"quoted-comma", http.Header{"If-Match": {`"one,two"`}}, cos.CondWrite{IfMatch: "one,two", MatchETag: true}, false},
		// forwarded as is: the remote store decides
		{"wildcard", http.Header{"If-Match": {"*"}}, cos.CondWrite{IfMatch: "*"}, false},
		{"quoted-star", http.Header{"If-Match": {`"*"`}}, cos.CondWrite{IfMatch: `"*"`}, false},
		{"list", http.Header{"If-Match": {`"one", "two"`}}, cos.CondWrite{IfMatch: `"one", "two"`}, false},
		{"bare-list", http.Header{"If-Match": {"one,two"}}, cos.CondWrite{IfMatch: "one,two"}, false},
		{"weak", http.Header{"If-Match": {`W/"etag"`}}, cos.CondWrite{IfMatch: `W/"etag"`}, false},
		{"none-match-etag", http.Header{"If-None-Match": {`"etag"`}}, cos.CondWrite{IfNoneMatch: `"etag"`}, false},
		{"both", http.Header{"If-Match": {`"etag"`}, "If-None-Match": {"*"}}, cos.CondWrite{IfMatch: "etag", IfNoneMatch: "*", MatchETag: true}, false},
		// rejected
		{"empty-quoted", http.Header{"If-Match": {`""`}}, cos.CondWrite{}, true},
		{"empty", http.Header{"If-Match": {""}}, cos.CondWrite{}, true},
		{"multiple-values", http.Header{"If-Match": {`"one"`, `"two"`}}, cos.CondWrite{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cw, err := cos.ParseCondWrite(tc.header)
			if tc.fail {
				if err == nil {
					t.Fatalf("ParseCondWrite(%v): expected error, got %+v", tc.header, cw)
				}
				return
			}
			if err != nil || cw != tc.want {
				t.Fatalf("ParseCondWrite(%v) = %+v, %v; want %+v", tc.header, cw, err, tc.want)
			}
		})
	}
}
