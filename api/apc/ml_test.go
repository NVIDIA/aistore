// Package apc_test: tests for API control messages and constants.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package apc_test

import (
	"encoding/json"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
)

// TestMossInUnmarshalJSON covers MossIn.UnmarshalJSON range validation - positive
// (whole-object, bounded, open-ended) and negative (invalid range / object name) cases.
func TestMossInUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{"plain", `{"objname":"obj"}`, false},
		{"whole-object-zeros", `{"objname":"obj","start":0,"length":0}`, false},
		{"bounded", `{"objname":"obj","start":10,"length":20}`, false},
		{"open-ended", `{"objname":"obj","start":10,"length":-1}`, false},
		{"open-ended-from-zero", `{"objname":"obj","length":-1}`, false},
		{"negative-start", `{"objname":"obj","start":-1,"length":10}`, true},
		{"length-below-neg-one", `{"objname":"obj","length":-2}`, true},
		{"nonzero-start-zero-length", `{"objname":"obj","start":5}`, true},
		{"empty-objname", `{"objname":""}`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var in apc.MossIn
			err := json.Unmarshal([]byte(tt.body), &in)
			if tt.wantErr && err == nil {
				t.Fatalf("%s: expected error, got none (in=%+v)", tt.body, in)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("%s: unexpected error: %v", tt.body, err)
			}
		})
	}
}

// TestMossInCheckRange covers the (offset+length) range semantics, including
// open-ended ranges where Length == -1 (cos.ContentLengthUnknown) means "read from Start to EOF".
func TestMossInCheckRange(t *testing.T) {
	const size = int64(100)
	tests := []struct {
		name           string
		start, length  int64
		wantOff, wantL int64
		wantErr        bool
	}{
		{"whole-object", 0, 0, 0, size, false},
		{"closed-leading", 0, 10, 0, 10, false},
		{"closed-middle", 40, 10, 40, 10, false},
		{"closed-to-exact-end", 90, 10, 90, 10, false},
		{"open-ended", 40, -1, 40, 60, false},
		{"open-ended-from-zero", 0, -1, 0, size, false},
		{"open-ended-last-byte", 99, -1, 99, 1, false},
		{"zero-length-with-start", 40, 0, 0, 0, true},
		{"closed-out-of-bounds-len", 90, 20, 0, 0, true},
		{"start-at-eof", 100, 10, 0, 0, true},
		{"open-ended-start-at-eof", 100, -1, 0, 0, true},
		{"start-past-eof", 150, -1, 0, 0, true},
		{"negative-start", -1, 10, 0, 0, true},
		{"length-below-neg-one", 0, -5, 0, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := &apc.MossIn{Start: tt.start, Length: tt.length}
			off, length, err := in.CheckRange(size)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("start=%d,length=%d: expected error, got off=%d,length=%d",
						tt.start, tt.length, off, length)
				}
				return
			}
			if err != nil {
				t.Fatalf("start=%d,length=%d: unexpected error: %v", tt.start, tt.length, err)
			}
			if off != tt.wantOff || length != tt.wantL {
				t.Fatalf("start=%d,length=%d: got (off=%d,length=%d), want (off=%d,length=%d)",
					tt.start, tt.length, off, length, tt.wantOff, tt.wantL)
			}
		})
	}
}
