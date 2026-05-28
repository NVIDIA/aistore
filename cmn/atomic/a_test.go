// Package atomic provides simple wrappers around numerics to enforce atomic access.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package atomic_test

import (
	"encoding/json"
	"strconv"
	stdatomic "sync/atomic"
	"testing"

	"github.com/NVIDIA/aistore/cmn/atomic"
)

func TestInt64JSONRoundTrip(t *testing.T) {
	for _, want := range []int64{0, 1, -1, 1 << 30, -1 << 30, 1700000000000000000} {
		in := atomic.NewInt64(want)
		data, err := json.Marshal(in)
		if err != nil {
			t.Fatalf("marshal %d: %v", want, err)
		}
		if string(data) != `{"v":`+strconv.FormatInt(want, 10)+`}` {
			t.Fatalf("unexpected wire format for %d: %s", want, data)
		}
		var got atomic.Int64
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("unmarshal %s: %v", data, err)
		}
		if got.Load() != want {
			t.Fatalf("round-trip mismatch: want %d, got %d (json=%s)", want, got.Load(), data)
		}
	}
}

// Cross-version IC sync compat: new code must accept legacy `{}` (from a
// pre-MarshalJSON peer), and an older default-struct decoder must accept the
// new `{"v":N}` without error - both yielding zero, matching current behavior.
func TestInt64JSONMixedVersionCompat(t *testing.T) {
	type newShape struct {
		EndTimeX atomic.Int64 `json:"EndTimeX"`
	}
	type oldShape struct {
		EndTimeX stdatomic.Int64 `json:"EndTimeX"`
	}

	// old -> new: legacy `{}` decodes to zero
	var n newShape
	if err := json.Unmarshal([]byte(`{"EndTimeX":{}}`), &n); err != nil {
		t.Fatalf("new failed to decode legacy {}: %v", err)
	}
	if n.EndTimeX.Load() != 0 {
		t.Fatalf("expected 0 from legacy {}, got %d", n.EndTimeX.Load())
	}

	// backward-compat: `{"v":N}` parsed to 0 by default struct decoder (unknown key ignored)
	var o oldShape
	if err := json.Unmarshal([]byte(`{"EndTimeX":{"v":1700000000000000000}}`), &o); err != nil {
		t.Fatalf("old failed to decode new format: %v", err)
	}
	if o.EndTimeX.Load() != 0 {
		t.Fatalf("old decoder should yield zero from unknown nested object, got %d", o.EndTimeX.Load())
	}

	// forward-compat: new also accepts a bare number
	var n2 newShape
	if err := json.Unmarshal([]byte(`{"EndTimeX":12345}`), &n2); err != nil {
		t.Fatalf("new failed to decode bare number: %v", err)
	}
	if n2.EndTimeX.Load() != 12345 {
		t.Fatalf("expected 12345, got %d", n2.EndTimeX.Load())
	}
}
