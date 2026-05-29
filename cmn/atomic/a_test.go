// Package atomic provides simple wrappers around numerics to enforce atomic access.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package atomic_test

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"

	jsoniter "github.com/json-iterator/go"
)

// Each atomic type with a custom MarshalJSON/UnmarshalJSON must round-trip
// through cos.MustMarshal (cos.JSON, frozen) into any of the three decoders:
// - default jsoniter: jsoniter.ConfigDefault
// - project frozen config cos.JSON
// - stdlib encoding/json.
var unmarshalers = []struct {
	name string
	fn   func([]byte, any) error
}{
	{"jsoniter", jsoniter.Unmarshal},
	{"cos.JSON", cos.JSON.Unmarshal},
	{"stdlib", json.Unmarshal},
}

func TestInt64JSONRoundTrip(t *testing.T) {
	for _, want := range []int64{0, 1, -1, 1 << 30, -1 << 30, 1700000000000000000} {
		data := cos.MustMarshal(atomic.NewInt64(want))
		if s, exp := string(data), `{"v":`+strconv.FormatInt(want, 10)+`}`; s != exp {
			t.Fatalf("wire format for %d: got %s, want %s", want, s, exp)
		}
		for _, u := range unmarshalers {
			t.Run(strconv.FormatInt(want, 10)+"/"+u.name, func(t *testing.T) {
				var got atomic.Int64
				if err := u.fn(data, &got); err != nil {
					t.Fatalf("unmarshal %s: %v", data, err)
				}
				if got.Load() != want {
					t.Fatalf("round-trip mismatch: want %d, got %d", want, got.Load())
				}
			})
		}
	}
}

func TestBoolJSONRoundTrip(t *testing.T) {
	for _, want := range []bool{true, false} {
		data := cos.MustMarshal(atomic.NewBool(want))
		if s, exp := string(data), strconv.FormatBool(want); s != exp {
			t.Fatalf("wire format for %v: got %s, want %s", want, s, exp)
		}
		for _, u := range unmarshalers {
			t.Run(strconv.FormatBool(want)+"/"+u.name, func(t *testing.T) {
				var got atomic.Bool
				if err := u.fn(data, &got); err != nil {
					t.Fatalf("unmarshal %s: %v", data, err)
				}
				if got.Load() != want {
					t.Fatalf("round-trip mismatch: want %v, got %v", want, got.Load())
				}
			})
		}
	}
}

func TestTimeJSONRoundTrip(t *testing.T) {
	for _, want := range []time.Time{
		time.Unix(0, 0),
		time.Unix(0, 1),
		time.Unix(0, 1700000000000000000),
	} {
		data := cos.MustMarshal(atomic.NewTime(want))
		if s, exp := string(data), strconv.FormatInt(want.UnixNano(), 10); s != exp {
			t.Fatalf("wire format for %v: got %s, want %s", want, s, exp)
		}
		for _, u := range unmarshalers {
			t.Run(strconv.FormatInt(want.UnixNano(), 10)+"/"+u.name, func(t *testing.T) {
				var got atomic.Time
				if err := u.fn(data, &got); err != nil {
					t.Fatalf("unmarshal %s: %v", data, err)
				}
				if got.Load().UnixNano() != want.UnixNano() {
					t.Fatalf("round-trip mismatch: want %v, got %v", want.UnixNano(), got.Load().UnixNano())
				}
			})
		}
	}
}
