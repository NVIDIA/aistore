// Package atomic provides simple wrappers around numerics to enforce atomic
// access.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package atomic

import (
	"encoding/json"
	"sync/atomic"
	"time"
)

//
// int32
//

type Int32 atomic.Int32

func NewInt32(i int32) *Int32 {
	v := &atomic.Int32{}
	v.Store(i)
	return (*Int32)(v)
}

func (v *Int32) Load() int32         { return (*atomic.Int32)(v).Load() }
func (v *Int32) Store(n int32)       { (*atomic.Int32)(v).Store(n) }
func (v *Int32) Add(n int32) int32   { return (*atomic.Int32)(v).Add(n) }
func (v *Int32) Inc() int32          { return (*atomic.Int32)(v).Add(1) }
func (v *Int32) Dec() int32          { return (*atomic.Int32)(v).Add(-1) }
func (v *Int32) CAS(o, n int32) bool { return (*atomic.Int32)(v).CompareAndSwap(o, n) }
func (v *Int32) Swap(n int32) int32  { return (*atomic.Int32)(v).Swap(n) }

//
// uint32
//

type Uint32 atomic.Uint32

func NewUint32(i uint32) *Uint32 {
	v := &atomic.Uint32{}
	v.Store(i)
	return (*Uint32)(v)
}

func (v *Uint32) Load() uint32         { return (*atomic.Uint32)(v).Load() }
func (v *Uint32) Store(n uint32)       { (*atomic.Uint32)(v).Store(n) }
func (v *Uint32) Add(n uint32) uint32  { return (*atomic.Uint32)(v).Add(n) }
func (v *Uint32) Inc() uint32          { return (*atomic.Uint32)(v).Add(1) }
func (v *Uint32) CAS(o, n uint32) bool { return (*atomic.Uint32)(v).CompareAndSwap(o, n) }
func (v *Uint32) Swap(n uint32) uint32 { return (*atomic.Uint32)(v).Swap(n) }

//
// int64
//

type Int64 atomic.Int64

func NewInt64(i int64) *Int64 {
	v := &atomic.Int64{}
	v.Store(i)
	return (*Int64)(v)
}

func (v *Int64) Load() int64         { return (*atomic.Int64)(v).Load() }
func (v *Int64) Store(n int64)       { (*atomic.Int64)(v).Store(n) }
func (v *Int64) Add(n int64) int64   { return (*atomic.Int64)(v).Add(n) }
func (v *Int64) Sub(n int64) int64   { return (*atomic.Int64)(v).Add(-n) }
func (v *Int64) Inc() int64          { return (*atomic.Int64)(v).Add(1) }
func (v *Int64) Dec() int64          { return (*atomic.Int64)(v).Add(-1) }
func (v *Int64) CAS(o, n int64) bool { return (*atomic.Int64)(v).CompareAndSwap(o, n) }
func (v *Int64) Swap(n int64) int64  { return (*atomic.Int64)(v).Swap(n) }

//
// uint64
//

type Uint64 atomic.Uint64

func NewUint64(i uint64) *Uint64 {
	v := &atomic.Uint64{}
	v.Store(i)
	return (*Uint64)(v)
}

func (v *Uint64) Load() uint64         { return (*atomic.Uint64)(v).Load() }
func (v *Uint64) Store(n uint64)       { (*atomic.Uint64)(v).Store(n) }
func (v *Uint64) Add(n uint64) uint64  { return (*atomic.Uint64)(v).Add(n) }
func (v *Uint64) Sub(n uint64) uint64  { return (*atomic.Uint64)(v).Add(^(n - 1)) }
func (v *Uint64) Inc() uint64          { return (*atomic.Uint64)(v).Add(1) }
func (v *Uint64) CAS(o, n uint64) bool { return (*atomic.Uint64)(v).CompareAndSwap(o, n) }
func (v *Uint64) Swap(n uint64) uint64 { return (*atomic.Uint64)(v).Swap(n) }

//
// bool
//

type Bool atomic.Bool

// interface guard
var (
	_ json.Marshaler   = (*Bool)(nil)
	_ json.Unmarshaler = (*Bool)(nil)
)

func NewBool(i bool) *Bool {
	v := &atomic.Bool{}
	v.Store(i)
	return (*Bool)(v)
}

func (v *Bool) Load() bool         { return (*atomic.Bool)(v).Load() }
func (v *Bool) Store(n bool)       { (*atomic.Bool)(v).Store(n) }
func (v *Bool) CAS(o, n bool) bool { return (*atomic.Bool)(v).CompareAndSwap(o, n) }
func (v *Bool) Swap(n bool) bool   { return (*atomic.Bool)(v).Swap(n) }

func (v *Bool) Toggle() bool {
	if v.CAS(true, false) {
		return true
	}
	return !v.CAS(false, true)
}

func (v *Bool) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.Load())
}

func (v *Bool) UnmarshalJSON(data []byte) error {
	var y bool
	if err := json.Unmarshal(data, &y); err != nil {
		return err
	}
	v.Store(y)
	return nil
}

//
// time
//

type Time atomic.Int64

// interface guard
var (
	_ json.Marshaler   = (*Time)(nil)
	_ json.Unmarshaler = (*Time)(nil)
)

func NewTime(t time.Time) *Time {
	v := &atomic.Int64{}
	v.Store(t.UnixNano())
	return (*Time)(v)
}

func (t *Time) Load() time.Time   { return time.Unix(0, (*atomic.Int64)(t).Load()) }
func (t *Time) Store(n time.Time) { (*atomic.Int64)(t).Store(n.UnixNano()) }

func (t *Time) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Load().UnixNano())
}

func (t *Time) UnmarshalJSON(data []byte) error {
	var y int64
	if err := json.Unmarshal(data, &y); err != nil {
		return err
	}
	t.Store(time.Unix(0, y))
	return nil
}
