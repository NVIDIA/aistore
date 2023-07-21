// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"runtime"
	rdebug "runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	jsoniter "github.com/json-iterator/go"
)

const MLCG32 = 1103515245 // xxhash seed

type (
	StrSet map[string]struct{}
	StrKVs map[string]string

	JSONRawMsgs map[string]jsoniter.RawMessage
)

// JSON is used to Marshal/Unmarshal API json messages and is initialized in init function.
var JSON jsoniter.API

func init() {
	rtie.Store(1013)

	jsonConf := jsoniter.Config{
		EscapeHTML:             false, // we don't send HTMLs
		ValidateJsonRawMessage: false, // RawMessages are validated by "morphing"
		DisallowUnknownFields:  true,  // make sure we have exactly the struct user requested.
		SortMapKeys:            true,
	}
	JSON = jsonConf.Froze()
}

//
// JSON & JSONLocal
//

func MustMarshalToString(v any) string {
	s, err := JSON.MarshalToString(v)
	debug.AssertNoErr(err)
	return s
}

// MustMarshal marshals v and panics if error occurs.
func MustMarshal(v any) []byte {
	b, err := JSON.Marshal(v)
	AssertNoErr(err)
	return b
}

func MorphMarshal(data, v any) error {
	// `data` can be of type `map[string]any` or just same type as `v`.
	// Therefore, the easiest way is to marshal the `data` again and unmarshal it
	// with hope that every field will be set correctly.
	b := MustMarshal(data)
	return JSON.Unmarshal(b, v)
}

func MustMorphMarshal(data, v any) {
	err := MorphMarshal(data, v)
	AssertNoErr(err)
}

////////////
// StrKVs //
////////////

func NewStrKVs(pairs ...string) (kvs StrKVs) {
	l := len(pairs) / 2
	debug.Assert(len(pairs) == l<<1)
	kvs = make(StrKVs, l)
	for i := 0; i < l; i++ {
		kvs[pairs[2*i]] = kvs[pairs[2*i+1]]
	}
	return
}

func (kvs StrKVs) Compare(other StrKVs) bool {
	if len(kvs) != len(other) {
		return false
	} else if len(kvs) > 0 {
		return reflect.DeepEqual(kvs, other)
	}
	return true
}

func (kvs StrKVs) Keys() []string {
	keys := make([]string, 0, len(kvs))
	for k := range kvs {
		keys = append(keys, k)
	}
	return keys
}

func (kvs StrKVs) KeyFor(value string) (key string) {
	for k, v := range kvs {
		if v == value {
			key = k
			break
		}
	}
	return
}

func (kvs StrKVs) Contains(key string) (ok bool) {
	_, ok = kvs[key]
	return
}

func (kvs StrKVs) ContainsAnyMatch(in []string) string {
	for _, k := range in {
		debug.Assert(k != "")
		for kk := range kvs {
			if strings.Contains(kk, k) {
				return kk
			}
		}
	}
	return ""
}

////////////
// StrSet //
////////////

func NewStrSet(keys ...string) (ss StrSet) {
	ss = make(StrSet, len(keys))
	ss.Add(keys...)
	return
}

func (ss StrSet) String() string {
	keys := ss.ToSlice()
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

func (ss StrSet) ToSlice() []string {
	keys := make([]string, len(ss))
	idx := 0
	for key := range ss {
		keys[idx] = key
		idx++
	}
	return keys
}

func (ss StrSet) Set(key string) {
	ss[key] = struct{}{}
}

func (ss StrSet) Add(keys ...string) {
	for _, key := range keys {
		ss[key] = struct{}{}
	}
}

func (ss StrSet) Contains(key string) (yes bool) {
	_, yes = ss[key]
	return
}

func (ss StrSet) Delete(key string) {
	delete(ss, key)
}

func (ss StrSet) Intersection(other StrSet) StrSet {
	result := make(StrSet)
	for key := range ss {
		if other.Contains(key) {
			result.Set(key)
		}
	}
	return result
}

func (ss StrSet) Clone() StrSet {
	result := make(StrSet, len(ss))
	for k, v := range ss {
		result[k] = v
	}
	return result
}

func (ss StrSet) All(xs ...string) bool {
	for _, x := range xs {
		if !ss.Contains(x) {
			return false
		}
	}
	return true
}

// shallow copy
func CopyStruct(dst, src any) {
	x := reflect.ValueOf(src)
	debug.Assert(x.Kind() == reflect.Ptr)
	starX := x.Elem()
	y := reflect.New(starX.Type())
	starY := y.Elem()
	starY.Set(starX)
	reflect.ValueOf(dst).Elem().Set(y.Elem())
}

func Infof(format string, a ...any) {
	if flag.Parsed() {
		nlog.InfoDepth(1, fmt.Sprintf(format, a...))
	} else {
		fmt.Printf(format+"\n", a...)
	}
}

func Errorf(format string, a ...any) {
	if flag.Parsed() {
		nlog.ErrorDepth(1, fmt.Sprintf(format, a...))
	} else {
		fmt.Fprintf(os.Stderr, format+"\n", a...)
	}
}

// FreeMemToOS calls GC and returns allocated memory to OS after that
// Use to clean up memory after a huge amount of memory becomes "free" to
// return it to OS immediately without waiting for GC does it automatically
// Params:
//
//	d - a delay before starting memory cleanup
func FreeMemToOS(d ...time.Duration) {
	if len(d) != 0 && d[0] != 0 {
		time.Sleep(d[0])
	}
	runtime.GC()
	rdebug.FreeOSMemory()
}

// (common use)
func Plural(num int) (s string) {
	if num != 1 {
		s = "s"
	}
	return
}
