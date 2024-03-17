// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"reflect"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/cmn/debug"
)

type (
	StrSet map[string]struct{}
	StrKVs map[string]string
)

////////////
// StrKVs //
////////////

func NewStrKVs(l int) StrKVs {
	return make(StrKVs, l)
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

func (kvs StrKVs) Delete(key string) {
	delete(kvs, key)
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
