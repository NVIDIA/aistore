// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import "strings"

func StringInSlice(s string, arr []string) bool {
	for _, el := range arr {
		if el == s {
			return true
		}
	}
	return false
}

// StrSlicesEqual compares content of two string slices. It is replacement for
// reflect.DeepEqual because the latter returns false if slices have the same
// values but in different order.
func StrSlicesEqual(lhs, rhs []string) bool {
	if len(lhs) == 0 && len(rhs) == 0 {
		return true
	}
	if len(lhs) != len(rhs) {
		return false
	}
	total := make(map[string]bool, len(lhs))
	for _, item := range lhs {
		total[item] = true
	}
	for _, item := range rhs {
		if _, ok := total[item]; ok {
			delete(total, item)
			continue
		}
		total[item] = true
	}
	return len(total) == 0
}

func AnyHasPrefixInSlice(prefix string, arr []string) bool {
	for _, el := range arr {
		if strings.HasPrefix(el, prefix) {
			return true
		}
	}
	return false
}

func DiscardFirstEntries(entries []*BucketEntry, n int) []*BucketEntry {
	if n == 0 {
		return entries
	}
	if n >= len(entries) {
		return entries[:0]
	}

	toDiscard := Min(len(entries), n)

	copy(entries, entries[toDiscard:])
	for i := len(entries) - toDiscard; i < len(entries); i++ {
		entries[i] = nil
	}

	return entries[:len(entries)-toDiscard]
}
