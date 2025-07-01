// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import "strings"

const _dfltLen = 16

func BHead(b []byte, ls ...int) string {
	l := _dfltLen
	if len(ls) > 0 {
		l = ls[0]
	}
	if len(b) > l {
		return string(b[:l]) + "..."
	}
	return string(b)
}

func SHead(s string) string {
	if len(s) > _dfltLen {
		return s[:_dfltLen] + "..."
	}
	return s
}

func IsLastB(s string, b byte) bool {
	l := len(s)
	return l > 0 && s[l-1] == b
}

func TrimLastB(s string, b byte) string {
	if l := len(s); s[l-1] == b {
		return s[:l-1]
	}
	return s
}

// [NOTE] common *nix expectation in re: `ls aaa/bbb*` and similar
// - `?` not supported
// - `\*` not supported
// see also: cmn.ObjHasPrefix and friends
func TrimPrefix(s string) string {
	if l := len(s); l > 0 && s[l-1] == WildcardMatchAll[0] {
		return s[:l-1]
	}
	return s
}

// left if non-empty; otherwise right
func Left(left, right string) string {
	if left != "" {
		return left
	}
	return right
}

// right if non-empty; otherwise left
func Right(left, right string) string {
	if right != "" {
		return right
	}
	return left
}

// (common use)
func Plural(num int) (s string) {
	if num != 1 {
		s = "s"
	}
	return
}

// optimal append(strings)
func AppendStrings(sb *strings.Builder /*in-out*/, prefix string, sepa byte, items ...string) {
	l := len(prefix)
	l += len(items) - 1 // times sepa
	for _, s := range items {
		l += len(s)
	}
	sb.Grow(l)

	sb.WriteString(prefix)
	sb.WriteString(items[0])
	for _, s := range items[1:] {
		sb.WriteByte(sepa)
		sb.WriteString(s)
	}
}

func StrDup(s string) string { return string([]byte(s)) }
