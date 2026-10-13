// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"slices"
	"strings"
	"unicode"

	"github.com/NVIDIA/aistore/cmn"

	jsoniter "github.com/json-iterator/go"
)

// usage footnote for name/value commands and flags
const (
	nvSyntaxRef = "Name/value syntax (forms, quoting, escapes, lists, JSON): " +
		cmn.GitHubHome + "/blob/main/docs/cli.md#namevalue-arguments"
	nvUsageRef = indent1 + nvSyntaxRef
)

// split an aggregate flag argument at unescaped whitespace; retain escapes and complete JSON values
func splitNVArgs(s string) (args []string) {
	start, escaped := -1, false
	var jsonEnd int
	for i, r := range s {
		if i < jsonEnd {
			continue
		}
		if escaped {
			escaped = false
			continue
		}
		if unicode.IsSpace(r) {
			if start >= 0 {
				args = append(args, s[start:i])
				start = -1
			}
			continue
		}
		if start < 0 {
			start = i
		}
		if r == '{' || r == '[' {
			prefix := s[start:i]
			pos := nvEqual(prefix)
			if prefix == "" || (pos >= 0 && strings.TrimSpace(prefix[pos+1:]) == "") {
				jsonEnd = i + nvJSONLen(s[i:])
			}
		}
		escaped = r == '\\'
	}
	if start >= 0 {
		args = append(args, s[start:])
	}
	return args
}

// return the length of a complete JSON object or array prefix, or zero
func nvJSONLen(s string) int {
	var (
		depth           int
		quoted, escaped bool
	)
	for i, r := range s {
		if quoted {
			switch {
			case escaped:
				escaped = false
			case r == '\\':
				escaped = true
			case r == '"':
				quoted = false
			}
			continue
		}
		switch r {
		case '"':
			quoted = true
		case '{', '[':
			depth++
		case '}', ']':
			depth--
			if depth == 0 {
				if nvJSON(s[:i+1]) {
					return i + 1
				}
				return 0
			}
		}
	}
	return 0
}

// name/value parsing options (all optional)
type nvOpts struct {
	// accept val as another member of name's list
	listValue func(name, val string) bool
	// validate and optionally normalize each name before examining later pairs or list members
	validateName func(name string) (string, error)
	// known names: a known name (or its assignment) in a value position means the previous name has no value
	names []string
}

func (o *nvOpts) isName(s string) bool {
	if o == nil || len(o.names) == 0 {
		return false
	}
	if pos := nvEqual(s); pos >= 0 {
		s = s[:pos]
	}
	return slices.Contains(o.names, strings.ToLower(strings.TrimSpace(s)))
}

// parse ordered name/value pairs without splitting quoted arguments or comma-separated values
// - accept name=value, name value, and whitespace around '='
// - preserve empty values; an empty right-hand side followed by another assignment stays empty
// - optionally collect command-specific list values; retain duplicates for the caller to handle
// - with known names: reject a missing value and an invalid list member instead of misaligning pairs
func parseNVPairs(args []string, opts *nvOpts) (pairs nvpairList, err error) {
	pairs = make(nvpairList, 0, len(args))
	for i := 0; i < len(args); {
		var pair nvpair
		pair, i, err = parseNVPair(args, i, opts)
		if err != nil {
			return pairs, err
		}
		if opts != nil && opts.validateName != nil {
			if pair.Name, err = opts.validateName(pair.Name); err != nil {
				return pairs, err
			}
		}
		if opts != nil && opts.listValue != nil && opts.listValue(pair.Name, pair.Value) {
			for i < len(args) && nvEqual(args[i]) < 0 {
				val := unescapeNV(args[i], true)
				if !opts.listValue(pair.Name, val) {
					if len(opts.names) > 0 && !opts.isName(args[i]) {
						return pairs, fmt.Errorf("invalid %s value %q", pair.Name, val)
					}
					break
				}
				pair.Value += "," + val
				i++
			}
		}
		pairs = append(pairs, pair)
	}
	return pairs, nil
}

// read one assignment; shell argument boundaries already protect quoted spaces in names and values
func parseNVPair(args []string, i int, opts *nvOpts) (pair nvpair, next int, err error) {
	arg := args[i]
	next = i + 1
	pos := nvEqual(arg)
	if pos >= 0 {
		pair.Name = unescapeNV(arg[:pos], false)
		pair.Value = unescapeNV(arg[pos+1:], true)
		if pair.Value == "" && next < len(args) && (nvEqual(args[next]) < 0 || nvJSON(args[next])) &&
			!opts.isName(args[next]) {
			pair.Value = unescapeNV(args[next], true)
			next++
		}
	} else {
		pair.Name = unescapeNV(arg, false)
		if next == len(args) {
			return pair, next, &errInvalidNVpair{notpair: pair.Name}
		}
		if opts.isName(args[next]) {
			return pair, next, &errInvalidNVpair{notpair: pair.Name, missing: true}
		}
		val := strings.TrimLeftFunc(args[next], unicode.IsSpace)
		next++
		if strings.HasPrefix(val, "=") {
			val = strings.TrimLeftFunc(val[1:], unicode.IsSpace)
			if val == "" && next < len(args) && !opts.isName(args[next]) {
				val = args[next]
				next++
			}
		}
		pair.Value = unescapeNV(val, true)
	}
	if pair.Name == "" {
		return pair, next, &errInvalidNVpair{notpair: arg, noName: true}
	}
	return pair, next, nil
}

// return the first unescaped '='; skip '\=' and '\\'
func nvEqual(s string) int {
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) && (s[i+1] == '=' || s[i+1] == '\\') {
			i++
			continue
		}
		if s[i] == '=' {
			return i
		}
	}
	return -1
}

// trim unescaped boundary whitespace and decode separator escapes; leave JSON payloads intact
func unescapeNV(s string, value bool) string {
	trimmed := strings.TrimSpace(s)
	if !strings.ContainsRune(s, '\\') {
		return trimmed
	}
	if value && nvJSON(trimmed) {
		return trimmed
	}
	var (
		b    strings.Builder
		last int
	)
	runes := []rune(s)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		escaped := false
		if r == '\\' && i+1 < len(runes) {
			next := runes[i+1]
			if next == '=' || next == '\\' || unicode.IsSpace(next) {
				r, escaped = next, true
				i++
			}
		}
		if b.Len() == 0 && !escaped && unicode.IsSpace(r) {
			continue
		}
		b.WriteRune(r)
		if escaped || !unicode.IsSpace(r) {
			last = b.Len()
		}
	}
	return b.String()[:last]
}

// recognize opaque JSON values whose quoting and escapes belong to the JSON decoder
func nvJSON(s string) bool {
	s = strings.TrimSpace(s)
	return len(s) > 1 && (s[0] == '{' || s[0] == '[') && jsoniter.Valid([]byte(s))
}
