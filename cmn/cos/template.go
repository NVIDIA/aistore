// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"
)

const (
	WildcardMatchAll = "*"
	EmptyMatchAll    = ""
)

const (
	maxTemplateExpansion = 10_000_000 // see Expand()
)

func MatchAll(template string) bool { return template == EmptyMatchAll || template == WildcardMatchAll }

// Supported range syntax includes 3 standalone variations, 3 alternative formats:
// 1. bash (or shell) brace expansion:
//    * `prefix-{0..100}-suffix`
//    * `prefix-{00001..00010..2}-gap-{001..100..2}-suffix`
// 2. at style:
//    * `prefix-@100-suffix`
//    * `prefix-@00001-gap-@100-suffix`
// 3. fmt style:
//    * `prefix-%06d-suffix`
// In all cases, prefix and/or suffix are optional.

// NOTE: see extended comment below documenting NewParsedTemplate

type (
	TemplateRange struct {
		Gap        string // characters after the range (to the next range or end of the string)
		Start      int64
		End        int64
		Step       int64
		DigitCount int
	}
	ParsedTemplate struct {
		Prefix      string
		Ranges      []TemplateRange
		at          []int64
		buf         bytes.Buffer
		rangesCount int
	}

	errTemplateInvalid struct {
		msg string
	}
)

const (
	invalidFmt      = "invalid 'fmt' template %q (expecting e.g. 'prefix-%%06d-suffix)"
	invalidBash     = "invalid 'bash' template %q (expecting e.g. 'prefix-{0001..0010..1}-suffix')"
	invalidAt       = "invalid 'at' template %q (expecting e.g. 'prefix-@00100-suffix')"
	startAfterEnd   = "invalid '%s' template %q: 'start' cannot be greater than 'end'"
	negativeStart   = "invalid '%s' template %q: 'start' is negative"
	nonPositiveStep = "invalid '%s' template %q: 'step' is non-positive"
)

var (
	ErrEmptyTemplate = errors.New("empty range template")

	errTemplateNotBash = errors.New("not a 'bash' template")
	errTemplateNotFmt  = errors.New("not an 'fmt' template")
	errTemplateNotAt   = errors.New("not an 'at' template")
)

func newErrTemplateInvalid(efmt string, a ...any) error {
	return &errTemplateInvalid{fmt.Sprintf(efmt, a...)}
}
func (e *errTemplateInvalid) Error() string { return e.msg }

////////////////////
// ParsedTemplate //
////////////////////

// NewParsedTemplate parses a user-supplied template string and returns a
// ParsedTemplate ready for iteration or expansion.
//
// Supported syntax (see file-level comment for full grammar) includes:
//   • brace ranges       – `{0001..1000}`, `{a..z}`, `{A..Z}`
//   • ‘at’ ranges        – `@0001`, `@1`, `@A`
//   • sprintf directives – `%05d`, `%x`, etc.
//
// Special-case behavior:
//
//   • **Empty match-all** (`""` or `"*"`).
//     Returns `ErrEmptyTemplate` so the caller can decide whether that means
//     "entire bucket", "whole namespace", or an error.
//
//   • **Prefix-only** (no ranges, e.g. `"logs/2025/07/"`).
//     Returns `(pt, nil)` where `pt.Ranges == nil` and therefore
//     `pt.IsPrefixOnly() == true`.  This is *not* an error because many
//     callers legitimately want prefix search; NOTE:
//     code that *requires* at least one range should explicitly call `pt.CheckIsRange()`
//
//   • **Range templates**.
//     Valid range syntax returns `(pt, nil)` with `pt.Ranges` populated.
//
// The function does **not** expand the template.  Call `pt.Expand()` (or
// `pt.InitIter()` + `pt.Next()`) to enumerate concrete object names.  Expansion
// is capped (`maxTemplateExpansion`) to prevent runaway memory use.
//
// Example:
//     pt, err := cos.NewParsedTemplate("img_{0001..0100}.jpg")
//     if err != nil { ... }
//     names, _ := pt.Expand()   // → 100 concrete names

func NewParsedTemplate(template string) (pt ParsedTemplate, err error) {
	if MatchAll(template) {
		return pt, ErrEmptyTemplate
	}

	pt, err = ParseBashTemplate(template)
	if err == nil || err != errTemplateNotBash {
		return pt, err
	}
	pt, err = ParseAtTemplate(template)
	if err == nil || err != errTemplateNotAt {
		return pt, err
	}
	pt, err = ParseFmtTemplate(template)
	if err == nil || err != errTemplateNotFmt {
		return pt, err
	}

	// IsPrefixOnly()
	return ParsedTemplate{Prefix: TrimPrefix(template)}, nil
}

func (pt *ParsedTemplate) IsRange() bool      { return len(pt.Ranges) > 0 }
func (pt *ParsedTemplate) IsPrefixOnly() bool { return len(pt.Ranges) == 0 }

func (pt *ParsedTemplate) CheckIsRange() (err error) {
	if len(pt.Ranges) == 0 {
		err = fmt.Errorf("prefix-only template not supported (prefix: %q)", pt.Prefix)
	}
	return
}

func (pt *ParsedTemplate) Clone() *ParsedTemplate {
	if pt == nil {
		return nil
	}
	clone := *pt
	return &clone
}

func (pt *ParsedTemplate) Count() int64 {
	count := int64(1)
	for _, tr := range pt.Ranges {
		step := (tr.End-tr.Start)/tr.Step + 1
		count *= step
	}
	return count
}

func (pt *ParsedTemplate) Expand(limit ...int) ([]string, error) {
	const (
		fmterr = "parsed-template: too large to expand (%d vs %d max)"
	)
	var n int
	if len(limit) > 0 && limit[0] >= 0 {
		n = limit[0]
	} else {
		cnt := pt.Count()
		if cnt > math.MaxInt32 {
			return nil, fmt.Errorf(fmterr, cnt, maxTemplateExpansion)
		}
		n = int(cnt)
	}
	if n > maxTemplateExpansion {
		return nil, fmt.Errorf(fmterr, n, maxTemplateExpansion)
	}

	var (
		i    int
		objs = make([]string, 0, n)
	)
	pt.InitIter()
	for objName, hasNext := pt.Next(); hasNext && i < n; objName, hasNext = pt.Next() {
		objs = append(objs, objName)
		i++
	}
	return objs, nil
}

func (pt *ParsedTemplate) InitIter() {
	pt.rangesCount = len(pt.Ranges)
	pt.at = make([]int64, pt.rangesCount)
	for i, tr := range pt.Ranges {
		pt.at[i] = tr.Start
	}
}

func (pt *ParsedTemplate) Next() (string, bool) {
	pt.buf.Reset()
	for i := pt.rangesCount - 1; i >= 0; i-- {
		if pt.at[i] > pt.Ranges[i].End {
			if i == 0 {
				return "", false
			}
			pt.at[i] = pt.Ranges[i].Start
			pt.at[i-1] += pt.Ranges[i-1].Step
		}
	}
	pt.buf.WriteString(pt.Prefix)
	for i, tr := range pt.Ranges {
		pt.buf.WriteString(fmt.Sprintf("%0*d%s", tr.DigitCount, pt.at[i], tr.Gap))
	}
	pt.at[pt.rangesCount-1] += pt.Ranges[pt.rangesCount-1].Step
	return pt.buf.String(), true
}

//
// parsing --- parsing --- parsing
//

// template: "prefix-%06d-suffix"
// (both prefix and suffix are optional, here and elsewhere)
func ParseFmtTemplate(template string) (pt ParsedTemplate, _ error) {
	percent := strings.IndexByte(template, '%')
	if percent == -1 {
		return pt, errTemplateNotFmt
	}
	if idx := strings.IndexByte(template[percent+1:], '%'); idx != -1 {
		return pt, errTemplateNotFmt
	}

	d := strings.IndexByte(template[percent:], 'd')
	if d == -1 {
		return pt, newErrTemplateInvalid(invalidFmt, template)
	}
	d += percent

	digitCount := 0
	if d-percent > 1 {
		s := template[percent+1 : d]
		if len(s) == 1 {
			return pt, newErrTemplateInvalid(invalidFmt, template)
		}
		if s[0] != '0' {
			return pt, newErrTemplateInvalid(invalidFmt, template)
		}
		i, err := strconv.ParseInt(s[1:], 10, 64)
		if err != nil {
			return pt, newErrTemplateInvalid(invalidFmt, template)
		} else if i < 0 {
			return pt, newErrTemplateInvalid(invalidFmt, template)
		}
		digitCount = int(i)
	}

	return ParsedTemplate{
		Prefix: template[:percent],
		Ranges: []TemplateRange{{
			Start:      0,
			End:        math.MaxInt64 - 1,
			Step:       1,
			DigitCount: digitCount,
			Gap:        template[d+1:],
		}},
	}, nil
}

// examples
// - single-range: "prefix{0001..0010}suffix"
// - multi-range:  "prefix-{00001..00010..2}-gap-{001..100..2}-suffix"
// (both prefix and suffix are optional, here and elsewhere)
func ParseBashTemplate(template string) (pt ParsedTemplate, _ error) {
	left := strings.IndexByte(template, '{')
	if left == -1 {
		return pt, errTemplateNotBash
	}
	right := strings.LastIndexByte(template, '}')
	if right == -1 {
		return pt, errTemplateNotBash
	}
	if right < left {
		return pt, newErrTemplateInvalid(invalidBash, template)
	}
	pt.Prefix = template[:left]

	for {
		tr := TemplateRange{}

		left := strings.IndexByte(template, '{')
		if left == -1 {
			break
		}

		right := strings.IndexByte(template, '}')
		if right == -1 {
			return pt, newErrTemplateInvalid(invalidBash, template)
		}
		if right < left {
			return pt, newErrTemplateInvalid(invalidBash, template)
		}
		inside := template[left+1 : right]

		numbers := strings.Split(inside, "..")

		switch {
		case len(numbers) < 2 || len(numbers) > 3:
			return pt, newErrTemplateInvalid(invalidBash, template)
		case len(numbers) == 2: // {0001..0999} case
			var err error
			if tr.Start, err = strconv.ParseInt(numbers[0], 10, 64); err != nil {
				return pt, err
			}
			if tr.End, err = strconv.ParseInt(numbers[1], 10, 64); err != nil {
				return pt, err
			}
			tr.Step = 1
			tr.DigitCount = min(len(numbers[0]), len(numbers[1]))
		case len(numbers) == 3: // {0001..0999..2} case
			var err error
			if tr.Start, err = strconv.ParseInt(numbers[0], 10, 64); err != nil {
				return pt, err
			}
			if tr.End, err = strconv.ParseInt(numbers[1], 10, 64); err != nil {
				return pt, err
			}
			if tr.Step, err = strconv.ParseInt(numbers[2], 10, 64); err != nil {
				return pt, err
			}
			tr.DigitCount = min(len(numbers[0]), len(numbers[1]))
		}

		if err := validateBoundaries("bash", template, tr.Start, tr.End, tr.Step); err != nil {
			return pt, err
		}

		// apply gap (either to next range or end of the template)
		template = template[right+1:]
		right = strings.Index(template, "{")
		if right >= 0 {
			tr.Gap = template[:right]
		} else {
			tr.Gap = template
		}

		pt.Ranges = append(pt.Ranges, tr)
	}

	return pt, nil
}

// e.g.:
// - multi range:  "prefix-@00001-gap-@100-suffix"
// - single range: "prefix@00100suffix"
func ParseAtTemplate(template string) (pt ParsedTemplate, _ error) {
	left := strings.IndexByte(template, '@')
	if left == -1 {
		return pt, errTemplateNotAt
	}
	pt.Prefix = template[:left]

	for {
		var (
			err    error
			tr     = TemplateRange{}
			left   = strings.IndexByte(template, '@')
			number string
		)
		if left == -1 {
			break
		}
		for left++; len(template) > left && unicode.IsDigit(rune(template[left])); left++ {
			number += string(template[left])
		}

		tr.Start = 0
		if tr.End, err = strconv.ParseInt(number, 10, 64); err != nil {
			return pt, err
		}
		tr.Step = 1
		tr.DigitCount = len(number)

		if err := validateBoundaries("at", template, tr.Start, tr.End, tr.Step); err != nil {
			return pt, err
		}

		// apply gap (either to next range or end of the template)
		template = template[left:]
		right := strings.IndexByte(template, '@')
		if right >= 0 {
			tr.Gap = template[:right]
		} else {
			tr.Gap = template
		}

		pt.Ranges = append(pt.Ranges, tr)
	}

	return pt, nil
}

func validateBoundaries(typ, template string, start, end, step int64) error {
	if start > end {
		return newErrTemplateInvalid(startAfterEnd, typ, template)
	}
	if start < 0 {
		return newErrTemplateInvalid(negativeStart, typ, template)
	}
	if step <= 0 {
		return newErrTemplateInvalid(nonPositiveStep, typ, template)
	}
	return nil
}
