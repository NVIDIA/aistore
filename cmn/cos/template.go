// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
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

func MatchAll(template string) bool { return template == EmptyMatchAll || template == WildcardMatchAll }

// Supported syntax includes 3 standalone variations, 3 alternative formats:
// 1. bash (or shell) brace expansion:
//    * `prefix-{0..100}-suffix`
//    * `prefix-{00001..00010..2}-gap-{001..100..2}-suffix`
// 2. at style:
//    * `prefix-@100-suffix`
//    * `prefix-@00001-gap-@100-suffix`
// 3. fmt style:
//    * `prefix-%06d-suffix`
// In all cases, prefix and/or suffix are optional.
//
// NOTE: if none of the above applies, `NewParsedTemplate()` simply returns
//       `ParsedTemplate{Prefix = original template string}` with nil Ranges

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

func NewParsedTemplate(template string) (parsed ParsedTemplate, err error) {
	if MatchAll(template) {
		err = ErrEmptyTemplate
		return
	}

	parsed, err = ParseBashTemplate(template)
	if err == nil || err != errTemplateNotBash {
		return
	}
	parsed, err = ParseAtTemplate(template)
	if err == nil || err != errTemplateNotAt {
		return
	}
	parsed, err = ParseFmtTemplate(template)
	if err == nil || err != errTemplateNotFmt {
		return
	}

	// "pure" prefix w/ no ranges
	return ParsedTemplate{Prefix: TrimPrefix(template)}, nil
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

// maxLen specifies maximum objects to be returned
func (pt *ParsedTemplate) ToSlice(maxLen ...int) []string {
	var i, n int
	if len(maxLen) > 0 && maxLen[0] >= 0 {
		n = maxLen[0]
	} else {
		n = int(pt.Count())
	}
	objs := make([]string, 0, n)
	pt.InitIter()
	for objName, hasNext := pt.Next(); hasNext && i < n; objName, hasNext = pt.Next() {
		objs = append(objs, objName)
		i++
	}
	return objs
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
func ParseFmtTemplate(template string) (pt ParsedTemplate, err error) {
	percent := strings.IndexByte(template, '%')
	if percent == -1 {
		err = errTemplateNotFmt
		return
	}
	if idx := strings.IndexByte(template[percent+1:], '%'); idx != -1 {
		err = errTemplateNotFmt
		return
	}

	d := strings.IndexByte(template[percent:], 'd')
	if d == -1 {
		err = newErrTemplateInvalid(invalidFmt, template)
		return
	}
	d += percent

	digitCount := 0
	if d-percent > 1 {
		s := template[percent+1 : d]
		if len(s) == 1 {
			err = newErrTemplateInvalid(invalidFmt, template)
			return
		}
		if s[0] != '0' {
			err = newErrTemplateInvalid(invalidFmt, template)
			return
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
func ParseBashTemplate(template string) (pt ParsedTemplate, err error) {
	left := strings.IndexByte(template, '{')
	if left == -1 {
		err = errTemplateNotBash
		return
	}
	right := strings.LastIndexByte(template, '}')
	if right == -1 {
		err = errTemplateNotBash
		return
	}
	if right < left {
		err = newErrTemplateInvalid(invalidBash, template)
		return
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
			err = newErrTemplateInvalid(invalidBash, template)
			return
		}
		if right < left {
			err = newErrTemplateInvalid(invalidBash, template)
			return
		}
		inside := template[left+1 : right]

		numbers := strings.Split(inside, "..")
		if len(numbers) < 2 || len(numbers) > 3 {
			err = newErrTemplateInvalid(invalidBash, template)
			return
		} else if len(numbers) == 2 { // {0001..0999} case
			if tr.Start, err = strconv.ParseInt(numbers[0], 10, 64); err != nil {
				return
			}
			if tr.End, err = strconv.ParseInt(numbers[1], 10, 64); err != nil {
				return
			}
			tr.Step = 1
			tr.DigitCount = min(len(numbers[0]), len(numbers[1]))
		} else if len(numbers) == 3 { // {0001..0999..2} case
			if tr.Start, err = strconv.ParseInt(numbers[0], 10, 64); err != nil {
				return
			}
			if tr.End, err = strconv.ParseInt(numbers[1], 10, 64); err != nil {
				return
			}
			if tr.Step, err = strconv.ParseInt(numbers[2], 10, 64); err != nil {
				return
			}
			tr.DigitCount = min(len(numbers[0]), len(numbers[1]))
		}
		if err = validateBoundaries("bash", template, tr.Start, tr.End, tr.Step); err != nil {
			return
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
	return
}

// e.g.:
// - multi range:  "prefix-@00001-gap-@100-suffix"
// - single range: "prefix@00100suffix"
func ParseAtTemplate(template string) (pt ParsedTemplate, err error) {
	left := strings.IndexByte(template, '@')
	if left == -1 {
		err = errTemplateNotAt
		return
	}
	pt.Prefix = template[:left]

	for {
		tr := TemplateRange{}

		left := strings.IndexByte(template, '@')
		if left == -1 {
			break
		}

		number := ""
		for left++; len(template) > left && unicode.IsDigit(rune(template[left])); left++ {
			number += string(template[left])
		}

		tr.Start = 0
		if tr.End, err = strconv.ParseInt(number, 10, 64); err != nil {
			return
		}
		tr.Step = 1
		tr.DigitCount = len(number)

		if err = validateBoundaries("at", template, tr.Start, tr.End, tr.Step); err != nil {
			return
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
	return
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
