// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
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

type (
	TemplateRange struct {
		Start      int64
		End        int64
		Step       int64
		DigitCount int
		Gap        string // characters after range (either to next range or end of string)
	}
	ParsedTemplate struct {
		Prefix string
		Ranges []TemplateRange
	}
)

var (
	ErrInvalidFmtFormat  = errors.New("input 'fmt' format is invalid should be 'prefix-%06d-suffix")
	ErrInvalidBashFormat = errors.New("input 'bash' format is invalid, should be 'prefix-{0001..0010..1}-suffix'")
	ErrInvalidAtFormat   = errors.New("input 'at' format is invalid, should be 'prefix-@00100-suffix'")

	ErrStartAfterEnd   = errors.New("'start' cannot be greater than 'end'")
	ErrNegativeStart   = errors.New("'start' is negative")
	ErrNonPositiveStep = errors.New("'step' is non positive number")
)

// Parsing:
// 1. bash-extension style: `file-{0..100}-suffix`
// 2. at-style: `file-@100-suffix`
// 3. fmt-style: `file-%06d-suffix`
// 4. if none of the above, fall back to just prefix matching
func NewParsedTemplate(template string) (ParsedTemplate, error) {
	if template == "" {
		return ParsedTemplate{}, errors.New("empty range template")
	}
	if parsed, err := ParseBashTemplate(template); err == nil {
		return parsed, nil
	}
	if parsed, err := ParseAtTemplate(template); err == nil {
		return parsed, nil
	}
	if parsed, err := ParseFmtTemplate(template); err == nil {
		return parsed, nil
	}
	return ParsedTemplate{Prefix: template}, nil
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
	var ( // nolint:prealloc // objs is preallocated farther down
		max  = math.MaxInt64
		objs []string
	)
	if len(maxLen) > 0 && maxLen[0] >= 0 {
		max = maxLen[0]
		objs = make([]string, 0, max)
	} else {
		objs = make([]string, 0, pt.Count())
	}

	getNext := pt.Iter()
	i := 0
	for objName, hasNext := getNext(); hasNext && i < max; objName, hasNext = getNext() {
		objs = append(objs, objName)
		i++
	}
	return objs
}

func (pt *ParsedTemplate) Iter() func() (string, bool) {
	rangesCount := len(pt.Ranges)
	at := make([]int64, rangesCount)

	for i, tr := range pt.Ranges {
		at[i] = tr.Start
	}

	var buf bytes.Buffer
	return func() (string, bool) {
		for i := rangesCount - 1; i >= 0; i-- {
			if at[i] > pt.Ranges[i].End {
				if i == 0 {
					return "", false
				}
				at[i] = pt.Ranges[i].Start
				at[i-1] += pt.Ranges[i-1].Step
			}
		}

		buf.Reset()
		buf.WriteString(pt.Prefix)
		for i, tr := range pt.Ranges {
			buf.WriteString(fmt.Sprintf("%0*d%s", tr.DigitCount, at[i], tr.Gap))
		}

		at[rangesCount-1] += pt.Ranges[rangesCount-1].Step
		return buf.String(), true
	}
}

func ParseFmtTemplate(template string) (pt ParsedTemplate, err error) {
	// "prefix-%06d-suffix"

	percent := strings.IndexByte(template, '%')
	if percent == -1 {
		err = ErrInvalidFmtFormat
		return
	}
	if idx := strings.IndexByte(template[percent+1:], '%'); idx != -1 {
		err = ErrInvalidFmtFormat
		return
	}

	d := strings.IndexByte(template[percent:], 'd')
	if d == -1 {
		err = ErrInvalidFmtFormat
		return
	}
	d += percent

	digitCount := 0
	if d-percent > 1 {
		s := template[percent+1 : d]
		if len(s) == 1 {
			err = ErrInvalidFmtFormat
			return
		}
		if s[0] != '0' {
			err = ErrInvalidFmtFormat
			return
		}
		i, err := strconv.ParseInt(s[1:], 10, 64)
		if err != nil {
			return pt, ErrInvalidFmtFormat
		} else if i < 0 {
			return pt, ErrInvalidFmtFormat
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

func ParseBashTemplate(template string) (pt ParsedTemplate, err error) {
	// "prefix-{00001..00010..2}-gap-{001..100..2}-suffix"

	left := strings.IndexByte(template, '{')
	if left == -1 {
		err = ErrInvalidBashFormat
		return
	}
	right := strings.LastIndexByte(template, '}')
	if right == -1 {
		err = ErrInvalidBashFormat
		return
	}
	if right < left {
		err = ErrInvalidBashFormat
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
			err = ErrInvalidBashFormat
			return
		}
		if right < left {
			err = ErrInvalidBashFormat
			return
		}
		inside := template[left+1 : right]

		numbers := strings.Split(inside, "..")
		if len(numbers) < 2 || len(numbers) > 3 {
			err = ErrInvalidBashFormat
			return
		} else if len(numbers) == 2 { // {0001..0999} case
			if tr.Start, err = strconv.ParseInt(numbers[0], 10, 64); err != nil {
				return
			}
			if tr.End, err = strconv.ParseInt(numbers[1], 10, 64); err != nil {
				return
			}
			tr.Step = 1
			tr.DigitCount = Min(len(numbers[0]), len(numbers[1]))
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
			tr.DigitCount = Min(len(numbers[0]), len(numbers[1]))
		}
		if err = validateBoundaries(tr.Start, tr.End, tr.Step); err != nil {
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

func ParseAtTemplate(template string) (pt ParsedTemplate, err error) {
	// "prefix-@00001-gap-@100-suffix"

	left := strings.IndexByte(template, '@')
	if left == -1 {
		err = ErrInvalidAtFormat
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

		if err = validateBoundaries(tr.Start, tr.End, tr.Step); err != nil {
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

func validateBoundaries(start, end, step int64) error {
	if start > end {
		return ErrStartAfterEnd
	}
	if start < 0 {
		return ErrNegativeStart
	}
	if step <= 0 {
		return ErrNonPositiveStep
	}
	return nil
}
