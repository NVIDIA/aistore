// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

const (
	QuantityPercent = "percent"
	QuantityBytes   = "bytes"
)

type (
	ParsedQuantity struct {
		Type  string
		Value uint64
	}
)

var (
	ErrQuantityUsage   = errors.New("invalid quantity, format should be '81%' or '1GB'")
	ErrQuantityPercent = errors.New("percent must be in the range (0, 100)")
	ErrQuantityBytes   = errors.New("value (bytes) must be non-negative")

	errQuantityNonNegative = errors.New("quantity should not be negative")
)

///////////////////
// ParseQuantity //
///////////////////

func ParseQuantity(quantity string) (ParsedQuantity, error) {
	var (
		idx     int
		number  string
		parsedQ ParsedQuantity
		sb      SB
	)
	quantity = strings.ReplaceAll(quantity, " ", "")
	sb.Init(len(quantity)) // at most all digits
	for ; idx < len(quantity) && unicode.IsDigit(rune(quantity[idx])); idx++ {
		sb.WriteUint8(quantity[idx])
	}
	number = sb.String()

	value, err := strconv.Atoi(number)
	if err != nil {
		return parsedQ, ErrQuantityUsage
	}
	if value < 0 {
		return parsedQ, errQuantityNonNegative
	}

	parsedQ.Value = uint64(value)
	if len(quantity) <= idx {
		return parsedQ, ErrQuantityUsage
	}

	suffix := quantity[idx:]
	if suffix == "%" {
		parsedQ.Type = QuantityPercent
		if parsedQ.Value == 0 || parsedQ.Value >= 100 {
			return parsedQ, ErrQuantityPercent
		}
	} else if value, err := ParseSize(quantity, UnitsIEC); err != nil {
		return parsedQ, err
	} else if value < 0 {
		return parsedQ, ErrQuantityBytes
	} else {
		parsedQ.Type = QuantityBytes
		parsedQ.Value = uint64(value)
	}

	return parsedQ, nil
}

func (pq ParsedQuantity) String() string {
	switch pq.Type {
	case QuantityPercent:
		return fmt.Sprintf("%d%%", pq.Value)
	case QuantityBytes:
		return ToSizeIEC(int64(pq.Value), 2)
	default:
		AssertMsg(false, "Unknown quantity type: "+pq.Type)
		return ""
	}
}
