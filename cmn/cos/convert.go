// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"fmt"
	"strconv"

	"github.com/NVIDIA/aistore/cmn/debug"
)

func IsParseBool(s string) bool {
	yes, err := ParseBool(s)
	_ = err // error means false
	return yes
}

// ParseBool converts string to bool (case-insensitive):
// y, yes, on -> true
// n, no, off, <empty value> -> false
// strconv handles the following:
// 1, true, t -> true
// 0, false, f -> false
func ParseBool(s string) (bool, error) {
	// the two most common
	if s == "" {
		return false, nil
	}
	if s == "true" {
		return true, nil
	}
	// add. options
	switch s {
	case "y", "yes", "on", "true":
		return true, nil
	case "n", "no", "off", "false":
		return false, nil
	}
	// gen. case
	return strconv.ParseBool(s)
}

func ConvertToString(value any) (valstr string, err error) {
	switch v := value.(type) {
	case string:
		valstr = v
	case bool, int, int32, int64, uint32, uint64, float32, float64:
		valstr = fmt.Sprintf("%v", v)
	default:
		debug.FailTypeCast(value)
		err = fmt.Errorf("failed to assert type: %v(%T)", value, value)
	}
	return
}
