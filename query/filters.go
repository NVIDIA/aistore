// Package query provides interface to iterate over objects with additional filtering
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package query

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
)

type (
	filterMeta struct {
		argsCnt  int
		argsType int
	}
)

const (
	stringArg = iota
	intArg

	AtimeBeforeF = "atime_before"
	AtimeAfterF  = "atime_after"
	AtimeF       = "atime"

	SizeF   = "size"
	SizeLeF = "size_le"
	SizeGeF = "size_ge"

	VersionF   = "version"
	VersionLeF = "version_le"
	VersionGeF = "version_ge"

	ExtF = "ext"
)

var functionMeta = map[string]filterMeta{
	AtimeF:       {2, intArg},
	AtimeBeforeF: {1, intArg},
	AtimeAfterF:  {1, intArg},

	SizeF:   {2, intArg},
	SizeLeF: {1, intArg},
	SizeGeF: {1, intArg},

	VersionF:   {2, intArg},
	VersionLeF: {1, intArg},
	VersionGeF: {1, intArg},

	ExtF: {1, stringArg},
}

func NewFilter(fname string, args []string) *FilterMsg {
	return &FilterMsg{
		Type:  FUNCTION,
		FName: fname,
		Args:  args,
	}
}

func NewOrFilter(filters ...*FilterMsg) *FilterMsg {
	return &FilterMsg{
		Type:    OR,
		Filters: filters,
	}
}

func NewAndFilter(filters ...*FilterMsg) *FilterMsg {
	return &FilterMsg{
		Type:    AND,
		Filters: filters,
	}
}

func ObjFilterFromMsg(filter *FilterMsg) (cluster.ObjectFilter, error) {
	if filter == nil {
		return nil, nil
	}
	switch filter.Type {
	case AND, OR:
		if len(filter.Filters) < 2 {
			return nil, fmt.Errorf("expected %s filter to have at least 2 inner filters, got %d", filter.Type, len(filter.Filters))
		}

		filters, err := extractObjectFilters(filter)
		if err != nil {
			return nil, err
		}
		if filter.Type == AND {
			return And(filters...), nil
		}
		return Or(filters...), nil
	case FUNCTION:
		return functionFilterMsgToObjectFilter(filter)
	default:
		return nil, fmt.Errorf("unknown type %s", filter.Type)
	}
}

func extractObjectFilters(filter *FilterMsg) ([]cluster.ObjectFilter, error) {
	filters := make([]cluster.ObjectFilter, 0, len(filter.Filters))
	for _, msgFilter := range filter.Filters {
		f, err := ObjFilterFromMsg(msgFilter)
		if err != nil {
			return nil, err
		}
		filters = append(filters, f)
	}
	return filters, nil
}

func functionFilterMsgToObjectFilter(filterMsg *FilterMsg) (cluster.ObjectFilter, error) {
	cos.Assert(filterMsg.Type == FUNCTION)
	var (
		err error
		v   []int64
		ok  bool
	)

	fMeta, ok := functionMeta[filterMsg.FName]
	if !ok {
		return nil, fmt.Errorf("unknown function name %s", filterMsg.FName)
	}
	if len(filterMsg.Args) != fMeta.argsCnt {
		return nil, fmt.Errorf("expected %d arguments, got %d", functionMeta[filterMsg.FName], len(filterMsg.Args))
	}

	if fMeta.argsType == stringArg {
		switch filterMsg.FName {
		case ExtF:
			return ExtFilter(filterMsg.Args[0]), nil
		default:
			cos.Assert(false)
			return nil, nil
		}
	}
	cos.Assert(fMeta.argsType == intArg)
	v, err = cos.StringSliceToIntSlice(filterMsg.Args)
	if err != nil {
		return nil, fmt.Errorf("%s failed: %v", filterMsg.FName, err)
	}

	switch filterMsg.FName {
	case AtimeF:
		return ATimeFilter(time.Unix(0, v[0]), time.Unix(0, v[1])), nil
	case AtimeAfterF:
		return ATimeAfterFilter(time.Unix(0, v[0])), nil
	case AtimeBeforeF:
		return ATimeBeforeFilter(time.Unix(0, v[0])), nil
	case SizeF:
		return SizeFilter(v[0], v[1]), nil
	case SizeLeF:
		return SizeLEFilter(v[0]), nil
	case SizeGeF:
		return SizeGEFilter(v[0]), nil
	case VersionF:
		return VersionFilter(int(v[0]), int(v[1])), nil
	case VersionLeF:
		return VersionLEFilter(int(v[0])), nil
	case VersionGeF:
		return VersionGEFilter(int(v[0])), nil
	default:
		cos.Assert(false)
		return nil, nil
	}
}

func ATimeFilter(after, before time.Time) cluster.ObjectFilter {
	return func(lom *cluster.LOM) bool {
		return lom.Atime().After(after) && lom.Atime().Before(before)
	}
}

func ATimeFilterMsg(after, before time.Time) *FilterMsg {
	return &FilterMsg{
		Type:  FUNCTION,
		FName: AtimeF,
		Args:  []string{cos.I2S(after.UnixNano()), cos.I2S(before.UnixNano())},
	}
}

func ATimeAfterFilter(time time.Time) cluster.ObjectFilter {
	return func(lom *cluster.LOM) bool {
		return lom.Atime().After(time)
	}
}

func ATimeAfterFilterMsg(after time.Time) *FilterMsg {
	return &FilterMsg{
		Type:  FUNCTION,
		FName: AtimeAfterF,
		Args:  []string{cos.I2S(after.UnixNano())},
	}
}

func ATimeBeforeFilter(time time.Time) cluster.ObjectFilter {
	return func(lom *cluster.LOM) bool {
		return lom.Atime().Before(time)
	}
}

func ATimeBeforeFilterMsg(before time.Time) *FilterMsg {
	return &FilterMsg{
		Type:  FUNCTION,
		FName: AtimeBeforeF,
		Args:  []string{cos.I2S(before.UnixNano())},
	}
}

func SizeFilter(min, max int64) cluster.ObjectFilter {
	return func(lom *cluster.LOM) bool {
		return lom.SizeBytes() >= min && lom.SizeBytes() <= max
	}
}

func SizeFilterMsg(min, max int64) *FilterMsg {
	return &FilterMsg{
		Type:  FUNCTION,
		FName: SizeF,
		Args:  []string{cos.I2S(min), cos.I2S(max)},
	}
}

func SizeLEFilter(n int64) cluster.ObjectFilter {
	return SizeFilter(0, n)
}

func SizeLEFilterMsg(n int64) *FilterMsg {
	return &FilterMsg{
		Type:  FUNCTION,
		FName: SizeLeF,
		Args:  []string{cos.I2S(n)},
	}
}

func SizeGEFilter(n int64) cluster.ObjectFilter {
	return SizeFilter(n, math.MaxInt64)
}

func SizeGEFilterMsg(n int64) *FilterMsg {
	return &FilterMsg{
		Type:  FUNCTION,
		FName: SizeGeF,
		Args:  []string{cos.I2S(n)},
	}
}

func VersionFilter(min, max int) cluster.ObjectFilter {
	return func(lom *cluster.LOM) bool {
		intVersion, err := strconv.Atoi(lom.Version())
		cos.AssertNoErr(err)
		return intVersion >= min && intVersion <= max
	}
}

func VersionFilterMsg(min, max int64) *FilterMsg {
	return &FilterMsg{
		Type:  FUNCTION,
		FName: VersionF,
		Args:  []string{cos.I2S(min), cos.I2S(max)},
	}
}

func VersionLEFilter(n int) cluster.ObjectFilter {
	return VersionFilter(0, n)
}

func VersionLEFilterMsg(n int64) *FilterMsg {
	return &FilterMsg{
		Type:  FUNCTION,
		FName: VersionLeF,
		Args:  []string{cos.I2S(n)},
	}
}

func VersionGEFilter(n int) cluster.ObjectFilter {
	return VersionFilter(n, math.MaxInt32)
}

func VersionGEFilterMsg(n int64) *FilterMsg {
	return &FilterMsg{
		Type:  FUNCTION,
		FName: VersionGeF,
		Args:  []string{cos.I2S(n)},
	}
}

func ExtFilter(ext string) cluster.ObjectFilter {
	return func(lom *cluster.LOM) bool {
		ext = strings.TrimPrefix(ext, ".")
		return strings.HasSuffix(lom.ObjName, "."+ext)
	}
}

func And(filters ...cluster.ObjectFilter) cluster.ObjectFilter {
	return func(lom *cluster.LOM) bool {
		for _, f := range filters {
			if !f(lom) {
				return false
			}
		}
		return true
	}
}

func Or(filters ...cluster.ObjectFilter) cluster.ObjectFilter {
	return func(lom *cluster.LOM) bool {
		for _, f := range filters {
			if f(lom) {
				return true
			}
		}
		return false
	}
}
