// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"fmt"
	"strconv"
	"strings"
)

func I2S(i int64) string {
	return strconv.FormatInt(i, 10)
}

func StringSliceToIntSlice(strs []string) ([]int64, error) {
	res := make([]int64, 0, len(strs))
	for _, s := range strs {
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, err
		}
		res = append(res, i)
	}
	return res, nil
}

func StrToSentence(str string) string {
	if str == "" {
		return ""
	}

	capitalized := CapitalizeString(str)

	if !strings.HasSuffix(capitalized, ".") {
		capitalized += "."
	}

	return capitalized
}

func ConvertToString(value interface{}) (valstr string, err error) {
	switch v := value.(type) {
	case string:
		valstr = v
	case bool, int, int32, int64, uint32, uint64, float32, float64:
		valstr = fmt.Sprintf("%v", v)
	default:
		err = fmt.Errorf("failed to assert type on param: %v (type %T)", value, value)
	}
	return
}
