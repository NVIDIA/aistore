// Package feat
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package feat

import "strconv"

type Flags uint64

const (
	EnforceIntraClusterAccess Flags = 1 << iota
)

var all = []struct {
	name  string
	value Flags
}{
	{name: "EnforceIntraClusterAccess", value: EnforceIntraClusterAccess},
}

func (cflags Flags) IsSet(flag Flags) bool { return cflags&flag == flag }
func (cflags Flags) Value() string         { return strconv.FormatUint(uint64(cflags), 10) }
func (cflags Flags) String() string {
	var s string
	for _, flag := range all {
		if cflags&flag.value != flag.value {
			continue
		}
		if s != "" {
			s += ","
		}
		s += flag.name
	}
	return s
}
