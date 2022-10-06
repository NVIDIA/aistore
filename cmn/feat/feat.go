// Package feat: global runtime-configurable feature flags
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package feat

import (
	"errors"
	"strconv"
)

type Flags uint64

// NOTE: when/if making any changes make absolutely sure to keep the enum below and `All` in-sync :NOTE

const (
	EnforceIntraClusterAccess Flags = 1 << iota
	DontHeadRemote                  // see also api/apc/lsmsg.go, and in particular `LsDontHeadRemote`
	SkipVC                          // skip loading existing object's metadata, Version and Checksum in particular
	DontAutoDetectFshare            // when promoting NFS shares to AIS
	ProvideS3APIViaRoot             // handle s3 compat via `aistore-hostname/` (default: `aistore-hostname/s3`)
)

var All = []string{
	"EnforceIntraClusterAccess",
	"DontHeadRemote",
	"SkipVC",
	"DontAutoDetectFshare",
	"ProvideS3APIViaRoot",
}

func (featfl Flags) IsSet(flag Flags) bool { return featfl&flag == flag }
func (featfl Flags) Value() string         { return strconv.FormatUint(uint64(featfl), 10) }

func StrToFeat(s string) (Flags, error) {
	for i, name := range All {
		if s == name {
			return 1 << i, nil
		}
	}
	return 0, errors.New("unknown feature flag '" + s + "'")
}

func (featfl Flags) String() (s string) {
	for i, name := range All {
		if featfl&(1<<i) != 0 {
			s += name + ","
		}
	}
	if s == "" {
		return "none"
	}
	return s[:len(s)-1]
}
