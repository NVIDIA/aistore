// Package feat: global runtime-configurable feature flags
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package feat

import (
	"errors"
	"strconv"

	"github.com/NVIDIA/aistore/cmn/cos"
)

type Flags cos.BitFlags

const FeaturesPropName = "features"

// NOTE: when/if making any changes make absolutely sure to keep the enum below and `All` in-sync :NOTE

const (
	EnforceIntraClusterAccess = Flags(1 << iota)
	DontHeadRemote            // see also api/apc/lsmsg.go, and in particular `LsDontHeadRemote`
	SkipVC                    // skip loading existing object's metadata, Version and Checksum in particular
	DontAutoDetectFshare      // when promoting NFS shares to AIS
	ProvideS3APIViaRoot       // handle s3 compat via `aistore-hostname/` (default: `aistore-hostname/s3`)
	FsyncPUT                  // when finalizing PUT(obj) fflush prior to (close, rename) sequence
)

var All = []string{
	"Enforce-IntraCluster-Access",
	"Do-not-HEAD-Remote-Bucket",
	"Skip-Loading-VersionChecksum-MD",
	"Do-not-Auto-Detect-FileShare",
	"Provide-S3-API-via-Root",
	"Fsync-PUT",
}

func (f Flags) IsSet(flag Flags) bool { return cos.BitFlags(f).IsSet(cos.BitFlags(flag)) }
func (f Flags) Set(flags Flags) Flags { return Flags(cos.BitFlags(f).Set(cos.BitFlags(flags))) }
func (f Flags) Value() string         { return strconv.FormatUint(uint64(f), 10) }

func StrToFeat(s string) (Flags, error) {
	if s == "" || s == "none" {
		return 0, nil
	}
	for i, name := range All {
		if s == name {
			return 1 << i, nil
		}
	}
	return 0, errors.New("unknown feature flag '" + s + "'")
}

func (f Flags) String() (s string) {
	for i, name := range All {
		if f&(1<<i) != 0 {
			s += name + ","
		}
	}
	if s == "" {
		return "none"
	}
	return s[:len(s)-1]
}
