// Package feat: global runtime-configurable feature flags
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package feat

import (
	"errors"
	"strconv"

	"github.com/NVIDIA/aistore/cmn/cos"
)

type Flags cos.BitFlags

const FeaturesPropName = "features"

// NOTE: when making changes, make sure to update `All` names as well

const (
	EnforceIntraClusterAccess = Flags(1 << iota)
	SkipVC                    // skip loading existing object's metadata, Version and Checksum (VC) in particular
	DontAutoDetectFshare      // do not auto-detect file share (NFS, SMB) when _promoting_ shared files to AIS
	ProvideS3APIviaRoot       // handle s3 requests via `aistore-hostname/` (default: `aistore-hostname/s3`)
	FsyncPUT                  // when finalizing PUT(obj) fflush prior to (close, rename) sequence
	LZ4Block1MB               // .tar.lz4 format, lz4 compression: max uncompressed block size=1MB (default: 256K)
	LZ4FrameChecksum          // checksum lz4 frames (default: don't)
	DontAllowPassingFQNtoETL  // do not allow passing fully-qualified name of a locally stored object to (local) ETL containers
	IgnoreLimitedCoexistence  // run in presence of "limited coexistence" type conflicts (same as e.g. CopyBckMsg.Force but globally)
	DisableFastColdGET        // use regular datapath to execute cold-GET operations
)

var All = []string{
	"Enforce-IntraCluster-Access",
	"Skip-Loading-VersionChecksum-MD",
	"Do-not-Auto-Detect-FileShare",
	"Provide-S3-API-via-Root",
	"Fsync-PUT",
	"LZ4-Block-1MB",
	"LZ4-Frame-Checksum",
	"Dont-Allow-Passing-FQN-to-ETL",
	"Ignore-LimitedCoexistence-Conflicts",
	"Disable-Fast-Cold-GET",
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
