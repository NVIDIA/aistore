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

// NOTE:
// - `Bucket` features(*) are a strict subset of all `Cluster` features, and can be changed
//    for individual buckets
// - when making any changes, make sure to update `Cluster` and maybe the `Bucket` enum as well (NOTE)

const (
	PropName = "features"
)

const (
	EnforceIntraClusterAccess = Flags(1 << iota)
	SkipVC                    // (*) skip loading existing object's metadata, Version and Checksum (VC) in particular
	DontAutoDetectFshare      // do not auto-detect file share (NFS, SMB) when _promoting_ shared files to AIS
	S3APIviaRoot              // handle s3 requests via `aistore-hostname/` (default: `aistore-hostname/s3`)
	FsyncPUT                  // (*) when finalizing PUT(object): fflush prior to (close, rename) sequence
	LZ4Block1MB               // .tar.lz4 format, lz4 compression: max uncompressed block size=1MB (default: 256K)
	LZ4FrameChecksum          // checksum lz4 frames (default: don't)
	DontAllowPassingFQNtoETL  // do not allow passing fully-qualified name of a locally stored object to (local) ETL containers
	IgnoreLimitedCoexistence  // run in presence of "limited coexistence" type conflicts (same as e.g. CopyBckMsg.Force but globally)
	S3PresignedRequest        // (*) pass-through client-signed (presigned) S3 requests for subsequent authentication by S3
	DontOptimizeVirtualDir    // when prefix doesn't end with '/' and is a subdirectory: don't assume there are no _prefixed_ obj names
	DisableColdGET            // disable cold-GET (from remote bucket)
	StreamingColdGET          // write and transmit cold-GET content back to user in parallel, without _finalizing_ in-cluster object
	S3ReverseProxy            // intra-cluster communications: instead of regular HTTP redirects reverse-proxy S3 API calls to designated targets
	S3UsePathStyle            // use older path-style addressing (as opposed to virtual-hosted style), e.g., https://s3.amazonaws.com/BUCKET/KEY
	DontDeleteWhenRebalancing // when objects get _rebalanced_ to their proper locations, do not delete their respective _misplaced_ sources
	DontSetControlPlaneToS    // intra-cluster control plane: do not set IPv4 ToS field (to low-latency)
	TrustCryptoSafeChecksums  // when checking whether objects are identical trust only cryptographically secure checksums
)

var Cluster = [...]string{
	"Enforce-IntraCluster-Access",
	"Skip-Loading-VersionChecksum-MD",
	"Do-not-Auto-Detect-FileShare",
	"S3-API-via-Root",
	"Fsync-PUT",
	"LZ4-Block-1MB",
	"LZ4-Frame-Checksum",
	"Do-not-Allow-Passing-FQN-to-ETL",
	"Ignore-LimitedCoexistence-Conflicts",
	"S3-Presigned-Request",
	"Do-not-Optimize-Listing-Virtual-Dirs",
	"Disable-Cold-GET",
	"Streaming-Cold-GET",
	"S3-Reverse-Proxy",
	"S3-Use-Path-Style", // https://aws.amazon.com/blogs/aws/amazon-s3-path-deprecation-plan-the-rest-of-the-story
	"Do-not-Delete-When-Rebalancing",
	"Do-not-Set-Control-Plane-ToS",
	"Trust-Crypto-Safe-Checksums",

	// "none" ====================
}

var Bucket = [...]string{
	"Skip-Loading-VersionChecksum-MD",
	"Fsync-PUT",
	"S3-Presigned-Request",
	"Disable-Cold-GET",
	"Streaming-Cold-GET",
	"S3-Use-Path-Style", // https://aws.amazon.com/blogs/aws/amazon-s3-path-deprecation-plan-the-rest-of-the-story

	// "none" ====================
}

func (f Flags) IsSet(flag Flags) bool { return cos.BitFlags(f).IsSet(cos.BitFlags(flag)) }
func (f Flags) Set(flags Flags) Flags { return Flags(cos.BitFlags(f).Set(cos.BitFlags(flags))) }
func (f Flags) String() string        { return strconv.FormatUint(uint64(f), 10) }

func IsBucketScope(name string) bool {
	for i := range Bucket {
		if name == Bucket[i] {
			return true
		}
	}
	return false
}

func CSV2Feat(s string) (Flags, error) {
	if s == "" || s == "none" {
		return 0, nil
	}
	for i, name := range Cluster {
		if s == name {
			return 1 << i, nil
		}
	}
	return 0, errors.New("unknown feature flag '" + s + "'")
}

func (f Flags) Names() (names []string) {
	if f == 0 {
		return names
	}
	for i, name := range Cluster {
		if f&(1<<i) != 0 {
			names = append(names, name)
		}
	}
	return names
}

func (f Flags) ClearName(n string) Flags {
	for i, name := range Cluster {
		if name == n {
			of := Flags(1 << i)
			return f &^ of
		}
	}
	return f
}
