// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn/feat"

	"github.com/urfave/cli"
)

// NOTE:
// - keep descriptions and tags (both!) aligned with cmn/feat/feat (server-side) source
// - bucket-scope features are a strict subset of all cluster features, can be changed on a per-bucket basis

const (
	featureFlagsJname = "features"

	clusterFeatures = "cluster." + featureFlagsJname
	bucketFeatures  = "bucket." + featureFlagsJname
)

var clusterFeatDesc = [...]string{
	"enforce intra-cluster access",
	"skip loading existing object's metadata, Version and Checksum (VC) in particular (advanced usage only)",
	"do not auto-detect file share (NFS, SMB) when _promoting_ shared files to AIS",
	"handle s3 requests via `aistore-hostname/` (default: `aistore-hostname/s3`)",
	"when finalizing PUT(object): fflush prior to (close, rename) sequence",
	".tar.lz4 format, lz4 compression: max uncompressed block size=1MB (default: 256K)",
	"checksum lz4 frames (default: don't)",
	"do not allow passing fully-qualified name of a locally stored object to (local) ETL containers",
	"run in presence of _limited coexistence_ type conflicts (same as e.g. CopyBckMsg.Force but globally)",
	"pass-through client-signed (presigned) S3 requests for subsequent authentication by S3",
	"when prefix doesn't end with '/' and is a subdirectory: don't assume there are no _prefixed_ obj names",
	"disable cold-GET (from remote bucket)",
	"write and transmit cold-GET content back to user in parallel, without _finalizing_ in-cluster object",
	"intra-cluster communications: instead of regular HTTP redirects reverse-proxy S3 API calls to designated targets",
	"use older path-style addressing (as opposed to virtual-hosted style), e.g., https://s3.amazonaws.com/BUCKET/KEY",
	"disable lazy deletion during global rebalance: do not delete misplaced sources of the migrated objects",
	"intra-cluster control plane: use default network priority (do not set IPv4 ToS to low-latency)",
	"when checking whether objects are identical trust only cryptographically secure checksums",
	"when versioning info is requested, use ListObjectVersions API (beware: extremely slow, versioned S3 buckets only)",
	"include (bucket, xaction) Prometheus variable labels with every GET and PUT transaction",
	"system-reserved (do not set: the flag may be redefined or removed at any time)",
	"resume interrupted multipart uploads from persisted partial manifests",
	"do not delete unrecognized/invalid FQNs during space cleanup ('ais space-cleanup')",
	"when bucket is n-way mirrored read object replica from the least-utilized mountpath",

	// apc.ResetToken ("none") ===========
}

// best-effort tags to group features in help output
var featTags = map[string]string{
	"Enforce-IntraCluster-Access":          "security",
	"Skip-Loading-VersionChecksum-MD":      "perf,integrity-",
	"Do-not-Auto-Detect-FileShare":         "promote,ops",
	"S3-API-via-Root":                      "s3,compat,ops",
	"Fsync-PUT":                            "integrity+,overhead",
	"LZ4-Block-1MB":                        "lz4",
	"LZ4-Frame-Checksum":                   "lz4",
	"Do-not-Allow-Passing-FQN-to-ETL":      "etl,security",
	"Ignore-LimitedCoexistence-Conflicts":  "ops,integrity-",
	"S3-Presigned-Request":                 "s3,security,compat",
	"Do-not-Optimize-Listing-Virtual-Dirs": "overhead",
	"Disable-Cold-GET":                     "perf,integrity-",
	"Streaming-Cold-GET":                   "perf,integrity-",
	"S3-Reverse-Proxy":                     "s3,net,ops",
	"S3-Use-Path-Style":                    "s3,compat",
	"Do-not-Delete-When-Rebalancing":       "integrity?,ops",
	"Do-not-Set-Control-Plane-ToS":         "net,ops",
	"Trust-Crypto-Safe-Checksums":          "integrity+,overhead",
	"S3-ListObjectVersions":                "s3,overhead",
	"Enable-Detailed-Prom-Metrics":         "telemetry,overhead",
	"System-Reserved":                      "ops",
	"Resume-Interrupted-MPU":               "mpu,ops",
	"Keep-Unknown-FQN":                     "integrity?,ops",
	"Load-Balance-GET":                     "perf",
}

// common (cluster, bucket) feature-flags (set, show) helper
func printFeatVerbose(c *cli.Context, flags feat.Flags, scopeBucket bool) error {
	fmt.Fprintln(c.App.Writer)
	flat := _flattenFeat(flags, scopeBucket)
	return teb.Print(flat, teb.FeatTagsDescTmplHdr+teb.PropValTmplNoHdr)
}

func _flattenFeat(flags feat.Flags, scopeBucket bool) (flat nvpairList) {
	for i, f := range feat.Cluster {
		if scopeBucket && !feat.IsBucketScope(f) {
			continue
		}
		nv := nvpair{Name: f, Value: "-\t " + fred("(description pending)")}

		if i < len(clusterFeatDesc) {
			nv.Value = clusterFeatDesc[i]
			tags := featTags[f]
			if flags.IsSet(1 << i) {
				nv.Value = tags + "\t " + fcyan(nv.Value)
			} else {
				nv.Value = tags + "\t " + nv.Value
			}
		}
		flat = append(flat, nv)
	}
	return flat
}
