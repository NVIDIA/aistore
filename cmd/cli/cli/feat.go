// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/urfave/cli"
)

// Features feat.Flags `json:"features,string" as per (bucket: cmn/api and cluster: cmn/config)

const (
	featureFlagsJname = "features"

	clusterFeatures = "cluster." + featureFlagsJname
	bucketFeatures  = "bucket." + featureFlagsJname
)

// NOTE:
// - `Bucket` features are a strict subset of all `Cluster` features, and can be changed for individual buckets;
// - for any changes, check server-side cmn/feat/feat

var clusterFeatDesc = [...]string{
	"enforce intra-cluster access",
	"(*) skip loading existing object's metadata, Version and Checksum (VC) in particular",
	"do not auto-detect file share (NFS, SMB) when _promoting_ shared files to AIS",
	"handle s3 requests via `aistore-hostname/` (default: `aistore-hostname/s3`)",
	"(*) when finalizing PUT(object): fflush prior to (close, rename) sequence",
	".tar.lz4 format, lz4 compression: max uncompressed block size=1MB (default: 256K)",
	"checksum lz4 frames (default: don't)",
	"do not allow passing fully-qualified name of a locally stored object to (local) ETL containers",
	"run in presence of _limited coexistence_ type conflicts (same as e.g. CopyBckMsg.Force but globally)",
	"(*) pass-through client-signed (presigned) S3 requests for subsequent authentication by S3",
	"when prefix doesn't end with '/' and is a subdirectory: don't assume there are no _prefixed_ obj names",
	"disable cold-GET (from remote bucket)",
	"write and transmit cold-GET content back to user in parallel, without _finalizing_ in-cluster object",
	"intra-cluster communications: instead of regular HTTP redirects reverse-proxy S3 API calls to designated targets",
	"use older path-style addressing (as opposed to virtual-hosted style), e.g., https://s3.amazonaws.com/BUCKET/KEY",
	"disable lazy deletion during global rebalance: do not delete misplaced sources of the migrated objects",
	"intra-cluster control plane: do not set IPv4 ToS field (to low-latency)",
	"when checking whether objects are identical trust only cryptographically secure checksums",
	"when versioning info is requested, use ListObjectVersions API (beware: extremely slow, versioned S3 buckets only)",
	"include (bucket, xaction) Prometheus variable labels with every GET and PUT transaction",

	// "none" ====================
}

// common (cluster, bucket) feature-flags (set, show) helper
func printFeatVerbose(c *cli.Context, flags feat.Flags, scopeBucket bool) error {
	fmt.Fprintln(c.App.Writer)
	flat := _flattenFeat(flags, scopeBucket)
	return teb.Print(flat, teb.FeatDescTmplHdr+teb.PropValTmplNoHdr)
}

func _flattenFeat(flags feat.Flags, scopeBucket bool) (flat nvpairList) {
	for i, f := range feat.Cluster {
		if scopeBucket && !feat.IsBucketScope(f) {
			continue
		}
		nv := nvpair{Name: f, Value: clusterFeatDesc[i]}
		if flags.IsSet(1 << i) {
			nv.Value = fcyan(nv.Value)
		}
		flat = append(flat, nv)
	}
	return flat
}
