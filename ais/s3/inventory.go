// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// NOTE currently implemented main assumption/requirement:
// one bucket, one inventory (for this same bucket), and one statically defined .csv

const (
	InvName   = ".inventory"
	InvSrcExt = ".csv.gz"
	InvDstExt = ".csv"
)

func InvPrefObjname(bck *cmn.Bck, name, id string) (prefix, objName string) {
	if name == "" {
		name = InvName
	}
	prefix = name + cos.PathSeparator + bck.Name
	if id != "" {
		prefix += cos.PathSeparator + id
	}
	if name == InvName {
		objName = prefix + InvDstExt
	} else {
		objName = InvName + cos.PathSeparator + prefix + InvDstExt
	}
	return prefix, objName
}
