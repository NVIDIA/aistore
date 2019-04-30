// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"

	"github.com/NVIDIA/aistore/fs"
)

//
// resolve and validate fqn
//
func ResolveFQN(fqn string, bowner Bowner, isLocal ...bool) (parsedFQN fs.ParsedFQN, hrwFQN string, errstr string) {
	var (
		err    error
		digest uint64
	)
	parsedFQN, err = fs.Mountpaths.FQN2Info(fqn)
	if err != nil {
		errstr = err.Error()
		return
	}
	// NOTE: "misplaced" (when hrwFQN != fqn) is to be checked separately, via lom.Misplaced()
	hrwFQN, digest, errstr = HrwFQN(parsedFQN.ContentType, parsedFQN.Bucket, parsedFQN.Objname, parsedFQN.IsLocal)
	if errstr != "" {
		return
	}
	bckIsLocal := parsedFQN.IsLocal
	parsedFQN.Digest = digest
	if len(isLocal) == 0 {
		if !bowner.Get().IsLocal(parsedFQN.Bucket) && bckIsLocal {
			errstr = fmt.Sprintf("local bucket (%s) for FQN (%s) does not exist", parsedFQN.Bucket, fqn)
			return
		}
	} else {
		bckIsLocal = isLocal[0] // caller has already done the above
	}
	if bckIsLocal != parsedFQN.IsLocal {
		errstr = fmt.Sprintf("%s (%s/%s) - bucket locality mismatch (%t != %t)",
			fqn, parsedFQN.Bucket, parsedFQN.Objname, bckIsLocal, parsedFQN.IsLocal)
	}
	return
}

func HrwFQN(contentType, bucket, objname string, isLocal bool) (fqn string, digest uint64, errstr string) {
	var mpathInfo *fs.MountpathInfo
	if mpathInfo, digest, errstr = hrwMpath(bucket, objname); errstr == "" {
		fqn = fs.CSM.FQN(mpathInfo, contentType, isLocal, bucket, objname)
	}
	return
}
