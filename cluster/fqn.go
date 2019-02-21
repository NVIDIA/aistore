// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"errors"
	"fmt"

	"github.com/NVIDIA/aistore/fs"
)

//
// resolve and validate fqn
//
func ResolveFQN(fqn string, bowner Bowner, isLocal ...bool) (parsedFQN fs.ParsedFQN, hrwFQN string, err error) {
	var errstr string
	parsedFQN, err = fs.Mountpaths.FQN2Info(fqn)
	if err != nil {
		return
	}
	// NOTE: "misplaced" (when hrwFQN != fqn) is to be checked separately, via lom.Misplaced()
	hrwFQN, errstr = FQN(parsedFQN.ContentType, parsedFQN.Bucket, parsedFQN.Objname, parsedFQN.IsLocal)
	if errstr != "" {
		err = errors.New(errstr)
		return
	}
	bckIsLocal := parsedFQN.IsLocal
	if len(isLocal) == 0 {
		if !bowner.Get().IsLocal(parsedFQN.Bucket) && bckIsLocal {
			err = fmt.Errorf("parsed local bucket (%s) for FQN (%s) does not exist", parsedFQN.Bucket, fqn)
			return
		}
	} else {
		bckIsLocal = isLocal[0] // caller has already done the above
	}
	if bckIsLocal != parsedFQN.IsLocal {
		err = fmt.Errorf("%s (%s/%s) - bucket locality mismatch (provided: %t, parsed: %t)", fqn, parsedFQN.Bucket, parsedFQN.Objname, bckIsLocal, parsedFQN.IsLocal)
	}
	return
}

func FQN(contentType, bucket, objname string, isLocal bool) (fqn, errstr string) {
	var mpathInfo *fs.MountpathInfo
	if mpathInfo, errstr = hrwMpath(bucket, objname); errstr == "" {
		fqn = fs.CSM.FQN(mpathInfo, contentType, isLocal, bucket, objname)
	}
	return
}
