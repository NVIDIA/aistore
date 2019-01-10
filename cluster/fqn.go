// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"errors"
	"fmt"

	"github.com/NVIDIA/dfcpub/fs"
)

//
// resolve and validate fqn
//
func ResolveFQN(fqn string, bowner Bowner, islocal ...bool) (parsedFQN fs.FQNparsed, hrwfqn string, err error) {
	var errstr string
	parsedFQN, err = fs.Mountpaths.FQN2Info(fqn)
	if err != nil {
		return
	}
	// NOTE: "misplaced" (when hrwfqn != fqn) is to be checked separately, via lom.Misplaced()
	hrwfqn, errstr = FQN(parsedFQN.ContentType, parsedFQN.Bucket, parsedFQN.Objname, parsedFQN.IsLocal)
	if errstr != "" {
		err = errors.New(errstr)
		return
	}
	var bislocal bool
	if len(islocal) == 0 {
		bmd := bowner.Get()
		bislocal = bmd.IsLocal(parsedFQN.Bucket)
	} else {
		bislocal = islocal[0] // caller has already done the above
	}
	if bislocal != parsedFQN.IsLocal {
		err = fmt.Errorf("%s (%s/%s) - islocal mismatch(%t, %t)", fqn, parsedFQN.Bucket, parsedFQN.Objname, bislocal, parsedFQN.IsLocal)
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
