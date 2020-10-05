// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"github.com/NVIDIA/aistore/fs"
)

//
// resolve and validate fqn
//
func ResolveFQN(fqn string) (parsedFQN fs.ParsedFQN, hrwFQN string, err error) {
	var digest uint64
	parsedFQN, err = fs.ParseFQN(fqn)
	if err != nil {
		return
	}
	// NOTE: "misplaced" (when hrwFQN != fqn) is to be checked separately, via lom.IsHRW()
	bck := &Bck{Bck: parsedFQN.Bck}
	hrwFQN, digest, err = HrwFQN(bck, parsedFQN.ContentType, parsedFQN.ObjName)
	if err != nil {
		return
	}
	parsedFQN.Digest = digest
	return
}

func HrwFQN(bck *Bck, contentType, objName string) (fqn string, digest uint64, err error) {
	var (
		mpathInfo *fs.MountpathInfo
		uname     = bck.MakeUname(objName)
	)
	if mpathInfo, digest, err = HrwMpath(uname); err == nil {
		fqn = fs.CSM.FQN(mpathInfo, bck.Bck, contentType, objName)
	}
	return
}
