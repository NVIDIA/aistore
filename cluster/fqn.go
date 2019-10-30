// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"github.com/NVIDIA/aistore/fs"
)

//
// resolve and validate fqn
//
func ResolveFQN(fqn string) (parsedFQN fs.ParsedFQN, hrwFQN string, err error) {
	var (
		digest uint64
	)
	parsedFQN, err = fs.Mountpaths.FQN2Info(fqn)
	if err != nil {
		return
	}
	// NOTE: "misplaced" (when hrwFQN != fqn) is to be checked separately, via lom.IsHRW()
	bck := &Bck{Name: parsedFQN.Bucket, Provider: parsedFQN.Provider}
	hrwFQN, digest, err = HrwFQN(parsedFQN.ContentType, bck, parsedFQN.ObjName)
	if err != nil {
		return
	}
	parsedFQN.Digest = digest
	return
}

func HrwFQN(contentType string, bck *Bck, objName string) (fqn string, digest uint64, err error) {
	var mpathInfo *fs.MountpathInfo
	if mpathInfo, digest, err = HrwMpath(bck.Name, objName); err == nil {
		fqn = fs.CSM.FQN(mpathInfo, contentType, bck.Name, bck.Provider, objName)
	}
	return
}
