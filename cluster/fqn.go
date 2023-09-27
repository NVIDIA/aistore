// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
)

func ResolveFQN(fqn string) (parsedFQN fs.ParsedFQN, hrwFQN string, err error) {
	var digest uint64
	parsedFQN, err = fs.ParseFQN(fqn)
	if err != nil {
		return
	}
	// NOTE: _misplaced_ (hrwFQN != fqn) is checked elsewhere (see lom.IsHRW())
	hrwFQN, digest, err = HrwFQN(&parsedFQN.Bck, parsedFQN.ContentType, parsedFQN.ObjName)
	if err != nil {
		return
	}
	parsedFQN.Digest = digest
	return
}

func HrwFQN(bck *cmn.Bck, contentType, objName string) (fqn string, digest uint64, err error) {
	var (
		mi    *fs.Mountpath
		uname = bck.MakeUname(objName)
	)
	if mi, digest, err = fs.Hrw(uname); err == nil {
		fqn = mi.MakePathFQN(bck, contentType, objName)
	}
	return
}
