// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
)

func ResolveFQN(fqn string, parsed *fs.ParsedFQN) (hrwFQN string, err error) {
	if err = parsed.Init(fqn); err != nil {
		return
	}

	// NOTE: _misplaced_ hrwFQN != fqn is checked elsewhere - see lom.IsHRW()

	var digest uint64
	hrwFQN, digest, err = HrwFQN(&parsed.Bck, parsed.ContentType, parsed.ObjName)
	if err != nil {
		return
	}
	parsed.Digest = digest
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
