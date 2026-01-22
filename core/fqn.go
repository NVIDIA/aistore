// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"github.com/NVIDIA/aistore/fs"
)

// 1. parse fqn
// 2. compute hrw fqn (may differ)
// 3. compute and set digest
func ResolveFQN(fqn string, parsed *fs.ParsedFQN) (hrwFQN string, _ error) {
	if err := parsed.Init(fqn); err != nil {
		return "", err
	}

	uname := parsed.Bck.MakeUname(parsed.ObjName)
	mi, digest, err := fs.Hrw(uname)
	if err == nil {
		// may differ from fqn if object is misplaced (see lom.IsHRW)
		hrwFQN = mi.MakePathFQN(&parsed.Bck, parsed.ContentType, parsed.ObjName)
	}
	parsed.Digest = digest
	return hrwFQN, err
}
