// Package space provides storage cleanup and eviction functionality.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package space

import (
	"strings"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
)

func sysBckEntryOrphaned(sysBck *cmn.Bck, contentType, objName string) bool {
	switch sysBck.Name {
	case cmn.SysNBI:
		return nbiEntryOrphaned(contentType, objName)
	case cmn.SysShardIdx:
		if contentType != fs.ObjCT {
			debug.Assertf(false, "unexpected content type: %q in system bucket: %q", contentType, cmn.SysShardIdx)
			return false
		}
		return shardIdxEntryOrphaned(objName)
	default:
		debug.Assert(false, sysBck.Name)
	}
	return false
}

func nbiEntryOrphaned(contentType, objName string) bool {
	switch contentType {
	case fs.ObjCT:
		return !nbiOwnerBucketExists(objName)
	case fs.ChunkCT:
		ci := fs.CSM.ParseUbase(objName, fs.ChunkCT)
		if !ci.Ok {
			return true
		}
		return !nbiOwnerBucketExists(ci.Base)
	case fs.ChunkMetaCT:
		if nbiOwnerBucketExists(objName) {
			return false
		}
		ci := fs.CSM.ParseUbase(objName, fs.ChunkMetaCT)
		if !ci.Ok {
			return true
		}
		return !nbiOwnerBucketExists(ci.Base)
	default:
		debug.Assertf(false, "unexpected content type: %q in system bucket: %q", contentType, cmn.SysNBI)
		return false
	}
}

// TODO: also validate NBI metadata once fs/nbi.go exposes a checker.
func nbiOwnerBucketExists(objName string) bool {
	srcBck, invName, err := meta.ParseUname(objName, true /*withObjname*/)
	if err != nil || invName == "" {
		return false
	}
	return srcBck.InitFast(core.T.Bowner()) == nil
}

func shardIdxEntryOrphaned(objName string) bool {
	srcBck, srcObj, err := meta.ParseUname(objName, true /*withObjname*/)
	if err != nil || !strings.HasSuffix(srcObj, core.IdxSuffix) {
		return true
	}
	srcObj = strings.TrimSuffix(srcObj, core.IdxSuffix)
	if srcObj == "" {
		return true
	}
	archlom := core.AllocLOM(srcObj)
	defer core.FreeLOM(archlom)
	if err := archlom.InitBck(srcBck); err != nil {
		return true
	}
	if !archlom.TryLock(false) {
		return false
	}
	defer archlom.Unlock(false)
	if err := archlom.Load(false /*cache*/, true /*locked*/); err != nil {
		if cos.IsNotExist(err) {
			return true
		}
		return false // exists but failed to load, not necessarily orphaned (keep)
	}
	return !archlom.HasShardIdx()
}
