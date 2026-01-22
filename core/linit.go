// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
)

// Local Object Metadata (LOM) - is cached. Respectively, lifecycle of any given LOM
// instance includes the following steps:
// 1) construct LOM instance and initialize its runtime state: lom = LOM{...}.Init()
// 2) load persistent state (aka lmeta) from one of the LOM caches or the underlying
//    filesystem: lom.Load(); Load(false) also entails *not adding* LOM to caches
//    (useful when deleting or moving objects
// 3) usage: lom.Atime(), lom.Cksum(), and other accessors
//    It is illegal to check LOM's existence and, generally, do almost anything
//    with it prior to loading - see previous
// 4) update persistent state in memory: lom.Set*() methods
//    (requires subsequent re-caching via lom.Recache())
// 5) update persistent state on disk: lom.Persist()
// 6) remove a given LOM instance from cache: lom.Uncache()
// 7) evict an entire bucket-load of LOM cache: cluster.EvictCache(bucket)
// 8) periodic (lazy) eviction followed by access-time synchronization: see LomCacheRunner

// NOTE: to facilitate fast path filtering-out
func (lom *LOM) PreInit(fqn string) error {
	var (
		parsed      fs.ParsedFQN
		hrwFQN, err = ResolveFQN(fqn, &parsed)
	)
	if err != nil {
		return err
	}
	debug.Assert(parsed.ContentType == fs.ObjCT)
	lom.isHRW = hrwFQN == fqn
	lom.FQN = fqn
	lom.mi = parsed.Mountpath
	lom.digest = parsed.Digest
	lom.ObjName = parsed.ObjName
	lom.bck = *(*meta.Bck)(&parsed.Bck)
	return nil
}

func (lom *LOM) PostInit() error {
	if err := lom.bck.InitFast(T.Bowner()); err != nil {
		return err
	}
	uname := lom.bck.MakeUname(lom.ObjName)
	lom.md.uname = cos.UnsafeSptr(uname)
	return nil
}

func (lom *LOM) InitFQN(fqn string, expbck *cmn.Bck) error {
	if err := lom.PreInit(fqn); err != nil {
		return err
	}
	if expbck != nil && !lom.Bucket().Equal(expbck) {
		err := fmt.Errorf("lom-init mismatch for %q: %s vs %s", fqn, lom.Bucket(), expbck)
		debug.AssertNoErr(err)
		return err
	}
	return lom.PostInit()
}

func (lom *LOM) InitCT(ct *CT) {
	debug.Assert(ct.contentType == fs.ObjCT)
	debug.Assert(ct.bck.Props != nil, ct.bck.String()+" must be initialized")
	lom.FQN = ct.fqn
	lom.isHRW = ct.isHRW
	lom.mi = ct.mi
	lom.digest = ct.digest
	lom.ObjName = ct.objName
	lom.bck = *ct.bck
	lom.md.uname = ct.UnamePtr()
}

func (lom *LOM) InitCmnBck(bck *cmn.Bck) (err error) {
	return lom.InitBck((*meta.Bck)(bck))
}

func (lom *LOM) InitBck(bck *meta.Bck) (err error) {
	lom.bck = *bck
	if err = lom.bck.InitFast(T.Bowner()); err != nil {
		return
	}
	uname := lom.bck.MakeUname(lom.ObjName)
	lom.md.uname = cos.UnsafeSptr(uname)
	lom.mi, lom.digest, err = fs.Hrw(uname)
	if err != nil {
		return
	}
	lom.FQN = lom.mi.MakePathFQN(lom.Bucket(), fs.ObjCT, lom.ObjName)
	lom.isHRW = true
	return
}

func (lom *LOM) String() string {
	var (
		sb cos.SB
		l  = 2 + len(lom.bck.Name) + 1 + len(lom.ObjName) + 4
	)
	sb.Init(l)
	sb.WriteString("o[")
	sb.WriteString(lom.bck.Name)
	sb.WriteUint8('/')
	sb.WriteString(lom.ObjName)
	if !lom.loaded() {
		sb.WriteString("(-)")
	}
	sb.WriteUint8(']')
	return sb.String()
}

// allocates and copies metadata (in particular, atime and uname)
// NOTE: cloned lom.bid() == 0 is possible - copying/transforming scenarios
func (lom *LOM) CloneTo(fqn string) *LOM {
	clone := lom.Clone()
	clone.FQN = fqn
	clone.md.copies = nil
	return clone
}

func (lom *LOM) Clone() *LOM {
	clone := AllocLOM("")
	*clone = *lom
	clone.md = lom.md
	return clone
}
