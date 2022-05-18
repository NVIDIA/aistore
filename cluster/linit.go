// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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
//    (requires subsequent re-caching via lom.ReCache())
// 5) update persistent state on disk: lom.Persist()
// 6) remove a given LOM instance from cache: lom.Uncache()
// 7) evict an entire bucket-load of LOM cache: cluster.EvictCache(bucket)
// 8) periodic (lazy) eviction followed by access-time synchronization: see LomCacheRunner
// =======================================================================================

func (lom *LOM) InitFQN(fqn string, expbck *cmn.Bck) (err error) {
	var parsedFQN fs.ParsedFQN
	parsedFQN, lom.HrwFQN, err = ResolveFQN(fqn)
	if err != nil {
		return
	}
	debug.Assert(parsedFQN.ContentType == fs.ObjectType)
	lom.FQN = fqn
	lom.mpathInfo = parsedFQN.MpathInfo
	lom.mpathDigest = parsedFQN.Digest
	lom.ObjName = parsedFQN.ObjName
	lom.bck = *(*Bck)(&parsedFQN.Bck)

	if expbck != nil {
		debug.Assert(!expbck.IsEmpty())
		if expbck.Name != parsedFQN.Bck.Name {
			return fmt.Errorf("lom-init %s: bucket mismatch (%s != %s)", lom.FQN, expbck.String(), parsedFQN.Bck)
		}
		if expbck.Provider != "" && expbck.Provider != lom.bck.Provider {
			return fmt.Errorf("lom-init %s: provider mismatch (%q != %q)", lom.FQN, lom.bck.Provider, expbck.Provider)
		}
		if !expbck.Ns.IsGlobal() && expbck.Ns != parsedFQN.Bck.Ns {
			return fmt.Errorf("lom-init %s: namespace mismatch (%s != %s)", lom.FQN, expbck.Ns, parsedFQN.Bck.Ns)
		}
	}

	if err = lom.bck.initFast(T.Bowner()); err != nil {
		return
	}
	lom.md.uname = lom.bck.MakeUname(lom.ObjName)
	return nil
}

func (lom *LOM) InitCT(ct *CT) {
	debug.Assert(ct.contentType == fs.ObjectType)
	debug.AssertMsg(ct.bck.Props != nil, ct.bck.String()+" must be initialized")
	lom.FQN = ct.fqn
	lom.HrwFQN = ct.hrwFQN
	lom.mpathInfo = ct.mpathInfo
	lom.mpathDigest = ct.digest
	lom.ObjName = ct.objName
	lom.bck = *ct.bck
	lom.md.uname = ct.Uname()
}

func (lom *LOM) InitBck(bck *cmn.Bck) (err error) {
	debug.Assert(!bck.IsEmpty())
	lom.bck = *(*Bck)(bck)
	if err = lom.bck.initFast(T.Bowner()); err != nil {
		return
	}
	lom.md.uname = lom.bck.MakeUname(lom.ObjName)
	lom.mpathInfo, lom.mpathDigest, err = HrwMpath(lom.md.uname)
	if err != nil {
		return
	}
	lom.FQN = lom.mpathInfo.MakePathFQN(lom.Bucket(), fs.ObjectType, lom.ObjName)
	lom.HrwFQN = lom.FQN
	return
}

func (lom *LOM) String() string {
	if lom.info != "" {
		return lom.info
	}
	return lom._string(bool(glog.FastV(4, glog.SmoduleCluster)))
}

func (lom *LOM) StringEx() string { return lom._string(true) }

func (lom *LOM) _string(verbose bool) string {
	var a, s string
	if verbose {
		s = "o[" + lom.bck.String() + "/" + lom.ObjName + ", " + lom.mpathInfo.String()
		if lom.md.Size != 0 {
			s += " size=" + cos.B2S(lom.md.Size, 1)
		}
		if lom.md.Ver != "" {
			s += " ver=" + lom.md.Ver
		}
		if lom.md.Cksum != nil {
			s += " " + lom.md.Cksum.String()
		}
	} else {
		s = "o[" + lom.bck.Name + "/" + lom.ObjName
	}
	if lom.loaded() {
		if lom.IsCopy() {
			a += "(copy)"
		} else if !lom.IsHRW() {
			a += "(misplaced)"
		}
		if n := lom.NumCopies(); n > 1 {
			a += fmt.Sprintf("(%dc)", n)
		}
	} else {
		a = "(-)"
		if !lom.IsHRW() {
			a += "(not-hrw)"
		}
	}
	lom.info = s + a + "]"
	return lom.info
}

// allocates and copies metadata (in particular, atime and uname)
func (lom *LOM) CloneMD(fqn string) *LOM {
	dst := AllocLOM("")
	*dst = *lom
	dst.md = lom.md
	dst.md.bckID = 0
	dst.md.copies = nil
	dst.FQN = fqn
	return dst
}

/////////
// LIF //
/////////

// LIF => LOF with a check for bucket existence
func (lif *LIF) LOM() (lom *LOM, err error) {
	b, objName := cmn.ParseUname(lif.Uname)
	lom = AllocLOM(objName)
	if err = lom.InitBck(&b); err != nil {
		FreeLOM(lom)
		return
	}
	if bprops := lom.Bprops(); bprops == nil {
		err = cmn.NewErrObjDefunct(lom.String(), 0, lif.BID)
		FreeLOM(lom)
	} else if bprops.BID != lif.BID {
		err = cmn.NewErrObjDefunct(lom.String(), bprops.BID, lif.BID)
		FreeLOM(lom)
	}
	return
}

func (lom *LOM) LIF() (lif LIF) {
	debug.Assert(lom.md.uname != "")
	debug.Assert(lom.Bprops() != nil && lom.Bprops().BID != 0)
	return LIF{lom.md.uname, lom.Bprops().BID}
}
