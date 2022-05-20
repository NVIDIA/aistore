// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
)

// LOM In Flight (LIF)
type (
	LIF struct {
		Uname       string
		BID         uint64
		mpathDigest uint64
	}
	lifUnlocker interface {
		CacheIdx() int // TODO: lowercase
		getLocker() *nlc
	}
)

// interface guard to make sure that LIF can be used to unlock LOM
var _ lifUnlocker = (*LIF)(nil)

// constructor
func (lom *LOM) LIF() (lif LIF) {
	debug.Assert(lom.md.uname != "")
	debug.Assert(lom.Bprops() != nil && lom.Bprops().BID != 0)
	return LIF{
		Uname:       lom.md.uname,
		BID:         lom.Bprops().BID,
		mpathDigest: lom.mpathDigest,
	}
}

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

// deferred unlocking
func (lif *LIF) CacheIdx() int   { return fs.LcacheIdx(lif.mpathDigest) }
func (lif *LIF) getLocker() *nlc { return &lomLocker[lif.CacheIdx()] }

func (lif *LIF) Unlock(exclusive bool) {
	nlc := lif.getLocker()
	nlc.Unlock(lif.Uname, exclusive)
}
