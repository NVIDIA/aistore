// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// LOM In Flight (LIF)
type (
	LIF struct {
		uname  string
		lid    lomBID
		digest uint64
	}
	lifUnlocker interface {
		CacheIdx() int
		getLocker() *nlc
	}
)

// interface guard to make sure that LIF can be used to unlock LOM
var _ lifUnlocker = (*LIF)(nil)

// constructor
func (lom *LOM) LIF() (lif LIF) {
	debug.Assert(lom.md.uname != nil)
	bprops := lom.Bprops()
	debug.Assert(bprops != nil && bprops.BID != 0)
	lid := lom.md.lid
	if lid == 0 {
		lid = lomBID(bprops.BID)
	}
	debug.Assert(lid.bid() == bprops.BID, lid.bid(), " vs ", bprops.BID)
	return LIF{
		uname:  *lom.md.uname,
		lid:    lid,
		digest: lom.digest,
	}
}

// LIF => LOF with a check for bucket existence
func (lif *LIF) LOM() (lom *LOM, err error) {
	b, objName := cmn.ParseUname(lif.uname)
	lom = AllocLOM(objName)
	if err = lom.InitBck(&b); err != nil {
		FreeLOM(lom)
		return nil, err
	}
	bprops := lom.Bprops()
	if bprops == nil {
		err = cmn.NewErrObjDefunct(lom.String(), 0, lif.lid.bid())
		FreeLOM(lom)
		return nil, err
	}
	if lif.lid.bid() != bprops.BID {
		err = cmn.NewErrObjDefunct(lom.String(), bprops.BID, lif.lid.bid())
		FreeLOM(lom)
		return nil, err
	}
	return lom, nil
}

// deferred unlocking

func (lif *LIF) CacheIdx() int   { return lcacheIdx(lif.digest) }
func (lif *LIF) getLocker() *nlc { return &g.locker[lif.CacheIdx()] }

func (lif *LIF) Unlock(exclusive bool) {
	nlc := lif.getLocker()
	nlc.Unlock(lif.uname, exclusive)
}
