// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
)

// LOM In Flight (LIF)
type (
	LIF struct {
		uname  string
		bid    uint64
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
	debug.Assert(lom.Bprops() != nil && lom.Bprops().BID != 0)
	bid := lom.bid()
	if bid == 0 {
		bid = lom.Bprops().BID
	}
	debug.Assert(bid == lom.Bprops().BID, bid, " vs ", lom.Bprops().BID) // TODO -- FIXME: 52 bits
	return LIF{
		uname:  *lom.md.uname,
		bid:    bid,
		digest: lom.digest,
	}
}

// LIF => LOF with a check for bucket existence
func (lif *LIF) LOM() (lom *LOM, err error) {
	b, objName := cmn.ParseUname(lif.uname)
	lom = AllocLOM(objName)
	if err = lom.InitBck(&b); err != nil {
		FreeLOM(lom)
		return
	}
	if bprops := lom.Bprops(); bprops == nil {
		err = cmn.NewErrObjDefunct(lom.String(), 0, lif.bid)
		FreeLOM(lom)
	} else if bprops.BID != lif.bid { // TODO -- FIXME: 52 bits
		err = cmn.NewErrObjDefunct(lom.String(), bprops.BID, lif.bid)
		FreeLOM(lom)
	}
	return
}

// deferred unlocking

func (lif *LIF) CacheIdx() int   { return fs.LcacheIdx(lif.digest) }
func (lif *LIF) getLocker() *nlc { return &g.locker[lif.CacheIdx()] }

func (lif *LIF) Unlock(exclusive bool) {
	nlc := lif.getLocker()
	nlc.Unlock(lif.uname, exclusive)
}
