// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"errors"

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

var errEmptyLIF = errors.New("empty LIF")

// LOM => LIF constructor
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

func (lif *LIF) Name() string {
	b, objName := cmn.ParseUname(lif.uname)
	return b.Cname(objName)
}

// LIF => LOM with a check for bucket existence
func (lif *LIF) LOM() (lom *LOM, err error) {
	if lif.uname == "" {
		return nil, errEmptyLIF
	}
	b, objName := cmn.ParseUname(lif.uname)
	lom = AllocLOM(objName)
	if err = lom.InitCmnBck(&b); err != nil {
		FreeLOM(lom)
		return nil, err
	}
	bprops := lom.Bprops()
	debug.Assert(bprops != nil)
	if bid := lif.lid.bid(); bid != 0 && bid != bprops.BID {
		err = cmn.NewErrObjDefunct(lom.String(), lif.lid.bid(), bprops.BID)
		FreeLOM(lom)
		return nil, err
	}
	lom.setbid(bprops.BID) // reconstruction path
	return lom, nil
}

// deferred unlocking

func (lif *LIF) CacheIdx() int   { return lcacheIdx(lif.digest) }
func (lif *LIF) getLocker() *nlc { return &g.locker[lif.CacheIdx()] }

func (lif *LIF) Unlock(exclusive bool) {
	nlc := lif.getLocker()
	nlc.Unlock(lif.uname, exclusive)
}

// non-blocking drain LIF workCh
func DrainLIF(workCh chan LIF) (n int) {
	for {
		select {
		case _, ok := <-workCh:
			if ok {
				n++
			}
		default:
			return n
		}
	}
}
