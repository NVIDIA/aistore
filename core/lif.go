// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"errors"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// LOM In Flight (LIF)
//
// core.LIF is a "terminal" value (pure scalars) that fully represents core.LOM across async boundaries
// (e.g., send => ack => lazydel / retransmit)

type (
	LIF struct {
		Uname  string
		BID    uint64
		Digest uint64
		Size   int64
	}
	lifUnlocker interface {
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
		Uname:  *lom.md.uname,
		BID:    lid.bid(),
		Digest: lom.digest,
		Size:   lom.Lsize(true), // TODO: pass size explicitly in: lom.LIF(size)
	}
}

func (lif *LIF) Cname() string {
	b, objName := cmn.ParseUname(lif.Uname)
	return b.Cname(objName)
}

// LIF => LOM with a check for bucket existence
func (lif *LIF) LOM() (lom *LOM, err error) {
	if lif.Uname == "" {
		return nil, errEmptyLIF
	}
	b, objName := cmn.ParseUname(lif.Uname)
	lom = AllocLOM(objName)
	if err = lom.InitCmnBck(&b); err != nil {
		FreeLOM(lom)
		return nil, err
	}
	bprops := lom.Bprops()
	debug.Assert(bprops != nil)
	if lif.BID != 0 && lif.BID != bprops.BID {
		err = cmn.NewErrObjDefunct(lom.String(), lif.BID, bprops.BID)
		FreeLOM(lom)
		return nil, err
	}
	lom.setbid(bprops.BID) // reconstruction path
	return lom, nil
}

// deferred unlocking
// (compare with lom.getLocker)
func (lif *LIF) getLocker() *nlc {
	idx := lcacheIdx(lif.Digest)
	return &g.locker[idx]
}

func (lif *LIF) Unlock(exclusive bool) {
	nlc := lif.getLocker()
	nlc.Unlock(lif.Uname, exclusive)
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
