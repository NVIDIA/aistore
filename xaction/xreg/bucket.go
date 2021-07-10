// Package registry provides core functionality for the AIStore extended actions xreg.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"fmt"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xaction"
)

type (
	// Providing default pre/post hooks
	BaseBckEntry struct{}

	// Serves to return the result of renewing
	DummyEntry struct {
		xact cluster.Xact
	}

	BucketEntry interface {
		BaseEntry
		// pre-renew: returns true iff the current active one exists and is either
		// - ok to keep running as is, or
		// - has been renew(ed) and is still ok
		PreRenewHook(previousEntry BucketEntry) (keep bool, err error)
		// post-renew hook
		PostRenewHook(previousEntry BucketEntry)
	}

	// BckFactory is an interface to provider a new instance of BucketEntry interface.
	BckFactory interface {
		// New should create empty stub for bucket xaction that could be started
		// with `Start()` method.
		New(args *Args) BucketEntry
		Kind() string
	}

	DirPromoteArgs struct {
		Dir    string
		Params *cmn.ActValPromote
	}

	TransferBckArgs struct {
		Phase   string
		BckFrom *cluster.Bck
		BckTo   *cluster.Bck
		DM      *bundle.DataMover
		DP      cluster.LomReaderProvider
		Meta    *cmn.Bck2BckMsg
	}

	ECEncodeArgs struct {
		Phase string
	}

	BckRenameArgs struct {
		RebID   string
		Phase   string
		BckFrom *cluster.Bck
		BckTo   *cluster.Bck
	}

	MNCArgs struct {
		Tag    string
		Copies int
	}
)

//////////////////
// BaseBckEntry //
//////////////////

func (*BaseBckEntry) PreRenewHook(previousEntry BucketEntry) (keep bool, err error) {
	e := previousEntry.Get()
	_, keep = e.(xaction.XactDemand)
	return
}

func (*BaseBckEntry) PostRenewHook(_ BucketEntry) {}

////////////////
// DummyEntry //
////////////////

// interface guard
var (
	_ BaseEntry = (*DummyEntry)(nil)
)

func (*DummyEntry) Start(_ cmn.Bck) error { debug.Assert(false); return nil }
func (*DummyEntry) Kind() string          { debug.Assert(false); return "" }
func (d *DummyEntry) Get() cluster.Xact   { return d.xact }

//////////////
// registry //
//////////////

func RegFactory(entry BckFactory) { defaultReg.regFactory(entry) }

func (r *registry) regFactory(entry BckFactory) {
	debug.Assert(xaction.XactsDtor[entry.Kind()].Type == xaction.XactTypeBck)

	// It is expected that registrations happen at the init time. Therefore, it
	// is safe to assume that no `RenewXYZ` will happen before all xactions
	// are registered. Thus, no locking is needed.
	r.bckXacts[entry.Kind()] = entry
}

// RenewBucketXact is general function to renew bucket xaction without any
// additional or specific parameters.
func RenewBucketXact(kind string, bck *cluster.Bck, args *Args) (res RenewRes) {
	return defaultReg.renewBucketXact(kind, bck, args)
}

func (r *registry) renewBucketXact(kind string, bck *cluster.Bck, args *Args) (res RenewRes) {
	if args == nil {
		args = &Args{}
	}
	e := r.bckXacts[kind].New(args)
	res = r.renewBckXact(e, bck)
	if res.Err != nil {
		return
	}
	if res.UUID != "" {
		xact := res.Entry.Get()
		// NOTE: make sure existing on-demand is active to prevent it from (idle) expiration
		//       (see demand.go hkcb())
		if xactDemand, ok := xact.(xaction.XactDemand); ok {
			xactDemand.IncPending()
			xactDemand.DecPending()
		}
	}
	return
}

func RenewECEncode(t cluster.Target, bck *cluster.Bck, uuid, phase string) RenewRes {
	return defaultReg.renewECEncode(t, bck, uuid, phase)
}

func (r *registry) renewECEncode(t cluster.Target, bck *cluster.Bck, uuid, phase string) RenewRes {
	return r.renewBucketXact(cmn.ActECEncode, bck, &Args{T: t, UUID: uuid, Custom: &ECEncodeArgs{Phase: phase}})
}

func RenewMakeNCopies(t cluster.Target, uuid, tag string) { defaultReg.renewMakeNCopies(t, uuid, tag) }

func (r *registry) renewMakeNCopies(t cluster.Target, uuid, tag string) {
	var (
		cfg      = cmn.GCO.Get()
		bmd      = t.Bowner().Get()
		provider = cmn.ProviderAIS
	)
	bmd.Range(&provider, nil, func(bck *cluster.Bck) bool {
		if bck.Props.Mirror.Enabled {
			res := r.renewBckMakeNCopies(t, bck, uuid, tag, int(bck.Props.Mirror.Copies))
			if res.Err == nil {
				xact := res.Entry.Get()
				go xact.Run()
			}
		}
		return false
	})
	// TODO: remote ais
	for name, ns := range cfg.Backend.Providers {
		bmd.Range(&name, &ns, func(bck *cluster.Bck) bool {
			if bck.Props.Mirror.Enabled {
				res := r.renewBckMakeNCopies(t, bck, uuid, tag, int(bck.Props.Mirror.Copies))
				if res.Err == nil {
					xact := res.Entry.Get()
					go xact.Run()
				}
			}
			return false
		})
	}
}

func RenewBckMakeNCopies(t cluster.Target, bck *cluster.Bck, uuid, tag string, copies int) (res RenewRes) {
	return defaultReg.renewBckMakeNCopies(t, bck, uuid, tag, copies)
}

func (r *registry) renewBckMakeNCopies(t cluster.Target, bck *cluster.Bck, uuid, tag string, copies int) (res RenewRes) {
	e := r.bckXacts[cmn.ActMakeNCopies].New(&Args{T: t, UUID: uuid, Custom: &MNCArgs{tag, copies}})
	res = r.renewBckXact(e, bck)
	if res.Err != nil {
		return
	}
	if res.UUID != "" {
		res.Err = fmt.Errorf("%s xaction already running", e.Kind())
	}
	return
}

func RenewDirPromote(t cluster.Target, bck *cluster.Bck, dir string, params *cmn.ActValPromote) RenewRes {
	return defaultReg.renewDirPromote(t, bck, dir, params)
}

func (r *registry) renewDirPromote(t cluster.Target, bck *cluster.Bck, dir string, params *cmn.ActValPromote) RenewRes {
	return r.renewBucketXact(cmn.ActPromote, bck, &Args{
		T: t,
		Custom: &DirPromoteArgs{
			Dir:    dir,
			Params: params,
		},
	})
}

func RenewBckLoadLomCache(t cluster.Target, uuid string, bck *cluster.Bck) error {
	res := defaultReg.renewBckLoadLomCache(t, uuid, bck)
	return res.Err
}

func (r *registry) renewBckLoadLomCache(t cluster.Target, uuid string, bck *cluster.Bck) RenewRes {
	return r.renewBucketXact(cmn.ActLoadLomCache, bck, &Args{T: t, UUID: uuid})
}

func RenewPutMirror(t cluster.Target, lom *cluster.LOM) RenewRes {
	return defaultReg.renewPutMirror(t, lom)
}

func (r *registry) renewPutMirror(t cluster.Target, lom *cluster.LOM) RenewRes {
	return r.renewBucketXact(cmn.ActPutCopies, lom.Bck(), &Args{T: t, Custom: lom})
}

func RenewTransferBck(t cluster.Target, bckFrom, bckTo *cluster.Bck, uuid, kind,
	phase string, dm *bundle.DataMover, dp cluster.LomReaderProvider, meta *cmn.Bck2BckMsg) RenewRes {
	return defaultReg.renewTransferBck(t, bckFrom, bckTo, uuid, kind, phase, dm, dp, meta)
}

func (r *registry) renewTransferBck(t cluster.Target, bckFrom, bckTo *cluster.Bck, uuid, kind,
	phase string, dm *bundle.DataMover, dp cluster.LomReaderProvider, meta *cmn.Bck2BckMsg) RenewRes {
	return r.renewBucketXact(kind, bckTo, &Args{
		T:    t,
		UUID: uuid,
		Custom: &TransferBckArgs{
			Phase:   phase,
			BckFrom: bckFrom,
			BckTo:   bckTo,
			DM:      dm,
			DP:      dp,
			Meta:    meta,
		},
	})
}

func RenewBckRename(t cluster.Target, bckFrom, bckTo *cluster.Bck, uuid string, rmdVersion int64, phase string) RenewRes {
	return defaultReg.renewBckRename(t, bckFrom, bckTo, uuid, rmdVersion, phase)
}

func (r *registry) renewBckRename(t cluster.Target, bckFrom, bckTo *cluster.Bck,
	uuid string, rmdVersion int64, phase string) RenewRes {
	return r.renewBucketXact(cmn.ActMoveBck, bckTo, &Args{
		T:    t,
		UUID: uuid,
		Custom: &BckRenameArgs{
			Phase:   phase,
			RebID:   xaction.RebID2S(rmdVersion),
			BckFrom: bckFrom,
			BckTo:   bckTo,
		},
	})
}

func RenewObjList(t cluster.Target, bck *cluster.Bck, uuid string, msg *cmn.SelectMsg) RenewRes {
	return defaultReg.renewObjList(t, bck, uuid, msg)
}

func (r *registry) renewObjList(t cluster.Target, bck *cluster.Bck, uuid string, msg *cmn.SelectMsg) RenewRes {
	xact := r.getXact(uuid)
	if xact == nil || xact.Finished() {
		e := r.bckXacts[cmn.ActList].New(&Args{T: t, UUID: uuid, Custom: msg})
		return r.renewBckXact(e, bck, uuid)
	}
	return RenewRes{&DummyEntry{xact}, nil, uuid}
}
