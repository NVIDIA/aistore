// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/xact"
)

type (
	TCBArgs struct {
		Phase   string
		BckFrom *cluster.Bck
		BckTo   *cluster.Bck
		DP      cluster.DP
		Msg     *cmn.TCBMsg
	}

	TCObjsArgs struct {
		BckFrom *cluster.Bck
		BckTo   *cluster.Bck
		DP      cluster.DP
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

//////////////
// registry //
//////////////

func RegBckXact(entry Renewable) { dreg.regBckXact(entry) }

func (r *registry) regBckXact(entry Renewable) {
	debug.Assert(xact.Table[entry.Kind()].Scope == xact.ScopeBck)

	// It is expected that registrations happen at the init time. Therefore, it
	// is safe to assume that no `RenewXYZ` will happen before all xactions
	// are registered. Thus, no locking is needed.
	r.bckXacts[entry.Kind()] = entry
}

// RenewBucketXact is general function to renew bucket xaction without any
// additional or specific parameters.
func RenewBucketXact(kind string, bck *cluster.Bck, args Args) (res RenewRes) {
	e := dreg.bckXacts[kind].New(args, bck)
	return dreg.renew(e, bck)
}

func RenewECEncode(t cluster.Target, bck *cluster.Bck, uuid, phase string) RenewRes {
	return RenewBucketXact(apc.ActECEncode, bck, Args{t, uuid, &ECEncodeArgs{Phase: phase}})
}

func RenewMakeNCopies(t cluster.Target, uuid, tag string) {
	var (
		cfg      = cmn.GCO.Get()
		bmd      = t.Bowner().Get()
		provider = apc.ProviderAIS
	)
	bmd.Range(&provider, nil, func(bck *cluster.Bck) bool {
		if bck.Props.Mirror.Enabled {
			rns := RenewBckMakeNCopies(t, bck, uuid, tag, int(bck.Props.Mirror.Copies))
			if rns.Err == nil && !rns.IsRunning() {
				xact.GoRunW(rns.Entry.Get())
			}
		}
		return false
	})
	// TODO: remote ais
	for name, ns := range cfg.Backend.Providers {
		bmd.Range(&name, &ns, func(bck *cluster.Bck) bool {
			if bck.Props.Mirror.Enabled {
				rns := RenewBckMakeNCopies(t, bck, uuid, tag, int(bck.Props.Mirror.Copies))
				if rns.Err == nil && !rns.IsRunning() {
					xact.GoRunW(rns.Entry.Get())
				}
			}
			return false
		})
	}
}

func RenewBckMakeNCopies(t cluster.Target, bck *cluster.Bck, uuid, tag string, copies int) (res RenewRes) {
	e := dreg.bckXacts[apc.ActMakeNCopies].New(Args{t, uuid, &MNCArgs{tag, copies}}, bck)
	return dreg.renew(e, bck)
}

func RenewPromote(t cluster.Target, uuid string, bck *cluster.Bck, args *cluster.PromoteArgs) RenewRes {
	return RenewBucketXact(apc.ActPromote, bck, Args{t, uuid, args})
}

func RenewBckLoadLomCache(t cluster.Target, uuid string, bck *cluster.Bck) RenewRes {
	return RenewBucketXact(apc.ActLoadLomCache, bck, Args{T: t, UUID: uuid})
}

func RenewPutMirror(t cluster.Target, lom *cluster.LOM) RenewRes {
	return RenewBucketXact(apc.ActPutCopies, lom.Bck(), Args{T: t, Custom: lom})
}

func RenewTCB(t cluster.Target, uuid, kind string, custom *TCBArgs) RenewRes {
	return RenewBucketXact(kind, custom.BckTo /*NOTE: to not from*/, Args{t, uuid, custom})
}

func RenewTCObjs(t cluster.Target, uuid, kind string, custom *TCObjsArgs) RenewRes {
	return RenewBucketXact(kind, custom.BckFrom, Args{t, uuid, custom})
}

func RenewBckRename(t cluster.Target, bckFrom, bckTo *cluster.Bck, uuid string, rmdVersion int64, phase string) RenewRes {
	custom := &BckRenameArgs{
		Phase:   phase,
		RebID:   xact.RebID2S(rmdVersion),
		BckFrom: bckFrom,
		BckTo:   bckTo,
	}
	return RenewBucketXact(apc.ActMoveBck, bckTo, Args{t, uuid, custom})
}

func RenewObjList(t cluster.Target, bck *cluster.Bck, uuid string, msg *apc.ListObjsMsg) RenewRes {
	e := dreg.bckXacts[apc.ActList].New(Args{T: t, UUID: uuid, Custom: msg}, bck)
	return dreg.renewByID(e, bck)
}
