// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs/glob"
	"github.com/NVIDIA/aistore/xact"
)

type (
	TCBArgs struct {
		DP      cluster.DP
		BckFrom *meta.Bck
		BckTo   *meta.Bck
		Msg     *apc.TCBMsg
		Phase   string
	}
	TCObjsArgs struct {
		BckFrom *meta.Bck
		BckTo   *meta.Bck
		DP      cluster.DP
	}
	DsortArgs struct {
		BckFrom *meta.Bck
		BckTo   *meta.Bck
	}
	ECEncodeArgs struct {
		Phase string
	}
	BckRenameArgs struct {
		BckFrom *meta.Bck
		BckTo   *meta.Bck
		RebID   string
		Phase   string
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
	debug.Assert(xact.IsSameScope(entry.Kind(), xact.ScopeB, xact.ScopeGB))
	r.bckXacts[entry.Kind()] = entry // no locking: all reg-s are done at init time
}

// RenewBucketXact is general function to renew bucket xaction without any
// additional or specific parameters.
func RenewBucketXact(kind string, bck *meta.Bck, args Args, buckets ...*meta.Bck) (res RenewRes) {
	e := dreg.bckXacts[kind].New(args, bck)
	return dreg.renew(e, bck, buckets...)
}

func RenewECEncode(bck *meta.Bck, uuid, phase string) RenewRes {
	return RenewBucketXact(apc.ActECEncode, bck, Args{Custom: &ECEncodeArgs{Phase: phase}, UUID: uuid})
}

func RenewMakeNCopies(uuid, tag string) {
	var (
		cfg      = cmn.GCO.Get()
		bmd      = glob.T.Bowner().Get()
		provider = apc.AIS
	)
	bmd.Range(&provider, nil, func(bck *meta.Bck) bool {
		if bck.Props.Mirror.Enabled {
			rns := RenewBckMakeNCopies(bck, uuid, tag, int(bck.Props.Mirror.Copies))
			if rns.Err == nil && !rns.IsRunning() {
				xact.GoRunW(rns.Entry.Get())
			}
		}
		return false
	})
	// TODO: remais
	for name := range cfg.Backend.Providers {
		ns := cfg.Backend.Providers[name]
		bmd.Range(&name, &ns, func(bck *meta.Bck) bool {
			if bck.Props.Mirror.Enabled {
				rns := RenewBckMakeNCopies(bck, uuid, tag, int(bck.Props.Mirror.Copies))
				if rns.Err == nil && !rns.IsRunning() {
					xact.GoRunW(rns.Entry.Get())
				}
			}
			return false
		})
	}
}

func RenewBckMakeNCopies(bck *meta.Bck, uuid, tag string, copies int) (res RenewRes) {
	e := dreg.bckXacts[apc.ActMakeNCopies].New(Args{Custom: &MNCArgs{tag, copies}, UUID: uuid}, bck)
	return dreg.renew(e, bck)
}

func RenewPromote(uuid string, bck *meta.Bck, args *cluster.PromoteArgs) RenewRes {
	return RenewBucketXact(apc.ActPromote, bck, Args{Custom: args, UUID: uuid})
}

func RenewBckLoadLomCache(uuid string, bck *meta.Bck) RenewRes {
	return RenewBucketXact(apc.ActLoadLomCache, bck, Args{UUID: uuid})
}

func RenewPutMirror(lom *cluster.LOM) RenewRes {
	return RenewBucketXact(apc.ActPutCopies, lom.Bck(), Args{Custom: lom})
}

func RenewTCB(uuid, kind string, custom *TCBArgs) RenewRes {
	return RenewBucketXact(
		kind,
		custom.BckTo, // prevent concurrent copy/transform => same dst
		Args{Custom: custom, UUID: uuid},
		custom.BckFrom, custom.BckTo, // find when renewing
	)
}

func RenewDsort(id string, custom *DsortArgs) RenewRes {
	return RenewBucketXact(
		apc.ActDsort,
		custom.BckFrom,
		Args{Custom: custom, UUID: id},
		custom.BckFrom, custom.BckTo,
	)
}

func RenewBckRename(bckFrom, bckTo *meta.Bck, uuid string, rmdVersion int64, phase string) RenewRes {
	custom := &BckRenameArgs{
		Phase:   phase,
		RebID:   xact.RebID2S(rmdVersion),
		BckFrom: bckFrom,
		BckTo:   bckTo,
	}
	return RenewBucketXact(apc.ActMoveBck, bckTo, Args{Custom: custom, UUID: uuid})
}

func RenewLso(bck *meta.Bck, uuid string, msg *apc.LsoMsg) RenewRes {
	e := dreg.bckXacts[apc.ActList].New(Args{UUID: uuid, Custom: msg}, bck)
	return dreg.renewByID(e, bck)
}
