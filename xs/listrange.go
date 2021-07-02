// Package runners provides implementation for the AIStore extended actions.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/objwalk"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

// Assorted list/range xactions: evict, delete, prefetch multiple objects
//
// Supported ranges:
//   1. bash-extension style: `file-{0..100}`
//   2. at-style: `file-@100`
//   3. if none of the above, fall back to just prefix matching
//
// NOTE: rebalancing vs performance comment below.

type (
	bckAbortable interface { // two selected methods that _lrBase needs for itself
		Bck() cmn.Bck
		Aborted() bool
	}
	_lrBase struct {
		xact bckAbortable
		t    cluster.Target
		args *xreg.ListRangeArgs
	}
	evdFactory struct {
		xreg.BaseBckEntry
		xact *evictDelete

		t    cluster.Target
		kind string
		args *xreg.ListRangeArgs
	}
	evictDelete struct {
		xaction.XactBase
		_lrBase
	}

	lrCallback func(args *xreg.ListRangeArgs, lom *cluster.LOM) error

	PrfchFactory struct {
		xreg.BaseBckEntry
		xact *prefetch
		t    cluster.Target
		args *xreg.ListRangeArgs
	}
	prefetch struct {
		xaction.XactBase
		_lrBase
	}
)

// interface guard
var (
	_ cluster.Xact = (*evictDelete)(nil)
	_ cluster.Xact = (*prefetch)(nil)

	_ xreg.BckFactory = (*evdFactory)(nil)
	_ xreg.BckFactory = (*PrfchFactory)(nil)
)

//////////////////
// evict/delete //
//////////////////

func (p *evdFactory) New(args *xreg.XactArgs) xreg.BucketEntry {
	a := args.Custom.(*xreg.ListRangeArgs)
	debug.Assert(a.RangeMsg != nil || a.ListMsg != nil)
	debug.Assert(a.RangeMsg == nil || a.ListMsg == nil)
	return &evdFactory{t: args.T, kind: p.kind, args: a}
}

func (p *evdFactory) Start(bck cmn.Bck) error {
	p.xact = newEvictDelete(p.args.UUID, p.kind, bck, p.t, p.args)
	return nil
}

func (p *evdFactory) Kind() string      { return p.kind }
func (p *evdFactory) Get() cluster.Xact { return p.xact }

func newEvictDelete(uuid, kind string, bck cmn.Bck, t cluster.Target, a *xreg.ListRangeArgs) (ed *evictDelete) {
	xargs := xaction.Args{ID: xaction.BaseID(uuid), Kind: kind, Bck: &bck}
	ed = &evictDelete{
		XactBase: *xaction.NewXactBase(xargs),
		_lrBase:  _lrBase{t: t, args: a},
	}
	ed._lrBase.xact = ed
	return
}

func (r *evictDelete) Run() {
	var err error
	if r.args.RangeMsg != nil {
		err = r.iterateRange(r.args, r.do)
	} else {
		err = r.iterateList(r.args, r.args.ListMsg, r.do)
	}
	r.Finish(err)
}

func (r *evictDelete) do(args *xreg.ListRangeArgs, lom *cluster.LOM) error {
	errCode, err := r.t.DeleteObject(args.Ctx, lom, r.Kind() == cmn.ActEvictObjects)
	if errCode == http.StatusNotFound {
		return nil
	}
	if err != nil {
		if cmn.IsErrObjNought(err) {
			return nil
		}
		return err
	}
	r.ObjectsInc()
	r.BytesAdd(lom.SizeBytes(true)) // was loaded and evicted
	return nil
}

//////////////
// prefetch //
//////////////

func (*PrfchFactory) New(args *xreg.XactArgs) xreg.BucketEntry {
	a := args.Custom.(*xreg.ListRangeArgs)
	debug.Assert(a.RangeMsg != nil || a.ListMsg != nil)
	debug.Assert(a.RangeMsg == nil || a.ListMsg == nil)
	return &PrfchFactory{t: args.T, args: a}
}

func (p *PrfchFactory) Start(bck cmn.Bck) error {
	b := cluster.NewBckEmbed(bck)
	if err := b.Init(p.t.Bowner()); err != nil {
		if cmn.IsErrRemoteBckNotFound(err) {
			glog.Warning(err) // may show up later via ais/prxtrybck.go logic
		} else {
			return err
		}
	} else if b.IsAIS() {
		glog.Errorf("bucket %q: can only prefetch remote buckets", b)
		return fmt.Errorf("bucket %q: can only prefetch remote buckets", b)
	}
	p.xact = newPrefetch(p.args.UUID, p.Kind(), bck, p.t, p.args)
	return nil
}

func (*PrfchFactory) Kind() string        { return cmn.ActPrefetch }
func (p *PrfchFactory) Get() cluster.Xact { return p.xact }

func newPrefetch(uuid, kind string, bck cmn.Bck, t cluster.Target, a *xreg.ListRangeArgs) (prf *prefetch) {
	xargs := xaction.Args{ID: xaction.BaseID(uuid), Kind: kind, Bck: &bck}
	prf = &prefetch{
		XactBase: *xaction.NewXactBase(xargs),
		_lrBase:  _lrBase{t: t, args: a},
	}
	prf._lrBase.xact = prf
	return
}

func (r *prefetch) Run() {
	var err error
	if r.args.RangeMsg != nil {
		err = r.iterateRange(r.args, r.do)
	} else {
		err = r.iterateList(r.args, r.args.ListMsg, r.do)
	}
	r.Finish(err)
}

func (r *prefetch) do(args *xreg.ListRangeArgs, lom *cluster.LOM) error {
	var coldGet bool
	if err := lom.Load(true /*cache it*/, false /*locked*/); err != nil {
		coldGet = cmn.IsObjNotExist(err)
		if !coldGet {
			return err
		}
	}
	if !coldGet && lom.Version() != "" && lom.VersionConf().ValidateWarmGet {
		var err error
		if coldGet, _, err = r.t.CheckRemoteVersion(args.Ctx, lom); err != nil {
			return err
		}
	}
	if !coldGet {
		return nil
	}
	// Do not set Atime to current time as prefetching does not mean the object
	// was used. At the same time, zero Atime make the lom life-span in the cache
	// too short - the first housekeeping removes it. Set the special value:
	// negatve Now() for correct processing the LOM while housekeeping
	lom.SetAtimeUnix(-time.Now().UnixNano())
	if _, err := r.t.GetCold(args.Ctx, lom, cluster.Prefetch); err != nil {
		if err != cmn.ErrSkip {
			return err
		}
		return nil
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("prefetch: %s", lom)
	}
	r.ObjectsInc()
	r.BytesAdd(lom.SizeBytes())
	return nil
}

/////////////
// _lrbase //
/////////////

func (r *_lrBase) iterateRange(args *xreg.ListRangeArgs, cb lrCallback) error {
	pt, err := parseTemplate(args.RangeMsg.Template)
	if err != nil {
		return err
	}
	smap := r.t.Sowner().Get()
	if len(pt.Ranges) != 0 {
		return r.iterateTemplate(args, smap, &pt, cb)
	}
	return r.iteratePrefix(args, smap, pt.Prefix, cb)
}

func (r *_lrBase) iterateTemplate(args *xreg.ListRangeArgs, smap *cluster.Smap, pt *cos.ParsedTemplate, cb lrCallback) error {
	getNext := pt.Iter()
	for objName, hasNext := getNext(); hasNext; objName, hasNext = getNext() {
		if r.xact.Aborted() {
			return nil
		}
		lom := cluster.AllocLOM(objName)
		err := r.iterate(lom, args, cb, smap)
		cluster.FreeLOM(lom)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *_lrBase) iteratePrefix(args *xreg.ListRangeArgs, smap *cluster.Smap, prefix string, cb lrCallback) error {
	var (
		objList *cmn.BucketList
		err     error
	)
	bck := cluster.NewBckEmbed(r.xact.Bck())
	if err := bck.Init(r.t.Bowner()); err != nil {
		return err
	}
	bremote := bck.IsRemote()
	if !bremote {
		smap = nil // not needed
	}
	msg := &cmn.SelectMsg{Prefix: prefix, Props: cmn.GetPropsStatus}
	for !r.xact.Aborted() {
		if bremote {
			objList, _, err = r.t.Backend(bck).ListObjects(args.Ctx, bck, msg)
		} else {
			walk := objwalk.NewWalk(args.Ctx, r.t, bck, msg)
			objList, err = walk.DefaultLocalObjPage(msg)
		}
		if err != nil {
			return err
		}
		for _, be := range objList.Entries {
			if !be.IsStatusOK() {
				continue
			}
			if r.xact.Aborted() {
				return nil
			}
			lom := cluster.AllocLOM(be.Name)
			err := r.iterate(lom, args, cb, smap)
			cluster.FreeLOM(lom)
			if err != nil {
				return err
			}
		}

		// Stop when the last page is reached.
		if objList.ContinuationToken == "" {
			break
		}
		// Update `ContinuationToken` for the next request.
		msg.ContinuationToken = objList.ContinuationToken
	}
	return nil
}

func (r *_lrBase) iterateList(args *xreg.ListRangeArgs, listMsg *cmn.ListMsg, cb lrCallback) error {
	smap := r.t.Sowner().Get()
	for _, objName := range listMsg.ObjNames {
		if r.xact.Aborted() {
			break
		}
		lom := cluster.AllocLOM(objName)
		err := r.iterate(lom, args, cb, smap)
		cluster.FreeLOM(lom)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *_lrBase) iterate(lom *cluster.LOM, args *xreg.ListRangeArgs, cb lrCallback, smap *cluster.Smap) error {
	if err := lom.Init(r.xact.Bck()); err != nil {
		return err
	}
	if smap != nil {
		// NOTE: checking locality here to speed up iterating, trading off
		//       rebalancing use case for performance - TODO: add
		//       configurable option to lom.Load() instead.
		local, err := r.isLocLOM(lom, smap)
		if err != nil {
			return err
		}
		if !local {
			return nil
		}
	}
	return cb(args, lom)
}

// helpers ---------------

// Parsing:
// 1. bash-extension style: `file-{0..100}`
// 2. at-style: `file-@100`
// 3. if none of the above, fall back to just prefix matching
func parseTemplate(template string) (cos.ParsedTemplate, error) {
	if template == "" {
		return cos.ParsedTemplate{}, errors.New("empty range template")
	}
	if parsed, err := cos.ParseBashTemplate(template); err == nil {
		return parsed, nil
	}
	if parsed, err := cos.ParseAtTemplate(template); err == nil {
		return parsed, nil
	}
	return cos.ParsedTemplate{Prefix: template}, nil
}

// TODO -- FIXME: doesn't belong here - move it
func (r *_lrBase) isLocLOM(lom *cluster.LOM, smap *cluster.Smap) (bool, error) {
	tsi, err := cluster.HrwTarget(lom.Uname(), smap)
	if err != nil {
		return false, err
	}
	return tsi.ID() == r.t.Snode().ID(), nil
}
