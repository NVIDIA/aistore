// Package runners provides implementation for the AIStore extended actions.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xrun

import (
	"errors"
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

// assorted list/range xactions: evict, delete, prefetch multiple objects

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
	objCallback = func(args *xreg.ListRangeArgs, objName string) error

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
	return &evdFactory{
		t:    args.T,
		kind: p.kind,
		args: args.Custom.(*xreg.ListRangeArgs),
	}
}

func (p *evdFactory) Start(bck cmn.Bck) error {
	p.xact = newEvictDelete(p.args.UUID, p.kind, bck, p.t, p.args)
	return nil
}

func (p *evdFactory) Kind() string      { return p.kind }
func (p *evdFactory) Get() cluster.Xact { return p.xact }

func newEvictDelete(uuid, kind string, bck cmn.Bck, t cluster.Target, dpargs *xreg.ListRangeArgs) (ed *evictDelete) {
	xargs := xaction.Args{ID: xaction.BaseID(uuid), Kind: kind, Bck: &bck}
	ed = &evictDelete{
		XactBase: *xaction.NewXactBase(xargs),
		_lrBase:  _lrBase{t: t, args: dpargs},
	}
	ed._lrBase.xact = ed
	return
}

func (r *evictDelete) Run() {
	var err error
	if r.args.RangeMsg != nil {
		err = r.iterateBucketRange(r.args)
	} else {
		err = r.listOperation(r.args, r.args.ListMsg)
	}
	r.Finish(err)
}

func (r *evictDelete) doObjEvictDelete(args *xreg.ListRangeArgs, objName string) error {
	lom := &cluster.LOM{ObjName: objName}
	if err := lom.Init(r.xact.Bck()); err != nil {
		glog.Error(err)
		return nil
	}
	errCode, err := r.t.DeleteObject(args.Ctx, lom, r.Kind() == cmn.ActEvictObjects)
	if errCode == http.StatusNotFound {
		return nil
	}
	if err != nil {
		if cmn.IsObjNotExist(err) || cmn.IsStatusNotFound(err) {
			return nil
		}
		return err
	}
	r.ObjectsInc()
	r.BytesAdd(lom.SizeBytes(true)) // was loaded and evicted
	return nil
}

func (r *evictDelete) listOperation(args *xreg.ListRangeArgs, listMsg *cmn.ListMsg) error {
	return r.iterateList(args, listMsg, r.doObjEvictDelete)
}

func (r *evictDelete) iterateBucketRange(args *xreg.ListRangeArgs) error {
	return r.iterateRange(args, r.doObjEvictDelete)
}

//////////////
// prefetch //
//////////////

func (*PrfchFactory) New(args *xreg.XactArgs) xreg.BucketEntry {
	return &PrfchFactory{
		t:    args.T,
		args: args.Custom.(*xreg.ListRangeArgs),
	}
}

func (p *PrfchFactory) Start(bck cmn.Bck) error {
	p.xact = newPrefetch(p.args.UUID, p.Kind(), bck, p.t, p.args)
	return nil
}

func (*PrfchFactory) Kind() string        { return cmn.ActPrefetch }
func (p *PrfchFactory) Get() cluster.Xact { return p.xact }

func newPrefetch(uuid, kind string, bck cmn.Bck, t cluster.Target, dpargs *xreg.ListRangeArgs) (prf *prefetch) {
	xargs := xaction.Args{ID: xaction.BaseID(uuid), Kind: kind, Bck: &bck}
	prf = &prefetch{
		XactBase: *xaction.NewXactBase(xargs),
		_lrBase:  _lrBase{t: t, args: dpargs},
	}
	prf._lrBase.xact = prf
	return
}

func (r *prefetch) Run() {
	var err error
	if r.args.RangeMsg != nil {
		err = r.iterateBucketRange(r.args)
	} else {
		err = r.listOperation(r.args, r.args.ListMsg)
	}
	r.Finish(err)
}

func (r *prefetch) prefetchMissing(args *xreg.ListRangeArgs, objName string) error {
	lom := &cluster.LOM{ObjName: objName}
	err := lom.Init(r.xact.Bck())
	if err != nil {
		return err
	}
	var coldGet bool
	if err = lom.Load(true /*cache it*/, false /*locked*/); err != nil {
		coldGet = cmn.IsErrObjNought(err)
		if !coldGet {
			return err
		}
	}
	if lom.Bck().IsAIS() { // must not come here
		if coldGet {
			glog.Errorf("prefetch: %s", lom)
		}
		return nil
	}
	if !coldGet && lom.Version() != "" && lom.VersionConf().ValidateWarmGet {
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
	if _, err = r.t.GetCold(args.Ctx, lom, cluster.Prefetch); err != nil {
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

func (r *prefetch) listOperation(args *xreg.ListRangeArgs, listMsg *cmn.ListMsg) error {
	return r.iterateList(args, listMsg, r.prefetchMissing)
}

func (r *prefetch) iterateBucketRange(args *xreg.ListRangeArgs) error {
	return r.iterateRange(args, r.prefetchMissing)
}

//
// helpers
//

// Try to parse string as template:
// 1. As bash-style: `file-{0..100}`
// 2. As at-style: `file-@100`
// 3. Falls back to just a prefix without number ranges
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

func isLocalObject(smap *cluster.Smap, b cmn.Bck, objName, sid string) (bool, error) {
	bck := cluster.NewBckEmbed(b)
	si, err := cluster.HrwTarget(bck.MakeUname(objName), smap)
	if err != nil {
		return false, err
	}
	return si.ID() == sid, nil
}

func (r *_lrBase) iterateRange(args *xreg.ListRangeArgs, cb objCallback) error {
	debug.Assert(args.RangeMsg != nil)
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

func (r *_lrBase) iterateTemplate(args *xreg.ListRangeArgs, smap *cluster.Smap, pt *cos.ParsedTemplate,
	cb objCallback) error {
	var (
		getNext = pt.Iter()
		sid     = r.t.SID()
	)
	for objName, hasNext := getNext(); !r.xact.Aborted() && hasNext; objName, hasNext = getNext() {
		if r.xact.Aborted() {
			return nil
		}
		local, err := isLocalObject(smap, r.xact.Bck(), objName, sid)
		if err != nil {
			return err
		}
		if !local {
			continue
		}
		if err := cb(args, objName); err != nil {
			return err
		}
	}
	return nil
}

func (r *_lrBase) iteratePrefix(args *xreg.ListRangeArgs, smap *cluster.Smap, prefix string, cb objCallback) error {
	var (
		objList *cmn.BucketList
		sid     = r.t.SID()
		err     error
	)
	bck := cluster.NewBckEmbed(r.xact.Bck())
	if err := bck.Init(r.t.Bowner()); err != nil {
		return err
	}
	msg := &cmn.SelectMsg{Prefix: prefix, Props: cmn.GetPropsStatus}
	for !r.xact.Aborted() {
		if bck.IsAIS() {
			walk := objwalk.NewWalk(args.Ctx, r.t, bck, msg)
			objList, err = walk.DefaultLocalObjPage(msg)
		} else {
			objList, _, err = r.t.Backend(bck).ListObjects(args.Ctx, bck, msg)
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
			if bck.IsRemote() {
				local, err := isLocalObject(smap, r.xact.Bck(), be.Name, sid)
				if err != nil {
					return err
				}
				if !local {
					continue
				}
			}

			if err := cb(args, be.Name); err != nil {
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

func (r *_lrBase) iterateList(args *xreg.ListRangeArgs, listMsg *cmn.ListMsg, cb objCallback) error {
	var (
		smap = r.t.Sowner().Get()
		sid  = r.t.SID()
	)
	for _, obj := range listMsg.ObjNames {
		if r.xact.Aborted() {
			break
		}
		local, err := isLocalObject(smap, r.xact.Bck(), obj, sid)
		if err != nil {
			return err
		}
		if !local {
			continue
		}
		if err := cb(args, obj); err != nil {
			return err
		}
	}
	return nil
}
