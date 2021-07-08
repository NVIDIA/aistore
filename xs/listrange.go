// Package xs contains eXtended actions (xactions) except storage services
// (mirror, ec) and extensions (downloader, lru).
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"context"
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

// common for all list-range
type (
	lrwi interface { // list-range work item
		do(*cluster.LOM) error
	}
	lrxact interface { // two selected methods that lrbase needs for itself
		Bck() cmn.Bck
		Aborted() bool
	}
	lrbase struct {
		xact lrxact
		t    cluster.Target
		ctx  context.Context
		msg  *cmn.ListRangeMsg
	}
)

// concrete list-range type xactions (see also: archive.go)
type (
	evdFactory struct {
		xreg.BaseBckEntry
		xargs xreg.Args
		xact  *evictDelete
		kind  string
		msg   *cmn.ListRangeMsg
	}
	evictDelete struct {
		xaction.XactBase
		lrbase
	}
	prfFactory struct {
		xreg.BaseBckEntry
		xargs xreg.Args
		xact  *prefetch
		msg   *cmn.ListRangeMsg
	}
	prefetch struct {
		xaction.XactBase
		lrbase
	}
	TestXFactory struct{ prfFactory } // tests only
)

// interface guard
var (
	_ cluster.Xact = (*evictDelete)(nil)
	_ cluster.Xact = (*prefetch)(nil)

	_ xreg.BckFactory = (*evdFactory)(nil)
	_ xreg.BckFactory = (*prfFactory)(nil)

	_ lrwi = (*evictDelete)(nil)
	_ lrwi = (*prefetch)(nil)
)

//////////////////
// evict/delete //
//////////////////

func (p *evdFactory) New(args *xreg.Args) xreg.BucketEntry {
	msg := args.Custom.(*cmn.ListRangeMsg)
	debug.Assert(!msg.IsList() || !msg.HasTemplate())
	return &evdFactory{xargs: *args, kind: p.kind, msg: msg}
}

func (p *evdFactory) Start(bck cmn.Bck) error {
	p.xact = newEvictDelete(&p.xargs, p.kind, bck, p.msg)
	return nil
}

func (p *evdFactory) Kind() string      { return p.kind }
func (p *evdFactory) Get() cluster.Xact { return p.xact }

func newEvictDelete(xargs *xreg.Args, kind string, bck cmn.Bck, msg *cmn.ListRangeMsg) (ed *evictDelete) {
	ed = &evictDelete{
		lrbase: lrbase{t: xargs.T, ctx: context.Background(), msg: msg},
	}
	ed.InitBase(xargs.UUID, kind, &bck)
	ed.lrbase.xact = ed
	return
}

func (r *evictDelete) Run() {
	var err error
	if r.msg.IsList() {
		err = r.iterateList(r)
	} else {
		err = r.iterateRange(r)
	}
	r.Finish(err)
}

func (r *evictDelete) do(lom *cluster.LOM) error {
	errCode, err := r.t.DeleteObject(lom, r.Kind() == cmn.ActEvictObjects)
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

func (*prfFactory) New(args *xreg.Args) xreg.BucketEntry {
	msg := args.Custom.(*cmn.ListRangeMsg)
	debug.Assert(!msg.IsList() || !msg.HasTemplate())
	return &prfFactory{xargs: *args, msg: msg}
}

func (p *prfFactory) Start(bck cmn.Bck) error {
	b := cluster.NewBckEmbed(bck)
	if err := b.Init(p.xargs.T.Bowner()); err != nil {
		if cmn.IsErrRemoteBckNotFound(err) {
			glog.Warning(err) // may show up later via ais/prxtrybck.go logic
		} else {
			return err
		}
	} else if b.IsAIS() {
		glog.Errorf("bucket %q: can only prefetch remote buckets", b)
		return fmt.Errorf("bucket %q: can only prefetch remote buckets", b)
	}
	p.xact = newPrefetch(&p.xargs, p.Kind(), bck, p.msg)
	return nil
}

func (*prfFactory) Kind() string        { return cmn.ActPrefetch }
func (p *prfFactory) Get() cluster.Xact { return p.xact }

func newPrefetch(xargs *xreg.Args, kind string, bck cmn.Bck, msg *cmn.ListRangeMsg) (prf *prefetch) {
	prf = &prefetch{
		lrbase: lrbase{t: xargs.T, ctx: context.Background(), msg: msg},
	}
	prf.InitBase(xargs.UUID, kind, &bck)
	prf.lrbase.xact = prf
	return
}

func (r *prefetch) Run() {
	var err error
	if r.msg.IsList() {
		err = r.iterateList(r)
	} else {
		err = r.iterateRange(r)
	}
	r.Finish(err)
}

func (r *prefetch) do(lom *cluster.LOM) error {
	var coldGet bool
	if err := lom.Load(true /*cache it*/, false /*locked*/); err != nil {
		coldGet = cmn.IsObjNotExist(err)
		if !coldGet {
			return err
		}
	}
	if !coldGet && lom.Version() != "" && lom.VersionConf().ValidateWarmGet {
		var err error
		if coldGet, _, err = r.t.CheckRemoteVersion(r.ctx, lom); err != nil {
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
	if _, err := r.t.GetCold(r.ctx, lom, cluster.Prefetch); err != nil {
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

func (r *lrbase) iterateRange(wi lrwi) error {
	pt, err := parseTemplate(r.msg.Template)
	if err != nil {
		return err
	}
	smap := r.t.Sowner().Get()
	if len(pt.Ranges) != 0 {
		return r.iterateTemplate(smap, &pt, wi)
	}
	return r.iteratePrefix(smap, pt.Prefix, wi)
}

func (r *lrbase) iterateTemplate(smap *cluster.Smap, pt *cos.ParsedTemplate, wi lrwi) error {
	getNext := pt.Iter()
	for objName, hasNext := getNext(); hasNext; objName, hasNext = getNext() {
		if r.xact.Aborted() {
			return nil
		}
		lom := cluster.AllocLOM(objName)
		err := r.iterate(lom, wi, smap)
		cluster.FreeLOM(lom)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *lrbase) iteratePrefix(smap *cluster.Smap, prefix string, wi lrwi) error {
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
			objList, _, err = r.t.Backend(bck).ListObjects(bck, msg)
		} else {
			walk := objwalk.NewWalk(r.ctx, r.t, bck, msg)
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
			err := r.iterate(lom, wi, smap)
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

func (r *lrbase) iterateList(wi lrwi) error {
	smap := r.t.Sowner().Get()
	for _, objName := range r.msg.ObjNames {
		if r.xact.Aborted() {
			break
		}
		lom := cluster.AllocLOM(objName)
		err := r.iterate(lom, wi, smap)
		cluster.FreeLOM(lom)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *lrbase) iterate(lom *cluster.LOM, wi lrwi, smap *cluster.Smap) error {
	if err := lom.Init(r.xact.Bck()); err != nil {
		return err
	}
	if smap != nil {
		// NOTE: checking locality here to speed up iterating, trading off
		//       rebalancing use case for performance - TODO: add
		//       configurable option to lom.Load() instead.
		_, local, err := lom.HrwTarget(smap)
		if err != nil {
			return err
		}
		if !local {
			return nil
		}
	}
	return wi.do(lom)
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
