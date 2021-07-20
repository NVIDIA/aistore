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
	// one multi-object operation work item
	lrwi interface {
		do(*cluster.LOM, *lriterator) error
	}
	// two selected methods that lriterator needs for itself (a strict subset of cluster.Xact)
	lrxact interface {
		Bck() *cluster.Bck
		Aborted() bool
	}
	// common mult-obj operation context
	// common iterateList()/iterateRange() logic
	lriterator struct {
		xact             lrxact
		t                cluster.Target
		ctx              context.Context
		msg              *cmn.ListRangeMsg
		ignoreBackendErr bool // ignore backend API errors
		freeLOM          bool // free LOM upon return from lriterator.do()
	}
)

// concrete list-range type xactions (see also: archive.go)
type (
	evdFactory struct {
		xreg.BaseEntry
		xargs xreg.Args
		xact  *evictDelete
		kind  string
		msg   *cmn.ListRangeMsg
	}
	evictDelete struct {
		xaction.XactBase
		lriterator
	}
	prfFactory struct {
		xreg.BaseEntry
		xargs xreg.Args
		xact  *prefetch
		msg   *cmn.ListRangeMsg
	}
	prefetch struct {
		xaction.XactBase
		lriterator
	}
	TestXFactory struct{ prfFactory } // tests only
)

// interface guard
var (
	_ cluster.Xact = (*evictDelete)(nil)
	_ cluster.Xact = (*prefetch)(nil)

	_ xreg.Factory = (*evdFactory)(nil)
	_ xreg.Factory = (*prfFactory)(nil)

	_ lrwi = (*evictDelete)(nil)
	_ lrwi = (*prefetch)(nil)
)

////////////////
// lriterator //
////////////////

func (r *lriterator) init(xact lrxact, t cluster.Target, msg *cmn.ListRangeMsg, ignoreBackendErr, freeLOM bool) {
	r.xact = xact
	r.t = t
	r.ctx = context.Background()
	r.msg = msg
	r.ignoreBackendErr = ignoreBackendErr
	r.freeLOM = freeLOM
}

func (r *lriterator) iterateRange(wi lrwi, smap *cluster.Smap) error {
	pt, err := parseTemplate(r.msg.Template)
	if err != nil {
		return err
	}
	if len(pt.Ranges) != 0 {
		return r.iterateTemplate(smap, &pt, wi)
	}
	return r.iteratePrefix(smap, pt.Prefix, wi)
}

func (r *lriterator) iterateTemplate(smap *cluster.Smap, pt *cos.ParsedTemplate, wi lrwi) error {
	getNext := pt.Iter()
	for objName, hasNext := getNext(); hasNext; objName, hasNext = getNext() {
		if r.xact.Aborted() {
			return nil
		}
		lom := cluster.AllocLOM(objName)
		err := r.do(lom, wi, smap)
		if err != nil {
			cluster.FreeLOM(lom)
			return err
		}
		if r.freeLOM {
			cluster.FreeLOM(lom)
		}
	}
	return nil
}

func (r *lriterator) iteratePrefix(smap *cluster.Smap, prefix string, wi lrwi) error {
	var (
		objList *cmn.BucketList
		err     error
		bck     = r.xact.Bck()
	)
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
			err := r.do(lom, wi, smap)
			if err != nil {
				cluster.FreeLOM(lom)
				return err
			}
			if r.freeLOM {
				cluster.FreeLOM(lom)
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

func (r *lriterator) iterateList(wi lrwi, smap *cluster.Smap) error {
	for _, objName := range r.msg.ObjNames {
		if r.xact.Aborted() {
			break
		}
		lom := cluster.AllocLOM(objName)
		err := r.do(lom, wi, smap)
		if err != nil {
			cluster.FreeLOM(lom)
			return err
		}
		if r.freeLOM {
			cluster.FreeLOM(lom)
		}
	}
	return nil
}

func (r *lriterator) do(lom *cluster.LOM, wi lrwi, smap *cluster.Smap) error {
	if err := lom.Init(r.xact.Bck().Bck); err != nil {
		return err
	}
	if smap != nil {
		// NOTE: checking cluster locality to speed up iterating - trading off
		// rebalancing for performance (can be configurable).
		_, local, err := lom.HrwTarget(smap)
		if err != nil {
			return err
		}
		if !local {
			return nil
		}
	}
	return wi.do(lom, r)
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

//////////////////
// evict/delete //
//////////////////

func (p *evdFactory) New(args xreg.Args, bck *cluster.Bck) xreg.Renewable {
	msg := args.Custom.(*cmn.ListRangeMsg)
	debug.Assert(!msg.IsList() || !msg.HasTemplate())
	np := &evdFactory{xargs: args, kind: p.kind, msg: msg}
	np.Bck = bck
	return np
}

func (p *evdFactory) Start() error {
	p.xact = newEvictDelete(&p.xargs, p.kind, p.Bck, p.msg)
	return nil
}

func (p *evdFactory) Kind() string      { return p.kind }
func (p *evdFactory) Get() cluster.Xact { return p.xact }

func newEvictDelete(xargs *xreg.Args, kind string, bck *cluster.Bck, msg *cmn.ListRangeMsg) (ed *evictDelete) {
	ed = &evictDelete{}
	// NOTE: list defaults to aborting on errors other than non-existence
	ed.lriterator.init(ed, xargs.T, msg, !msg.IsList() /*ignoreBackendErr*/, true /*freeLOM*/)
	ed.InitBase(xargs.UUID, kind, bck)
	return
}

func (r *evictDelete) Run() {
	var (
		err  error
		smap = r.t.Sowner().Get()
	)
	if r.msg.IsList() {
		err = r.iterateList(r, smap)
	} else {
		err = r.iterateRange(r, smap)
	}
	r.Finish(err)
}

func (r *evictDelete) do(lom *cluster.LOM, _ *lriterator) error {
	errCode, err := r.t.DeleteObject(lom, r.Kind() == cmn.ActEvictObjects)
	if errCode == http.StatusNotFound {
		return nil
	}
	if err != nil {
		if cmn.IsErrObjNought(err) {
			err = nil
		} else if r.ignoreBackendErr {
			glog.Warning(err)
			err = nil
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

func (*prfFactory) New(args xreg.Args, bck *cluster.Bck) xreg.Renewable {
	msg := args.Custom.(*cmn.ListRangeMsg)
	debug.Assert(!msg.IsList() || !msg.HasTemplate())
	np := &prfFactory{xargs: args, msg: msg}
	np.Bck = bck
	return np
}

func (p *prfFactory) Start() error {
	b := cluster.NewBckEmbed(p.Bck.Bck)
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
	p.xact = newPrefetch(&p.xargs, p.Kind(), p.Bck, p.msg)
	return nil
}

func (*prfFactory) Kind() string        { return cmn.ActPrefetch }
func (p *prfFactory) Get() cluster.Xact { return p.xact }

func newPrefetch(xargs *xreg.Args, kind string, bck *cluster.Bck, msg *cmn.ListRangeMsg) (prf *prefetch) {
	prf = &prefetch{}
	// NOTE: list defaults to aborting on errors other than non-existence
	prf.lriterator.init(prf, xargs.T, msg, !msg.IsList() /*ignoreBackendErr*/, true /*freeLOM*/)
	prf.InitBase(xargs.UUID, kind, bck)
	prf.lriterator.xact = prf
	return
}

func (r *prefetch) Run() {
	var (
		err  error
		smap = r.t.Sowner().Get()
	)
	if r.msg.IsList() {
		err = r.iterateList(r, smap)
	} else {
		err = r.iterateRange(r, smap)
	}
	r.Finish(err)
}

func (r *prefetch) do(lom *cluster.LOM, _ *lriterator) error {
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
			if r.ignoreBackendErr {
				glog.Warning(err)
				err = nil
			}
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
		if err == cmn.ErrSkip {
			err = nil
		} else if r.ignoreBackendErr {
			glog.Warning(err)
			err = nil
		}
		return err
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("prefetch: %s", lom)
	}
	r.ObjectsInc()
	r.BytesAdd(lom.SizeBytes())
	return nil
}
