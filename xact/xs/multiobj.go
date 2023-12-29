// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// Assorted multi-object (list/range templated) xactions: evict, delete, prefetch multiple objects
//
// Supported range syntax includes:
//   1. bash-extension style: `file-{0..100}`
//   2. at-style: `file-@100`
//   3. if none of the above, fall back to just prefix matching

const (
	lrpList = iota + 1
	lrpRange
	lrpPrefix
)

// common for all list-range
type (
	// one multi-object operation work item
	lrwi interface {
		do(*core.LOM, *lriterator)
	}
	// a strict subset of core.Xact, includes only the methods
	// lriterator needs for itself
	lrxact interface {
		Bck() *meta.Bck
		IsAborted() bool
		Finished() bool
	}
	// common multi-obj operation context and iterList()/iterRangeOrPref() logic
	lriterator struct {
		xctn lrxact
		msg  *apc.ListRange
		lrp  int // { lrpList, ... } enum
	}
)

// concrete list-range type xactions (see also: archive.go)
type (
	evdFactory struct {
		xreg.RenewBase
		xctn *evictDelete
		msg  *apc.ListRange
		kind string
	}
	evictDelete struct {
		lriterator
		xact.Base
		config *cmn.Config
	}
	prfFactory struct {
		xreg.RenewBase
		xctn *prefetch
		msg  *apc.ListRange
	}
	prefetch struct {
		lriterator
		xact.Base
		config *cmn.Config
	}

	TestXFactory struct{ prfFactory } // tests only
)

// interface guard
var (
	_ core.Xact = (*evictDelete)(nil)
	_ core.Xact = (*prefetch)(nil)

	_ xreg.Renewable = (*evdFactory)(nil)
	_ xreg.Renewable = (*prfFactory)(nil)

	_ lrwi = (*evictDelete)(nil)
	_ lrwi = (*prefetch)(nil)
)

////////////////
// lriterator //
////////////////

func (r *lriterator) init(xctn lrxact, msg *apc.ListRange) {
	r.xctn = xctn
	r.msg = msg
}

func (r *lriterator) rangeOrPref(wi lrwi, smap *meta.Smap) error {
	pt, err := cos.NewParsedTemplate(r.msg.Template)
	if err != nil {
		// NOTE: [making an exception] treating an empty or '*' template
		// as an empty prefix to facilitate scope 'all-objects'
		// (e.g., "archive entire bucket as a single shard (caution!)
		if err == cos.ErrEmptyTemplate {
			pt.Prefix = ""
			goto pref
		}
		return err
	}
	if err := cmn.ValidatePrefix(pt.Prefix); err != nil {
		nlog.Errorln(err)
		return err
	}
	if len(pt.Ranges) != 0 {
		r.lrp = lrpRange
		return r.iterRange(smap, &pt, wi)
	}
pref:
	r.lrp = lrpPrefix
	return r.iteratePrefix(smap, pt.Prefix, wi)
}

func (r *lriterator) iterRange(smap *meta.Smap, pt *cos.ParsedTemplate, wi lrwi) error {
	pt.InitIter()
	for objName, hasNext := pt.Next(); hasNext; objName, hasNext = pt.Next() {
		if r.xctn.IsAborted() || r.xctn.Finished() {
			return nil
		}
		lom := core.AllocLOM(objName)
		err := r.do(lom, wi, smap)
		core.FreeLOM(lom)
		if err != nil {
			return err
		}
	}
	return nil
}

// compare with ais/plstcx.go (TODO: unify)
func (r *lriterator) iteratePrefix(smap *meta.Smap, prefix string, wi lrwi) error {
	if err := cmn.ValidatePrefix(prefix); err != nil {
		return err
	}
	var (
		err     error
		lst     *cmn.LsoResult
		msg     = &apc.LsoMsg{Prefix: prefix, Props: apc.GetPropsStatus}
		bck     = r.xctn.Bck()
		npg     = newNpgCtx(bck, msg, noopCb)
		bremote = bck.IsRemote()
	)
	if err := bck.Init(core.T.Bowner()); err != nil {
		return err
	}
	if !bremote {
		smap = nil // not needed
	}
	for {
		if r.xctn.IsAborted() || r.xctn.Finished() {
			break
		}
		if bremote {
			lst = &cmn.LsoResult{Entries: allocLsoEntries()}
			_, err = core.T.Backend(bck).ListObjects(bck, msg, lst) // (TODO comment above)
			if err != nil {
				freeLsoEntries(lst.Entries)
			}
		} else {
			npg.page.Entries = allocLsoEntries()
			err = npg.nextPageA()
			lst = &npg.page
		}
		if err != nil {
			return err
		}
		for _, be := range lst.Entries {
			if !be.IsStatusOK() {
				continue
			}
			if r.xctn.IsAborted() || r.xctn.Finished() {
				freeLsoEntries(lst.Entries)
				return nil
			}
			lom := core.AllocLOM(be.Name)
			err := r.do(lom, wi, smap)
			core.FreeLOM(lom)
			if err != nil {
				freeLsoEntries(lst.Entries)
				return err
			}
		}
		freeLsoEntries(lst.Entries)
		// last page listed
		if lst.ContinuationToken == "" {
			break
		}
		// token for the next page
		msg.ContinuationToken = lst.ContinuationToken
	}
	return nil
}

func (r *lriterator) iterList(wi lrwi, smap *meta.Smap) error {
	r.lrp = lrpList
	for _, objName := range r.msg.ObjNames {
		if r.xctn.IsAborted() || r.xctn.Finished() {
			break
		}
		lom := core.AllocLOM(objName)
		err := r.do(lom, wi, smap)
		core.FreeLOM(lom)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *lriterator) do(lom *core.LOM, wi lrwi, smap *meta.Smap) error {
	if err := lom.InitBck(r.xctn.Bck().Bucket()); err != nil {
		return err
	}
	if smap != nil {
		_, local, err := lom.HrwTarget(smap)
		if err != nil {
			return err
		}
		if !local {
			return nil
		}
	}
	// NOTE: lom is alloc-ed prior to the call and freed upon return
	wi.do(lom, r)
	return nil
}

//////////////////
// evict/delete //
//////////////////

func (p *evdFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	msg := args.Custom.(*apc.ListRange)
	debug.Assert(!msg.IsList() || !msg.HasTemplate())
	np := &evdFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, kind: p.kind, msg: msg}
	return np
}

func (p *evdFactory) Start() error {
	p.xctn = newEvictDelete(&p.Args, p.kind, p.Bck, p.msg)
	return nil
}

func (p *evdFactory) Kind() string   { return p.kind }
func (p *evdFactory) Get() core.Xact { return p.xctn }

func (*evdFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprKeepAndStartNew, nil
}

func newEvictDelete(xargs *xreg.Args, kind string, bck *meta.Bck, msg *apc.ListRange) (ed *evictDelete) {
	ed = &evictDelete{config: cmn.GCO.Get()}
	ed.lriterator.init(ed, msg)
	ed.InitBase(xargs.UUID, kind, bck)
	return
}

func (r *evictDelete) Run(*sync.WaitGroup) {
	smap := core.T.Sowner().Get()
	if r.msg.IsList() {
		_ = r.iterList(r, smap)
	} else {
		_ = r.rangeOrPref(r, smap)
	}
	r.Finish()
}

func (r *evictDelete) do(lom *core.LOM, lrit *lriterator) {
	errCode, err := core.T.DeleteObject(lom, r.Kind() == apc.ActEvictObjects)
	if err == nil { // done
		r.ObjsAdd(1, lom.SizeBytes(true))
		return
	}
	if errCode == http.StatusNotFound || cmn.IsErrObjNought(err) {
		if lrit.lrp == lrpList {
			goto eret // unlike range and prefix
		}
		return
	}
eret:
	r.AddErr(err)
	if r.config.FastV(5, cos.SmoduleXs) {
		nlog.Warningln(err)
	}
}

func (r *evictDelete) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	return
}

//////////////
// prefetch //
//////////////

func (*prfFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	msg := args.Custom.(*apc.ListRange)
	debug.Assert(!msg.IsList() || !msg.HasTemplate())
	np := &prfFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, msg: msg}
	return np
}

func (p *prfFactory) Start() error {
	b := p.Bck
	if err := b.Init(core.T.Bowner()); err != nil {
		if !cmn.IsErrRemoteBckNotFound(err) {
			return err
		}
		nlog.Warningln(err) // may show up later via ais/prxtrybck.go logic
	} else if b.IsAIS() {
		nlog.Errorf("bucket %q: can only prefetch remote buckets", b)
		return fmt.Errorf("bucket %q: can only prefetch remote buckets", b)
	}
	p.xctn = newPrefetch(&p.Args, p.Kind(), p.Bck, p.msg)
	return nil
}

func (*prfFactory) Kind() string     { return apc.ActPrefetchObjects }
func (p *prfFactory) Get() core.Xact { return p.xctn }

func (*prfFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprKeepAndStartNew, nil
}

func newPrefetch(xargs *xreg.Args, kind string, bck *meta.Bck, msg *apc.ListRange) (prf *prefetch) {
	prf = &prefetch{config: cmn.GCO.Get()}
	prf.lriterator.init(prf, msg)
	prf.InitBase(xargs.UUID, kind, bck)
	prf.lriterator.xctn = prf
	return
}

func (r *prefetch) Run(*sync.WaitGroup) {
	smap := core.T.Sowner().Get()
	if r.msg.IsList() {
		_ = r.iterList(r, smap)
	} else {
		_ = r.rangeOrPref(r, smap)
	}
	r.Finish()
}

func (r *prefetch) do(lom *core.LOM, lrit *lriterator) {
	var (
		err     error
		errCode int
	)
	if err = lom.Load(true /*cache it*/, false /*locked*/); err != nil {
		if !cmn.IsErrObjNought(err) {
			goto eret
		}
	} else {
		if !lom.VersionConf().ValidateWarmGet {
			return // nothing to do
		}
		var eq bool
		if eq, errCode, err = lom.CompareRemoteMD(); eq {
			return // nothing to do
		}
		if err != nil {
			goto emaybe
		}
	}

	// NOTE minimal locking, optimistic concurrency
	// Not setting atime (a.k.a. access time) as prefetching != actual access.
	//
	// On the other hand, zero atime makes the object's lifespan in the cache too short - the first
	// housekeeping traversal will remove it. Using neative `-now` value for subsequent correction
	// (see cluster/lom_cache_hk.go).

	lom.SetAtimeUnix(-time.Now().UnixNano())
	errCode, err = core.T.GetCold(context.Background(), lom, cmn.OwtGetPrefetchLock)
	if err == nil { // done
		r.ObjsAdd(1, lom.SizeBytes())
		return
	}

emaybe:
	if errCode == http.StatusNotFound || cmn.IsErrObjNought(err) {
		if lrit.lrp == lrpList {
			goto eret // listing is explicit
		}
		return
	}
	if err == cmn.ErrSkip {
		return
	}
eret:
	r.AddErr(err)
	if r.config.FastV(4, cos.SmoduleXs) {
		nlog.Warningln(err)
	}
}

func (r *prefetch) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	return
}
