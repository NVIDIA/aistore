// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
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
