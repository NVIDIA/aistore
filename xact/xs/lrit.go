// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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
		IsAborted() bool
		Finished() bool
	}
	// common multi-obj operation context and iterList()/iterRangeOrPref() logic
	lriterator struct {
		parent lrxact
		msg    *apc.ListRange
		bck    *meta.Bck
		pt     *cos.ParsedTemplate
		prefix string
		lrp    int // { lrpList, ... } enum
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

func (r *lriterator) init(xctn lrxact, msg *apc.ListRange, bck *meta.Bck) error {
	r.parent = xctn
	r.msg = msg
	r.bck = bck
	if msg.IsList() {
		r.lrp = lrpList
		return nil
	}

	pt, err := cos.NewParsedTemplate(msg.Template)
	if err != nil {
		// [Making Exception] treating an empty or '*' template
		// as an empty prefix, to facilitate all-objects scope
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
		r.pt = &pt
		r.lrp = lrpRange
		return nil
	}
pref:
	r.prefix = pt.Prefix
	r.lrp = lrpPrefix
	return nil
}

func (r *lriterator) run(wi lrwi, smap *meta.Smap) (err error) {
	switch r.lrp {
	case lrpList:
		err = r._list(wi, smap)
	case lrpRange:
		err = r._range(wi, smap)
	case lrpPrefix:
		err = r._prefix(wi, smap)
	}
	return err
}

func (r *lriterator) done() bool { return r.parent.IsAborted() || r.parent.Finished() }

func (r *lriterator) _list(wi lrwi, smap *meta.Smap) error {
	r.lrp = lrpList
	for _, objName := range r.msg.ObjNames {
		if r.done() {
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

func (r *lriterator) _range(wi lrwi, smap *meta.Smap) error {
	r.pt.InitIter()
	for objName, hasNext := r.pt.Next(); hasNext; objName, hasNext = r.pt.Next() {
		if r.done() {
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

// (compare with ais/plstcx)
func (r *lriterator) _prefix(wi lrwi, smap *meta.Smap) error {
	var (
		err     error
		lst     *cmn.LsoResult
		msg     = &apc.LsoMsg{Prefix: r.prefix, Props: apc.GetPropsStatus}
		npg     = newNpgCtx(r.bck, msg, noopCb)
		bremote = r.bck.IsRemote()
	)
	if err := r.bck.Init(core.T.Bowner()); err != nil {
		return err
	}
	if !bremote {
		smap = nil // not needed
	}
	for {
		if r.done() {
			break
		}
		if bremote {
			var errCode int
			lst = &cmn.LsoResult{Entries: allocLsoEntries()}
			errCode, err = core.T.Backend(r.bck).ListObjects(r.bck, msg, lst) // (TODO comment above)
			if err != nil {
				freeLsoEntries(lst.Entries)
				if errCode == http.StatusNotFound && !cos.IsNotExist(err, 0) {
					debug.Assert(false, err, errCode) // TODO: remove
					err = cos.NewErrNotFound(nil, err.Error())
				}
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
			if r.done() {
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

// NOTE: (smap != nil) to filter non-locals
func (r *lriterator) do(lom *core.LOM, wi lrwi, smap *meta.Smap) error {
	if err := lom.InitBck(r.bck.Bucket()); err != nil {
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
