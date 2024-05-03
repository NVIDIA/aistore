// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// Assorted multi-object (list/range templated) xactions: evict, delete, prefetch multiple objects
//
// Supported range syntax includes:
//   1. bash-extension style: `file-{0..100}`
//   2. at-style: `file-@100`
//   3. if none of the above, fall back to just prefix matching

// TODO:
// - user-assigned (configurable) num-workers
// - jogger(s) per mountpath type concurrency

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

	// running concurrency
	lrpair struct {
		lom *core.LOM
		wi  lrwi
	}
	lrworker struct {
		lrit *lriterator
	}

	// common multi-object operation context and list|range|prefix logic
	lriterator struct {
		parent lrxact
		msg    *apc.ListRange
		bck    *meta.Bck
		pt     *cos.ParsedTemplate
		prefix string
		lrp    int // { lrpList, ... } enum

		// running concurrency
		workCh  chan lrpair
		workers []*lrworker
		wg      sync.WaitGroup
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

func (r *lriterator) init(xctn lrxact, msg *apc.ListRange, bck *meta.Bck, blocking ...bool) error {
	avail := fs.GetAvail()
	l := len(avail)
	if l == 0 {
		return cmn.ErrNoMountpaths
	}
	r.parent = xctn
	r.msg = msg
	r.bck = bck

	// list is the simplest and always single-threaded
	if msg.IsList() {
		r.lrp = lrpList
		return nil
	}
	if err := r._inipr(msg); err != nil {
		return err
	}
	if l == 1 {
		return nil
	}
	if len(blocking) > 0 && blocking[0] {
		return nil
	}

	// num-workers == num-mountpaths but note:
	// these are not _joggers_
	r.workers = make([]*lrworker, 0, l)
	for range avail {
		r.workers = append(r.workers, &lrworker{r})
	}
	r.workCh = make(chan lrpair, l)
	return nil
}

// [NOTE] treating an empty ("") or wildcard ('*') template
// as an empty prefix, to facilitate all-objects scope, e.g.:
// - "copy entire source bucket", or even
// - "archive entire bucket as a single shard" (caution!)

func (r *lriterator) _inipr(msg *apc.ListRange) error {
	pt, err := cos.NewParsedTemplate(msg.Template)
	if err != nil {
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
	for _, worker := range r.workers {
		r.wg.Add(1)
		go worker.run()
	}
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

func (r *lriterator) wait() {
	if r.workers == nil {
		return
	}
	close(r.workCh)
	r.wg.Wait()
}

func (r *lriterator) done() bool { return r.parent.IsAborted() || r.parent.Finished() }

func (r *lriterator) _list(wi lrwi, smap *meta.Smap) error {
	r.lrp = lrpList
	for _, objName := range r.msg.ObjNames {
		if r.done() {
			break
		}
		lom := core.AllocLOM(objName)
		done, err := r.do(lom, wi, smap)
		if err != nil {
			core.FreeLOM(lom)
			return err
		}
		if done {
			core.FreeLOM(lom)
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
		done, err := r.do(lom, wi, smap)
		if err != nil {
			core.FreeLOM(lom)
			return err
		}
		if done {
			core.FreeLOM(lom)
		}
	}
	return nil
}

// (compare with ais/plstcx)
func (r *lriterator) _prefix(wi lrwi, smap *meta.Smap) error {
	var (
		err     error
		ecode   int
		lst     *cmn.LsoRes
		msg     = &apc.LsoMsg{Prefix: r.prefix, Props: apc.GetPropsStatus}
		npg     = newNpgCtx(r.bck, msg, noopCb, nil /*core.LsoInvCtx bucket inventory*/)
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
			lst = &cmn.LsoRes{Entries: allocLsoEntries()}
			ecode, err = core.T.Backend(r.bck).ListObjects(r.bck, msg, lst) // (TODO comment above)
		} else {
			npg.page.Entries = allocLsoEntries()
			err = npg.nextPageA()
			lst = &npg.page
		}
		if err != nil {
			nlog.Errorln(core.T.String()+":", err, ecode)
			freeLsoEntries(lst.Entries)
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
			done, err := r.do(lom, wi, smap)
			if err != nil {
				core.FreeLOM(lom)
				freeLsoEntries(lst.Entries)
				return err
			}
			if done {
				core.FreeLOM(lom)
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

func (r *lriterator) do(lom *core.LOM, wi lrwi, smap *meta.Smap) (bool /*this lom done*/, error) {
	if err := lom.InitBck(r.bck.Bucket()); err != nil {
		return false, err
	}
	// (smap != nil) to filter non-locals
	if smap != nil {
		_, local, err := lom.HrwTarget(smap)
		if err != nil {
			return false, err
		}
		if !local {
			return true, nil
		}
	}

	if r.workers == nil {
		wi.do(lom, r)
		return true, nil
	}
	r.workCh <- lrpair{lom, wi} // lom eventually freed below
	return false, nil
}

//////////////
// lrworker //
//////////////

func (worker *lrworker) run() {
	for {
		lrpair, ok := <-worker.lrit.workCh
		if !ok {
			break
		}
		lrpair.wi.do(lrpair.lom, worker.lrit)
		core.FreeLOM(lrpair.lom)
		if worker.lrit.parent.IsAborted() {
			break
		}
	}
	worker.lrit.wg.Done()
}
