// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/sys"
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
		do(*core.LOM, *lrit, []byte)
	}
	// a strict subset of core.Xact, includes only the methods
	// lrit needs for itself
	lrxact interface {
		Abort(error) bool
		IsAborted() bool // used exclusively to break iteration
		Finished() bool  // ditto
		Name() string
		ChanAbort() <-chan error
	}

	// running concurrency
	lrpair struct {
		lom *core.LOM
		wi  lrwi
	}
	lrworker struct {
		lrit *lrit
	}

	// common multi-object operation context and list|range|prefix logic
	lrit struct {
		parent lrxact
		bck    *meta.Bck
		// [--- traverse
		msg     *apc.ListRange
		pt      *cos.ParsedTemplate
		prefix  string // bucket/prefix
		lsflags uint64 // assorted lsmsg flags (`LsNoRecursion`)
		// --- traverse]
		buf []byte // when (prealloc && no-workers)
		// nwp: num-workers parallelism
		// (these are _not_ joggers)
		nwp struct {
			workCh   chan lrpair
			workers  []*lrworker
			chanFull cos.ChanFull
			wg       sync.WaitGroup
		}
		lrp int // enum { lrpList, ... }
	}
)

//////////
// lrit //
//////////

func (r *lrit) init(xctn lrxact, msg *apc.ListRange, bck *meta.Bck, lsflags uint64, numWorkers, confBurst int) error {
	l := fs.NumAvail()
	if l == 0 {
		xctn.Abort(cmn.ErrNoMountpaths)
		return cmn.ErrNoMountpaths
	}
	r.parent = xctn
	r.msg = msg
	r.bck = bck
	r.lsflags = lsflags

	if msg.IsList() {
		debug.Assert(lsflags == 0, "not expecting 'lsflags' with list iterator: ", lsflags)
		r.lrp = lrpList
	} else {
		// init _range or _prefix
		if err := r._inipr(msg); err != nil {
			return err
		}
	}

	// run single-threaded
	if numWorkers == nwpNone {
		return nil
	}

	// tuneup number of concurrent workers (heuristic)
	if numWorkers == nwpDflt {
		numWorkers = l
	}
	if a := sys.MaxParallelism(); a > numWorkers+8 {
		var bump bool
		a <<= 1
		switch r.lrp {
		case lrpList:
			bump = len(msg.ObjNames) > a
		case lrpRange:
			bump = int(r.pt.Count()) > a
		case lrpPrefix:
			bump = true // err on that other side
		}
		if bump {
			numWorkers += 2
		}
	}
	numWorkers, err := throttleNwp(r.parent.Name(), numWorkers, l)
	if err != nil {
		return err
	}
	if numWorkers == nwpNone {
		return nil
	}
	debug.Assert(numWorkers > 0 && numWorkers < 9999, numWorkers)

	r._iniNwp(numWorkers, confBurst)
	return nil
}

func (r *lrit) _iniNwp(numWorkers, confBurst int) {
	r.nwp.workers = make([]*lrworker, 0, numWorkers)
	for range numWorkers {
		r.nwp.workers = append(r.nwp.workers, &lrworker{r})
	}

	// [burst] work channel capacity: up to 4 pending work items per
	r.nwp.workCh = make(chan lrpair, max(min(numWorkers*nwpBurstMult, nwpBurstMax), confBurst))
	nlog.Infoln(r.parent.Name(), "workers:", numWorkers)
}

// [NOTE] treating an empty ("") or wildcard ('*') template
// as an empty prefix, to facilitate all-objects scope, e.g.:
// - "copy entire source bucket", or even
// - "archive entire bucket as a single shard" (caution!)

func (r *lrit) _inipr(msg *apc.ListRange) error {
	pt, err := cos.NewParsedTemplate(msg.Template)
	if err != nil {
		if err == cos.ErrEmptyTemplate {
			pt.Prefix = cos.EmptyMatchAll
			goto pref
		}
		return err
	}
	if err := cmn.ValidatePrefix("bad list-range request", pt.Prefix); err != nil {
		nlog.Errorln(err)
		return err
	}
	if pt.IsRange() {
		r.pt = &pt
		r.lrp = lrpRange
		return nil
	}
pref:
	debug.Assert(pt.IsPrefixOnly())
	r.prefix = pt.Prefix
	r.lrp = lrpPrefix
	return nil
}

func (r *lrit) run(wi lrwi, smap *meta.Smap, prealloc bool) (err error) {
	if r.nwp.workers == nil {
		if prealloc {
			r.buf, _ = core.T.PageMM().Alloc()
		}
		goto iterate
	}
	for _, worker := range r.nwp.workers {
		var (
			buf  []byte
			slab *memsys.Slab
		)
		if prealloc {
			buf, slab = core.T.PageMM().Alloc()
		}
		r.nwp.wg.Add(1)
		go worker.run(buf, slab)
	}
iterate:
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

func (r *lrit) wait() {
	if r.nwp.workers == nil {
		if r.buf != nil {
			core.T.PageMM().Free(r.buf)
		}
		return
	}
	close(r.nwp.workCh)
	r.nwp.wg.Wait()
}

func (r *lrit) done() bool { return r.parent.IsAborted() || r.parent.Finished() }

func (r *lrit) _list(wi lrwi, smap *meta.Smap) error {
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

func (r *lrit) _range(wi lrwi, smap *meta.Smap) error {
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
func (r *lrit) _prefix(wi lrwi, smap *meta.Smap) error {
	var (
		lst     *cmn.LsoRes
		lsmsg   = &apc.LsoMsg{Prefix: r.prefix, Props: apc.GetPropsStatus, Flags: r.lsflags | apc.LsNoDirs}
		npg     = newNpgCtx(r.bck, lsmsg, noopCb, nil /*inventory*/, nil /*bp: see below*/)
		bremote = r.bck.IsRemote()
	)
	if err := r.bck.Init(core.T.Bowner()); err != nil {
		return err
	}
	if bremote {
		npg.bp = core.T.Backend(r.bck)
	} else {
		smap = nil // no need
	}

	for !r.done() {
		var (
			err   error
			ecode int
		)
		if bremote {
			lst = &cmn.LsoRes{Entries: allocLsoEntries()}
			// TODO: this is the last remaining place where we list remote bucket by each and every target
			ecode, err = npg.bp.ListObjects(r.bck, lsmsg, lst)
		} else {
			npg.page.Entries = allocLsoEntries()
			err = npg.nextPageA()
			lst = &npg.page
		}
		if err != nil {
			nlog.Errorln(core.T.String(), "[", err, "ecode", ecode, "]")
			freeLsoEntries(lst.Entries)
			return err
		}
		for _, be := range lst.Entries {
			if !be.IsStatusOK() {
				continue
			}
			if be.IsAnyFlagSet(apc.EntryIsDir) { // always skip virtual dirs
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
		lsmsg.ContinuationToken = lst.ContinuationToken
	}

	return nil
}

func (r *lrit) do(lom *core.LOM, wi lrwi, smap *meta.Smap) (bool /*this lom done*/, error) {
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

	if r.nwp.workers == nil {
		wi.do(lom, r, r.buf)
		return true, nil
	}

	l, c := len(r.nwp.workCh), cap(r.nwp.workCh)
	r.nwp.chanFull.Check(l, c)

	// lom eventually freed below
	// TODO: consider core.LIF with subsequent lif.LOM() conversion

	r.nwp.workCh <- lrpair{lom, wi}
	return false, nil
}

//////////////
// lrworker //
//////////////

func (worker *lrworker) run(buf []byte, slab *memsys.Slab) {
	workCh := worker.lrit.nwp.workCh
	stopCh := worker.lrit.parent.ChanAbort()
outer:
	for {
		select {
		case lrpair, ok := <-workCh:
			if !ok {
				break outer
			}
			lrpair.wi.do(lrpair.lom, worker.lrit, buf)
			core.FreeLOM(lrpair.lom)
		case <-stopCh:
			break outer
		}
	}

	worker.lrit.nwp.wg.Done()
	if buf != nil {
		slab.Free(buf)
	}
}
