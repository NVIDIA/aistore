// Package query provides interface to iterate over objects with additional filtering
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package query

import (
	"io"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/objwalk/walkinfo"
)

type (
	ObjectsListingXact struct {
		cmn.XactBase // ID() serves as well as a query handle
		t            cluster.Target
		timer        *time.Timer
		wi           *walkinfo.WalkInfo
		mtx          sync.Mutex
		buff         []*cmn.BucketEntry
		fetchingDone bool

		query               *ObjectsQuery
		resultCh            chan *Result
		lastDiscardedResult string
	}

	Result struct {
		entry *cmn.BucketEntry
		err   error
	}
)

const (
	xactionTTL = 10 * time.Minute // TODO: it should be Xaction argument
)

func NewObjectsListing(t cluster.Target, query *ObjectsQuery, wi *walkinfo.WalkInfo, id string) *ObjectsListingXact {
	cmn.Assert(query.BckSource.Bck != nil)
	cmn.Assert(id != "")
	return &ObjectsListingXact{
		XactBase: *cmn.NewXactBaseBck(id, cmn.ActQueryObjects, *query.BckSource.Bck),
		t:        t,
		wi:       wi,
		resultCh: make(chan *Result),
		query:    query,
		timer:    time.NewTimer(xactionTTL),
	}
}

func (r *ObjectsListingXact) stop() {
	close(r.resultCh)
	r.timer.Stop()
}

func (r *ObjectsListingXact) IsMountpathXact() bool { return false } // TODO -- FIXME

func (r *ObjectsListingXact) Start() {
	defer func() {
		r.fetchingDone = true
	}()

	cmn.Assert(r.query.ObjectsSource != nil)
	cmn.Assert(r.query.BckSource != nil)
	cmn.Assert(r.query.BckSource.Bck != nil)

	Registry.Put(r.ID().String(), r)

	if r.query.ObjectsSource.Pt != nil {
		r.startFromTemplate()
		return
	}

	r.startFromBck()
}

// TODO: make thread-safe
func (r *ObjectsListingXact) LastDiscardedResult() string {
	return r.lastDiscardedResult
}

func (r *ObjectsListingXact) putResult(res *Result) (end bool) {
	select {
	case <-r.ChanAbort():
		return true
	case <-r.timer.C:
		return true
	case r.resultCh <- res:
		r.timer.Reset(xactionTTL)
		return res.err != nil
	}
}

func (r *ObjectsListingXact) startFromTemplate() {
	defer func() {
		r.stop()
	}()

	var (
		iter   = r.query.ObjectsSource.Pt.Iter()
		config = cmn.GCO.Get()
		smap   = r.t.GetSowner().Get()
	)

	for objName, hasNext := iter(); hasNext; objName, hasNext = iter() {
		lom := &cluster.LOM{T: r.t, ObjName: objName}
		if err := lom.Init(*r.query.BckSource.Bck, config); err != nil {
			r.putResult(&Result{err: err})
			return
		}
		si, err := cluster.HrwTarget(lom.Uname(), smap)
		if err != nil {
			r.putResult(&Result{err: err})
			return
		}

		if si.ID() != r.t.Snode().ID() {
			continue
		}

		if err = lom.Load(); err != nil {
			if !cmn.IsObjNotExist(err) {
				r.putResult(&Result{err: err})
				return
			}
			continue
		}

		if lom.IsCopy() {
			continue
		}

		if !r.query.Filter()(lom) {
			continue
		}

		if r.putResult(&Result{entry: &cmn.BucketEntry{Name: lom.ObjName}, err: err}) {
			return
		}
	}
}

func (r *ObjectsListingXact) startFromBck() {
	defer func() {
		r.stop()
	}()

	cb := func(fqn string, de fs.DirEntry) error {
		entry, err := r.wi.Callback(fqn, de)
		if entry == nil && err == nil {
			return nil
		}
		if r.putResult(&Result{entry: entry, err: err}) {
			return cmn.NewAbortedError(r.t.Snode().DaemonID + " ResultSetXact")
		}
		return nil
	}

	opts := &fs.WalkBckOptions{
		Options: fs.Options{
			Bck:      *r.query.BckSource.Bck,
			CTs:      []string{fs.ObjectType},
			Callback: cb,
			Sorted:   true,
		},
		ValidateCallback: func(fqn string, de fs.DirEntry) error {
			if de.IsDir() {
				return r.wi.ProcessDir(fqn)
			}
			return nil
		},
	}

	if err := fs.WalkBck(opts); err != nil {
		if _, ok := err.(cmn.AbortedError); !ok {
			glog.Error(err)
		}
	}
}

// Should be called with lock acquired.
func (r *ObjectsListingXact) peekN(n uint) (result []*cmn.BucketEntry, err error) {
	if len(r.buff) >= int(n) && n != 0 {
		return r.buff[:n], nil
	}

	for len(r.buff) < int(n) || n == 0 {
		res, ok := <-r.resultCh
		if !ok {
			err = io.EOF
			break
		}
		if res.err != nil {
			err = res.err
			break
		}
		r.buff = append(r.buff, res.entry)
	}

	size := cmn.Min(int(n), len(r.buff))
	if size == 0 {
		size = len(r.buff)
	}
	return r.buff[:size], err
}

// Should be called with lock acquired.
func (r *ObjectsListingXact) discardN(n uint) {
	if len(r.buff) > 0 && n > 0 {
		size := cmn.Min(int(n), len(r.buff))
		r.lastDiscardedResult = r.buff[size-1].Name
		r.buff = r.buff[size:]
	}

	if r.fetchingDone && len(r.buff) == 0 {
		Registry.Delete(r.ID().String())
		r.Finish()
	}
}

// PeekN returns first N objects from a query.
// It doesn't move a cursor so subsequent Peek/Next requests will reuse the objects.
func (r *ObjectsListingXact) PeekN(n uint) (result []*cmn.BucketEntry, err error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return r.peekN(n)
}

// Discards all objects from buff until object > last is reached.
func (r *ObjectsListingXact) DiscardUntil(last string) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if len(r.buff) == 0 {
		return
	}

	i := 0
	for ; i < len(r.buff); i++ {
		if !cmn.PageMarkerIncludesObject(last, r.buff[i].Name) {
			break
		}
	}

	r.discardN(uint(i))
}

// Should be called with lock acquired.
func (r *ObjectsListingXact) nextN(n uint) (result []*cmn.BucketEntry, err error) {
	result, err = r.peekN(n)
	r.discardN(uint(len(result)))
	return result, err
}

// NextN returns at most n next elements until error occurs from Next() call
func (r *ObjectsListingXact) NextN(n uint) (result []*cmn.BucketEntry, err error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return r.nextN(n)
}

// Returns single object from a query xaction. Returns io.EOF if no more results.
// Next() moves cursor so fetched object will be forgotten by a target.
func (r *ObjectsListingXact) Next() (entry *cmn.BucketEntry, err error) {
	res, err := r.NextN(1)
	if len(res) == 0 {
		return nil, err
	}
	cmn.Assert(len(res) == 1)
	return res[0], err
}

func (r *ObjectsListingXact) ForEach(apply func(entry *cmn.BucketEntry) error) error {
	var (
		entry *cmn.BucketEntry
		err   error
	)
	for entry, err = r.Next(); err == nil; entry, err = r.Next() {
		if err := apply(entry); err != nil {
			r.Abort()
			return err
		}
	}
	if err != io.EOF {
		return err
	}
	return nil
}

func (r *ObjectsListingXact) PageMarkerFulfilled(pageMarker string) bool {
	// Everything, that target has, has been already fetched.
	return r.Finished() && !r.Aborted() && r.LastDiscardedResult() != "" && cmn.PageMarkerIncludesObject(pageMarker, r.LastDiscardedResult())
}

func (r *ObjectsListingXact) PageMarkerUnsatisfiable(pageMarker string) bool {
	return pageMarker != "" && !cmn.PageMarkerIncludesObject(pageMarker, r.LastDiscardedResult())
}
