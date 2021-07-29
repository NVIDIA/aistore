// Package query provides interface to iterate over objects with additional filtering
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package query

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/objwalk/walkinfo"
	"github.com/NVIDIA/aistore/xaction"
)

const (
	xactionTTL = 10 * time.Minute // TODO: see "quiescence"; see #1085
)

type (
	ObjectsListingXact struct {
		xaction.XactBase    // ID() serves as well as a query handle
		t                   cluster.Target
		ctx                 context.Context
		msg                 *cmn.SelectMsg
		timer               *time.Timer
		mtx                 sync.Mutex
		buff                []*cmn.BucketEntry
		query               *ObjectsQuery
		resultCh            chan *Result
		lastDiscardedResult string
		fetchingDone        atomic.Bool
	}

	Result struct {
		entry *cmn.BucketEntry
		err   error
	}
)

func NewObjectsListing(ctx context.Context, t cluster.Target, query *ObjectsQuery, msg *cmn.SelectMsg) (x *ObjectsListingXact) {
	x = &ObjectsListingXact{
		t:        t,
		ctx:      ctx,
		msg:      msg,
		resultCh: make(chan *Result),
		query:    query,
		timer:    time.NewTimer(xactionTTL),
	}
	x.InitBase(msg.UUID, cmn.ActQueryObjects, query.BckSource.Bck)
	return
}

func (r *ObjectsListingXact) stop() {
	close(r.resultCh)
	r.timer.Stop()
}

func (r *ObjectsListingXact) Run(*sync.WaitGroup) {
	defer r.fetchingDone.Store(true)
	debug.Assert(r.query.ObjectsSource != nil)
	debug.Assert(r.query.BckSource != nil)
	debug.Assert(r.query.BckSource.Bck != nil)

	Registry.Put(r.ID(), r)

	if r.query.ObjectsSource.Pt != nil {
		r.startFromTemplate()
		return
	}

	r.startFromBck()
}

func (r *ObjectsListingXact) LastDiscardedResult() string { return r.lastDiscardedResult }

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
	var (
		iter = r.query.ObjectsSource.Pt.Iter()
		bck  = r.query.BckSource.Bck
		smap = r.t.Sowner().Get()
	)
	defer r.stop()
	debug.Assert(bck.IsAIS())

	for objName, hasNext := iter(); hasNext; objName, hasNext = iter() {
		lom := &cluster.LOM{ObjName: objName}
		if err := lom.Init(bck.Bck); err != nil {
			r.putResult(&Result{err: err})
			return
		}
		_, local, err := lom.HrwTarget(smap)
		if err != nil {
			r.putResult(&Result{err: err})
			return
		}
		if !local {
			continue
		}
		if err = lom.Load(true /*cache it*/, false /*locked*/); err != nil {
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
	defer r.stop()
	debug.Assert(r.msg != nil)
	debug.Assert(r.ctx != nil)

	bck := r.query.BckSource.Bck

	// TODO: filtering for cloud buckets is not yet supported.
	if bck.IsCloud() && !r.msg.IsFlagSet(cmn.SelectCached) {
		si, err := cluster.HrwTargetTask(r.ID(), r.t.Sowner().Get())
		if err != nil {
			// TODO: should we handle it somehow?
			return
		}
		if si.ID() != r.t.SID() {
			// We are not the target which should list the cloud objects.
			return
		}

		for {
			bckList, _, err := r.t.Backend(bck).ListObjects(bck, r.msg)
			if err != nil {
				// TODO: should we do `r.putResult(&Result{err: err})`?
				return
			}
			if len(bckList.Entries) == 0 {
				// Finished all objects.
				return
			}
			for _, entry := range bckList.Entries {
				r.putResult(&Result{entry: entry})
			}
			if bckList.ContinuationToken == "" {
				// Empty page marker - no more pages.
				return
			}
			r.msg.ContinuationToken = bckList.ContinuationToken
		}
	}

	wi := walkinfo.NewWalkInfo(r.ctx, r.t, r.msg)
	wi.SetObjectFilter(r.query.Filter())

	cb := func(fqn string, de fs.DirEntry) error {
		entry, err := wi.Callback(fqn, de)
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
			Bck:      bck.Bck,
			CTs:      []string{fs.ObjectType},
			Callback: cb,
			Sorted:   true,
		},
		ValidateCallback: func(fqn string, de fs.DirEntry) error {
			if de.IsDir() {
				return wi.ProcessDir(fqn)
			}
			return nil
		},
	}

	if err := fs.WalkBck(opts); err != nil {
		if _, ok := err.(*cmn.ErrAborted); !ok {
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

	size := cos.Min(int(n), len(r.buff))
	if size == 0 {
		size = len(r.buff)
	}
	return r.buff[:size], err
}

// Should be called with lock acquired.
func (r *ObjectsListingXact) discardN(n uint) {
	if len(r.buff) > 0 && n > 0 {
		size := cos.Min(int(n), len(r.buff))
		r.lastDiscardedResult = r.buff[size-1].Name
		r.buff = r.buff[size:]
	}

	if r.fetchingDone.Load() && len(r.buff) == 0 {
		Registry.Delete(r.ID())
		r.Finish(nil)
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
		if !cmn.TokenIncludesObject(last, r.buff[i].Name) {
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
	debug.Assert(len(res) == 1)
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

func (r *ObjectsListingXact) TokenFulfilled(token string) bool {
	return r.Finished() && !r.Aborted() && r.LastDiscardedResult() != "" && cmn.TokenIncludesObject(token, r.LastDiscardedResult())
}

func (r *ObjectsListingXact) TokenUnsatisfiable(token string) bool {
	return token != "" && !cmn.TokenIncludesObject(token, r.LastDiscardedResult())
}
