// Package query provides interface to iterate over objects with additional filtering
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package query

import (
	"io"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/objwalk/walkinfo"
)

type (
	Xact struct {
		cmn.XactBase
		t     cluster.Target
		timer *time.Timer
		wi    *walkinfo.WalkInfo

		query      *ObjectsQuery
		resultCh   chan *Result
		lastResult string
	}

	Result struct {
		entry *cmn.BucketEntry
		err   error
	}
)

const (
	xactionTTL = 10 * time.Minute // TODO: it should be Xaction argument
)

func NewListObjects(t cluster.Target, query *ObjectsQuery, wi *walkinfo.WalkInfo, id string) *Xact {
	cmn.Assert(query.BckSource.Bck != nil)
	return &Xact{
		XactBase: *cmn.NewXactBaseWithBucket(id, cmn.ActQuery, *query.BckSource.Bck),
		t:        t,
		wi:       wi,
		resultCh: make(chan *Result),
		query:    query,
		timer:    time.NewTimer(xactionTTL),
	}
}

// Start without specified handle means that we won't be able
// to get this specific result set if we loose a reference to it.
// Sometimes we know that we won't and that's ok.
func (r *Xact) Start() {
	r.StartWithHandle("")
}

func (r *Xact) stop() {
	close(r.resultCh)
	r.timer.Stop()
	r.Finish()
}

func (r *Xact) IsMountpathXact() bool { return false } // TODO -- FIXME

func (r *Xact) StartWithHandle(handle string) {
	cmn.Assert(r.query.ObjectsSource != nil)
	cmn.Assert(r.query.BckSource != nil)
	cmn.Assert(r.query.BckSource.Bck != nil)

	Registry.Put(handle, r)

	if r.query.ObjectsSource.Pt != nil {
		r.startFromTemplate(handle)
		return
	}

	r.startFromBck(handle)
}

// TODO: make thread-safe
func (r *Xact) LastResult() string {
	return r.lastResult
}

func (r *Xact) putResult(res *Result) (end bool) {
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

func (r *Xact) startFromTemplate(handle string) {
	defer func() {
		r.stop()
		Registry.Delete(handle)
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

func (r *Xact) startFromBck(handle string) {
	defer func() {
		Registry.Delete(handle)
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

func (r *Xact) Next() (*cmn.BucketEntry, error) {
	res, ok := <-r.resultCh
	if !ok {
		return nil, io.EOF
	}
	r.lastResult = res.entry.Name
	return res.entry, res.err
}

// NextN returns at most n next elements until error occurs from Next() call
func (r *Xact) NextN(n uint) (result []*cmn.BucketEntry, err error) {
	if n == 0 {
		result = make([]*cmn.BucketEntry, 0, cmn.DefaultListPageSize)
	} else {
		result = make([]*cmn.BucketEntry, 0, n)
	}

	for n == 0 || len(result) < int(n) {
		entry, err := r.Next()
		if err != nil {
			return result, err
		}
		result = append(result, entry)
	}

	return result, nil
}

func (r *Xact) ForEach(apply func(entry *cmn.BucketEntry) error) error {
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

func (r *Xact) PageMarkerFulfilled(pageMarker string) bool {
	// Everything, that target has, has been already fetched.
	return r.Finished() && !r.Aborted() && r.LastResult() != "" && cmn.PageMarkerIncludesObject(pageMarker, r.LastResult())
}

func (r *Xact) PageMarkerUnsatisfiable(pageMarker string) bool {
	return pageMarker != "" && !cmn.PageMarkerIncludesObject(pageMarker, r.LastResult())
}

func ATimeAfterFilter(time time.Time) cluster.ObjectFilter {
	return func(lom *cluster.LOM) bool {
		return lom.Atime().After(time)
	}
}

func ATimeBeforeFilter(time time.Time) cluster.ObjectFilter {
	return func(lom *cluster.LOM) bool {
		return lom.Atime().Before(time)
	}
}

func And(filters ...cluster.ObjectFilter) cluster.ObjectFilter {
	return func(lom *cluster.LOM) bool {
		for _, f := range filters {
			if !f(lom) {
				return false
			}
		}
		return true
	}
}

func Or(filters ...cluster.ObjectFilter) cluster.ObjectFilter {
	return func(lom *cluster.LOM) bool {
		for _, f := range filters {
			if f(lom) {
				return true
			}
		}
		return false
	}
}
