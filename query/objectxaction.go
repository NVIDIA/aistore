// Package query provides interface to iterate over objects with additional filtering
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package query

import (
	"io"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

type (
	ResultSetXact struct {
		cmn.XactBase
		t     cluster.Target
		timer *time.Timer

		query    *ObjectsQuery
		resultCh chan *Result
	}

	Result struct {
		lom *cluster.LOM
		err error
	}
)

const (
	xactionTTL = 10 * time.Minute // TODO: it should be Xaction argument
)

func NewResultSet(t cluster.Target, query *ObjectsQuery) *ResultSetXact {
	return &ResultSetXact{
		t:        t,
		resultCh: make(chan *Result),
		timer:    time.NewTimer(xactionTTL),
		query:    query,
	}
}

// Start without specified handle means that we won't be able
// to get this specific result set if we loose a reference to it.
// Sometimes we know that we won't and that's ok.
func (r *ResultSetXact) Start() {
	r.StartWithHandle("")
}

func (r *ResultSetXact) IsMountpathXact() bool { return false } // TODO -- FIXME

func (r *ResultSetXact) StartWithHandle(handle string) {
	cmn.Assert(r.query.ObjectsSource != nil)
	cmn.Assert(r.query.ObjectsSource.pt != nil)
	cmn.Assert(r.query.BckSource != nil)
	cmn.Assert(r.query.BckSource.Bck != nil)

	Registry.Put(handle, r)

	var (
		iter   = r.query.ObjectsSource.pt.Iter()
		config = cmn.GCO.Get()
		smap   = r.t.GetSowner().Get()
	)

	defer func() {
		close(r.resultCh)
		Registry.Delete(handle)
		r.timer.Stop()
	}()

	for objName, hasNext := iter(); hasNext; objName, hasNext = iter() {
		lom := &cluster.LOM{T: r.t, ObjName: objName}
		if err := lom.Init(*r.query.BckSource.Bck, config); err != nil {
			r.resultCh <- &Result{err: err}
			return
		}
		si, err := cluster.HrwTarget(lom.Uname(), smap)
		if err != nil {
			r.resultCh <- &Result{err: err}
			return
		}

		if si.ID() != r.t.Snode().ID() {
			continue
		}

		if err = lom.Load(); err != nil {
			if !cmn.IsObjNotExist(err) {
				r.resultCh <- &Result{err: err}
				return
			}
			continue
		}

		if !r.query.filter(lom) {
			continue
		}

		select {
		case <-r.ChanAbort():
			return
		case <-r.timer.C:
			return
		case r.resultCh <- &Result{lom: lom, err: err}:
			r.timer.Reset(xactionTTL)
			break
		}
	}
}

func (r *ResultSetXact) Next() (*cluster.LOM, error) {
	res, ok := <-r.resultCh
	if !ok {
		return nil, io.EOF
	}
	return res.lom, res.err
}

// NextN returns at most n next elements until error occurs from Next() call
func (r *ResultSetXact) NextN(n int) ([]*cluster.LOM, error) {
	result := make([]*cluster.LOM, 0, n)
	for ; n > 0; n-- {
		lom, err := r.Next()
		if err != nil {
			return result, err
		}
		result = append(result, lom)
	}
	return result, nil
}

func (r *ResultSetXact) ForEach(apply func(*cluster.LOM) error) error {
	var (
		lom *cluster.LOM
		err error
	)
	for lom, err = r.Next(); err == nil; lom, err = r.Next() {
		if err := apply(lom); err != nil {
			r.Abort()
			return err
		}
	}
	if err != io.EOF {
		return err
	}
	return nil
}

func ATimeAfterFilter(time time.Time) ObjectFilter {
	return func(lom *cluster.LOM) bool {
		return lom.Atime().After(time)
	}
}

func ATimeBeforeFilter(time time.Time) ObjectFilter {
	return func(lom *cluster.LOM) bool {
		return lom.Atime().Before(time)
	}
}

func And(filters ...ObjectFilter) ObjectFilter {
	return func(lom *cluster.LOM) bool {
		for _, f := range filters {
			if !f(lom) {
				return false
			}
		}
		return true
	}
}

func Or(filters ...ObjectFilter) ObjectFilter {
	return func(lom *cluster.LOM) bool {
		for _, f := range filters {
			if f(lom) {
				return true
			}
		}
		return false
	}
}
