// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/xact"
)

// pebl (pending blob downloads) lifecycle - no backend, no cluster:
// - children are bare XactBlobDl instances that never Run; the test finishes them directly
// - termination callback installed before the child can finish (as with core.BlobParams.TermCB)

const peblTestSize = 64 * cos.KiB

var peblTestInit sync.Once

func newPeblParent(t *testing.T) *prefetch {
	t.Helper()
	peblTestInit.Do(func() {
		xact.Init(func() {}) // onFinished => incFinished
		target := mock.NewTarget(mock.NewBaseBownerMock())
		fs.New(target, 0)
		fs.PutMPI(make(fs.MPI), make(fs.MPI))
	})

	r := &prefetch{xlabs: map[string]string{}}
	r.InitBase(context.Background(), cos.GenUUID(), apc.ActPrefetchObjects, nil)
	r.pebl.init(r)
	return r
}

// a spawned child, as seen by prefetch.blobdl: accounted in peblSize, callback pre-installed
func newPeblChild(r *prefetch) *XactBlobDl {
	x := &XactBlobDl{fullSize: peblTestSize}
	x.InitBase(r.Context(), cos.GenUUID(), apc.ActBlobDl, nil)
	x.AddNotif(&xact.NotifXact{
		Base: nl.Base{When: core.UponTerm, F: r.pebl.done},
		Xact: x,
	})
	r.stats.peblSize.Add(x.Size())
	return x
}

func checkPeblDrained(t *testing.T, r *prefetch) {
	t.Helper()
	tassert.Errorf(t, r.pebl.num() == 0, "expected zero pending+completing, got %d", r.pebl.num())
	r.pebl.mu.Lock()
	l := len(r.pebl.pending)
	r.pebl.mu.Unlock()
	tassert.Errorf(t, l == 0, "expected empty pending, got %d", l)
	tassert.Errorf(t, r.stats.peblSize.Load() == 0, "expected zero peblSize, got %d", r.stats.peblSize.Load())
}

// child finishes (and its callback fires) before prefetch.blobdl gets to pebl.add
func TestPeblFinishBeforeAdd(t *testing.T) {
	r := newPeblParent(t)
	x := newPeblChild(r)

	x.Finish() // callback: nothing in pending - no-op
	tassert.Fatalf(t, r.pebl.num() == 0, "callback must not account for an unknown child (num=%d)", r.pebl.num())

	r.pebl.add(x)
	tassert.Fatalf(t, r.pebl.num() == 1, "expected 1 after add, got %d", r.pebl.num())

	n := r.pebl.reap() // as in blobdl: IsDone => reap
	tassert.Fatalf(t, n == 0, "expected reap to return 0, got %d", n)
	tassert.Errorf(t, r.Objs() == 1, "expected 1 object accounted, got %d", r.Objs())
	checkPeblDrained(t, r)

	// idempotent
	tassert.Errorf(t, r.pebl.reap() == 0 && r.Objs() == 1, "second reap must be a no-op")

	// An already failed child must not be counted as a successful prefetch.
	x = newPeblChild(r)
	x.AddErr(errors.New("injected early failure"))
	x.Finish()
	r.pebl.add(x)
	r.pebl.reap()
	tassert.Errorf(t, r.Objs() == 1 && r.ErrCnt() == 1,
		"expected one successful object and one error, got %d and %d", r.Objs(), r.ErrCnt())
	checkPeblDrained(t, r)
}

// callbacks and concurrent reapers race; each child must be accounted exactly once
func TestPeblExactlyOnce(t *testing.T) {
	const (
		numChildren = 14 // seven distinct errors fit below cos.Errs' default cap of eight
		numReapers  = 4
	)
	r := newPeblParent(t)

	children := make([]*XactBlobDl, numChildren)
	for i := range children {
		children[i] = newPeblChild(r)
		r.pebl.add(children[i])
	}
	tassert.Fatalf(t, r.pebl.num() == numChildren, "expected %d, got %d", numChildren, r.pebl.num())

	var (
		wg   sync.WaitGroup
		stop = make(chan struct{})
	)
	for range numReapers {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					r.pebl.reap()
				}
			}
		})
	}

	var fwg sync.WaitGroup
	for i, x := range children {
		fwg.Go(func() {
			if i%2 == 1 {
				x.AddErr(fmt.Errorf("injected child failure #%d", i)) // (distinct: cos.Errs dedups)
			}
			x.Finish()
		})
	}
	fwg.Wait()
	close(stop)
	wg.Wait()

	// stragglers: finished but neither callback nor reapers found them yet
	r.pebl.reap()

	tassert.Errorf(t, r.Objs() == numChildren/2, "expected %d successful children, got %d", numChildren/2, r.Objs())
	tassert.Errorf(t, r.ErrCnt() == numChildren/2,
		"expected %d child errors, got %d", numChildren/2, r.ErrCnt())
	// exactly-once for failures, too: both paths subtract peblSize and decrement num
	checkPeblDrained(t, r)
}

// user aborts the parent while it is waiting for (long-running) children
func TestPeblWaitAbort(t *testing.T) {
	const numChildren = 8
	r := newPeblParent(t)

	children := make([]*XactBlobDl, numChildren)
	var childrenWG sync.WaitGroup
	for i := range children {
		x := newPeblChild(r)
		children[i] = x
		r.pebl.add(x)
		childrenWG.Go(func() {
			<-x.ChanAbort() // runs until aborted
			x.Finish()
		})
	}

	waitDone := make(chan struct{})
	go func() {
		r.pebl.wait()
		close(waitDone)
	}()

	time.Sleep(100 * time.Millisecond) // let wait() enter its (4s) sleep
	r.Abort(nil)

	// must not sit out the sleep interval, let alone the 32m timeout
	select {
	case <-waitDone:
	case <-time.After(3 * time.Second):
		t.Fatal("pebl.wait did not react to parent's abort")
	}
	childrenWG.Wait()
	for _, x := range children {
		tassert.Errorf(t, x.IsAborted(), "%s: expected aborted", x.Name())
	}
	tassert.Errorf(t, r.Objs() == 0, "expected no objects, got %d", r.Objs())
	tassert.Errorf(t, r.ErrCnt() == 0, "aborted parent must not collect child errors (errs=%d)", r.ErrCnt())
	checkPeblDrained(t, r)
}

// parent already aborted by the time Run gets to the pending children
func TestPeblAbortDrain(t *testing.T) {
	const numChildren = 8
	r := newPeblParent(t)
	var childrenWG sync.WaitGroup
	for range numChildren {
		x := newPeblChild(r)
		r.pebl.add(x)
		childrenWG.Go(func() {
			<-x.ChanAbort()
			time.Sleep(50 * time.Millisecond) // some cleanup
			x.Finish()
		})
	}
	r.Abort(nil)

	started := time.Now()
	r.pebl.abort(r.AbortErr())
	childrenWG.Wait()
	tassert.Errorf(t, time.Since(started) < 5*time.Second, "abort drain took %v", time.Since(started))
	checkPeblDrained(t, r)
}

// at the cap, busy() reaps before refusing
func TestPeblBusyReapsAtCap(t *testing.T) {
	r := newPeblParent(t)

	running := make([]*XactBlobDl, maxPebls)
	for i := range running {
		running[i] = newPeblChild(r)
		r.pebl.add(running[i])
	}
	tassert.Fatalf(t, r.pebl.busy(), "expected busy at %d running", maxPebls)

	for _, x := range running {
		x.Finish() // callbacks remove them
	}
	tassert.Errorf(t, r.pebl.num() == 0, "expected 0 after all finished, got %d", r.pebl.num())

	// finished-before-add at the cap: only busy's reap can clear them
	for range maxPebls {
		x := newPeblChild(r)
		x.Finish() // callback: not in pending yet
		r.pebl.add(x)
	}
	tassert.Fatalf(t, r.pebl.num() == maxPebls, "expected %d, got %d", maxPebls, r.pebl.num())
	_ = r.pebl.busy() // result depends on host CPU/mem; the reap is what's tested
	tassert.Errorf(t, r.Objs() == 2*maxPebls, "expected %d objects, got %d", 2*maxPebls, r.Objs())
	checkPeblDrained(t, r)
}
