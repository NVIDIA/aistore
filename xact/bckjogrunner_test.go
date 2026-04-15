// Package xact_test tests BckJogRunner without a running cluster.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xact_test

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/xact"
)

// nwCases covers every distinct code path in BckJogRunner.Init / TuneNumWorkers:
//   - NwpNone (-1): Init exits early, no TuneNumWorkers call, CbObj called inline
//   - NwpDflt (0):  media-type default multiplier applied by _defaultNW
//   - 1:            explicit value below NwpMin (the load-based throttle floor)
//   - NwpMin (2):   at the throttle floor
//   - 4:            typical explicit count, pool expected under normal load
//   - 100:          large value, clamped to sys.MaxParallelism()+4
var nwCases = []struct {
	name string
	nw   int
}{
	{"NwpNone(-1)", xact.NwpNone},
	{"NwpDflt(0)", xact.NwpDflt},
	{"explicit(1)", 1},
	{"NwpMin(2)", xact.NwpMin},
	{"explicit(4)", 4},
	{"capped(100)", 100},
}

// objCounts returns the object-count dimension of the test matrix.
// The 1000-object case is omitted under -short to keep CI fast.
func objCounts(t *testing.T) []int {
	t.Helper()
	counts := []int{0, 1, 50, 100}
	if !testing.Short() {
		counts = append(counts, 1000)
	}
	return counts
}

// TestTuneNumWorkers verifies the worker-count tuning function against its
// documented contracts, without touching the filesystem or cluster:
//   - NwpDflt (0) applies a media-type default, yielding a positive count
//   - explicit small values pass through unchanged under normal system load
//   - explicit large values are capped at sys.MaxParallelism()+4
func TestTuneNumWorkers(t *testing.T) {
	tools.PrepareMountPaths(t, 4)

	t.Run("default", func(t *testing.T) {
		n, err := xact.TuneNumWorkers("test", xact.NwpDflt, 4)
		tassert.CheckFatal(t, err)
		if n != xact.NwpNone && n <= 0 {
			t.Fatalf("NwpDflt: expected positive worker count, got %d", n)
		}
	})

	t.Run("explicit_small", func(t *testing.T) {
		// 2 is well below the sys cap; should not be amplified
		n, err := xact.TuneNumWorkers("test", 2, 4)
		tassert.CheckFatal(t, err)
		if n != xact.NwpNone && n > 2 {
			t.Fatalf("explicit small: expected at most 2 workers, got %d", n)
		}
	})

	t.Run("explicit_large_capped", func(t *testing.T) {
		// any value above sys.MaxParallelism()+4 must be clamped
		sysCap := sys.MaxParallelism() + 4
		n, err := xact.TuneNumWorkers("test", 100_000, 4)
		tassert.CheckFatal(t, err)
		if n != xact.NwpNone && n > sysCap {
			t.Fatalf("explicit large: expected workers <= sys cap %d, got %d", sysCap, n)
		}
	})
}

// TestBckJogRunnerVisitAll is a (numObjs × numWorkers) table-driven test.
// For every combination of object count and worker configuration it asserts that
// CbObj is called exactly once per object — the core invariant of BckJogRunner.
func TestBckJogRunnerVisitAll(t *testing.T) {
	for _, numObjs := range objCounts(t) {
		objSize := rand.Int64N(4096) + 1 // random 1–4096 bytes per outer iteration
		t.Run(fmt.Sprintf("objs=%d", numObjs), func(t *testing.T) {
			out := tools.PrepareObjects(t, tools.ObjectsDesc{
				CTs:           []tools.ContentTypeDesc{{Type: fs.ObjCT, ContentCnt: numObjs}},
				MountpathsCnt: 2,
				ObjectSize:    objSize,
			})
			defer os.RemoveAll(out.Dir)

			bck := meta.CloneBck(out.Bck)
			config := cmn.GCO.Get()

			for _, tc := range nwCases {
				t.Run(tc.name, func(t *testing.T) {
					var visited int64

					var runner xact.BckJogRunner
					err := runner.Init(cos.GenUUID(), apc.ActCopyBck, bck, xact.BckJogRunnerOpts{
						CbObj: func(_ *core.LOM, _ []byte) error {
							atomic.AddInt64(&visited, 1)
							return nil
						},
						NumWorkers: tc.nw,
					}, config)
					tassert.CheckFatal(t, err)

					t.Logf("objSize=%d numWorkers requested=%d actual=%d",
						objSize, tc.nw, runner.NumWorkers())

					if tc.nw == xact.NwpNone {
						if n := runner.NumWorkers(); n != 0 {
							t.Fatalf("NwpNone: expected 0 workers, got %d", n)
						}
					}

					runner.Run()
					tassert.CheckFatal(t, runner.Wait())

					if v := atomic.LoadInt64(&visited); v != int64(numObjs) {
						t.Fatalf("numWorkers=%d: expected %d objects visited, got %d",
							tc.nw, numObjs, v)
					}
				})
			}
		})
	}
}

// TestBckJogRunnerWalkBck is a (numObjs × numWorkers) table-driven test.
// For every combination it verifies that the WalkBck override is respected:
// the runner registers as dstBck but traverses srcBck, so the visited count
// must equal the number of objects in src (not dst, which has none).
func TestBckJogRunnerWalkBck(t *testing.T) {
	for _, numObjs := range objCounts(t) {
		objSize := rand.Int64N(4096) + 1
		t.Run(fmt.Sprintf("objs=%d", numObjs), func(t *testing.T) {
			// srcBck holds the objects; dstBck is an empty registration bucket.
			outSrc := tools.PrepareObjects(t, tools.ObjectsDesc{
				CTs:           []tools.ContentTypeDesc{{Type: fs.ObjCT, ContentCnt: numObjs}},
				MountpathsCnt: 2,
				ObjectSize:    objSize,
			})
			defer os.RemoveAll(outSrc.Dir)

			srcBck := meta.CloneBck(outSrc.Bck)
			dstBck := meta.CloneBck(&cmn.Bck{Name: "dst-" + outSrc.Bck.Name, Provider: outSrc.Bck.Provider})
			config := cmn.GCO.Get()

			for _, tc := range nwCases {
				t.Run(tc.name, func(t *testing.T) {
					var visited int64

					var runner xact.BckJogRunner
					err := runner.Init(cos.GenUUID(), apc.ActCopyBck, dstBck, xact.BckJogRunnerOpts{
						CbObj: func(_ *core.LOM, _ []byte) error {
							atomic.AddInt64(&visited, 1)
							return nil
						},
						WalkBck:    srcBck,
						NumWorkers: tc.nw,
					}, config)
					tassert.CheckFatal(t, err)

					t.Logf("objSize=%d numWorkers requested=%d actual=%d",
						objSize, tc.nw, runner.NumWorkers())

					runner.Run()
					tassert.CheckFatal(t, runner.Wait())

					if v := atomic.LoadInt64(&visited); v != int64(numObjs) {
						t.Fatalf("numWorkers=%d: expected %d src objects visited, got %d",
							tc.nw, numObjs, v)
					}
				})
			}
		})
	}
}

// TestBckJogRunnerAbortBeforeRun verifies that aborting before Run causes
// Wait to return an abort error immediately without visiting any objects.
// Run is intentionally not called: ListenFinished never fires, so the select
// in BckJog.Wait fires deterministically on ChanAbort.
func TestBckJogRunnerAbortBeforeRun(t *testing.T) {
	out := tools.PrepareObjects(t, tools.ObjectsDesc{
		CTs:           []tools.ContentTypeDesc{{Type: fs.ObjCT, ContentCnt: 50}},
		MountpathsCnt: 2,
		ObjectSize:    1,
	})
	defer os.RemoveAll(out.Dir)

	bck := meta.CloneBck(out.Bck)
	config := cmn.GCO.Get()

	for _, tc := range nwCases {
		t.Run(tc.name, func(t *testing.T) {
			var (
				runner  xact.BckJogRunner
				visited int64
			)
			err := runner.Init(cos.GenUUID(), apc.ActCopyBck, bck, xact.BckJogRunnerOpts{
				CbObj: func(_ *core.LOM, _ []byte) error {
					atomic.AddInt64(&visited, 1)
					return nil
				},
				NumWorkers: tc.nw,
			}, config)
			tassert.CheckFatal(t, err)

			runner.Abort(errors.New("pre-run abort"))

			waitErr := runner.Wait()
			if waitErr == nil {
				t.Fatal("expected abort error, got nil")
			}
			if !cmn.IsErrAborted(waitErr) {
				t.Fatalf("expected ErrAborted, got %v", waitErr)
			}
			if v := atomic.LoadInt64(&visited); v != 0 {
				t.Fatalf("expected 0 objects visited, got %d", v)
			}
		})
	}
}

// TestBckJogRunnerAbortFromCallback verifies that calling Abort inside CbObj
// causes Wait to return an abort error and stops further processing.
//
// NwpNone is excluded: inline dispatch calls CbObj synchronously in the
// jogger goroutine; after the abort fires the jogger may finish all remaining
// objects before BckJog.Wait picks up ChanAbort, making the visited-count
// assertion unreliable. Abort-from-callback is a worker-pool concern.
//
// Using numObjs > workCh capacity ensures joggers block on a full channel
// while workers are processing, preventing ListenFinished from firing before
// BckJog.Wait has a chance to pick up ChanAbort.
func TestBckJogRunnerAbortFromCallback(t *testing.T) {
	const numObjs = 1000

	out := tools.PrepareObjects(t, tools.ObjectsDesc{
		CTs:           []tools.ContentTypeDesc{{Type: fs.ObjCT, ContentCnt: numObjs}},
		MountpathsCnt: 2,
		ObjectSize:    1,
	})
	defer os.RemoveAll(out.Dir)

	bck := meta.CloneBck(out.Bck)
	config := cmn.GCO.Get()

	for _, tc := range nwCases {
		if tc.nw == xact.NwpNone {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			var (
				runner   xact.BckJogRunner
				visited  int64
				once     sync.Once
				causeErr = errors.New("callback-abort")
			)
			err := runner.Init(cos.GenUUID(), apc.ActCopyBck, bck, xact.BckJogRunnerOpts{
				CbObj: func(_ *core.LOM, _ []byte) error {
					once.Do(func() { runner.Abort(causeErr) })
					atomic.AddInt64(&visited, 1)
					return nil
				},
				NumWorkers: tc.nw,
			}, config)
			tassert.CheckFatal(t, err)

			runner.Run()
			waitErr := runner.Wait()
			if waitErr == nil {
				t.Fatalf("expected abort error, got nil (visited=%d/%d)", atomic.LoadInt64(&visited), numObjs)
			}
			if !cmn.IsErrAborted(waitErr) {
				t.Fatalf("expected ErrAborted, got %v", waitErr)
			}
			if v := atomic.LoadInt64(&visited); v == numObjs {
				t.Fatalf("expected abort to cut processing short, visited all %d objects", numObjs)
			}
		})
	}
}
