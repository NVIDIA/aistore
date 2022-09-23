// Package mpather provides per-mountpath concepts.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package mpather_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestJoggerGroup(t *testing.T) {
	var (
		desc = tools.ObjectsDesc{
			CTs: []tools.ContentTypeDesc{
				{Type: fs.WorkfileType, ContentCnt: 10},
				{Type: fs.ObjectType, ContentCnt: 500},
			},
			MountpathsCnt: 10,
			ObjectSize:    cos.KiB,
		}
		out     = tools.PrepareObjects(t, desc)
		counter = atomic.NewInt32(0)
	)
	defer os.RemoveAll(out.Dir)

	jg := mpather.NewJoggerGroup(&mpather.JoggerGroupOpts{
		T:   out.T,
		Bck: out.Bck,
		CTs: []string{fs.ObjectType},
		VisitObj: func(lom *cluster.LOM, buf []byte) error {
			tassert.Errorf(t, len(buf) == 0, "buffer expected to be empty")
			counter.Inc()
			return nil
		},
	})

	jg.Run()
	<-jg.ListenFinished()

	tassert.Errorf(
		t, int(counter.Load()) == len(out.FQNs[fs.ObjectType]),
		"invalid number of objects visited (%d vs %d)", counter.Load(), len(out.FQNs[fs.ObjectType]),
	)

	err := jg.Stop()
	tassert.CheckFatal(t, err)
}

func TestJoggerGroupParallel(t *testing.T) {
	var (
		parallelOptions = []int{2, 8, 24}
		objectsCnt      = 1000
		mpathsCnt       = 3

		desc = tools.ObjectsDesc{
			CTs: []tools.ContentTypeDesc{
				{Type: fs.ObjectType, ContentCnt: objectsCnt},
			},
			MountpathsCnt: mpathsCnt,
			ObjectSize:    cos.KiB,
		}
		out     = tools.PrepareObjects(t, desc)
		counter *atomic.Int32

		mmsa = memsys.PageMM()
	)
	defer os.RemoveAll(out.Dir)

	slab, err := mmsa.GetSlab(memsys.PageSize)
	tassert.CheckFatal(t, err)

	baseJgOpts := &mpather.JoggerGroupOpts{
		T:    out.T,
		Bck:  out.Bck,
		CTs:  []string{fs.ObjectType},
		Slab: slab,
		VisitObj: func(lom *cluster.LOM, buf []byte) error {
			b := bytes.NewBuffer(buf[:0])
			_, err = b.WriteString(lom.FQN)
			tassert.CheckFatal(t, err)

			if rand.Intn(objectsCnt/mpathsCnt)%20 == 0 {
				// Sometimes sleep a while, to check if in this time some other goroutine does not populate the buffer.
				time.Sleep(10 * time.Millisecond)
			}
			fqn := b.String()
			// Checks that there are no concurrent writes on the same buffer.
			tassert.Errorf(t, fqn == lom.FQN, "expected the correct FQN %q to be read, got %q", fqn, b.String())
			counter.Inc()
			return nil
		},
	}

	for _, baseJgOpts.Parallel = range parallelOptions {
		t.Run(fmt.Sprintf("TestJoggerGroupParallel/%d", baseJgOpts.Parallel), func(t *testing.T) {
			counter = atomic.NewInt32(0)
			jg := mpather.NewJoggerGroup(baseJgOpts)
			jg.Run()
			<-jg.ListenFinished()

			tassert.Errorf(
				t, int(counter.Load()) == len(out.FQNs[fs.ObjectType]),
				"invalid number of objects visited (%d vs %d)", counter.Load(), len(out.FQNs[fs.ObjectType]),
			)

			err := jg.Stop()
			tassert.CheckFatal(t, err)
		})
	}
}

func TestJoggerGroupLoad(t *testing.T) {
	var (
		desc = tools.ObjectsDesc{
			CTs: []tools.ContentTypeDesc{
				{Type: fs.WorkfileType, ContentCnt: 10},
				{Type: fs.ObjectType, ContentCnt: 500},
			},
			MountpathsCnt: 10,
			ObjectSize:    cos.KiB,
		}
		out     = tools.PrepareObjects(t, desc)
		counter = atomic.NewInt32(0)
	)
	defer os.RemoveAll(out.Dir)

	jg := mpather.NewJoggerGroup(&mpather.JoggerGroupOpts{
		T:   out.T,
		Bck: out.Bck,
		CTs: []string{fs.ObjectType},
		VisitObj: func(lom *cluster.LOM, buf []byte) error {
			tassert.Errorf(t, lom.SizeBytes() == desc.ObjectSize, "incorrect object size (lom probably not loaded)")
			tassert.Errorf(t, len(buf) == 0, "buffer expected to be empty")
			counter.Inc()
			return nil
		},
		DoLoad: mpather.Load,
	})

	jg.Run()
	<-jg.ListenFinished()

	tassert.Errorf(
		t, int(counter.Load()) == len(out.FQNs[fs.ObjectType]),
		"invalid number of objects visited (%d vs %d)", counter.Load(), len(out.FQNs[fs.ObjectType]),
	)

	err := jg.Stop()
	tassert.CheckFatal(t, err)
}

func TestJoggerGroupError(t *testing.T) {
	var (
		desc = tools.ObjectsDesc{
			CTs: []tools.ContentTypeDesc{
				{Type: fs.ObjectType, ContentCnt: 50},
			},
			MountpathsCnt: 4,
			ObjectSize:    cos.KiB,
		}
		out     = tools.PrepareObjects(t, desc)
		counter = atomic.NewInt32(0)
	)
	defer os.RemoveAll(out.Dir)

	jg := mpather.NewJoggerGroup(&mpather.JoggerGroupOpts{
		T:   out.T,
		Bck: out.Bck,
		CTs: []string{fs.ObjectType},
		VisitObj: func(lom *cluster.LOM, buf []byte) error {
			counter.Inc()
			return fmt.Errorf("oops")
		},
	})

	jg.Run()
	<-jg.ListenFinished()

	tassert.Errorf(
		t, int(counter.Load()) <= desc.MountpathsCnt,
		"joggers should not visit more than #mountpaths objects",
	)

	err := jg.Stop()
	tassert.Errorf(t, err != nil && strings.Contains(err.Error(), "oops"), "expected an error")
}

// This test checks if single LOM error will cause all joggers to stop.
func TestJoggerGroupOneErrorStopsAll(t *testing.T) {
	var (
		totalObjCnt = 5000
		mpathsCnt   = 4
		failAt      = int32(totalObjCnt/mpathsCnt) / 5 // Fail more or less at 20% of objects jogged.
		desc        = tools.ObjectsDesc{
			CTs: []tools.ContentTypeDesc{
				{Type: fs.ObjectType, ContentCnt: totalObjCnt},
			},
			MountpathsCnt: mpathsCnt,
			ObjectSize:    cos.KiB,
		}
		out = tools.PrepareObjects(t, desc)

		mpaths      = fs.GetAvail()
		counters    = make(map[string]*atomic.Int32, len(mpaths))
		failOnMpath *fs.MountpathInfo
		failed      atomic.Bool
	)
	defer os.RemoveAll(out.Dir)

	for _, failOnMpath = range mpaths {
		counters[failOnMpath.Path] = atomic.NewInt32(0)
	}

	jg := mpather.NewJoggerGroup(&mpather.JoggerGroupOpts{
		T:   out.T,
		Bck: out.Bck,
		CTs: []string{fs.ObjectType},
		VisitObj: func(lom *cluster.LOM, buf []byte) error {
			cnt := counters[lom.MpathInfo().Path].Inc()

			// Fail only once, on one mpath.
			if cnt == failAt && failed.CAS(false, true) {
				failOnMpath = lom.MpathInfo()
				return fmt.Errorf("oops")
			}
			return nil
		},
	})

	jg.Run()
	<-jg.ListenFinished()

	for mpath, counter := range counters {
		// Expected at least one object to be skipped at each mountpath, when error occurred at 20% of objects jogged.
		visitCount := counter.Load()
		if mpath == failOnMpath.Path {
			tassert.Fatalf(t, visitCount == failAt, "jogger on fail mpath %q expected to visit %d: visited %d", mpath, failAt, visitCount)
		}
		tassert.Errorf(t, int(visitCount) <= out.MpathObjectsCnt[mpath], "jogger on mpath %q expected to visit at most %d, visited %d",
			mpath, out.MpathObjectsCnt[mpath], counter.Load())
	}

	err := jg.Stop()
	tassert.Errorf(t, err != nil && strings.Contains(err.Error(), "oops"), "expected an error")
}

func TestJoggerGroupMultiContentTypes(t *testing.T) {
	var (
		cts  = []string{fs.ObjectType, fs.ECSliceType, fs.ECMetaType}
		desc = tools.ObjectsDesc{
			CTs: []tools.ContentTypeDesc{
				{Type: fs.WorkfileType, ContentCnt: 10},
				{Type: fs.ObjectType, ContentCnt: 541},
				{Type: fs.ECSliceType, ContentCnt: 244},
				{Type: fs.ECMetaType, ContentCnt: 405},
			},
			MountpathsCnt: 10,
			ObjectSize:    cos.KiB,
		}
		out = tools.PrepareObjects(t, desc)
	)
	defer os.RemoveAll(out.Dir)

	counters := make(map[string]*atomic.Int32, len(cts))
	for _, ct := range cts {
		counters[ct] = atomic.NewInt32(0)
	}
	jg := mpather.NewJoggerGroup(&mpather.JoggerGroupOpts{
		T:   out.T,
		Bck: out.Bck,
		CTs: cts,
		VisitObj: func(lom *cluster.LOM, buf []byte) error {
			tassert.Errorf(t, len(buf) == 0, "buffer expected to be empty")
			counters[fs.ObjectType].Inc()
			return nil
		},
		VisitCT: func(ct *cluster.CT, buf []byte) error {
			tassert.Errorf(t, len(buf) == 0, "buffer expected to be empty")
			counters[ct.ContentType()].Inc()
			return nil
		},
	})

	jg.Run()
	<-jg.ListenFinished()

	// NOTE: No need to check `fs.WorkfileType == 0` since we would get panic when
	//  increasing the counter (counter for `fs.WorkfileType` is not allocated).
	for _, ct := range cts {
		tassert.Errorf(
			t, int(counters[ct].Load()) == len(out.FQNs[ct]),
			"invalid number of %q visited (%d vs %d)", ct, counters[ct].Load(), len(out.FQNs[ct]),
		)
	}

	err := jg.Stop()
	tassert.CheckFatal(t, err)
}
