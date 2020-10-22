// Package mpather provides per-mountpath concepts.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package mpather_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func TestJoggerGroup(t *testing.T) {
	var (
		desc = tutils.ObjectsDesc{
			CTs: []tutils.ContentTypeDesc{
				{Type: fs.WorkfileType, ContentCnt: 10},
				{Type: fs.ObjectType, ContentCnt: 500},
			},
			MountpathsCnt: 10,
			ObjectSize:    cmn.KiB,
		}
		out     = tutils.PrepareObjects(t, desc)
		counter = atomic.NewInt32(0)
	)
	defer os.RemoveAll(out.Dir)

	jg := mpather.NewJoggerGroup(&mpather.JoggerGroupOpts{
		T:   out.T,
		Bck: out.Bck,
		CTs: []string{fs.ObjectType},
		VisitObj: func(lom *cluster.LOM, buf []byte) error {
			tassert.Errorf(t, lom.Size() == 0, "expected LOM to not be loaded")
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

func TestJoggerGroupLoad(t *testing.T) {
	var (
		desc = tutils.ObjectsDesc{
			CTs: []tutils.ContentTypeDesc{
				{Type: fs.WorkfileType, ContentCnt: 10},
				{Type: fs.ObjectType, ContentCnt: 500},
			},
			MountpathsCnt: 10,
			ObjectSize:    cmn.KiB,
		}
		out     = tutils.PrepareObjects(t, desc)
		counter = atomic.NewInt32(0)
	)
	defer os.RemoveAll(out.Dir)

	jg := mpather.NewJoggerGroup(&mpather.JoggerGroupOpts{
		T:   out.T,
		Bck: out.Bck,
		CTs: []string{fs.ObjectType},
		VisitObj: func(lom *cluster.LOM, buf []byte) error {
			tassert.Errorf(t, lom.Size() == desc.ObjectSize, "incorrect object size (lom probably not loaded)")
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
		desc = tutils.ObjectsDesc{
			CTs: []tutils.ContentTypeDesc{
				{Type: fs.ObjectType, ContentCnt: 50},
			},
			MountpathsCnt: 4,
			ObjectSize:    cmn.KiB,
		}
		out     = tutils.PrepareObjects(t, desc)
		counter = atomic.NewInt32(0)
	)
	defer os.RemoveAll(out.Dir)

	jg := mpather.NewJoggerGroup(&mpather.JoggerGroupOpts{
		T:   out.T,
		Bck: out.Bck,
		CTs: []string{fs.ObjectType},
		VisitObj: func(lom *cluster.LOM, buf []byte) error {
			counter.Inc()
			return fmt.Errorf("upsss")
		},
	})

	jg.Run()
	<-jg.ListenFinished()

	tassert.Errorf(
		t, int(counter.Load()) <= desc.MountpathsCnt,
		"joggers should not visit more than #mountpaths objects",
	)

	err := jg.Stop()
	tassert.Errorf(t, err != nil && strings.Contains(err.Error(), "upss"), "expected an error")
}

func TestJoggerGroupMultiContentTypes(t *testing.T) {
	var (
		cts  = []string{fs.ObjectType, ec.SliceType, ec.MetaType}
		desc = tutils.ObjectsDesc{
			CTs: []tutils.ContentTypeDesc{
				{Type: fs.WorkfileType, ContentCnt: 10},
				{Type: fs.ObjectType, ContentCnt: 541},
				{Type: ec.SliceType, ContentCnt: 244},
				{Type: ec.MetaType, ContentCnt: 405},
			},
			MountpathsCnt: 10,
			ObjectSize:    cmn.KiB,
		}
		out = tutils.PrepareObjects(t, desc)
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
			tassert.Errorf(t, lom.Size() == 0, "expected LOM to not be loaded")
			tassert.Errorf(t, len(buf) == 0, "buffer expected to be empty")
			counters[lom.ParsedFQN.ContentType].Inc()
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
