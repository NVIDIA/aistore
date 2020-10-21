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
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func TestJoggerGroup(t *testing.T) {
	var (
		desc = tutils.ObjectsDesc{
			CTs:           []string{fs.ObjectType},
			MountpathsCnt: 10,
			ObjectsCnt:    500,
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
		t, int(counter.Load()) == desc.ObjectsCnt,
		"invalid number of objects visited (%d vs %d)", counter.Load(), desc.ObjectsCnt,
	)

	err := jg.Stop()
	tassert.CheckFatal(t, err)
}

func TestJoggerGroupLoad(t *testing.T) {
	var (
		desc = tutils.ObjectsDesc{
			CTs:           []string{fs.ObjectType},
			MountpathsCnt: 10,
			ObjectsCnt:    500,
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
		t, int(counter.Load()) == desc.ObjectsCnt,
		"invalid number of objects visited (%d vs %d)", counter.Load(), desc.ObjectsCnt,
	)

	err := jg.Stop()
	tassert.CheckFatal(t, err)
}

func TestJoggerGroupError(t *testing.T) {
	var (
		desc = tutils.ObjectsDesc{
			CTs:           []string{fs.ObjectType},
			MountpathsCnt: 4,
			ObjectsCnt:    50,
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
