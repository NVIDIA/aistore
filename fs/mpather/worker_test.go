// Package mpather provides per-mountpath concepts.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package mpather_test

import (
	"os"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
)

func TestWorkerGroup(t *testing.T) {
	var (
		desc = tutils.ObjectsDesc{
			CTs: []tutils.ContentTypeDesc{
				{Type: fs.ObjectType, ContentCnt: 100},
			},
			MountpathsCnt: 10,
			ObjectSize:    cos.KiB,
		}
		out     = tutils.PrepareObjects(t, desc)
		counter = atomic.NewInt32(0)
	)
	defer os.RemoveAll(out.Dir)

	wg := mpather.NewWorkerGroup(&mpather.WorkerGroupOpts{
		Callback: func(lom *cluster.LOM, buf []byte) {
			counter.Inc()
		},
		QueueSize: 10,
	})
	defer wg.Stop()

	wg.Run()

	for _, fqn := range out.FQNs[fs.ObjectType] {
		lom := &cluster.LOM{FQN: fqn}
		err := lom.Init(out.Bck)
		tassert.CheckError(t, err)

		wg.Do(lom)
	}

	// Give some time for the workers to pick all the tasks.
	time.Sleep(time.Second)

	tassert.Errorf(
		t, int(counter.Load()) == len(out.FQNs[fs.ObjectType]),
		"invalid number of objects visited (%d vs %d)", counter.Load(), len(out.FQNs[fs.ObjectType]),
	)
}
