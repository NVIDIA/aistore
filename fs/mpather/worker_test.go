// Package mpather provides per-mountpath concepts.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package mpather_test

import (
	"os"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestWorkerGroup(t *testing.T) {
	var (
		desc = tools.ObjectsDesc{
			CTs: []tools.ContentTypeDesc{
				{Type: fs.ObjectType, ContentCnt: 100},
			},
			MountpathsCnt: 10,
			ObjectSize:    cos.KiB,
		}
		out     = tools.PrepareObjects(t, desc)
		counter = atomic.NewInt32(0)
	)
	defer os.RemoveAll(out.Dir)

	wg := mpather.NewWorkerGroup(&mpather.WorkerGroupOpts{
		Callback: func(_ *core.LOM, _ []byte) {
			counter.Inc()
		},
		QueueSize: 10,
	})
	defer wg.Stop()

	wg.Run()

	for _, fqn := range out.FQNs[fs.ObjectType] {
		lom := &core.LOM{}
		err := lom.InitFQN(fqn, &out.Bck)
		tassert.CheckError(t, err)

		_, err = wg.PostLIF(lom)
		tassert.CheckError(t, err)
	}

	// Give some time for the workers to pick all the tasks.
	time.Sleep(time.Second)

	tassert.Errorf(
		t, int(counter.Load()) == len(out.FQNs[fs.ObjectType]),
		"invalid number of objects visited (%d vs %d)", counter.Load(), len(out.FQNs[fs.ObjectType]),
	)
}
