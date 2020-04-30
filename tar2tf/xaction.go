// Package ta2tf provides core functionality for integrating with TensorFlow tools
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */

package tar2tf

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/go-tfdata/tfdata/transform"
)

type (
	Xact struct {
		cmn.XactBase
		Job *SamplesStreamJob
		T   cluster.Target
		sync.RWMutex
	}
)

func (t *Xact) IsMountpathXact() bool { return false }
func (t *Xact) Run() {
	defer func() {
		t.EndTime(time.Now())
		t.Job.Wg.Done()
	}()

	streamer := newSamplesStreamer(t)
	err := cluster.HrwIterMatchingObjects(t.T, cluster.NewBckEmbed(t.Bck()), t.Job.Template, func(lom *cluster.LOM) error {
		tarReader, err := newTarSamplesReader(lom)
		if err != nil {
			return err
		}
		if t.Job.ShuffleTar {
			tarReader.Shuffle()
		}

		return streamer.Stream(transform.NewSampleTransformer(tarReader, t.Job.Conversions...))
	})

	if err != nil {
		glog.Errorf("error %s", err.Error())
	}
}
