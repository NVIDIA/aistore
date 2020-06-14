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
	"github.com/NVIDIA/aistore/query"
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
		t.SetEndTime(time.Now())
		t.Job.Wg.Done()
	}()

	var (
		streamer = newSamplesStreamer(t)
		objSrc   = query.TemplateObjSource(&t.Job.Template)
		bckSrc   = query.BckSource(t.Bck())

		// doesn't use xaction registry, but it's not necessary as this xaction's life span
		// is the same as tar2tf request life span. If request get's canceled,
		// the xaction will terminate as well
		q         = query.NewQuery(objSrc, bckSrc, nil)
		resultSet = query.NewResultSet(t.T, q)
	)
	go resultSet.Start()

	err := resultSet.ForEach(func(lom *cluster.LOM) error {
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
		glog.Error(err)
	}
}
