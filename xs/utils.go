// Package xs contains eXtended actions (xactions) except storage services
// (mirror, ec) and extensions (downloader, lru).
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xreg"
)

const (
	doneSendingOpcode = 31415

	waitRegRecv   = 4 * time.Second
	waitUnregRecv = 2 * waitRegRecv
)

func newDM(prefix string, r xreg.RenewBase, recv transport.ReceiveObj, sizePDU int32) (dm *bundle.DataMover, err error) {
	// NOTE:
	// transport endpoint `trname` must be identical across all participating targets
	bmd := r.Args.T.Bowner().Get()
	trname := fmt.Sprintf("%s-%s-%s-%d", prefix, r.Bck.Provider, r.Bck.Name, bmd.Version)

	dmExtra := bundle.Extra{Multiplier: 1, SizePDU: sizePDU}
	dm, err = bundle.NewDataMover(r.Args.T, trname, recv, cluster.RegularPut, dmExtra)
	if err != nil {
		return
	}
	if err = dm.RegRecv(); err != nil {
		var total time.Duration // retry for upto waitRegRecv
		glog.Error(err)
		sleep := cos.CalcProbeFreq(waitRegRecv)
		for err != nil && transport.IsErrDuplicateTrname(err) && total < waitRegRecv {
			time.Sleep(sleep)
			total += sleep
			err = dm.RegRecv()
		}
	}
	return
}
