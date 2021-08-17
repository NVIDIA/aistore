// Package xs contains eXtended actions (xactions) except storage services
// (mirror, ec) and extensions (downloader, lru).
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"strings"
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
	waitUnregRecv = waitRegRecv
)

func newArchTcoDM(prefix string, r xreg.RenewBase, recv transport.ReceiveObj, sizePDU int32) (dm *bundle.DataMover, err error) {
	trname := prefix + r.Bck.Provider + "-" + r.Bck.Name // NOTE: must be identical across cluster
	dmExtra := bundle.Extra{Multiplier: 1, SizePDU: sizePDU}
	dm, err = bundle.NewDataMover(r.T, trname, recv, cluster.RegularPut, dmExtra)
	if err != nil {
		return
	}
	// TODO better
	if err = dm.RegRecv(); err != nil {
		var total time.Duration
		glog.Error(err)
		sleep := cos.CalcProbeFreq(waitRegRecv)
		for err != nil && strings.Contains(err.Error(), "duplicate trname") && total < waitRegRecv {
			time.Sleep(sleep)
			total += sleep
			err = dm.RegRecv()
		}
	}
	return
}

func unregArchTcoDM(dm *bundle.DataMover, lpending func() int) {
	sleep := cos.CalcProbeFreq(waitUnregRecv)
	for tot := time.Duration(0); tot < waitUnregRecv; tot += sleep {
		if lpending() == 0 {
			break
		}
		time.Sleep(sleep)
	}
	dm.UnregRecv()
}
