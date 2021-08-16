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
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xreg"
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
		for err != nil && strings.Contains(err.Error(), "duplicate trname") && total < delayUnregRecvMax {
			time.Sleep(delayUnregRecv)
			total += delayUnregRecv
			err = dm.RegRecv()
		}
	}
	return
}

func unregArchTcoDM(dm *bundle.DataMover, lpending func() int) {
	time.Sleep(2 * delayUnregRecv)
	for tot := time.Duration(0); tot < delayUnregRecvMax; tot += delayUnregRecv {
		if lpending() == 0 {
			break
		}
		time.Sleep(delayUnregRecv)
	}
	dm.UnregRecv()
}
