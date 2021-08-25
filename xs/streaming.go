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
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xreg"
)

const (
	doneSendingOpcode = 31415

	waitRegRecv   = 4 * time.Second
	waitUnregRecv = 2 * waitRegRecv
)

type (
	streamingF struct {
		xreg.RenewBase
		xact cluster.Xact
		kind string
		dm   *bundle.DataMover
	}
)

func (p *streamingF) Kind() string      { return p.kind }
func (p *streamingF) Get() cluster.Xact { return p.xact }

func (p *streamingF) WhenPrevIsRunning(xprev xreg.Renewable) (xreg.WPR, error) {
	debug.Assertf(false, "%s vs %s", p.Str(p.Kind()), xprev) // xreg.usePrev() must've returned true
	return xreg.WprUse, nil
}

func (p *streamingF) newDM(prefix string, recv transport.ReceiveObj, sizePDU int32) (err error) {
	// NOTE:
	// transport endpoint `trname` must be identical across all participating targets
	bmd := p.Args.T.Bowner().Get()
	trname := fmt.Sprintf("%s-%s-%s-%d", prefix, p.Bck.Provider, p.Bck.Name, bmd.Version)

	dmExtra := bundle.Extra{Multiplier: 1, SizePDU: sizePDU}
	p.dm, err = bundle.NewDataMover(p.Args.T, trname, recv, cluster.RegularPut, dmExtra)
	if err != nil {
		return
	}
	if err = p.dm.RegRecv(); err != nil {
		var total time.Duration // retry for upto waitRegRecv
		glog.Error(err)
		sleep := cos.CalcProbeFreq(waitRegRecv)
		for err != nil && transport.IsErrDuplicateTrname(err) && total < waitRegRecv {
			time.Sleep(sleep)
			total += sleep
			err = p.dm.RegRecv()
		}
	}
	return
}
