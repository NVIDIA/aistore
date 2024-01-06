// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
)

// NOTE: compare with core/ldp.go

type (
	OfflineDP struct {
		comm           Communicator
		tcbmsg         *apc.TCBMsg
		config         *cmn.Config
		requestTimeout time.Duration
	}
)

// interface guard
var _ core.DP = (*OfflineDP)(nil)

func NewOfflineDP(msg *apc.TCBMsg, config *cmn.Config) (*OfflineDP, error) {
	comm, err := GetCommunicator(msg.Transform.Name)
	if err != nil {
		return nil, err
	}
	pr := &OfflineDP{comm: comm, tcbmsg: msg, config: config}
	pr.requestTimeout = time.Duration(msg.Transform.Timeout)
	return pr, nil
}

// Returns reader resulting from lom ETL transformation.
// TODO -- FIXME: comm.OfflineTransform to support latestVer and sync
func (dp *OfflineDP) Reader(lom *core.LOM, latestVer, sync bool) (cos.ReadOpenCloser, cos.OAH, error) {
	var (
		r      cos.ReadCloseSizer // note: +sizer
		err    error
		action = "read [" + dp.tcbmsg.Transform.Name + "]-transformed " + lom.Cname()
	)
	debug.Assert(!latestVer && !sync, "NIY") // TODO -- FIXME
	call := func() (int, error) {
		r, err = dp.comm.OfflineTransform(lom.Bck(), lom.ObjName, dp.requestTimeout)
		return 0, err
	}
	// TODO: Check if ETL pod is healthy and wait some more if not (yet).
	err = cmn.NetworkCallWithRetry(&cmn.RetryArgs{
		Call:      call,
		Action:    action,
		SoftErr:   5,
		HardErr:   2,
		Sleep:     50 * time.Millisecond,
		BackOff:   true,
		Verbosity: cmn.RetryLogQuiet,
	})
	if dp.config.FastV(5, cos.SmoduleETL) {
		nlog.Infoln(action, err)
	}
	if err != nil {
		return nil, nil, err
	}
	lom.SetAtimeUnix(time.Now().UnixNano())
	oah := &cmn.ObjAttrs{
		Size:  r.Size(),
		Ver:   "",            // transformed object - current version does not apply
		Cksum: cos.NoneCksum, // TODO: checksum
		Atime: lom.AtimeUnix(),
	}
	return cos.NopOpener(r), oah, nil
}
