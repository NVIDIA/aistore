// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
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
func (dp *OfflineDP) Reader(lom *core.LOM, latestVer, sync bool) (resp core.ReadResp) {
	var (
		r      cos.ReadCloseSizer // note: +sizer
		action = "read [" + dp.tcbmsg.Transform.Name + "]-transformed " + lom.Cname()
	)
	debug.Assert(!latestVer && !sync, "NIY") // TODO -- FIXME
	call := func() (_ int, err error) {
		r, err = dp.comm.OfflineTransform(lom, dp.requestTimeout)
		return 0, err
	}
	// TODO: Check if ETL pod is healthy and wait some more if not (yet).
	lom.SetAtimeUnix(time.Now().UnixNano())
	resp.Err = cmn.NetworkCallWithRetry(&cmn.RetryArgs{
		Call:      call,
		Action:    action,
		SoftErr:   5,
		HardErr:   2,
		Sleep:     50 * time.Millisecond,
		BackOff:   true,
		Verbosity: cmn.RetryLogQuiet,
	})
	if cmn.Rom.FastV(5, cos.SmoduleETL) {
		nlog.Infoln(action, resp.Err)
	}
	if resp.Err != nil {
		return resp
	}
	oah := &cmn.ObjAttrs{
		Size:  r.Size(),
		Ver:   nil,           // NOTE: transformed object - current version does not apply
		Cksum: cos.NoneCksum, // TODO: checksum
		Atime: lom.AtimeUnix(),
	}
	resp.OAH = oah

	// [NOTE] ref 6079834
	// non-trivial limitation: this reader cannot be transmitted to
	// multiple targets (where we actually rely on real re-opening);
	// current usage: tcb/tco
	resp.R = cos.NopOpener(r)
	return resp
}
