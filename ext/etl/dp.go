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

	lomCtx struct {
		dp  *OfflineDP
		lom *core.LOM
		cmn.RetryArgs
		// resulting read-close-sizer
		rcs cos.ReadCloseSizer
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
// TODO: Check if ETL pod is healthy and wait some more if not (yet).
func (dp *OfflineDP) Reader(lom *core.LOM, latestVer, sync bool) (resp core.ReadResp) {
	var (
		action = "read [" + dp.tcbmsg.Transform.Name + "]-transformed " + lom.Cname()
		ctx    = dp.newLomCtx(lom, action)
	)
	debug.Assert(!latestVer && !sync, "NIY")

	lom.SetAtimeUnix(time.Now().UnixNano())
	resp.Ecode, resp.Err = cmn.NetworkCallWithRetry(&ctx.RetryArgs)
	if cmn.Rom.FastV(5, cos.SmoduleETL) {
		nlog.Infoln(action, resp.Err)
	}
	if resp.Err != nil {
		return resp
	}
	debug.Assert(resp.Ecode == 0)
	oah := &cmn.ObjAttrs{
		Size:  ctx.rcs.Size(),
		Ver:   nil,           // NOTE: transformed object - current version does not apply
		Cksum: cos.NoneCksum, // TODO: checksum
		Atime: lom.AtimeUnix(),
	}
	resp.OAH = oah

	// [NOTE] ref 6079834
	// non-trivial limitation: this reader cannot be transmitted to
	// multiple targets (where we actually rely on real re-opening);
	// current usage: tcb/tco
	resp.R = cos.NopOpener(ctx.rcs)
	return resp
}

//
// transform-one-lom context
//

// TODO -- FIXME: all (time, error count) tunables are hardcoded
func (dp *OfflineDP) newLomCtx(lom *core.LOM, action string) *lomCtx {
	ctx := &lomCtx{
		dp:  dp,
		lom: lom,
		RetryArgs: cmn.RetryArgs{
			Action:    action,
			SoftErr:   5,
			HardErr:   2,
			Sleep:     50 * time.Millisecond,
			BackOff:   true,
			Verbosity: cmn.RetryLogQuiet,
		},
	}
	ctx.RetryArgs.Call = ctx.call
	return ctx
}

func (ctx *lomCtx) call() (ecode int, err error) {
	ctx.rcs, ecode, err = ctx.dp.comm.OfflineTransform(ctx.lom, ctx.dp.requestTimeout)
	return ecode, err
}
