// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core"
)

type OfflineDP struct {
	comm           Communicator
	tcbmsg         *apc.TCBMsg
	config         *cmn.Config
	requestTimeout time.Duration
}

// interface guard
var _ core.DP = (*OfflineDP)(nil)

func NewOfflineDP(msg *apc.TCBMsg, config *cmn.Config) (*OfflineDP, error) {
	comm, err := GetCommunicator(msg.Transform.Name)
	if err != nil {
		return nil, err
	}
	return &OfflineDP{
		comm:           comm,
		tcbmsg:         msg,
		config:         config,
		requestTimeout: time.Duration(msg.Transform.Timeout),
	}, nil
}

// takes (possibly remote) source reader
// returns _transformed_ reader and relevant context, including attrs
func (dp *OfflineDP) GetROC(lom *core.LOM, latestVer, sync bool) core.ReadResp {
	// [NOTE] ref 6079834
	// non-trivial limitation: this reader cannot be transmitted to
	// multiple targets (where we actually rely on real re-opening);
	// current usage: tcb/tco

	// TODO: Checksum of the post-transformed object
	return dp.comm.OfflineTransform(lom, dp.requestTimeout, latestVer, sync)
}
