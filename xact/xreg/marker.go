// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/xact"
)

func GetRebMarked() (out xact.Marked) {
	dreg.entries.mtx.RLock()
	entry := dreg.entries.findRunningKind(apc.ActRebalance)
	dreg.entries.mtx.RUnlock()
	if entry != nil {
		out.Xact = entry.Get()
	}
	out.Interrupted = fs.MarkerExists(fname.RebalanceMarker) && entry == nil
	return
}

func GetResilverMarked() (out xact.Marked) {
	dreg.entries.mtx.RLock()
	entry := dreg.entries.findRunningKind(apc.ActResilver)
	dreg.entries.mtx.RUnlock()
	if entry != nil {
		out.Xact = entry.Get()
	}
	out.Interrupted = fs.MarkerExists(fname.ResilverMarker) && entry == nil
	return
}
