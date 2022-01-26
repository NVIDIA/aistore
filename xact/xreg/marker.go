// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/xact"
)

func GetRebMarked() (out xact.Marked) {
	entry := dreg.getRunning(XactFilter{Kind: cmn.ActRebalance})
	if entry != nil {
		out.Xact = entry.Get()
	}
	out.Interrupted = fs.MarkerExists(cmn.RebalanceMarker) && entry == nil
	return
}

func GetResilverMarked() (out xact.Marked) {
	entry := dreg.getRunning(XactFilter{Kind: cmn.ActResilver})
	if entry != nil {
		out.Xact = entry.Get()
	}
	out.Interrupted = fs.MarkerExists(cmn.ResilverMarker) && entry == nil
	return
}
