// Package xreg provides registry and (renew, find) functions for AIS eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/xaction"
)

func GetRebMarked() (out xaction.XactMarked) {
	out.Xact = defaultReg.GetXactRunning(cmn.ActRebalance)
	out.Interrupted = fs.MarkerExists(cmn.RebalanceMarker) && out.Xact == nil
	return
}

func GetResilverMarked() (out xaction.XactMarked) {
	out.Xact = defaultReg.GetXactRunning(cmn.ActResilver)
	out.Interrupted = fs.MarkerExists(cmn.ResilverMarker) && out.Xact == nil
	return
}
