// Package registry provides core functionality for the AIStore extended actions registry.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package registry

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/xaction"
)

const (
	rebalanceMarker = ".rebalance_marker"
	resilverMarker  = ".resilver_marker"
)

func GetRebMarked() (out xaction.XactMarked) {
	out.Xact = Registry.GetXactRunning(cmn.ActRebalance)
	out.Interrupted = fs.MarkerExists(GetMarkerName(cmn.ActRebalance)) && out.Xact == nil
	return
}

func GetResilverMarked() (out xaction.XactMarked) {
	out.Xact = Registry.GetXactRunning(cmn.ActResilver)
	out.Interrupted = fs.MarkerExists(GetMarkerName(cmn.ActResilver)) && out.Xact == nil
	return
}

func GetMarkerName(kind string) (marker string) {
	switch kind {
	case cmn.ActResilver:
		marker = resilverMarker
	case cmn.ActRebalance:
		marker = rebalanceMarker
	default:
		cmn.Assert(false)
	}
	return
}
