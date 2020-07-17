// Package xaction provides core functionality for the AIStore extended actions.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xaction

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
)

const (
	rebalanceMarker = ".rebalance_marker"
	resilverMarker  = ".resilver_marker"
)

type XactMarked struct {
	Xact        cmn.Xact
	Interrupted bool
}

func GetRebMarked() (out XactMarked) {
	out.Xact = Registry.GetXactRunning(cmn.ActRebalance)
	out.Interrupted = fs.MarkerExists(GetMarkerName(cmn.ActRebalance)) && out.Xact == nil
	return
}
func GetResilverMarked() (out XactMarked) {
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
