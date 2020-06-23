// Package reb provides resilvering and rebalancing functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/xaction"
)

const (
	rebalanceMarker = ".rebalance_marker"
	resilverMarker  = ".resilver_marker"
)

func IsRebalancing(kind string) (aborted, running bool) {
	cmn.Assert(kind == cmn.ActRebalance || kind == cmn.ActResilver)
	if fs.MarkerExists(getMarkerName(kind)) {
		aborted = true
	}
	running = xaction.Registry.IsXactRunning(xaction.XactQuery{Kind: kind})
	if !running {
		return
	}
	aborted = false
	return
}

func getMarkerName(kind string) (marker string) {
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
