// Package runners provides implementation for the AIStore extended actions.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xrun

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

// for additional startup-time reg-s see lru, downloader, ec
func init() {
	xreg.RegGlobXact(&electionProvider{})
	xreg.RegGlobXact(&resilverProvider{})
	xreg.RegGlobXact(&rebalanceProvider{})

	xreg.RegBckXact(&BckRenameProvider{})
	xreg.RegBckXact(&evictDeleteProvider{kind: cmn.ActEvictObjects})
	xreg.RegBckXact(&evictDeleteProvider{kind: cmn.ActDelete})
	xreg.RegBckXact(&PrefetchProvider{})
}
