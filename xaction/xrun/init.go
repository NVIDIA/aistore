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
	xreg.RegGlobXact(&eleFactory{})
	xreg.RegGlobXact(&resilverFactory{})
	xreg.RegGlobXact(&rebFactory{})

	xreg.RegBckXact(&MovFactory{})
	xreg.RegBckXact(&evdFactory{kind: cmn.ActEvictObjects})
	xreg.RegBckXact(&evdFactory{kind: cmn.ActDelete})
	xreg.RegBckXact(&PrfchFactory{})
}
