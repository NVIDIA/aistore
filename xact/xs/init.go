// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/xact/xreg"
)

// for additional startup-time reg-s see lru, downloader, ec
func Preg() {
	xreg.RegNonBckXact(&eleFactory{})
}

func Treg(coi COI) {
	// scope: global and multi-bucket
	xreg.RegNonBckXact(&eleFactory{})
	xreg.RegNonBckXact(&resFactory{})
	xreg.RegNonBckXact(&rebFactory{})
	xreg.RegNonBckXact(&nsummFactory{})
	xreg.RegNonBckXact(&mossFactory{})

	// scope: bucket
	xreg.RegBckXact(&bmvFactory{})
	xreg.RegBckXact(&evdFactory{kind: apc.ActEvictObjects})
	xreg.RegBckXact(&evdFactory{kind: apc.ActDeleteObjects})
	xreg.RegBckXact(&evdFactory{kind: apc.ActEvictRemoteBck})
	xreg.RegBckXact(&prfFactory{})
	xreg.RegBckXact(&proFactory{})
	xreg.RegBckXact(&llcFactory{})

	xreg.RegBckXact(&archFactory{streamingF: streamingF{kind: apc.ActArchive}})
	xreg.RegBckXact(&lsoFactory{streamingF: streamingF{kind: apc.ActList}})
	xreg.RegBckXact(&blobFactory{})

	gcoi = coi
	xreg.RegBckXact(&tcbFactory{kind: apc.ActCopyBck})
	xreg.RegBckXact(&tcbFactory{kind: apc.ActETLBck})
	xreg.RegBckXact(&tcoFactory{streamingF: streamingF{kind: apc.ActETLObjects}})
	xreg.RegBckXact(&tcoFactory{streamingF: streamingF{kind: apc.ActCopyObjects}})

	xreg.RegBckXact(&rechunkFactory{kind: apc.ActRechunk})
}
