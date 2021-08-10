// Package xs contains eXtended actions (xactions) except storage services
// (mirror, ec) and extensions (downloader, lru).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/xreg"
)

// for additional startup-time reg-s see lru, downloader, ec
func init() {
	xreg.RegNonBckXact(&eleFactory{})
	xreg.RegNonBckXact(&rslvrFactory{})
	xreg.RegNonBckXact(&rebFactory{})

	xreg.RegBckXact(&MovFactory{})
	xreg.RegBckXact(&evdFactory{kind: cmn.ActEvictObjects})
	xreg.RegBckXact(&evdFactory{kind: cmn.ActDeleteObjects})
	xreg.RegBckXact(&prfFactory{})
	xreg.RegBckXact(&tcoFactory{kind: cmn.ActETLObjects})
	xreg.RegBckXact(&tcoFactory{kind: cmn.ActCopyObjects})

	xreg.RegBckXact(&olFactory{})

	xreg.RegBckXact(&proFactory{})
	xreg.RegBckXact(&llcFactory{})
	xreg.RegBckXact(&archFactory{})
}
