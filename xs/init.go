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
	xreg.RegGlobXact(&eleFactory{})
	xreg.RegGlobXact(&rslvrFactory{})
	xreg.RegGlobXact(&rebFactory{})

	xreg.RegFactory(&MovFactory{})
	xreg.RegFactory(&evdFactory{kind: cmn.ActEvictObjects})
	xreg.RegFactory(&evdFactory{kind: cmn.ActDelete})
	xreg.RegFactory(&prfFactory{})

	xreg.RegFactory(&olFactory{})

	xreg.RegFactory(&proFactory{})
	xreg.RegFactory(&llcFactory{})
	xreg.RegFactory(&archFactory{})
}
