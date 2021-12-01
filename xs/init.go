// Package xs contains eXtended actions (xactions) except storage services
// (mirror, ec) and extensions (downloader, lru).
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/xreg"
)

var verbose bool

// for additional startup-time reg-s see lru, downloader, ec
func Init() {
	verbose = bool(glog.FastV(4, glog.SmoduleXs))

	xreg.RegNonBckXact(&eleFactory{})
	xreg.RegNonBckXact(&resFactory{})
	xreg.RegNonBckXact(&rebFactory{})
	xreg.RegNonBckXact(&etlFactory{})

	xreg.RegBckXact(&bmvFactory{})
	xreg.RegBckXact(&evdFactory{kind: cmn.ActEvictObjects})
	xreg.RegBckXact(&evdFactory{kind: cmn.ActDeleteObjects})
	xreg.RegBckXact(&prfFactory{})

	xreg.RegBckXact(&olFactory{})

	xreg.RegBckXact(&proFactory{})
	xreg.RegBckXact(&llcFactory{})

	xreg.RegBckXact(&tcoFactory{streamingF: streamingF{kind: cmn.ActETLObjects}})
	xreg.RegBckXact(&tcoFactory{streamingF: streamingF{kind: cmn.ActCopyObjects}})
	xreg.RegBckXact(&archFactory{streamingF: streamingF{kind: cmn.ActArchive}})
}
