// Package space provides storage cleanup and eviction functionality (the latter based on the
// least recently used cache replacement). It also serves as a built-in garbage-collection
// mechanism for orphaned workfiles.
/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
 */
package space

import (
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/xact/xreg"
)

var verbose bool

func Xreg(config *cmn.Config) {
	xreg.RegNonBckXact(&lruFactory{})
	xreg.RegNonBckXact(&clnFactory{})

	verbose = config.FastV(4, glog.SmoduleSpace)
}
