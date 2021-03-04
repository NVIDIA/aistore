// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"github.com/NVIDIA/aistore/cmn"
)

// interface guards
var (
	_ cmn.GetJopts = (*Smap)(nil)
	_ cmn.GetJopts = (*BMD)(nil)
	_ cmn.GetJopts = (*RMD)(nil)
)

var (
	smapJspOpts = cmn.CCSign(cmn.MetaverSmap)
	bmdJspOpts  = cmn.CCSign(cmn.MetaverBMD)
	rmdJspOpts  = cmn.CCSign(cmn.MetaverRMD)
)

func (*Smap) GetJopts() cmn.Jopts { return smapJspOpts }
func (*BMD) GetJopts() cmn.Jopts  { return bmdJspOpts }
func (*RMD) GetJopts() cmn.Jopts  { return rmdJspOpts }
