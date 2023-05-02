// Package meta: cluster-level metadata
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package meta

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
)

// interface guards
var (
	_ jsp.Opts = (*Smap)(nil)
	_ jsp.Opts = (*BMD)(nil)
	_ jsp.Opts = (*RMD)(nil)
)

var (
	smapJspOpts = jsp.CCSign(cmn.MetaverSmap)
	bmdJspOpts  = jsp.CCSign(cmn.MetaverBMD)
	rmdJspOpts  = jsp.CCSign(cmn.MetaverRMD)
)

func (*Smap) JspOpts() jsp.Options { return smapJspOpts }
func (*BMD) JspOpts() jsp.Options  { return bmdJspOpts }
func (*RMD) JspOpts() jsp.Options  { return rmdJspOpts }
