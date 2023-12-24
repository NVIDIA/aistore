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

// Compress, Checksum, Sign (CCS)

var (
	bmdJspOpts = jsp.CCSign(cmn.MetaverBMD) // ditto
	rmdJspOpts = jsp.CCSign(cmn.MetaverRMD) // ditto
)

func (*Smap) JspOpts() jsp.Options {
	opts := jsp.CCSign(cmn.MetaverSmap)
	opts.OldMetaverOk = 1
	return opts
}

func (*BMD) JspOpts() jsp.Options { return bmdJspOpts }
func (*RMD) JspOpts() jsp.Options { return rmdJspOpts }
