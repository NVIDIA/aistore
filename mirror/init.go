// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

func init() {
	xreg.RegFactory(&cpyFactory{kind: cmn.ActCopyBck})
	xreg.RegFactory(&cpyFactory{kind: cmn.ActETLBck})
	xreg.RegFactory(&mncFactory{})
	xreg.RegFactory(&putFactory{})
}
