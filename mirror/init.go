// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/xreg"
)

func init() {
	xreg.RegBckXact(&cpyFactory{kind: cmn.ActCopyBck})
	xreg.RegBckXact(&cpyFactory{kind: cmn.ActETLBck})
	xreg.RegBckXact(&mncFactory{})
	xreg.RegBckXact(&putFactory{})
}
