// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/xact/xreg"
)

func Init() {
	xreg.RegBckXact(&tcbFactory{kind: cmn.ActCopyBck})
	xreg.RegBckXact(&tcbFactory{kind: cmn.ActETLBck})
	xreg.RegBckXact(&mncFactory{})
	xreg.RegBckXact(&putFactory{})
}
