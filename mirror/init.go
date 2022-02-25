// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/xact/xreg"
)

func Init() {
	xreg.RegBckXact(&tcbFactory{kind: apc.ActCopyBck})
	xreg.RegBckXact(&tcbFactory{kind: apc.ActETLBck})
	xreg.RegBckXact(&mncFactory{})
	xreg.RegBckXact(&putFactory{})
}
