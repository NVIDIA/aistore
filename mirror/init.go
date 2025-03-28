// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"github.com/NVIDIA/aistore/xact/xreg"
)

func Init() {
	xreg.RegBckXact(&mncFactory{})
	xreg.RegBckXact(&putFactory{})
}
