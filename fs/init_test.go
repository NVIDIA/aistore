// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package fs_test

import (
	"github.com/NVIDIA/aistore/cmn"
)

func init() {
	config := cmn.GCO.BeginUpdate()
	config.Log.Level = "3"
	cmn.GCO.CommitUpdate(config)
}
