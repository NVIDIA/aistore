// Package transport provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 *
 */
package transport_test

import (
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/hk"
)

func init() {
	config := cmn.GCO.BeginUpdate()
	config.Timeout.TransportIdleTeardown = cos.Duration(4 * time.Second)
	cmn.GCO.CommitUpdate(config)
	hk.TestInit()
}
