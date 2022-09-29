// Package objwalk provides common context and helper methods for object listing and
// object querying.
/*
 * Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
 */
package objwalk

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
)

var (
	allmap map[string]cos.BitFlags
)

func init() {
	allmap = make(map[string]cos.BitFlags, len(apc.GetPropsAll))
	for i, n := range apc.GetPropsAll {
		allmap[n] = cos.BitFlags(1) << i
	}
}
