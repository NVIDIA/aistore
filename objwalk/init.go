// Package objwalk provides common context and helper methods for object listing and
// object querying.
/*
 * Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
 */
package objwalk

import (
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
)

var (
	allmap map[string]cos.BitFlags
)

func init() {
	names := strings.Split(apc.AllListObjectsProps, ",")
	allmap = make(map[string]cos.BitFlags, len(names))
	for i, n := range names {
		allmap[n] = cos.BitFlags(1) << i
	}
}
