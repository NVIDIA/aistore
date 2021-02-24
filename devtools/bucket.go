// Package devtools provides common low-level utilities for AIStore development tools.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package devtools

import (
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
)

func BckExists(ctx *Ctx, proxyURL string, bck cmn.Bck) (bool, error) {
	baseParams := BaseAPIParams(ctx, proxyURL)
	bcks, err := api.ListBuckets(baseParams, cmn.QueryBcks(bck))
	if err != nil {
		return false, err
	}
	return bcks.Contains(cmn.QueryBcks(bck)), nil
}
