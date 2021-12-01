// Package recipes contains all the recipes for soak test
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package recipes

import (
	"time"

	"github.com/NVIDIA/aistore/bench/soaktest/soakprim"
)

// Reads parts of objects and config
func recReadPartCfg(rctx *soakprim.RecipeContext) {
	conds := &soakprim.PreConds{
		ExpBuckets: []string{},
	}

	rctx.Pre(conds)
	rctx.MakeBucket("b1")
	rctx.MakeBucket("b2")
	rctx.Post(nil)

	conds.ExpBuckets = []string{"b2", "b1"}
	rctx.Pre(conds)
	rctx.Put("b1", time.Second*8, 30)
	rctx.Put("b2", time.Second*10, 20)
	rctx.Post(nil)

	rctx.Pre(conds)
	rctx.GetCfg(time.Second * 10)
	rctx.Get("b1", time.Second*10, false, 0, 60)
	rctx.Get("b2", time.Second*10, false, 30, 100) // checksum=false because check doesn't factor in range
	rctx.Post(nil)
}
