// Package recipes contains all the recipes for soak test
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package recipes

import (
	"time"

	"github.com/NVIDIA/aistore/bench/soaktest/soakprim"
)

// Basic recipe with a target going down.
// NOTE: It does not restore the target at the end and expects the recipe framework to do so.
func recBasicTargetDown(rctx *soakprim.RecipeContext) {
	conds := &soakprim.PreConds{
		NumTargets: 3,
	}
	rctx.Pre(conds)
	rctx.MakeBucket("d1")
	rctx.MakeBucket("d2")
	rctx.Post(nil)

	conds.ExpBuckets = []string{"d1", "d2"}
	rctx.Pre(conds)
	rctx.Put("d1", time.Second*10, 8)
	rctx.Put("d2", time.Second*10, 10)
	rctx.Post(nil)

	postConds := soakprim.GetPostConds()
	rctx.Pre(conds)
	rctx.Put("d1", time.Second*10, 5)
	rctx.Get("d2", time.Second*8, true, 0, 0)
	// Target down during get, causes some errors
	rctx.RemoveTarget(postConds, time.Second*3)
	rctx.Post(postConds)

	rctx.Pre(conds)
	rctx.Put("d2", time.Second*10, 3)
	rctx.Get("d1", time.Second*8, true, 0, 0)
	rctx.RestoreTarget(postConds, time.Second*3)
	rctx.Post(postConds)

	rctx.Pre(conds)
	rctx.Get("d1", time.Second*5, true, 0, 0)
	rctx.Destroy("d2")
	rctx.RemoveTarget(postConds, 0)
	rctx.Post(postConds)
}
