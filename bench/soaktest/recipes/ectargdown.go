// Package recipes contains all the recipes for soak test
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package recipes

import (
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/bench/soaktest/soakprim"
	"github.com/NVIDIA/aistore/cmn"
)

// Basic test for ec by bringing a target down
func recECTargetDown(rctx *soakprim.RecipeContext) {
	conds := &soakprim.PreConds{
		NumTargets: 4,
	}
	rctx.Pre(conds)
	rctx.MakeBucket("ec1")
	rctx.MakeBucket("b2")
	rctx.Post(nil)

	conds.ExpBuckets = []string{"ec1", "b2"}
	rctx.Pre(conds)
	rctx.SetBucketProps("ec1", &cmn.BucketPropsToUpdate{
		EC: &cmn.ECConfToUpdate{
			Enabled:      api.Bool(true),
			ObjSizeLimit: api.Int64(1),
			DataSlices:   api.Int(2),
			ParitySlices: api.Int(1),
		},
	})
	rctx.Post(nil)

	rctx.Pre(conds)
	rctx.Put("ec1", time.Second*12, 10)
	rctx.Put("b2", time.Second*12, 10)
	rctx.Post(nil)

	// Give EC some extra time to create parity slices
	rctx.Pre(conds)
	rctx.Get("b2", time.Second*20, true, 0, 0)
	rctx.Post(nil)

	postConds := soakprim.GetPostConds()
	rctx.Pre(conds)
	rctx.Get("ec1", time.Second*50, true, 0, 0)
	rctx.Get("b2", time.Second*50, true, 0, 0)
	rctx.RemoveTarget(postConds, time.Second*5)
	rctx.Post(postConds)
}
