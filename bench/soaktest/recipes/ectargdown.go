// Package recipes contains all the recipes for soak test
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package recipes

import (
	"time"

	"github.com/NVIDIA/aistore/bench/soaktest/soakprim"
	"github.com/NVIDIA/aistore/cmn"
)

func recECTargDown(rctx *soakprim.RecipeContext) {
	// basic test for ec by bringing a target down

	conds := &soakprim.PreConds{
		NumTargets: 4,
	}
	rctx.Pre(conds)
	rctx.MakeBucket("ec1")
	rctx.MakeBucket("ec2")
	rctx.Post(nil)

	conds.ExpBuckets = []string{"ec1", "ec2"}
	rctx.Pre(conds)
	rctx.SetBucketProps("ec1", cmn.BucketProps{EC: cmn.ECConf{Enabled: true, DataSlices: 2, ParitySlices: 2}})
	rctx.SetBucketProps("ec2", cmn.BucketProps{EC: cmn.ECConf{Enabled: true, DataSlices: 2, ParitySlices: 2}})
	rctx.Post(nil)

	rctx.Pre(conds)
	rctx.Put("ec1", time.Second*12, 10)
	rctx.Put("ec2", time.Second*12, 10)
	rctx.Post(nil)

	// Give EC some extra time to create parity slices
	rctx.Pre(conds)
	rctx.Get("ec1", time.Second*20, true, 0, 0)
	rctx.Get("ec2", time.Second*20, true, 0, 0)
	rctx.Post(nil)

	postConds := soakprim.GetPostConds()
	rctx.Pre(conds)
	rctx.Get("ec1", time.Second*20, true, 0, 0)
	rctx.Get("ec2", time.Second*20, true, 0, 0)
	rctx.RemoveTarget(postConds, time.Second*5)
	rctx.Post(postConds)
}
