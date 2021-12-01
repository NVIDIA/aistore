// Package recipes contains all the recipes for soak test
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package recipes

import (
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/bench/soaktest/soakprim"
)

// Cycles through the 3 buckets, putting in cur, reading from last, and deleting last2
func recCyclePut(rctx *soakprim.RecipeContext) {
	cur := 3
	last := 2
	last2 := 1

	conds := &soakprim.PreConds{
		ExpBuckets: []string{},
	}

	genBckName := func(n int) string {
		return "cyc" + strconv.Itoa(n)
	}

	rctx.Pre(conds)
	rctx.MakeBucket(genBckName(cur))
	rctx.MakeBucket(genBckName(last))
	rctx.MakeBucket(genBckName(last2))
	rctx.Post(nil)

	conds.ExpBuckets = []string{genBckName(cur), genBckName(last), genBckName(last2)}
	rctx.Pre(conds)
	rctx.Put(genBckName(last), time.Second*20, 25)
	rctx.Put(genBckName(last2), time.Second*20, 25)
	rctx.Post(nil)

	for i := 0; i < 50; i++ {
		conds.ExpBuckets = []string{genBckName(cur), genBckName(last), genBckName(last2)}
		next := cur + 1
		rctx.MakeBucket(genBckName(next))
		rctx.Put(genBckName(cur), time.Minute, 25)
		rctx.Get(genBckName(last), time.Second*50, true, 0, 0)
		rctx.Destroy(genBckName(last2))
		rctx.Post(nil)

		last2 = last
		last = cur
		cur = next
	}

	conds.ExpBuckets = []string{genBckName(cur), genBckName(last), genBckName(last2)}
	rctx.Pre(conds)
	rctx.Destroy(genBckName(last))
	rctx.Destroy(genBckName(last2))
	rctx.Post(nil)
}
