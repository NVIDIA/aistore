package recipes

import (
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/bench/soaktest/soakprim"
	"github.com/NVIDIA/aistore/cmn"
)

func recCyclePut(rctx *soakprim.RecipeContext) {
	//cycles through the 3 buckets, putting in cur, reading from last, and deleting last2

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
	rctx.MakeBucket(genBckName(cur), &cmn.BucketProps{})
	rctx.MakeBucket(genBckName(last), &cmn.BucketProps{})
	rctx.MakeBucket(genBckName(last2), &cmn.BucketProps{})
	rctx.Post(nil)

	conds.ExpBuckets = []string{genBckName(cur), genBckName(last), genBckName(last2)}
	rctx.Pre(conds)
	rctx.Put(genBckName(last), 4*cmn.GiB, 1024*cmn.KiB, 500*cmn.MiB, 2)
	rctx.Put(genBckName(last2), 4*cmn.GiB, 1024*cmn.KiB, 500*cmn.MiB, 2)
	rctx.Post(nil)

	for i := 0; i < 50; i++ {
		conds.ExpBuckets = []string{genBckName(cur), genBckName(last), genBckName(last2)}
		next := cur + 1
		rctx.MakeBucket(genBckName(next), &cmn.BucketProps{})
		rctx.Put(genBckName(cur), 500*cmn.MiB, 5*cmn.MiB, 100*cmn.MiB, 2)
		rctx.Get(genBckName(last), time.Second*3, true, 4, 0, 0)
		rctx.Destroy(genBckName(last2))
		rctx.Post(nil)

		last2 = last
		last = cur
		cur = next
	}

	conds.ExpBuckets = []string{genBckName(last), genBckName(last2)}
	rctx.Pre(conds)
	rctx.Destroy(genBckName(last))
	rctx.Destroy(genBckName(last2))
	rctx.Post(nil)
}
