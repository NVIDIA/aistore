package recipes

import (
	"time"

	"github.com/NVIDIA/aistore/bench/soaktest/soakprim"
	"github.com/NVIDIA/aistore/cmn"
)

func recBasicPut(rctx *soakprim.RecipeContext) {
	//most basic recipe that puts some stuff in buckets,
	// the 'hello world' of recipes

	conds := &soakprim.PreConds{
		ExpBuckets: []string{},
	}

	rctx.Pre(conds)
	rctx.MakeBucket("b1", &cmn.BucketProps{})
	rctx.MakeBucket("b2", &cmn.BucketProps{EC: cmn.ECConf{Enabled: true}})
	rctx.Post(nil)

	conds.ExpBuckets = []string{"b2", "b1"}
	rctx.Pre(conds)
	rctx.Put("b1", 4*cmn.GiB, 1024*cmn.KiB, 500*cmn.MiB, 2)
	rctx.Put("b2", 4*cmn.GiB, 1024*cmn.KiB, 500*cmn.MiB, 2)
	rctx.Post(nil)

	conds.ExpBuckets = []string{"b2", "b1"}
	rctx.Pre(conds)
	rctx.Destroy("b2")
	rctx.Get("b1", time.Second*10, true, 4, 0, 0)
	rctx.Get("b1", time.Second*5, true, 4, 0, 0)
	rctx.Post(nil)

	conds.ExpBuckets = []string{"b1"}
	rctx.Pre(conds)
	rctx.Destroy("b1")
	conds.ExpBuckets = []string{}
	rctx.Post(nil)
}
