package recipes

import (
	"time"

	"github.com/NVIDIA/aistore/bench/soaktest/soakprim"
)

func recReadPartCfg(rctx *soakprim.RecipeContext) {
	//reads parts of files and config

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
	rctx.GetCfg("b1", time.Second*10)
	rctx.Get("b2", time.Second*10, false, 30, 100) //checksum=false because check doesn't factor in range
	rctx.Post(nil)

	rctx.Pre(conds)
	rctx.Get("b1", time.Second*10, false, 0, 60)
	rctx.GetCfg("b2", time.Second*10)
	rctx.Post(nil)
}
