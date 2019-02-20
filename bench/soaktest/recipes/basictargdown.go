package recipes

import (
	"time"

	"github.com/NVIDIA/aistore/bench/soaktest/soakprim"
	"github.com/NVIDIA/aistore/cmn"
)

func recBasicTargDown(rctx *soakprim.RecipeContext) {
	//basic recipe with a target going down,
	// note that it does not restore the target at the end and expects the recipe framework for doing so

	conds := &soakprim.PreConds{
		NumTargets: 3,
	}
	rctx.Pre(conds)
	rctx.MakeBucket("d1", &cmn.BucketProps{})
	rctx.MakeBucket("d2", &cmn.BucketProps{})
	rctx.Post(nil)

	conds.ExpBuckets = []string{"d1", "d2"}
	rctx.Pre(conds)
	rctx.Put("d1", 4*cmn.GiB, 1024*cmn.KiB, 500*cmn.MiB, 2)
	rctx.Put("d2", 4*cmn.GiB, 1024*cmn.KiB, 500*cmn.MiB, 2)
	rctx.Post(nil)

	postConds := soakprim.GetPostConds()
	rctx.Pre(conds)
	rctx.Put("d1", 2*cmn.GiB, 1024*cmn.KiB, 500*cmn.MiB, 2)
	rctx.Get("d2", time.Second*10, true, 4, 0, 0)
	rctx.RemoveTarget(postConds)
	rctx.Post(postConds)

	rctx.Pre(conds)
	rctx.Put("d2", 2*cmn.GiB, 1024*cmn.KiB, 500*cmn.MiB, 2)
	rctx.Get("d1", time.Second*10, true, 4, 0, 0)
	rctx.RestoreTarget(postConds)
	rctx.Post(postConds)

	rctx.Pre(conds)
	rctx.Get("d1", time.Second*10, true, 4, 0, 0)
	rctx.Destroy("d2")
	rctx.RemoveTarget(postConds)
	rctx.Post(postConds)

}
