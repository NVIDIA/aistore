// Package soakprim provides the framework for running soak tests
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package soakprim

import (
	"fmt"
	"os"
	"sort"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/bench/soaktest/report"
	"github.com/NVIDIA/aistore/bench/soaktest/soakcmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

type PreConds struct {
	ExpBuckets []string // non-prefixed
	NumTargets int
}

type PostConds struct {
	NumTargets int
}

// Pre checks are run before a phase of a recipe, if not met, the recipe halts
func (*RecipeContext) Pre(conds *PreConds) {
	defer func() {
		if Terminated {
			report.Writef(report.SummaryLevel, "User terminated in pre ...")
			os.Exit(1)
		}
	}()

	if conds == nil {
		return
	}

	if conds.ExpBuckets != nil {
		expBuckets := conds.ExpBuckets
		sort.Strings(expBuckets)

		actBuckets := fetchBuckets("pre")

		var toDelete []string
		missing := false
		for i, j := 0, 0; i < len(actBuckets) || j < len(expBuckets); {
			if i == len(actBuckets) || (j < len(expBuckets) && expBuckets[j] < actBuckets[i]) {
				missing = true
				break
			} else if j == len(expBuckets) || (i < len(actBuckets) && actBuckets[i] < expBuckets[j]) {
				toDelete = append(toDelete, actBuckets[i])
				i++
			} else {
				i++
				j++
			}
		}

		if missing {
			eStr := cos.MustMarshal(expBuckets)
			aStr := cos.MustMarshal(actBuckets)
			cos.AssertNoErr(fmt.Errorf("missing buckets in pre, expected: %v, actual: %v", string(eStr), string(aStr)))
		}

		for _, bckName := range toDelete {
			prefixedBckName := bckNamePrefix(bckName)
			api.DestroyBucket(soakcmn.BaseAPIParams(primaryURL), prefixedBckName)
			report.Writef(report.SummaryLevel, "Pre: deleted extraneous bucket: %v\n", prefixedBckName)
		}
	}

	if conds.NumTargets > 0 {
		smap := fetchSmap("pre")
		actual := smap.CountTargets()
		if actual < conds.NumTargets {
			cos.AssertNoErr(fmt.Errorf("too few targets in pre, required: %v, actual: %v", conds.NumTargets, actual))
		}
	}
}

// Post checks are run after a recipe, to see if there's degradation
func (rctx *RecipeContext) Post(conds *PostConds) {
	defer func() {
		report.Flush()

		if Terminated {
			report.Writef(report.SummaryLevel, "User terminated in post ...\n")
			os.Exit(1)
		}
	}()

	report.Writef(report.DetailLevel, "Post: Starting wait...\n")
	rctx.wg.Wait()
	report.Writef(report.DetailLevel, "Post: Finished wait...\n")

	rctx.repCtx.FlushRecipePhase()

	for tag, err := range rctx.failedPrimitives {
		report.Writef(report.SummaryLevel, "Primitive %v failed: %v\n", tag, err)
	}
	rctx.failedPrimitives = map[string]error{}

	if conds == nil {
		return
	}

	if conds.NumTargets > 0 {
		numTargets := fetchSmap("GetPostConds").CountTargets()
		if numTargets != conds.NumTargets {
			report.Writef(report.SummaryLevel, "Post: wrong # of targets, exp: %v, actual %v\n", conds.NumTargets, numTargets)
		}

		updateSysInfo() // Fetch new capacity since # of targets changed
	}
}

func GetPostConds() *PostConds {
	res := &PostConds{}

	smap := fetchSmap("GetPostConds")
	res.NumTargets = smap.CountTargets()

	return res
}
