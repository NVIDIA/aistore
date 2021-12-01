// Package scheduler provides scheduling of recipes and regression within soaktest
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package scheduler

import (
	"time"

	"github.com/NVIDIA/aistore/bench/soaktest/recipes"
	"github.com/NVIDIA/aistore/bench/soaktest/report"
	"github.com/NVIDIA/aistore/bench/soaktest/soakcmn"
	"github.com/NVIDIA/aistore/bench/soaktest/soakprim"
)

// RunRegressionPhase returns bool indicating if the soak run should terminate
func RunRegressionPhase(rctx *soakprim.RecipeContext) {
	report.Writef(report.ConsoleLevel, "[starting %v regression]\n", soakcmn.Params.RegPhaseDuration)
	rctx.StartRegression()

	timer := time.NewTimer(soakcmn.Params.RegPhaseDuration)

	select {
	case <-timer.C:
		break
	case <-soakprim.RunningCh:
		break
	}
	rctx.FinishRegression()
	report.Writef(report.ConsoleLevel, "[finished regression]\n")
}

// RunRandom runs all recipes in random order
func RunRandom() {
	rctx := &soakprim.RecipeContext{}

	if soakcmn.Params.RecDisable {
		for !soakprim.Terminated {
			RunRegressionPhase(rctx)
		}
		return
	}

	for i := 0; soakcmn.Params.NumCycles == 0 || i < soakcmn.Params.NumCycles; i++ {
		recipeList := recipes.GetShuffledRecipeList()
		for _, r := range recipeList {
			r.RunRecipe(rctx)
			if soakprim.Terminated {
				return
			}

			if soakcmn.Params.RegPhaseDisable {
				continue
			}
			RunRegressionPhase(rctx)
			if soakprim.Terminated {
				return
			}
		}
	}
}
