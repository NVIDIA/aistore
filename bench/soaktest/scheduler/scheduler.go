package scheduler

import (
	"github.com/NVIDIA/aistore/bench/soaktest/recipes"
	"github.com/NVIDIA/aistore/bench/soaktest/soakprim"
)

var (
	RunShort  bool
	NumCycles int
)

//RunRandom runs all recipes in random order
func RunRandom() {
	rctx := &soakprim.RecipeContext{}

	for i := 0; NumCycles == 0 || i < NumCycles; i++ {
		recipeList := recipes.GetShuffledRecipeList(RunShort)
		for _, r := range recipeList {
			r.RunRecipe(rctx)
			if soakprim.Terminated {
				return
			}
		}
	}

}
