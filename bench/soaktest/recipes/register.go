// All tests registered here

package recipes

import (
	"math/rand"

	"github.com/NVIDIA/aistore/bench/soaktest/report"

	"github.com/NVIDIA/aistore/bench/soaktest/soakprim"
)

//Special file for registering recipes

var (
	loadedRecipes = []*Recipe{
		&Recipe{"Basic Put", recBasicPut, true},
		&Recipe{"Basic Target Down", recBasicTargDown, true},

		&Recipe{"Cycle Put", recCyclePut, false},
	}

	rnd = rand.New(rand.NewSource(1234))
)

type Recipe struct {
	Name string

	run   func(*soakprim.RecipeContext)
	short bool
}

func (rec *Recipe) RunRecipe(rctx *soakprim.RecipeContext) {
	defer func() {
		if r := recover(); r != nil {
			report.Writef(report.SummaryLevel, "Recipe %v failed: %v\n", rec.Name, r)
		}

		if err := rctx.PostRecipe(); err != nil {
			report.Writef(report.SummaryLevel, "Error while running PostRecipe: %v\n", err)
		}
		report.Flush()
	}()
	rctx.PreRecipe(rec.Name)

	report.Writef(report.ConsoleLevel, "[recipe %s started]\n", rec.Name)
	rec.run(rctx)
	report.Writef(report.ConsoleLevel, "[recipe %s finished]\n", rec.Name)
}

func GetShuffledRecipeList(short bool) []*Recipe {
	var shuffledRecipes []*Recipe
	perm := rnd.Perm(len(loadedRecipes))
	for _, randIndex := range perm {
		r := loadedRecipes[randIndex]
		if short && !r.short {
			continue
		}

		shuffledRecipes = append(shuffledRecipes, r)
	}

	return shuffledRecipes
}
