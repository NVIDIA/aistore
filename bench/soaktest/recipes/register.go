// Package recipes contains all the recipes for soak test
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package recipes

// special file where all the recipes are registered

import (
	"html/template"
	"math/rand"
	"os"
	"text/tabwriter"

	"github.com/NVIDIA/aistore/bench/soaktest/report"
	"github.com/NVIDIA/aistore/bench/soaktest/soakcmn"
	"github.com/NVIDIA/aistore/bench/soaktest/soakprim"
	"github.com/NVIDIA/aistore/cmn/cos"
)

const (
	recipeTmpl = "RecipeID\t Name\t Short\t Description\n" +
		"{{ range $key, $value := . }}" +
		"{{$value.RecipeID}}\t {{$value.Name}}\t {{$value.Short}}\t {{$value.Description}}\n" +
		"{{end}}\n"
)

var (
	loadedRecipes = []*Recipe{
		{
			1, "Basic PUT GET", true, false,
			"basic recipe that PUTs and GETs into buckets, considered the `hello world` of recipes",
			recBasicPut,
		},
		{
			2, "Basic Target Down", true, true,
			"basic recipe where a target goes down and comes back up during PUT/GET",
			recBasicTargetDown,
		},
		{
			3, "EC Target Down", true, true,
			"basic recipe for EC by performing GET while a target is down",
			recECTargetDown,
		},
		{
			4, "Read Part Config", true, false,
			"recipe that reads parts of files and config from bucket",
			recReadPartCfg,
		},
		{
			5, "Cycle PUT GET", false, false,
			"constantly cycles through buckets, running PUT in one, GET in another, and deleting the last",
			recCyclePut,
		},
	}

	rnd = rand.New(rand.NewSource(1))
)

type Recipe struct {
	RecipeID int
	Name     string

	Short        bool
	KillsTargets bool
	Description  string

	run func(*soakprim.RecipeContext)
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

func GetShuffledRecipeList() []*Recipe {
	perm := rnd.Perm(len(loadedRecipes))
	shuffledRecipes := make([]*Recipe, 0, len(perm))
	for _, randIndex := range perm {
		r := loadedRecipes[randIndex]

		if soakcmn.Params.RecSet != nil {
			if _, ok := soakcmn.Params.RecSet[r.RecipeID]; !ok {
				continue
			}
		} else if soakcmn.Params.Short && !r.Short {
			continue
		} else if soakcmn.Params.KeepTargets && r.KillsTargets {
			continue
		}

		shuffledRecipes = append(shuffledRecipes, r)
	}

	return shuffledRecipes
}

func GetValidRecipeIDs() map[int]struct{} {
	v := make(map[int]struct{})
	for _, x := range loadedRecipes {
		v[x.RecipeID] = struct{}{}
	}
	return v
}

func PrintRecipes() {
	tmpl, err := template.New("List Template").Parse(recipeTmpl)
	cos.AssertNoErr(err)

	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 8, 0, '\t', 0)
	err = tmpl.Execute(w, loadedRecipes)
	cos.AssertNoErr(err)

	w.Flush()
}
