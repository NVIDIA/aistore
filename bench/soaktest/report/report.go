// Package report provides the framework for collecting results of the soaktest
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package report

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/bench/soaktest/soakcmn"
	"github.com/NVIDIA/aistore/bench/soaktest/stats"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// Uses the stats package to generate csv reports of the soaktest.
// Maintains awareness of recipe, phase and primitive

type Context struct {
	currentRecipe string
	recipeNumber  map[string]int

	phaseNumber int

	primitiveStatsPhaseQueue []*stats.PrimitiveStat
	primStatsMux             sync.Mutex

	recipeGetStats stats.RecipeStats
	recipePutStats stats.RecipeStats

	recipeCfgStats stats.RecipeStats
}

func NewReportContext() *Context {
	return &Context{
		primitiveStatsPhaseQueue: make([]*stats.PrimitiveStat, 0),
		recipeNumber:             make(map[string]int),
	}
}

func (rctx *Context) RecordRegression(stat *stats.PrimitiveStat) {
	stat.RecipeName = rctx.currentRecipe
	stat.RecipeNum = rctx.recipeNumber[stat.RecipeName]

	regressionWriter.WriteStat(stat)
}

func (rctx *Context) BeginRecipe(name string) {
	val, ok := rctx.recipeNumber[name]
	if !ok {
		val = 1
	} else {
		val++
	}
	rctx.recipeNumber[name] = val

	rctx.currentRecipe = name

	timestamp := time.Now()
	rctx.recipeGetStats = stats.RecipeStats{RecipeName: name, RecipeNum: val, OpType: soakcmn.OpTypeGet, BeginTime: timestamp}
	rctx.recipePutStats = stats.RecipeStats{RecipeName: name, RecipeNum: val, OpType: soakcmn.OpTypePut, BeginTime: timestamp}

	rctx.recipeCfgStats = stats.RecipeStats{RecipeName: name, RecipeNum: val, OpType: soakcmn.OpTypeCfg, BeginTime: timestamp}

	rctx.phaseNumber = 0
}

// EndRecipe should be called after the recipe
func (rctx *Context) EndRecipe() error {
	timestamp := time.Now()
	rctx.recipeGetStats.EndTime = timestamp
	rctx.recipePutStats.EndTime = timestamp

	summaryWriter.WriteStat(rctx.recipeGetStats)
	summaryWriter.WriteStat(rctx.recipePutStats)

	rctx.recipeCfgStats.EndTime = timestamp
	// Not all recipes get cfg, don't log if not used
	if rctx.recipeCfgStats.HasData() {
		summaryWriter.WriteStat(rctx.recipeCfgStats)
	}

	rctx.currentRecipe = ""

	return nil
}

// FlushRecipePhase should be called after each phase
func (rctx *Context) FlushRecipePhase() {
	rctx.phaseNumber++
	Writef(ConsoleLevel, "[Phase %v]\n", rctx.phaseNumber)

	// No primitives in this phase
	if len(rctx.primitiveStatsPhaseQueue) == 0 {
		Writef(DetailLevel, "skipping flush stats, none found in phase...\n")
		return
	}

	// all of the following should be put to logs
	for _, stat := range rctx.primitiveStatsPhaseQueue {
		stat.RecipeName = rctx.currentRecipe
		stat.RecipeNum = rctx.recipeNumber[stat.RecipeName]

		b := cos.MustMarshal(stat)
		Writef(DetailLevel, string(b))

		detailWriter.WriteStat(stat)

		if stat.OpType == soakcmn.OpTypeGet || stat.Fatal {
			rctx.recipeGetStats.Add(stat)
		}
		if stat.OpType == soakcmn.OpTypePut || stat.Fatal {
			rctx.recipePutStats.Add(stat)
		}
		if stat.OpType == soakcmn.OpTypeCfg || stat.Fatal {
			rctx.recipeCfgStats.Add(stat)
		}
	}

	rctx.primitiveStatsPhaseQueue = rctx.primitiveStatsPhaseQueue[:0]
}

func (rctx *Context) PutPrimitiveStats(p *stats.PrimitiveStat) {
	rctx.primStatsMux.Lock()
	rctx.primitiveStatsPhaseQueue = append(rctx.primitiveStatsPhaseQueue, p)
	rctx.primStatsMux.Unlock()
}

// The system info stats don't need a context
func WriteSystemInfoStats(systemInfoStats *cmn.ClusterSysInfo) {
	sysinfoStats := stats.ParseClusterSysInfo(systemInfoStats, time.Now())
	for _, st := range sysinfoStats {
		sysinfoWriter.WriteStat(st)
	}
}
