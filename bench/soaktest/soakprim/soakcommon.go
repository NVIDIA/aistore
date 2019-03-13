// Package soakprim provides the framework for running soak tests
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package soakprim

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/NVIDIA/aistore/bench/soaktest/soakcmn"

	"github.com/NVIDIA/aistore/cmn"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/bench/soaktest/report"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/tutils"
)

const (
	soakprefix = "soaktest-prim-"
)

var (
	Terminated bool
	RunningCh  chan struct{} // chan with nothing written, closed when Terimated is true

	primaryIP   string
	primaryPort string
	primaryURL  string // protected, we ensure this never changes

	totalCapacity uint64
	recCapacity   int64
	regCapacity   int64
)

type RecipeContext struct {
	primitiveCount   map[string]int
	failedPrimitives map[string]error
	origTargets      cluster.NodeMap
	targetMutex      *sync.Mutex
	wg               *sync.WaitGroup

	regCtx *regressionContext
	repCtx *report.ReportContext
}

func init() {
	Terminated = false
	RunningCh = make(chan struct{})

	terminateCh := make(chan os.Signal, 2)
	signal.Notify(terminateCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-terminateCh
		Terminate()
	}()
}

func Terminate() {
	defer func() {
		recover() //Prevents panic from double close of RunningCh
	}()
	Terminated = true
	close(RunningCh)
}

// SetPrimaryURL should be called once at start of script
func SetPrimaryURL() {
	primaryIP = soakcmn.Params.IP
	primaryPort = soakcmn.Params.Port
	primaryURL = "http://" + primaryIP + ":" + primaryPort
}

func CleanupSoak() {
	cleanupRecipeBuckets()
	cleanupRegression()
}

func updateSysInfo() {
	systemInfoStats, err := api.GetClusterSysInfo(tutils.BaseAPIParams(primaryURL))
	if err == nil {
		report.WriteSystemInfoStats(&systemInfoStats)
	} else {
		report.Writef(report.SummaryLevel, "Encountered error while writing systemInfoStats: %v\n", err)
	}

	totalCapacity = 0
	for _, v := range systemInfoStats.Target {
		totalCapacity += v.FSCapacity
	}

	recCapacity = int64(float64(totalCapacity/100) * soakcmn.Params.RecPctCapacity)
	regCapacity = int64(float64(totalCapacity/100) * soakcmn.Params.RegPctCapacity)
}

func (rctx *RecipeContext) PreRecipe(recipeName string) {
	if rctx.repCtx == nil {
		rctx.repCtx = report.NewReportContext()
	}

	//Do recipe level setup
	rctx.primitiveCount = map[string]int{}
	rctx.failedPrimitives = map[string]error{}
	rctx.targetMutex = &sync.Mutex{}
	rctx.wg = &sync.WaitGroup{}

	bckNames := fetchBuckets("PreRecipe")
	for _, bckName := range bckNames {
		report.Writef(report.SummaryLevel, "found extraneous bucket: %v\n", bckNamePrefix(bckName))
		api.DestroyLocalBucket(tutils.BaseAPIParams(primaryURL), bckNamePrefix(bckName))
	}

	smap := fetchSmap("PreRecipe")
	rctx.origTargets = smap.Tmap

	rctx.repCtx.BeginRecipe(recipeName)

	if !soakcmn.Params.RecRegDisable {
		rctx.StartRegression()
	}
}

func (rctx *RecipeContext) PostRecipe() error {
	//Do recipe level cleanup
	rctx.primitiveCount = map[string]int{}
	rctx.failedPrimitives = map[string]error{}

	smap := fetchSmap("PostRecipe")
	for k, v := range rctx.origTargets {
		_, ok := smap.Tmap[k]
		if !ok {
			tutils.RegisterTarget(primaryURL, v, *smap)
			smap = fetchSmap("PostRecipe")
		}
	}

	if len(rctx.origTargets) != smap.CountTargets() {
		origTargStr, _ := json.Marshal(rctx.origTargets)
		newTargStr, _ := json.Marshal(smap.Tmap)

		report.Writef(report.SummaryLevel,
			"post recipe target count changed\n"+
				"original targets:\n%v\n"+
				"new targets:\n%v\n",
			string(origTargStr), string(newTargStr))
	} else {
		report.Writef(report.DetailLevel, "post recipe target count unchanged\n")
	}

	cleanupRecipeBuckets()

	if !soakcmn.Params.RecRegDisable {
		rctx.FinishRegression()
	}

	if err := rctx.repCtx.EndRecipe(); err != nil {
		return err
	}
	return nil
}

func cleanupRecipeBuckets() {
	bckNames := fetchBuckets("PostRecipe")
	for _, bckName := range bckNames {
		api.DestroyLocalBucket(tutils.BaseAPIParams(primaryURL), bckNamePrefix(bckName))
	}
}

func bckNamePrefix(bucketname string) string {
	return soakprefix + bucketname
}

func bcknameDePrefix(bckName string) (res string) {
	if strings.HasPrefix(bckName, soakprefix) {
		return strings.TrimPrefix(bckName, soakprefix)
	}
	return ""
}

//fetchBuckets returns a list of buckets in the proxy without the soakprefix
func fetchBuckets(tag string) []string {
	bckNames, err := api.GetBucketNames(tutils.BaseAPIParams(primaryURL), cmn.LocalBs)

	if err != nil {
		cmn.AssertNoErr(fmt.Errorf("error fetching bucketnames for %v: %v", tag, err.Error()))
	}

	var res []string
	for _, bckName := range bckNames.Local {
		if bckName = bcknameDePrefix(bckName); bckName != "" {
			res = append(res, bckName)
		}
	}

	sort.Strings(res)
	return res
}

func fetchSmap(tag string) *cluster.Smap {
	smap, err := api.GetClusterMap(tutils.BaseAPIParams(primaryURL))

	if err != nil {
		cmn.AssertNoErr(fmt.Errorf("error fetching smap for %v: %v", tag, err))
	}

	return &smap
}
