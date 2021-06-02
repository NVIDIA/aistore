// Package soakprim provides the framework for running soak tests
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package soakprim

import (
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/bench/soaktest/report"
	"github.com/NVIDIA/aistore/bench/soaktest/soakcmn"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

const (
	soakPrefix              = "soaktest-prim"
	maxRestoreTargetAttempt = 30
)

var (
	uniqueProcessPrefix = fmt.Sprintf("%s-%d-", soakPrefix, os.Getpid())

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
		if r := recover(); r != nil { // Prevents panic from double close of RunningCh
			fmt.Println("Recovered", r)
		}
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
	cleanupAllRecipeBuckets()
	cleanupRegression()
}

func updateSysInfo() {
	systemInfoStats, err := api.GetClusterSysInfo(soakcmn.BaseAPIParams(primaryURL))
	if err == nil {
		report.WriteSystemInfoStats(&systemInfoStats)
	} else {
		report.Writef(report.SummaryLevel, "Encountered error while writing systemInfoStats: %v\n", err)
	}

	totalCapacity = 0
	for _, v := range systemInfoStats.Target {
		totalCapacity += v.Total
	}

	recCapacity = int64(float64(totalCapacity/100) * soakcmn.Params.RecPctCapacity)
	regCapacity = int64(float64(totalCapacity/100) * soakcmn.Params.RegPctCapacity)
}

func (rctx *RecipeContext) restoreTargets() {
	smap := fetchSmap("restoreTargets")
	for i := 0; i < maxRestoreTargetAttempt; i++ {
		missingOne := false
		for k, v := range rctx.origTargets {
			if _, ok := smap.Tmap[k]; ok {
				continue
			}
			if _, err := soakcmn.JoinCluster(primaryURL, v); err != nil {
				report.Writef(report.SummaryLevel, "got error while re-registering %s: %v", v.ID(), err)
			}
			missingOne = true
		}
		if !missingOne {
			return
		}
		time.Sleep(500 * time.Millisecond)
		smap = fetchSmap("restoreTargets")
		if i > 0 {
			report.Writef(report.SummaryLevel, "Attempt %d/%d to re-register targets", i, maxRestoreTargetAttempt)
		}
	}
}

func (rctx *RecipeContext) PreRecipe(recipeName string) {
	if rctx.repCtx == nil {
		rctx.repCtx = report.NewReportContext()
	}

	// Do recipe level setup
	rctx.primitiveCount = map[string]int{}
	rctx.failedPrimitives = map[string]error{}
	rctx.targetMutex = &sync.Mutex{}
	rctx.wg = &sync.WaitGroup{}

	bckNames := fetchBuckets("PreRecipe")
	for _, bckName := range bckNames {
		report.Writef(report.SummaryLevel, "found extraneous bucket: %v\n", bckNamePrefix(bckName))
		api.DestroyBucket(soakcmn.BaseAPIParams(primaryURL), bckNamePrefix(bckName))
	}

	smap := fetchSmap("PreRecipe")
	rctx.origTargets = smap.Tmap

	rctx.repCtx.BeginRecipe(recipeName)

	if !soakcmn.Params.RecRegDisable {
		rctx.StartRegression()
	}
}

func (rctx *RecipeContext) PostRecipe() error {
	// Do recipe level cleanup
	rctx.primitiveCount = map[string]int{}
	rctx.failedPrimitives = map[string]error{}

	rctx.restoreTargets()

	smap := fetchSmap("PostRecipe")
	if len(rctx.origTargets) != smap.CountActiveTargets() {
		origTargStr := cos.MustMarshal(rctx.origTargets)
		newTargStr := cos.MustMarshal(smap.Tmap)

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

	return rctx.repCtx.EndRecipe()
}

func cleanupRecipeBuckets() {
	bckNames := fetchBuckets("PostRecipe")
	for _, bckName := range bckNames {
		api.DestroyBucket(soakcmn.BaseAPIParams(primaryURL), bckNamePrefix(bckName))
	}
}

func cleanupAllRecipeBuckets() {
	if soakcmn.Params.LocalCleanup {
		return
	}

	bcks, _ := api.ListBuckets(soakcmn.BaseAPIParams(primaryURL), cmn.QueryBcks{Provider: cmn.ProviderAIS})
	for _, bck := range bcks {
		if strings.HasPrefix(bck.Name, soakPrefix) {
			api.DestroyBucket(soakcmn.BaseAPIParams(primaryURL), bck)
		}
	}
}

func bckNamePrefix(bckName string) cmn.Bck {
	return cmn.Bck{
		Name:     uniqueProcessPrefix + bckName,
		Provider: cmn.ProviderAIS,
	}
}

func bckNameWithoutPrefix(bckName string) (res string) {
	if strings.HasPrefix(bckName, uniqueProcessPrefix) {
		return strings.TrimPrefix(bckName, uniqueProcessPrefix)
	}
	return ""
}

// fetchBuckets returns a list of buckets in the proxy without the soakPrefix
func fetchBuckets(tag string) []string {
	bcks, err := api.ListBuckets(soakcmn.BaseAPIParams(primaryURL), cmn.QueryBcks{Provider: cmn.ProviderAIS})
	if err != nil {
		cos.AssertNoErr(fmt.Errorf("error fetching buckets for %v: %v", tag, err.Error()))
	}

	var res []string
	for _, bck := range bcks {
		if bck.Name = bckNameWithoutPrefix(bck.Name); bck.Name != "" {
			res = append(res, bck.Name)
		}
	}

	sort.Strings(res)
	return res
}

func fetchSmap(tag string) *cluster.Smap {
	smap, err := api.GetClusterMap(soakcmn.BaseAPIParams(primaryURL))
	if err != nil {
		cos.Assertf(false, "failed to fetch smap for %s: %v", tag, err)
	}
	return smap
}
