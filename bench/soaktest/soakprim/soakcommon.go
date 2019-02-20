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

	primaryIP   string
	primaryPort string
	primaryURL  string // protected, we ensure this never changes
)

type RecipeContext struct {
	primitiveCount   map[string]int
	failedPrimitives map[string]error
	origTargets      cluster.NodeMap
	wg               *sync.WaitGroup

	regCtx *regressionContext
	repCtx *report.ReportContext
}

func init() {
	Terminated = false
	terminateCh := make(chan os.Signal, 2)
	signal.Notify(terminateCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-terminateCh
		Terminated = true
	}()

}

// SetPrimaryURL should be called once at start of script
func SetPrimaryURL(ip string, port string) {
	primaryIP = ip
	primaryPort = port
	primaryURL = "http://" + primaryIP + ":" + primaryPort
}

func (rctx *RecipeContext) PreRecipe(recipeName string) {
	if rctx.repCtx == nil {
		rctx.repCtx = report.NewReportContext()
	}

	//Do recipe level setup
	rctx.primitiveCount = map[string]int{}
	rctx.failedPrimitives = map[string]error{}
	rctx.wg = &sync.WaitGroup{}

	bckNames := fetchBuckets("PreRecipe")
	for _, bckName := range bckNames {
		report.Writef(report.SummaryLevel, "found extraneous bucket: %v\n", bckNamePrefix(bckName))
		api.DestroyLocalBucket(tutils.BaseAPIParams(primaryURL), bckNamePrefix(bckName))
	}

	smap := fetchSmap("PreRecipe")
	rctx.origTargets = smap.Tmap

	rctx.repCtx.BeginRecipe(recipeName)

	rctx.startRegression()
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

	bckNames := fetchBuckets("PostRecipe")
	for _, bckName := range bckNames {
		api.DestroyLocalBucket(tutils.BaseAPIParams(primaryURL), bckNamePrefix(bckName))
	}

	systemInfoStats, err := api.GetClusterSysInfo(tutils.BaseAPIParams(primaryURL))
	if err != nil {
		return err
	}
	rctx.repCtx.WriteSystemInfoStats(&systemInfoStats)

	rctx.finishRegression()

	if err := rctx.repCtx.EndRecipe(); err != nil {
		return err
	}
	return nil
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
