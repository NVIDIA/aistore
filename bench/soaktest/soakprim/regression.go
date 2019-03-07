package soakprim

import (
	"sync"

	"github.com/NVIDIA/aistore/bench/soaktest/report"
	"github.com/NVIDIA/aistore/bench/soaktest/soakcmn"

	"github.com/NVIDIA/aistore/tutils"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/bench/soaktest/stats"
)

const (
	regBucket = "soaktest-reg-0"
)

type regressionContext struct {
	bckName string
	wg      *sync.WaitGroup
	stopCh  chan struct{}
}

//regression runs a constant get request throughout the testing

func init() {
	if exists, _ := tutils.DoesLocalBucketExist(primaryURL, regBucket); exists {
		api.DestroyLocalBucket(tutils.BaseAPIParams(primaryURL), regBucket)
	}
	api.CreateLocalBucket(tutils.BaseAPIParams(primaryURL), regBucket)
}

func cleanupRegression() {
	if exists, _ := tutils.DoesLocalBucketExist(primaryURL, regBucket); exists {
		api.DestroyLocalBucket(tutils.BaseAPIParams(primaryURL), regBucket)
	}
}

func setupRegression() *regressionContext {
	report.Writef(report.ConsoleLevel, "Setting up regression (maximum %v)...\n", regCapacity)

	params := &AISLoaderExecParams{
		pctput:       100,
		totalputsize: regCapacity,
		duration:     soakcmn.Params.RegSetupDuration,
		minsize:      soakcmn.Params.RegMinFilesize,
		maxsize:      soakcmn.Params.RegMaxFilesize,
	}
	aisStopCh := make(chan *stats.PrimitiveStat, 1)
	AISExec(aisStopCh, regBucket, soakcmn.Params.RegSetupWorkers, params)
	setupStat := <-aisStopCh
	close(aisStopCh)

	report.Writef(report.ConsoleLevel, "Done setting up regression (actual size %v) ...\n", setupStat.TotalSize)

	regctx := &regressionContext{}
	regctx.bckName = regBucket //all regression tests share a bucket for now

	return regctx
}

// worker function for regression. Must call in go func
func regressionWorker(stopCh chan struct{}, wg *sync.WaitGroup, recordRegression func(*stats.PrimitiveStat)) {
	wg.Add(1)

	aisLoaderExecParams := &AISLoaderExecParams{
		pctput:     0,
		stopable:   true,
		stopCh:     make(chan struct{}),
		verifyhash: false,

		duration:     0,
		totalputsize: 0,
	}

	aisExecResultCh := make(chan *stats.PrimitiveStat, 1)

	aisExecWg := &sync.WaitGroup{}
	aisExecWg.Add(1)

	go func() {
		defer aisExecWg.Done()
		AISExec(aisExecResultCh, regBucket, soakcmn.Params.RegWorkers, aisLoaderExecParams)
	}()

	<-stopCh

	aisLoaderExecParams.stopCh <- struct{}{}
	aisExecWg.Wait()

	stat := <-aisExecResultCh
	recordRegression(stat)

	wg.Done()
}

func (rctx *RecipeContext) StartRegression() {
	//Record system stats when we start
	updateSysInfo()

	if rctx.regCtx == nil {
		rctx.regCtx = setupRegression()
	}

	regCtx := rctx.regCtx
	regCtx.stopCh = make(chan struct{})
	regCtx.wg = &sync.WaitGroup{}
	if rctx.repCtx == nil {
		rctx.repCtx = report.NewReportContext()
	}
	go regressionWorker(regCtx.stopCh, regCtx.wg, rctx.repCtx.RecordRegression)
}

func (rctx *RecipeContext) FinishRegression() {
	regCtx := rctx.regCtx

	if regCtx == nil {
		return
	}

	//Record system stats when we finish
	updateSysInfo()

	if regCtx.wg != nil {
		regCtx.stopCh <- struct{}{}
		regCtx.wg.Wait()
	}
}
