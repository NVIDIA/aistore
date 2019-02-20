package soakprim

import (
	"sync"

	"github.com/NVIDIA/aistore/tutils"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/bench/soaktest/stats"
	"github.com/NVIDIA/aistore/cmn"
)

const (
	regBucket   = "soaktest-reg-0"
	numRegFiles = 200
	regFilesize = 2 * cmn.MiB
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

func setupRegression() *regressionContext {
	params := &AISLoaderExecParams{
		pctput:       100,
		totalputsize: numRegFiles * regFilesize,
		minsize:      regFilesize,
		maxsize:      regFilesize,
	}
	aisStopCh := make(chan *stats.PrimitiveStat, 1)
	AISExec(aisStopCh, regBucket, 1, params)
	close(aisStopCh)

	regctx := &regressionContext{}
	regctx.bckName = regBucket //all regression tests share a bucket for now

	return regctx
}

// worker function for regression. Must call in go func
func regressionWorker(stopCh chan struct{}, wg *sync.WaitGroup, recordRegression func(*stats.PrimitiveStat)) {
	wg.Add(1)

	aisLoaderExecParams := &AISLoaderExecParams{
		pctput:   0,
		stopable: true,
		stopCh:   make(chan struct{}),

		duration:     0,
		totalputsize: 0,
	}

	aisExecResultCh := make(chan *stats.PrimitiveStat, 1)

	aisExecWg := &sync.WaitGroup{}
	aisExecWg.Add(1)

	go func() {
		defer aisExecWg.Done()
		AISExec(aisExecResultCh, regBucket, 1, aisLoaderExecParams)
	}()

	<-stopCh

	aisLoaderExecParams.stopCh <- struct{}{}
	aisExecWg.Wait()

	stat := <-aisExecResultCh
	recordRegression(stat)

	wg.Done()
}

func (rctx *RecipeContext) startRegression() {
	if rctx.regCtx == nil {
		rctx.regCtx = setupRegression()
	}

	regCtx := rctx.regCtx
	regCtx.stopCh = make(chan struct{})
	regCtx.wg = &sync.WaitGroup{}
	go regressionWorker(regCtx.stopCh, regCtx.wg, rctx.repCtx.RecordRegression)
}

func (rctx *RecipeContext) finishRegression() {
	regCtx := rctx.regCtx

	if regCtx == nil {
		return
	}

	if regCtx.wg != nil {
		regCtx.stopCh <- struct{}{}
		regCtx.wg.Wait()
	}
}
