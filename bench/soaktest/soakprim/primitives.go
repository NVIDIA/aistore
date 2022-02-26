// Package soakprim provides the framework for running soak tests
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package soakprim

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/bench/soaktest/report"
	"github.com/NVIDIA/aistore/bench/soaktest/soakcmn"
	"github.com/NVIDIA/aistore/bench/soaktest/stats"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

type primTag struct {
	primType string
	num      int
}

func (pt *primTag) String() string {
	return fmt.Sprintf("%v %v", pt.primType, pt.num)
}

func (rctx *RecipeContext) startPrim(primType string) *primTag {
	rctx.wg.Add(1)

	val, ok := rctx.primitiveCount[primType]
	if !ok {
		val = 1
	} else {
		val++
	}
	rctx.primitiveCount[primType] = val

	tag := &primTag{primType: primType, num: val}

	report.Writef(report.DetailLevel, "--- %v STARTED ---\n", tag)

	return tag
}

func (rctx *RecipeContext) finishPrim(tag fmt.Stringer) {
	if err := recover(); err != nil {
		rctx.failedPrimitives[tag.String()] = err.(error)
	}

	report.Writef(report.DetailLevel, "--- %s ENDED ---\n", tag)

	rctx.wg.Done()
}

func (rctx *RecipeContext) MakeBucket(bucketname string) {
	tag := rctx.startPrim("MakeBucket")
	go func() {
		defer rctx.finishPrim(tag)
		err := api.CreateBucket(soakcmn.BaseAPIParams(primaryURL), bckNamePrefix(bucketname), nil)
		cos.AssertNoErr(err)
	}()
}

func (rctx *RecipeContext) SetBucketProps(bucketname string, props *cmn.BucketPropsToUpdate) {
	tag := rctx.startPrim("SetBucketProps")
	go func() {
		defer rctx.finishPrim(tag)
		_, err := api.SetBucketProps(soakcmn.BaseAPIParams(primaryURL), bckNamePrefix(bucketname), props)
		cos.AssertNoErr(err)
	}()
}

// Put is the primitive that puts into an existing bucket
// pctSize is the percent of capacity allocated to recipes, recipe assumes responsibility to ensure at most 100 is used
func (rctx *RecipeContext) Put(bucketname string, maxDuration time.Duration, pctSize float64) {
	tag := rctx.startPrim("PUT")

	if pctSize > 100 {
		cos.AssertNoErr(fmt.Errorf("attempted to use %v pct of recipe capacity", pctSize))
	}

	primPutSize := int64(float64(recCapacity) / 100 * pctSize)

	params := &AISLoaderExecParams{
		pctput:       100,
		duration:     maxDuration,
		totalputsize: primPutSize,
		minsize:      soakcmn.Params.RecMinFilesize,
		maxsize:      soakcmn.Params.RecMaxFilesize,
	}
	go func() {
		defer rctx.finishPrim(tag)
		ch := make(chan *stats.PrimitiveStat, 1)
		AISExec(ch, soakcmn.OpTypePut, bckNamePrefix(bucketname), soakcmn.Params.RecPrimWorkers, params)
		stat := <-ch
		stat.ID = tag.String()
		rctx.repCtx.PutPrimitiveStats(stat)
	}()
}

// Get has parameters readoffpct and readlenpct as a 100 pct of min filesize
func (rctx *RecipeContext) Get(bucketName string, duration time.Duration, checksum bool, readOffPct, readLenPct float64) {
	tag := rctx.startPrim("GET")
	params := &AISLoaderExecParams{
		pctput:     0,
		duration:   duration,
		verifyhash: checksum,
		readoff:    int64(float64(soakcmn.Params.RecMinFilesize/100) * readOffPct),
		readlen:    int64(float64(soakcmn.Params.RecMinFilesize/100) * readLenPct),
	}
	go func() {
		ch := make(chan *stats.PrimitiveStat, 1)
		defer rctx.finishPrim(tag)
		AISExec(ch, soakcmn.OpTypeGet, bckNamePrefix(bucketName), soakcmn.Params.RecPrimWorkers, params)
		stat := <-ch
		stat.ID = tag.String()
		rctx.repCtx.PutPrimitiveStats(stat)
	}()
}

// GetCfg gets the config of the proxy and thus has no bucket
func (rctx *RecipeContext) GetCfg(duration time.Duration) {
	tag := rctx.startPrim("GetCFG")
	params := &AISLoaderExecParams{
		pctput:   0,
		duration: duration,
	}

	go func() {
		ch := make(chan *stats.PrimitiveStat, 1)
		defer rctx.finishPrim(tag)
		AISExec(ch, soakcmn.OpTypeCfg, cmn.Bck{} /** bucket not required for getcfg **/, soakcmn.Params.RecPrimWorkers, params)
		stat := <-ch
		stat.ID = tag.String()
		rctx.repCtx.PutPrimitiveStats(stat)
	}()
}

// TODO: DELETE, AISLOADER

func (rctx *RecipeContext) Rename(bucketName, newName string) {
	tag := rctx.startPrim("Rename")
	go func() {
		defer rctx.finishPrim(tag)
		_, err := api.RenameBucket(soakcmn.BaseAPIParams(primaryURL), bckNamePrefix(bucketName), bckNamePrefix(newName))
		cos.AssertNoErr(err)
	}()
}

func (rctx *RecipeContext) Destroy(bucketName string) {
	tag := rctx.startPrim("Destroy")
	go func() {
		defer rctx.finishPrim(tag)
		err := api.DestroyBucket(soakcmn.BaseAPIParams(primaryURL), bckNamePrefix(bucketName))
		cos.AssertNoErr(err)
	}()
}

func (rctx *RecipeContext) RemoveTarget(conds *PostConds, delay time.Duration) {
	if conds != nil {
		conds.NumTargets--
	}

	tag := rctx.startPrim("RemoveTarget")
	go func() {
		defer rctx.finishPrim(tag)
		time.Sleep(delay)
		rctx.targetMutex.Lock()
		smap := fetchSmap("RestoreTarget")
		for _, v := range smap.Tmap {
			args := &apc.ActValRmNode{DaemonID: v.ID(), SkipRebalance: true}
			err := soakcmn.UnregisterNode(primaryURL, args)
			rctx.targetMutex.Unlock()
			cos.AssertNoErr(err)
			return
		}
		rctx.targetMutex.Unlock()
		cos.AssertNoErr(fmt.Errorf("no targets to remove"))
	}()
}

func (rctx *RecipeContext) RestoreTarget(conds *PostConds, delay time.Duration) {
	if conds != nil {
		conds.NumTargets++
	}

	tag := rctx.startPrim("RestoreTarget")
	go func() {
		defer rctx.finishPrim(tag)
		time.Sleep(delay)
		rctx.targetMutex.Lock()
		smap := fetchSmap("RestoreTarget")
		for k, v := range rctx.origTargets {
			_, ok := smap.Tmap[k]
			if !ok {
				_, err := soakcmn.JoinCluster(primaryURL, v)
				rctx.targetMutex.Unlock()
				cos.AssertNoErr(err)
				return
			}
		}
		rctx.targetMutex.Unlock()
		cos.AssertNoErr(fmt.Errorf("no targets to restore"))
	}()
}
