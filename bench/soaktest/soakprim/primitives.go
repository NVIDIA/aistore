package soakprim

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/bench/soaktest/report"
	"github.com/NVIDIA/aistore/bench/soaktest/stats"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
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

func (rctx *RecipeContext) finishPrim(tag *primTag) {
	if err := recover(); err != nil {
		rctx.failedPrimitives[tag.String()] = err.(error)
	}

	report.Writef(report.DetailLevel, "--- %v ENDED ---\n", tag)

	rctx.wg.Done()
}

func (rctx *RecipeContext) MakeBucket(bucketname string, bprops *cmn.BucketProps) {
	tag := rctx.startPrim("MakeBucket")
	go func() {
		defer rctx.finishPrim(tag)
		err := api.CreateLocalBucket(tutils.BaseAPIParams(primaryURL), bckNamePrefix(bucketname))
		cmn.AssertNoErr(err)
	}()
}

func (rctx *RecipeContext) Put(bucketname string, size int64, minSize int, maxSize int, numWorkers int) {
	tag := rctx.startPrim("PUT")
	params := &AISLoaderExecParams{
		pctput:       100,
		totalputsize: size,
		minsize:      minSize,
		maxsize:      maxSize,
	}
	go func() {
		defer rctx.finishPrim(tag)
		ch := make(chan *stats.PrimitiveStat, 1)
		AISExec(ch, bckNamePrefix(bucketname), numWorkers, params)
		stat := <-ch
		stat.ID = tag.String()
		rctx.repCtx.PutPrimitiveStats(stat)
	}()
}

func (rctx *RecipeContext) Get(bucketname string, duration time.Duration, checksum bool, numWorkers int, readoff int, readlen int) {
	tag := rctx.startPrim("GET")
	params := &AISLoaderExecParams{
		pctput:     0,
		duration:   duration,
		verifyhash: checksum,
		readoff:    readoff,
		readlen:    readlen,
	}
	go func() {
		ch := make(chan *stats.PrimitiveStat, 1)
		defer rctx.finishPrim(tag)
		AISExec(ch, bckNamePrefix(bucketname), numWorkers, params)
		stat := <-ch
		stat.ID = tag.String()
		rctx.repCtx.PutPrimitiveStats(stat)
	}()
}

//TODO: DELETE, AISLOADER

func (rctx *RecipeContext) Rename(bucketname string, newname string) {
	tag := rctx.startPrim("Rename")
	go func() {
		defer rctx.finishPrim(tag)
		err := api.RenameLocalBucket(tutils.BaseAPIParams(primaryURL), bckNamePrefix(bucketname), bckNamePrefix(newname))
		cmn.AssertNoErr(err)
	}()
}

func (rctx *RecipeContext) Destroy(bucketname string) {
	tag := rctx.startPrim("Destroy")
	go func() {
		defer rctx.finishPrim(tag)
		err := api.DestroyLocalBucket(tutils.BaseAPIParams(primaryURL), bckNamePrefix(bucketname))
		cmn.AssertNoErr(err)
	}()
}

func (rctx *RecipeContext) RemoveTarget(conds *PostConds) {
	if conds != nil {
		conds.NumTargets--
	}

	tag := rctx.startPrim("RemoveTarget")
	defer rctx.finishPrim(tag)
	smap := fetchSmap("RestoreTarget")
	for _, v := range smap.Tmap {
		err := tutils.UnregisterTarget(primaryURL, v.DaemonID)
		cmn.AssertNoErr(err)
		break
	}
}

func (rctx *RecipeContext) RestoreTarget(conds *PostConds) {
	if conds != nil {
		conds.NumTargets++
	}

	tag := rctx.startPrim("RestoreTarget")
	defer rctx.finishPrim(tag)
	smap := fetchSmap("RestoreTarget")
	for k, v := range rctx.origTargets {
		_, ok := smap.Tmap[k]
		if !ok {
			err := tutils.RegisterTarget(primaryURL, v, *smap)
			cmn.AssertNoErr(err)
			break
		}
	}
}
