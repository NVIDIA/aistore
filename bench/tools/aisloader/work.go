// Package aisloader
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */

package aisloader

import (
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/bench/tools/aisloader/stats"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/xoshiro256"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tools/readers"
)

// GET(batch): an operation that gets a batch of objects (or archived files) in one shot.
//
// TODO:
// * stats - to compare get-batch vs GET
// * features
//   - plain vs archive
//   - multipart mode (w/ streaming default)
//   - continue-on-error
//   - multiple buckets
//   - supported input and output formats (other than TAR)
// * documentation
//   - update docs/aisloader.md; add usage examples
// ---------------------------------------------------------------------------------------

const (
	opFree = iota
	opPut
	opGet
	opUpdateExisting // {GET followed by PUT(same name, same size)} combo
	opPutMultipart   // multipart upload operation
	opGetBatch       // get-batch
)

type (
	workOrder struct {
		err       error
		sgl       *memsys.SGL
		moss      *apc.MossReq // <= name-getter.PickBatch()
		objName   string
		cksumType string
		latencies httpLatencies
		op        int
		size      int64
		start     int64
		end       int64
		startPut  int64 // PUT in `opUpdateExisting`
	}
)

func postNewWorkOrder() (err error) {
	totalWOs++

	// operation
	op := opGet
	switch {
	case shouldUsePercentage(runParams.putPct):
		// when a certain percentage of PUTs becomes PUT(multi-part)
		op = opPut
		if runParams.multipartChunks > 0 && shouldUsePercentage(runParams.multipartPct) {
			op = opPutMultipart
		}
	case runParams.getBatchSize > 0:
		// when GET becomes GET(batch)
		op = opGetBatch
	case runParams.updateExistingPct > 0 && shouldUsePercentage(runParams.updateExistingPct):
		// when a percentage of GET(foo) is followed up by PUT(foo)
		op = opUpdateExisting
	}

	// work order
	var wo *workOrder
	switch op {
	case opPut:
		wo, err = newPutWorkOrder()
	case opPutMultipart:
		wo, err = newMultipartWorkOrder()
	case opGet, opUpdateExisting:
		wo, err = newGetWorkOrder(op)
	default:
		debug.Assert(op == opGetBatch, op)
		wo, err = newGetBatchWorkOrder()
	}

	if err == nil {
		workCh <- wo
	}
	return err
}

func completeWorkOrder(wo *workOrder, terminating bool) {
	if wo.err == nil && traceHTTPSig.Load() {
		var lat *stats.MetricLatsAgg
		switch wo.op {
		case opGet:
			lat = &intervalStats.statsd.GetLat
		case opPut:
			lat = &intervalStats.statsd.PutLat
		}
		if lat != nil {
			lat.Add("latency.proxyconn", wo.latencies.ProxyConn)
			lat.Add("latency.proxy", wo.latencies.Proxy)
			lat.Add("latency.targetconn", wo.latencies.TargetConn)
			lat.Add("latency.target", wo.latencies.Target)
			lat.Add("latency.posthttp", wo.latencies.PostHTTP)
			lat.Add("latency.proxyheader", wo.latencies.ProxyWroteHeader)
			lat.Add("latency.proxyrequest", wo.latencies.ProxyWroteRequest)
			lat.Add("latency.targetheader", wo.latencies.TargetWroteHeader)
			lat.Add("latency.proxyresponse", wo.latencies.ProxyFirstResponse)
			lat.Add("latency.targetrequest", wo.latencies.TargetWroteRequest)
			lat.Add("latency.targetresponse", wo.latencies.TargetFirstResponse)
		}
	}

	elapsed := time.Duration(wo.end - wo.start)
	if elapsed <= 0 {
		err := fmt.Errorf("unexpected: non-positive latency %v: %s", elapsed, wo.String())
		debug.AssertNoErr(err)
		elapsed = 0
	}

	switch wo.op {
	case opUpdateExisting:
		elapsed = time.Duration(wo.startPut - wo.start)
		debug.Assert(elapsed >= 0)
		fallthrough
	case opGet:
		getPending--
		intervalStats.statsd.Get.AddPending(getPending)
		if wo.err != nil {
			fmt.Fprintln(os.Stderr, "GET failed:", wo.err)
			intervalStats.statsd.Get.AddErr()
			intervalStats.get.AddErr()
			return
		}
		intervalStats.get.Add(wo.size, elapsed)
		intervalStats.statsd.Get.Add(wo.size, elapsed)
		if wo.op == opGet {
			return
		}

		elapsed = time.Duration(wo.end - wo.startPut)
		putPending++
		fallthrough
	case opPut:
		putPending--
		intervalStats.statsd.Put.AddPending(putPending)
		if wo.err == nil {
			if wo.op != opUpdateExisting {
				if !stopping.Load() {
					objnameGetter.Add(wo.objName)
				}
			}
			intervalStats.put.Add(wo.size, elapsed)
			intervalStats.statsd.Put.Add(wo.size, elapsed)
		} else {
			fmt.Fprintln(os.Stderr, "PUT failed:", wo.err)
			intervalStats.put.AddErr()
			intervalStats.statsd.Put.AddErr()
		}
		if wo.sgl == nil || terminating {
			return
		}

		now, l := mono.NanoTime(), len(wo2Free)

		// cleanup: free previously executed PUT SGLs
		for i := 0; i < l; i++ {
			if terminating {
				return
			}
			w := wo2Free[i]
			// delaying freeing sgl for `wo2FreeDelay`
			// (background at https://github.com/golang/go/issues/30597)
			if time.Duration(now-w.end) < wo2FreeDelay {
				break
			}
			if w.sgl != nil && !w.sgl.IsNil() {
				w.sgl.Free()
				w.sgl = nil
				freeWO(w)
				copy(wo2Free[i:], wo2Free[i+1:])
				i--
				l--
				wo2Free = wo2Free[:l]
			}
		}
		// append to free later
		wo2Free = append(wo2Free, wo)
	case opPutMultipart:
		putPending--
		intervalStats.statsd.Put.AddPending(putPending)
		if wo.err == nil {
			if !stopping.Load() {
				objnameGetter.Add(wo.objName)
			}
			intervalStats.putMPU.Add(wo.size, elapsed)
			intervalStats.statsd.Put.Add(wo.size, elapsed)
		} else {
			fmt.Fprintln(os.Stderr, "Multipart PUT failed:", wo.err)
			intervalStats.putMPU.AddErr()
			intervalStats.statsd.Put.AddErr()
		}
		// No SGL cleanup needed for multipart operations

	case opGetBatch:
		getBatchPending--
		intervalStats.statsd.GetBatch.AddPending(getBatchPending)
		if wo.err == nil {
			intervalStats.getBatch.Add(wo.size, elapsed)
			intervalStats.statsd.GetBatch.Add(wo.size, elapsed)

			// TODO: possibly, check unusually long `elapsed`
		} else {
			fmt.Fprintln(os.Stderr, "GetBatch failed:", wo.err)
			intervalStats.getBatch.AddErr()
			intervalStats.statsd.GetBatch.AddErr()
		}

	default:
		debug.Assert(false, wo.op)
	}
}

func doPut(wo *workOrder) {
	var readParams = readers.Arg{
		Type:      runParams.readerType,
		Path:      runParams.tmpDir,
		Name:      wo.objName,
		Size:      wo.size,
		CksumType: wo.cksumType,
	}
	if runParams.readerType == readers.SG {
		wo.sgl = gmm.NewSGL(wo.size)
		readParams.SGL = wo.sgl
	}

	// PUT(shard)
	if runParams.archParams.use {
		mime, err := archive.Mime(runParams.archParams.format, wo.objName)
		if err != nil {
			wo.err = err
			return
		}
		readParams.Arch = &readers.Arch{
			Mime:    mime,
			Prefix:  runParams.archParams.prefix,
			MinSize: runParams.archParams.minSz,
			MaxSize: runParams.archParams.maxSz,
			Num:     runParams.archParams.numFiles,
		}
	}

	r, err := readers.New(&readParams)
	if err != nil {
		wo.err = err
		return
	}

	url := runParams.proxyURL
	if runParams.randomProxy {
		debug.Assert(!isDirectS3())
		psi, err := runParams.smap.GetRandProxy(false /*excl. primary*/)
		if err != nil {
			fmt.Fprintln(os.Stderr, "PUT(wo) err:", err, wo.String())
			os.Exit(1)
		}
		url = psi.URL(cmn.NetPublic)
	}
	if !traceHTTPSig.Load() {
		if isDirectS3() {
			wo.err = s3put(runParams.bck, wo.objName, r)
		} else {
			wo.err = put(url, runParams.bck, wo.objName, r.Cksum(), r)
		}
	} else {
		debug.Assert(!isDirectS3())
		wo.err = putWithTrace(url, runParams.bck, wo.objName, &wo.latencies, r.Cksum(), r)
	}
	if runParams.readerType == readers.File {
		r.Close()
		os.Remove(path.Join(runParams.tmpDir, wo.objName))
	}
}

func doMultipart(wo *workOrder) {
	var url = runParams.proxyURL

	if runParams.randomProxy {
		debug.Assert(!isDirectS3())
		psi, err := runParams.smap.GetRandProxy(false /*excl. primary*/)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Multipart PUT(wo) err:", err, wo.String())
			os.Exit(1)
		}
		url = psi.URL(cmn.NetPublic)
	}

	wo.err = putMultipart(url, runParams.bck, wo.objName, wo.size, runParams.multipartChunks, wo.cksumType)
}

func doGet(wo *workOrder) {
	var (
		url = runParams.proxyURL
	)
	if runParams.randomProxy {
		debug.Assert(!isDirectS3())
		psi, err := runParams.smap.GetRandProxy(false /*excl. primary*/)
		if err != nil {
			fmt.Fprintln(os.Stderr, "GET(wo) err:", err, wo.String())
			os.Exit(1)
		}
		url = psi.URL(cmn.NetPublic)
	}
	if !traceHTTPSig.Load() {
		if isDirectS3() {
			wo.size, wo.err = s3getDiscard(runParams.bck, wo.objName)
		} else {
			wo.size, wo.err = getDiscard(url, runParams.bck,
				wo.objName, runParams.readOff, runParams.readLen, runParams.verifyHash, runParams.latest)
		}
	} else {
		debug.Assert(!isDirectS3())
		wo.size, wo.err = getTraceDiscard(url, runParams.bck,
			wo.objName, &wo.latencies, runParams.readOff, runParams.readLen, runParams.verifyHash, runParams.latest)
	}
}

func doGetBatch(wo *workOrder) {
	var url = runParams.proxyURL

	if runParams.randomProxy {
		debug.Assert(!isDirectS3())
		psi, err := runParams.smap.GetRandProxy(false /*excl. primary*/)
		if err != nil {
			fmt.Fprintln(os.Stderr, "GetBatch(wo):", err, wo.String())
			os.Exit(1)
		}
		url = psi.URL(cmn.NetPublic)
	}

	// build GetBatch request from wo.batch
	// (earlier objnameGetter.PickBatch())
	debug.Assert(wo.moss != nil && len(wo.moss.In) > 0)

	// TODO: command-line to support multipart
	wo.moss.StreamingGet = true

	// do
	wo.size, wo.err = getBatchDiscard(url, runParams.bck, wo.moss)
}

func worker(wos <-chan *workOrder, results chan<- *workOrder, wg *sync.WaitGroup, numGets *atomic.Int64) {
	defer wg.Done()

	for {
		wo, more := <-wos
		if !more {
			return
		}

		wo.start = mono.NanoTime()

		switch wo.op {
		case opPut:
			doPut(wo)
		case opGet:
			doGet(wo)
			numGets.Inc()
		case opUpdateExisting:
			// TODO: fix latency stats
			doGet(wo)
			if wo.err == nil {
				numGets.Inc()
				wo.startPut = mono.NanoTime()
				doPut(wo)
			}
		case opPutMultipart:
			doMultipart(wo)
		case opGetBatch:
			doGetBatch(wo)
		default:
			debug.Assert(false, wo.op)
		}

		wo.end = mono.NanoTime()
		results <- wo
	}
}

///////////////
// workOrder //
///////////////

func newPutWorkOrder() (*workOrder, error)       { return _newPutWO(opPut) }
func newMultipartWorkOrder() (*workOrder, error) { return _newPutWO(opPutMultipart) }

func _randInRange(minSize, maxSize int64) int64 {
	span := uint64(maxSize - minSize + 1)
	v := xoshiro256.Hash(totalWOs) % span
	return minSize + int64(v)
}

func _newPutWO(op int) (*workOrder, error) {
	objName, err := _genObjName()
	if err != nil {
		return nil, err
	}
	size := runParams.minSize
	if runParams.maxSize != runParams.minSize {
		size = _randInRange(runParams.minSize, runParams.maxSize)
	}
	putPending++

	wo := allocWO(op)
	{
		wo.objName = objName
		wo.size = size
		wo.cksumType = runParams.cksumType
	}
	return wo, nil
}

func _genObjName() (string, error) {
	cnt := objNameCnt.Inc()
	if runParams.maxputs != 0 && cnt-1 == runParams.maxputs {
		return "", fmt.Errorf("number of PUT objects reached '--maxputs' limit (%d)", runParams.maxputs)
	}

	var (
		comps [3]string
		idx   = 0
	)

	if runParams.subDir != "" {
		comps[idx] = runParams.subDir
		idx++
	}

	if runParams.numVirtDirs != 0 {
		comps[idx] = fmt.Sprintf("%05x", cnt%runParams.numVirtDirs)
		idx++
	}

	if useRandomObjName {
		comps[idx] = cos.RandStringWithSrc(rnd, randomObjNameLen)
		idx++
	} else {
		objectNumber := (cnt - 1) << suffixIDMaskLen
		objectNumber |= suffixID
		comps[idx] = strconv.FormatUint(objectNumber, 16)
		idx++
	}

	name := path.Join(comps[0:idx]...)

	// new shard: add extension
	if runParams.archParams.use && cos.Ext(name) == "" {
		if runParams.archParams.format != "" {
			name += runParams.archParams.format
		} else {
			name += archive.ExtTar // default .tar
		}
	}
	return name, nil
}

func newGetWorkOrder(op int) (*workOrder, error) {
	debug.Assert(op == opGet || op == opUpdateExisting, op)
	if objnameGetter.Len() == 0 {
		return nil, errors.New("no objects in bucket")
	}

	getPending++
	wo := allocWO(op)
	wo.objName = objnameGetter.Pick()
	return wo, nil
}

func newGetBatchWorkOrder() (*workOrder, error) {
	if objnameGetter.Len() == 0 {
		err := errors.New("no objects in bucket")
		fmt.Fprintln(os.Stderr, "newGetBatchWorkOrder:", err)
		return nil, err
	}

	getBatchPending++
	wo := allocGetBatchWO(runParams.getBatchSize)
	wo.moss.In = objnameGetter.PickBatch(wo.moss.In)

	return wo, nil
}

func (wo *workOrder) String() string {
	var opName, s string
	switch wo.op {
	case opPut:
		opName = opLabelPut
	case opGet:
		opName = opLabelGet
	case opPutMultipart:
		opName = opLabelMPU
	case opUpdateExisting:
		opName = "GET-PUT(new-version)"
	case opGetBatch:
		opName = opLabelGBT
		if wo.moss != nil && len(wo.moss.In) > 0 {
			s = ", batch " + strconv.Itoa(len(wo.moss.In))
		}
	}
	cname := runParams.bck.Cname(wo.objName)
	if wo.start == 0 {
		return fmt.Sprintf("wo[%s %s%s]", opName, cname, s)
	}
	lat := time.Duration(wo.end - wo.start)
	return fmt.Sprintf("wo[%s %s, %v, size: %d%s]", opName, cname, lat, wo.size, s)
}

// returns true based on the given PUT percentage (0-100)
// uses totalWOs counter with xoshiro256 hash for deterministic, well-distributed decisions
func shouldUsePercentage(pct int) bool {
	if pct <= 0 {
		return false
	}
	if pct >= 100 {
		return true
	}
	v := xoshiro256.Hash(totalWOs) % 100
	return v < uint64(pct)
}
