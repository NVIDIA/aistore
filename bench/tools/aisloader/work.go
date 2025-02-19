// Package aisloader
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */

package aisloader

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/bench/tools/aisloader/stats"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tools/readers"
)

const (
	opPut = iota
	opGet
	opUpdateExisting // {GET followed by PUT(same name, same size)} combo
	opConfig
)

type (
	workOrder struct {
		err       error
		sgl       *memsys.SGL
		bck       cmn.Bck
		proxyURL  string
		objName   string // virtual-dir + "/" + objName
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
	if runParams.getConfig {
		workCh <- newGetConfigWorkOrder()
		return nil
	}

	var (
		pct = runParams.putPct
		put bool
	)
	switch pct {
	case 0:
	case 25:
		put = mono.NanoTime()&0x3 == 0x3
	case 50:
		put = mono.NanoTime()&1 == 1
	case 75:
		put = mono.NanoTime()&0x3 > 0
	case 100:
		put = true
	default:
		put = pct > rnd.IntN(100)
	}

	var wo *workOrder
	if put {
		wo, err = newPutWorkOrder()
	} else {
		var op = opGet
		if pct = runParams.updateExistingPct; pct > 0 {
			if pct > rnd.IntN(100) {
				op = opUpdateExisting
			}
		}
		wo, err = newGetWorkOrder(op)
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

	delta := time.Duration(wo.end - wo.start)

	switch wo.op {
	case opUpdateExisting:
		delta = time.Duration(wo.startPut - wo.start)
		fallthrough
	case opGet:
		if delta <= 0 {
			fmt.Fprintf(os.Stderr, "[ERROR] %s has the same start time as end time", wo)
			return
		}
		getPending--
		intervalStats.statsd.Get.AddPending(getPending)
		if wo.err != nil {
			fmt.Println("GET failed:", wo.err) // TODO: not necessarily when opGetPutNewVer
			intervalStats.statsd.Get.AddErr()
			intervalStats.get.AddErr()
			return
		}
		intervalStats.get.Add(wo.size, delta)
		intervalStats.statsd.Get.Add(wo.size, delta)
		if wo.op == opGet {
			return
		}

		delta = time.Duration(wo.end - wo.startPut)
		putPending++
		fallthrough
	case opPut:
		if delta <= 0 {
			fmt.Fprintf(os.Stderr, "[ERROR] %s has the same start time as end time", wo)
			return
		}
		putPending--
		intervalStats.statsd.Put.AddPending(putPending)
		if wo.err == nil {
			if wo.op != opUpdateExisting {
				bucketObjsNames.AddObjName(wo.objName)
			}
			intervalStats.put.Add(wo.size, delta)
			intervalStats.statsd.Put.Add(wo.size, delta)
		} else {
			fmt.Println("PUT failed:", wo.err)
			intervalStats.put.AddErr()
			intervalStats.statsd.Put.AddErr()
		}
		if wo.sgl == nil || terminating {
			return
		}

		now, l := mono.NanoTime(), len(wo2Free)
		// free previously executed PUT SGLs
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
				copy(wo2Free[i:], wo2Free[i+1:])
				i--
				l--
				wo2Free = wo2Free[:l]
			}
		}
		// append to free later
		wo2Free = append(wo2Free, wo)
	case opConfig:
		if wo.err == nil {
			intervalStats.getConfig.Add(1, delta)
			intervalStats.statsd.Config.Add(delta, wo.latencies.Proxy, wo.latencies.ProxyConn)
		} else {
			fmt.Println("get-config failed:", wo.err)
			intervalStats.getConfig.AddErr()
		}
	default:
		debug.Assert(false, wo.op)
	}
}

func doPut(wo *workOrder) {
	var (
		sgl *memsys.SGL
		url = wo.proxyURL
	)
	if runParams.readerType == readers.TypeSG {
		sgl = gmm.NewSGL(wo.size)
		wo.sgl = sgl
	}
	r, err := readers.New(readers.Params{
		Type: runParams.readerType,
		SGL:  sgl,
		Path: runParams.tmpDir,
		Name: wo.objName,
		Size: wo.size,
	}, wo.cksumType)

	if err != nil {
		wo.err = err
		return
	}
	if runParams.randomProxy {
		debug.Assert(!isDirectS3())
		psi, err := runParams.smap.GetRandProxy(false /*excl. primary*/)
		if err != nil {
			fmt.Printf("PUT(wo): %v\n", err)
			os.Exit(1)
		}
		url = psi.URL(cmn.NetPublic)
	}
	if !traceHTTPSig.Load() {
		if isDirectS3() {
			wo.err = s3put(wo.bck, wo.objName, r)
		} else {
			wo.err = put(url, wo.bck, wo.objName, r.Cksum(), r)
		}
	} else {
		debug.Assert(!isDirectS3())
		wo.err = putWithTrace(url, wo.bck, wo.objName, &wo.latencies, r.Cksum(), r)
	}
	if runParams.readerType == readers.TypeFile {
		r.Close()
		os.Remove(path.Join(runParams.tmpDir, wo.objName))
	}
}

func doGet(wo *workOrder) {
	var (
		url = wo.proxyURL
	)
	if runParams.randomProxy {
		debug.Assert(!isDirectS3())
		psi, err := runParams.smap.GetRandProxy(false /*excl. primary*/)
		if err != nil {
			fmt.Printf("GET(wo): %v\n", err)
			os.Exit(1)
		}
		url = psi.URL(cmn.NetPublic)
	}
	if !traceHTTPSig.Load() {
		if isDirectS3() {
			wo.size, wo.err = s3getDiscard(wo.bck, wo.objName)
		} else {
			wo.size, wo.err = getDiscard(url, wo.bck,
				wo.objName, runParams.readOff, runParams.readLen, runParams.verifyHash, runParams.latest)
		}
	} else {
		debug.Assert(!isDirectS3())
		wo.size, wo.err = getTraceDiscard(url, wo.bck,
			wo.objName, &wo.latencies, runParams.readOff, runParams.readLen, runParams.verifyHash, runParams.latest)
	}
}

func doGetConfig(wo *workOrder) {
	wo.latencies, wo.err = getConfig(wo.proxyURL)
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
		case opConfig:
			doGetConfig(wo)
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

func newPutWorkOrder() (*workOrder, error) {
	objName, err := _genObjName()
	if err != nil {
		return nil, err
	}
	size := runParams.minSize
	if runParams.maxSize != runParams.minSize {
		d := rnd.Int64N(runParams.maxSize + 1 - runParams.minSize)
		size = runParams.minSize + d
	}
	putPending++
	return &workOrder{
		proxyURL:  runParams.proxyURL,
		bck:       runParams.bck,
		op:        opPut,
		objName:   objName,
		size:      size,
		cksumType: runParams.cksumType,
	}, nil
}

func _genObjName() (string, error) {
	cnt := objNameCnt.Inc()
	if runParams.maxputs != 0 && cnt-1 == runParams.maxputs {
		return "", fmt.Errorf("number of PUT objects reached maxputs limit (%d)", runParams.maxputs)
	}

	var (
		comps [3]string
		idx   = 0
	)

	if runParams.subDir != "" {
		comps[idx] = runParams.subDir
		idx++
	}

	if runParams.putShards != 0 {
		comps[idx] = fmt.Sprintf("%05x", cnt%runParams.putShards)
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

	return path.Join(comps[0:idx]...), nil
}

func newGetWorkOrder(op int) (*workOrder, error) {
	if bucketObjsNames.Len() == 0 {
		return nil, errors.New("no objects in bucket")
	}

	getPending++
	return &workOrder{
		proxyURL: runParams.proxyURL,
		bck:      runParams.bck,
		op:       op,
		objName:  bucketObjsNames.ObjName(),
	}, nil
}

func newGetConfigWorkOrder() *workOrder {
	return &workOrder{
		proxyURL: runParams.proxyURL,
		op:       opConfig,
	}
}

func (wo *workOrder) String() string {
	var errstr, opName string
	switch wo.op {
	case opPut:
		opName = http.MethodPut
	case opGet:
		opName = http.MethodGet
	case opUpdateExisting:
		opName = "GET-PUT(new-version)"
	case opConfig:
		opName = "CONFIG"
	}

	if wo.err != nil {
		errstr = ", error: " + wo.err.Error()
	}

	return fmt.Sprintf("WO: %s/%s, duration %s, size: %d, type: %s%s",
		wo.bck.String(), wo.objName, time.Duration(wo.end-wo.start), wo.size, opName, errstr)
}
