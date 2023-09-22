// Package aisloader
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */

package aisloader

import (
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
	opConfig
)

type (
	workOrder struct {
		op        int
		proxyURL  string
		bck       cmn.Bck
		objName   string // In the format of 'virtual dir' + "/" + objName
		size      int64
		err       error
		start     time.Time
		end       time.Time
		latencies httpLatencies
		cksumType string
		sgl       *memsys.SGL
	}
)

func postNewWorkOrder() (err error) {
	var wo *workOrder
	switch {
	case runParams.getConfig:
		wo = newGetConfigWorkOrder()
	case runParams.putPct == 100:
		wo, err = newPutWorkOrder()
	case runParams.putPct == 0:
		wo, err = newGetWorkOrder()
	default:
		var put bool
		if runParams.putPct == 50 {
			put = mono.NanoTime()&1 == 1
		} else {
			put = runParams.putPct > rnd.Intn(99)
		}
		if put {
			wo, err = newPutWorkOrder()
		} else {
			wo, err = newGetWorkOrder()
		}
	}
	if err == nil {
		workCh <- wo
	}
	return
}

func validateWorkOrder(wo *workOrder, delta time.Duration) error {
	if wo.op == opGet || wo.op == opPut {
		if delta == 0 {
			return fmt.Errorf("%s has the same start time as end time", wo)
		}
	}
	return nil
}

func completeWorkOrder(wo *workOrder, terminating bool) {
	delta := timeDelta(wo.end, wo.start)

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

	if err := validateWorkOrder(wo, delta); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "[ERROR] %s", err.Error())
		return
	}

	switch wo.op {
	case opGet:
		getPending--
		intervalStats.statsd.Get.AddPending(getPending)
		if wo.err == nil {
			intervalStats.get.Add(wo.size, delta)
			intervalStats.statsd.Get.Add(wo.size, delta)
		} else {
			fmt.Println("GET failed: ", wo.err)
			intervalStats.statsd.Get.AddErr()
			intervalStats.get.AddErr()
		}
	case opPut:
		putPending--
		intervalStats.statsd.Put.AddPending(putPending)
		if wo.err == nil {
			bucketObjsNames.AddObjName(wo.objName)
			intervalStats.put.Add(wo.size, delta)
			intervalStats.statsd.Put.Add(wo.size, delta)
		} else {
			fmt.Println("PUT failed: ", wo.err)
			intervalStats.put.AddErr()
			intervalStats.statsd.Put.AddErr()
		}
		if wo.sgl == nil || terminating {
			return
		}

		now, l := time.Now(), len(wo2Free)
		debug.Assert(!wo.end.IsZero())
		// free previously executed PUT SGLs
		for i := 0; i < l; i++ {
			if terminating {
				return
			}
			w := wo2Free[i]
			// delaying freeing sgl for `wo2FreeDelay`
			// (background at https://github.com/golang/go/issues/30597)
			if now.Sub(w.end) < wo2FreeDelay {
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
			fmt.Println("GET config failed: ", wo.err)
			intervalStats.getConfig.AddErr()
		}
	default:
		debug.Assert(false) // Should never be here
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
		wo.latencies, wo.err = putWithTrace(url, wo.bck, wo.objName, r.Cksum(), r)
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
				wo.objName, runParams.verifyHash, runParams.readOff, runParams.readLen)
		}
	} else {
		debug.Assert(!isDirectS3())
		wo.size, wo.latencies, wo.err = getTraceDiscard(url, wo.bck,
			wo.objName, runParams.verifyHash, runParams.readOff, runParams.readLen)
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

		wo.start = time.Now()

		switch wo.op {
		case opPut:
			doPut(wo)
		case opGet:
			doGet(wo)
			numGets.Inc()
		case opConfig:
			doGetConfig(wo)
		default:
			// Should not come here
		}

		wo.end = time.Now()
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
		size = rnd.Int63n(runParams.maxSize+1-runParams.minSize) + runParams.minSize
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

func newGetWorkOrder() (*workOrder, error) {
	if bucketObjsNames.Len() == 0 {
		return nil, fmt.Errorf("no objects in bucket")
	}

	getPending++
	return &workOrder{
		proxyURL: runParams.proxyURL,
		bck:      runParams.bck,
		op:       opGet,
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
	case opGet:
		opName = http.MethodGet
	case opPut:
		opName = http.MethodPut
	case opConfig:
		opName = "CONFIG"
	}

	if wo.err != nil {
		errstr = ", error: " + wo.err.Error()
	}

	return fmt.Sprintf("WO: %s/%s, start:%s end:%s, size: %d, type: %s%s",
		wo.bck, wo.objName, wo.start.Format(time.StampMilli), wo.end.Format(time.StampMilli), wo.size, opName, errstr)
}
