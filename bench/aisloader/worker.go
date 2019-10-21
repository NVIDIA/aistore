/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

// worker routines

package main

import (
	"os"
	"path"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tutils"
)

func doPut(wo *workOrder) {
	var sgl *memsys.SGL
	if runParams.usingSG {
		sgl = tutils.Mem2.NewSGL(wo.size)
		defer func() {
			// FIXME: due to critical bug (https://github.com/golang/go/issues/30597)
			// we need to postpone `sgl.Free` to a little bit later time, otherwise
			// we will experience 'read after free'. Sleep time is number taken
			// from thin air - increase if panics are still happening.
			go func() {
				time.Sleep(4 * time.Second)
				sgl.Free()
			}()
		}()
	}

	r, err := tutils.NewReader(tutils.ParamReader{
		Type: runParams.readerType,
		SGL:  sgl,
		Path: runParams.tmpDir,
		Name: wo.objName,
		Size: wo.size,
	})

	if runParams.readerType == tutils.ReaderTypeFile {
		defer os.Remove(path.Join(runParams.tmpDir, wo.objName))
	}

	if err != nil {
		wo.err = err
		return
	}
	if !traceHTTPSig.Load() {
		wo.err = tutils.Put(wo.proxyURL, wo.bucket, wo.provider, wo.objName, r.XXHash(), r)
	} else {
		wo.latencies, wo.err = tutils.PutWithTrace(wo.proxyURL, wo.bucket, wo.provider,
			wo.objName, r.XXHash(), r)
	}
}

func doGet(wo *workOrder) {
	if !traceHTTPSig.Load() {
		wo.size, wo.err = tutils.GetDiscard(wo.proxyURL, wo.bucket, wo.provider,
			wo.objName, runParams.verifyHash, runParams.readOff, runParams.readLen)
	} else {
		wo.size, wo.latencies, wo.err = tutils.GetTraceDiscard(wo.proxyURL, wo.bucket, wo.provider,
			wo.objName, runParams.verifyHash, runParams.readOff, runParams.readLen)
	}
}

func doGetConfig(wo *workOrder) {
	wo.latencies, wo.err = tutils.GetConfig(wo.proxyURL)
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
