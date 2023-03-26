// Package aisloader
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */

// worker routines

package aisloader

import (
	"os"
	"path"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tools/readers"
)

func doPut(wo *workOrder) {
	var sgl *memsys.SGL
	if runParams.readerType == readers.ReaderTypeSG {
		sgl = gmm.NewSGL(wo.size)
		wo.sgl = sgl
	}

	r, err := readers.NewReader(readers.ParamReader{
		Type: runParams.readerType,
		SGL:  sgl,
		Path: runParams.tmpDir,
		Name: wo.objName,
		Size: wo.size,
	}, wo.cksumType)

	if runParams.readerType == readers.ReaderTypeFile {
		defer os.Remove(path.Join(runParams.tmpDir, wo.objName))
	}

	if err != nil {
		wo.err = err
		return
	}
	if !traceHTTPSig.Load() {
		wo.err = put(wo.proxyURL, wo.bck, wo.objName, r.Cksum(), r)
	} else {
		wo.latencies, wo.err = putWithTrace(wo.proxyURL, wo.bck, wo.objName, r.Cksum(), r)
	}
}

func doGet(wo *workOrder) {
	if !traceHTTPSig.Load() {
		wo.size, wo.err = getDiscard(wo.proxyURL, wo.bck,
			wo.objName, runParams.verifyHash, runParams.readOff, runParams.readLen)
	} else {
		wo.size, wo.latencies, wo.err = getTraceDiscard(wo.proxyURL, wo.bck,
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
