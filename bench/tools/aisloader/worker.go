// Package aisloader
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */

// worker routines

package aisloader

import (
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tools/readers"
)

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
