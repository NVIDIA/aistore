/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

// worker routines

package main

import (
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/memsys"
	"github.com/NVIDIA/dfcpub/tutils"
)

func doPut(wo *workOrder) {
	var sgl *memsys.SGL
	if runParams.usingSG {
		sgl = tutils.Mem2.NewSGL(wo.size)
		defer sgl.Free()
	}

	r, err := tutils.NewReader(tutils.ParamReader{
		Type: runParams.readerType,
		SGL:  sgl,
		Path: runParams.tmpDir,
		Name: wo.objName,
		Size: wo.size,
	})

	if err != nil {
		wo.err = err
		return
	}

	wo.err = tutils.Put(wo.proxyURL, r, wo.bucket, wo.objName, true /* silent */)
}

func doGet(wo *workOrder) {
	wo.size, wo.latencies, wo.err = tutils.Get(wo.proxyURL, wo.bucket, wo.objName, nil /* wg */, nil /* errCh */, true, /* silent */
		runParams.verifyHash /* validate */)
}

func doGetConfig(wo *workOrder) {
	wo.latencies, wo.err = tutils.GetConfig(wo.proxyURL)
}

func worker(wos <-chan *workOrder, results chan<- *workOrder, wg *sync.WaitGroup) {
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
		case opConfig:
			doGetConfig(wo)
		default:
			// Should not come here
		}

		wo.end = time.Now()
		results <- wo
	}
}
