/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

// worker routines

package main

import (
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client/readers"

	"github.com/NVIDIA/dfcpub/pkg/client"
)

func doPut(wo *workOrder) {
	var sgl *dfc.SGLIO
	if runParams.usingSG {
		sgl = dfc.NewSGLIO(uint64(wo.size))
		defer sgl.Free()
	}

	r, err := readers.NewReader(readers.ParamReader{
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

	wo.err = client.Put(wo.proxyURL, r, wo.bucket, wo.objName, true /* silent */)
}

func doGet(wo *workOrder) {
	wo.size, wo.latencies, wo.err = client.Get(wo.proxyURL, wo.bucket, wo.objName, nil /* wg */, nil /* errch */, true, /* silent */
		runParams.verifyHash /* validate */)
}

func doGetConfig(wo *workOrder) {
	wo.latencies, wo.err = client.GetConfig(wo.proxyURL)
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
