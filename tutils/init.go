// Package tutils provides common low-level utilities for all aistore unit and integration tests
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package tutils

import (
	"net/http"
	"net/http/httptrace"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/memsys"
)

const (
	registerTimeout = time.Minute * 2
	bucketTimeout   = time.Minute
)

type (
	tracectx struct {
		tr           *traceableTransport
		trace        *httptrace.ClientTrace
		tracedClient *http.Client
	}
)

var (
	transportArgs = cmn.TransportArgs{
		Timeout:          600 * time.Second,
		IdleConnsPerHost: 100,
		UseHTTPProxyEnv:  true,
	}
	transport         = cmn.NewTransport(transportArgs)
	DefaultHTTPClient = &http.Client{}
	HTTPClient        *http.Client
	HTTPClientGetPut  *http.Client

	Mem2 *memsys.Mem2
)

func init() {
	Mem2 = memsys.GMM()
	HTTPClient = cmn.NewClient(transportArgs)

	transportArgs.WriteBufferSize, transportArgs.ReadBufferSize = 65536, 65536
	HTTPClientGetPut = cmn.NewClient(transportArgs)
}

func newTraceCtx() *tracectx {
	tctx := &tracectx{}

	tctx.tr = &traceableTransport{
		transport: transport,
		tsBegin:   time.Now(),
	}
	tctx.trace = &httptrace.ClientTrace{
		GotConn:              tctx.tr.GotConn,
		WroteHeaders:         tctx.tr.WroteHeaders,
		WroteRequest:         tctx.tr.WroteRequest,
		GotFirstResponseByte: tctx.tr.GotFirstResponseByte,
	}
	tctx.tracedClient = &http.Client{
		Transport: tctx.tr,
		Timeout:   600 * time.Second,
	}

	return tctx
}
