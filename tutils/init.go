// Package tutils provides common low-level utilities for all aistore unit and integration tests
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package tutils

import (
	"net"
	"net/http"
	"net/http/httptrace"
	"time"

	"github.com/NVIDIA/aistore/memsys"
)

const (
	registerTimeout = time.Minute * 2
	bucketTimeout   = time.Minute // wait for bucket is available on all targets
)

var (
	transport = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: 60 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: 600 * time.Second,
		MaxIdleConnsPerHost: 100, // arbitrary number, to avoid connect: cannot assign requested address
	}
	BaseHTTPClient = &http.Client{}
	HTTPClient     = &http.Client{
		Timeout:   600 * time.Second,
		Transport: transport,
	}
	Mem2 *memsys.Mem2
)

func init() {
	Mem2 = memsys.GMM()
}

type (
	tracectx struct {
		tr           *traceableTransport
		trace        *httptrace.ClientTrace
		tracedClient *http.Client
	}
)

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
