// Package tutils provides common low-level utilities for all aistore unit and integration tests
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package tutils

import (
	"net/http"
	"net/http/httptrace"
	"os"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/containers"
	"github.com/NVIDIA/aistore/memsys"
)

const (
	registerTimeout = time.Minute * 2
	bucketTimeout   = time.Minute
)

const (
	proxyURL      = "http://localhost:8080"      // the url for the cluster's proxy (local)
	dockerEnvFile = "/tmp/docker_ais/deploy.env" // filepath of Docker deployment config

)

type (
	tracectx struct {
		tr           *traceableTransport
		trace        *httptrace.ClientTrace
		tracedClient *http.Client
	}
)

var (
	proxyURLReadOnly string // user-defined primary proxy URL - it is read-only variable and tests mustn't change it

	transportArgs = cmn.TransportArgs{
		Timeout:          600 * time.Second,
		IdleConnsPerHost: 100,
		UseHTTPProxyEnv:  true,
	}
	transport         = cmn.NewTransport(transportArgs)
	DefaultHTTPClient = &http.Client{}
	HTTPClient        *http.Client
	HTTPClientGetPut  *http.Client

	RemoteCluster struct {
		UUID string
		URL  string
	}

	MMSA *memsys.MMSA
)

func init() {
	MMSA = memsys.DefaultPageMM()
	HTTPClient = cmn.NewClient(transportArgs)

	transportArgs.WriteBufferSize, transportArgs.ReadBufferSize = 65536, 65536
	HTTPClientGetPut = cmn.NewClient(transportArgs)

	initProxyURL()

	if remote := os.Getenv("REMOTE_CLUSTER"); remote != "" {
		parts := strings.Split(remote, "=")
		if len(parts) != 2 {
			cmn.ExitLogf("REMOTE_CLUSTER variable should be in form: UUID=URL")
		}
		RemoteCluster.UUID = parts[0]
		RemoteCluster.URL = parts[1]
	}
}

func initProxyURL() {
	envVars := ParseEnvVariables(dockerEnvFile)                        // Gets the fields from the .env file from which the docker was deployed
	primaryHostIP, port := envVars["PRIMARY_HOST_IP"], envVars["PORT"] // Host IP and port of primary cluster

	proxyURLReadOnly = proxyURL
	if containers.DockerRunning() && proxyURLReadOnly == proxyURL {
		proxyURLReadOnly = "http://" + primaryHostIP + ":" + port
	}

	// This is needed for testing on Kubernetes if we want to run 'make test-XXX'
	// Many of the other packages do not accept the 'url' flag
	cliAISURL := os.Getenv("AISURL")
	if cliAISURL != "" {
		proxyURLReadOnly = "http://" + cliAISURL
	}

	// Primary proxy can change if proxy tests are run and
	// no new cluster is re-deployed before each test.
	// Finds who is the current primary proxy.
	primary, err := GetPrimaryProxy(proxyURLReadOnly)

	// TODO: since `tutils` is used in various places like `aisloader` we cannot
	//  simply fail on error. If pinging proxy fails, it must be lazily
	//  discovered once `proxyURLReadOnly` is accessed somewhere in the code.
	proxyURLReadOnly = "FAILED TO INITIALIZE"
	if err == nil {
		proxyURLReadOnly = primary.URL(cmn.NetworkPublic)
	}
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
