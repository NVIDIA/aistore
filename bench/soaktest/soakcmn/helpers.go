// Package soakcmn provides constants, variables and functions shared across soaktest
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package soakcmn

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/devtools"
)

var (
	HTTPClient *http.Client

	transportArgs = cmn.TransportArgs{
		Timeout:          600 * time.Second,
		IdleConnsPerHost: 100,
		UseHTTPProxyEnv:  true,
	}

	registerTimeout = time.Minute * 2
	devtoolsCtx     *devtools.Ctx
)

func init() {
	envURL := os.Getenv(cmn.EnvVars.Endpoint)
	transportArgs.UseHTTPS = cmn.IsHTTPS(envURL)
	transportArgs.SkipVerify = cmn.IsParseBool(os.Getenv(cmn.EnvVars.SkipVerifyCrt))
	HTTPClient = cmn.NewClient(transportArgs)

	devtoolsCtx = &devtools.Ctx{
		Client: HTTPClient,
		Log:    logf,
	}
}

func logf(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
}

func BaseAPIParams(primaryURL string) api.BaseParams {
	return devtools.BaseAPIParams(devtoolsCtx, primaryURL)
}

func JoinCluster(proxyURL string, node *cluster.Snode) (string, error) {
	return devtools.JoinCluster(devtoolsCtx, proxyURL, node, registerTimeout)
}

func UnregisterNode(proxyURL string, args *cmn.ActValDecommision) error {
	return devtools.UnregisterNode(devtoolsCtx, proxyURL, args, registerTimeout)
}
