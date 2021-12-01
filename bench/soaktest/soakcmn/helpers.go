// Package soakcmn provides constants, variables and functions shared across soaktest
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools"
)

var (
	HTTPClient *http.Client

	transportArgs = cmn.TransportArgs{
		Timeout:         600 * time.Second,
		UseHTTPProxyEnv: true,
	}

	registerTimeout = time.Minute * 2
	devtoolsCtx     *devtools.Ctx
)

func init() {
	envURL := os.Getenv(cmn.EnvVars.Endpoint)
	transportArgs.UseHTTPS = cos.IsHTTPS(envURL)
	transportArgs.SkipVerify = cos.IsParseBool(os.Getenv(cmn.EnvVars.SkipVerifyCrt))
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

func UnregisterNode(proxyURL string, args *cmn.ActValRmNode) error {
	return devtools.RemoveNodeFromSmap(devtoolsCtx, proxyURL, args.DaemonID, registerTimeout)
}
