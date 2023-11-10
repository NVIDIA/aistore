// Package integration_test.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
)

var (
	proxyURL             string
	baseParams           api.BaseParams
	initialClusterConfig *cmn.ClusterConfig
)

func setBucket() (bck cmn.Bck, err error) {
	bucket := os.Getenv("BUCKET")
	if bucket == "" {
		bucket = apc.AIS + apc.BckProviderSeparator + trand.String(7)
	}
	bck, _, err = cmn.ParseBckObjectURI(bucket, cmn.ParseURIOpts{})
	if err != nil {
		return bck, fmt.Errorf("failed to parse 'BUCKET' env variable, err: %v", err)
	} else if err := bck.Validate(); err != nil {
		return bck, fmt.Errorf("failed to validate 'BUCKET' env variable, err: %v", err)
	}
	tlog.Logf("Using bucket %s\n", bck)
	return bck, nil
}

func waitForCluster() (primaryURL string, err error) {
	const (
		retryCount = 30
		sleep      = time.Second
	)
	var (
		proxyCnt, targetCnt, retry int
	)
	pc := os.Getenv(env.AIS.NumProxy)
	tc := os.Getenv(env.AIS.NumTarget)
	if pc != "" || tc != "" {
		proxyCnt, err = strconv.Atoi(pc)
		if err != nil {
			err = fmt.Errorf("error EnvVars: %s. err: %v", env.AIS.NumProxy, err)
			return
		}
		targetCnt, err = strconv.Atoi(tc)
		if err != nil {
			err = fmt.Errorf("error EnvVars: %s. err: %v", env.AIS.NumTarget, err)
			return
		}
	}
	_, err = tools.WaitForClusterState(tools.GetPrimaryURL(), "cluster startup", -1, proxyCnt, targetCnt)
	if err != nil {
		err = fmt.Errorf("error waiting for cluster startup, err: %v", err)
		return
	}
	tlog.Logf("Pinging primary for readiness ")
	for {
		if retry%5 == 4 {
			fmt.Fprintf(os.Stdout, "%ds --- ", retry+1)
		}
		primaryURL = tools.GetPrimaryURL()
		err = api.Health(tools.BaseAPIParams(primaryURL), true /*primary is ready to rebalance*/)
		if err == nil {
			fmt.Fprintln(os.Stdout, "")
			break
		}
		if retry >= retryCount {
			fmt.Fprintln(os.Stdout, "")
			err = fmt.Errorf("timed out waiting for cluster startup: %v", err)
			return
		}
		retry++
		time.Sleep(sleep)
	}
	tlog.Logln("Cluster is ready")
	return
}

func initTestEnv() {
	tools.InitLocalCluster()
	proxyURL = tools.RandomProxyURL()
	baseParams = tools.BaseAPIParams(proxyURL)
}

func TestMain(m *testing.M) {
	flag.Parse()

	var (
		primaryURL string
		err        error
		exists     bool
	)

	initTestEnv()
	if cliBck, err = setBucket(); err == nil {
		primaryURL, err = waitForCluster()
	}

	if err != nil {
		goto fail
	}

	if !cliBck.IsAIS() {
		exists, err = tools.BucketExists(nil, tools.GetPrimaryURL(), cliBck)
		if err == nil && !exists {
			s := "%q not found \n(hint: "
			s += "check whether %q exists and make sure to build aisnode executable with the corresponding build tag)"
			err = fmt.Errorf(s, cliBck, cliBck)
		}
		if err != nil {
			goto fail
		}
	}

	initialClusterConfig, err = api.GetClusterConfig(tools.BaseAPIParams(primaryURL))
	if err != nil {
		goto fail
	}

	m.Run()
	return

fail:
	tlog.Logln("FAIL: " + err.Error())
	os.Exit(1)
}

func TestInvalidHTTPMethod(t *testing.T) {
	bp := tools.BaseAPIParams()
	proxyURL := tools.RandomProxyURL(t)

	req, err := http.NewRequest("TEST", proxyURL, http.NoBody)
	tassert.CheckFatal(t, err)
	tassert.DoAndCheckResp(t, bp.Client, req, http.StatusBadRequest)
}
