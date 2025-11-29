// Package integration_test.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
)

var (
	proxyURL             string
	baseParams           api.BaseParams
	initialClusterConfig *cmn.ClusterConfig
)

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

	cliIOCtxChunksConf, err = setIOCtxChunksConf()
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

	if err := setSignHMAC(); err != nil {
		goto fail
	}

	m.Run()
	return

fail:
	tlog.Logln("FAIL: " + err.Error())
	os.Exit(1)
}

func waitForCluster() (primaryURL string, err error) {
	const (
		retryCount = 30
		sleep      = time.Second
	)
	var (
		proxyCnt, targetCnt, retry int
	)
	pc := os.Getenv(env.TestNumProxy)
	tc := os.Getenv(env.TestNumTarget)
	if pc != "" || tc != "" {
		proxyCnt, err = strconv.Atoi(pc)
		if err != nil {
			err = fmt.Errorf("error EnvVars: %s. err: %v", env.TestNumProxy, err)
			return
		}
		targetCnt, err = strconv.Atoi(tc)
		if err != nil {
			err = fmt.Errorf("error EnvVars: %s. err: %v", env.TestNumTarget, err)
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
	tlog.Logfln("Using bucket %s", bck.String())
	return bck, nil
}

func setIOCtxChunksConf() (*ioCtxChunksConf, error) {
	ioctx := os.Getenv(env.TestNumChunks)
	if ioctx == "" {
		return nil, nil
	}
	n, err := strconv.Atoi(ioctx)
	switch {
	case err != nil:
		return nil, fmt.Errorf("failed to parse '%s=%s' env variable, err: %v", env.TestNumChunks, ioctx, err)
	case n == 0:
		return nil, nil
	case n < 0:
		return nil, fmt.Errorf("invalid '%s=%s' env variable (must be non-negative integer)", env.TestNumChunks, ioctx)
	}
	return &ioCtxChunksConf{
		numChunks: n,
		multipart: true,
	}, nil
}

func setSignHMAC() error {
	s := os.Getenv(env.TestSignHMAC)
	if s == "" {
		return nil
	}
	enable, err := strconv.ParseBool(s)
	if err != nil || !enable {
		return err
	}
	proxyURL := tools.GetPrimaryURL()
	bp := tools.BaseAPIParams(proxyURL)
	return api.SetClusterConfig(bp, cos.StrKVs{"auth.cluster_key.enabled": "true"}, false /*transient*/)
}
