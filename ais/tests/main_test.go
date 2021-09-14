// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools"
	"github.com/NVIDIA/aistore/devtools/tlog"
	"github.com/NVIDIA/aistore/devtools/tutils"
)

var (
	proxyURL   string
	baseParams api.BaseParams
)

func setBucket() (bck cmn.Bck, err error) {
	bucket := os.Getenv("BUCKET")
	if bucket == "" {
		bucket = cmn.ProviderAIS + cmn.BckProviderSeparator + cos.RandString(7)
	}
	bck, _, err = cmn.ParseBckObjectURI(bucket, cmn.ParseURIOpts{})
	if err != nil {
		return bck, fmt.Errorf("failed to parse 'BUCKET' env variable, err: %v", err)
	} else if err := bck.Validate(); err != nil {
		return bck, fmt.Errorf("failed to validate 'BUCKET' env variable, err: %v", err)
	}
	tlog.Logf("Using %q bucket\n", bck)
	return bck, nil
}

func waitForCluster() error {
	var (
		err        error
		proxyCnt   int
		targetCnt  int
		retryCount = 5
		sleep      = 5 * time.Second
	)
	pc := os.Getenv(cmn.EnvVars.NumProxy)
	tc := os.Getenv(cmn.EnvVars.NumTarget)
	if pc != "" || tc != "" {
		proxyCnt, err = strconv.Atoi(pc)
		if err != nil {
			return fmt.Errorf("error EnvVars: %s. err: %v", cmn.EnvVars.NumProxy, err)
		}
		targetCnt, err = strconv.Atoi(tc)
		if err != nil {
			return fmt.Errorf("error EnvVars: %s. err: %v", cmn.EnvVars.NumTarget, err)
		}
	}

	tlog.Logln("Waiting for cluster map init...")
	_, err = tutils.WaitForClusterState(tutils.GetPrimaryURL(), "startup", -1, proxyCnt, targetCnt)
	if err != nil {
		return fmt.Errorf("error waiting for cluster startup, err: %v", err)
	}

	tlog.Logln("Waiting for primary health check...")
	retry := 0
	for {
		if retry > 0 {
			tlog.Logf("Pinging primary for health (#%d)...\n", retry+1)
		}
		err = api.Health(tutils.BaseAPIParams(tutils.GetPrimaryURL()), true /*primary is ready to rebalance*/)
		if err == nil {
			time.Sleep(time.Second)
			break
		}
		if retry == retryCount {
			return fmt.Errorf("error waiting for cluster startup, err: %v", err)
		}
		retry++
		time.Sleep(sleep)
	}
	tlog.Logln("Cluster is ready")
	time.Sleep(2 * time.Second)
	return nil
}

func initTestEnv() {
	tutils.InitLocalCluster()
	proxyURL = tutils.RandomProxyURL()
	baseParams = tutils.BaseAPIParams(proxyURL)
}

func TestMain(m *testing.M) {
	flag.Parse()

	var (
		err    error
		exists bool
	)

	initTestEnv()
	if cliBck, err = setBucket(); err == nil {
		err = waitForCluster()
	}

	if err != nil {
		goto fail
	}

	if !cliBck.IsAIS() {
		exists, err = devtools.BckExists(tutils.DevtoolsCtx, tutils.GetPrimaryURL(), cliBck)
		if err == nil && !exists {
			err = fmt.Errorf("bucket %q does not exist, make sure that the cluster is compiled with selected provider", cliBck)
		}
		if err != nil {
			goto fail
		}
	}

	rand.Seed(time.Now().UnixNano())
	m.Run()
	return

fail:
	tlog.Logln("FAIL: " + err.Error())
	os.Exit(1)
}
