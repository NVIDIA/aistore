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
	"github.com/NVIDIA/aistore/devtools/tutils"
)

var (
	proxyURL   string
	baseParams api.BaseParams
)

func setBucket() (bck cmn.Bck, err error) {
	var (
		bucket   = os.Getenv("BUCKET")
		provider = os.Getenv("PROVIDER")
	)
	if bucket == "" {
		bucket = cmn.RandString(7)
		tutils.Logf("Using BUCKET=%q\n", bucket)
	}
	bck, _, err = cmn.ParseBckObjectURI(bucket)
	if err != nil {
		return bck, fmt.Errorf("failed to parse 'BUCKET' env variable, err: %v", err)
	}
	if provider != "" {
		if bck.Provider != "" && bck.Provider != provider {
			return bck, fmt.Errorf("provider is set for both 'BUCKET' (%q) and 'PROVIDER' (%q) env variables",
				bck, provider)
		}
		if err = cmn.ValidateProvider(provider); err != nil {
			return bck, fmt.Errorf("%v (make sure 'PROVIDER' env variable is properly set)", err)
		}
		bck.Provider = provider
	}
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

	tutils.Logln("Waiting for clustermap init...")
	_, err = tutils.WaitForClusterState(tutils.GetPrimaryURL(), "startup", -1, proxyCnt, targetCnt)
	if err != nil {
		return fmt.Errorf("error waiting for cluster startup, err: %v", err)
	}

	tutils.Logln("Waiting for primary proxy health check...")
	retry := 0
	for {
		if retry > 0 {
			tutils.Logf("Pinging primary for health (#%d)...\n", retry+1)
		}
		err = api.Health(tutils.BaseAPIParams(tutils.GetPrimaryURL()))
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
	tutils.Logln("Cluster is ready")
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

	initTestEnv()
	var err error
	if cliBck, err = setBucket(); err == nil {
		err = waitForCluster()
	}

	if err != nil {
		tutils.Logln("FAIL: " + err.Error())
		os.Exit(1)
	}

	rand.Seed(time.Now().UnixNano())
	m.Run()
}
