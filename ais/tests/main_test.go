// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/devtools/tlog"
	"github.com/NVIDIA/aistore/devtools/trand"
	"github.com/NVIDIA/aistore/devtools/tutils"
)

var (
	proxyURL   string
	baseParams api.BaseParams
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

func waitForCluster() error {
	const (
		retryCount = 30
		sleep      = time.Second
	)
	var (
		err                        error
		proxyCnt, targetCnt, retry int
	)
	pc := os.Getenv(env.AIS.NumProxy)
	tc := os.Getenv(env.AIS.NumTarget)
	if pc != "" || tc != "" {
		proxyCnt, err = strconv.Atoi(pc)
		if err != nil {
			return fmt.Errorf("error EnvVars: %s. err: %v", env.AIS.NumProxy, err)
		}
		targetCnt, err = strconv.Atoi(tc)
		if err != nil {
			return fmt.Errorf("error EnvVars: %s. err: %v", env.AIS.NumTarget, err)
		}
	}
	_, err = tutils.WaitForClusterState(tutils.GetPrimaryURL(), "startup", -1, proxyCnt, targetCnt)
	if err != nil {
		return fmt.Errorf("error waiting for cluster startup, err: %v", err)
	}
	tlog.Logf("Pinging primary for readiness ")
	for {
		if retry%5 == 4 {
			fmt.Fprintf(os.Stdout, "%ds --- ", retry+1)
		}
		err = api.Health(tutils.BaseAPIParams(tutils.GetPrimaryURL()), true /*primary is ready to rebalance*/)
		if err == nil {
			fmt.Fprintln(os.Stdout, "")
			break
		}
		if retry >= retryCount {
			fmt.Fprintln(os.Stdout, "")
			return fmt.Errorf("timed out waiting for cluster startup: %v", err)
		}
		retry++
		time.Sleep(sleep)
	}
	tlog.Logln("Cluster is ready")
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
		exists, err = tutils.BucketExists(nil, tutils.GetPrimaryURL(), cliBck)
		if err == nil && !exists {
			s := "%q not found \n(hint: "
			s += "check whether %q exists and make sure to build aisnode executable with the corresponding build tag)"
			err = fmt.Errorf(s, cliBck, cliBck)
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
