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
	"github.com/NVIDIA/aistore/tutils"
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
		if !cmn.IsValidProvider(provider) {
			return bck, fmt.Errorf("invalid provider set for 'PROVIDER' (%q) env variable", provider)
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
	var cntNodes []int
	if proxyCnt != 0 || targetCnt != 0 {
		cntNodes = []int{proxyCnt, targetCnt}
	}
	_, err = tutils.WaitForPrimaryProxy(tutils.GetPrimaryURL(), "startup", -1, true, cntNodes...)
	if err != nil {
		return fmt.Errorf("error waiting for cluster startup, err: %v", err)
	}

	tutils.Logln("Waiting for primary proxy healthcheck...")
	retry := 0
	for {
		tutils.Logf("Pinging for health of primary, #iter: %d\n", retry)
		err = api.Health(tutils.BaseAPIParams(tutils.GetPrimaryURL()))
		if err == nil {
			break
		}
		if retry == retryCount {
			return fmt.Errorf("error waiting for cluster startup, err: %v", err)
		}
		retry++
		time.Sleep(sleep)
	}
	tutils.Logln("Cluster ready...")
	time.Sleep(2 * time.Second)
	return nil
}

func TestMain(m *testing.M) {
	flag.IntVar(&numfiles, "numfiles", 100, "Number of the files to download")
	flag.IntVar(&numworkers, "numworkers", 10, "Number of the workers")
	flag.StringVar(&match, "match", ".*", "object name regex")
	flag.UintVar(&pagesize, "pagesize", 0, "The maximum number of objects returned in one page")
	flag.StringVar(&prefix, "prefix", "", "Object name prefix")
	flag.BoolVar(&abortonerr, "abortonerr", abortonerr, "abort on error")
	flag.StringVar(&prefetchRange, "prefetchrange", prefetchRange, "Range for Prefix-Regex Prefetch")
	flag.BoolVar(&skipdel, "nodel", false, "Run only PUT and GET in a loop and do cleanup once at the end")
	flag.IntVar(&numops, "numops", 40, "Number of PUT/GET per worker")
	flag.IntVar(&fnlen, "fnlen", 20, "Length of randomly generated filenames")
	// When running multiple tests at the same time on different threads, ensure that
	// They are given different seeds, as the tests are completely deterministic based on
	// choice of seed, so they will interfere with each other.
	flag.Int64Var(&baseseed, "seed", baseseed, "Seed to use for random number generators")
	flag.DurationVar(&multiProxyTestDuration, "duration", 3*time.Minute,
		"The length to run the Multiple Proxy Stress Test for")
	flag.StringVar(&clichecksum, "checksum", "all", "all | xxhash | coldmd5")
	flag.IntVar(&cycles, "cycles", 15, "Number of PUT cycles")

	flag.Parse()

	var err error
	if cliBck, err = setBucket(); err == nil {
		err = waitForCluster()
	}

	if err != nil {
		tutils.Logln("FAIL: " + err.Error())
		os.Exit(1)
	}

	rand.Seed(time.Now().UnixNano())
	os.Exit(m.Run())
}
