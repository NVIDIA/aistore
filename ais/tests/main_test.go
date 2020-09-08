// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"flag"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
)

func setBucket() {
	var (
		err      error
		bucket   = os.Getenv("BUCKET")
		provider = os.Getenv("PROVIDER")
	)
	if bucket == "" {
		bucket = cmn.RandString(7)
		tutils.Logf("Using BUCKET=%q\n", bucket)
	}
	cliBck, _, err = cmn.ParseBckObjectURI(bucket)
	if err != nil {
		tutils.Logf("Failed to parse 'BUCKET' env variable, err: %v\n", err)
		os.Exit(1)
	}
	if provider != "" {
		if cliBck.Provider != "" && cliBck.Provider != provider {
			tutils.Logf("Provider is set for both 'BUCKET' (%q) and 'PROVIDER' (%q) env variables\n", cliBck, provider)
			os.Exit(1)
		}
		if !cmn.IsValidProvider(provider) {
			tutils.Logf("Invalid provider set for 'PROVIDER' (%q) env variable\n", provider)
			os.Exit(1)
		}
		cliBck.Provider = provider
	}
}

func waitForCluster() {
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
			tutils.Logf("Error EnvVars: %s. err: %v", cmn.EnvVars.NumProxy, err)
			os.Exit(1)
		}
		targetCnt, err = strconv.Atoi(tc)
		if err != nil {
			tutils.Logf("Error EnvVars: %s. err: %v", cmn.EnvVars.NumTarget, err)
			os.Exit(1)
		}
	}

	tutils.Logln("Waiting for clustermap init...")
	var cntNodes []int
	if proxyCnt != 0 || targetCnt != 0 {
		cntNodes = []int{proxyCnt, targetCnt}
	}
	_, err = tutils.WaitForPrimaryProxy(tutils.GetPrimaryURL(), "startup", -1, true, cntNodes...)
	if err != nil {
		tutils.Logf("Error waiting for cluster startup, err: %v\n", err)
		os.Exit(1)
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
			tutils.Logf("Error waiting for cluster startup, err: %v\n", err)
			os.Exit(1)
		}
		retry++
		time.Sleep(sleep)
	}
	tutils.Logln("Cluster ready...")
	time.Sleep(2 * time.Second)
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

	setBucket()
	waitForCluster()

	rand.Seed(time.Now().UnixNano())
	os.Exit(m.Run())
}
