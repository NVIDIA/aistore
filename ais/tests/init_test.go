// Package ais_test contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais_test

import (
	"flag"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/containers"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
)

const (
	baseDir                 = "/tmp/ais"
	LocalDestDir            = "/tmp/ais/dest" // client-side download destination
	LocalSrcDir             = "/tmp/ais/src"  // client-side src directory for upload
	ColdValidStr            = "coldmd5"
	ChksumValidStr          = "chksum"
	ColdMD5str              = "coldmd5"
	EvictCBStr              = "evictCB"
	DeleteDir               = "/tmp/ais/delete"
	ChecksumWarmValidateDir = "/tmp/ais/checksumWarmValidate"
	ChecksumWarmValidateStr = "checksumWarmValidate"
	RangeGetDir             = "/tmp/ais/rangeGet"
	RangeGetStr             = "rangeGet"
	DeleteStr               = "delete"
	SmokeDir                = "/tmp/ais/smoke" // smoke test dir
	SmokeStr                = "smoke"
	largeFileSize           = 4 * cmn.MiB
	proxyURL                = "http://localhost:8080"      // the url for the cluster's proxy (local)
	proxyURLNext            = "http://localhost:11080"     // the url for the next cluster's proxy (local)
	dockerEnvFile           = "/tmp/docker_ais/deploy.env" // filepath of Docker deployment config
	readerType              = tutils.ReaderTypeSG
	rebalanceTimeout        = 5 * time.Minute
	rebalanceStartTimeout   = 10 * time.Second
)

var (
	numops                 int
	numfiles               int
	numworkers             int
	match                  = ".*"
	pagesize               int64
	fnlen                  int
	prefix                 string
	abortonerr             = false
	prefetchRange          = "0:2000"
	skipdel                bool
	baseseed               = int64(1062984096)
	multiProxyTestDuration time.Duration
	clichecksum            string
	cycles                 int

	clibucket                string
	proxyURLReadOnly         string // user-defined primary proxy URL - it is read-only variable and tests mustn't change it
	proxyNextTierURLReadOnly string // user-defined primary proxy URL for the second cluster - it is read-only variable and tests mustn't change it

	envVars        = tutils.ParseEnvVariables(dockerEnvFile) // Gets the fields from the .env file from which the docker was deployed
	primaryHostIP  = envVars["PRIMARY_HOST_IP"]              // Host IP of primary cluster
	nextTierHostIP = envVars["NEXT_TIER_HOST_IP"]            // IP of the next tier cluster
	port           = envVars["PORT"]
)

func TestMain(m *testing.M) {
	flag.StringVar(&proxyNextTierURLReadOnly, "urlnext", proxyURLNext, "Proxy URL Next Tier")
	flag.IntVar(&numfiles, "numfiles", 100, "Number of the files to download")
	flag.IntVar(&numworkers, "numworkers", 10, "Number of the workers")
	flag.StringVar(&match, "match", ".*", "object name regex")
	flag.Int64Var(&pagesize, "pagesize", 1000, "The maximum number of objects returned in one page")
	flag.StringVar(&prefix, "prefix", "", "Object name prefix")
	flag.BoolVar(&abortonerr, "abortonerr", abortonerr, "abort on error")
	flag.StringVar(&prefetchRange, "prefetchrange", prefetchRange, "Range for Prefix-Regex Prefetch")
	flag.BoolVar(&skipdel, "nodel", false, "Run only PUT and GET in a loop and do cleanup once at the end")
	flag.IntVar(&numops, "numops", 4, "Number of PUT/GET per worker")
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

	proxyURLReadOnly = proxyURL
	if containers.DockerRunning() && proxyURLReadOnly == proxyURL {
		proxyURLReadOnly = "http://" + primaryHostIP + ":" + port
	}
	if containers.DockerRunning() && proxyNextTierURLReadOnly == proxyURLNext {
		proxyNextTierURLReadOnly = "http://" + nextTierHostIP + ":" + port
	}

	clibucket = os.Getenv("BUCKET")
	if clibucket == "" {
		cmn.ExitInfof("Bucket name is empty")
	}

	// This is needed for testing on Kubernetes if we want to run 'make test-XXX'
	// Many of the other packages do not accept the 'url' flag
	cliAISURL := os.Getenv("AISURL")
	if cliAISURL != "" {
		proxyURLReadOnly = "http://" + cliAISURL
	}
	// primary proxy can change if proxy tests are run and no new cluster is re-deployed before each test.
	// finds who is the current primary proxy
	primary, err := tutils.GetPrimaryProxy(proxyURLReadOnly)
	if err != nil {
		cmn.ExitInfof("Failed to get primary proxy, err = %v", err)
	}
	proxyURLReadOnly = primary.URL(cmn.NetworkPublic)

	if proxyURLReadOnly == proxyNextTierURLReadOnly {
		cmn.ExitInfof("Proxy URL for first and next tier cluster cannot be the same")
	}

	rand.Seed(time.Now().UnixNano())
	os.Exit(m.Run())
}
