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

	"github.com/NVIDIA/aistore/cmn"
)

const (
	baseDir                 = "/tmp/ais"
	ColdValidStr            = "coldmd5"
	ChksumValidStr          = "chksum"
	ColdMD5str              = "coldmd5"
	EvictCBStr              = "evictCB"
	ChecksumWarmValidateStr = "checksumWarmValidate"
	RangeGetStr             = "rangeGet"
	DeleteStr               = "delete"
	SmokeStr                = "smoke"
	largeFileSize           = 4 * cmn.MiB
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
	prefetchRange          = "{0..200}"
	skipdel                bool
	baseseed               = int64(1062984096)
	multiProxyTestDuration time.Duration
	clichecksum            string
	cycles                 int

	clibucket string
)

func TestMain(m *testing.M) {
	flag.IntVar(&numfiles, "numfiles", 100, "Number of the files to download")
	flag.IntVar(&numworkers, "numworkers", 10, "Number of the workers")
	flag.StringVar(&match, "match", ".*", "object name regex")
	flag.Int64Var(&pagesize, "pagesize", 0, "The maximum number of objects returned in one page")
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

	clibucket = os.Getenv("BUCKET")
	if clibucket == "" {
		cmn.ExitInfof("Bucket name is empty")
	}

	rand.Seed(time.Now().UnixNano())
	os.Exit(m.Run())
}
