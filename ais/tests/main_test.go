// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"flag"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
)

func TestMain(m *testing.M) {
	flag.IntVar(&numfiles, "numfiles", 100, "Number of the files to download")
	flag.IntVar(&numworkers, "numworkers", 10, "Number of the workers")
	flag.StringVar(&match, "match", ".*", "object name regex")
	flag.UintVar(&pagesize, "pagesize", 0, "The maximum number of objects returned in one page")
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
		clibucket = cmn.RandString(7)
		tutils.Logf("Using BUCKET=%q\n", clibucket)
	}

	rand.Seed(time.Now().UnixNano())
	os.Exit(m.Run())
}
