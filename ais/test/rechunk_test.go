// Package integration_test.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact"
)

func TestRechunk(t *testing.T) {
	// 1) provision small and large sized objects in the same bucket with `ioContexts.chunksConf` set to `initialLimit`
	// 2) rechunk using api.RechunkBucket (direct POST approach)
	// 3) verify the chunking state of the objects
	tests := []struct {
		name         string
		initialLimit int64 // initial ObjSizeLimit (0 = infinite, all monolithic)
		finalLimit   int64 // final ObjSizeLimit after rechunk
		smallSize    int64 // small object size
		largeSize    int64 // large object size
		chunkSize    int64 // chunk size to use
	}{
		{
			name:         "disabled-to-chunked",
			initialLimit: 0,
			finalLimit:   50 * cos.KiB,
			chunkSize:    16 * cos.KiB,
			smallSize:    10 * cos.KiB,  // monolithic => monolithic
			largeSize:    100 * cos.KiB, // monolithic => chunked
		},
		{
			name:         "chunked-to-disabled",
			initialLimit: 50 * cos.KiB,
			finalLimit:   0, // change to no chunking (all monolithic)
			chunkSize:    16 * cos.KiB,
			smallSize:    10 * cos.KiB,  // monolithic => monolithic
			largeSize:    100 * cos.KiB, // chunked => monolithic
		},
		{
			name:         "monolithic-to-chunked",
			initialLimit: 1 * cos.MiB,
			finalLimit:   50 * cos.KiB,
			chunkSize:    16 * cos.KiB,
			smallSize:    10 * cos.KiB,  // monolithic => monolithic
			largeSize:    100 * cos.KiB, // monolithic => chunked
		},
		{
			name:         "chunked-to-monolithic",
			initialLimit: 50 * cos.KiB,
			finalLimit:   1 * cos.MiB,
			chunkSize:    16 * cos.KiB,
			smallSize:    10 * cos.KiB,  // monolithic => monolithic
			largeSize:    100 * cos.KiB, // chunked => monolithic
		},
		{
			name:         "rechunk-smaller-chunks",
			initialLimit: 50 * cos.KiB,
			finalLimit:   60 * cos.KiB,
			chunkSize:    8 * cos.KiB,
			smallSize:    10 * cos.KiB,  // monolithic => monolithic
			largeSize:    100 * cos.KiB, // chunked => chunked
		},
	}

	for _, tt := range tests {
		for _, approach := range []string{"xaction", "post"} {
			t.Run(tt.name+"/"+approach, func(t *testing.T) {
				testRechunkScenario(t, tt.smallSize, tt.largeSize, tt.initialLimit, tt.finalLimit, tt.chunkSize, approach)
			})
		}
	}
}

// startRechunk triggers rechunk using one of two approaches:
// - "xaction": Change bucket props + `api.StartXaction`
// - "post": Direct `api.RechunkBucket` call
func startRechunk(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, objSizeLimit, chunkSize int64, approach string) (xid string, err error) {
	t.Helper()

	switch approach {
	case "xaction":
		_, err = api.SetBucketProps(baseParams, bck, &cmn.BpropsToSet{
			Chunks: &cmn.ChunksConfToSet{
				ObjSizeLimit: apc.Ptr(cos.SizeIEC(objSizeLimit)),
				ChunkSize:    apc.Ptr(cos.SizeIEC(chunkSize)),
			},
		})
		if err != nil {
			return "", err
		}

		xargs := &xact.ArgsMsg{
			Kind: apc.ActRechunk,
			Bck:  bck,
		}
		return api.StartXaction(baseParams, xargs, "")

	case "post":
		return api.RechunkBucket(baseParams, bck, objSizeLimit, chunkSize, "" /*prefix*/)

	default:
		t.Fatalf("unknown rechunk approach: %q (must be 'xaction' or 'post')", approach)
		return "", nil
	}
}

func testRechunkScenario(t *testing.T, smallSize, largeSize, initialLimit, finalLimit, chunkSize int64, approach string) {
	var (
		numSmall   = 5
		numLarge   = 5
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
		}
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	initMountpaths(t, proxyURL)

	// Save original bucket props for cleanup
	p, err := api.HeadBucket(baseParams, bck, false)
	tassert.CheckFatal(t, err)

	t.Cleanup(func() {
		_, err := api.SetBucketProps(baseParams, bck, &cmn.BpropsToSet{
			Chunks: &cmn.ChunksConfToSet{
				ObjSizeLimit: apc.Ptr(p.Chunks.ObjSizeLimit),
				ChunkSize:    apc.Ptr(p.Chunks.ChunkSize),
			},
		})
		tassert.CheckError(t, err)
	})

	// Set initial chunk config
	tlog.Logfln("Setting initial chunk config (limit=%s, chunkSize=%s)...", cos.ToSizeIEC(initialLimit, 0), cos.ToSizeIEC(chunkSize, 0))
	_, err = api.SetBucketProps(baseParams, bck, &cmn.BpropsToSet{
		Chunks: &cmn.ChunksConfToSet{
			ObjSizeLimit: apc.Ptr(cos.SizeIEC(initialLimit)),
			ChunkSize:    apc.Ptr(cos.SizeIEC(chunkSize)),
		},
	})
	tassert.CheckFatal(t, err)

	// Create objects with initial config
	tlog.Logfln("Creating %d small objects (size=%s) and %d large objects (size=%s)...",
		numSmall, cos.ToSizeIEC(smallSize, 0), numLarge, cos.ToSizeIEC(largeSize, 0))

	mSmall := ioContext{
		t:             t,
		bck:           bck,
		num:           numSmall,
		fileSizeRange: [2]uint64{uint64(smallSize + 1), uint64(smallSize + chunkSize - 1)},
		prefix:        "small-",
		chunksConf:    &ioCtxChunksConf{multipart: false},
	}
	if initialLimit > 0 && smallSize > initialLimit {
		maxSize := int64(mSmall.fileSizeRange[1])
		mSmall.chunksConf = &ioCtxChunksConf{multipart: true, numChunks: int((maxSize + chunkSize - 1) / chunkSize)}
	}
	mSmall.init(true /*cleanup*/)
	mSmall.puts()

	mLarge := ioContext{
		t:             t,
		bck:           bck,
		num:           numLarge,
		fileSizeRange: [2]uint64{uint64(largeSize + 1), uint64(largeSize + chunkSize - 1)},
		prefix:        "large-",
		chunksConf:    &ioCtxChunksConf{multipart: false},
	}
	if initialLimit > 0 && largeSize > initialLimit {
		maxSize := int64(mLarge.fileSizeRange[1])
		mLarge.chunksConf = &ioCtxChunksConf{multipart: true, numChunks: int((maxSize + chunkSize - 1) / chunkSize)}
	}
	mLarge.init(true /*cleanup*/)
	mLarge.puts()

	// Verify initial state
	validateChunking(t, baseParams, bck, initialLimit, chunkSize, &mSmall, &mLarge, "initial")

	// Start rechunk with new config
	tlog.Logfln("Starting rechunk (approach=%s, limit=%s, chunkSize=%s)...", approach, cos.ToSizeIEC(finalLimit, 0), cos.ToSizeIEC(chunkSize, 0))
	xid, err := startRechunk(t, baseParams, bck, finalLimit, chunkSize, approach)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, xid != "", "rechunk xaction ID should not be empty")

	// Wait for rechunk to complete
	tlog.Logfln("Waiting for rechunk xaction %s to complete...", xid)
	_, err = api.WaitForXactionIC(baseParams, &xact.ArgsMsg{ID: xid, Kind: apc.ActRechunk, Bck: bck, Timeout: 2 * time.Minute})
	tassert.CheckFatal(t, err)

	// Verify final state after rechunk
	validateChunking(t, baseParams, bck, finalLimit, chunkSize, &mSmall, &mLarge, "after rechunk")

	tlog.Logfln("SUCCESS: Rechunk completed and validated")
}

func TestRechunkAbort(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
	var (
		numObjs    = 10000         // large enough to ensure rechunk takes time
		objSize    = 100 * cos.KiB // objects that will be chunked
		chunkSize  = 16 * cos.KiB
		sizeLimit  = 50 * cos.KiB // chunk objects > 50KiB
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
		}
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	initMountpaths(t, proxyURL)

	// Save original bucket props for cleanup
	p, err := api.HeadBucket(baseParams, bck, false)
	tassert.CheckFatal(t, err)

	t.Cleanup(func() {
		_, err := api.SetBucketProps(baseParams, bck, &cmn.BpropsToSet{
			Chunks: &cmn.ChunksConfToSet{
				ObjSizeLimit: apc.Ptr(p.Chunks.ObjSizeLimit),
				ChunkSize:    apc.Ptr(p.Chunks.ChunkSize),
			},
		})
		tassert.CheckError(t, err)
	})

	m := ioContext{
		t:             t,
		bck:           bck,
		num:           numObjs,
		fileSizeRange: [2]uint64{uint64(objSize), uint64(objSize + chunkSize - 1)},
		prefix:        "rechunk-abort-",
		chunksConf:    &ioCtxChunksConf{multipart: false},
	}
	m.init(true /*cleanup*/)
	m.puts()

	// Start rechunk with xaction approach
	tlog.Logfln("Starting rechunk (limit=%s, chunkSize=%s)...", cos.ToSizeIEC(int64(sizeLimit), 0), cos.ToSizeIEC(int64(chunkSize), 0))
	xid, err := startRechunk(t, baseParams, bck, int64(sizeLimit), int64(chunkSize), "xaction")
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, xid != "", "rechunk xaction ID should not be empty")
	tlog.Logfln("Rechunk xaction started with ID: %s", xid)

	// Wait a bit to let rechunk start processing some objects
	time.Sleep(500 * time.Millisecond)

	// Abort the rechunk xaction
	tlog.Logfln("Aborting rechunk xaction %s...", xid)
	abortArgs := &xact.ArgsMsg{
		ID:   xid,
		Kind: apc.ActRechunk,
		Bck:  bck,
	}
	err = api.AbortXaction(baseParams, abortArgs)
	tassert.CheckFatal(t, err)
	tlog.Logln("Rechunk xaction aborted successfully")

	// List objects and count how many are chunked (informational)
	tlog.Logln("Listing objects to check chunking state after abort...")
	lsmsg := &apc.LsoMsg{Props: apc.GetPropsChunked}
	lst, err := api.ListObjects(baseParams, bck, lsmsg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(lst.Entries) == numObjs, "expected %d total objects, got %d", numObjs, len(lst.Entries))

	var chunkedCount int
	for _, entry := range lst.Entries {
		if (entry.Flags & apc.EntryIsChunked) != 0 {
			chunkedCount++
		}
	}
	tlog.Logfln("After abort: %d/%d objects are chunked (partial rechunk completed)", chunkedCount, len(lst.Entries))
	if chunkedCount == 0 || chunkedCount == numObjs {
		tlog.Logfln("Warning: all objects are chunked or none are chunked after abort")
	}

	// Validate all objects are accessible and not corrupted
	tlog.Logln("Validating object data integrity after abort...")
	m.gets(nil, true /*withValidation*/)
	tlog.Logfln("SUCCESS: All %d objects validated successfully after rechunk abort", m.num)
}

type chunkProps struct {
	sizeLimit int64
	chunkSize int64
}

func TestRechunkTwice(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	tests := []struct {
		name                 string
		firstProps           chunkProps
		secondProps          chunkProps
		expectedErrorMessage string
	}{
		{
			name:                 "same-args",
			firstProps:           chunkProps{sizeLimit: 50 * cos.KiB, chunkSize: 16 * cos.KiB},
			secondProps:          chunkProps{sizeLimit: 50 * cos.KiB, chunkSize: 16 * cos.KiB},
			expectedErrorMessage: "is already running - not starting",
		},
		{
			name:                 "different-args",
			firstProps:           chunkProps{sizeLimit: 50 * cos.KiB, chunkSize: 16 * cos.KiB},
			secondProps:          chunkProps{sizeLimit: 30 * cos.KiB, chunkSize: 8 * cos.KiB},
			expectedErrorMessage: "rechunk with different objsize_limit",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testRechunkTwiceScenario(t, tt.firstProps, tt.secondProps, tt.expectedErrorMessage)
		})
	}
}

func testRechunkTwiceScenario(t *testing.T, firstProps, secondProps chunkProps, expectedErrorMsg string) {
	var (
		numObjs    = 5000 // large enough to ensure rechunk takes time
		objSize    = 100 * cos.KiB
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
		}
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	initMountpaths(t, proxyURL)

	// Save original bucket props for cleanup
	p, err := api.HeadBucket(baseParams, bck, false)
	tassert.CheckFatal(t, err)

	t.Cleanup(func() {
		_, err := api.SetBucketProps(baseParams, bck, &cmn.BpropsToSet{
			Chunks: &cmn.ChunksConfToSet{
				ObjSizeLimit: apc.Ptr(p.Chunks.ObjSizeLimit),
				ChunkSize:    apc.Ptr(p.Chunks.ChunkSize),
			},
		})
		tassert.CheckError(t, err)
	})

	// Create objects as monolithic (no chunking initially)
	tlog.Logfln("Creating %d objects (size=%s) as monolithic...", numObjs, cos.ToSizeIEC(int64(objSize), 0))
	m := ioContext{
		t:             t,
		bck:           bck,
		num:           numObjs,
		fileSizeRange: [2]uint64{uint64(objSize), uint64(objSize + int(firstProps.chunkSize) - 1)},
		prefix:        "rechunk-twice-",
		chunksConf:    &ioCtxChunksConf{multipart: false},
	}
	m.init(true /*cleanup*/)
	m.puts()

	// Start first rechunk with xaction approach
	tlog.Logfln("Starting first rechunk (limit=%s, chunkSize=%s)...",
		cos.ToSizeIEC(firstProps.sizeLimit, 0), cos.ToSizeIEC(firstProps.chunkSize, 0))
	xid1, err := startRechunk(t, baseParams, bck, firstProps.sizeLimit, firstProps.chunkSize, "xaction")
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, xid1 != "", "first rechunk xaction ID should not be empty")
	tlog.Logfln("First rechunk xaction started with ID: %s", xid1)

	// Change to second chunk config
	tlog.Logfln("Changing chunk config to different values (limit=%s, chunkSize=%s)...",
		cos.ToSizeIEC(secondProps.sizeLimit, 0), cos.ToSizeIEC(secondProps.chunkSize, 0))
	_, err = api.SetBucketProps(baseParams, bck, &cmn.BpropsToSet{
		Chunks: &cmn.ChunksConfToSet{
			ObjSizeLimit: apc.Ptr(cos.SizeIEC(secondProps.sizeLimit)),
			ChunkSize:    apc.Ptr(cos.SizeIEC(secondProps.chunkSize)),
		},
	})
	tassert.CheckFatal(t, err)

	// Immediately try to start second rechunk xaction on the same bucket
	tlog.Logln("Attempting to start second rechunk xaction on same bucket (should fail)...")
	xargs := &xact.ArgsMsg{
		Kind: apc.ActRechunk,
		Bck:  bck,
	}
	xid2, err := api.StartXaction(baseParams, xargs, "")

	// Should get expected error
	tassert.Fatalf(t, err != nil, "expected error when starting duplicate rechunk, but got xid=%s", xid2)
	herr, ok := err.(*cmn.ErrHTTP)
	tassert.Fatalf(t, ok && strings.Contains(herr.Message, expectedErrorMsg),
		"expected error containing %q, got: %v", expectedErrorMsg, err)
	tlog.Logfln("SUCCESS: Got expected error: %v", err)

	// Wait for first rechunk to complete
	tlog.Logfln("Waiting for first rechunk xaction %s to complete...", xid1)
	_, err = api.WaitForXactionIC(baseParams, &xact.ArgsMsg{ID: xid1, Kind: apc.ActRechunk, Bck: bck, Timeout: 5 * time.Minute})
	tassert.CheckFatal(t, err)
	tlog.Logln("First rechunk xaction completed successfully")

	// Validate all objects are properly chunked (according to first props)
	tlog.Logln("Validating that all objects are chunked...")
	lsmsg := &apc.LsoMsg{Props: apc.GetPropsChunked}
	lst, err := api.ListObjects(baseParams, bck, lsmsg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(lst.Entries) == numObjs, "expected %d total objects, got %d", numObjs, len(lst.Entries))

	var chunkedCount int
	for _, entry := range lst.Entries {
		if (entry.Flags & apc.EntryIsChunked) != 0 {
			chunkedCount++
		}
	}
	tlog.Logfln("Chunking validation: %d/%d objects are chunked", chunkedCount, len(lst.Entries))
	tassert.Fatalf(t, chunkedCount == numObjs, "expected all %d objects to be chunked, but only %d are", numObjs, chunkedCount)

	// Validate all objects are accessible and data is intact
	tlog.Logln("Validating object data integrity with GET...")
	m.gets(nil, true /*withValidation*/)

	// Validate chunks on disk (all objects should be chunked with firstProps.chunkSize)
	tlog.Logfln("Validating chunk files on disk (chunkSize=%s)...", cos.ToSizeIEC(firstProps.chunkSize, 0))
	m.objNames = m.objNames[:100] // validate only first 100 objects
	validateChunksOnDisk(t, &m, true /*shouldBeChunked*/, firstProps.chunkSize)

	tlog.Logfln("SUCCESS: All %d objects are properly chunked and validated", m.num)
}

func TestRechunkWhenRebRes(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	var (
		numObjs   = 20000 // large enough to ensure rebalance takes time
		objSize   = 200 * cos.KiB
		chunkSize = 16 * cos.KiB
		sizeLimit = 50 * cos.KiB
		m         = ioContext{
			t:             t,
			num:           numObjs,
			fileSizeRange: [2]uint64{uint64(objSize), uint64(objSize + chunkSize - 1)},
			chunksConf:    &ioCtxChunksConf{multipart: false},
		}
	)

	m.initAndSaveState(true /*cleanup*/)
	m.expectTargets(2)

	// Prepare for rebalance
	target := m.startMaintenanceNoRebalance()
	tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)
	m.puts()

	baseParams := tools.BaseAPIParams(m.proxyURL)

	// Save original bucket props for cleanup
	p, err := api.HeadBucket(baseParams, m.bck, false)
	tassert.CheckFatal(t, err)

	t.Cleanup(func() {
		_, err := api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{
			Chunks: &cmn.ChunksConfToSet{
				ObjSizeLimit: apc.Ptr(p.Chunks.ObjSizeLimit),
				ChunkSize:    apc.Ptr(p.Chunks.ChunkSize),
			},
		})
		tassert.CheckError(t, err)
	})

	// Configure chunking on the bucket BEFORE triggering rebalance
	tlog.Logfln("Setting chunk config (limit=%s, chunkSize=%s)...",
		cos.ToSizeIEC(int64(sizeLimit), 0), cos.ToSizeIEC(int64(chunkSize), 0))
	_, err = api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{
		Chunks: &cmn.ChunksConfToSet{
			ObjSizeLimit: apc.Ptr(cos.SizeIEC(sizeLimit)),
			ChunkSize:    apc.Ptr(cos.SizeIEC(chunkSize)),
		},
	})
	tassert.CheckFatal(t, err)

	// Trigger rebalance right before attempting rechunk
	rebID := m.stopMaintenance(target)
	tlog.Logfln("Rebalance started with ID: %s", rebID)

	xargs := xact.ArgsMsg{Kind: apc.ActRebalance, Timeout: tools.RebalanceStartTimeout}
	err = api.WaitForXactionNode(baseParams, &xargs, xactSnapRunning)
	tassert.CheckError(t, err)
	tlog.Logln("Rebalance is now running")

	defer func() {
		// Wait for rebalance to complete
		args := xact.ArgsMsg{ID: rebID, Kind: apc.ActRebalance, Timeout: tools.RebalanceTimeout}
		_, _ = api.WaitForXactionIC(baseParams, &args)
	}()

	// Try to start rechunk while rebalance is running - should fail immediately
	tlog.Logln("Attempting to start rechunk while rebalance is running...")
	rechunkArgs := &xact.ArgsMsg{
		Kind: apc.ActRechunk,
		Bck:  m.bck,
	}
	_, err = api.StartXaction(baseParams, rechunkArgs, "")

	tassert.Fatalf(t, err != nil, "expected error when starting rechunk during rebalance, but got none")
}

func TestRechunkPrefix(t *testing.T) {
	var (
		numPrefixed    = 50
		numNonPrefixed = 50
		objSize        = 100 * cos.KiB
		chunkSize      = 16 * cos.KiB
		sizeLimit      = 50 * cos.KiB
		targetPrefix   = "prefixed-"
		otherPrefix    = "other-"
		proxyURL       = tools.RandomProxyURL(t)
		baseParams     = tools.BaseAPIParams(proxyURL)
		bck            = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
		}
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	initMountpaths(t, proxyURL)

	// Save original bucket props for cleanup
	p, err := api.HeadBucket(baseParams, bck, false)
	tassert.CheckFatal(t, err)

	t.Cleanup(func() {
		_, err := api.SetBucketProps(baseParams, bck, &cmn.BpropsToSet{
			Chunks: &cmn.ChunksConfToSet{
				ObjSizeLimit: apc.Ptr(p.Chunks.ObjSizeLimit),
				ChunkSize:    apc.Ptr(p.Chunks.ChunkSize),
			},
		})
		tassert.CheckError(t, err)
	})

	// Create objects with target prefix
	tlog.Logfln("Creating %d objects with prefix %q (size=%s)...", numPrefixed, targetPrefix, cos.ToSizeIEC(int64(objSize), 0))
	mPrefixed := ioContext{
		t:             t,
		bck:           bck,
		num:           numPrefixed,
		fileSizeRange: [2]uint64{uint64(objSize), uint64(objSize + chunkSize - 1)},
		prefix:        targetPrefix,
		chunksConf:    &ioCtxChunksConf{multipart: false},
	}
	mPrefixed.init(true /*cleanup*/)
	mPrefixed.puts()

	// Create objects with different prefix
	tlog.Logfln("Creating %d objects with prefix %q (size=%s)...", numNonPrefixed, otherPrefix, cos.ToSizeIEC(int64(objSize), 0))
	mOther := ioContext{
		t:             t,
		bck:           bck,
		num:           numNonPrefixed,
		fileSizeRange: [2]uint64{uint64(objSize), uint64(objSize + chunkSize - 1)},
		prefix:        otherPrefix,
		chunksConf:    &ioCtxChunksConf{multipart: false},
	}
	mOther.init(true /*cleanup*/)
	mOther.puts()

	// Run rechunk with prefix filter using api.RechunkBucket
	tlog.Logfln("Starting rechunk with prefix filter %q (limit=%s, chunkSize=%s)...",
		targetPrefix, cos.ToSizeIEC(int64(sizeLimit), 0), cos.ToSizeIEC(int64(chunkSize), 0))
	xid, err := api.RechunkBucket(baseParams, bck, int64(sizeLimit), int64(chunkSize), targetPrefix)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, xid != "", "rechunk xaction ID should not be empty")
	tlog.Logfln("Rechunk xaction started with ID: %s", xid)

	// Wait for rechunk to complete
	tlog.Logln("Waiting for rechunk to complete...")
	_, err = api.WaitForXactionIC(baseParams, &xact.ArgsMsg{ID: xid, Kind: apc.ActRechunk, Bck: bck, Timeout: 5 * time.Minute})
	tassert.CheckFatal(t, err)
	tlog.Logln("Rechunk completed")

	// List objects and validate chunking state
	tlog.Logln("Validating chunking state - only prefixed objects should be chunked...")
	lsmsg := &apc.LsoMsg{Props: apc.GetPropsChunked}
	lst, err := api.ListObjects(baseParams, bck, lsmsg, api.ListArgs{})
	tassert.CheckFatal(t, err)

	expectedTotal := numPrefixed + numNonPrefixed
	tassert.Fatalf(t, len(lst.Entries) == expectedTotal,
		"expected %d total objects, got %d", expectedTotal, len(lst.Entries))

	var prefixedChunked, otherChunked int
	var prefixedTotal, otherTotal int

	for _, entry := range lst.Entries {
		isChunked := (entry.Flags & apc.EntryIsChunked) != 0
		hasPrefixed := strings.HasPrefix(entry.Name, targetPrefix)
		hasOther := strings.HasPrefix(entry.Name, otherPrefix)

		if hasPrefixed {
			prefixedTotal++
			if isChunked {
				prefixedChunked++
			}
		} else if hasOther {
			otherTotal++
			if isChunked {
				otherChunked++
			}
		}
	}

	tlog.Logfln("Prefixed objects (%s): %d/%d chunked", targetPrefix, prefixedChunked, prefixedTotal)
	tlog.Logfln("Other objects (%s): %d/%d chunked", otherPrefix, otherChunked, otherTotal)

	// Validate counts
	tassert.Fatalf(t, prefixedTotal == numPrefixed, "expected %d prefixed objects, found %d", numPrefixed, prefixedTotal)
	tassert.Fatalf(t, otherTotal == numNonPrefixed, "expected %d other objects, found %d", numNonPrefixed, otherTotal)

	// Validate that all prefixed objects are chunked
	tassert.Fatalf(t, prefixedChunked == numPrefixed, "expected all %d prefixed objects to be chunked, but only %d are", numPrefixed, prefixedChunked)

	// Validate that no other objects are chunked
	tassert.Fatalf(t, otherChunked == 0, "expected 0 other objects to be chunked, but %d are", otherChunked)

	// Validate chunks on disk
	tlog.Logfln("Validating chunk files on disk for prefixed objects...")
	validateChunksOnDisk(t, &mPrefixed, true /*shouldBeChunked*/, int64(chunkSize))

	tlog.Logfln("Validating chunk files on disk for other objects...")
	validateChunksOnDisk(t, &mOther, false /*shouldBeChunked*/, int64(chunkSize))
}

func TestRechunkIdempotent(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	var (
		numSmall  = 50
		numLarge  = 50
		smallSize = 30 * cos.KiB
		largeSize = 100 * cos.KiB
		chunkSize = 16 * cos.KiB
		sizeLimit = 50 * cos.KiB // between small and large
		proxyURL  = tools.RandomProxyURL(t)
		bck       = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
		}
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	initMountpaths(t, proxyURL)

	baseParams := tools.BaseAPIParams(proxyURL)

	// Save original bucket props for cleanup
	p, err := api.HeadBucket(baseParams, bck, false)
	tassert.CheckFatal(t, err)

	t.Cleanup(func() {
		_, err := api.SetBucketProps(baseParams, bck, &cmn.BpropsToSet{
			Chunks: &cmn.ChunksConfToSet{
				ObjSizeLimit: apc.Ptr(p.Chunks.ObjSizeLimit),
				ChunkSize:    apc.Ptr(p.Chunks.ChunkSize),
			},
		})
		tassert.CheckError(t, err)
	})

	// Create small objects
	tlog.Logfln("Creating %d small objects (size=%s)...", numSmall, cos.ToSizeIEC(int64(smallSize), 0))
	mSmall := ioContext{
		t:             t,
		bck:           bck,
		num:           numSmall,
		fileSizeRange: [2]uint64{uint64(smallSize), uint64(smallSize + cos.KiB - 1)},
		prefix:        "small-",
		chunksConf:    &ioCtxChunksConf{multipart: false},
	}
	mSmall.init(true /*cleanup*/)
	mSmall.puts()

	// Create large objects
	tlog.Logfln("Creating %d large objects (size=%s)...", numLarge, cos.ToSizeIEC(int64(largeSize), 0))
	mLarge := ioContext{
		t:             t,
		bck:           bck,
		num:           numLarge,
		fileSizeRange: [2]uint64{uint64(largeSize), uint64(largeSize + cos.KiB - 1)},
		prefix:        "large-",
		chunksConf:    &ioCtxChunksConf{multipart: false},
	}
	mLarge.init(true /*cleanup*/)
	mLarge.puts()

	// First rechunk with xaction approach
	tlog.Logfln("Starting first rechunk (limit=%s, chunkSize=%s)...",
		cos.ToSizeIEC(int64(sizeLimit), 0), cos.ToSizeIEC(int64(chunkSize), 0))
	xid1, err := startRechunk(t, baseParams, bck, int64(sizeLimit), int64(chunkSize), "xaction")
	tassert.CheckFatal(t, err)
	tlog.Logfln("First rechunk xaction started with ID: %s", xid1)

	// Wait for first rechunk to complete
	tlog.Logln("Waiting for first rechunk to complete...")
	_, err = api.WaitForXactionIC(baseParams, &xact.ArgsMsg{ID: xid1, Kind: apc.ActRechunk, Bck: bck, Timeout: 5 * time.Minute})
	tassert.CheckFatal(t, err)
	tlog.Logln("First rechunk completed")

	// Validate: only large objects should be chunked
	tlog.Logln("Validating chunking state after first rechunk...")
	validateChunking(t, baseParams, bck, int64(sizeLimit), int64(chunkSize), &mSmall, &mLarge, "after first rechunk")

	// Capture first rechunk state
	lsmsg := &apc.LsoMsg{Props: apc.GetPropsChunked}
	lst1, err := api.ListObjects(baseParams, bck, lsmsg, api.ListArgs{})
	tassert.CheckFatal(t, err)

	var chunkedCount1 int
	for _, entry := range lst1.Entries {
		if (entry.Flags & apc.EntryIsChunked) != 0 {
			chunkedCount1++
		}
	}
	tlog.Logfln("After first rechunk: %d/%d objects are chunked", chunkedCount1, len(lst1.Entries))

	// Second rechunk (idempotent - same config, xaction approach)
	tlog.Logln("Starting second rechunk xaction (idempotent)...")
	xid2, err := startRechunk(t, baseParams, bck, int64(sizeLimit), int64(chunkSize), "xaction")
	tassert.CheckFatal(t, err)
	tlog.Logfln("Second rechunk xaction started with ID: %s", xid2)

	// Wait for second rechunk to complete
	tlog.Logln("Waiting for second rechunk to complete...")
	_, err = api.WaitForXactionIC(baseParams, &xact.ArgsMsg{ID: xid2, Kind: apc.ActRechunk, Bck: bck, Timeout: 5 * time.Minute})
	tassert.CheckFatal(t, err)
	tlog.Logln("Second rechunk completed")

	// Validate: chunking state should be identical
	tlog.Logln("Validating chunking state after second rechunk (should be identical)...")
	validateChunking(t, baseParams, bck, int64(sizeLimit), int64(chunkSize), &mSmall, &mLarge, "after second rechunk")

	// Capture second rechunk state
	lst2, err := api.ListObjects(baseParams, bck, lsmsg, api.ListArgs{})
	tassert.CheckFatal(t, err)

	var chunkedCount2 int
	for _, entry := range lst2.Entries {
		if (entry.Flags & apc.EntryIsChunked) != 0 {
			chunkedCount2++
		}
	}
	tlog.Logfln("After second rechunk: %d/%d objects are chunked", chunkedCount2, len(lst2.Entries))

	// Compare states
	tassert.Fatalf(t, chunkedCount1 == chunkedCount2,
		"chunked count mismatch: first=%d, second=%d", chunkedCount1, chunkedCount2)
	tassert.Fatalf(t, len(lst1.Entries) == len(lst2.Entries),
		"object count mismatch: first=%d, second=%d", len(lst1.Entries), len(lst2.Entries))

	// Verify all objects are still accessible
	tlog.Logln("Validating object data integrity...")
	mSmall.gets(nil, true /*withValidation*/)
	mLarge.gets(nil, true /*withValidation*/)

	tlog.Logfln("SUCCESS: Rechunk is idempotent - %d objects chunked in both runs", chunkedCount1)
}

// validateChunking validates chunking state via both list API and disk inspection
func validateChunking(t *testing.T, baseParams api.BaseParams, bck cmn.Bck, sizeLimit, chunkSize int64,
	mSmall, mLarge *ioContext, stage string) {
	t.Helper()

	tlog.Logfln("Validating chunking state (%s)...", stage)

	// List objects with chunked property
	lsmsg := &apc.LsoMsg{Props: apc.GetPropsChunked}
	lst, err := api.ListObjects(baseParams, bck, lsmsg, api.ListArgs{})
	tassert.CheckFatal(t, err)

	expectedTotal := mSmall.num + mLarge.num
	tassert.Fatalf(t, len(lst.Entries) == expectedTotal,
		"expected %d total objects, got %d", expectedTotal, len(lst.Entries))

	// Validate chunking via list API
	var (
		smallChunked, largeChunked int
		smallTotal, largeTotal     int
	)

	for _, entry := range lst.Entries {
		isChunked := (entry.Flags & apc.EntryIsChunked) != 0
		isSmall := strings.HasPrefix(entry.Name, "small-")
		isLarge := strings.HasPrefix(entry.Name, "large-")

		if isSmall {
			smallTotal++
			if isChunked {
				smallChunked++
			}
		} else if isLarge {
			largeTotal++
			if isChunked {
				largeChunked++
			}
		}
	}

	tassert.Fatalf(t, smallTotal == mSmall.num, "expected %d small objects, found %d", mSmall.num, smallTotal)
	tassert.Fatalf(t, largeTotal == mLarge.num, "expected %d large objects, found %d", mLarge.num, largeTotal)

	// Determine expected chunking based on size limit
	// Note: sizeLimit = 0 means no chunking (infinite limit)
	smallShouldChunk := sizeLimit > 0 && mSmall.fileSizeRange[0] > uint64(sizeLimit)
	largeShouldChunk := sizeLimit > 0 && mLarge.fileSizeRange[0] > uint64(sizeLimit)

	tlog.Logfln("Small objects: %d/%d chunked (expected: %v)", smallChunked, smallTotal, smallShouldChunk)
	tlog.Logfln("Large objects: %d/%d chunked (expected: %v)", largeChunked, largeTotal, largeShouldChunk)

	if smallShouldChunk {
		tassert.Fatalf(t, smallChunked == smallTotal, "expected all %d small objects to be chunked, but only %d are", smallTotal, smallChunked)
	} else {
		tassert.Fatalf(t, smallChunked == 0, "expected 0 small objects to be chunked, but %d are", smallChunked)
	}

	if largeShouldChunk {
		tassert.Fatalf(t, largeChunked == largeTotal, "expected all %d large objects to be chunked, but only %d are", largeTotal, largeChunked)
	} else {
		tassert.Fatalf(t, largeChunked == 0, "expected 0 large objects to be chunked, but %d are", largeChunked)
	}

	// Validate chunking via disk inspection
	validateChunksOnDisk(t, mSmall, smallShouldChunk, chunkSize)
	validateChunksOnDisk(t, mLarge, largeShouldChunk, chunkSize)
}

// validateChunksOnDisk validates chunk files on disk
func validateChunksOnDisk(t *testing.T, m *ioContext, shouldBeChunked bool, chunkSize int64) {
	t.Helper()

	for _, objName := range m.objNames {
		chunks := m.findObjChunksOnDisk(m.bck, objName)

		if !shouldBeChunked {
			// Object should be monolithic (no chunk files)
			tassert.Fatalf(t, len(chunks) == 0,
				"object %s should be monolithic but has %d chunk files", objName, len(chunks))
		} else {
			// Object should be chunked
			tassert.Fatalf(t, len(chunks) > 0,
				"object %s should be chunked but has no chunk files", objName)

			// Validate each chunk file size
			for _, chunkPath := range chunks {
				fi, err := os.Stat(chunkPath)
				tassert.CheckFatal(t, err)

				// Each chunk (except possibly the last) should be <= chunkSize
				tassert.Fatalf(t, fi.Size() <= chunkSize,
					"chunk file %s has size %s, expected <= %s",
					chunkPath, cos.ToSizeIEC(fi.Size(), 0), cos.ToSizeIEC(chunkSize, 0))
			}
		}
	}
}
