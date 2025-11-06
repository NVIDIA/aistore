// Package integration_test.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	"net/http"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact"
)

// TestBlobDownload tests the api.BlobDownload API for multiple objects.
// It validates:
// 1. Objects are downloaded with specified chunk size
// 2. All objects are cached after blob download
// 3. Exact number of chunks matches expectation based on objSize/chunkSize
// 4. Objects can be retrieved successfully
func TestBlobDownload(t *testing.T) {
	const (
		objSize    = 64 * cos.MiB
		chunkSize  = 16 * cos.MiB
		numObjs    = 5
		numWorkers = 4
	)
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		prefix     = "blob-download/" + trand.String(5)
	)

	// Setup ioContext
	m := ioContext{
		t:             t,
		bck:           cliBck,
		num:           numObjs,
		fileSize:      objSize,
		fixedSize:     true,
		prefix:        prefix,
		getErrIsFatal: true,
	}

	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: m.bck})

	m.init(true /*cleanup*/)
	initMountpaths(t, proxyURL)

	// Provision objects to remote bucket and evict
	tlog.Logfln("Provisioning %d objects of %s each to remote bucket", numObjs, cos.ToSizeIEC(objSize, 0))
	m.remotePuts(true /*evict*/)

	// Calculate expected number of chunks per object
	expectedChunks := int((objSize + chunkSize - 1) / chunkSize) // ceiling division
	tlog.Logfln("Expected chunks per object: %d (objSize=%s, chunkSize=%s)", expectedChunks, cos.ToSizeIEC(objSize, 0), cos.ToSizeIEC(chunkSize, 0))

	// Perform blob download for each object
	tlog.Logfln("Starting blob download for %d objects with chunk size %s", numObjs, cos.ToSizeIEC(chunkSize, 0))
	blobMsg := &apc.BlobMsg{
		ChunkSize:  chunkSize,
		FullSize:   objSize,
		NumWorkers: numWorkers,
		LatestVer:  false,
	}

	xids := make([]string, numObjs)
	for i, objName := range m.objNames {
		xid, err := api.BlobDownload(baseParams, m.bck, objName, blobMsg)
		tassert.CheckFatal(t, err)
		xids[i] = xid
	}

	// Wait for all blob download xactions to complete
	// Note: blob download is a single-target xaction that doesn't report to IC,
	// so we query the target's registry directly instead of IC
	tlog.Logfln("Waiting for blob download xactions to complete")
	xactFinished := func(snaps xact.MultiSnap) (bool, bool) {
		tid, _, err := snaps.RunningTarget("")
		if err != nil {
			return false, false
		}
		finished := tid == "" // not running = finished
		return finished, false
	}
	for _, xid := range xids {
		args := xact.ArgsMsg{ID: xid, Kind: apc.ActBlobDl, Timeout: tools.EvictPrefetchTimeout}
		err := api.WaitForXactionNode(baseParams, &args, xactFinished)
		tassert.CheckFatal(t, err)
	}

	// Verify all objects are cached
	tlog.Logfln("Verifying all objects are cached after blob download")
	cachedList, err := api.ListObjects(baseParams, m.bck, &apc.LsoMsg{Prefix: prefix, Props: apc.GetPropsCached}, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(cachedList.Entries) == numObjs, "expected %d cached objects, got %d", numObjs, len(cachedList.Entries))

	// Verify all objects are chunked with exact expected number of chunks
	tlog.Logfln("Verifying objects are chunked with exactly %d chunks each", expectedChunks)
	for _, objName := range m.objNames {
		chunks := m.findObjChunksOnDisk(m.bck, objName)
		tassert.Fatalf(t, len(chunks)+1 == expectedChunks,
			"object %s: expected exactly %d chunk files, found %d",
			objName, expectedChunks, len(chunks)+1)
	}
	tlog.Logfln("All objects have correct number of chunks (%d)", expectedChunks)

	// Verify all objects can be retrieved successfully
	tlog.Logfln("Verifying all objects are GETable")
	m.gets(nil, true)

	tlog.Logfln("Blob download test completed successfully")
}

// TestBlobDownloadAbort tests aborting a blob download and verifies cleanup.
// It validates:
// 1. Blob download can be aborted mid-download
// 2. No chunks remain on disk after abort
func TestBlobDownloadAbort(t *testing.T) {
	const (
		objSize    = 512 * cos.MiB // Large object to ensure we can abort mid-download
		chunkSize  = 16 * cos.MiB
		numWorkers = 2
	)
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		objName    = "blob-abort-test-" + trand.String(5)
		bck        = cliBck
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: bck})

	initMountpaths(t, proxyURL)

	// Provision a large object to remote bucket
	tlog.Logfln("Provisioning large object %s (%s)", objName, cos.ToSizeIEC(objSize, 0))
	r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cos.ChecksumNone})
	tassert.CheckFatal(t, err)
	_, err = api.PutObject(&api.PutArgs{
		BaseParams: baseParams,
		Bck:        bck,
		ObjName:    objName,
		Reader:     r,
		Size:       uint64(objSize),
	})
	tassert.CheckFatal(t, err)

	// Evict the object
	tlog.Logfln("Evicting object %s", objName)
	err = api.EvictObject(baseParams, bck, objName)
	tassert.CheckFatal(t, err)

	// Start blob download
	tlog.Logfln("Starting blob download for %s", objName)
	blobMsg := &apc.BlobMsg{
		ChunkSize:  chunkSize,
		FullSize:   objSize,
		NumWorkers: numWorkers,
		LatestVer:  false,
	}
	xid, err := api.BlobDownload(baseParams, bck, objName, blobMsg)
	tassert.CheckFatal(t, err)
	tlog.Logfln("Blob download started with xid=%s", xid)

	// Give it a moment to start downloading
	time.Sleep(500 * time.Millisecond)

	// Abort the xaction
	tlog.Logfln("Aborting blob download xid=%s", xid)
	args := xact.ArgsMsg{ID: xid, Kind: apc.ActBlobDl, Timeout: tools.RebalanceTimeout}
	err = api.AbortXaction(baseParams, &args)
	tassert.CheckFatal(t, err)

	// Wait for the xaction to finish aborting
	tlog.Logfln("Waiting for blob download to finish aborting")
	xactFinished := func(snaps xact.MultiSnap) (bool, bool) {
		tid, _, err := snaps.RunningTarget("")
		if err != nil {
			return false, false
		}
		finished := tid == "" // not running = finished
		return finished, false
	}
	err = api.WaitForXactionNode(baseParams, &args, xactFinished)
	tassert.CheckFatal(t, err)
	tlog.Logfln("Blob download aborted and finished")

	// Verify no chunks remain on disk
	m := &ioContext{t: t, bck: bck}
	tlog.Logfln("Verifying no chunks remain on disk after abort")
	err = tools.WaitForCondition(func() bool {
		return len(m.findObjChunksOnDisk(bck, objName)) == 0
	}, tools.WaitRetryOpts{MaxRetries: 10, Interval: 1 * time.Second})
	tassert.CheckFatal(t, err)

	chunks := m.findObjChunksOnDisk(bck, objName)
	tlog.Logfln("Found chunk files after abort", chunks)
	tassert.Fatalf(t, len(chunks) == 0, "expected 0 chunk files after abort, found %d", len(chunks))

	// Cleanup: delete the remote object
	tlog.Logfln("Cleaning up remote object")
	err = api.DeleteObject(baseParams, bck, objName)
	tassert.CheckFatal(t, err)

	tlog.Logfln("Blob download abort test completed successfully")
}

func TestPrefetchWithBlobThreshold(t *testing.T) {
	const (
		blobThresh   = 128 * cos.MiB
		smallObjSize = 32 * cos.MiB  // below blob threshold
		largeObjSize = 256 * cos.MiB // above blob threshold
		chunkSize    = 16 * cos.MiB
		numSmallObjs = 3
		numLargeObjs = 2
	)

	tests := []struct {
		name     string
		evict    bool
		longOnly bool
	}{
		{name: "prefetch-blob-with-evict", evict: true},
		{name: "prefetch-blob-no-evict", evict: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				proxyURL = tools.RandomProxyURL(t)
				prefix   = "prefetch-blob/" + tt.name + trand.String(5)
			)

			// Setup small objects context
			mSmall := ioContext{
				t:             t,
				bck:           cliBck,
				num:           numSmallObjs,
				fileSizeRange: [2]uint64{smallObjSize, smallObjSize + cos.MiB},
				prefix:        prefix + "/small",
				getErrIsFatal: true,
			}

			// Setup large objects context
			mLarge := ioContext{
				t:             t,
				bck:           cliBck,
				num:           numLargeObjs,
				fileSizeRange: [2]uint64{largeObjSize, largeObjSize + chunkSize},
				prefix:        prefix + "/large",
				getErrIsFatal: true,
			}

			tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: mSmall.bck, Long: tt.longOnly})

			mSmall.init(true /*cleanup*/)
			mLarge.init(true /*cleanup*/)
			initMountpaths(t, proxyURL)
			baseParams := tools.BaseAPIParams(proxyURL)

			// Provision small and large objects to remote backend
			tlog.Logfln("Provisioning %d small (%s) and %d large (%s) objects (evict=%v)...",
				numSmallObjs, cos.ToSizeIEC(smallObjSize, 0), numLargeObjs, cos.ToSizeIEC(largeObjSize, 0), tt.evict)
			mSmall.remotePuts(tt.evict)
			mLarge.remotePuts(tt.evict)

			// Prefetch with blob threshold
			tlog.Logfln("Prefetching with blob threshold=%s", cos.ToSizeIEC(blobThresh, 0))
			msg := &apc.PrefetchMsg{
				BlobThreshold: blobThresh,
			}
			msg.Template = prefix + "/*" // prefetch both small and large
			xid, err := api.Prefetch(baseParams, mSmall.bck, msg)
			tassert.CheckFatal(t, err)
			args := xact.ArgsMsg{ID: xid, Kind: apc.ActPrefetchObjects, Timeout: tools.EvictPrefetchTimeout}
			_, err = api.WaitForXactionIC(baseParams, &args)
			tassert.CheckFatal(t, err)

			// Verify all objects are GETable with correct content
			tlog.Logln("Verifying small objects are GETable...")
			mSmall.gets(nil, true) // TODO: validate content by checksum

			tlog.Logln("Verifying large objects are GETable...")
			mLarge.gets(nil, true) // TODO: validate content by checksum

			// When evicted and prefetched with blob download:
			// - Small objects (< threshold): regular prefetch, no chunking
			// - Large objects (>= threshold): blob download, chunked if size > chunk size
			if tt.evict {
				// Verify large objects are chunked (blob download chunked them)
				tlog.Logfln("Verifying large objects are chunked after blob download")
				lsLarge, err := api.ListObjects(baseParams, mLarge.bck, &apc.LsoMsg{Prefix: mLarge.prefix, Props: apc.GetPropsChunked}, api.ListArgs{})
				tassert.CheckFatal(t, err)
				tassert.Fatalf(t, len(lsLarge.Entries) == mLarge.num, "expected %d large objects, got %d", mLarge.num, len(lsLarge.Entries))

				// Large objects should be chunked (downloaded via blob download)
				for _, objName := range mLarge.objNames {
					chunks := mLarge.findObjChunksOnDisk(mLarge.bck, objName)
					tassert.Fatalf(t, len(chunks) > 0, "expected at least 1 chunk file for large object %s, found %d", objName, len(chunks))
				}

				// Small objects should NOT be chunked (regular prefetch)
				tlog.Logfln("Verifying small objects are NOT chunked (regular prefetch)")
				for _, objName := range mSmall.objNames {
					chunks := mSmall.findObjChunksOnDisk(mSmall.bck, objName)
					tassert.Fatalf(t, len(chunks) == 0, "expected 0 chunk files for small object %s (regular prefetch), found %d", objName, len(chunks))
				}
			} else {
				// Both large and small objects should not be chunked (warm GET)
				for _, objName := range mLarge.objNames {
					chunks := mLarge.findObjChunksOnDisk(mLarge.bck, objName)
					tassert.Fatalf(t, len(chunks) == 0, "expected 0 chunk files for large object %s (warm GET), found %d", objName, len(chunks))
				}
				for _, objName := range mSmall.objNames {
					chunks := mSmall.findObjChunksOnDisk(mSmall.bck, objName)
					tassert.Fatalf(t, len(chunks) == 0, "expected 0 chunk files for small object %s (warm GET), found %d", objName, len(chunks))
				}
			}
		})
	}
}

// TestBlobDownloadStreamGet tests blob download via cold GET with the apc.HdrBlobDownload header.
// It validates:
// 1. Cold GET with blob download returns correct bytes
// 2. Object is cached after blob download
// 3. Object is chunked on disk after blob download
// 4. Warm GET returns the same correct bytes
func TestBlobDownloadStreamGet(t *testing.T) {
	const objSize = 16 * cos.MiB
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cliBck
		objName    = t.Name() + "-" + trand.String(5)
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: bck})
	initMountpaths(t, proxyURL)

	reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cos.ChecksumNone})
	tassert.CheckFatal(t, err)
	tlog.Logfln("Provisioning object %s with %s of data", objName, cos.ToSizeIEC(objSize, 0))

	// Put object to remote bucket
	putArgs := &api.PutArgs{
		BaseParams: baseParams,
		Bck:        bck,
		ObjName:    objName,
		Reader:     reader,
		Size:       uint64(objSize),
	}
	_, err = api.PutObject(putArgs)
	tassert.CheckFatal(t, err)
	defer api.DeleteObject(baseParams, bck, objName)

	// Evict to ensure cold GET
	tlog.Logfln("Evicting object %s", objName)
	err = api.EvictObject(baseParams, bck, objName)
	tassert.CheckFatal(t, err)

	// Cold GET with blob download header
	tlog.Logfln("Performing cold GET with blob download header")
	coldGetBuf := &bytes.Buffer{}
	getArgs := &api.GetArgs{
		Writer: coldGetBuf,
		Header: http.Header{apc.HdrBlobDownload: []string{"true"}},
	}
	result, size, err := api.GetObjectReader(baseParams, bck, objName, getArgs)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, size == objSize, "expected size %d, got %d", objSize, size)

	readerDup, err := reader.Open()
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, tools.ReaderEqual(readerDup, result), "cold GET: data mismatch")

	// Verify object is NOW cached after cold GET
	tlog.Logfln("Verifying object is cached after cold GET")
	cachedList, err := api.ListObjects(baseParams, bck, &apc.LsoMsg{Prefix: objName, Props: apc.GetPropsCached}, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(cachedList.Entries) == 1, "object %s should be cached after cold GET, found %d entries", objName, len(cachedList.Entries))

	// Verify object is chunked after blob download
	tlog.Logfln("Verifying object is chunked after blob download")
	m := &ioContext{t: t, bck: bck}
	chunks := m.findObjChunksOnDisk(bck, objName)
	tassert.Fatalf(t, len(chunks) > 0, "expected at least 1 chunk file for object %s, found %d", objName, len(chunks))
	tlog.Logfln("Found %d chunk files for object %s", len(chunks), objName)

	// Warm GET (no blob download header needed - should read from cache)
	tlog.Logfln("Performing warm GET")
	result, size, err = api.GetObjectReader(baseParams, bck, objName, nil /*getArgs*/)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, size == objSize, "warm GET: expected size %d, got %d", objSize, size)

	readerDup, err = reader.Open()
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, tools.ReaderEqual(readerDup, result), "warm GET: data mismatch")
}
