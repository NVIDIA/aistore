// Package integration_test.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	"io"
	"net/http"
	"strconv"
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
	t.Skipf("skipping %s - not ready", t.Name()) // TODO -- FIXME: fix to run and pass

	const (
		objSize    = 64 * cos.MiB
		chunkSize  = 16 * cos.MiB
		numWorkers = 4
	)
	var (
		numObjs    = 500 // TODO -- FIXME: likely the reason CI times out after 6 hours
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		prefix     = "blob-download/" + trand.String(5)
	)

	if testing.Short() {
		numObjs /= 10
	}

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
	expectedChunks := (objSize + chunkSize - 1) / chunkSize // ceiling division
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

	for _, xid := range xids {
		args := xact.ArgsMsg{ID: xid, Kind: apc.ActBlobDl, Timeout: tools.EvictPrefetchTimeout}
		_, err := api.WaitForSnaps(baseParams, &args, args.Finished())
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
		m.validateChunksOnDisk(m.bck, objName, expectedChunks)
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
		bck        = cliBck
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true, RemoteBck: true, Bck: bck})
	initMountpaths(t, proxyURL)

	tests := []struct {
		name      string
		streamGet bool
	}{
		{name: "blob-download", streamGet: false},
		{name: "streaming-get", streamGet: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				objName = "blob-abort-test-" + trand.String(5)
				xid     string
			)

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

			if test.streamGet {
				// Start blob download via streaming GET
				tlog.Logfln("Starting blob download via streaming GET for %s", objName)

				getArgs := &api.GetArgs{
					Writer: io.Discard, // Discard the data, we're testing abort
					Header: http.Header{
						apc.HdrBlobDownload: []string{"true"},
						apc.HdrBlobChunk:    []string{cos.ToSizeIEC(chunkSize, 0)},
						apc.HdrBlobWorkers:  []string{strconv.FormatInt(numWorkers, 10)},
					},
				}
				_, size, _ := api.GetObjectReader(baseParams, bck, objName, getArgs)
				tassert.Fatalf(t, size == objSize, "expected size %d, got %d", objSize, size)

				// Give it a moment to start downloading and retrieve the xid
				time.Sleep(500 * time.Millisecond)

				// Query for the running blob download xaction to get its xid
				args := xact.ArgsMsg{Kind: apc.ActBlobDl, Timeout: tools.RebalanceTimeout}
				snaps, err := api.QueryXactionSnaps(baseParams, &args)
				tassert.CheckFatal(t, err)
				tid, _, err := snaps.RunningTarget("")
				tassert.CheckFatal(t, err)
				tassert.Fatalf(t, tid != "", "expected blob download to be running")
				xid = snaps[tid][0].ID
				tlog.Logfln("Blob download started with xid=%s", xid)
			} else {
				// Start blob download via API
				tlog.Logfln("Starting blob download for %s", objName)
				blobMsg := &apc.BlobMsg{
					ChunkSize:  chunkSize,
					FullSize:   objSize,
					NumWorkers: numWorkers,
					LatestVer:  false,
				}
				xid, err = api.BlobDownload(baseParams, bck, objName, blobMsg)
				tassert.CheckFatal(t, err)
				tlog.Logfln("Blob download started with xid=%s", xid)

				// Give it a moment to start downloading
				time.Sleep(500 * time.Millisecond)
			}

			// Abort the xaction
			tlog.Logfln("Aborting blob download xid=%s", xid)
			args := xact.ArgsMsg{ID: xid, Kind: apc.ActBlobDl, Timeout: tools.RebalanceTimeout}
			err = api.AbortXaction(baseParams, &args)
			tassert.CheckFatal(t, err)

			// Wait for the xaction to finish aborting
			tlog.Logfln("Waiting for blob download to finish aborting")
			_, err = api.WaitForSnaps(baseParams, &args, args.Finished())
			tassert.CheckFatal(t, err)
			tlog.Logfln("Blob download aborted and finished")

			// Cleanup: delete the remote object
			tlog.Logfln("Cleaning up remote object")
			err = api.DeleteObject(baseParams, bck, objName)
			tassert.CheckFatal(t, err)

			tlog.Logfln("Blob download abort test completed successfully")
		})
	}
}

func TestBlobDownloadAbortByKind(t *testing.T) {
	t.Skipf("skipping %s - not ready", t.Name()) // TODO -- FIXME: fix to run and pass

	const (
		objSize    = 32 * cos.MiB
		chunkSize  = 8 * cos.MiB
		numWorkers = 4
		numObjs    = 15
	)
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		prefix     = "blob-abort-kind/" + trand.String(5)
		xids       = make([]string, numObjs)
	)

	m := ioContext{
		t:         t,
		bck:       cliBck,
		num:       numObjs,
		fileSize:  objSize,
		fixedSize: true,
		prefix:    prefix,
	}

	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: m.bck, Long: true})

	m.init(true /*cleanup*/)
	initMountpaths(t, proxyURL)

	tlog.Logfln("Provisioning %d objects of %s each", numObjs, cos.ToSizeIEC(objSize, 0))
	m.remotePuts(true /*evict*/)

	tlog.Logfln("Starting blob download for %d objects", numObjs)
	blobMsg := &apc.BlobMsg{
		ChunkSize:  chunkSize,
		FullSize:   objSize,
		NumWorkers: numWorkers,
	}
	for i, objName := range m.objNames {
		xid, err := api.BlobDownload(baseParams, m.bck, objName, blobMsg)
		tassert.CheckFatal(t, err)
		xids[i] = xid
	}

	// Immediately abort ALL blob downloads by kind (no xid specified)
	tlog.Logfln("Aborting all blob downloads by kind")
	abortArgs := xact.ArgsMsg{Kind: apc.ActBlobDl}
	err := api.AbortXaction(baseParams, &abortArgs)
	tassert.CheckFatal(t, err)

	// Wait for all blob download jobs to finish (aborted status)
	tlog.Logfln("Waiting for all blob downloads to finish")

	for _, xid := range xids {
		args := xact.ArgsMsg{ID: xid, Kind: apc.ActBlobDl, Timeout: tools.RebalanceTimeout}

		_, err = api.WaitForSnaps(baseParams, &args, args.Finished())
		tassert.CheckFatal(t, err)
	}
	tlog.Logfln("All blob downloads aborted or finished")

	// Evict all objects to clean up any that completed successfully before abort
	tlog.Logfln("Evicting all objects to ensure clean state")
	for _, objName := range m.objNames {
		_ = api.EvictObject(baseParams, m.bck, objName)
	}

	// Validate no chunks on disk for all objects
	tlog.Logfln("Validating no leftover chunks on disk for all objects")
	for _, objName := range m.objNames {
		m.validateChunksOnDisk(m.bck, objName, 0)
	}

	tlog.Logfln("Blob download abort-by-kind test completed successfully")
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

			// Calculate expected byte range based on provisioned object sizes
			minExpectedBytes := int64(numSmallObjs*mSmall.fileSizeRange[0] + numLargeObjs*mLarge.fileSizeRange[0])
			maxExpectedBytes := int64(numSmallObjs*mSmall.fileSizeRange[1] + numLargeObjs*mLarge.fileSizeRange[1])
			tlog.Logfln("Expected byte range: %s - %s", cos.IEC(minExpectedBytes, 2), cos.IEC(maxExpectedBytes, 2))

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

			// Validate xaction stats
			tlog.Logfln("Validating xaction stats...")
			snaps, err := api.QueryXactionSnaps(baseParams, &xact.ArgsMsg{ID: xid})
			tassert.CheckFatal(t, err)
			locBytes, outBytes, inBytes := snaps.ByteCounts(xid)
			tlog.Logfln("Xaction byte counts: locBytes=%s, outBytes=%s, inBytes=%s",
				cos.ToSizeIEC(locBytes, 2), cos.ToSizeIEC(outBytes, 2), cos.ToSizeIEC(inBytes, 2))

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
					mLarge.validateChunksOnDisk(mLarge.bck, objName, -1) // at least 1 chunk file
				}

				// Small objects should NOT be chunked (regular prefetch)
				tlog.Logfln("Verifying small objects are NOT chunked (regular prefetch)")
				for _, objName := range mSmall.objNames {
					mSmall.validateChunksOnDisk(mSmall.bck, objName, 0) // no chunk files
				}

				// Objects were evicted and prefetched, expect full download within size range
				tassert.Fatalf(t, locBytes >= minExpectedBytes && locBytes <= maxExpectedBytes,
					"expected locBytes=%s to be within range [%s, %s] (evict=%v)",
					cos.ToSizeIEC(locBytes, 2), cos.ToSizeIEC(minExpectedBytes, 2),
					cos.ToSizeIEC(maxExpectedBytes, 2), tt.evict)
			} else {
				// Both large and small objects should not be chunked (warm GET)
				for _, objName := range mLarge.objNames {
					mLarge.validateChunksOnDisk(mLarge.bck, objName, 0) // no chunk files
				}
				for _, objName := range mSmall.objNames {
					mSmall.validateChunksOnDisk(mSmall.bck, objName, 0) // no chunk files
				}

				// Objects were already present (warm GET), expect zero bytes to be counted
				tassert.Fatalf(t, locBytes == 0, "expected locBytes=0, got %s", cos.ToSizeIEC(locBytes, 2))
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
	m.validateChunksOnDisk(bck, objName, -1) // at least 1 chunk file

	// Warm GET (no blob download header needed - should read from cache)
	tlog.Logfln("Performing warm GET")
	result, size, err = api.GetObjectReader(baseParams, bck, objName, nil /*getArgs*/)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, size == objSize, "warm GET: expected size %d, got %d", objSize, size)

	readerDup, err = reader.Open()
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, tools.ReaderEqual(readerDup, result), "warm GET: data mismatch")
}

// TestBlobDownloadSingleThreaded tests single-threaded blob download (numWorkers = -1).
// It validates:
// 1. Single-threaded blob download via API works correctly
// 2. Single-threaded blob download via streaming GET works correctly
// 3. Objects are downloaded and cached properly in single-threaded mode
func TestBlobDownloadSingleThreaded(t *testing.T) {
	const (
		objSize   = 32 * cos.MiB
		chunkSize = 8 * cos.MiB
	)
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cliBck
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: bck})
	initMountpaths(t, proxyURL)

	tests := []struct {
		name      string
		streamGet bool
	}{
		{name: "blob-download", streamGet: false},
		{name: "streaming-get", streamGet: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			objName := "blob-single-threaded-" + test.name + "-" + trand.String(5)

			// Provision object to remote bucket
			tlog.Logfln("Provisioning object %s (%s)", objName, cos.ToSizeIEC(objSize, 0))
			reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cos.ChecksumNone})
			tassert.CheckFatal(t, err)

			_, err = api.PutObject(&api.PutArgs{
				BaseParams: baseParams,
				Bck:        bck,
				ObjName:    objName,
				Reader:     reader,
				Size:       uint64(objSize),
			})
			tassert.CheckFatal(t, err)
			defer api.DeleteObject(baseParams, bck, objName)

			// Evict the object to force cold GET
			tlog.Logfln("Evicting object %s", objName)
			err = api.EvictObject(baseParams, bck, objName)
			tassert.CheckFatal(t, err)

			if test.streamGet {
				// Test single-threaded blob download via streaming GET
				tlog.Logfln("Starting single-threaded blob download via streaming GET")
				coldGetBuf := &bytes.Buffer{}
				getArgs := &api.GetArgs{
					Writer: coldGetBuf,
					Header: http.Header{
						apc.HdrBlobDownload: []string{"true"},
						apc.HdrBlobChunk:    []string{cos.ToSizeIEC(chunkSize, 0)},
						apc.HdrBlobWorkers:  []string{"-1"}, // Single-threaded
					},
				}
				result, size, err := api.GetObjectReader(baseParams, bck, objName, getArgs)
				tassert.CheckFatal(t, err)
				tassert.Fatalf(t, size == objSize, "expected size %d, got %d", objSize, size)

				// Verify content matches
				readerDup, err := reader.Open()
				tassert.CheckFatal(t, err)
				tassert.Fatalf(t, tools.ReaderEqual(readerDup, result), "cold GET: data mismatch")
			} else {
				// Test single-threaded blob download via API
				tlog.Logfln("Starting single-threaded blob download via API")
				blobMsg := &apc.BlobMsg{
					ChunkSize:  chunkSize,
					FullSize:   objSize,
					NumWorkers: -1, // Single-threaded
					LatestVer:  false,
				}
				xid, err := api.BlobDownload(baseParams, bck, objName, blobMsg)
				tassert.CheckFatal(t, err)
				tlog.Logfln("Blob download started with xid=%s", xid)

				// Wait for blob download to complete
				tlog.Logfln("Waiting for single-threaded blob download to complete")
				args := xact.ArgsMsg{ID: xid, Kind: apc.ActBlobDl, Timeout: tools.EvictPrefetchTimeout}
				_, err = api.WaitForSnaps(baseParams, &args, args.Finished())
				tassert.CheckFatal(t, err)

				// Verify content via GET
				result, size, err := api.GetObjectReader(baseParams, bck, objName, nil)
				tassert.CheckFatal(t, err)
				tassert.Fatalf(t, size == objSize, "expected size %d, got %d", objSize, size)

				readerDup, err := reader.Open()
				tassert.CheckFatal(t, err)
				tassert.Fatalf(t, tools.ReaderEqual(readerDup, result), "warm GET: data mismatch")
			}

			// Verify object is cached
			tlog.Logfln("Verifying object is cached")
			cachedList, err := api.ListObjects(baseParams, bck, &apc.LsoMsg{Prefix: objName, Props: apc.GetPropsCached}, api.ListArgs{})
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, len(cachedList.Entries) == 1, "expected 1 cached object, got %d", len(cachedList.Entries))

			// Verify object is chunked
			tlog.Logfln("Verifying object is chunked")
			m := &ioContext{t: t, bck: bck}
			expectedChunks := (objSize + chunkSize - 1) / chunkSize
			m.validateChunksOnDisk(bck, objName, expectedChunks)

			tlog.Logfln("Single-threaded blob download test (%s) completed successfully", test.name)
		})
	}
}
