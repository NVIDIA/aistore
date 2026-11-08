// Package integration_test.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact"
)

func blobdlFinished(snaps xact.MultiSnap) (bool, bool, error) {
	for _, targetSnaps := range snaps {
		for _, snap := range targetSnaps {
			if snap.IsFinished() {
				return true, false, nil
			}
		}
	}
	return false, false, nil
}

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
		numWorkers = 4
	)
	var (
		numObjs    = 10
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
	// Note: blob download's direct-target start path does not register an IC listener,
	// so the generic wait queries the target's registry directly instead.
	tlog.Logfln("Waiting for blob download xactions to complete")

	for _, xid := range xids {
		args := xact.ArgsMsg{ID: xid, Kind: apc.ActBlobDl, Timeout: tools.EvictPrefetchTimeout}
		err := api.WaitForXaction(baseParams, &args)
		tassert.CheckFatal(t, err)
	}

	// Query each target without including snapshots from its peers.
	args := xact.ArgsMsg{Kind: apc.ActBlobDl, Bck: m.bck}
	snaps, err := api.QueryXactionSnaps(baseParams, &args)
	tassert.CheckFatal(t, err)
	for tid := range snaps {
		args.DaemonID = tid
		selected, err := api.QueryXactionSnaps(baseParams, &args)
		tassert.CheckFatal(t, err)
		_, found := selected[tid]
		tassert.Errorf(t, len(selected) == 1 && found,
			"expected only %s's blob downloads, got %v", tid, selected)
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
	// Explicit blob GET must fall through and serve an object that is already warm.
	m.gets(&api.GetArgs{Header: http.Header{
		apc.HdrBlobDownload: []string{"true"},
	}}, true)

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
				body    io.ReadCloser
				size    int64
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
					Header: http.Header{
						apc.HdrBlobDownload: []string{"true"},
						apc.HdrBlobChunk:    []string{cos.ToSizeIEC(chunkSize, 0)},
						apc.HdrBlobWorkers:  []string{strconv.FormatInt(numWorkers, 10)},
					},
				}
				body, size, err = api.GetObjectReader(baseParams, bck, objName, getArgs)
				tassert.CheckFatal(t, err)
				tassert.Fatalf(t, size == objSize, "expected size %d, got %d", objSize, size)

				// Wait for the streaming blob download and retrieve its xid.
				args := xact.ArgsMsg{Kind: apc.ActBlobDl, OnlyRunning: true, Timeout: tools.RebalanceTimeout}
				snaps, err := api.WaitForSnaps(baseParams, &args, args.Started())
				tassert.CheckFatal(t, err)
				_, snap, err := snaps.RunningTarget("")
				tassert.CheckFatal(t, err)
				tassert.Fatalf(t, snap != nil, "expected blob download to be running")
				xid = snap.ID
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
				args := xact.ArgsMsg{ID: xid, Kind: apc.ActBlobDl, OnlyRunning: true, Timeout: tools.RebalanceTimeout}
				_, err = api.WaitForSnaps(baseParams, &args, args.Started())
				tassert.CheckFatal(t, err)
			}
			lock, err := api.CheckObjectLock(baseParams, bck, objName)
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, lock == apc.LockWrite, "expected write lock, got %d", lock)

			// Abort the xaction
			tlog.Logfln("Aborting blob download xid=%s", xid)
			args := xact.ArgsMsg{ID: xid, Kind: apc.ActBlobDl, Timeout: tools.RebalanceTimeout}
			err = api.AbortXaction(baseParams, &args)
			tassert.CheckFatal(t, err)
			if body != nil {
				cos.Close(body) // unblock the streaming handler after server-side abort
			}

			// Wait for the xaction to finish aborting
			tlog.Logfln("Waiting for blob download to finish aborting")
			_, err = api.WaitForSnaps(baseParams, &args, blobdlFinished)
			tassert.CheckFatal(t, err)
			tlog.Logfln("Blob download aborted and finished")
			lock, err = api.CheckObjectLock(baseParams, bck, objName)
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, lock == apc.LockNone, "expected released lock, got %d", lock)

			// Cleanup: delete the remote object
			tlog.Logfln("Cleaning up remote object")
			err = api.DeleteObject(baseParams, bck, objName)
			tassert.CheckFatal(t, err)

			tlog.Logfln("Blob download abort test completed successfully")
		})
	}
}

func TestBlobDownloadRuntimeError(t *testing.T) {
	const (
		objSize   = 4 * cos.MiB
		chunkSize = cos.MiB
	)
	m := ioContext{
		t: t, bck: cliBck, num: 1, prefix: t.Name() + "-", fileSize: objSize, fixedSize: true,
	}
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: m.bck})
	m.init(true /*cleanup*/)
	initMountpaths(t, m.proxyURL)
	m.remotePuts(true /*evict*/)

	bp := tools.BaseAPIParams(m.proxyURL)
	snaps, err := api.GetSnaps(bp, &xact.ArgsMsg{Kind: apc.ActBlobDl})
	tassert.CheckFatal(t, err)
	before := cos.NewStrSet(snaps.GetUUIDs()...)
	var buf bytes.Buffer

	_, err = api.GetObject(bp, m.bck, m.objNames[0], &api.GetArgs{
		Writer: &buf,
		Header: http.Header{
			apc.HdrBlobDownload:    []string{"true"},
			apc.HdrBlobChunk:       []string{cos.ToSizeIEC(chunkSize, 0)},
			apc.HdrBlobReadTimeout: []string{time.Nanosecond.String()}, // intentionally trigger runtime error with short timeout
		},
	})
	tassert.Fatalf(t, errors.Is(err, io.ErrUnexpectedEOF), "expected incomplete response, got %v", err)
	tassert.Fatalf(t, buf.Len() == 0, "unexpected error response appended to object stream: %q", buf.String())

	snaps, err = api.GetSnaps(bp, &xact.ArgsMsg{Kind: apc.ActBlobDl})
	tassert.CheckFatal(t, err)
	var found int
	for _, targetSnaps := range snaps {
		for _, snap := range targetSnaps {
			if before.Contains(snap.ID) {
				continue
			}
			found++
			tassert.Fatalf(t, snap.IsFinished(), "expected finished blob download, got %+v", snap)
			tassert.Fatalf(t, !snap.AbortedX, "expected ordinary runtime error, got %+v", snap)
			tassert.Fatalf(t, strings.Contains(snap.Err, context.DeadlineExceeded.Error()),
				"expected timeout error, got %+v", snap)
		}
	}
	tassert.Fatalf(t, found == 1, "expected one failed blob download, got %d", found)
	m.validateChunksOnDisk(m.bck, m.objNames[0], 0)
	tassert.Fatalf(t, !tools.CheckObjIsPresent(m.proxyURL, m.bck, m.objNames[0]), "partial object remains cached")
	lock, err := api.CheckObjectLock(bp, m.bck, m.objNames[0])
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, lock == apc.LockNone, "expected released lock, got %d", lock)
}

func TestBlobDownloadRenewFailureUnlocks(t *testing.T) {
	const objSize = 4 * cos.MiB
	m := ioContext{
		t: t, bck: cliBck, num: 1, prefix: t.Name() + "-", fileSize: objSize, fixedSize: true,
	}
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: m.bck})
	m.init(true /*cleanup*/)
	m.remotePuts(true /*evict*/)
	bp, objName := tools.BaseAPIParams(m.proxyURL), m.objNames[0]

	_, err := api.BlobDownload(bp, m.bck, objName, &apc.BlobMsg{FullSize: objSize + 1})
	tassert.Fatalf(t, err != nil && strings.Contains(err.Error(), "user-specified size "+strconv.FormatInt(objSize+1, 10)+
		", have "+strconv.FormatInt(objSize, 10)), "expected full-size mismatch, got %v", err)
	lock, err := api.CheckObjectLock(bp, m.bck, objName)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, lock == apc.LockNone, "expected released lock, got %d", lock)

	// The failed candidate did not retain ownership: a valid retry can run to completion.
	xid, err := api.BlobDownload(bp, m.bck, objName, &apc.BlobMsg{FullSize: objSize, ChunkSize: cos.MiB})
	tassert.CheckFatal(t, err)
	tassert.CheckFatal(t, api.WaitForXaction(bp, &xact.ArgsMsg{
		ID: xid, Kind: apc.ActBlobDl, Timeout: tools.EvictPrefetchTimeout,
	}))
	lock, err = api.CheckObjectLock(bp, m.bck, objName)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, lock == apc.LockNone, "expected released lock, got %d", lock)
}

func TestBlobDownloadParentCancellation(t *testing.T) {
	const (
		objSize    = 8 * cos.MiB
		chunkSize  = 2 * cos.MiB
		numWorkers = 2
	)
	var (
		bp  = tools.BaseAPIParams()
		bck = cliBck
		m   = ioContext{
			t: t, bck: bck, num: 1, fileSize: objSize, fixedSize: true,
			prefix: t.Name() + "/" + trand.String(5),
		}
	)
	tools.CheckSkip(t, &tools.SkipTestArgs{CloudBck: true, Bck: bck})
	m.init(true /*cleanup*/)
	initMountpaths(t, bp.URL)
	m.remotePuts(true /*evict*/)
	objName := m.objNames[0]

	t.Run("streaming-get", func(t *testing.T) {
		getArgs := &api.GetArgs{Header: http.Header{
			apc.HdrBlobDownload: []string{"true"},
			apc.HdrBlobChunk:    []string{strconv.FormatInt(chunkSize, 10)},
			apc.HdrBlobWorkers:  []string{strconv.Itoa(numWorkers)},
		}}
		body, size, err := api.GetObjectReader(bp, bck, objName, getArgs)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, size == objSize, "expected size %d, got %d", objSize, size)
		_, err = io.CopyN(io.Discard, body, 1) // cancel after the backend has started delivering data
		tassert.CheckFatal(t, err)
		xargs := &xact.ArgsMsg{Kind: apc.ActBlobDl, Timeout: tools.RebalanceTimeout}
		snaps, err := api.WaitForSnaps(bp, xargs, xargs.Started())
		tassert.CheckFatal(t, err)
		tid, snap, err := snaps.RunningTarget("")
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, snap != nil, "expected blob download to be running")
		lock, err := api.CheckObjectLock(bp, bck, objName)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, lock == apc.LockWrite, "expected write lock, got %d", lock)
		tassert.CheckFatal(t, body.Close())
		xargs.ID = snap.ID
		snaps, err = api.WaitForSnaps(bp, xargs, blobdlFinished)
		tassert.CheckFatal(t, err)
		errText := snaps[tid][0].Err
		// TODO: client disconnect may surface as a transport write error first instead of the expected context cancellation.
		tassert.Fatalf(t, errText != "", "expected client disconnect to fail the blob download")
		m.validateChunksOnDisk(bck, objName, 0)
		lock, err = api.CheckObjectLock(bp, bck, objName)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, lock == apc.LockNone, "expected released lock, got %d", lock)
	})

	t.Run("prefetch", func(t *testing.T) {
		msg := &apc.PrefetchMsg{
			ListRange:      apc.ListRange{ObjNames: []string{objName}},
			BlobThreshold:  cos.MiB,
			BlobChunkSize:  cmn.ChunkSizeMin,
			BlobNumWorkers: numWorkers,
		}
		prefetchXID, err := api.Prefetch(bp, bck, msg)
		tassert.CheckFatal(t, err)
		xargs := &xact.ArgsMsg{Kind: apc.ActBlobDl, Timeout: tools.RebalanceTimeout}
		snaps, err := api.WaitForSnaps(bp, xargs, xargs.Started())
		tassert.CheckFatal(t, err)
		tid, snap, err := snaps.RunningTarget("")
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, snap != nil, "expected blob download to be running")
		lock, err := api.CheckObjectLock(bp, bck, objName)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, lock == apc.LockWrite, "expected write lock, got %d", lock)

		err = api.AbortXaction(bp, &xact.ArgsMsg{ID: prefetchXID, Kind: apc.ActPrefetchObjects})
		tassert.CheckFatal(t, err)
		xargs.ID = snap.ID
		snaps, err = api.WaitForSnaps(bp, xargs, blobdlFinished)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, len(snaps[tid]) == 1, "expected blob download %s on %s", xargs.ID, tid)
		// parent's abort reaches the child two ways - whichever comes first:
		// - context cancellation (the child's context derives from the parent's)
		// - explicit abort (see xs/prefetch pebl)
		fin := snaps[tid][0]
		tassert.Fatalf(t, fin.AbortedX || strings.Contains(fin.Err, context.Canceled.Error()),
			"expected blob download aborted or context-canceled, got err=%q, abort-err=%q", fin.Err, fin.AbortErr)
		args := xact.ArgsMsg{ID: prefetchXID, Kind: apc.ActPrefetchObjects, Timeout: tools.RebalanceTimeout}
		tassert.CheckFatal(t, api.WaitForXaction(bp, &args))
		m.validateChunksOnDisk(bck, objName, 0)
		lock, err = api.CheckObjectLock(bp, bck, objName)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, lock == apc.LockNone, "expected released lock, got %d", lock)
	})
}

func TestBlobDownloadLockContention(t *testing.T) {
	var (
		numObjs       = 10
		regularGets   = 2
		blobGets      = 4
		objSize       = int64(16 * cos.MiB)
		baseChunkSize = int64(cos.MiB)
	)
	if !testing.Short() {
		objSize, baseChunkSize = 256*cos.MiB, 8*cos.MiB
		regularGets, blobGets = 4, 8
	}
	m := ioContext{
		t: t, bck: cliBck, num: numObjs, prefix: t.Name() + "/" + trand.String(5),
		fileSize: uint64(objSize), fixedSize: true,
	}
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: m.bck})
	m.init(true /*cleanup*/)
	initMountpaths(t, m.proxyURL)
	bp := tools.BaseAPIParams(m.proxyURL)
	sources := make([]readers.Reader, numObjs)
	for i := range numObjs {
		objName := m.prefix + "-" + strconv.Itoa(i)
		m.objNames = append(m.objNames, objName)
		sources[i], _ = m.update(objName, cos.ChecksumNone)
	}
	for _, objName := range m.objNames {
		tassert.CheckFatal(t, api.EvictObject(bp, m.bck, objName))
	}

	// Start one blob-download leader per object and leave every response unread.
	bodies := make([]io.ReadCloser, numObjs)
	chunkSizes := make([]int64, numObjs)
	for i, objName := range m.objNames {
		chunkSize := int64(1<<uint(i%4)) * baseChunkSize
		body, size, err := api.GetObjectReader(bp, m.bck, objName, &api.GetArgs{Header: http.Header{
			apc.HdrBlobDownload: []string{"true"},
			apc.HdrBlobChunk:    []string{strconv.FormatInt(chunkSize, 10)},
			apc.HdrBlobWorkers:  []string{"2"},
		}})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, size == objSize, "%s: expected size %d, got %d", objName, objSize, size)
		bodies[i], chunkSizes[i] = body, chunkSize
	}

	// Confirm that all leaders hold their object locks concurrently.
	for _, objName := range m.objNames {
		lock, err := api.CheckObjectLock(bp, m.bck, objName)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, lock == apc.LockWrite, "%s: expected write lock, got %d", objName, lock)
	}

	// Mix fail-fast blob GETs and regular GETs that wait for each leader.
	var (
		blobErrCh       = make(chan error, numObjs*blobGets)
		backgroundErrCh = make(chan error, numObjs*blobGets)
		regularErrCh    = make(chan error, numObjs*regularGets)
	)
	for i := range m.objNames {
		for j := range blobGets {
			args := &api.GetArgs{Header: http.Header{
				apc.HdrBlobDownload: []string{"true"},
				apc.HdrBlobChunk:    []string{strconv.FormatInt(int64(1<<uint(j%5))*cos.MiB, 10)},
				apc.HdrBlobWorkers:  []string{"2"},
			}}
			go func() { blobErrCh <- m.get(bp, i, 0, args, true /*validate*/) }()
		}
		for range regularGets {
			go func() { regularErrCh <- m.get(bp, i, 0, nil, true /*validate*/) }()
		}
	}
	// Stress the same locks with background writers randomly distributed over the limited name set.
	for range numObjs * blobGets {
		objName := m.objNames[rand.IntN(numObjs)]
		go func() {
			_, err := api.BlobDownload(bp, m.bck, objName, &apc.BlobMsg{})
			backgroundErrCh <- err
		}()
	}
	for range numObjs * blobGets {
		for _, errCh := range []<-chan error{blobErrCh, backgroundErrCh} {
			err := <-errCh
			herr := cmn.AsErrHTTP(err)
			tassert.Fatalf(t, herr != nil && herr.TypeCode == "ErrBusy", "expected ErrBusy, got %v", err)
		}
	}
	tassert.Fatalf(t, len(regularErrCh) == 0,
		"%d regular GETs completed before the leaders released their locks", len(regularErrCh))

	// Drain and validate every leader, releasing its lock for all waiting GETs.
	for i, objName := range m.objNames {
		expected, err := sources[i].Open()
		tassert.CheckFatal(t, err)
		equal := tools.ReaderEqual(expected, bodies[i])
		cos.Close(expected)
		cos.Close(bodies[i])
		tassert.Fatalf(t, equal, "%s: blob GET returned invalid content", objName)
	}
	for range numObjs * regularGets {
		tassert.CheckFatal(t, <-regularErrCh)
	}
	for i, objName := range m.objNames {
		m.validateChunksOnDisk(m.bck, objName, int(objSize/chunkSizes[i]))
	}
	for _, objName := range m.objNames {
		lock, err := api.CheckObjectLock(bp, m.bck, objName)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, lock == apc.LockNone, "%s: expected released lock, got %d", objName, lock)
	}
}

// TestBlobDownloadRWStress follows TestRWStress: mixed operations randomly
// collide over a bounded set of object names.
func TestBlobDownloadRWStress(t *testing.T) {
	var (
		numObjs = 6
		numOps  = 96
		objSize = int64(4 * cos.MiB)
	)
	if !testing.Short() {
		numObjs, numOps, objSize = 10, 400, 32*cos.MiB
	}
	m := ioContext{
		t: t, bck: cliBck, num: numObjs, prefix: t.Name() + "/" + trand.String(5),
		fileSize: uint64(objSize), fixedSize: true,
	}
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: m.bck})
	m.init(true /*cleanup*/)
	initMountpaths(t, m.proxyURL)
	bp := tools.BaseAPIParams(m.proxyURL)
	m.remotePuts(true /*evict*/)

	// Mix readers, streaming and cache-only blob writers, and evictions.
	ops := [...]string{http.MethodGet, "blob-get", apc.ActBlobDl, apc.ActEvictObjects}
	results := make(chan opRes, numOps)
	wg := cos.NewLimitedWaitGroup(40, 0)
	for i := range numOps {
		idx, op := rand.IntN(numObjs), ops[i%len(ops)]
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			switch op {
			case http.MethodGet:
				err = m.get(bp, idx, 0, nil, true /*validate*/)
			case "blob-get":
				args := &api.GetArgs{Header: http.Header{
					apc.HdrBlobDownload: []string{"true"},
					apc.HdrBlobChunk:    []string{strconv.FormatInt(int64(1<<uint(i%4))*cos.MiB, 10)},
				}}
				err = m.get(bp, idx, 0, args, true /*validate*/)
			case apc.ActBlobDl:
				var xid string
				xid, err = api.BlobDownload(bp, m.bck, m.objNames[idx], &apc.BlobMsg{ChunkSize: cos.MiB})
				if err == nil && xid != "" {
					args := xact.ArgsMsg{ID: xid, Kind: apc.ActBlobDl, Timeout: tools.RebalanceTimeout}
					err = api.WaitForXaction(bp, &args)
				}
			default:
				err = api.EvictObject(bp, m.bck, m.objNames[idx])
			}
			results <- opRes{op: op, err: err}
		}()
	}
	wg.Wait()
	close(results)
	var success, busy, benign int
	for result := range results {
		if result.err == nil {
			success++
			continue
		}
		if result.op == apc.ActEvictObjects && api.HTTPStatus(result.err) == http.StatusNotFound {
			benign++
			continue // another concurrent eviction already made it cold
		}
		herr := cmn.AsErrHTTP(result.err)
		if herr != nil && herr.TypeCode == "ErrBusy" {
			busy++
			continue
		}
		t.Fatalf("%s failed: %v", result.op, result.err)
	}
	tassert.Fatalf(t, success+busy+benign == numOps,
		"expected %d operations, got success=%d, busy=%d, benign=%d", numOps, success, busy, benign)
	tlog.Logfln("Completed %d operations: success=%d, busy=%d, benign=%d", numOps, success, busy, benign)

	for i := range numObjs {
		tassert.CheckFatal(t, m.get(bp, i, 0, nil, true /*validate*/))
	}
	for _, objName := range m.objNames {
		lock, err := api.CheckObjectLock(bp, m.bck, objName)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, lock == apc.LockNone, "%s: expected released lock, got %d", objName, lock)
	}
}

func TestBlobDownloadColdGetContention(t *testing.T) {
	var (
		regularGets = 4
		blobGets    = 12
		objSize     = int64(32 * cos.MiB)
	)
	if !testing.Short() {
		objSize = 256 * cos.MiB
		regularGets, blobGets = 8, 16
	}
	m := ioContext{
		t: t, bck: cliBck, num: 1, prefix: t.Name() + "/" + trand.String(5),
		fileSize: uint64(objSize), fixedSize: true,
	}
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: m.bck})
	m.init(true /*cleanup*/)
	initMountpaths(t, m.proxyURL)
	bp := tools.BaseAPIParams(m.proxyURL)
	objName := m.prefix + "-0"
	m.objNames = append(m.objNames, objName)
	source, _ := m.update(objName, cos.ChecksumNone)
	tassert.CheckFatal(t, api.EvictObject(bp, m.bck, objName))
	leaderErrCh := make(chan error, 1)
	go func() {
		body, size, err := api.GetObjectReader(bp, m.bck, objName, nil)
		if err == nil && size != objSize {
			err = errors.New("cold GET returned unexpected size")
		}
		if err == nil {
			expected, openErr := source.Open()
			if openErr != nil {
				err = openErr
			} else {
				if !tools.ReaderEqual(expected, body) {
					err = errors.New("cold GET returned invalid content")
				}
				cos.Close(expected)
			}
		}
		if body != nil {
			cos.Close(body)
		}
		leaderErrCh <- err
	}()

	deadline := time.Now().Add(tools.RebalanceStartTimeout)
	for {
		lock, err := api.CheckObjectLock(bp, m.bck, objName)
		tassert.CheckFatal(t, err)
		if lock == apc.LockWrite {
			break
		}
		tassert.Fatalf(t, time.Now().Before(deadline), "timed out waiting for cold GET to acquire the write lock")
		time.Sleep(cos.PollSleepShort)
	}

	blobErrCh := make(chan error, blobGets)
	regularErrCh := make(chan error, regularGets)
	for i := range blobGets {
		args := &api.GetArgs{Header: http.Header{
			apc.HdrBlobDownload: []string{"true"},
			apc.HdrBlobChunk:    []string{strconv.FormatInt(int64(1<<uint(i%5))*cos.MiB, 10)},
			apc.HdrBlobWorkers:  []string{"2"},
		}}
		go func() { blobErrCh <- m.get(bp, 0, 0, args, true /*validate*/) }()
	}
	for range regularGets {
		go func() { regularErrCh <- m.get(bp, 0, 0, nil, true /*validate*/) }()
	}

	for range blobGets {
		err := <-blobErrCh
		herr := cmn.AsErrHTTP(err)
		tassert.Fatalf(t, herr != nil && herr.TypeCode == "ErrBusy", "expected ErrBusy, got %v", err)
	}
	tassert.CheckFatal(t, <-leaderErrCh)
	for range regularGets {
		tassert.CheckFatal(t, <-regularErrCh)
	}
	m.validateChunksOnDisk(m.bck, objName, 0)
}

func TestBlobDownloadLockContentionLatestVersion(t *testing.T) {
	for _, tc := range []struct {
		name   string
		latest bool
	}{
		{"regular-get", false},
		{"latest-get", true},
	} {
		t.Run(tc.name, func(t *testing.T) { testBlobDownloadLatestVersion(t, tc.latest) })
	}
}

func testBlobDownloadLatestVersion(t *testing.T, latest bool) {
	var (
		regularGets = 4
		blobGets    = 8
		objSize     = int64(32 * cos.MiB) // exceed socket buffering while the leader is unread
		chunkSize   = int64(2 * cos.MiB)
	)
	if !testing.Short() {
		objSize, chunkSize = 64*cos.MiB, 4*cos.MiB
		regularGets, blobGets = 16, 32
	}
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiresRemoteCluster: true})
	var (
		proxyURL  = tools.RandomProxyURL(t)
		bp        = tools.BaseAPIParams(proxyURL)
		remoteBP  = tools.BaseAPIParams(tools.RemoteCluster.URL)
		remoteBck = cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
		bck       = cmn.Bck{Name: remoteBck.Name, Provider: apc.AIS, Ns: cmn.Ns{UUID: tools.RemoteCluster.UUID}}
		objName   = t.Name() + "-" + trand.String(5)
	)
	m := ioContext{t: t, bck: bck, proxyURL: proxyURL, objNames: []string{objName}}
	// Create a remote AIS bucket and enable versioning on its local namespace.
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	initMountpaths(t, proxyURL)
	_, err := api.SetBucketProps(bp, bck, &cmn.BpropsToSet{Versioning: &cmn.VersionConfToSet{Enabled: apc.Ptr(true)}})
	tassert.CheckFatal(t, err)

	// Put version 1 on the remote cluster and cache it through the local cluster.
	oldReader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cos.ChecksumNone})
	tassert.CheckFatal(t, err)
	putArgs := &api.PutArgs{BaseParams: remoteBP, Bck: remoteBck, ObjName: objName, Reader: oldReader, Size: uint64(objSize)}
	_, err = api.PutObject(putArgs)
	tassert.CheckFatal(t, err)
	cached, size, err := api.GetObjectReader(bp, bck, objName, nil)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, size == objSize, "expected size %d, got %d", objSize, size)
	expected, err := oldReader.Open()
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, tools.ReaderEqual(expected, cached), "failed to cache initial object version")
	cos.Close(expected)
	cos.Close(cached)

	// Replace version 1 directly on the remote cluster, leaving the local copy stale.
	newReader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cos.ChecksumCesXxh})
	tassert.CheckFatal(t, err)
	putArgs.Reader = newReader // out-of-band update on the remote cluster
	_, err = api.PutObject(putArgs)
	tassert.CheckFatal(t, err)
	// Start a latest-version blob GET that must replace the stale cached version.
	leader, size, err := api.GetObjectReader(bp, bck, objName, &api.GetArgs{
		Query: url.Values{apc.QparamLatestVer: []string{"true"}},
		Header: http.Header{
			apc.HdrBlobDownload: []string{"true"},
			apc.HdrBlobChunk:    []string{strconv.FormatInt(chunkSize, 10)},
			apc.HdrBlobWorkers:  []string{"2"},
		},
	})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, size == objSize, "expected size %d, got %d", objSize, size)

	// Race explicit blob GETs and regular GETs against the in-flight replacement.
	blobErrCh := make(chan error, blobGets)
	regularErrCh := make(chan error, regularGets)
	for i := range blobGets {
		args := &api.GetArgs{
			Query: url.Values{apc.QparamLatestVer: []string{"true"}},
			Header: http.Header{
				apc.HdrBlobDownload: []string{"true"},
				apc.HdrBlobChunk:    []string{strconv.FormatInt(int64(1<<uint(i%4))*cos.MiB, 10)},
				apc.HdrBlobWorkers:  []string{"2"},
			},
		}
		go func() { blobErrCh <- m.get(bp, 0, 0, args, true /*validate*/) }()
	}
	for range regularGets {
		go func() {
			var got cos.CksumHashSize
			got.Init(cos.ChecksumCesXxh)
			args := &api.GetArgs{Writer: &got}
			if latest {
				args.Query = url.Values{apc.QparamLatestVer: []string{"true"}}
			}
			err := m.get(bp, 0, 0, args, true /*validate*/)
			if err == nil {
				got.Finalize()
				if got.Size != objSize || !got.Equal(newReader.Cksum()) {
					err = errors.New("waiting GET returned stale object version")
				}
			}
			regularErrCh <- err
		}()
	}
	for range blobGets {
		err := <-blobErrCh
		herr := cmn.AsErrHTTP(err)
		tassert.Fatalf(t, herr != nil && herr.TypeCode == "ErrBusy", "expected ErrBusy, got %v", err)
	}

	// The leader returns version 2 and releases its lock for all waiting GETs.
	tassert.Fatalf(t, len(regularErrCh) == 0, "regular GETs completed before the leader released its lock")
	expected, err = newReader.Open()
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, tools.ReaderEqual(expected, leader), "blob GET returned stale object version")
	cos.Close(expected)
	cos.Close(leader)
	// Regular GETs, with and without latest-ver, wait for the leader and return version 2.
	for range regularGets {
		tassert.CheckFatal(t, <-regularErrCh)
	}
	// blob download stores the remote checksum: latest-version GETs must not refetch
	m.validateChunksOnDisk(bck, objName, int(objSize/chunkSize))
}

func TestBlobDownloadRemoteAISReadTimeout(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiresRemoteCluster: true})
	const objSize = 2 * cmn.ChunkSizeMin
	var (
		proxyURL  = tools.RandomProxyURL(t)
		bp        = tools.BaseAPIParams(proxyURL)
		remoteBP  = tools.BaseAPIParams(tools.RemoteCluster.URL)
		remoteBck = cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
		bck       = cmn.Bck{Name: remoteBck.Name, Provider: apc.AIS, Ns: cmn.Ns{UUID: tools.RemoteCluster.Alias}}
		objName   = t.Name() + "-object"
	)
	tlog.Logfln("remote cluster: alias %q, UUID %q, URL %s", tools.RemoteCluster.Alias, tools.RemoteCluster.UUID, tools.RemoteCluster.URL)
	tlog.Logfln("create %s (via %s)", bck.Cname(""), proxyURL)
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	initMountpaths(t, proxyURL)

	tlog.Logfln("put %s (size %s) directly to the remote cluster", remoteBck.Cname(objName), cos.ToSizeIEC(objSize, 0))
	_, err := api.PutObject(&api.PutArgs{
		BaseParams: remoteBP, Bck: remoteBck, ObjName: objName,
		Reader: readers.NewBytes(bytes.Repeat([]byte{'x'}, objSize)), Size: objSize,
	})
	tassert.CheckFatal(t, err)

	xid, err := api.BlobDownload(bp, bck, objName, &apc.BlobMsg{
		ChunkSize: cmn.ChunkSizeMin, NumWorkers: 2, ChunkReadTimeout: cos.Duration(time.Nanosecond),
	})
	tassert.CheckFatal(t, err)
	tlog.Logfln("%s[%s]: %s, chunk read timeout 1ns", apc.ActBlobDl, xid, bck.Cname(objName))

	args := &xact.ArgsMsg{ID: xid, Kind: apc.ActBlobDl, Timeout: tools.RebalanceTimeout}
	snaps, err := api.WaitForSnaps(bp, args, args.Finished())
	tassert.CheckFatal(t, err)

	var errText string
	for tid, targetSnaps := range snaps {
		for _, snap := range targetSnaps {
			if snap.ID != xid {
				continue
			}
			tlog.Logfln("%s[%s] on %s: finished %t, aborted %t, err %q, abort-err %q",
				apc.ActBlobDl, xid, meta.Tname(tid), snap.IsFinished(), snap.IsAborted(), snap.Err, snap.AbortErr)
			errText = snap.Err
		}
	}
	tassert.Fatalf(t, strings.Contains(errText, context.DeadlineExceeded.Error()),
		"expected context deadline, got %q", errText)

	tlog.Logfln("validate: no chunks on disk for %s", bck.Cname(objName))
	m := ioContext{t: t, bck: bck}
	m.validateChunksOnDisk(bck, objName, 0)
}

func TestBlobDownloadAbortByKind(t *testing.T) {
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

		err = api.WaitForXaction(baseParams, &args)
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
		blobThresh   = 8 * cos.MiB
		smallObjSize = 4 * cos.MiB  // below blob threshold
		largeObjSize = 16 * cos.MiB // above blob threshold
		chunkSize    = 4 * cos.MiB
		numSmallObjs = 1
		numLargeObjs = 1
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
			err = api.WaitForXaction(baseParams, &args)
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

func TestPrefetchChunkPolicy(t *testing.T) {
	const (
		bucketChunk  = 512 * cos.KiB
		requestChunk = 256 * cos.KiB
	)
	tools.CheckSkip(t, &tools.SkipTestArgs{CloudBck: true, Bck: cliBck, Long: true})
	tests := []struct {
		name                                      string
		objSize, threshold, objLimit, requestSize int64
		wantChunked                               bool
		wantBlob                                  bool
	}{
		{name: "threshold-disabled", objSize: 2*cos.MiB + 17, objLimit: cos.MiB,
			requestSize: requestChunk, wantChunked: true},
		{name: "bucket-threshold-wins", objSize: 1536*cos.KiB + 17, threshold: cos.MiB,
			objLimit: 2 * cos.MiB, requestSize: requestChunk},
		{name: "request-threshold-wins", objSize: 2 * cos.MiB, threshold: 2 * cos.MiB,
			objLimit: cos.MiB, requestSize: requestChunk, wantChunked: true, wantBlob: true},
		{name: "auto-disabled", objSize: 2*cos.MiB + 17, threshold: cos.MiB,
			requestSize: requestChunk, wantChunked: true, wantBlob: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proxyURL := tools.RandomProxyURL(t)
			bp := tools.BaseAPIParams(proxyURL)
			bck := cmn.Bck{Name: "prefetch-policy-" + trand.String(8), Provider: apc.AIS}
			props := &cmn.BpropsToSet{Chunks: &cmn.ChunksConfToSet{
				ObjSizeLimit: apc.Ptr(cos.SizeIEC(tc.objLimit)), ChunkSize: apc.Ptr(cos.SizeIEC(bucketChunk)),
			}}
			tools.CreateBucket(t, proxyURL, bck, props, true /* cleanup */)
			tools.SetBackendBck(t, bp, bck, cliBck)

			objName := "prefetch-policy/" + tc.name + "-" + trand.String(8)
			reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: tc.objSize, CksumType: cos.ChecksumNone})
			tassert.CheckFatal(t, err)
			tools.PutObject(t, cliBck, objName, reader, uint64(tc.objSize))
			defer api.DeleteObject(bp, cliBck, objName)
			tassert.CheckFatal(t, api.EvictObject(bp, cliBck, objName))

			msg := &apc.PrefetchMsg{ListRange: apc.ListRange{ObjNames: []string{objName}},
				BlobThreshold: tc.threshold, BlobChunkSize: tc.requestSize}
			xid, err := api.Prefetch(bp, bck, msg)
			tassert.CheckFatal(t, err)
			err = api.WaitForXaction(bp, &xact.ArgsMsg{ID: xid, Kind: apc.ActPrefetchObjects, Timeout: tools.EvictPrefetchTimeout})
			tassert.CheckFatal(t, err)

			marker := " job:[cold:(1,"
			if tc.wantBlob {
				marker = " job:[blob-started:(1,"
			}
			snaps, err := api.QueryXactionSnaps(bp, &xact.ArgsMsg{ID: xid})
			tassert.CheckFatal(t, err)
			matched := false
			for _, targetSnaps := range snaps {
				for _, snap := range targetSnaps {
					matched = matched || snap.ID == xid && strings.Contains(snap.CtlMsg, marker)
				}
			}
			tassert.Fatalf(t, matched, "expected prefetch stats to contain %q", marker)

			op, err := api.HeadObjectV2(bp, bck, objName, apc.GetPropsChunked, api.HeadArgs{})
			tassert.CheckFatal(t, err)
			if !tc.wantChunked {
				tassert.Fatalf(t, op.Chunks == nil || op.Chunks.ChunkCount == 0, "expected monolithic object, got %+v", op.Chunks)
				return
			}
			wantChunkSize := int64(bucketChunk)
			if tc.objLimit == 0 {
				wantChunkSize = tc.requestSize
			}
			wantChunks := int((tc.objSize + wantChunkSize - 1) / wantChunkSize)
			tassert.Fatalf(t, op.Chunks != nil && op.Chunks.ChunkCount == wantChunks && op.Chunks.MaxChunkSize == wantChunkSize,
				"expected %d chunks of up to %s, got %+v", wantChunks, cos.ToSizeIEC(wantChunkSize, 0), op.Chunks)
		})
	}
}

func TestGetObjectBlobThreshold(t *testing.T) {
	const objSize = 16 * cos.MiB
	tests := []struct {
		name         string
		threshold    int64
		blobDownload bool
		chunks       int
	}{
		{name: "disabled", threshold: 0, chunks: 0},
		{name: "object-below-threshold", threshold: objSize + 1, chunks: 0},
		{name: "object-at-threshold", threshold: objSize, chunks: -1},
		{name: "object-above-threshold", threshold: objSize - 1, chunks: -1},
		{name: "flag-with-object-below-threshold", threshold: objSize + 1, blobDownload: true, chunks: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m := ioContext{
				t:         t,
				bck:       cliBck,
				num:       1,
				prefix:    t.Name() + "-",
				fileSize:  objSize,
				fixedSize: true,
			}

			tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: m.bck})
			m.init(true /*cleanup*/)
			initMountpaths(t, m.proxyURL)
			m.remotePuts(true /*evict*/)

			buf := bytes.NewBuffer(nil)
			args := &api.GetArgs{
				Writer:        buf,
				BlobThreshold: test.threshold,
			}
			if test.blobDownload {
				args.Header = http.Header{apc.HdrBlobDownload: []string{"true"}}
			}
			oah, err := api.GetObject(tools.BaseAPIParams(m.proxyURL), m.bck, m.objNames[0], args)
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, oah.Size() == objSize, "expected size %d, got %d", objSize, oah.Size())
			tassert.Fatalf(t, int64(buf.Len()) == objSize, "expected %d bytes, got %d", objSize, buf.Len())
			m.validateChunksOnDisk(m.bck, m.objNames[0], test.chunks)
		})
	}
}

// blob download does not apply to range reads:
// - threshold only: regular (range) GET
// - explicit blob download: 400
func TestGetObjectBlobRange(t *testing.T) {
	const (
		objSize  = 16 * cos.MiB
		rngStart = cos.MiB
		rngLen   = 64 * cos.KiB
	)
	m := ioContext{
		t:         t,
		bck:       cliBck,
		num:       1,
		prefix:    t.Name() + "-",
		fileSize:  objSize,
		fixedSize: true,
	}
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: m.bck})
	m.init(true /*cleanup*/)
	initMountpaths(t, m.proxyURL)
	m.remotePuts(true /*evict*/)

	var (
		baseParams = tools.BaseAPIParams(m.proxyURL)
		objName    = m.objNames[0]
		rngHdr     = cmn.MakeRangeHdr(rngStart, rngLen)
	)

	tlog.Logfln("explicit blob download with range %q: expecting 400", rngHdr)
	_, err := api.GetObject(baseParams, m.bck, objName, &api.GetArgs{Header: http.Header{
		cos.HdrRange:        []string{rngHdr},
		apc.HdrBlobDownload: []string{"true"},
	}})
	tassert.Fatalf(t, api.HTTPStatus(err) == http.StatusBadRequest, "expected HTTP 400, got %v", err)

	tlog.Logfln("blob threshold with range %q: expecting regular range GET", rngHdr)
	rng := bytes.NewBuffer(nil)
	_, err = api.GetObject(baseParams, m.bck, objName, &api.GetArgs{
		Writer:        rng,
		BlobThreshold: objSize / 2,
		Header:        http.Header{cos.HdrRange: []string{rngHdr}},
	})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, int64(rng.Len()) == rngLen, "expected %d bytes, got %d", rngLen, rng.Len())
	m.validateChunksOnDisk(m.bck, objName, 0)

	tlog.Logfln("comparing range with the full object")
	full := bytes.NewBuffer(nil)
	_, err = api.GetObject(baseParams, m.bck, objName, &api.GetArgs{Writer: full})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, bytes.Equal(rng.Bytes(), full.Bytes()[rngStart:rngStart+rngLen]), "range content mismatch")
}

func TestGetObjectBlobLatestVersion(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiresRemoteCluster: true})
	var (
		proxyURL  = tools.RandomProxyURL(t)
		bp        = tools.BaseAPIParams(proxyURL)
		remoteBP  = tools.BaseAPIParams(tools.RemoteCluster.URL)
		remoteBck = cmn.Bck{
			Name: trand.String(10), Provider: apc.AIS,
		}
		bck = cmn.Bck{
			Name: remoteBck.Name, Provider: apc.AIS, Ns: cmn.Ns{UUID: tools.RemoteCluster.Alias},
		}
		v1 = bytes.Repeat([]byte{'1'}, 8*cos.MiB)
		v2 = bytes.Repeat([]byte{'2'}, len(v1))
	)
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	p, err := api.HeadBucket(bp, bck, false)
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		_, err := api.SetBucketProps(bp, bck, &cmn.BpropsToSet{Versioning: &cmn.VersionConfToSet{
			Enabled:         apc.Ptr(p.Versioning.Enabled),
			ValidateWarmGet: apc.Ptr(p.Versioning.ValidateWarmGet),
			Sync:            apc.Ptr(p.Versioning.Sync),
		}})
		tassert.CheckError(t, err)
	})

	tests := []struct {
		name     string
		query    url.Values
		validate bool
		sync     bool
	}{
		{name: "query", query: url.Values{apc.QparamLatestVer: []string{"true"}}},
		{name: "validate-warm-get", validate: true},
		{name: "synchronize", sync: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := api.SetBucketProps(bp, bck, &cmn.BpropsToSet{Versioning: &cmn.VersionConfToSet{
				Enabled: apc.Ptr(true), ValidateWarmGet: apc.Ptr(test.validate), Sync: apc.Ptr(test.sync),
			}})
			tassert.CheckFatal(t, err)

			objName := t.Name() + "-" + trand.String(5)
			_, err = api.PutObject(&api.PutArgs{
				BaseParams: remoteBP, Bck: remoteBck, ObjName: objName,
				Reader: readers.NewBytes(v1), Size: uint64(len(v1)),
			})
			tassert.CheckFatal(t, err)

			var buf bytes.Buffer
			_, err = api.GetObject(bp, bck, objName, &api.GetArgs{Writer: &buf})
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, bytes.Equal(buf.Bytes(), v1), "failed to cache initial object version")

			// out-of-band overwrite leaves the cached version stale
			_, err = api.PutObject(&api.PutArgs{
				BaseParams: remoteBP, Bck: remoteBck, ObjName: objName,
				Reader: readers.NewBytes(v2), Size: uint64(len(v2)),
			})
			tassert.CheckFatal(t, err)

			buf.Reset()
			oah, err := api.GetObject(bp, bck, objName, &api.GetArgs{
				Writer: &buf,
				Query:  test.query,
				Header: http.Header{apc.HdrBlobDownload: []string{"true"}},
			})
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, oah.Size() == int64(len(v2)), "expected size %d, got %d", len(v2), oah.Size())
			tassert.Fatalf(t, bytes.Equal(buf.Bytes(), v2), "blob GET returned stale object version")
			tassert.Fatalf(t, tools.CheckObjIsPresent(proxyURL, bck, objName), "latest object version is not cached")

			_, err = api.SetBucketProps(bp, bck, &cmn.BpropsToSet{Versioning: &cmn.VersionConfToSet{
				ValidateWarmGet: apc.Ptr(false), Sync: apc.Ptr(false),
			}})
			tassert.CheckFatal(t, err)
			buf.Reset()
			_, err = api.GetObject(bp, bck, objName, &api.GetArgs{Writer: &buf})
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, bytes.Equal(buf.Bytes(), v2), "warm GET returned stale object version")
		})
	}
}

// TestBlobDownloadStreamGet tests blob download via cold GET.
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
	tlog.Logfln("Performing cold GET with blob threshold")
	coldGetBuf := &bytes.Buffer{}
	getArgs := &api.GetArgs{
		Writer:        coldGetBuf,
		BlobThreshold: objSize,
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

func TestBlobDownloadChunkSizeBounds(t *testing.T) {
	const objSize = 65 * cos.MiB // just above blob downloader's 64MiB streaming limit
	tests := []struct {
		name      string
		streaming bool
		chunks    int
	}{
		{name: "cache-only", chunks: 1},
		{name: "streaming", streaming: true, chunks: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := ioContext{
				t: t, bck: cliBck, num: 1, fileSize: objSize, fixedSize: true,
				prefix: t.Name() + "/" + trand.String(5),
			}
			tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: m.bck})
			m.init(true /*cleanup*/)
			initMountpaths(t, m.proxyURL)
			m.remotePuts(true /*evict*/)
			bp, objName := tools.BaseAPIParams(m.proxyURL), m.objNames[0]

			if tc.streaming {
				_, err := api.GetObject(bp, m.bck, objName, &api.GetArgs{
					Writer: io.Discard,
					Header: http.Header{
						apc.HdrBlobDownload: []string{"true"},
						apc.HdrBlobChunk:    []string{cos.ToSizeIEC(cmn.ChunkSizeMax, 0)},
					},
				})
				tassert.CheckFatal(t, err)
			} else {
				xid, err := api.BlobDownload(bp, m.bck, objName, &apc.BlobMsg{
					ChunkSize: cmn.ChunkSizeMax, FullSize: objSize,
				})
				tassert.CheckFatal(t, err)
				err = api.WaitForXaction(bp, &xact.ArgsMsg{
					ID: xid, Kind: apc.ActBlobDl, Timeout: tools.EvictPrefetchTimeout,
				})
				tassert.CheckFatal(t, err)
			}
			m.validateChunksOnDisk(m.bck, objName, tc.chunks)
		})
	}
}

func TestBlobDownloadCacheOnlyMaxChunkSize(t *testing.T) {
	const (
		maxChunkSize = 64 * cos.MiB
		objSize      = maxChunkSize + 1
		readTimeout  = 2 * time.Minute
	)
	m := ioContext{
		t: t, bck: cliBck, num: 1, fileSize: objSize, fixedSize: true,
		prefix: t.Name() + "/" + trand.String(5),
	}
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true, RemoteBck: true, Bck: m.bck})
	m.init(true /*cleanup*/)
	initMountpaths(t, m.proxyURL)
	m.remotePuts(false /*evict*/)
	bp, objName := tools.BaseAPIParams(m.proxyURL), m.objNames[0]
	config := tools.GetClusterConfig(t)
	t.Cleanup(func() {
		tools.SetClusterConfig(t, cos.StrKVs{"timeout.send_file_time": config.Timeout.SendFile.String()})
	})
	tools.SetClusterConfig(t, cos.StrKVs{"timeout.send_file_time": readTimeout.String()})

	tests := []struct {
		name      string
		chunkSize int64
	}{
		{name: "at-max", chunkSize: maxChunkSize},
		{name: "above-max", chunkSize: objSize},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tassert.CheckFatal(t, api.EvictObject(bp, m.bck, objName))
			xid, err := api.BlobDownload(bp, m.bck, objName, &apc.BlobMsg{
				ChunkSize: tc.chunkSize, ChunkReadTimeout: cos.Duration(readTimeout),
			})
			tassert.CheckFatal(t, err)
			err = api.WaitForXaction(bp, &xact.ArgsMsg{
				ID: xid, Kind: apc.ActBlobDl, Timeout: tools.EvictPrefetchTimeout * 5,
			})
			tassert.CheckFatal(t, err)
			m.validateChunksOnDisk(m.bck, objName, 2)
		})
	}
}

func TestBlobDownloadParallelAdmission(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: cliBck})
	const (
		objSize   = 2 * cos.MiB
		chunkSize = 4 * cos.MiB
	)
	m := ioContext{t: t, bck: cliBck, num: 1, fileSize: objSize, fixedSize: true,
		prefix: "blob-admission/" + trand.String(8)}
	m.init(true /*cleanup*/)
	m.remotePuts(false /*evict*/)
	bp := tools.BaseAPIParams(m.proxyURL)
	objName := m.objNames[0]

	for _, tc := range []struct {
		name    string
		workers int
		mode    string
		status  int
	}{
		{"job/serial", -1, "job", http.StatusBadRequest},
		{"job/one-worker", 1, "job", http.StatusBadRequest},
		{"job/one-chunk", 0, "job", http.StatusBadRequest},
		{"stream/serial", -1, "stream", http.StatusBadRequest},
		{"stream/one-worker", 1, "stream", http.StatusBadRequest},
		{"stream/one-chunk", 0, "stream", 0},
		{"threshold/fallback", 0, "threshold", 0},
		{"prefetch/serial", -1, "prefetch", http.StatusBadRequest},
		{"prefetch/one-worker", 1, "prefetch", http.StatusBadRequest},
		{"prefetch/fallback", 0, "prefetch", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := api.EvictObject(bp, m.bck, objName)
			tassert.Fatalf(t, err == nil || api.HTTPStatus(err) == http.StatusNotFound, "evict: %v", err)
			var xid, kind string
			switch tc.mode {
			case "job":
				kind = apc.ActBlobDl
				xid, err = api.BlobDownload(bp, m.bck, objName, &apc.BlobMsg{ChunkSize: chunkSize, NumWorkers: tc.workers})
			case "prefetch":
				kind = apc.ActPrefetchObjects
				msg := &apc.PrefetchMsg{BlobThreshold: cos.MiB, BlobChunkSize: chunkSize, BlobNumWorkers: tc.workers}
				msg.ObjNames = []string{objName}
				xid, err = api.Prefetch(bp, m.bck, msg)
			default:
				header := http.Header{
					apc.HdrBlobChunk:   []string{strconv.Itoa(chunkSize)},
					apc.HdrBlobWorkers: []string{strconv.Itoa(tc.workers)},
				}
				if tc.mode == "stream" {
					header.Set(apc.HdrBlobDownload, "true")
				} else {
					header.Set(apc.HdrBlobThreshold, strconv.Itoa(cos.MiB))
				}
				_, err = api.GetObject(bp, m.bck, objName, &api.GetArgs{Header: header})
			}
			if xid != "" {
				tassert.CheckFatal(t, api.WaitForXaction(bp, &xact.ArgsMsg{ID: xid, Kind: kind, Timeout: tools.EvictPrefetchTimeout}))
			}
			if tc.status != 0 {
				tassert.Fatalf(t, api.HTTPStatus(err) == tc.status, "expected HTTP %d, got %v", tc.status, err)
				tassert.Fatalf(t, !tools.CheckObjIsPresent(bp.URL, m.bck, objName), "rejected download stored the object")
				return
			}
			tassert.CheckFatal(t, err)
			op, err := api.HeadObjectV2(bp, m.bck, objName, apc.JoinProps(apc.GetPropsChunked, apc.GetPropsSize), api.HeadArgs{FltPresence: apc.FltPresent})
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, op.Size == objSize && op.Chunks != nil && op.Chunks.ChunkCount == 0,
				"expected regular cold GET to store an unchunked object, got %+v", op)
		})
	}
}

func TestBlobDownloadParallel(t *testing.T) {
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
	props, err := api.HeadBucket(baseParams, bck, false)
	tassert.CheckFatal(t, err)

	for _, test := range []struct {
		name      string
		streamGet bool
		features  feat.Flags
	}{
		{name: "blob-download", streamGet: false},
		{name: "streaming-get", streamGet: true},
		{name: "blob-download-fsync", features: feat.FsyncPUT},
		{name: "streaming-get-fsync", streamGet: true, features: feat.FsyncPUT},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Cleanup(func() {
				_, err := api.SetBucketProps(baseParams, bck, &cmn.BpropsToSet{Features: apc.Ptr(props.Features)})
				tassert.CheckError(t, err)
			})
			features := props.Features&^feat.FsyncPUT | test.features
			_, err := api.SetBucketProps(baseParams, bck, &cmn.BpropsToSet{Features: &features})
			tassert.CheckFatal(t, err)
			objName := "blob-parallel-" + test.name + "-" + trand.String(5)

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
			t.Cleanup(func() { tassert.CheckError(t, api.DeleteObject(baseParams, bck, objName)) })

			// Evict the object to force cold GET
			tlog.Logfln("Evicting object %s", objName)
			err = api.EvictObject(baseParams, bck, objName)
			tassert.CheckFatal(t, err)

			if test.streamGet {
				// Test parallel blob download via streaming GET
				tlog.Logfln("Starting parallel blob download via streaming GET")
				coldGetBuf := &bytes.Buffer{}
				getArgs := &api.GetArgs{
					Writer: coldGetBuf,
					Header: http.Header{
						apc.HdrBlobDownload: []string{"true"},
						apc.HdrBlobChunk:    []string{cos.ToSizeIEC(chunkSize, 0)},
						apc.HdrBlobWorkers:  []string{"2"},
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
				// Test parallel blob download via API
				tlog.Logfln("Starting parallel blob download via API")
				blobMsg := &apc.BlobMsg{
					ChunkSize:  chunkSize,
					FullSize:   objSize,
					NumWorkers: 2,
					LatestVer:  false,
				}
				xid, err := api.BlobDownload(baseParams, bck, objName, blobMsg)
				tassert.CheckFatal(t, err)
				tlog.Logfln("Blob download started with xid=%s", xid)

				// Wait for blob download to complete
				tlog.Logfln("Waiting for parallel blob download to complete")
				args := xact.ArgsMsg{ID: xid, Kind: apc.ActBlobDl, Timeout: tools.EvictPrefetchTimeout}
				err = api.WaitForXaction(baseParams, &args)
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

			tlog.Logfln("Parallel blob download test (%s) completed successfully", test.name)
		})
	}
}

// TestPrefetchBlobInvalidWorkers validates that prefetch rejects an out-of-range
// BlobNumWorkers (negative values or one worker).
func TestPrefetchBlobInvalidWorkers(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cliBck
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: bck})

	for _, n := range []int{-2, -100} {
		msg := &apc.PrefetchMsg{
			BlobThreshold:  cos.MiB,
			BlobNumWorkers: n,
		}
		msg.Template = "prefetch-bad-workers/" + trand.String(5) + "/*"
		_, err := api.Prefetch(baseParams, bck, msg)
		tassert.Fatalf(t, err != nil, "expected error for blob-num-workers=%d, got nil", n)
		tassert.Fatalf(t, strings.Contains(err.Error(), "blob-num-workers"),
			"expected error to mention 'blob-num-workers', got: %v", err)
	}
}

// TestPrefetchBlobNumWorkers validates that PrefetchMsg.BlobNumWorkers is
// accepted across its full valid range and produces a correct, byte-identical
// download in every case.
//
// Covered values:
//
//	2 (minimum parallelism), 0 (auto-tuned), small positive (4),
//	above blobMaxWorkers cap (64).
func TestPrefetchBlobNumWorkers(t *testing.T) {
	const (
		objSize    = 4 * cos.MiB // small but > MinBlobDlPrefetchSize (1MiB)
		blobThresh = cos.MiB
	)

	for _, tc := range []struct {
		name      string
		requested int
	}{
		{name: "two-workers", requested: 2},
		{name: "auto-zero", requested: 0},
		{name: "small-positive", requested: 4},
		{name: "over-max-cap", requested: 64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var (
				proxyURL   = tools.RandomProxyURL(t)
				baseParams = tools.BaseAPIParams(proxyURL)
				bck        = cliBck
				uniq       = "prefetch-blob-workers/" + tc.name + "/" + trand.String(5)
				objName    = uniq + "/obj"
			)

			tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: bck, Long: true})
			initMountpaths(t, proxyURL)

			tlog.Logfln("Provisioning %s of size %s", objName, cos.ToSizeIEC(objSize, 0))
			reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cos.ChecksumNone})
			tassert.CheckFatal(t, err)
			tools.PutObject(t, bck, objName, reader, uint64(objSize))
			t.Cleanup(func() { tassert.CheckError(t, api.DeleteObject(baseParams, bck, objName)) })

			tlog.Logfln("Evicting %s", objName)
			tassert.CheckFatal(t, api.EvictObject(baseParams, bck, objName))

			msg := &apc.PrefetchMsg{
				BlobThreshold:  blobThresh,
				BlobChunkSize:  cos.MiB,
				BlobNumWorkers: tc.requested,
			}
			msg.Template = uniq + "/*"

			tlog.Logfln("Prefetching with blob-threshold=%s, blob-num-workers=%d",
				cos.ToSizeIEC(blobThresh, 0), tc.requested)
			xid, err := api.Prefetch(baseParams, bck, msg)
			tassert.CheckFatal(t, err)
			args := xact.ArgsMsg{ID: xid, Kind: apc.ActPrefetchObjects, Timeout: tools.EvictPrefetchTimeout}
			err = api.WaitForXaction(baseParams, &args)
			tassert.CheckFatal(t, err)

			// Byte-equality check: result must be identical across all valid BlobNumWorkers values.
			result, size, err := api.GetObjectReader(baseParams, bck, objName, nil)
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, size == objSize, "expected size %d, got %d", objSize, size)
			readerDup, err := reader.Open()
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, tools.ReaderEqual(readerDup, result),
				"data mismatch after blob prefetch with blob-num-workers=%d", tc.requested)
		})
	}
}

func TestRemaisBlobDownloadVersionPin(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiresRemoteCluster: true, RequiredDeployment: tools.ClusterTypeLocal})
	const chunkSize = 4 * cos.MiB
	var (
		proxyURL  = tools.RandomProxyURL(t)
		bp        = tools.BaseAPIParams(proxyURL)
		remoteBP  = tools.BaseAPIParams(tools.RemoteCluster.URL)
		remoteBck = cmn.Bck{Name: "pin-" + trand.String(8), Provider: apc.AIS}
		bck       = cmn.Bck{Name: remoteBck.Name, Provider: apc.AIS, Ns: cmn.Ns{UUID: tools.RemoteCluster.UUID}}
		objName   = "object"
		v1        = bytes.Repeat([]byte{'1'}, 2*chunkSize)
		v2        = bytes.Repeat([]byte{'2'}, len(v1))
	)
	// seed a versioned source
	tlog.Logfln("creating versioned bucket %s on %s", remoteBck.Name, remoteBP.URL)
	tools.CreateBucket(t, remoteBP.URL, remoteBck, &cmn.BpropsToSet{
		Versioning: &cmn.VersionConfToSet{Enabled: apc.Ptr(true)},
	}, true /*cleanup*/)
	put := func(data []byte) error {
		_, err := api.PutObject(&api.PutArgs{BaseParams: remoteBP, Bck: remoteBck, ObjName: objName,
			Reader: readers.NewBytes(data), Size: uint64(len(data))})
		return err
	}
	tlog.Logfln("writing v1: %s/%s (%d bytes)", remoteBck.Name, objName, len(v1))
	tassert.CheckFatal(t, put(v1))
	tlog.Logfln("registering remote bucket and initializing mountpaths")
	_, err := api.HeadBucket(bp, bck, false)
	tassert.CheckFatal(t, err)
	t.Cleanup(func() { tassert.CheckError(t, api.EvictRemoteBucket(bp, bck, false)) })
	initMountpaths(t, proxyURL)

	remoteURL, err := url.Parse(remoteBP.URL)
	tassert.CheckFatal(t, err)
	forward := httputil.NewSingleHostReverseProxy(remoteURL)
	forward.Transport = remoteBP.Client.Transport
	var ranges atomic.Int64
	updated := make(chan error, 1)
	// match the remote scheme: the remote proxy redirects to its targets, and
	// those redirects must reuse the same (TLS or plain) remote-AIS client
	newBridge := httptest.NewServer
	if cos.IsHTTPS(remoteBP.URL) {
		newBridge = httptest.NewTLSServer
	}
	bridge := newBridge(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/"+remoteBck.Name+"/"+objName) && r.Header.Get(cos.HdrRange) != "" {
			n := ranges.Add(1)
			tlog.Logfln("range %d: %s", n, r.Header.Get(cos.HdrRange))
			// overwrite before forwarding the second range
			if n == 2 {
				tlog.Logfln("overwriting source with v2 before range %d", n)
				err := put(v2)
				updated <- err
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadGateway)
					return
				}
				tlog.Logfln("source overwritten with v2")
			}
		}
		forward.ServeHTTP(w, r)
	}))
	t.Cleanup(bridge.Close)
	config, err := api.GetClusterConfig(bp)
	tassert.CheckFatal(t, err)
	backend := config.Backend
	backend.Conf = maps.Clone(backend.Conf)
	// use a fresh alias to refresh remote clients
	backend.Set(apc.AIS, cmn.BackendConfAIS{"pin-" + trand.String(8): []string{bridge.URL}})
	t.Cleanup(func() {
		tlog.Logfln("restoring backend config")
		err := api.SetClusterConfigUsingMsg(bp, &cmn.ConfigToSet{Backend: &config.Backend}, false)
		tassert.CheckError(t, err)
		if err == nil {
			// give proxies time to refresh the restored route before closing the bridge
			time.Sleep(2 * config.Timeout.CplaneOperation.D())
		}
	})
	tlog.Logfln("routing remote requests through %s", bridge.URL)
	tassert.CheckFatal(t, api.SetClusterConfigUsingMsg(bp, &cmn.ConfigToSet{Backend: &backend}, false))

	msg := &apc.BlobMsg{ChunkSize: chunkSize, NumWorkers: 2}
	// pin v1, then change the source between ranges
	tlog.Logfln("starting first fill: chunk size %d, two workers", chunkSize)
	xid, err := api.BlobDownload(bp, bck, objName, msg)
	tassert.CheckFatal(t, err)
	args := &xact.ArgsMsg{ID: xid, Kind: apc.ActBlobDl, Timeout: tools.RebalanceTimeout}
	tlog.Logfln("waiting for first fill %s", xid)
	snaps, err := api.WaitForSnaps(bp, args, args.Finished())
	tassert.CheckFatal(t, err)
	select {
	case err := <-updated:
		tassert.CheckFatal(t, err)
	default:
		t.Fatal("did not overwrite the source between range reads")
	}
	tassert.Fatalf(t, ranges.Load() == 2, "expected two range reads, got %d", ranges.Load())
	var errText string
	for _, targetSnaps := range snaps {
		for _, snap := range targetSnaps {
			if snap.ID == xid {
				errText = snap.Err
			}
		}
	}
	tlog.Logfln("first fill %s finished: %q", xid, errText)
	tassert.Fatalf(t, strings.Contains(errText, "precondition failed"), "expected pin mismatch, got %q", errText)
	// failed fills must leave no cached object or chunks
	tlog.Logfln("checking failed fill left no cached object or chunks")
	tassert.Fatalf(t, !tools.CheckObjIsPresent(proxyURL, bck, objName), "failed download published a cached object")
	m := ioContext{t: t, bck: bck}
	m.validateChunksOnDisk(bck, objName, 0)

	// retry with a new pin
	tlog.Logfln("starting fresh fill of v2")
	xid, err = api.BlobDownload(bp, bck, objName, msg)
	tassert.CheckFatal(t, err)
	tlog.Logfln("waiting for fresh fill %s", xid)
	tassert.CheckFatal(t, api.WaitForXaction(bp, &xact.ArgsMsg{ID: xid, Kind: apc.ActBlobDl, Timeout: tools.RebalanceTimeout}))
	tlog.Logfln("reading cached object and checking v2")
	var out bytes.Buffer
	_, err = api.GetObject(bp, bck, objName, &api.GetArgs{Writer: &out})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, bytes.Equal(out.Bytes(), v2), "fresh fill returned mixed revisions")
	m.validateChunksOnDisk(bck, objName, 2)
	tlog.Logfln("verified v2 bytes and two cached chunks")
}

func TestBlobDownloadRangeLength(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal})
	const (
		chunkSize = 64 * cos.MiB
		fullSize  = 4*chunkSize + 17 // allow two workers
		objName   = "object"
	)
	bp := tools.BaseAPIParams(tools.RandomProxyURL(t))
	initMountpaths(t, bp.URL)
	for _, tc := range []struct{ fault string }{
		{"short-length"}, {"ignored-range"}, {"empty"}, {"short-body"},
	} {
		t.Run(tc.fault, func(t *testing.T) {
			smap := meta.Smap{Pmap: make(meta.NodeMap), Tmap: make(meta.NodeMap), UUID: cos.GenUUID(), Version: 1}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == apc.URLPathDae.S:
					w.Header().Set(cos.HdrContentType, cos.ContentJSON)
					_ = json.NewEncoder(w).Encode(&smap)
				case strings.HasPrefix(r.URL.Path, apc.URLPathBuckets.S):
					w.Header().Set(apc.HdrBucketProps, "{}")
				case r.Method == http.MethodHead:
					w.Header().Set(cos.HdrContentLength, strconv.Itoa(fullSize))
				default:
					length, code := chunkSize, http.StatusPartialContent
					switch tc.fault {
					case "short-length":
						length = 1
					case "ignored-range":
						length, code = fullSize, http.StatusOK
					case "empty":
						length = 0
					}
					w.Header().Set(cos.HdrContentLength, strconv.Itoa(length))
					w.WriteHeader(code)
					if length > 0 {
						_, _ = w.Write([]byte{'x'})
					}
				}
			}))
			t.Cleanup(server.Close)
			alias := "blob-length-" + trand.String(8)
			tassert.CheckFatal(t, api.AttachRemoteAIS(bp, alias, server.URL))
			t.Cleanup(func() { tassert.CheckError(t, api.DetachRemoteAIS(bp, alias)) })
			bck := cmn.Bck{Name: alias, Provider: apc.AIS, Ns: cmn.Ns{UUID: smap.UUID}}
			_, err := api.HeadBucket(bp, bck, false)
			tassert.CheckFatal(t, err)
			t.Cleanup(func() { tassert.CheckError(t, api.EvictRemoteBucket(bp, bck, false)) })
			xid, err := api.BlobDownload(bp, bck, objName, &apc.BlobMsg{ChunkSize: chunkSize, NumWorkers: 2})
			tassert.CheckFatal(t, err)
			args := &xact.ArgsMsg{ID: xid, Kind: apc.ActBlobDl, Timeout: 15 * time.Second}
			snaps, err := api.WaitForSnaps(bp, args, args.Finished())
			if err != nil {
				_ = api.AbortXaction(bp, args)
				t.Fatal(err)
			}
			var errText string
			for _, targetSnaps := range snaps {
				for _, snap := range targetSnaps {
					if snap.ID == xid {
						errText = snap.Err
					}
				}
			}
			if tc.fault == "short-body" {
				tassert.Fatalf(t, strings.Contains(errText, "unexpected EOF"), "expected truncated body error, got %q", errText)
			} else {
				tassert.Fatalf(t, strings.Contains(errText, "expected") && strings.Contains(errText, "bytes, got"), "expected range length error, got %q", errText)
			}
			tassert.Fatalf(t, !tools.CheckObjIsPresent(bp.URL, bck, objName), "failed download published a cached object")
			m := ioContext{t: t, bck: bck}
			m.validateChunksOnDisk(bck, objName, 0)
		})
	}
}

func TestBlobDownloadChecksum(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal})
	proxyURL := tools.RandomProxyURL(t)
	bp := tools.BaseAPIParams(proxyURL)

	for _, test := range []struct {
		checksum   string
		outOfOrder bool
	}{
		{cos.ChecksumCesXxh, false},
		{cos.ChecksumSHA256, false},
		{cos.ChecksumNone, false},
		{cos.ChecksumCesXxh, true},
		{cos.ChecksumSHA256, true},
		{cos.ChecksumNone, true},
	} {
		name := test.checksum + "/parallel"
		if test.outOfOrder {
			name = test.checksum + "/out-of-order"
		}
		t.Run(name, func(t *testing.T) {
			chunkSize, fullChunks := int64(64*cos.KiB), 2
			if test.outOfOrder {
				tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
				chunkSize, fullChunks = 64*cos.MiB, 4
			}
			var (
				smap   = meta.Smap{Pmap: make(meta.NodeMap), Tmap: make(meta.NodeMap), UUID: cos.GenUUID(), Version: 1}
				data   = append(bytes.Repeat([]byte("checksum"), int(chunkSize)*fullChunks/8), []byte("unequal last part")...)
				second chan struct{}
			)
			if test.outOfOrder {
				second = make(chan struct{})
			}
			data[chunkSize] ^= 0xff // distinguish equal-sized chunks
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == apc.URLPathDae.S {
					w.Header().Set(cos.HdrContentType, cos.ContentJSON)
					_ = json.NewEncoder(w).Encode(&smap)
					return
				}
				if strings.HasPrefix(r.URL.Path, apc.URLPathBuckets.S) {
					w.Header().Set(apc.HdrBucketProps, "{}")
					return
				}
				rangeHdr := r.Header.Get(cos.HdrRange)
				if r.Method == http.MethodGet && second != nil && rangeHdr == cmn.MakeRangeHdr(0, chunkSize) {
					// Hold the first response until the second chunk has been streamed.
					select {
					case <-second:
					case <-r.Context().Done():
						return
					}
				}
				http.ServeContent(w, r, "object", time.Time{}, bytes.NewReader(data))
				if r.Method == http.MethodGet && second != nil && rangeHdr == cmn.MakeRangeHdr(chunkSize, chunkSize) {
					close(second)
				}
			}))
			t.Cleanup(server.Close)
			alias := "blob-checksum-" + trand.String(8)
			tassert.CheckFatal(t, api.AttachRemoteAIS(bp, alias, server.URL))
			t.Cleanup(func() { tassert.CheckError(t, api.DetachRemoteAIS(bp, alias)) })
			bck := cmn.Bck{Name: "checksum-" + trand.String(8), Provider: apc.AIS, Ns: cmn.Ns{UUID: smap.UUID}}
			_, err := api.HeadBucket(bp, bck, false)
			tassert.CheckFatal(t, err)
			t.Cleanup(func() { tassert.CheckError(t, api.EvictRemoteBucket(bp, bck, false)) })
			_, err = api.SetBucketProps(bp, bck, &cmn.BpropsToSet{
				Cksum: &cmn.CksumConfToSet{Type: apc.Ptr(test.checksum)},
			})
			tassert.CheckFatal(t, err)

			const objName = "object"
			xid, err := api.BlobDownload(bp, bck, objName, &apc.BlobMsg{
				ChunkSize: chunkSize, NumWorkers: 2,
			})
			tassert.CheckFatal(t, err)
			tassert.CheckFatal(t, api.WaitForXaction(bp, &xact.ArgsMsg{ID: xid, Kind: apc.ActBlobDl, Timeout: tools.RebalanceTimeout}))

			props, err := api.HeadObjectV2(bp, bck, objName, apc.GetPropsChecksum, api.HeadArgs{FltPresence: apc.FltPresent})
			tassert.CheckFatal(t, err)
			actual := props.ObjAttrs.Checksum()
			if test.checksum == cos.ChecksumNone {
				tassert.Errorf(t, cos.NoneC(actual), "expected no checksum, got %v", actual)
			} else {
				tassert.Fatalf(t, actual != nil, "missing checksum")
				expectedType := actual.Type()
				tassert.Errorf(t, expectedType == test.checksum || expectedType == cos.ChecksumCRC32C,
					"unexpected checksum type %q", expectedType)
				if test.outOfOrder {
					expectedType = cos.ChecksumCRC32C
				}
				expected := cos.NewCksum(expectedType, cos.ChecksumB2S(data, expectedType))
				tassert.Errorf(t, expected.Equal(actual), "expected %v, got %v", expected, actual)
			}
		})
	}
}
