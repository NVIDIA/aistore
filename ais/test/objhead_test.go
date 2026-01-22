// Package integration_test.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/xact"
)

// TestObjHeadFlt tests various FltPresence filter modes
func TestObjHeadFlt(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{Name: "test-head-flt", Provider: apc.AIS}
		objName    = "test-obj"
		objSize    = int64(1024)
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	// PUT an object
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

	// Test FltPresent - should succeed
	t.Run("FltPresent", func(t *testing.T) {
		props, err := api.HeadObject(baseParams, bck, objName, api.HeadArgs{FltPresence: apc.FltPresent})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, props.Present, "object should be marked as present")
		tassert.Fatalf(t, props.Size == objSize, "size mismatch: expected %d, got %d", objSize, props.Size)
		tassert.Fatalf(t, props.Atime != 0, "access time should be set")
	})

	// Test FltPresentNoProps - should succeed with no props when cached
	t.Run("FltPresentNoPropsCached", func(t *testing.T) {
		props, err := api.HeadObject(baseParams, bck, objName, api.HeadArgs{FltPresence: apc.FltPresentNoProps})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, props == nil, "props should be nil for FltPresentNoProps")
	})

	// Test FltPresentNoProps on evicted object - must fail (object not present/cached)
	t.Run("FltPresentNoPropsEvicted", func(t *testing.T) {
		tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: cliBck})

		evictBck := cliBck
		evictObj := "test-evict-" + cos.GenTie()

		// PUT to remote bucket (will be cached)
		r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cos.ChecksumNone})
		tassert.CheckFatal(t, err)
		_, err = api.PutObject(&api.PutArgs{
			BaseParams: baseParams,
			Bck:        evictBck,
			ObjName:    evictObj,
			Reader:     r,
			Size:       uint64(objSize),
		})
		tassert.CheckFatal(t, err)
		defer func() {
			_ = api.DeleteObject(baseParams, evictBck, evictObj)
		}()

		// Verify it works when cached
		_, err = api.HeadObject(baseParams, evictBck, evictObj, api.HeadArgs{FltPresence: apc.FltPresentNoProps})
		tassert.CheckFatal(t, err)

		// Evict the object (exists remotely, not cached locally)
		err = api.EvictObject(baseParams, evictBck, evictObj)
		tassert.CheckFatal(t, err)

		// FltPresentNoProps should now fail - object not cached (though it exists remotely)
		_, err = api.HeadObject(baseParams, evictBck, evictObj, api.HeadArgs{FltPresence: apc.FltPresentNoProps})
		tassert.Fatalf(t, err != nil, "FltPresentNoProps should fail for evicted (non-cached) object")
		tassert.Fatalf(t, cos.IsNotExist(err, 0), "should return 404 when not cached locally")
	})

	// Test FltExistsOutside - should fail (object IS cached)
	t.Run("FltExistsOutside", func(t *testing.T) {
		_, err := api.HeadObject(baseParams, bck, objName, api.HeadArgs{FltPresence: apc.FltExistsOutside})
		tassert.Fatalf(t, err != nil, "FltExistsOutside should fail when object is cached")
	})

	// Test non-existent object with FltPresent - should fail
	t.Run("NotFound_FltPresent", func(t *testing.T) {
		_, err := api.HeadObject(baseParams, bck, "non-existent", api.HeadArgs{FltPresence: apc.FltPresent})
		tassert.Fatalf(t, err != nil, "HEAD on non-existent object should fail")
		tassert.Fatalf(t, cos.IsNotExist(err, 0), "should return 404")
	})
}

// TestObjHeadRemoteEvicted tests HEAD on evicted remote objects (cold HEAD path)
func TestObjHeadRemoteEvicted(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: cliBck})

	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cliBck
		objName    = "test-head-evict-" + cos.GenTie()
		objSize    = int64(2048)
	)

	// PUT to remote bucket
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
	defer func() {
		_ = api.DeleteObject(baseParams, bck, objName)
	}()

	// Verify object is cached
	props, err := api.HeadObject(baseParams, bck, objName, api.HeadArgs{FltPresence: apc.FltPresent})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, props.Present, "object should be cached after PUT")
	tassert.Fatalf(t, props.Atime != 0, "atime should be set for cached object")

	// Evict the object
	err = api.EvictObject(baseParams, bck, objName)
	tassert.CheckFatal(t, err)
	tlog.Logf("Evicted %s\n", bck.Cname(objName))

	// Test cold HEAD after eviction
	t.Run("ColdHEAD_AfterEvict", func(t *testing.T) {
		props, err := api.HeadObject(baseParams, bck, objName, api.HeadArgs{})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, !props.Present, "object should NOT be marked as present (not cached)")
		tassert.Fatalf(t, props.Atime == 0, "atime should be 0 for non-cached object")
		tassert.Fatalf(t, props.Size == objSize, "size should match even when not cached")
	})

	// Test FltPresent after eviction - should fail
	t.Run("FltPresent_AfterEvict", func(t *testing.T) {
		_, err := api.HeadObject(baseParams, bck, objName, api.HeadArgs{FltPresence: apc.FltPresent})
		tassert.Fatalf(t, err != nil, "FltPresent should fail for evicted object")
		tassert.Fatalf(t, cos.IsNotExist(err, 0), "should return 404 for evicted + FltPresent")
	})

	// Test FltExistsOutside after eviction - should succeed
	t.Run("FltExistsOutside_AfterEvict", func(t *testing.T) {
		props, err := api.HeadObject(baseParams, bck, objName, api.HeadArgs{FltPresence: apc.FltExistsOutside})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, !props.Present, "object should not be present locally")
		tassert.Fatalf(t, props.Size == objSize, "size should be available from remote")
	})
}

// TestObjHeadLatestVersion tests the latest=true flag behavior
func TestObjHeadLatestVersion(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: cliBck})

	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cliBck
		objName    = "test-head-latest-" + cos.GenTie()
		objSize    = int64(1024)
	)

	// PUT object to remote bucket
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
	defer func() {
		_ = api.DeleteObject(baseParams, bck, objName)
	}()

	// Test latest=true when object is cached
	t.Run("Latest_Cached", func(t *testing.T) {
		props, err := api.HeadObject(baseParams, bck, objName, api.HeadArgs{LatestVer: true})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, props.Present, "object should still be present after latest check")
		tlog.Logf("Verified latest version for cached object: size=%d\n", props.Size)
	})

	// Evict and test latest=true on non-cached object
	err = api.EvictObject(baseParams, bck, objName)
	tassert.CheckFatal(t, err)

	t.Run("Latest_Evicted", func(t *testing.T) {
		props, err := api.HeadObject(baseParams, bck, objName, api.HeadArgs{LatestVer: true})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, !props.Present, "object should not be present after eviction + latest")
		tassert.Fatalf(t, props.Size == objSize, "size should match from remote")
	})
}

// TestObjHeadV2Selective tests the V2 HEAD endpoint with selective property retrieval
func TestObjHeadV2Selective(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{Name: "test-head-v2", Provider: apc.AIS}
		objName    = "test-obj-v2"
		objSize    = int64(10 * cos.KiB)
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	// Set bucket checksum type
	_, err := api.SetBucketProps(baseParams, bck, &cmn.BpropsToSet{
		Cksum: &cmn.CksumConfToSet{Type: apc.Ptr(cos.ChecksumOneXxh)},
	})
	tassert.CheckFatal(t, err)

	// PUT an object
	r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cos.ChecksumOneXxh})
	tassert.CheckFatal(t, err)
	_, err = api.PutObject(&api.PutArgs{
		BaseParams: baseParams,
		Bck:        bck,
		ObjName:    objName,
		Reader:     r,
		Size:       uint64(objSize),
	})
	tassert.CheckFatal(t, err)

	// Set custom metadata
	customMD := cos.StrKVs{"user-key": "user-value"}
	err = api.SetObjectCustomProps(baseParams, bck, objName, customMD, false)
	tassert.CheckFatal(t, err)

	tlog.Logf("Created object %s with size %d and custom metadata\n", objName, objSize)

	t.Run("DefaultProps", func(t *testing.T) {
		opV2, err := api.HeadObjectV2(baseParams, bck, objName, "", api.HeadArgs{})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, opV2 != nil, "props should not be nil")
		tassert.Fatalf(t, opV2.Size == objSize, "size mismatch: expected %d, got %d", objSize, opV2.Size)

		// These fields should NOT be populated (not requested)
		tassert.Fatalf(t, opV2.LastModified == "", "last-modified should be empty")
		tassert.Fatalf(t, opV2.ETag == "", "etag should be empty")
		tassert.Fatalf(t, opV2.Location == nil, "location should be nil (not requested)")
		tassert.Fatalf(t, opV2.Mirror == nil, "mirror should be nil (not requested)")
		tassert.Fatalf(t, opV2.EC == nil, "EC should be nil (not requested)")
	})

	t.Run("WithChecksum", func(t *testing.T) {
		opV2, err := api.HeadObjectV2(baseParams, bck, objName, apc.GetPropsChecksum, api.HeadArgs{})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, opV2 != nil, "props should not be nil")
		tassert.Fatalf(t, opV2.Cksum != nil, "checksum should be present")
		tassert.Fatalf(t, opV2.Cksum.Type() == cos.ChecksumOneXxh, "checksum should be XXHash, got %s", opV2.Cksum.Type())
		tassert.Fatalf(t, opV2.Cksum.Val() == r.Cksum().Val(), "checksum value should be equal")
	})

	t.Run("LastModifiedUpdates", func(t *testing.T) {
		// Create a new object for this test
		testObjName := "test-last-modified-" + cos.GenUUID()
		r1, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cos.ChecksumOneXxh})
		tassert.CheckFatal(t, err)
		_, err = api.PutObject(&api.PutArgs{
			BaseParams: baseParams,
			Bck:        bck,
			ObjName:    testObjName,
			Reader:     r1,
		})
		tassert.CheckFatal(t, err)

		// Get initial LastModified
		opV2Before, err := api.HeadObjectV2(baseParams, bck, testObjName, apc.GetPropsLastModified, api.HeadArgs{})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, opV2Before.LastModified != "", "initial last-modified should be present")
		tlog.Logf("Initial LastModified: %s\n", opV2Before.LastModified)

		time.Sleep(1 * time.Second) // sleep to ensure the last-modified changes

		// Update the object with new content
		r2, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize + 100, CksumType: cos.ChecksumOneXxh})
		tassert.CheckFatal(t, err)
		_, err = api.PutObject(&api.PutArgs{
			BaseParams: baseParams,
			Bck:        bck,
			ObjName:    testObjName,
			Reader:     r2,
		})
		tassert.CheckFatal(t, err)

		// Get updated LastModified
		opV2After, err := api.HeadObjectV2(baseParams, bck, testObjName, apc.GetPropsLastModified, api.HeadArgs{})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, opV2After.LastModified != "", "updated last-modified should be present")
		tlog.Logf("Updated LastModified: %s\n", opV2After.LastModified)

		// Validate LastModified has changed
		tassert.Fatalf(t, opV2Before.LastModified != opV2After.LastModified,
			"LastModified should change after update: before=%s, after=%s",
			opV2Before.LastModified, opV2After.LastModified)
	})

	t.Run("WithLocation", func(t *testing.T) {
		props := apc.GetPropsLocation + apc.LsPropsSepa + apc.GetPropsSize
		opV2, err := api.HeadObjectV2(baseParams, bck, objName, props, api.HeadArgs{})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, opV2 != nil, "props should not be nil")
		tassert.Fatalf(t, opV2.Size == objSize, "size mismatch")
		tassert.Fatalf(t, opV2.Location != nil && *opV2.Location != "", "location should be populated")

		// Mirror/EC should NOT be populated
		tassert.Fatalf(t, opV2.Mirror == nil, "mirror should be nil (not requested)")
		tassert.Fatalf(t, opV2.EC == nil, "EC should be nil (not requested)")
	})

	t.Run("WithCopies", func(t *testing.T) {
		props := apc.GetPropsCopies + apc.LsPropsSepa + apc.GetPropsSize
		opV2, err := api.HeadObjectV2(baseParams, bck, objName, props, api.HeadArgs{})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, opV2 != nil, "props should not be nil")
		tassert.Fatalf(t, opV2.Size == objSize, "size mismatch")
		tassert.Fatalf(t, opV2.Mirror != nil, "mirror should be populated")
		tassert.Fatalf(t, opV2.Mirror.Copies >= 1, "mirror copies should be >= 1")

		// Location and EC should NOT be populated
		tassert.Fatalf(t, opV2.Location == nil, "location should be nil (not requested)")
		tassert.Fatalf(t, opV2.EC == nil, "EC should be nil (not requested)")
	})

	t.Run("WithCustom", func(t *testing.T) {
		props := apc.GetPropsCustom + apc.LsPropsSepa + apc.GetPropsSize
		opV2, err := api.HeadObjectV2(baseParams, bck, objName, props, api.HeadArgs{})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, opV2 != nil, "props should not be nil")
		tassert.Fatalf(t, opV2.Size == objSize, "size mismatch")
		// Check custom metadata
		tassert.Fatalf(t, opV2.CustomMD != nil, "custom metadata should be populated")
		val, ok := opV2.CustomMD["user-key"]
		tassert.Fatalf(t, ok && val == "user-value", "custom metadata should contain user-key=user-value")
	})

	t.Run("MultipleProps", func(t *testing.T) {
		props := apc.GetPropsSize + apc.LsPropsSepa + apc.GetPropsChecksum + apc.LsPropsSepa +
			apc.GetPropsLocation + apc.LsPropsSepa + apc.GetPropsCopies
		opV2, err := api.HeadObjectV2(baseParams, bck, objName, props, api.HeadArgs{})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, opV2 != nil, "props should not be nil")
		tassert.Fatalf(t, opV2.Size == objSize, "size mismatch")
		tassert.Fatalf(t, opV2.Cksum != nil, "checksum should be present")
		tassert.Fatalf(t, opV2.Location != nil && *opV2.Location != "", "location should be populated")
		tassert.Fatalf(t, opV2.Mirror != nil && opV2.Mirror.Copies >= 1, "mirror should be populated")

		// EC should NOT be populated (not requested)
		tassert.Fatalf(t, opV2.EC == nil, "EC should be nil (not requested)")
	})

	t.Run("InvalidProperty", func(t *testing.T) {
		// Request an invalid property - should return 400 Bad Request
		props := apc.GetPropsSize + apc.LsPropsSepa + "invalid_prop"
		_, err := api.HeadObjectV2(baseParams, bck, objName, props, api.HeadArgs{})
		tassert.Fatalf(t, err != nil, "expected error for invalid property")

		// Verify it's a 400 Bad Request with the expected message
		herr, ok := err.(*cmn.ErrHTTP)
		tassert.Fatalf(t, ok, "expected ErrHTTP, got %T", err)
		tassert.Fatalf(t, herr.Status == http.StatusBadRequest,
			"expected status 400, got %d", herr.Status)
		tassert.Fatalf(t, strings.Contains(herr.Message, "invalid property") &&
			strings.Contains(herr.Message, "invalid_prop"),
			"expected error message about invalid property, got: %s", herr.Message)
	})

	t.Run("WithChunks", func(t *testing.T) {
		var (
			chunkedObjName = "test-chunked-obj"
			chunkSize      = int64(4 * cos.KiB)
			objSizeLimit   = int64(8 * cos.KiB)
			largeObjSize   = int64(20 * cos.KiB) // larger than objSizeLimit to trigger chunking
		)

		// Set bucket chunk properties
		_, err := api.SetBucketProps(baseParams, bck, &cmn.BpropsToSet{
			Chunks: &cmn.ChunksConfToSet{
				ObjSizeLimit: apc.Ptr(cos.SizeIEC(objSizeLimit)),
				ChunkSize:    apc.Ptr(cos.SizeIEC(chunkSize)),
			},
		})
		tassert.CheckFatal(t, err)

		// PUT a large object (larger than objSizeLimit)
		largeReader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: largeObjSize, CksumType: cos.ChecksumOneXxh})
		tassert.CheckFatal(t, err)
		_, err = api.PutObject(&api.PutArgs{
			BaseParams: baseParams,
			Bck:        bck,
			ObjName:    chunkedObjName,
			Reader:     largeReader,
			Size:       uint64(largeObjSize),
		})
		tassert.CheckFatal(t, err)

		// Run rechunk job
		xid, err := api.RechunkBucket(baseParams, bck, &apc.RechunkMsg{ObjSizeLimit: objSizeLimit, ChunkSize: chunkSize})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, xid != "", "rechunk xaction ID should not be empty")

		// Wait for rechunk to complete
		_, err = api.WaitForXactionIC(baseParams, &xact.ArgsMsg{ID: xid, Kind: apc.ActRechunk, Bck: bck, Timeout: 2 * time.Minute})
		tassert.CheckFatal(t, err)

		// Use HeadObjectV2 with "chunked" property
		props := apc.GetPropsChunked + apc.LsPropsSepa + apc.GetPropsSize
		opV2, err := api.HeadObjectV2(baseParams, bck, chunkedObjName, props, api.HeadArgs{})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, opV2 != nil, "props should not be nil")
		tassert.Fatalf(t, opV2.Size == largeObjSize, "size mismatch: expected %d, got %d", largeObjSize, opV2.Size)

		// Validate chunk properties
		tassert.Fatalf(t, opV2.Chunks != nil, "chunks should be populated")
		expectedChunkCount := int((largeObjSize + chunkSize - 1) / chunkSize) // ceiling division
		tassert.Fatalf(t, opV2.Chunks.ChunkCount == expectedChunkCount,
			"chunk count mismatch: expected %d, got %d", expectedChunkCount, opV2.Chunks.ChunkCount)
		tassert.Fatalf(t, opV2.Chunks.MaxChunkSize > 0 && opV2.Chunks.MaxChunkSize <= chunkSize,
			"max chunk size should be > 0 and <= %d, got %d", chunkSize, opV2.Chunks.MaxChunkSize)
	})

	t.Run("V1vsV2Consistency", func(t *testing.T) {
		// Get V1 (all props)
		opV1, err := api.HeadObject(baseParams, bck, objName, api.HeadArgs{})
		tassert.CheckFatal(t, err)

		// Get V2 (selective props)
		props := apc.GetPropsSize + apc.LsPropsSepa + apc.GetPropsChecksum + apc.LsPropsSepa + apc.GetPropsAtime
		opV2, err := api.HeadObjectV2(baseParams, bck, objName, props, api.HeadArgs{})
		tassert.CheckFatal(t, err)

		// Validate: V1 and V2 should return the same values for requested properties
		tassert.Fatalf(t, opV1.Size == opV2.Size, "size mismatch between V1 and V2")
		tassert.Fatalf(t, opV1.Cksum.Equal(opV2.Cksum), "checksum mismatch between V1 and V2")
		tassert.Fatalf(t, opV1.Atime == opV2.Atime, "atime mismatch between V1 and V2")
	})
}

// TestObjHeadV2RemoteBucket tests V2 HEAD with remote cloud bucket
func TestObjHeadV2RemoteBucket(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: cliBck})

	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cliBck
		objName    = "test-remote-v2-" + cos.GenTie()
		objSize    = int64(5 * cos.KiB)
	)

	// PUT to remote bucket
	r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cos.ChecksumOneXxh})
	tassert.CheckFatal(t, err)
	_, err = api.PutObject(&api.PutArgs{
		BaseParams: baseParams,
		Bck:        bck,
		ObjName:    objName,
		Reader:     r,
		Size:       uint64(objSize),
	})
	tassert.CheckFatal(t, err)
	defer func() {
		_ = api.DeleteObject(baseParams, bck, objName)
	}()

	// Test 1: Request selective properties while cached
	t.Run("CachedSelectiveProps", func(t *testing.T) {
		opV2, err := api.HeadObjectV2(baseParams, bck, objName, apc.GetPropsChecksum, api.HeadArgs{})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, opV2 != nil, "props should not be nil")
		tassert.Fatalf(t, opV2.Size == objSize, "size mismatch")
		tassert.Fatalf(t, opV2.Cksum != nil, "checksum should be present")
		tassert.Fatalf(t, opV2.Present, "object should be marked as present/cached")
	})

	// Test 2: Evict and request selective properties (cold HEAD)
	t.Run("EvictedColdHEAD", func(t *testing.T) {
		// Evict the object
		err := api.EvictObject(baseParams, bck, objName)
		tassert.CheckFatal(t, err)

		// Request selective properties - should trigger cold HEAD
		opV2, err := api.HeadObjectV2(baseParams, bck, objName, apc.GetPropsChecksum, api.HeadArgs{})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, opV2 != nil, "props should not be nil")
		tassert.Fatalf(t, opV2.Size == objSize, "size mismatch after cold HEAD: expected %d, got %d", objSize, opV2.Size)
		tassert.Fatalf(t, opV2.Cksum != nil, "checksum should be present after cold HEAD")
		tassert.Fatalf(t, !opV2.Present, "object should NOT be marked as cached after eviction")
	})
}
