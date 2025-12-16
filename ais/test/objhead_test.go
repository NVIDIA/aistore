// Package integration_test.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
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
