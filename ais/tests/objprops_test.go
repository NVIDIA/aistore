// Package ais_test contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais_test

import (
	"path"
	"strconv"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func propsStats(t *testing.T, proxyURL string) (objChanged int64, bytesChanged int64) {
	cstats := tutils.GetClusterStats(t, proxyURL)
	objChanged = 0
	bytesChanged = 0

	for _, v := range cstats.Target {
		objChanged += tutils.GetNamedTargetStats(v, stats.VerChangeCount)
		bytesChanged += tutils.GetNamedTargetStats(v, stats.VerChangeSize)
	}
	return
}

func propsUpdateObjects(t *testing.T, proxyURL, bucket string, oldVersions map[string]string, msg *cmn.SelectMsg,
	versionEnabled bool, bckIsAIS bool) (newVersions map[string]string) {
	newVersions = make(map[string]string, len(oldVersions))
	tutils.Logf("Updating objects...\n")
	r, err := tutils.NewRandReader(int64(fileSize), true /* withHash */)
	if err != nil {
		t.Errorf("Failed to create reader: %v", err)
		t.Fail()
	}
	baseParams := tutils.BaseAPIParams(proxyURL)
	for fname := range oldVersions {
		putArgs := api.PutObjectArgs{
			BaseParams: baseParams,
			Bucket:     bucket,
			Object:     fname,
			Hash:       r.XXHash(),
			Reader:     r,
		}
		err = api.PutObject(putArgs)
		if err != nil {
			t.Errorf("Failed to put new data to object %s/%s, err: %v", bucket, fname, err)
		}
	}

	reslist := testListBucket(t, proxyURL, bucket, msg, 0)
	if reslist == nil {
		return
	}

	var (
		ver string
		ok  bool
	)
	for _, m := range reslist.Entries {
		if ver, ok = oldVersions[m.Name]; !ok {
			continue
		}
		tutils.Logf("Object %s new version %s\n", m.Name, m.Version)
		newVersions[m.Name] = m.Version

		if !m.CheckExists() && !bckIsAIS {
			t.Errorf("Object %s/%s is not marked as cached one", bucket, m.Name)
		}
		if !versionEnabled {
			continue
		}

		if ver == m.Version {
			t.Errorf("Object %s/%s version has not changed", bucket, m.Name)
			t.Fail()
		} else if m.Version == "" {
			t.Errorf("Object %s/%s version is empty", bucket, m.Name)
			t.Fail()
		}
	}

	return
}

func propsReadObjects(t *testing.T, proxyURL, bucket string, objList map[string]string) {
	versChanged, bytesChanged := propsStats(t, proxyURL)
	tutils.Logf("Version mismatch stats before test. Objects: %d, bytes fetched: %d\n", versChanged, bytesChanged)

	baseParams := tutils.BaseAPIParams(proxyURL)
	for object := range objList {
		_, err := api.GetObject(baseParams, bucket, object)
		if err != nil {
			t.Errorf("Failed to read %s/%s, err: %v", bucket, object, err)
			continue
		}
	}

	versChangedFinal, bytesChangedFinal := propsStats(t, proxyURL)
	tutils.Logf("Version mismatch stats after test. Objects: %d, bytes fetched: %d\n", versChangedFinal, bytesChangedFinal)
	if versChanged != versChangedFinal || bytesChanged != bytesChangedFinal {
		t.Errorf("All objects must be retreived from the cache but cold get happened: %d times (%d bytes)",
			versChangedFinal-versChanged, bytesChangedFinal-bytesChanged)
		t.Fail()
	}
}

func propsEvict(t *testing.T, proxyURL, bucket, provider string, objMap map[string]string, msg *cmn.SelectMsg, versionEnabled bool) {
	// generate a object list to evict (evict 1/3 of total objects - random selection)
	toEvict := len(objMap) / 3
	if toEvict == 0 {
		toEvict = 1
	}
	toEvictList := make([]string, 0, toEvict)
	evictMap := make(map[string]bool, toEvict)
	tutils.Logf("Evicting %v objects:\n", toEvict)

	for fname := range objMap {
		evictMap[fname] = true
		toEvictList = append(toEvictList, fname)
		tutils.Logf("    %s/%s\n", bucket, fname)
		if len(toEvictList) >= toEvict {
			break
		}
	}

	err := api.EvictList(tutils.BaseAPIParams(proxyURL), bucket, provider, toEvictList, true, 0)
	if err != nil {
		t.Errorf("Failed to evict objects: %v\n", err)
		t.Fail()
	}

	tutils.Logf("Reading object list...\n")

	// read a new object list and check that evicted objects do not have atime and iscached==false
	// version must be the same
	reslist := testListBucket(t, proxyURL, bucket, msg, 0)
	if reslist == nil {
		return
	}

	for _, m := range reslist.Entries {
		oldVersion, ok := objMap[m.Name]
		if !ok {
			continue
		}
		tutils.Logf("%s/%s [%d] - iscached: [%v], atime [%v]\n", bucket, m.Name, m.Flags, m.CheckExists, m.Atime)

		// invalid object: rebalance leftover or uploaded directly to target
		if !m.IsStatusOK() {
			continue
		}

		if _, wasEvicted := evictMap[m.Name]; wasEvicted {
			if m.Atime != "" {
				t.Errorf("Evicted object %s/%s still has atime '%s'", bucket, m.Name, m.Atime)
				t.Fail()
			}
			if m.CheckExists() {
				t.Errorf("Evicted object %s/%s is still marked as cached one", bucket, m.Name)
				t.Fail()
			}
		}

		if !versionEnabled {
			continue
		}

		if m.Version == "" {
			t.Errorf("Object %s/%s version is empty", bucket, m.Name)
			t.Fail()
		} else if m.Version != oldVersion {
			t.Errorf("Object %s/%s version has changed from %s to %s", bucket, m.Name, oldVersion, m.Version)
			t.Fail()
		}
	}
}

func propsRecacheObjects(t *testing.T, proxyURL, bucket string, objs map[string]string, msg *cmn.SelectMsg, versionEnabled bool) {
	tutils.Logf("Refetching objects...\n")
	propsReadObjects(t, proxyURL, bucket, objs)
	tutils.Logf("Checking objects properties after refetching...\n")
	reslist := testListBucket(t, proxyURL, bucket, msg, 0)
	if reslist == nil {
		t.Errorf("Unexpected error: no object in the bucket %s", bucket)
		t.Fail()
	}
	var (
		version string
		ok      bool
	)
	for _, m := range reslist.Entries {
		if version, ok = objs[m.Name]; !ok {
			continue
		}

		if !m.CheckExists() {
			t.Errorf("Object %s/%s is not marked as cached one", bucket, m.Name)
		}
		if m.Atime == "" {
			t.Errorf("Object %s/%s access time is empty", bucket, m.Name)
		}

		if !versionEnabled {
			continue
		}

		if m.Version == "" {
			t.Errorf("Failed to read object %s/%s version", bucket, m.Name)
			t.Fail()
		} else if version != m.Version {
			t.Errorf("Object %s/%s versions mismatch: old[%s], new[%s]", bucket, m.Name, version, m.Version)
			t.Fail()
		}
	}
}

func propsRebalance(t *testing.T, proxyURL, bucket string, objects map[string]string, msg *cmn.SelectMsg, versionEnabled bool, bckIsAIS bool) {
	baseParams := tutils.BaseAPIParams(proxyURL)
	propsCleanupObjects(t, proxyURL, bucket, objects)

	smap := tutils.GetClusterMap(t, proxyURL)
	l := smap.CountTargets()
	if l < 2 {
		t.Skipf("Only %d targets found, need at least 2", l)
	}

	removeTarget := tutils.ExtractTargetNodes(smap)[0]

	tutils.Logf("Removing a target: %s\n", removeTarget.ID())
	err := tutils.UnregisterNode(proxyURL, removeTarget.ID())
	tassert.CheckFatal(t, err)
	smap, err = tutils.WaitForPrimaryProxy(
		proxyURL,
		"target is gone",
		smap.Version, testing.Verbose(),
		smap.CountProxies(),
		smap.CountTargets()-1,
	)
	tassert.CheckError(t, err)

	tutils.Logf("Target %s [%s] is removed\n", removeTarget.ID(), removeTarget.URL(cmn.NetworkPublic))

	// rewrite objects and compare versions - they should change
	newobjs := propsUpdateObjects(t, proxyURL, bucket, objects, msg, versionEnabled, bckIsAIS)

	tutils.Logf("Reregistering target...\n")
	err = tutils.RegisterNode(proxyURL, removeTarget, smap)
	tassert.CheckFatal(t, err)
	_, err = tutils.WaitForPrimaryProxy(
		proxyURL,
		"to join target back",
		smap.Version, testing.Verbose(),
		smap.CountProxies(),
		smap.CountTargets()+1,
	)
	tassert.CheckFatal(t, err)
	tutils.WaitForRebalanceToComplete(t, baseParams, rebalanceTimeout)

	tutils.Logf("Reading file versions...\n")
	reslist := testListBucket(t, proxyURL, bucket, msg, 0)
	if reslist == nil {
		t.Errorf("Unexpected error: no object in the bucket %s", bucket)
		t.Fail()
	}
	var (
		version  string
		ok       bool
		objFound int
	)
	for _, m := range reslist.Entries {
		if version, ok = newobjs[m.Name]; !ok {
			continue
		}

		if !m.IsStatusOK() {
			continue
		}

		objFound++

		if !m.CheckExists() && !bckIsAIS {
			t.Errorf("Object %s/%s is not marked as cached one", bucket, m.Name)
		}
		if m.Atime == "" {
			t.Errorf("Object %s/%s access time is empty", bucket, m.Name)
		}

		if !versionEnabled {
			continue
		}

		tutils.Logf("Object %s/%s, version before rebalance [%s], after [%s]\n", bucket, m.Name, version, m.Version)
		if version != m.Version {
			t.Errorf("Object %s/%s version mismatch: existing [%s], expected [%s]", bucket, m.Name, m.Version, version)
		}
	}

	if objFound != len(objects) {
		t.Errorf("The number of objects after rebalance differs for the number before it. Current: %d, expected %d", objFound, len(objects))
	}
}

func propsCleanupObjects(t *testing.T, proxyURL, bucket string, newVersions map[string]string) {
	errCh := make(chan error, 100)
	wg := &sync.WaitGroup{}
	for objname := range newVersions {
		wg.Add(1)
		go tutils.Del(proxyURL, bucket, objname, "", wg, errCh, !testing.Verbose())
	}
	wg.Wait()
	tassert.SelectErr(t, errCh, "delete", abortonerr)
	close(errCh)
}

func propsTestCore(t *testing.T, versionEnabled bool, bckIsAIS bool) {
	const (
		objCountToTest = 15
		filesize       = cmn.MiB
	)
	var (
		filesPutCh = make(chan string, objCountToTest)
		fileslist  = make(map[string]string, objCountToTest)
		errCh      = make(chan error, objCountToTest)
		numPuts    = objCountToTest
		bucket     = clibucket
		versionDir = "versionid"
		proxyURL   = tutils.GetPrimaryURL()
	)

	sgl := tutils.Mem2.NewSGL(filesize)
	defer sgl.Free()

	// Create a few objects
	tutils.Logf("Creating %d objects...\n", numPuts)
	ldir := LocalSrcDir + "/" + versionDir
	tutils.PutRandObjs(proxyURL, bucket, ldir, readerType, versionDir, filesize, numPuts, errCh, filesPutCh, sgl)
	tassert.SelectErr(t, errCh, "put", false)
	close(filesPutCh)
	close(errCh)
	for fname := range filesPutCh {
		if fname != "" {
			fileslist[path.Join(versionDir, fname)] = ""
		}
	}

	// Read object versions
	msg := &cmn.SelectMsg{Prefix: versionDir}
	msg.AddProps(cmn.GetPropsVersion, cmn.GetPropsIsCached, cmn.GetPropsAtime, cmn.GetPropsStatus)
	reslist := testListBucket(t, proxyURL, bucket, msg, 0)
	if reslist == nil {
		t.Errorf("Unexpected error: no object in the bucket %s", bucket)
		t.Fail()
		return
	}

	// PUT objects must have all properties set: atime, iscached, version
	for _, m := range reslist.Entries {
		if _, ok := fileslist[m.Name]; !ok {
			continue
		}
		tutils.Logf("Initial version %s - %v\n", m.Name, m.Version)

		if !m.CheckExists() && !bckIsAIS {
			t.Errorf("Object %s/%s is not marked as cached one", bucket, m.Name)
		}

		if m.Atime == "" {
			t.Errorf("Object %s/%s access time is empty", bucket, m.Name)
		}

		if !versionEnabled {
			continue
		}

		if m.Version == "" {
			t.Errorf("Failed to read object %q version", m.Name)
			t.Fail()
		} else {
			fileslist[m.Name] = m.Version
		}
	}

	// rewrite objects and compare versions - they should change
	newVersions := propsUpdateObjects(t, proxyURL, bucket, fileslist, msg, versionEnabled, bckIsAIS)
	if len(newVersions) != len(fileslist) {
		t.Errorf("Number of objects mismatch. Expected: %d objects, after update: %d", len(fileslist), len(newVersions))
	}

	// check that files are read from cache
	propsReadObjects(t, proxyURL, bucket, fileslist)

	if !bckIsAIS {
		// try to evict some files and check if they are gone
		propsEvict(t, proxyURL, bucket, cmn.Cloud, newVersions, msg, versionEnabled)

		// read objects to put them to the cache. After that all objects must have iscached=true
		propsRecacheObjects(t, proxyURL, bucket, newVersions, msg, versionEnabled)
	}

	// test rebalance should keep object versions
	propsRebalance(t, proxyURL, bucket, newVersions, msg, versionEnabled, bckIsAIS)

	// cleanup
	propsCleanupObjects(t, proxyURL, bucket, newVersions)
}

func propsMainTest(t *testing.T, versioning bool) {
	proxyURL := tutils.GetPrimaryURL()
	chkVersion := true

	config := tutils.GetClusterConfig(t)
	oldChkVersion := config.Versioning.ValidateWarmGet
	oldVersioning := config.Versioning.Enabled

	newConfig := make(cmn.SimpleKVs)
	if oldVersioning != versioning {
		newConfig[cmn.HeaderBucketVerEnabled] = strconv.FormatBool(versioning)
	}
	warmCheck := chkVersion && versioning
	if oldChkVersion != warmCheck {
		newConfig[cmn.HeaderBucketVerValidateWarm] = strconv.FormatBool(warmCheck)
	}
	if len(newConfig) != 0 {
		tutils.SetClusterConfig(t, newConfig)
	}
	created := createBucketIfNotExists(t, proxyURL, clibucket)

	defer func() {
		// restore configuration
		newConfig := make(cmn.SimpleKVs)
		oldWarmCheck := oldChkVersion && oldVersioning
		if oldWarmCheck != warmCheck {
			newConfig[cmn.HeaderBucketVerValidateWarm] = strconv.FormatBool(oldWarmCheck)
		}
		if oldVersioning != versioning {
			newConfig[cmn.HeaderBucketVerEnabled] = strconv.FormatBool(oldVersioning)
		}
		if len(newConfig) != 0 {
			tutils.SetClusterConfig(t, newConfig)
		}
		if created {
			tutils.DestroyBucket(t, proxyURL, clibucket)
		}
	}()

	props, err := api.HeadBucket(tutils.DefaultBaseAPIParams(t), clibucket)
	if err != nil {
		t.Fatalf("Could not execute HeadBucket Request: %v", err)
	}
	versionEnabled := props.Versioning.Enabled
	bckIsAIS := props.CloudProvider == cmn.ProviderAIS
	propsTestCore(t, versionEnabled, bckIsAIS)
}

func TestObjPropsVersionEnabled(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	propsMainTest(t, true)
}

func TestObjPropsVersionDisabled(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	propsMainTest(t, false)
}
