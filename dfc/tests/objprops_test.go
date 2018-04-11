/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/pkg/client/readers"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
)

func propsStats(t *testing.T) (objChanged int64, bytesChanged int64) {
	stats := getClusterStats(httpclient, t)
	objChanged = 0
	bytesChanged = 0

	for _, v := range stats.Target {
		objChanged += v.Core.Numvchanged
		bytesChanged += v.Core.Bytesvchanged
	}

	return
}

func propsUpdateObjects(t *testing.T, bucket string, oldVersions map[string]string, msg *dfc.GetMsg,
	versionEnabled bool, isLocalBucket bool) (newVersions map[string]string) {
	newVersions = make(map[string]string, len(oldVersions))
	tlogf("Updating objects...\n")
	r, err := readers.NewRandReader(int64(fileSize), true /* withHash */)
	if err != nil {
		t.Errorf("Failed to create reader: %v", err)
		t.Fail()
	}
	for fname, _ := range oldVersions {
		err = client.Put(proxyurl, r, bucket, fname, false)
		if err != nil {
			t.Errorf("Failed to put new data to object %s/%s, err: %v", bucket, fname, err)
			t.Fail()
		}
	}

	reslist := testListBucket(t, bucket, msg)
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
		tlogf("Object %s new version %s\n", m.Name, m.Version)
		newVersions[m.Name] = m.Version

		if !m.IsCached && !isLocalBucket {
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

func propsReadObjects(t *testing.T, bucket string, filelist map[string]string) {
	versChanged, bytesChanged := propsStats(t)
	tlogf("Version mismatch stats before test. Objects: %d, bytes fetched: %d\n", versChanged, bytesChanged)

	for fname, _ := range filelist {
		_, _, err := client.Get(proxyurl, bucket, fname, nil, nil, false, false)
		if err != nil {
			t.Errorf("Failed to read %s/%s, err: %v", bucket, fname, err)
			continue
		}
	}

	versChangedFinal, bytesChangedFinal := propsStats(t)
	tlogf("Version mismatch stats after test. Objects: %d, bytes fetched: %d\n", versChangedFinal, bytesChangedFinal)
	if versChanged != versChangedFinal || bytesChanged != bytesChangedFinal {
		t.Errorf("All objects must be retreived from the cache but cold get happened: %d times (%d bytes)",
			versChangedFinal-versChanged, bytesChangedFinal-bytesChanged)
		t.Fail()
	}
}

func propsEvict(t *testing.T, bucket string, objMap map[string]string, msg *dfc.GetMsg, versionEnabled bool) {
	// generate a object list to evict (evict 1/3 of total objects - random selection)
	toEvict := len(objMap) / 3
	if toEvict == 0 {
		toEvict = 1
	}
	toEvictList := make([]string, 0, toEvict)
	evictMap := make(map[string]bool, toEvict)
	tlogf("Evicting %v objects:\n", toEvict)

	for fname, _ := range objMap {
		evictMap[fname] = true
		toEvictList = append(toEvictList, fname)
		tlogf("    %s/%s\n", bucket, fname)
		if len(toEvictList) >= toEvict {
			break
		}
	}

	err := client.EvictList(proxyurl, bucket, toEvictList, true, 0)
	if err != nil {
		t.Errorf("Failed to evict objects: %v\n", err)
		t.Fail()
	}

	tlogf("Reading object list...\n")

	// read a new object list and check that evicted objects do not have atime and iscached==false
	// version must be the same
	reslist := testListBucket(t, bucket, msg)
	if reslist == nil {
		return
	}

	for _, m := range reslist.Entries {
		oldVersion, ok := objMap[m.Name]
		if !ok {
			continue
		}
		tlogf("%s/%s - iscached: [%v], atime [%v]\n", bucket, m.Name, m.IsCached, m.Atime)

		if _, wasEvicted := evictMap[m.Name]; wasEvicted {
			if m.Atime != "" {
				t.Errorf("Evicted object %s/%s still has atime '%s'", bucket, m.Name, m.Atime)
				t.Fail()
			}
			if m.IsCached {
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

func propsRecacheObjects(t *testing.T, bucket string, objs map[string]string, msg *dfc.GetMsg, versionEnabled bool) {
	tlogf("Refetching objects...\n")
	propsReadObjects(t, bucket, objs)
	tlogf("Checking objects properties after refetching...\n")
	reslist := testListBucket(t, bucket, msg)
	if reslist == nil {
		t.Errorf("Unexpected erorr: no object in the bucket %s", bucket)
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

		if !m.IsCached {
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

func propsRebalance(t *testing.T, bucket string, objects map[string]string, msg *dfc.GetMsg, versionEnabled bool, isLocalBucket bool) {
	propsCleanupObjects(t, bucket, objects)

	smap := getClusterMap(httpclient, t)
	l := len(smap.Smap)
	if l < 2 {
		t.Skipf("Only %d targets found, need at least 2", l)
	}

	var removedSid string
	for sid := range smap.Smap {
		removedSid = sid
		break
	}

	tlogf("Removing a target: %s\n", removedSid)
	unregisterTarget(removedSid, t)

	tlogf("Target %s is removed\n", removedSid)

	// rewrite objects and compare versions - they should change
	newobjs := propsUpdateObjects(t, bucket, objects, msg, versionEnabled, isLocalBucket)

	tlogf("Reregistering target...\n")
	registerTarget(removedSid, &smap, t)
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		smap = getClusterMap(httpclient, t)
		if len(smap.Smap) == l {
			break
		}
	}

	smap = getClusterMap(httpclient, t)
	if l != len(smap.Smap) {
		t.Errorf("Target failed to reregister. Current number of targets: %d (expected %d)", len(smap.Smap), l)
	}
	//
	// wait for rebalance to run its course
	//
	waitProgressBar("Rebalance: ", time.Second*10)

	tlogf("Reading file versions...\n")
	reslist := testListBucket(t, bucket, msg)
	if reslist == nil {
		t.Errorf("Unexpected erorr: no object in the bucket %s", bucket)
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

		objFound++

		if !m.IsCached && !isLocalBucket {
			t.Errorf("Object %s/%s is not marked as cached one", bucket, m.Name)
		}
		if m.Atime == "" {
			t.Errorf("Object %s/%s access time is empty", bucket, m.Name)
		}

		if !versionEnabled {
			continue
		}

		tlogf("Object %s/%s, version before rebalance [%s], after [%s]\n", bucket, m.Name, version, m.Version)
		if version != m.Version {
			t.Errorf("Object %s/%s version mismatch: existing [%s], expected [%s]", bucket, m.Name, m.Version, version)
		}
	}

	if objFound != len(objects) {
		t.Errorf("The number of objects after rebalance differs for the number before it. Current: %d, expected %d", objFound, len(objects))
	}
}

func propsCleanupObjects(t *testing.T, bucket string, newVersions map[string]string) {
	errch := make(chan error, 100)
	wg := &sync.WaitGroup{}
	for objname, _ := range newVersions {
		wg.Add(1)
		go client.Del(proxyurl, bucket, objname, wg, errch, false)
	}
	wg.Wait()
	selectErr(errch, "delete", t, abortonerr)
	close(errch)
}

func propsMainTest(t *testing.T, versionEnabled bool, isLocalBucket bool) {
	const objCountToTest = 15
	var (
		filesput   = make(chan string, objCountToTest)
		fileslist  = make(map[string]string, objCountToTest)
		errch      = make(chan error, objCountToTest)
		filesize   = uint64(1024 * 1024)
		numPuts    = objCountToTest
		bucket     = clibucket
		versionDir = "versionid"
		sgl        *dfc.SGLIO
	)

	parse()
	if usingSG {
		sgl = dfc.NewSGLIO(filesize)
		defer sgl.Free()
	}

	// Create a few objects
	tlogf("Creating %d objects...\n", numPuts)
	ldir := LocalSrcDir + "/" + versionDir
	htype := dfc.ChecksumNone
	putRandomFiles(0, baseseed+110, filesize, int(numPuts), bucket, t, nil, errch, filesput, ldir, versionDir, htype, true, sgl)
	selectErr(errch, "put", t, false)
	close(filesput)
	for fname := range filesput {
		if fname != "" {
			fileslist[versionDir+"/"+fname] = ""
		}
	}

	// Read object versions
	msg := &dfc.GetMsg{
		GetPrefix: versionDir,
		GetProps:  dfc.GetPropsVersion + ", " + dfc.GetPropsIsCached + ", " + dfc.GetPropsAtime,
	}
	reslist := testListBucket(t, bucket, msg)
	if reslist == nil {
		t.Errorf("Unexpected erorr: no object in the bucket %s", bucket)
		t.Fail()
		return
	}

	// PUT objects must have all properties set: atime, iscached, version
	for _, m := range reslist.Entries {
		if _, ok := fileslist[m.Name]; !ok {
			continue
		}
		tlogf("Intial version %s - %v\n", m.Name, m.Version)

		if !m.IsCached && !isLocalBucket {
			t.Errorf("Object %s/%s is not marked as cached one", bucket, m.Name)
		}

		if m.Atime == "" {
			t.Errorf("Object %s/%s access time is empty", bucket, m.Name)
		}

		if !versionEnabled {
			continue
		}

		if m.Version == "" {
			t.Error("Failed to read object version")
			t.Fail()
		} else {
			fileslist[m.Name] = m.Version
		}
	}

	// rewrite objects and compare versions - they should change
	newVersions := propsUpdateObjects(t, bucket, fileslist, msg, versionEnabled, isLocalBucket)
	if len(newVersions) != len(fileslist) {
		t.Errorf("Number of objects mismatch. Expected: %d objects, after update: %d", len(fileslist), len(newVersions))
	}

	// check that files are read from cache
	propsReadObjects(t, bucket, fileslist)

	if !isLocalBucket {
		// try to evict some files and check if they are gone
		propsEvict(t, bucket, newVersions, msg, versionEnabled)

		// read objects to put them to the cache. After that all objects must have iscached=true
		propsRecacheObjects(t, bucket, newVersions, msg, versionEnabled)
	}

	// test rebalance should keep object versions
	propsRebalance(t, bucket, newVersions, msg, versionEnabled, isLocalBucket)

	// cleanup
	propsCleanupObjects(t, bucket, newVersions)
}

func Test_objpropsVersionEnabled(t *testing.T) {
	var (
		chkVersion    = true
		versioning    = "all"
		isLocalBucket = false
	)
	config := getConfig(proxyurl+"/v1/daemon", httpclient, t)
	versionCfg := config["version_config"].(map[string]interface{})
	oldChkVersion := versionCfg["validate_warm_get"].(bool)
	oldVersioning := versionCfg["versioning"].(string)
	if oldChkVersion != chkVersion {
		setConfig("validate_warm_get", fmt.Sprintf("%v", chkVersion), proxyurl+"/v1/cluster", httpclient, t)
	}
	if oldVersioning != versioning {
		setConfig("versioning", versioning, proxyurl+"/v1/cluster", httpclient, t)
	}

	// Skip the test when given a local bucket
	props, err := client.HeadBucket(proxyurl, clibucket)
	if err != nil {
		t.Errorf("Could not execute HeadBucket Request: %v", err)
		return
	}
	if props.CloudProvider == dfc.ProviderDfc {
		isLocalBucket = true
	}
	versionEnabled := props.Versioning != dfc.VersionNone

	propsMainTest(t, versionEnabled, isLocalBucket)

	// restore configuration
	if oldChkVersion != chkVersion {
		setConfig("validate_warm_get", fmt.Sprintf("%v", oldChkVersion), proxyurl+"/v1/cluster", httpclient, t)
	}
	if oldVersioning != versioning {
		setConfig("versioning", oldVersioning, proxyurl+"/v1/cluster", httpclient, t)
	}
}

func Test_objpropsVersionDisabled(t *testing.T) {
	var (
		chkVersion    = true
		versioning    = "none"
		isLocalBucket = false
	)
	config := getConfig(proxyurl+"/v1/daemon", httpclient, t)
	versionCfg := config["version_config"].(map[string]interface{})
	oldChkVersion := versionCfg["validate_warm_get"].(bool)
	oldVersioning := versionCfg["versioning"].(string)
	if oldChkVersion != chkVersion {
		setConfig("validate_warm_get", fmt.Sprintf("%v", chkVersion), proxyurl+"/v1/cluster", httpclient, t)
	}
	if oldVersioning != versioning {
		setConfig("versioning", versioning, proxyurl+"/v1/cluster", httpclient, t)
	}

	// Skip the test when given a local bucket
	props, err := client.HeadBucket(proxyurl, clibucket)
	if err != nil {
		t.Errorf("Could not execute HeadBucket Request: %v", err)
		return
	}
	if props.CloudProvider == dfc.ProviderDfc {
		isLocalBucket = true
	}
	versionEnabled := props.Versioning != dfc.VersionNone

	propsMainTest(t, versionEnabled, isLocalBucket)

	// restore configuration
	if oldChkVersion != chkVersion {
		setConfig("validate_warm_get", fmt.Sprintf("%v", oldChkVersion), proxyurl+"/v1/cluster", httpclient, t)
	}
	if oldVersioning != versioning {
		setConfig("versioning", oldVersioning, proxyurl+"/v1/cluster", httpclient, t)
	}
}
