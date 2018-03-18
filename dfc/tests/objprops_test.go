/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

import (
	"encoding/json"
	"fmt"
	"testing"

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

func propsUpdateObjects(t *testing.T, bucket string, oldVersions map[string]string, msgbytes []byte) (newVersions map[string]string) {
	newVersions = make(map[string]string, len(oldVersions))
	tlogf("Rewriting objects...\n")
	r, err := readers.NewRandReader(int64(fileSize), true /* withHash */)
	if err != nil {
		t.Errorf("Failed to create reader: %v", err)
		t.FailNow()
	}

	for fname, _ := range oldVersions {
		err = client.Put(proxyurl, r, bucket, fname, false)
		if err != nil {
			t.Errorf("Failed to put new data to object %s/%s, err: %v", bucket, fname, err)
			t.Fail()
		}
	}

	reslist := testListBucket(t, bucket, msgbytes)
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
		tlogf("New version %s - %v\n", m.Name, m.Version)
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
	tlogf("Before test: Version mismathed: %d, bytes fetched: %d\n", versChanged, bytesChanged)

	for fname, _ := range filelist {
		_, err := client.Get(proxyurl, bucket, fname, nil, nil, false, false)
		if err != nil {
			t.Errorf("Failed to read %s/%s, err: %s", bucket, fname, err)
			continue
		}
	}

	versChangedFinal, bytesChangedFinal := propsStats(t)
	tlogf("After test: Version mismathed: %d, bytes fetched: %d\n", versChangedFinal, bytesChangedFinal)
	if versChanged != versChangedFinal || bytesChanged != bytesChangedFinal {
		t.Errorf("All objects must be retreived from the cache but cold get happened: %d (%d bytes)", versChangedFinal, bytesChangedFinal)
		t.Fail()
	}
}

func propsEvict(t *testing.T, bucket string, objMap map[string]string, msgbytes []byte) {
	// generate a object list to evict (evict 1/3 of total objects - random selection)
	toEvict := len(objMap) / 3
	if toEvict == 0 {
		toEvict = 1
	}
	toEvictList := make([]string, 0, toEvict)
	evictMap := make(map[string]bool, toEvict)
	tlogf("Evicting %v objects...\n", toEvict)

	tlogf("Objects to evict:\n")
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

	reslist := testListBucket(t, bucket, msgbytes)
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
				t.Errorf("Evicted object %s/%s still marked as cached one", bucket, m.Name)
				t.Fail()
			}
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

func propsRecacheObjects(t *testing.T, bucket string, objs map[string]string, msgbytes []byte) {
	tlogf("Recaching objects...\n")
	propsReadObjects(t, bucket, objs)
	tlogf("Checking objects properties after rereading objects...\n")
	reslist := testListBucket(t, bucket, msgbytes)
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

		if m.Version == "" {
			t.Error("Failed to read object %s/%s version", bucket, m.Name)
			t.Fail()
		} else if version != m.Version {
			t.Error("Object %s/%s versions mismath: old[%s] = new[%s]", bucket, m.Name, version, m.Version)
			t.Fail()
		}
	}

}

func Test_objprops(t *testing.T) {
	var (
		oldChkVersion bool
		chkVersion    = true
		filesput      = make(chan string, 10)
		fileslist     = make(map[string]string, 10)
		errch         = make(chan error, 15)
		filesize      = uint64(1024 * 1024)
		numPuts       = 10
		bucket        = clibucket
		versionDir    = "versionid"
		sgl           *dfc.SGLIO
		err           error
	)

	parse()
	if usingSG {
		sgl = dfc.NewSGLIO(filesize)
		defer sgl.Free()
	}

	// Skip the test when given a local bucket
	server, err := client.HeadBucket(proxyurl, clibucket)
	if err != nil {
		t.Errorf("Could not execute HeadBucket Request: %v", err)
		return
	}
	if server == "dfc" {
		t.Skipf("Version is unavailable for local bucket %s", clibucket)
	}

	config := getConfig(proxyurl+"/v1/daemon", httpclient, t)
	versionCfg := config["version_config"].(map[string]interface{})
	oldChkVersion = versionCfg["validate_warm_get"].(bool)
	if oldChkVersion != chkVersion {
		setConfig("validate_warm_get", fmt.Sprintf("%v", chkVersion), proxyurl+"/v1/cluster", httpclient, t)
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
	tlogf("Created objects:\n")
	for fname, _ := range fileslist {
		tlogf("    %s\n", fname)
	}

	// Read object versions
	msg := &dfc.GetMsg{
		GetPrefix: versionDir,
		GetProps:  dfc.GetPropsVersion + ", " + dfc.GetPropsIsCached + ", " + dfc.GetPropsAtime,
	}
	jsbytes, err := json.Marshal(msg)
	if err != nil {
		t.Errorf("Unexpected json-marshal failure, err: %v", err)
		t.Fail()
		return
	}
	reslist := testListBucket(t, bucket, jsbytes)
	if reslist == nil {
		t.Errorf("Unexpected erorr: no object in the bucket %s", bucket)
		t.Fail()
		return
	}

	for _, m := range reslist.Entries {
		if _, ok := fileslist[m.Name]; !ok {
			continue
		}
		tlogf("Intial version %s - %v\n", m.Name, m.Version)
		if m.Version == "" {
			if server == "aws" {
				t.Error("AWS bucket has versioning disabled")
				t.FailNow()
			} else if server == "gcp" {
				t.Error("GCP failed to read object version")
				t.FailNow()
			}
		} else {
			fileslist[m.Name] = m.Version
		}

		if !m.IsCached {
			t.Errorf("Object %s/%s is not marked as cached one", bucket, m.Name)
		}

		if m.Atime == "" {
			t.Errorf("Object %s/%s access time is empty", bucket, m.Name)
		}
	}

	// rewrite objects and compare versions - they should change
	newVersions := propsUpdateObjects(t, bucket, fileslist, jsbytes)

	// check that files are read from cache
	propsReadObjects(t, bucket, fileslist)

	// try to evict some files and check if they are gone
	propsEvict(t, bucket, newVersions, jsbytes)

	// read objects to put them to the cache. After that all objects must have iscached=true
	propsRecacheObjects(t, bucket, newVersions, jsbytes)

	// restore configuration
	if oldChkVersion != chkVersion {
		setConfig("validate_warm_get", fmt.Sprintf("%v", oldChkVersion), proxyurl+"/v1/cluster", httpclient, t)
	}
}
