// Package ais_test contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais_test

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/containers"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

type Test struct {
	name   string
	method func(*testing.T)
}

type regressionTestData struct {
	bck        cmn.Bck
	renamedBck cmn.Bck
	numBuckets int
	rename     bool
	wait       bool
}

const (
	rootDir = "/tmp/ais"

	ListRangeStr   = "__listrange"
	TestBucketName = "TESTAISBUCKET"
)

var (
	HighWaterMark    = int32(80)
	LowWaterMark     = int32(60)
	UpdTime          = time.Second * 20
	configRegression = map[string]string{
		"periodic.stats_time":   fmt.Sprintf("%v", UpdTime),
		"lru.enabled":           "true",
		"lru.lowwm":             fmt.Sprintf("%d", LowWaterMark),
		"lru.highwm":            fmt.Sprintf("%d", HighWaterMark),
		"lru.capacity_upd_time": fmt.Sprintf("%v", UpdTime),
		"lru.dont_evict_time":   fmt.Sprintf("%v", UpdTime),
	}
)

func TestLocalListBucketGetTargetURL(t *testing.T) {
	const (
		num      = 1000
		filesize = 1024
	)
	var (
		filenameCh = make(chan string, num)
		errCh      = make(chan error, num)
		targets    = make(map[string]struct{})
		bck        = cmn.Bck{
			Name:     TestBucketName,
			Provider: cmn.ProviderAIS,
		}
		proxyURL   = tutils.GetPrimaryURL()
		baseParams = tutils.DefaultBaseAPIParams(t)
	)
	smap := tutils.GetClusterMap(t, proxyURL)
	if smap.CountTargets() == 1 {
		tutils.Logln("Warning: more than 1 target should deployed for best utility of this test.")
	}
	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	tutils.PutRandObjs(proxyURL, bck, SmokeStr, filesize, num, errCh, filenameCh, true)
	tassert.SelectErr(t, errCh, "put", true)
	close(filenameCh)
	close(errCh)

	msg := &cmn.SelectMsg{PageSize: int(pagesize), Props: cmn.GetTargetURL}
	bl, err := api.ListBucket(baseParams, bck, msg, num)
	tassert.CheckFatal(t, err)

	if len(bl.Entries) != num {
		t.Errorf("Expected %d bucket list entries, found %d\n", num, len(bl.Entries))
	}

	for _, e := range bl.Entries {
		if e.TargetURL == "" {
			t.Error("Target URL in response is empty")
		}
		if _, ok := targets[e.TargetURL]; !ok {
			targets[e.TargetURL] = struct{}{}
		}
		baseParams := tutils.BaseAPIParams(e.TargetURL)
		l, err := api.GetObject(baseParams, bck, e.Name)
		tassert.CheckFatal(t, err)
		if uint64(l) != filesize {
			t.Errorf("Expected filesize: %d, actual filesize: %d\n", filesize, l)
		}
	}

	if smap.CountTargets() != len(targets) { // The objects should have been distributed to all targets
		t.Errorf("Expected %d different target URLs, actual: %d different target URLs", smap.CountTargets(), len(targets))
	}

	// Ensure no target URLs are returned when the property is not requested
	msg.Props = ""
	bl, err = api.ListBucket(baseParams, bck, msg, num)
	tassert.CheckFatal(t, err)

	if len(bl.Entries) != num {
		t.Errorf("Expected %d bucket list entries, found %d\n", num, len(bl.Entries))
	}

	for _, e := range bl.Entries {
		if e.TargetURL != "" {
			t.Fatalf("Target URL: %s returned when empty target URL expected\n", e.TargetURL)
		}
	}
}

func TestCloudListBucketGetTargetURL(t *testing.T) {
	const (
		numberOfFiles = 100
		fileSize      = 1024
	)

	var (
		fileNameCh = make(chan string, numberOfFiles)
		errCh      = make(chan error, numberOfFiles)
		targets    = make(map[string]struct{})
		bck        = cmn.Bck{
			Name:     clibucket,
			Provider: cmn.Cloud,
		}
		proxyURL   = tutils.GetPrimaryURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		prefix     = tutils.GenRandomString(32)
	)

	if !isCloudBucket(t, proxyURL, bck) {
		t.Skipf("%s requires a cloud bucket", t.Name())
	}
	smap := tutils.GetClusterMap(t, proxyURL)
	if smap.CountTargets() == 1 {
		tutils.Logln("Warning: more than 1 target should deployed for best utility of this test.")
	}

	tutils.PutRandObjs(proxyURL, bck, prefix, fileSize, numberOfFiles, errCh, fileNameCh, true)
	tassert.SelectErr(t, errCh, "put", true)
	close(fileNameCh)
	close(errCh)
	defer func() {
		files := make([]string, numberOfFiles)
		for i := 0; i < numberOfFiles; i++ {
			files[i] = path.Join(prefix, <-fileNameCh)
		}
		err := api.DeleteList(baseParams, bck, files, true, 0)
		if err != nil {
			t.Errorf("Failed to delete objects from bucket %s, err: %v", bck, err)
		}
	}()

	listBucketMsg := &cmn.SelectMsg{Prefix: prefix, PageSize: int(pagesize), Props: cmn.GetTargetURL}
	bucketList, err := api.ListBucket(baseParams, bck, listBucketMsg, 0)
	tassert.CheckFatal(t, err)

	if len(bucketList.Entries) != numberOfFiles {
		t.Errorf("Number of entries in bucket list [%d] must be equal to [%d].\n",
			len(bucketList.Entries), numberOfFiles)
	}

	for _, object := range bucketList.Entries {
		if object.TargetURL == "" {
			t.Errorf("Target URL in response is empty for object [%s]", object.Name)
		}
		if _, ok := targets[object.TargetURL]; !ok {
			targets[object.TargetURL] = struct{}{}
		}
		baseParams := tutils.BaseAPIParams(object.TargetURL)
		objectSize, err := api.GetObject(baseParams, bck, object.Name)
		tassert.CheckFatal(t, err)
		if uint64(objectSize) != fileSize {
			t.Errorf("Expected fileSize: %d, actual fileSize: %d\n", fileSize, objectSize)
		}
	}

	// The objects should have been distributed to all targets
	if smap.CountTargets() != len(targets) {
		t.Errorf("Expected %d different target URLs, actual: %d different target URLs",
			smap.CountTargets(), len(targets))
	}

	// Ensure no target URLs are returned when the property is not requested
	listBucketMsg.Props = ""
	bucketList, err = api.ListBucket(baseParams, bck, listBucketMsg, 0)
	tassert.CheckFatal(t, err)

	if len(bucketList.Entries) != numberOfFiles {
		t.Errorf("Expected %d bucket list entries, found %d\n", numberOfFiles, len(bucketList.Entries))
	}

	for _, object := range bucketList.Entries {
		if object.TargetURL != "" {
			t.Fatalf("Target URL: %s returned when empty target URL expected\n", object.TargetURL)
		}
	}
}

// 1. PUT file
// 2. Corrupt the file
// 3. GET file
func TestGetCorruptFileAfterPut(t *testing.T) {
	const filesize = 1024
	var (
		num        = 2
		filenameCh = make(chan string, num)
		errCh      = make(chan error, 100)
		bck        = cmn.Bck{
			Name:     TestBucketName,
			Provider: cmn.ProviderAIS,
		}
		proxyURL   = tutils.GetPrimaryURL()
		baseParams = tutils.DefaultBaseAPIParams(t)
	)
	if containers.DockerRunning() {
		t.Skip(fmt.Sprintf("%q requires setting Xattrs, doesn't work with docker", t.Name()))
	}

	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	tutils.PutRandObjs(proxyURL, bck, SmokeStr, filesize, num, errCh, filenameCh)
	tassert.SelectErr(t, errCh, "put", false)
	close(filenameCh)
	close(errCh)

	// Test corrupting the file contents
	// Note: The following tests can only work when running on a local setup(targets are co-located with
	//       where this test is running from, because it searches a local file system)
	objName := <-filenameCh
	fqn := findObjOnDisk(bck, objName)
	tutils.Logf("Corrupting file data[%s]: %s\n", objName, fqn)
	err := ioutil.WriteFile(fqn, []byte("this file has been corrupted"), 0644)
	tassert.CheckFatal(t, err)
	_, err = api.GetObjectWithValidation(baseParams, bck, path.Join(SmokeStr, objName))
	if err == nil {
		t.Error("Error is nil, expected non-nil error on a a GET for an object with corrupted contents")
	}
}

func TestRegressionBuckets(t *testing.T) {
	var (
		bck = cmn.Bck{
			Name:     TestBucketName,
			Provider: cmn.ProviderAIS,
		}
		proxyURL = tutils.GetPrimaryURL()
	)
	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)
	doBucketRegressionTest(t, proxyURL, regressionTestData{bck: bck})
}

func TestRenameBucket(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		bck = cmn.Bck{
			Name:     TestBucketName,
			Provider: cmn.ProviderAIS,
		}
		proxyURL   = tutils.GetPrimaryURL()
		baseParams = tutils.DefaultBaseAPIParams(t)
		guid       = cmn.GenUUID()
		renamedBck = cmn.Bck{
			Name:     bck.Name + "_" + guid,
			Provider: cmn.ProviderAIS,
		}
	)
	for _, wait := range []bool{true, false} {
		t.Run(fmt.Sprintf("wait=%v", wait), func(t *testing.T) {
			tutils.CreateFreshBucket(t, proxyURL, bck)
			tutils.DestroyBucket(t, proxyURL, renamedBck) // cleanup post Ctrl-C etc.
			defer tutils.DestroyBucket(t, proxyURL, bck)
			defer tutils.DestroyBucket(t, proxyURL, renamedBck)

			b, err := api.GetBucketNames(baseParams, bck)
			tassert.CheckFatal(t, err)

			doBucketRegressionTest(t, proxyURL, regressionTestData{
				bck: bck, renamedBck: renamedBck, numBuckets: len(b.AIS), rename: true, wait: wait,
			})
		})
	}
}

//
// doBucketRe*
//

func doBucketRegressionTest(t *testing.T, proxyURL string, rtd regressionTestData) {
	const filesize = 1024
	var (
		numPuts    = 2036
		filesPutCh = make(chan string, numPuts)
		errCh      = make(chan error, numPuts)
		bck        = rtd.bck
	)

	tutils.PutRandObjs(proxyURL, rtd.bck, SmokeStr, filesize, numPuts, errCh, filesPutCh)
	close(filesPutCh)
	filesPut := make([]string, 0, len(filesPutCh))
	for fname := range filesPutCh {
		filesPut = append(filesPut, fname)
	}
	tassert.SelectErr(t, errCh, "put", true)
	if rtd.rename {
		baseParams := tutils.BaseAPIParams(proxyURL)
		err := api.RenameBucket(baseParams, rtd.bck, rtd.renamedBck)
		tassert.CheckFatal(t, err)
		tutils.Logf("Renamed %s(numobjs=%d) => %s\n", rtd.bck, numPuts, rtd.renamedBck)
		if rtd.wait {
			postRenameWaitAndCheck(t, proxyURL, rtd, numPuts, filesPut)
		}
		bck = rtd.renamedBck
	}
	sema := make(chan struct{}, 16)
	del := func() {
		tutils.Logf("Deleting %d objects\n", len(filesPut))
		wg := &sync.WaitGroup{}
		for _, fname := range filesPut {
			wg.Add(1)
			sema <- struct{}{}
			go func(fn string) {
				tutils.Del(proxyURL, bck, "smoke/"+fn, wg, errCh, true /* silent */)
				<-sema
			}(fname)
		}
		wg.Wait()
		tassert.SelectErr(t, errCh, "delete", abortonerr)
		close(errCh)
	}
	getRandomFiles(proxyURL, bck, numPuts, SmokeStr+"/", t, errCh)
	tassert.SelectErr(t, errCh, "get", false)
	if !rtd.rename || rtd.wait {
		del()
	} else {
		postRenameWaitAndCheck(t, proxyURL, rtd, numPuts, filesPut)
		del()
	}
}

func postRenameWaitAndCheck(t *testing.T, proxyURL string, rtd regressionTestData, numPuts int, filesPutCh []string) {
	baseParams := tutils.BaseAPIParams(proxyURL)
	tutils.WaitForBucketXactionToComplete(t, baseParams, rtd.bck, cmn.ActRenameLB /*kind*/, rebalanceTimeout)
	tutils.Logf("xaction (rename %s=>%s) done\n", rtd.bck, rtd.renamedBck)

	buckets, err := api.GetBucketNames(baseParams, rtd.bck)
	tassert.CheckFatal(t, err)

	if len(buckets.AIS) != rtd.numBuckets {
		t.Fatalf("wrong number of ais buckets (names) before and after rename (before: %d. after: %+v)",
			rtd.numBuckets, buckets.AIS)
	}

	renamedBucketExists := false
	for _, b := range buckets.AIS {
		if b == rtd.renamedBck.Name {
			renamedBucketExists = true
		} else if b == rtd.bck.Name {
			t.Fatalf("original ais bucket %s still exists after rename", rtd.bck)
		}
	}

	if !renamedBucketExists {
		t.Fatalf("renamed ais bucket %s does not exist after rename", rtd.renamedBck)
	}

	bckList, err := api.ListBucket(baseParams, rtd.renamedBck, &cmn.SelectMsg{}, 0)
	tassert.CheckFatal(t, err)
	unique := make(map[string]bool)
	for _, e := range bckList.Entries {
		base := filepath.Base(e.Name)
		unique[base] = true
	}
	if len(unique) != numPuts {
		for _, name := range filesPutCh {
			if _, ok := unique[name]; !ok {
				tutils.Logf("not found: %s\n", name)
			}
		}
		t.Fatalf("wrong number of objects in the bucket %s renamed as %s (before: %d. after: %d)",
			rtd.bck, rtd.renamedBck, numPuts, len(unique))
	}
}

func TestRenameObjects(t *testing.T) {
	var (
		renameStr    = "rename"
		numPuts      = 1000
		objsPutCh    = make(chan string, numPuts)
		errCh        = make(chan error, 2*numPuts)
		newBaseNames = make([]string, 0, numPuts) // new basenames
		proxyURL     = tutils.GetPrimaryURL()
		baseParams   = tutils.DefaultBaseAPIParams(t)
		bck          = cmn.Bck{
			Name:     t.Name(),
			Provider: cmn.ProviderAIS,
		}
	)

	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	tutils.PutRandObjs(proxyURL, bck, "", 0, numPuts, errCh, objsPutCh)
	tassert.SelectErr(t, errCh, "put", false)
	close(objsPutCh)
	i := 0
	for objName := range objsPutCh {
		newObjName := path.Join(renameStr, objName) + ".renamed" // objname fqn
		newBaseNames = append(newBaseNames, newObjName)
		if err := api.RenameObject(baseParams, bck, objName, newObjName); err != nil {
			t.Fatalf("Failed to rename object from %s => %s, err: %v", objName, newObjName, err)
		}
		i++
		if i%50 == 0 {
			tutils.Logf("Renamed %s => %s\n", objName, newObjName)
		}
	}

	// get renamed objects
	for _, newObjName := range newBaseNames {
		_, err := api.GetObject(baseParams, bck, newObjName)
		if err != nil {
			errCh <- err
		}
	}
	tassert.SelectErr(t, errCh, "get", false)
}

func TestObjectPrefix(t *testing.T) {
	var (
		proxyURL = tutils.GetPrimaryURL()
		bck      = cmn.Bck{Name: clibucket}
	)
	if created := createBucketIfNotExists(t, proxyURL, bck); created {
		defer tutils.DestroyBucket(t, proxyURL, bck)
	}

	prefixFileNumber = numfiles
	prefixCreateFiles(t, proxyURL)
	prefixLookup(t, proxyURL)
	prefixCleanup(t, proxyURL)
}

func TestObjectsVersions(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	propsMainTest(t, true /*versioning enabled*/)
}

func TestReregisterMultipleTargets(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		filesSentOrig = make(map[string]int64)
		filesRecvOrig = make(map[string]int64)
		bytesSentOrig = make(map[string]int64)
		bytesRecvOrig = make(map[string]int64)
		filesSent     int64
		filesRecv     int64
		bytesSent     int64
		bytesRecv     int64
	)

	m := ioContext{
		t:   t,
		num: 10000,
	}
	m.saveClusterState()

	if m.originalTargetCount < 2 {
		t.Fatalf("Must have at least 2 targets in the cluster, have only %d", m.originalTargetCount)
	}
	targetsToUnregister := m.originalTargetCount - 1

	// Step 0: Collect rebalance stats
	clusterStats := tutils.GetClusterStats(t, m.proxyURL)
	for targetID, targetStats := range clusterStats.Target {
		filesSentOrig[targetID] = tutils.GetNamedTargetStats(targetStats, stats.TxRebCount)
		filesRecvOrig[targetID] = tutils.GetNamedTargetStats(targetStats, stats.RxRebCount)
		bytesSentOrig[targetID] = tutils.GetNamedTargetStats(targetStats, stats.TxRebSize)
		bytesRecvOrig[targetID] = tutils.GetNamedTargetStats(targetStats, stats.RxRebSize)
	}

	// Step 1: Unregister multiple targets
	targets := tutils.ExtractTargetNodes(m.smap)
	for i := 0; i < targetsToUnregister; i++ {
		tutils.Logf("Unregistering target %s\n", targets[i].ID())
		err := tutils.UnregisterNode(m.proxyURL, targets[i].ID())
		tassert.CheckFatal(t, err)
	}

	n := tutils.GetClusterMap(t, m.proxyURL).CountTargets()
	if n != m.originalTargetCount-targetsToUnregister {
		t.Fatalf("%d target(s) expected after unregister, actually %d target(s)",
			m.originalTargetCount-targetsToUnregister, n)
	}
	tutils.Logf("The cluster now has %d target(s)\n", n)

	// Step 2: PUT objects into a newly created bucket
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)
	m.puts()

	// Step 3: Start performing GET requests
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.getsUntilStop()
	}()

	// Step 4: Simultaneously reregister each
	wg := &sync.WaitGroup{}
	for i := 0; i < targetsToUnregister; i++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			m.reregisterTarget(targets[r])
		}(i)
		time.Sleep(5 * time.Second) // wait some time before reregistering next target
	}
	wg.Wait()
	tutils.Logf("Stopping GETs...\n")
	m.stopGets()

	m.wg.Wait()

	baseParams := tutils.BaseAPIParams(m.proxyURL)
	tutils.WaitForRebalanceToComplete(t, baseParams, rebalanceTimeout)

	clusterStats = tutils.GetClusterStats(t, m.proxyURL)
	for targetID, targetStats := range clusterStats.Target {
		filesSent += tutils.GetNamedTargetStats(targetStats, stats.TxRebCount) - filesSentOrig[targetID]
		filesRecv += tutils.GetNamedTargetStats(targetStats, stats.RxRebCount) - filesRecvOrig[targetID]
		bytesSent += tutils.GetNamedTargetStats(targetStats, stats.TxRebSize) - bytesSentOrig[targetID]
		bytesRecv += tutils.GetNamedTargetStats(targetStats, stats.RxRebSize) - bytesRecvOrig[targetID]
	}

	// Step 5: Log rebalance stats
	tutils.Logf("Rebalance sent     %s in %d files\n", cmn.B2S(bytesSent, 2), filesSent)
	tutils.Logf("Rebalance received %s in %d files\n", cmn.B2S(bytesRecv, 2), filesRecv)

	m.ensureNoErrors()
	m.assertClusterState()
}

func TestGetClusterStats(t *testing.T) {
	proxyURL := tutils.GetPrimaryURL()
	smap := tutils.GetClusterMap(t, proxyURL)
	stats := tutils.GetClusterStats(t, proxyURL)

	for k, v := range stats.Target {
		tdstats := tutils.GetDaemonStats(t, smap.Tmap[k].PublicNet.DirectURL)
		tdcapstats := tdstats["capacity"].(map[string]interface{})
		dcapstats := v.Capacity
		for fspath, fstats := range dcapstats {
			tfstats := tdcapstats[fspath].(map[string]interface{})
			used, err := strconv.ParseInt(tfstats["used"].(string), 10, 64)
			if err != nil {
				t.Fatalf("Could not decode Target Stats: fstats.Used")
			}
			avail, err := strconv.ParseInt(tfstats["avail"].(string), 10, 64)
			if err != nil {
				t.Fatalf("Could not decode Target Stats: fstats.Avail")
			}
			usedpct, err := tfstats["usedpct"].(json.Number).Int64()
			if err != nil {
				t.Fatalf("Could not decode Target Stats: fstats.Usedpct")
			}
			if used != int64(fstats.Used) || avail != int64(fstats.Avail) || usedpct != int64(fstats.Usedpct) {
				t.Errorf("Stats are different when queried from Target and Proxy: "+
					"Used: %v, %v | Available:  %v, %v | Percentage: %v, %v",
					tfstats["used"], fstats.Used, tfstats["avail"], fstats.Avail, tfstats["usedpct"], fstats.Usedpct)
			}
			if fstats.Usedpct > HighWaterMark {
				t.Error("Used Percentage above High Watermark")
			}
		}
	}
}

func TestConfig(t *testing.T) {
	oconfig := tutils.GetClusterConfig(t)
	olruconfig := oconfig.LRU
	operiodic := oconfig.Periodic

	tutils.SetClusterConfig(t, configRegression)

	nconfig := tutils.GetClusterConfig(t)
	nlruconfig := nconfig.LRU
	nperiodic := nconfig.Periodic

	if nperiodic.StatsTimeStr != configRegression["periodic.stats_time"] {
		t.Errorf("StatsTime was not set properly: %v, should be: %v",
			nperiodic.StatsTimeStr, configRegression["periodic.stats_time"])
	} else {
		o := operiodic.StatsTimeStr
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"periodic.stats_time": o})
	}
	if nlruconfig.DontEvictTimeStr != configRegression["lru.dont_evict_time"] {
		t.Errorf("DontEvictTime was not set properly: %v, should be: %v",
			nlruconfig.DontEvictTimeStr, configRegression["lru.dont_evict_time"])
	} else {
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"lru.dont_evict_time": olruconfig.DontEvictTimeStr})
	}
	if nlruconfig.CapacityUpdTimeStr != configRegression["lru.capacity_upd_time"] {
		t.Errorf("CapacityUpdTime was not set properly: %v, should be: %v",
			nlruconfig.CapacityUpdTimeStr, configRegression["lru.capacity_upd_time"])
	} else {
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"lru.capacity_upd_time": olruconfig.CapacityUpdTimeStr})
	}
	if hw, err := strconv.Atoi(configRegression["lru.highwm"]); err != nil {
		t.Fatalf("Error parsing HighWM: %v", err)
	} else if nlruconfig.HighWM != int64(hw) {
		t.Errorf("HighWatermark was not set properly: %d, should be: %d",
			nlruconfig.HighWM, hw)
	} else {
		oldhwmStr, err := cmn.ConvertToString(olruconfig.HighWM)
		if err != nil {
			t.Fatalf("Error parsing HighWM: %v", err)
		}
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"lru.highwm": oldhwmStr})
	}
	if lw, err := strconv.Atoi(configRegression["lru.lowwm"]); err != nil {
		t.Fatalf("Error parsing LowWM: %v", err)
	} else if nlruconfig.LowWM != int64(lw) {
		t.Errorf("LowWatermark was not set properly: %d, should be: %d",
			nlruconfig.LowWM, lw)
	} else {
		oldlwmStr, err := cmn.ConvertToString(olruconfig.LowWM)
		if err != nil {
			t.Fatalf("Error parsing LowWM: %v", err)
		}
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"lru.lowwm": oldlwmStr})
	}
	if pt, err := cmn.ParseBool(configRegression["lru.enabled"]); err != nil {
		t.Fatalf("Error parsing lru.enabled: %v", err)
	} else if nlruconfig.Enabled != pt {
		t.Errorf("lru.enabled was not set properly: %v, should be %v",
			nlruconfig.Enabled, pt)
	} else {
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"lru.enabled": fmt.Sprintf("%v", olruconfig.Enabled)})
	}
}

func TestLRU(t *testing.T) {
	var (
		proxyURL   = tutils.GetPrimaryURL()
		baseParams = tutils.BaseAPIParams(proxyURL)

		m = &ioContext{
			t: t,
			bck: cmn.Bck{
				Name:     clibucket,
				Provider: cmn.Cloud,
			},
			num: 100,
		}
	)

	if !isCloudBucket(t, proxyURL, m.bck) {
		t.Skipf("%s requires a cloud bucket", t.Name())
	}

	m.saveClusterState()
	m.cloudPuts()
	defer m.cloudDelete()

	// Remember targets' watermarks
	var (
		usedPct      = int32(100)
		cluStats     = tutils.GetClusterStats(t, proxyURL)
		filesEvicted = make(map[string]int64)
		bytesEvicted = make(map[string]int64)
	)

	// Find out min usage % across all targets
	for k, v := range cluStats.Target {
		filesEvicted[k] = tutils.GetNamedTargetStats(v, "lru.evict.n")
		bytesEvicted[k] = tutils.GetNamedTargetStats(v, "lru.evict.size")
		for _, c := range v.Capacity {
			usedPct = cmn.MinI32(usedPct, c.Usedpct)
		}
	}

	var (
		lowWM  = usedPct - 5
		highWM = usedPct - 2
	)
	if int(lowWM) < 2 {
		t.Skipf("The current space usage is too low (%d) for the LRU to be tested", lowWM)
		return
	}

	tutils.Logf("LRU: current min space usage in the cluster: %d%%\n", usedPct)
	tutils.Logf("Setting 'lru.lowm=%d' and 'lru.highwm=%d'\n", lowWM, highWM)

	// All targets: set new watermarks; restore upon exit
	oconfig := tutils.GetClusterConfig(t)
	defer func() {
		lowWMStr, _ := cmn.ConvertToString(oconfig.LRU.LowWM)
		highWMStr, _ := cmn.ConvertToString(oconfig.LRU.HighWM)
		tutils.SetClusterConfig(t, cmn.SimpleKVs{
			"lru.lowwm":             lowWMStr,
			"lru.highwm":            highWMStr,
			"lru.dont_evict_time":   oconfig.LRU.DontEvictTimeStr,
			"lru.capacity_upd_time": oconfig.LRU.CapacityUpdTimeStr,
		})
	}()

	// Cluster-wide reduce dont-evict-time
	lowWMStr, _ := cmn.ConvertToString(lowWM)
	highWMStr, _ := cmn.ConvertToString(highWM)
	tutils.SetClusterConfig(t, cmn.SimpleKVs{
		"lru.lowwm":             lowWMStr,
		"lru.highwm":            highWMStr,
		"lru.dont_evict_time":   "0s",
		"lru.capacity_upd_time": "2s",
	})

	tutils.Logln("starting LRU...")
	err := api.ExecXaction(baseParams, cmn.Bck{}, cmn.ActLRU, cmn.ActXactStart)
	tassert.CheckFatal(t, err)
	tutils.WaitForBucketXactionToStart(t, baseParams, m.bck, cmn.ActLRU)
	tutils.WaitForBucketXactionToComplete(t, baseParams, m.bck, cmn.ActLRU, rebalanceTimeout)

	// Check results
	tutils.Logln("checking the results...")
	cluStats = tutils.GetClusterStats(t, proxyURL)
	for k, v := range cluStats.Target {
		diffFilesEvicted := tutils.GetNamedTargetStats(v, "lru.evict.n") - filesEvicted[k]
		diffBytesEvicted := tutils.GetNamedTargetStats(v, "lru.evict.size") - bytesEvicted[k]
		tutils.Logf(
			"Target %s: evicted %d files - %s (%dB) total\n",
			k, diffFilesEvicted, cmn.B2S(diffBytesEvicted, 2), diffBytesEvicted,
		)

		if diffFilesEvicted == 0 {
			t.Errorf("Target %s: LRU failed to any evict files", k)
		}
	}
}

func TestPrefetchList(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}
	var (
		toprefetch    = make(chan string, numfiles)
		netprefetches = int64(0)
		proxyURL      = tutils.GetPrimaryURL()
		baseParams    = tutils.BaseAPIParams(proxyURL)
		bck           = cmn.Bck{
			Name:     clibucket,
			Provider: cmn.Cloud,
		}
	)

	if !isCloudBucket(t, proxyURL, bck) {
		t.Skipf("Cannot prefetch from ais bucket %s", clibucket)
	}

	// 1. Get initial number of prefetches
	smap := tutils.GetClusterMap(t, proxyURL)
	for _, v := range smap.Tmap {
		stats := tutils.GetDaemonStats(t, v.PublicNet.DirectURL)
		npf, err := getPrefetchCnt(stats)
		if err != nil {
			t.Fatalf("Could not decode target stats: pre.n")
		}
		netprefetches -= npf
	}

	// 2. Get keys to prefetch
	n := int64(getMatchingKeys(t, proxyURL, bck, match, []chan string{toprefetch}, nil))
	close(toprefetch) // to exit for-range
	files := make([]string, 0)
	for i := range toprefetch {
		files = append(files, i)
	}

	// 3. Evict those objects from the cache and prefetch them
	tutils.Logf("Evicting and Prefetching %d objects\n", len(files))
	err := api.EvictList(baseParams, bck, files, true, 0)
	if err != nil {
		t.Error(err)
	}
	err = api.PrefetchList(baseParams, bck, files, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 5. Ensure that all the prefetches occurred.
	for _, v := range smap.Tmap {
		stats := tutils.GetDaemonStats(t, v.PublicNet.DirectURL)
		npf, err := getPrefetchCnt(stats)
		if err != nil {
			t.Fatalf("Could not decode target stats: pre.n")
		}
		netprefetches += npf
	}
	if netprefetches != n {
		t.Errorf("Did not prefetch all files: Missing %d of %d\n", (n - netprefetches), n)
	}
}

// FIXME: stop type-casting and use stats constants, here and elsewhere
func getPrefetchCnt(stats map[string]interface{}) (npf int64, err error) {
	corestats := stats["core"].(map[string]interface{})
	if _, ok := corestats["pre.n"]; !ok {
		return
	}
	npf, err = corestats["pre.n"].(json.Number).Int64()
	return
}

func TestDeleteList(t *testing.T) {
	var (
		err        error
		prefix     = ListRangeStr + "/tstf-"
		wg         = &sync.WaitGroup{}
		errCh      = make(chan error, numfiles)
		files      = make([]string, 0, numfiles)
		bck        = cmn.Bck{Name: clibucket}
		proxyURL   = tutils.GetPrimaryURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
	)
	if created := createBucketIfNotExists(t, proxyURL, bck); created {
		defer tutils.DestroyBucket(t, proxyURL, bck)
	}

	// 1. Put files to delete
	for i := 0; i < numfiles; i++ {
		r, err := tutils.NewRandReader(fileSize, true /* withHash */)
		tassert.CheckFatal(t, err)

		keyname := fmt.Sprintf("%s%d", prefix, i)

		wg.Add(1)
		go tutils.PutAsync(wg, proxyURL, bck, keyname, r, errCh)
		files = append(files, keyname)
	}
	wg.Wait()
	tassert.SelectErr(t, errCh, "put", true)
	tutils.Logf("PUT done.\n")

	// 2. Delete the objects
	err = api.DeleteList(baseParams, bck, files, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 3. Check to see that all the files have been deleted
	msg := &cmn.SelectMsg{Prefix: prefix, PageSize: int(pagesize)}
	bktlst, err := api.ListBucket(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	if len(bktlst.Entries) != 0 {
		t.Errorf("Incorrect number of remaining files: %d, should be 0", len(bktlst.Entries))
	}
}

func TestPrefetchRange(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}
	var (
		netprefetches  = int64(0)
		err            error
		rmin, rmax     int64
		re             *regexp.Regexp
		proxyURL       = tutils.GetPrimaryURL()
		baseParams     = tutils.BaseAPIParams(proxyURL)
		prefetchPrefix = "regressionList/obj"
		prefetchRegex  = "\\d*"
		bck            = cmn.Bck{
			Name:     clibucket,
			Provider: cmn.Cloud,
		}
	)

	if !isCloudBucket(t, proxyURL, bck) {
		t.Skipf("Cannot prefetch from ais bucket %s", clibucket)
	}

	// 1. Get initial number of prefetches
	smap := tutils.GetClusterMap(t, proxyURL)
	for _, v := range smap.Tmap {
		stats := tutils.GetDaemonStats(t, v.PublicNet.DirectURL)
		npf, err := getPrefetchCnt(stats)
		if err != nil {
			t.Fatalf("Could not decode target stats: pre.n")
		}
		netprefetches -= npf
	}

	// 2. Parse arguments
	if prefetchRange != "" {
		ranges := strings.Split(prefetchRange, ":")
		if rmin, err = strconv.ParseInt(ranges[0], 10, 64); err != nil {
			t.Errorf("Error parsing range min: %v", err)
		}
		if rmax, err = strconv.ParseInt(ranges[1], 10, 64); err != nil {
			t.Errorf("Error parsing range max: %v", err)
		}
	}

	// 3. Discover the number of items we expect to be prefetched
	if re, err = regexp.Compile(prefetchRegex); err != nil {
		t.Errorf("Error compiling regex: %v", err)
	}
	msg := &cmn.SelectMsg{Prefix: prefetchPrefix, PageSize: int(pagesize)}
	objsToFilter := testListBucket(t, proxyURL, bck, msg, 0)
	files := make([]string, 0)
	if objsToFilter != nil {
		for _, be := range objsToFilter.Entries {
			if oname := strings.TrimPrefix(be.Name, prefetchPrefix); oname != be.Name {
				s := re.FindStringSubmatch(oname)
				if s == nil {
					continue
				}
				if i, err := strconv.ParseInt(s[0], 10, 64); err != nil && s[0] != "" {
					continue
				} else if s[0] == "" || (rmin == 0 && rmax == 0) || (i >= rmin && i <= rmax) {
					files = append(files, be.Name)
				}
			}
		}
	}

	// 4. Evict those objects from the cache, and then prefetch them
	tutils.Logf("Evicting and Prefetching %d objects\n", len(files))
	err = api.EvictRange(baseParams, bck, prefetchPrefix, prefetchRegex, prefetchRange, true, 0)
	if err != nil {
		t.Error(err)
	}
	err = api.PrefetchRange(baseParams, bck, prefetchPrefix, prefetchRegex, prefetchRange, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 5. Ensure that all the prefetches occurred
	for _, v := range smap.Tmap {
		stats := tutils.GetDaemonStats(t, v.PublicNet.DirectURL)
		npf, err := getPrefetchCnt(stats)
		if err != nil {
			t.Fatalf("Could not decode target stats: pre.n")
		}
		netprefetches += npf
	}
	if netprefetches != int64(len(files)) {
		t.Errorf("Did not prefetch all files: Missing %d of %d\n",
			int64(len(files))-netprefetches, len(files))
	}
}

func TestDeleteRange(t *testing.T) {
	var (
		err            error
		prefix         = ListRangeStr + "/tstf-"
		quarter        = numfiles / 4
		third          = numfiles / 3
		smallrangesize = third - quarter + 1
		smallrange     = fmt.Sprintf("%d:%d", quarter, third)
		bigrange       = fmt.Sprintf("0:%d", numfiles)
		regex          = "\\d?\\d"
		wg             = &sync.WaitGroup{}
		errCh          = make(chan error, numfiles)
		proxyURL       = tutils.GetPrimaryURL()
		baseParams     = tutils.DefaultBaseAPIParams(t)
		bck            = cmn.Bck{Name: clibucket}
	)

	if created := createBucketIfNotExists(t, proxyURL, bck); created {
		defer tutils.DestroyBucket(t, proxyURL, bck)
	}

	// 1. Put files to delete
	for i := 0; i < numfiles; i++ {
		r, err := tutils.NewRandReader(fileSize, true /* withHash */)
		tassert.CheckFatal(t, err)

		wg.Add(1)
		go tutils.PutAsync(wg, proxyURL, bck, fmt.Sprintf("%s%d", prefix, i), r, errCh)
	}
	wg.Wait()
	tassert.SelectErr(t, errCh, "put", true)
	tutils.Logf("PUT done.\n")

	// 2. Delete the small range of objects
	err = api.DeleteRange(baseParams, bck, prefix, regex, smallrange, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 3. Check to see that the correct files have been deleted
	msg := &cmn.SelectMsg{Prefix: prefix, PageSize: int(pagesize)}
	bktlst, err := api.ListBucket(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	if len(bktlst.Entries) != numfiles-smallrangesize {
		t.Errorf("Incorrect number of remaining files: %d, should be %d", len(bktlst.Entries), numfiles-smallrangesize)
	}
	filemap := make(map[string]*cmn.BucketEntry)
	for _, entry := range bktlst.Entries {
		filemap[entry.Name] = entry
	}
	for i := 0; i < numfiles; i++ {
		keyname := fmt.Sprintf("%s%d", prefix, i)
		_, ok := filemap[keyname]
		if ok && i >= quarter && i <= third {
			t.Errorf("File exists that should have been deleted: %s", keyname)
		} else if !ok && (i < quarter || i > third) {
			t.Errorf("File does not exist that should not have been deleted: %s", keyname)
		}
	}

	// 4. Delete the big range of objects
	err = api.DeleteRange(baseParams, bck, prefix, regex, bigrange, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 5. Check to see that all the files have been deleted
	bktlst, err = api.ListBucket(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	if len(bktlst.Entries) != 0 {
		t.Errorf("Incorrect number of remaining files: %d, should be 0", len(bktlst.Entries))
	}
}

// Testing only ais bucket objects since generally not concerned with cloud bucket object deletion
func TestStressDeleteRange(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}
	const (
		numFiles   = 20000 // FIXME: must divide by 10 and by the numReaders
		numReaders = 200
	)
	var (
		err          error
		prefix       = ListRangeStr + "/tstf-"
		wg           = &sync.WaitGroup{}
		errCh        = make(chan error, numFiles)
		proxyURL     = tutils.GetPrimaryURL()
		regex        = "\\d*"
		tenth        = numFiles / 10
		partialRange = fmt.Sprintf("%d:%d", 0, numFiles-tenth-1) // TODO: partial range with non-zero left boundary
		rnge         = fmt.Sprintf("0:%d", numFiles)
		readersList  [numReaders]tutils.Reader
		baseParams   = tutils.DefaultBaseAPIParams(t)
		bck          = cmn.Bck{
			Name:     TestBucketName,
			Provider: cmn.ProviderAIS,
		}
	)

	tutils.CreateFreshBucket(t, proxyURL, bck)

	// 1. PUT
	for i := 0; i < numReaders; i++ {
		random := rand.New(rand.NewSource(int64(i)))
		size := random.Int63n(cmn.KiB*128) + cmn.KiB/3
		tassert.CheckFatal(t, err)
		reader, err := tutils.NewRandReader(size, true /* withHash */)
		tassert.CheckFatal(t, err)
		readersList[i] = reader

		wg.Add(1)
		go func(i int, reader tutils.Reader) {
			defer wg.Done()

			for j := 0; j < numFiles/numReaders; j++ {
				objname := fmt.Sprintf("%s%d", prefix, i*numFiles/numReaders+j)
				putArgs := api.PutObjectArgs{
					BaseParams: baseParams,
					Bck:        bck,
					Object:     objname,
					Hash:       reader.XXHash(),
					Reader:     reader,
				}
				err = api.PutObject(putArgs)
				if err != nil {
					errCh <- err
				}
				reader.Seek(0, io.SeekStart)
			}
			tutils.Progress(i, 99)
		}(i, reader)
	}
	wg.Wait()
	tassert.SelectErr(t, errCh, "put", true)

	// 2. Delete a range of objects
	tutils.Logf("Deleting objects in range: %s\n", partialRange)
	err = api.DeleteRange(tutils.BaseAPIParams(proxyURL), bck, prefix, regex, partialRange, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 3. Check to see that correct objects have been deleted
	expectedRemaining := tenth
	msg := &cmn.SelectMsg{Prefix: prefix, PageSize: int(pagesize)}
	bktlst, err := api.ListBucket(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	if len(bktlst.Entries) != expectedRemaining {
		t.Errorf("Incorrect number of remaining objects: %d, expected: %d", len(bktlst.Entries), expectedRemaining)
	}

	filemap := make(map[string]*cmn.BucketEntry)
	for _, entry := range bktlst.Entries {
		filemap[entry.Name] = entry
	}
	for i := 0; i < numFiles; i++ {
		objname := fmt.Sprintf("%s%d", prefix, i)
		_, ok := filemap[objname]
		if ok && i < numFiles-tenth {
			t.Errorf("%s exists (expected to be deleted)", objname)
		} else if !ok && i >= numFiles-tenth {
			t.Errorf("%s does not exist", objname)
		}
	}

	// 4. Delete the entire range of objects
	tutils.Logf("Deleting objects in range: %s\n", rnge)
	err = api.DeleteRange(tutils.BaseAPIParams(proxyURL), bck, prefix, regex, rnge, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 5. Check to see that all files have been deleted
	msg = &cmn.SelectMsg{Prefix: prefix, PageSize: int(pagesize)}
	bktlst, err = api.ListBucket(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	if len(bktlst.Entries) != 0 {
		t.Errorf("Incorrect number of remaining files: %d, should be 0", len(bktlst.Entries))
	}

	tutils.DestroyBucket(t, proxyURL, bck)
}
