// Package ais_test contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/ais"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/tutils"
	jsoniter "github.com/json-iterator/go"
)

type Test struct {
	name   string
	method func(*testing.T)
}

type regressionTestData struct {
	bucket          string
	renamedBucket   string
	numLocalBuckets int
	rename          bool
}

const (
	rootDir = "/tmp/ais"

	TestLocalBucketName   = "TESTLOCALBUCKET"
	RenameLocalBucketName = "renamebucket"
	RenameDir             = rootDir + "/rename"
	RenameStr             = "rename"
	ListRangeStr          = "__listrange"
)

var (
	HighWaterMark    = uint32(80)
	LowWaterMark     = uint32(60)
	UpdTime          = time.Second * 20
	configRegression = map[string]string{
		"stats_time":        fmt.Sprintf("%v", UpdTime),
		"dont_evict_time":   fmt.Sprintf("%v", UpdTime),
		"capacity_upd_time": fmt.Sprintf("%v", UpdTime),
		"lowwm":             fmt.Sprintf("%d", LowWaterMark),
		"highwm":            fmt.Sprintf("%d", HighWaterMark),
		"lru.enabled":       "true",
	}
)

func TestLocalListBucketGetTargetURL(t *testing.T) {
	const (
		num      = 1000
		filesize = 1024
		bucket   = TestLocalBucketName
	)
	var (
		filenameCh = make(chan string, num)
		errCh      = make(chan error, num)
		sgl        *memsys.SGL
		targets    = make(map[string]struct{})
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
	)
	smap := getClusterMap(t, proxyURL)
	if len(smap.Tmap) == 1 {
		tutils.Logln("Warning: more than 1 target should deployed for best utility of this test.")
	}

	sgl = tutils.Mem2.NewSGL(filesize)
	defer sgl.Free()
	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	tutils.PutRandObjs(proxyURL, bucket, SmokeDir, readerType, SmokeStr, filesize, num, errCh, filenameCh, sgl)
	selectErr(errCh, "put", t, true)
	close(filenameCh)
	close(errCh)

	msg := &cmn.GetMsg{GetPageSize: int(pagesize), GetProps: cmn.GetTargetURL}
	bl, err := api.ListBucket(tutils.DefaultBaseAPIParams(t), bucket, msg, num)
	tutils.CheckFatal(err, t)

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
		l, err := api.GetObject(baseParams, bucket, e.Name)
		tutils.CheckFatal(err, t)
		if uint64(l) != filesize {
			t.Errorf("Expected filesize: %d, actual filesize: %d\n", filesize, l)
		}
	}

	if len(smap.Tmap) != len(targets) { // The objects should have been distributed to all targets
		t.Errorf("Expected %d different target URLs, actual: %d different target URLs", len(smap.Tmap), len(targets))
	}

	// Ensure no target URLs are returned when the property is not requested
	msg.GetProps = ""
	bl, err = api.ListBucket(tutils.DefaultBaseAPIParams(t), bucket, msg, num)
	tutils.CheckFatal(err, t)

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
		sgl        *memsys.SGL
		bucketName = clibucket
		targets    = make(map[string]struct{})
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		random     = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
		prefix     = tutils.FastRandomFilename(random, 32)
	)

	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skipf("%s requires a cloud bucket", t.Name())
	}
	smap := getClusterMap(t, proxyURL)
	if len(smap.Tmap) == 1 {
		tutils.Logln("Warning: more than 1 target should deployed for best utility of this test.")
	}

	sgl = tutils.Mem2.NewSGL(fileSize)
	defer sgl.Free()
	tutils.PutRandObjs(proxyURL, bucketName, SmokeDir, readerType, prefix, fileSize, numberOfFiles, errCh, fileNameCh, sgl)
	selectErr(errCh, "put", t, true)
	close(fileNameCh)
	close(errCh)
	defer func() {
		files := make([]string, numberOfFiles)
		for i := 0; i < numberOfFiles; i++ {
			files[i] = path.Join(prefix, <-fileNameCh)
		}
		err := api.DeleteList(tutils.BaseAPIParams(proxyURL), bucketName, cmn.CloudBs, files, true, 0)
		if err != nil {
			t.Error("Unable to delete files during cleanup from cloud bucket.")
		}
	}()

	listBucketMsg := &cmn.GetMsg{GetPrefix: prefix, GetPageSize: int(pagesize), GetProps: cmn.GetTargetURL}
	bucketList, err := api.ListBucket(tutils.DefaultBaseAPIParams(t), bucketName, listBucketMsg, 0)
	tutils.CheckFatal(err, t)

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
		objectSize, err := api.GetObject(baseParams, bucketName, object.Name)
		tutils.CheckFatal(err, t)
		if uint64(objectSize) != fileSize {
			t.Errorf("Expected fileSize: %d, actual fileSize: %d\n", fileSize, objectSize)
		}
	}

	// The objects should have been distributed to all targets
	if len(smap.Tmap) != len(targets) {
		t.Errorf("Expected %d different target URLs, actual: %d different target URLs",
			len(smap.Tmap), len(targets))
	}

	// Ensure no target URLs are returned when the property is not requested
	listBucketMsg.GetProps = ""
	bucketList, err = api.ListBucket(tutils.DefaultBaseAPIParams(t), bucketName, listBucketMsg, 0)
	tutils.CheckFatal(err, t)

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
		sgl        *memsys.SGL
		fqn        string
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
	)
	if tutils.DockerRunning() {
		t.Skip(fmt.Sprintf("%q requires setting Xattrs, doesn't work with docker", t.Name()))
	}

	bucket := TestLocalBucketName
	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	sgl = tutils.Mem2.NewSGL(filesize)
	defer sgl.Free()
	tutils.PutRandObjs(proxyURL, bucket, SmokeDir, readerType, SmokeStr, filesize, num, errCh, filenameCh, sgl)
	selectErr(errCh, "put", t, false)
	close(filenameCh)
	close(errCh)

	// Test corrupting the file contents
	// Note: The following tests can only work when running on a local setup(targets are co-located with
	//       where this test is running from, because it searches a local file system)
	var fName string
	fsWalkFunc := func(path string, info os.FileInfo, err error) error {
		if info.IsDir() && info.Name() == "cloud" {
			return filepath.SkipDir
		}
		if filepath.Base(path) == fName && strings.Contains(path, bucket) {
			fqn = path
		}
		return nil
	}

	fName = <-filenameCh
	filepath.Walk(rootDir, fsWalkFunc)
	tutils.Logf("Corrupting file data[%s]: %s\n", fName, fqn)
	err := ioutil.WriteFile(fqn, []byte("this file has been corrupted"), 0644)
	tutils.CheckFatal(err, t)
	_, err = api.GetObjectWithValidation(tutils.DefaultBaseAPIParams(t), bucket, path.Join(SmokeStr, fName))
	if err == nil {
		t.Error("Error is nil, expected non-nil error on a a GET for an object with corrupted contents")
	}

	// Test corrupting the file xattr
	fName = <-filenameCh
	filepath.Walk(rootDir, fsWalkFunc)
	tutils.Logf("Corrupting file xattr[%s]: %s\n", fName, fqn)
	if errstr := fs.SetXattr(fqn, cmn.XattrXXHash, []byte("01234abcde")); errstr != "" {
		t.Error(errstr)
	}
	_, err = api.GetObjectWithValidation(tutils.DefaultBaseAPIParams(t), bucket, path.Join(SmokeStr, fName))
	if err == nil {
		t.Error("Error is nil, expected non-nil error on a GET for an object with corrupted xattr")
	}
}

func TestRegressionLocalBuckets(t *testing.T) {
	bucket := TestLocalBucketName
	proxyURL := getPrimaryURL(t, proxyURLReadOnly)
	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)
	doBucketRegressionTest(t, proxyURL, regressionTestData{bucket: bucket})

}

func TestRenameLocalBuckets(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}
	var (
		proxyURL      = getPrimaryURL(t, proxyURLReadOnly)
		bucket        = TestLocalBucketName
		renamedBucket = bucket + "_renamed"
	)

	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	tutils.DestroyLocalBucket(t, proxyURL, renamedBucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, renamedBucket)

	b, err := api.GetBucketNames(tutils.DefaultBaseAPIParams(t), cmn.LocalBs)
	tutils.CheckFatal(err, t)

	doBucketRegressionTest(t, proxyURL, regressionTestData{
		bucket: bucket, renamedBucket: renamedBucket, numLocalBuckets: len(b.Local), rename: true,
	})
}

func TestListObjectsPrefix(t *testing.T) {
	const fileSize = 1024
	var (
		numFiles   = 20
		prefix     = "regressionList"
		bucket     = clibucket
		errCh      = make(chan error, numFiles*5)
		filesPutCh = make(chan string, numfiles)
		dir        = DeleteDir
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
	)

	sgl := tutils.Mem2.NewSGL(fileSize)
	defer sgl.Free()
	tutils.Logf("Create a list of %d objects\n", numFiles)
	if isCloudBucket(t, proxyURL, bucket) {
		tutils.Logf("Cleaning up the cloud bucket %s\n", bucket)
		msg := &cmn.GetMsg{GetPageSize: 1000, GetPrefix: prefix}
		reslist, err := listObjects(t, proxyURL, msg, bucket, 0)
		tutils.CheckFatal(err, t)
		for _, entry := range reslist.Entries {
			err := tutils.Del(proxyURL, bucket, entry.Name, "", nil, nil, false)
			tutils.CheckFatal(err, t)
		}
	} else {
		tutils.Logf("Recreating the local bucket %s\n", bucket)
		tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
		defer tutils.DestroyLocalBucket(t, proxyURL, bucket)
	}
	fileList := make([]string, 0, numFiles)
	for i := 0; i < numFiles; i++ {
		fname := fmt.Sprintf("obj%d", i+1)
		fileList = append(fileList, fname)
	}

	tutils.PutObjsFromList(proxyURL, bucket, dir, readerType, prefix, fileSize, fileList, errCh, filesPutCh, sgl)
	defer func() {
		// cleanup objects created by the test
		for _, fname := range fileList {
			if err := tutils.Del(proxyURL, bucket, prefix+"/"+fname, "", nil, nil, false); err != nil {
				t.Error(err)
			}
		}
	}()

	close(filesPutCh)
	selectErr(errCh, "list - put", t, true /* fatal - if PUT does not work then it makes no sense to continue */)
	close(errCh)
	type testParams struct {
		title    string
		prefix   string
		pageSize int
		limit    int
		expected int
	}
	tests := []testParams{
		{
			"Full list - default pageSize",
			prefix, 0, 0,
			numFiles,
		},
		{
			"Full list - small pageSize - no limit",
			prefix, int(numFiles / 7), 0,
			numFiles,
		},
		{
			"Full list - limited",
			prefix, 0, 8,
			8,
		},
		{
			"Full list - with prefix",
			prefix + "/obj1", 0, 0,
			11, //obj1 and obj10..obj19
		},
		{
			"Full list - with prefix and limit",
			prefix + "/obj1", 0, 2,
			2, //obj1 and obj10
		},
		{
			"Empty list - prefix",
			prefix + "/nothing", 0, 0,
			0,
		},
	}

	for idx, test := range tests {
		tutils.Logf("%d. %s\n    Prefix: [%s], Expected objects: %d\n", idx+1, test.title, test.prefix, test.expected)
		msg := &cmn.GetMsg{GetPageSize: test.pageSize, GetPrefix: test.prefix}
		reslist, err := listObjects(t, proxyURL, msg, bucket, test.limit)
		if err != nil {
			t.Error(err)
			continue
		}
		if len(reslist.Entries) != test.expected {
			t.Errorf("Test %d. %s failed: returned %d objects instead of %d",
				idx+1, test.title, len(reslist.Entries), test.expected)
		}
	}
}

func TestRenameObjects(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}
	var (
		err       error
		numPuts   = 10
		objsPutCh = make(chan string, numPuts)
		errCh     = make(chan error, numPuts)
		basenames = make([]string, 0, numPuts) // basenames
		bnewnames = make([]string, 0, numPuts) // new basenames
		sgl       *memsys.SGL
		proxyURL  = getPrimaryURL(t, proxyURLReadOnly)
	)

	tutils.CreateFreshLocalBucket(t, proxyURL, RenameLocalBucketName)

	defer func() {
		// cleanup
		wg := &sync.WaitGroup{}
		for _, newObj := range bnewnames {
			wg.Add(1)
			go tutils.Del(proxyURL, RenameLocalBucketName, newObj, "", wg, errCh, !testing.Verbose())
		}

		wg.Wait()
		selectErr(errCh, "delete", t, false)
		close(errCh)
		tutils.DestroyLocalBucket(t, proxyURL, RenameLocalBucketName)
	}()

	time.Sleep(time.Second * 5)

	if err = cmn.CreateDir(RenameDir); err != nil {
		t.Errorf("Error creating dir: %v", err)
	}

	sgl = tutils.Mem2.NewSGL(1024 * 1024)
	defer sgl.Free()
	tutils.PutRandObjs(proxyURL, RenameLocalBucketName, RenameDir, readerType, RenameStr, 0, numPuts, errCh, objsPutCh, sgl)
	selectErr(errCh, "put", t, false)
	close(objsPutCh)
	for fname := range objsPutCh {
		basenames = append(basenames, fname)
	}

	// rename
	for _, fname := range basenames {
		oldObj := path.Join(RenameStr, fname)
		newObj := oldObj + ".renamed" // objname fqn
		bnewnames = append(bnewnames, newObj)
		if err := api.RenameObject(tutils.DefaultBaseAPIParams(t), RenameLocalBucketName, oldObj, newObj); err != nil {
			t.Fatalf("Failed to rename object from %s => %s, err: %v", oldObj, newObj, err)
		}
		tutils.Logln(fmt.Sprintf("Rename %s => %s successful", oldObj, newObj))
	}

	// get renamed objects
	waitProgressBar("Rename/move: ", time.Second*5)
	for _, newObj := range bnewnames {
		_, err := api.GetObject(tutils.DefaultBaseAPIParams(t), RenameLocalBucketName, newObj)
		if err != nil {
			errCh <- err
		}
	}
	selectErr(errCh, "get", t, false)
}

func TestObjectPrefix(t *testing.T) {
	proxyURL := getPrimaryURL(t, proxyURLReadOnly)
	if created := createLocalBucketIfNotExists(t, proxyURL, clibucket); created {
		defer tutils.DestroyLocalBucket(t, proxyURL, clibucket)
	}

	prefixFileNumber = numfiles
	prefixCreateFiles(t, proxyURL)
	prefixLookup(t, proxyURL)
	prefixCleanup(t, proxyURL)
}

func TestObjectsVersions(t *testing.T) {
	propsMainTest(t, cmn.VersionAll)
}

func TestRebalance(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}
	const filesize = 1024 * 128
	var (
		randomTarget *cluster.Snode
		numPuts      = 40
		filesPutCh   = make(chan string, numPuts)
		errCh        = make(chan error, 100)
		wg           = &sync.WaitGroup{}
		sgl          *memsys.SGL
		proxyURL     = getPrimaryURL(t, proxyURLReadOnly)
	)
	filesSentOrig := make(map[string]int64)
	bytesSentOrig := make(map[string]int64)
	filesRecvOrig := make(map[string]int64)
	bytesRecvOrig := make(map[string]int64)
	stats := getClusterStats(t, proxyURL)
	for k, v := range stats.Target { // FIXME: stats names => API package
		bytesSentOrig[k], filesSentOrig[k], bytesRecvOrig[k], filesRecvOrig[k] =
			getNamedTargetStats(v, "tx.size"), getNamedTargetStats(v, "tx.n"),
			getNamedTargetStats(v, "rx.size"), getNamedTargetStats(v, "rx.n")
	}

	//
	// step 1. config
	//
	if created := createLocalBucketIfNotExists(t, proxyURL, clibucket); created {
		defer tutils.DestroyLocalBucket(t, proxyURL, clibucket)
	}

	//
	// cluster-wide reduce startup_delay_time
	//
	waitProgressBar("Rebalance: ", time.Second*10)

	//
	// step 2. unregister random target
	//
	smap := getClusterMap(t, proxyURL)
	l := len(smap.Tmap)
	if l < 2 {
		t.Fatalf("Must have 2 or more targets in the cluster, have only %d", l)
	}
	randomTarget = extractTargetNodes(smap)[0]

	err := tutils.UnregisterTarget(proxyURL, randomTarget.DaemonID)
	tutils.CheckFatal(err, t)
	tutils.Logf("Unregistered %s: cluster size = %d (targets)\n", randomTarget.DaemonID, l-1)
	//
	// step 3. put random files => (cluster - 1)
	//
	sgl = tutils.Mem2.NewSGL(filesize)
	defer sgl.Free()
	tutils.PutRandObjs(proxyURL, clibucket, SmokeDir, readerType, SmokeStr, filesize, numPuts, errCh, filesPutCh, sgl)
	selectErr(errCh, "put", t, false)

	//
	// step 4. register back
	//
	err = tutils.RegisterTarget(proxyURL, randomTarget, smap)
	tutils.CheckFatal(err, t)
	for i := 0; i < 25; i++ {
		time.Sleep(time.Second)
		smap = getClusterMap(t, proxyURL)
		if len(smap.Tmap) == l {
			break
		}
	}
	if len(smap.Tmap) != l {
		t.Errorf("Re-registration timed out: target %s, original num targets %d\n", randomTarget.DaemonID, l)
		return
	}
	tutils.Logf("Re-registered %s: the cluster is now back to %d targets\n", randomTarget.DaemonID, l)
	//
	// step 5. wait for rebalance to run its course
	//
	waitForRebalanceToComplete(t, proxyURL)
	//
	// step 6. statistics
	//
	stats = getClusterStats(t, proxyURL)
	var bsent, fsent, brecv, frecv int64
	for k, v := range stats.Target { // FIXME: stats names => API package
		bsent += getNamedTargetStats(v, "tx.size") - bytesSentOrig[k]
		fsent += getNamedTargetStats(v, "tx.n") - filesSentOrig[k]
		brecv += getNamedTargetStats(v, "rx.size") - bytesRecvOrig[k]
		frecv += getNamedTargetStats(v, "rx.n") - filesRecvOrig[k]
	}

	//
	// step 7. cleanup
	//
	close(filesPutCh) // to exit for-range
	for fname := range filesPutCh {
		wg.Add(1)
		go tutils.Del(proxyURL, clibucket, "smoke/"+fname, "", wg, errCh, !testing.Verbose())
	}
	wg.Wait()
	selectErr(errCh, "delete", t, abortonerr)
	close(errCh)
	if !t.Failed() && testing.Verbose() {
		tutils.Logf("Rebalance: sent     %.2f MB in %d files\n", float64(bsent)/1000/1000, fsent)
		tutils.Logf("           received %.2f MB in %d files\n", float64(brecv)/1000/1000, frecv)
	}
}

func TestGetClusterStats(t *testing.T) {
	proxyURL := getPrimaryURL(t, proxyURLReadOnly)
	smap := getClusterMap(t, proxyURL)
	stats := getClusterStats(t, proxyURL)

	for k, v := range stats.Target {
		tdstats := getDaemonStats(t, smap.Tmap[k].PublicNet.DirectURL)
		tdcapstats := tdstats["capacity"].(map[string]interface{})
		dcapstats := v.Capacity
		for fspath, fstats := range dcapstats {
			tfstats := tdcapstats[fspath].(map[string]interface{})
			used, err := tfstats["used"].(json.Number).Int64()
			if err != nil {
				t.Fatalf("Could not decode Target Stats: fstats.Used")
			}
			avail, err := tfstats["avail"].(json.Number).Int64()
			if err != nil {
				t.Fatalf("Could not decode Target Stats: fstats.Avail")
			}
			usedpct, err := tfstats["usedpct"].(json.Number).Int64()
			if err != nil {
				t.Fatalf("Could not decode Target Stats: fstats.Usedpct")
			}
			if used != int64(fstats.Used) || avail != int64(fstats.Avail) || usedpct != fstats.Usedpct {
				t.Errorf("Stats are different when queried from Target and Proxy: "+
					"Used: %v, %v | Available:  %v, %v | Percentage: %v, %v",
					tfstats["used"], fstats.Used, tfstats["avail"], fstats.Avail, tfstats["usedpct"], fstats.Usedpct)
			}
			if fstats.Usedpct > int64(HighWaterMark) {
				t.Error("Used Percentage above High Watermark")
			}
		}
	}
}

func TestConfig(t *testing.T) {
	proxyURL := getPrimaryURL(t, proxyURLReadOnly)
	oconfig := getDaemonConfig(t, proxyURL)
	olruconfig := oconfig.LRU
	operiodic := oconfig.Periodic

	for k, v := range configRegression {
		setClusterConfig(t, proxyURL, k, v)
	}

	nconfig := getDaemonConfig(t, proxyURL)
	nlruconfig := nconfig.LRU
	nperiodic := nconfig.Periodic

	if nperiodic.StatsTimeStr != configRegression["stats_time"] {
		t.Errorf("StatsTime was not set properly: %v, should be: %v",
			nperiodic.StatsTimeStr, configRegression["stats_time"])
	} else {
		o := operiodic.StatsTimeStr
		setClusterConfig(t, proxyURL, "stats_time", o)
	}
	if nlruconfig.DontEvictTimeStr != configRegression["dont_evict_time"] {
		t.Errorf("DontEvictTime was not set properly: %v, should be: %v",
			nlruconfig.DontEvictTimeStr, configRegression["dont_evict_time"])
	} else {
		setClusterConfig(t, proxyURL, "dont_evict_time", olruconfig.DontEvictTimeStr)
	}
	if nlruconfig.CapacityUpdTimeStr != configRegression["capacity_upd_time"] {
		t.Errorf("CapacityUpdTime was not set properly: %v, should be: %v",
			nlruconfig.CapacityUpdTimeStr, configRegression["capacity_upd_time"])
	} else {
		setClusterConfig(t, proxyURL, "capacity_upd_time", olruconfig.CapacityUpdTimeStr)
	}
	if hw, err := strconv.Atoi(configRegression["highwm"]); err != nil {
		t.Fatalf("Error parsing HighWM: %v", err)
	} else if nlruconfig.HighWM != int64(hw) {
		t.Errorf("HighWatermark was not set properly: %d, should be: %d",
			nlruconfig.HighWM, hw)
	} else {
		setClusterConfig(t, proxyURL, "highwm", olruconfig.HighWM)
	}
	if lw, err := strconv.Atoi(configRegression["lowwm"]); err != nil {
		t.Fatalf("Error parsing LowWM: %v", err)
	} else if nlruconfig.LowWM != int64(lw) {
		t.Errorf("LowWatermark was not set properly: %d, should be: %d",
			nlruconfig.LowWM, lw)
	} else {
		setClusterConfig(t, proxyURL, "lowwm", olruconfig.LowWM)
	}
	if pt, err := strconv.ParseBool(configRegression["lru.enabled"]); err != nil {
		t.Fatalf("Error parsing lru.enabled: %v", err)
	} else if nlruconfig.Enabled != pt {
		t.Errorf("lru.enabled was not set properly: %v, should be %v",
			nlruconfig.Enabled, pt)
	} else {
		setClusterConfig(t, proxyURL, "lru.enabled", olruconfig.Enabled)
	}
}

func TestLRU(t *testing.T) {
	var (
		errCh    = make(chan error, 100)
		usedpct  = 100
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
	)
	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skipf("%s test requires a cloud bucket", t.Name())
	}

	getRandomFiles(proxyURL, 20, clibucket, "", t, nil, errCh)
	// The error could be no object in the bucket. In that case, consider it as not an error;
	// this test will be skipped
	if len(errCh) != 0 {
		t.Logf("LRU: need a cloud bucket with at least 20 objects")
		t.Skip("skipping - cannot test LRU.")
	}

	//
	// remember targets' watermarks
	//
	smap := getClusterMap(t, proxyURL)
	lwms := make(map[string]interface{})
	hwms := make(map[string]interface{})
	bytesEvictedOrig := make(map[string]int64)
	filesEvictedOrig := make(map[string]int64)
	for k, di := range smap.Tmap {
		cfg := getDaemonConfig(t, di.URL(cmn.NetworkPublic))
		lwms[k] = cfg.LRU.LowWM
		hwms[k] = cfg.LRU.HighWM
	}
	// add a few more
	getRandomFiles(proxyURL, 3, clibucket, "", t, nil, errCh)
	selectErr(errCh, "get", t, true)
	//
	// find out min usage %% across all targets
	//
	stats := getClusterStats(t, proxyURL)
	for k, v := range stats.Target {
		bytesEvictedOrig[k], filesEvictedOrig[k] = getNamedTargetStats(v, "lru.evict.size"), getNamedTargetStats(v, "lru.evict.n")
		for _, c := range v.Capacity {
			usedpct = cmn.Min(usedpct, int(c.Usedpct))
		}
	}
	tutils.Logf("LRU: current min space usage in the cluster: %d%%\n", usedpct)
	var (
		lowwm  = usedpct - 5
		highwm = usedpct - 1
	)
	if int(lowwm) < 3 {
		t.Logf("The current space usage is too low (%d) for the LRU to be tested", lowwm)
		t.Skip("skipping - cannot test LRU.")
		return
	}
	oconfig := getDaemonConfig(t, proxyURL)
	if t.Failed() {
		return
	}
	//
	// all targets: set new watermarks; restore upon exit
	//
	olruconfig := oconfig.LRU
	operiodic := oconfig.Periodic
	defer func() {
		setClusterConfig(t, proxyURL, "dont_evict_time", olruconfig.DontEvictTimeStr)
		setClusterConfig(t, proxyURL, "capacity_upd_time", olruconfig.CapacityUpdTimeStr)
		setClusterConfig(t, proxyURL, "highwm", olruconfig.HighWM)
		setClusterConfig(t, proxyURL, "lowwm", olruconfig.LowWM)
		for k, di := range smap.Tmap {
			setDaemonConfig(t, di.URL(cmn.NetworkPublic), "highwm", hwms[k])
			setDaemonConfig(t, di.URL(cmn.NetworkPublic), "lowwm", lwms[k])
		}
	}()
	//
	// cluster-wide reduce dont-evict-time
	//
	dontevicttimestr := "30s"
	capacityupdtimestr := "5s"
	sleeptime, err := time.ParseDuration(operiodic.StatsTimeStr) // to make sure the stats get updated
	if err != nil {
		t.Fatalf("Failed to parse stats_time: %v", err)
	}
	setClusterConfig(t, proxyURL, "dont_evict_time", dontevicttimestr)
	setClusterConfig(t, proxyURL, "capacity_upd_time", capacityupdtimestr)
	if t.Failed() {
		return
	}
	setClusterConfig(t, proxyURL, "lowwm", lowwm)
	if t.Failed() {
		return
	}
	setClusterConfig(t, proxyURL, "highwm", highwm)
	if t.Failed() {
		return
	}
	waitProgressBar("LRU: ", sleeptime/2)
	getRandomFiles(proxyURL, 1, clibucket, "", t, nil, errCh)
	waitProgressBar("LRU: ", sleeptime/2)
	//
	// results
	//
	stats = getClusterStats(t, proxyURL)
	for k, v := range stats.Target {
		bytes := getNamedTargetStats(v, "lru.evict.size") - bytesEvictedOrig[k]
		tutils.Logf("Target %s: evicted %d files - %.2f MB (%dB) total\n",
			k, getNamedTargetStats(v, "lru.evict.n")-filesEvictedOrig[k], float64(bytes)/1000/1000, bytes)
		//
		// TestingEnv() - cannot reliably verify space utilization by tmpfs
		//
		if oconfig.TestFSP.Count > 0 {
			continue
		}
		for mpath, c := range v.Capacity {
			if c.Usedpct < int64(lowwm-1) || c.Usedpct > int64(lowwm+1) {
				t.Errorf("Target %s failed to reach lwm %d%%: mpath %s, used space %d%%", k, lowwm, mpath, c.Usedpct)
			}
		}
	}
}

func TestPrefetchList(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}
	var (
		toprefetch    = make(chan string, numfiles)
		netprefetches = int64(0)
		proxyURL      = getPrimaryURL(t, proxyURLReadOnly)
		baseParams    = tutils.BaseAPIParams(proxyURL)
	)

	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skipf("Cannot prefetch from local bucket %s", clibucket)
	}

	// 1. Get initial number of prefetches
	smap := getClusterMap(t, proxyURL)
	for _, v := range smap.Tmap {
		stats := getDaemonStats(t, v.PublicNet.DirectURL)
		npf, err := getPrefetchCnt(stats)
		if err != nil {
			t.Fatalf("Could not decode target stats: pre.n")
		}
		netprefetches -= npf
	}

	// 2. Get keys to prefetch
	n := int64(getMatchingKeys(proxyURL, match, clibucket, []chan string{toprefetch}, nil, t))
	close(toprefetch) // to exit for-range
	files := make([]string, 0)
	for i := range toprefetch {
		files = append(files, i)
	}

	// 3. Evict those objects from the cache and prefetch them
	tutils.Logf("Evicting and Prefetching %d objects\n", len(files))
	err := api.EvictList(baseParams, clibucket, cmn.CloudBs, files, true, 0)
	if err != nil {
		t.Error(err)
	}
	err = api.PrefetchList(baseParams, clibucket, cmn.CloudBs, files, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 5. Ensure that all the prefetches occurred.
	for _, v := range smap.Tmap {
		stats := getDaemonStats(t, v.PublicNet.DirectURL)
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
		err      error
		prefix   = ListRangeStr + "/tstf-"
		wg       = &sync.WaitGroup{}
		errCh    = make(chan error, numfiles)
		files    = make([]string, 0, numfiles)
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
	)
	if created := createLocalBucketIfNotExists(t, proxyURL, clibucket); created {
		defer tutils.DestroyLocalBucket(t, proxyURL, clibucket)
	}

	// 1. Put files to delete
	for i := 0; i < numfiles; i++ {
		r, err := tutils.NewRandReader(fileSize, true /* withHash */)
		tutils.CheckFatal(err, t)

		keyname := fmt.Sprintf("%s%d", prefix, i)

		wg.Add(1)
		go tutils.PutAsync(wg, proxyURL, clibucket, keyname, r, errCh)
		files = append(files, keyname)

	}
	wg.Wait()
	selectErr(errCh, "put", t, true)
	tutils.Logf("PUT done.\n")

	// 2. Delete the objects
	err = api.DeleteList(tutils.BaseAPIParams(proxyURL), clibucket, "", files, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 3. Check to see that all the files have been deleted
	msg := &cmn.GetMsg{GetPrefix: prefix, GetPageSize: int(pagesize)}
	bktlst, err := api.ListBucket(tutils.DefaultBaseAPIParams(t), clibucket, msg, 0)
	tutils.CheckFatal(err, t)
	if len(bktlst.Entries) != 0 {
		t.Errorf("Incorrect number of remaining files: %d, should be 0", len(bktlst.Entries))
	}
}

func TestPrefetchRange(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}
	var (
		netprefetches  = int64(0)
		err            error
		rmin, rmax     int64
		re             *regexp.Regexp
		proxyURL       = getPrimaryURL(t, proxyURLReadOnly)
		baseParams     = tutils.BaseAPIParams(proxyURL)
		prefetchPrefix = "regressionList/obj"
		prefetchRegex  = "\\d*"
	)

	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skipf("Cannot prefetch from local bucket %s", clibucket)
	}

	// 1. Get initial number of prefetches
	smap := getClusterMap(t, proxyURL)
	for _, v := range smap.Tmap {
		stats := getDaemonStats(t, v.PublicNet.DirectURL)
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
	msg := &cmn.GetMsg{GetPrefix: prefetchPrefix, GetPageSize: int(pagesize)}
	objsToFilter := testListBucket(t, proxyURL, clibucket, msg, 0)
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
	err = api.EvictRange(baseParams, clibucket, cmn.CloudBs, prefetchPrefix, prefetchRegex, prefetchRange, true, 0)
	if err != nil {
		t.Error(err)
	}
	err = api.PrefetchRange(baseParams, clibucket, cmn.CloudBs, prefetchPrefix, prefetchRegex, prefetchRange, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 5. Ensure that all the prefetches occurred
	for _, v := range smap.Tmap {
		stats := getDaemonStats(t, v.PublicNet.DirectURL)
		npf, err := getPrefetchCnt(stats)
		if err != nil {
			t.Fatalf("Could not decode target stats: pre.n")
		}
		netprefetches += npf
	}
	if netprefetches != int64(len(files)) {
		t.Errorf("Did not prefetch all files: Missing %d of %d\n",
			(int64(len(files)) - netprefetches), len(files))
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
		proxyURL       = getPrimaryURL(t, proxyURLReadOnly)
		baseParams     = tutils.DefaultBaseAPIParams(t)
	)

	if created := createLocalBucketIfNotExists(t, proxyURL, clibucket); created {
		defer tutils.DestroyLocalBucket(t, proxyURL, clibucket)
	}

	// 1. Put files to delete
	for i := 0; i < numfiles; i++ {
		r, err := tutils.NewRandReader(fileSize, true /* withHash */)
		tutils.CheckFatal(err, t)

		wg.Add(1)
		go tutils.PutAsync(wg, proxyURL, clibucket, fmt.Sprintf("%s%d", prefix, i), r, errCh)
	}
	wg.Wait()
	selectErr(errCh, "put", t, true)
	tutils.Logf("PUT done.\n")

	// 2. Delete the small range of objects
	err = api.DeleteRange(tutils.BaseAPIParams(proxyURL), clibucket, "", prefix, regex, smallrange, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 3. Check to see that the correct files have been deleted
	msg := &cmn.GetMsg{GetPrefix: prefix, GetPageSize: int(pagesize)}
	bktlst, err := api.ListBucket(baseParams, clibucket, msg, 0)
	tutils.CheckFatal(err, t)
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
	err = api.DeleteRange(tutils.BaseAPIParams(proxyURL), clibucket, "", prefix, regex, bigrange, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 5. Check to see that all the files have been deleted
	bktlst, err = api.ListBucket(baseParams, clibucket, msg, 0)
	tutils.CheckFatal(err, t)
	if len(bktlst.Entries) != 0 {
		t.Errorf("Incorrect number of remaining files: %d, should be 0", len(bktlst.Entries))
	}
}

// Testing only local bucket objects since generally not concerned with cloud bucket object deletion
func TestStressDeleteRange(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
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
		proxyURL     = getPrimaryURL(t, proxyURLReadOnly)
		regex        = "\\d*"
		tenth        = numFiles / 10
		partial_rnge = fmt.Sprintf("%d:%d", 0, numFiles-tenth-1) // TODO: partial range with non-zero left boundary
		rnge         = fmt.Sprintf("0:%d", numFiles)
		readersList  [numReaders]tutils.Reader
		baseParams   = tutils.DefaultBaseAPIParams(t)
	)

	tutils.CreateFreshLocalBucket(t, proxyURL, TestLocalBucketName)

	// 1. PUT
	for i := 0; i < numReaders; i++ {
		random := rand.New(rand.NewSource(int64(i)))
		size := random.Int63n(cmn.KiB*128) + cmn.KiB/3
		tutils.CheckFatal(err, t)
		reader, err := tutils.NewRandReader(size, true /* withHash */)
		tutils.CheckFatal(err, t)
		readersList[i] = reader
		wg.Add(1)

		go func(i int, reader tutils.Reader) {
			for j := 0; j < numFiles/numReaders; j++ {
				objname := fmt.Sprintf("%s%d", prefix, i*numFiles/numReaders+j)
				putArgs := api.PutObjectArgs{
					BaseParams: baseParams,
					Bucket:     TestLocalBucketName,
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
			wg.Done()
			tutils.Progress(i, 99)
		}(i, reader)
	}
	wg.Wait()
	selectErr(errCh, "put", t, true)

	// 2. Delete a range of objects
	tutils.Logf("Deleting objects in range: %s\n", partial_rnge)
	err = api.DeleteRange(tutils.BaseAPIParams(proxyURL), TestLocalBucketName, cmn.LocalBs, prefix, regex, partial_rnge, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 3. Check to see that correct objects have been deleted
	expectedRemaining := tenth
	msg := &cmn.GetMsg{GetPrefix: prefix, GetPageSize: int(pagesize)}
	bktlst, err := api.ListBucket(baseParams, TestLocalBucketName, msg, 0)
	tutils.CheckFatal(err, t)
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
	err = api.DeleteRange(tutils.BaseAPIParams(proxyURL), TestLocalBucketName, cmn.LocalBs, prefix, regex, rnge, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 5. Check to see that all files have been deleted
	msg = &cmn.GetMsg{GetPrefix: prefix, GetPageSize: int(pagesize)}
	bktlst, err = api.ListBucket(baseParams, TestLocalBucketName, msg, 0)
	tutils.CheckFatal(err, t)
	if len(bktlst.Entries) != 0 {
		t.Errorf("Incorrect number of remaining files: %d, should be 0", len(bktlst.Entries))
	}

	tutils.DestroyLocalBucket(t, proxyURL, TestLocalBucketName)
}

func doRenameRegressionTest(t *testing.T, proxyURL string, rtd regressionTestData, numPuts int, filesPutCh chan string) {
	baseParams := tutils.BaseAPIParams(proxyURL)
	err := api.RenameLocalBucket(baseParams, rtd.bucket, rtd.renamedBucket)
	tutils.CheckFatal(err, t)

	buckets, err := api.GetBucketNames(baseParams, cmn.LocalBs)
	tutils.CheckFatal(err, t)

	if len(buckets.Local) != rtd.numLocalBuckets {
		t.Fatalf("wrong number of local buckets (names) before and after rename (before: %d. after: %d)",
			rtd.numLocalBuckets, len(buckets.Local))
	}

	renamedBucketExists := false
	for _, b := range buckets.Local {
		if b == rtd.renamedBucket {
			renamedBucketExists = true
		} else if b == rtd.bucket {
			t.Fatalf("original local bucket %s still exists after rename", rtd.bucket)
		}
	}

	if !renamedBucketExists {
		t.Fatalf("renamed local bucket %s does not exist after rename", rtd.renamedBucket)
	}

	objs, err := tutils.ListObjects(proxyURL, rtd.renamedBucket, cmn.LocalBs, "", numPuts+1)
	tutils.CheckFatal(err, t)

	if len(objs) != numPuts {
		for name := range filesPutCh {
			found := false
			for _, n := range objs {
				if strings.Contains(n, name) || strings.Contains(name, n) {
					found = true
					break
				}
			}
			if !found {
				tutils.Logf("not found: %s\n", name)
			}
		}
		t.Fatalf("wrong number of objects in the bucket %s renamed as %s (before: %d. after: %d)",
			rtd.bucket, rtd.renamedBucket, numPuts, len(objs))
	}
}

func doBucketRegressionTest(t *testing.T, proxyURL string, rtd regressionTestData) {
	const filesize = 1024
	var (
		numPuts    = 64
		filesPutCh = make(chan string, numPuts)
		errCh      = make(chan error, 100)
		wg         = &sync.WaitGroup{}
		bucket     = rtd.bucket
	)

	sgl := tutils.Mem2.NewSGL(filesize)
	defer sgl.Free()
	tutils.PutRandObjs(proxyURL, bucket, SmokeDir, readerType, SmokeStr, filesize, numPuts, errCh, filesPutCh, sgl)
	close(filesPutCh)
	selectErr(errCh, "put", t, true)
	if rtd.rename {
		doRenameRegressionTest(t, proxyURL, rtd, numPuts, filesPutCh)
		tutils.Logf("\nRenamed %s(numobjs=%d) => %s\n", bucket, numPuts, rtd.renamedBucket)
		bucket = rtd.renamedBucket
	}

	getRandomFiles(proxyURL, numPuts, bucket, SmokeStr+"/", t, nil, errCh)
	selectErr(errCh, "get", t, false)
	for fname := range filesPutCh {
		wg.Add(1)
		go tutils.Del(proxyURL, bucket, "smoke/"+fname, "", wg, errCh, !testing.Verbose())
	}
	wg.Wait()
	selectErr(errCh, "delete", t, abortonerr)
	close(errCh)
}

//========
//
// Helpers
//
//========

func waitForRebalanceToComplete(t *testing.T, proxyURL string) {
	time.Sleep(ais.NeighborRebalanceStartDelay)
OUTER:
	for {
		// Wait before querying so the rebalance statistics are populated.
		time.Sleep(time.Second * 3)
		tutils.Logln("Waiting for rebalance to complete.")

		rebalanceStats, err := getXactionRebalance(proxyURL)
		if err != nil {
			t.Fatalf("Unable to get rebalance stats. Error: [%v]", err)
		}

		targetsCompleted := 0
		for _, targetStats := range rebalanceStats.TargetStats {
			if len(targetStats.Xactions) > 0 {
				for _, xaction := range targetStats.Xactions {
					if xaction.Status != cmn.XactionStatusCompleted {
						continue OUTER
					}
				}
			}

			targetsCompleted++
			if targetsCompleted == len(rebalanceStats.TargetStats) {
				return
			}
		}
	}
}

func getXactionRebalance(proxyURL string) (stats.RebalanceStats, error) {
	var rebalanceStats stats.RebalanceStats
	responseBytes, err := tutils.GetXactionResponse(proxyURL, cmn.ActGlobalReb)
	if err != nil {
		return rebalanceStats, err
	}

	err = json.Unmarshal(responseBytes, &rebalanceStats)
	if err != nil {
		return rebalanceStats,
			fmt.Errorf("failed to unmarshal rebalance stats: %v", err)
	}

	return rebalanceStats, nil
}

func waitProgressBar(prefix string, wait time.Duration) {
	const tickerStep = time.Second * 1

	ticker := time.NewTicker(tickerStep)
	tutils.Logf(prefix)
	idx := 1
waitloop:
	for range ticker.C {
		elapsed := tickerStep * time.Duration(idx)
		tutils.Logf("----%d%%", (elapsed * 100 / wait))
		if elapsed >= wait {
			tutils.Logln("")
			break waitloop
		}
		idx++
	}
	ticker.Stop()
}

func getClusterStats(t *testing.T, proxyURL string) (stats stats.ClusterStats) {
	baseParams := tutils.BaseAPIParams(proxyURL)
	clusterStats, err := api.GetClusterStats(baseParams)
	tutils.CheckFatal(err, t)
	return clusterStats
}

func getNamedTargetStats(trunner *stats.Trunner, name string) int64 {
	if v, ok := trunner.Core.Tracker[name]; !ok {
		return 0
	} else {
		return v.Value
	}
}

func getDaemonStats(t *testing.T, url string) (stats map[string]interface{}) {
	q := tutils.GetWhatRawQuery(cmn.GetWhatStats, "")
	url = fmt.Sprintf("%s?%s", url+cmn.URLPath(cmn.Version, cmn.Daemon), q)
	resp, err := tutils.BaseHTTPClient.Get(url)
	if err != nil {
		t.Fatalf("Failed to perform get, err = %v", err)
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body, err = %v", err)
	}

	if resp.StatusCode >= http.StatusBadRequest {
		t.Fatalf("HTTP error = %d, message = %s", err, string(b))
	}

	dec := jsoniter.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	// If this isn't used, json.Unmarshal converts uint32s to floats, losing precision
	err = dec.Decode(&stats)
	if err != nil {
		t.Fatalf("Failed to unmarshal config: %v", err)
	}

	return
}

func getClusterMap(t *testing.T, URL string) cluster.Smap {
	baseParams := tutils.BaseAPIParams(URL)
	smap, err := api.GetClusterMap(baseParams)
	tutils.CheckFatal(err, t)
	return smap
}

func getDaemonConfig(t *testing.T, URL string) (config *cmn.Config) {
	var err error
	baseParams := tutils.BaseAPIParams(URL)
	config, err = api.GetDaemonConfig(baseParams)
	tutils.CheckFatal(err, t)
	return
}

func setDaemonConfig(t *testing.T, daemonURL, key string, value interface{}) {
	baseParams := tutils.BaseAPIParams(daemonURL)
	err := api.SetDaemonConfig(baseParams, key, value)
	tutils.CheckFatal(err, t)
}

func setClusterConfig(t *testing.T, proxyURL, key string, value interface{}) {
	baseParams := tutils.BaseAPIParams(proxyURL)
	err := api.SetClusterConfig(baseParams, key, value)
	tutils.CheckFatal(err, t)
}

func selectErr(errCh chan error, verb string, t *testing.T, errisfatal bool) {
	select {
	case err := <-errCh:
		if errisfatal {
			t.Fatalf("Failed to %s files: %v", verb, err)
		} else {
			t.Errorf("Failed to %s files: %v", verb, err)
		}
	default:
	}
}
