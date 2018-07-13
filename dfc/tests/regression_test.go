/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dfc_test provides distributed file-based cache with Amazon and Google Cloud backends.
package dfc_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	_ "net/http/pprof" // profile
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
	"github.com/NVIDIA/dfcpub/pkg/client/readers"
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
	rootDir = "/tmp/dfc"

	RestAPIDaemonSuffix   = "/" + dfc.Rversion + "/" + dfc.Rdaemon
	TestLocalBucketName   = "TESTLOCALBUCKET"
	RenameLocalBucketName = "renamebucket"
	RenameDir             = rootDir + "/rename"
	RenameStr             = "rename"
	ListRangeStr          = "__listrange"
)

var (
	RenameMsg        = dfc.ActionMsg{Action: dfc.ActRename}
	HighWaterMark    = uint32(80)
	LowWaterMark     = uint32(60)
	UpdTime          = time.Second * 20
	configRegression = map[string]string{
		"stats_time":         fmt.Sprintf("%v", UpdTime),
		"dont_evict_time":    fmt.Sprintf("%v", UpdTime),
		"capacity_upd_time":  fmt.Sprintf("%v", UpdTime),
		"startup_delay_time": fmt.Sprintf("%v", UpdTime),
		"lowwm":              fmt.Sprintf("%d", LowWaterMark),
		"highwm":             fmt.Sprintf("%d", HighWaterMark),
		"lru_enabled":        "true",
	}
	httpclient = &http.Client{}
)

func TestLocalListBucketGetTargetURL(t *testing.T) {
	const (
		num      = 1000
		filesize = uint64(1024)
		seed     = int64(111)
		bucket   = TestLocalBucketName
	)
	var (
		filenameCh = make(chan string, num)
		errch      = make(chan error, num)
		sgl        *dfc.SGLIO
		targets    = make(map[string]struct{})
	)

	smap, err := client.GetClusterMap(proxyurl)
	checkFatal(err, t)
	if len(smap.Tmap) == 1 {
		tlogln("Warning: more than 1 target should deployed for best utility of this test.")
	}

	if usingSG {
		sgl = dfc.NewSGLIO(filesize)
		defer sgl.Free()
	}

	err = client.CreateLocalBucket(proxyurl, bucket)
	checkFatal(err, t)

	defer func() {
		err = client.DestroyLocalBucket(proxyurl, bucket)
		checkFatal(err, t)
	}()

	putRandomFiles(seed, filesize, num, bucket, t, nil, errch, filenameCh, SmokeDir, SmokeStr, true, sgl)
	selectErr(errch, "put", t, true)

	msg := &dfc.GetMsg{GetPageSize: int(pagesize), GetProps: dfc.GetTargetURL}
	bl, err := client.ListBucket(proxyurl, bucket, msg, num)
	checkFatal(err, t)

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
		l, _, err := client.Get(e.TargetURL, bucket, e.Name, nil, nil, false, false)
		checkFatal(err, t)
		if uint64(l) != filesize {
			t.Errorf("Expected filesize: %d, actual filesize: %d\n", filesize, l)
		}
	}

	if len(smap.Tmap) != len(targets) { // The objects should have been distributed to all targets
		t.Errorf("Expected %d different target URLs, actual: %d different target URLs", len(smap.Tmap), len(targets))
	}

	// Ensure no target URLs are returned when the property is not requested
	msg.GetProps = ""
	bl, err = client.ListBucket(proxyurl, bucket, msg, num)
	checkFatal(err, t)

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
		fileSize      = uint64(1024)
		seed          = int64(111)
	)

	random := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	prefix := client.FastRandomFilename(random, 32)

	var (
		fileNameCh = make(chan string, numberOfFiles)
		errorCh    = make(chan error, numberOfFiles)
		sgl        *dfc.SGLIO
		bucketName = clibucket
		targets    = make(map[string]struct{})
	)

	isCloud := isCloudBucket(t, proxyurl, clibucket)
	if !isCloud {
		t.Skip("TestCloudListBucketGetTargetURL test is for cloud buckets only")
	}

	clusterMap, err := client.GetClusterMap(proxyurl)
	checkFatal(err, t)
	if len(clusterMap.Tmap) == 1 {
		tlogln("Warning: more than 1 target should deployed for best utility of this test.")
	}

	if usingSG {
		sgl = dfc.NewSGLIO(fileSize)
		defer sgl.Free()
	}

	putRandomFiles(seed, fileSize, numberOfFiles, bucketName, t, nil, errorCh, fileNameCh, SmokeDir, prefix, true, sgl)
	selectErr(errorCh, "put", t, true)
	defer func() {
		files := make([]string, numberOfFiles)
		for i := 0; i < numberOfFiles; i++ {
			files[i] = prefix + "/" + <-fileNameCh
		}
		err := client.DeleteList(proxyurl, bucketName, files, true, 0)
		if err != nil {
			t.Error("Unable to delete files during cleanup from cloud bucket.")
		}
	}()

	listBucketMsg := &dfc.GetMsg{GetPrefix: prefix, GetPageSize: int(pagesize), GetProps: dfc.GetTargetURL}
	bucketList, err := client.ListBucket(proxyurl, bucketName, listBucketMsg, 0)
	checkFatal(err, t)

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
		objectSize, _, err := client.Get(object.TargetURL, bucketName, object.Name,
			nil, nil, false, false)
		checkFatal(err, t)
		if uint64(objectSize) != fileSize {
			t.Errorf("Expected fileSize: %d, actual fileSize: %d\n", fileSize, objectSize)
		}
	}

	// The objects should have been distributed to all targets
	if len(clusterMap.Tmap) != len(targets) {
		t.Errorf("Expected %d different target URLs, actual: %d different target URLs",
			len(clusterMap.Tmap), len(targets))
	}

	// Ensure no target URLs are returned when the property is not requested
	listBucketMsg.GetProps = ""
	bucketList, err = client.ListBucket(proxyurl, bucketName, listBucketMsg, 0)
	checkFatal(err, t)

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
	var (
		num        = 2
		filenameCh = make(chan string, num)
		errch      = make(chan error, 100)
		sgl        *dfc.SGLIO
		filesize   = uint64(1024)
		seed       = int64(111)
		fqn        string
	)
	bucket := TestLocalBucketName
	err := client.CreateLocalBucket(proxyurl, bucket)
	checkFatal(err, t)

	defer func() {
		err = client.DestroyLocalBucket(proxyurl, bucket)
		checkFatal(err, t)
	}()

	if usingSG {
		sgl = dfc.NewSGLIO(filesize)
		defer sgl.Free()
	}

	putRandomFiles(seed, filesize, num, bucket, t, nil, errch, filenameCh, SmokeDir, SmokeStr, true, sgl)
	selectErr(errch, "put", t, false)

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
	tlogf("Corrupting file data[%s]: %s\n", fName, fqn)
	err = ioutil.WriteFile(fqn, []byte("this file has been corrupted"), 0644)
	checkFatal(err, t)
	_, _, err = client.Get(proxyurl, bucket, SmokeStr+"/"+fName, nil, nil, false, true)
	if err == nil {
		t.Error("Error is nil, expected non-nil error on a a GET for an object with corrupted contents")
	}

	// Test corrupting the file xattr
	fName = <-filenameCh
	filepath.Walk(rootDir, fsWalkFunc)
	tlogf("Corrupting file xattr[%s]: %s\n", fName, fqn)
	if errstr := dfc.Setxattr(fqn, dfc.XattrXXHashVal, []byte("01234abcde")); errstr != "" {
		t.Error(errstr)
	}
	_, _, err = client.Get(proxyurl, bucket, SmokeStr+"/"+fName, nil, nil, false, true)
	if err == nil {
		t.Error("Error is nil, expected non-nil error on a GET for an object with corrupted xattr")
	}
}

func TestRegressionLocalBuckets(t *testing.T) {
	bucket := TestLocalBucketName
	err := client.CreateLocalBucket(proxyurl, bucket)
	checkFatal(err, t)

	defer func() {
		err = client.DestroyLocalBucket(proxyurl, bucket)
		checkFatal(err, t)
	}()
	doBucketRegressionTest(t, regressionTestData{bucket: bucket})

}

func TestRenameLocalBuckets(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	bucket := TestLocalBucketName
	renamedBucket := bucket + "_renamed"
	err := client.CreateLocalBucket(proxyurl, bucket)
	checkFatal(err, t)

	defer func() {
		err = client.DestroyLocalBucket(proxyurl, renamedBucket)
		checkFatal(err, t)
	}()

	b, err := client.ListBuckets(proxyurl, true)
	checkFatal(err, t)

	doBucketRegressionTest(t, regressionTestData{
		bucket: bucket, renamedBucket: renamedBucket, numLocalBuckets: len(b.Local), rename: true,
	})
}

func TestListObjects(t *testing.T) {
	var (
		numFiles        = 20
		prefix          = "regressionList"
		fileSize uint64 = 1024
		bucket          = clibucket
		seed            = baseseed + 101
		errch           = make(chan error, numFiles*5)
		filesput        = make(chan string, numfiles)
		dir             = DeleteDir
		sgl      *dfc.SGLIO
	)
	if usingSG {
		sgl = dfc.NewSGLIO(fileSize)
		defer sgl.Free()
	}
	tlogf("Create a list of %d objects", numFiles)
	created := createLocalBucketIfNotExists(t, proxyurl, clibucket)
	fileList := make([]string, 0, numFiles)
	for i := 0; i < numFiles; i++ {
		fname := fmt.Sprintf("obj%d", i+1)
		fileList = append(fileList, fname)
	}
	fillWithRandomData(seed, fileSize, fileList, bucket, t, errch, filesput, dir, prefix, !testing.Verbose(), sgl)
	close(filesput)
	selectErr(errch, "list - put", t, true /* fatal - if PUT does not work then it makes no sense to continue */)

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
		tlogf("%d. %s\n    Prefix: [%s], Expected objects: %d\n", idx+1, test.title, test.prefix, test.expected)
		msg := &dfc.GetMsg{GetPageSize: test.pageSize, GetPrefix: test.prefix}
		reslist, err := listObjects(t, msg, bucket, test.limit)
		if err != nil {
			t.Error(err)
			continue
		}
		if len(reslist.Entries) != test.expected {
			t.Errorf("Test %d. %s failed: returned %d objects instead of %d",
				idx+1, test.title, len(reslist.Entries), test.expected)
		}
	}

	if usingFile {
		for name := range filesput {
			os.Remove(dir + "/" + name)
		}
	}

	if created {
		if err := client.DestroyLocalBucket(proxyurl, clibucket); err != nil {
			t.Errorf("Failed to delete local bucket: %v", err)
		}
	}
}

func TestRenameObjects(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	var (
		injson    []byte
		err       error
		numPuts   = 10
		filesput  = make(chan string, numPuts)
		errch     = make(chan error, numPuts)
		basenames = make([]string, 0, numPuts) // basenames
		bnewnames = make([]string, 0, numPuts) // new basenames
		sgl       *dfc.SGLIO
	)

	err = client.CreateLocalBucket(proxyurl, RenameLocalBucketName)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		// cleanup
		wg := &sync.WaitGroup{}
		for _, fname := range bnewnames {
			wg.Add(1)
			go client.Del(proxyurl, RenameLocalBucketName, RenameStr+"/"+fname, wg, errch, !testing.Verbose())
		}

		if usingFile {
			for _, fname := range basenames {
				err = os.Remove(RenameDir + "/" + fname)
				if err != nil {
					t.Errorf("Failed to remove file %s: %v", fname, err)
				}
			}
		}

		wg.Wait()
		selectErr(errch, "delete", t, false)
		close(errch)
		err = client.DestroyLocalBucket(proxyurl, RenameLocalBucketName)
		checkFatal(err, t)
	}()

	time.Sleep(time.Second * 5)

	if err = dfc.CreateDir(RenameDir); err != nil {
		t.Errorf("Error creating dir: %v", err)
	}

	if usingSG {
		sgl = dfc.NewSGLIO(1024 * 1024)
		defer sgl.Free()
	}

	putRandomFiles(baseseed+1, 0, numPuts, RenameLocalBucketName, t, nil, nil, filesput, RenameDir,
		RenameStr, !testing.Verbose(), sgl)
	selectErr(errch, "put", t, false)
	close(filesput)
	for fname := range filesput {
		basenames = append(basenames, fname)
	}

	// rename
	for _, fname := range basenames {
		RenameMsg.Name = RenameStr + "/" + fname + ".renamed" // objname
		bnewnames = append(bnewnames, fname+".renamed")       // base name
		injson, err = json.Marshal(RenameMsg)
		if err != nil {
			t.Fatalf("Failed to marshal RenameMsg: %v", err)
		}

		err := client.HTTPRequest("POST",
			proxyurl+"/"+dfc.Rversion+"/"+dfc.Robjects+"/"+RenameLocalBucketName+"/"+RenameStr+"/"+fname,
			bytes.NewBuffer(injson))
		if err != nil {
			t.Fatalf("Failed to send request, err = %v", err)
		}

		tlogln(fmt.Sprintf("Rename %s/%s => %s", RenameStr, fname, RenameMsg.Name))
	}

	// get renamed objects
	waitProgressBar("Rename/move: ", time.Second*5)
	for _, fname := range bnewnames {
		client.Get(proxyurl, RenameLocalBucketName, RenameStr+"/"+fname, nil,
			errch, !testing.Verbose(), false /* validate */)
	}
	selectErr(errch, "get", t, false)
}

func TestObjectPrefix(t *testing.T) {
	created := createLocalBucketIfNotExists(t, proxyurl, clibucket)

	prefixFileNumber = numfiles
	prefixCreateFiles(t)
	prefixLookup(t)
	prefixCleanup(t)

	if created {
		if err := client.DestroyLocalBucket(proxyurl, clibucket); err != nil {
			t.Errorf("Failed to delete local bucket: %v", err)
		}
	}
}

func TestObjectsVersions(t *testing.T) {
	propsMainTest(t, dfc.VersionAll)
}

func TestRegressionCloudBuckets(t *testing.T) {
	isCloud := isCloudBucket(t, proxyurl, clibucket)
	if !isCloud {
		t.Skip("TestRegressionCloudBuckets test is for cloud buckets only")
	}

	doBucketRegressionTest(t, regressionTestData{bucket: clibucket})
}

func TestRebalance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	var (
		sid             string
		targetDirectURL string
		numPuts         = 40
		filesput        = make(chan string, numPuts)
		errch           = make(chan error, 100)
		wg              = &sync.WaitGroup{}
		sgl             *dfc.SGLIO
		filesize        = uint64(1024 * 128)
	)
	filesSentOrig := make(map[string]int64)
	bytesSentOrig := make(map[string]int64)
	filesRecvOrig := make(map[string]int64)
	bytesRecvOrig := make(map[string]int64)
	stats := getClusterStats(httpclient, t)
	for k, v := range stats.Target {
		bytesSentOrig[k], filesSentOrig[k], bytesRecvOrig[k], filesRecvOrig[k] =
			v.Core.Numsentbytes, v.Core.Numsentfiles, v.Core.Numrecvbytes, v.Core.Numrecvfiles
	}

	created := createLocalBucketIfNotExists(t, proxyurl, clibucket)

	//
	// step 1. config
	//
	oconfig := getConfig(proxyurl+"/"+dfc.Rversion+"/"+dfc.Rdaemon, httpclient, t)
	orebconfig := oconfig["rebalance_conf"].(map[string]interface{})
	defer func() {
		setConfig("startup_delay_time", orebconfig["startup_delay_time"].(string), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)

		if created {
			if err := client.DestroyLocalBucket(proxyurl, clibucket); err != nil {
				t.Errorf("Failed to delete local bucket: %v", err)
			}
		}
	}()

	//
	// cluster-wide reduce startup_delay_time
	//
	startupdelaytimestr := "20s"
	setConfig("startup_delay_time", startupdelaytimestr, proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t) // NOTE: 1 second
	if t.Failed() {
		return
	}
	waitProgressBar("Rebalance: ", time.Second*10)

	//
	// step 2. unregister random target
	//
	smap := getClusterMap(t)
	l := len(smap.Tmap)
	if l < 2 {
		t.Fatalf("Must have 2 or more targets in the cluster, have only %d", l)
	}
	for sid = range smap.Tmap {
		break
	}
	targetDirectURL = smap.Tmap[sid].DirectURL

	err := client.UnregisterTarget(proxyurl, sid)
	checkFatal(err, t)
	tlogf("Unregistered %s: cluster size = %d (targets)\n", sid, l-1)
	//
	// step 3. put random files => (cluster - 1)
	//
	if usingSG {
		sgl = dfc.NewSGLIO(filesize)
		defer sgl.Free()
	}
	putRandomFiles(baseseed, filesize, numPuts, clibucket, t, nil, errch, filesput, SmokeDir,
		SmokeStr, !testing.Verbose(), sgl)
	selectErr(errch, "put", t, false)

	//
	// step 4. register back
	//
	err = client.RegisterTarget(sid, targetDirectURL, smap)
	checkFatal(err, t)
	for i := 0; i < 25; i++ {
		time.Sleep(time.Second)
		smap = getClusterMap(t)
		if len(smap.Tmap) == l {
			break
		}
	}
	if len(smap.Tmap) != l {
		t.Errorf("Re-registration timed out: target %s, original num targets %d\n", sid, l)
		return
	}
	tlogf("Re-registered %s: the cluster is now back to %d targets\n", sid, l)
	//
	// step 5. wait for rebalance to run its course
	//
	waitForRebalanceToComplete(t)
	//
	// step 6. statistics
	//
	stats = getClusterStats(httpclient, t)
	var bsent, fsent, brecv, frecv int64
	for k, v := range stats.Target {
		bsent += v.Core.Numsentbytes - bytesSentOrig[k]
		fsent += v.Core.Numsentfiles - filesSentOrig[k]
		brecv += v.Core.Numrecvbytes - bytesRecvOrig[k]
		frecv += v.Core.Numrecvfiles - filesRecvOrig[k]
	}

	//
	// step 7. cleanup
	//
	close(filesput) // to exit for-range
	for fname := range filesput {
		if usingFile {
			err := os.Remove(SmokeDir + "/" + fname)
			if err != nil {
				t.Error(err)
			}
		}

		wg.Add(1)
		go client.Del(proxyurl, clibucket, "smoke/"+fname, wg, errch, !testing.Verbose())
	}
	wg.Wait()
	selectErr(errch, "delete", t, abortonerr)
	close(errch)
	if !t.Failed() && testing.Verbose() {
		fmt.Printf("Rebalance: sent     %.2f MB in %d files\n", float64(bsent)/1000/1000, fsent)
		fmt.Printf("           received %.2f MB in %d files\n", float64(brecv)/1000/1000, frecv)
	}
}

func TestGetClusterStats(t *testing.T) {
	smap := getClusterMap(t)
	stats := getClusterStats(httpclient, t)

	for k, v := range stats.Target {
		tdstats := getDaemonStats(httpclient, t, smap.Tmap[k].DirectURL)
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
			if uint64(used) != fstats.Used || uint64(avail) != fstats.Avail || uint32(usedpct) != fstats.Usedpct {
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
	oconfig := getConfig(proxyurl+"/"+dfc.Rversion+"/"+dfc.Rdaemon, httpclient, t)
	olruconfig := oconfig["lru_config"].(map[string]interface{})
	orebconfig := oconfig["rebalance_conf"].(map[string]interface{})
	operiodic := oconfig["periodic"].(map[string]interface{})

	for k, v := range configRegression {
		setConfig(k, v, proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	}

	nconfig := getConfig(proxyurl+"/"+dfc.Rversion+"/"+dfc.Rdaemon, httpclient, t)
	nlruconfig := nconfig["lru_config"].(map[string]interface{})
	nrebconfig := nconfig["rebalance_conf"].(map[string]interface{})
	nperiodic := nconfig["periodic"].(map[string]interface{})

	if nperiodic["stats_time"] != configRegression["stats_time"] {
		t.Errorf("StatsTime was not set properly: %v, should be: %v",
			nperiodic["stats_time"], configRegression["stats_time"])
	} else {
		o := operiodic["stats_time"].(string)
		setConfig("stats_time", o, proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	}
	if nlruconfig["dont_evict_time"] != configRegression["dont_evict_time"] {
		t.Errorf("DontEvictTime was not set properly: %v, should be: %v",
			nlruconfig["dont_evict_time"], configRegression["dont_evict_time"])
	} else {
		o := olruconfig["dont_evict_time"].(string)
		setConfig("dont_evict_time", o, proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	}
	if nlruconfig["capacity_upd_time"] != configRegression["capacity_upd_time"] {
		t.Errorf("CapacityUpdTime was not set properly: %v, should be: %v",
			nlruconfig["capacity_upd_time"], configRegression["capacity_upd_time"])
	} else {
		o := olruconfig["capacity_upd_time"].(string)
		setConfig("capacity_upd_time", o, proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	}
	if nrebconfig["startup_delay_time"] != configRegression["startup_delay_time"] {
		t.Errorf("StartupDelayTime was not set properly: %v, should be: %v",
			nrebconfig["startup_delay_time"], configRegression["startup_delay_time"])
	} else {
		o := orebconfig["startup_delay_time"].(string)
		setConfig("startup_delay_time", o, proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	}
	if hw, err := strconv.Atoi(configRegression["highwm"]); err != nil {
		t.Fatalf("Error parsing HighWM: %v", err)
	} else if nlruconfig["highwm"] != float64(hw) {
		t.Errorf("HighWatermark was not set properly: %.0f, should be: %d",
			nlruconfig["highwm"], hw)
	} else {
		o := olruconfig["highwm"].(float64)
		setConfig("highwm", strconv.Itoa(int(o)), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	}
	if lw, err := strconv.Atoi(configRegression["lowwm"]); err != nil {
		t.Fatalf("Error parsing LowWM: %v", err)
	} else if nlruconfig["lowwm"] != float64(lw) {
		t.Errorf("LowWatermark was not set properly: %.0f, should be: %d",
			nlruconfig["lowwm"], lw)
	} else {
		o := olruconfig["lowwm"].(float64)
		setConfig("lowwm", strconv.Itoa(int(o)), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	}
	if pt, err := strconv.ParseBool(configRegression["lru_enabled"]); err != nil {
		t.Fatalf("Error parsing LRUEnabled: %v", err)
	} else if nlruconfig["lru_enabled"] != pt {
		t.Errorf("LRUEnabled was not set properly: %v, should be %v",
			nlruconfig["lru_enabled"], pt)
	} else {
		o := olruconfig["lru_enabled"].(bool)
		setConfig("lru_enabled", strconv.FormatBool(o), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	}
}

func TestLRU(t *testing.T) {
	var (
		errch   = make(chan error, 100)
		usedpct = uint32(100)
	)
	isCloud := isCloudBucket(t, proxyurl, clibucket)
	if !isCloud {
		t.Skip("TestLRU test is for cloud buckets only")
	}

	getRandomFiles(0, 20, clibucket, "", t, nil, errch)
	// The error could be no object in the bucket. In that case, consider it as not an error;
	// this test will be skipped
	if len(errch) != 0 {
		t.Logf("LRU: need a cloud bucket with at least 20 objects")
		t.Skip()
	}

	//
	// remember targets' watermarks
	//
	smap := getClusterMap(t)
	lwms := make(map[string]interface{})
	hwms := make(map[string]interface{})
	bytesEvictedOrig := make(map[string]int64)
	filesEvictedOrig := make(map[string]int64)
	for k, di := range smap.Tmap {
		cfg := getConfig(di.DirectURL+RestAPIDaemonSuffix, httpclient, t)
		lrucfg := cfg["lru_config"].(map[string]interface{})
		lwms[k] = lrucfg["lowwm"]
		hwms[k] = lrucfg["highwm"]
	}
	// add a few more
	getRandomFiles(0, 3, clibucket, "", t, nil, errch)
	selectErr(errch, "get", t, true)
	//
	// find out min usage %% across all targets
	//
	stats := getClusterStats(httpclient, t)
	for k, v := range stats.Target {
		bytesEvictedOrig[k], filesEvictedOrig[k] = v.Core.Bytesevicted, v.Core.Filesevicted
		for _, c := range v.Capacity {
			usedpct = min(usedpct, c.Usedpct)
		}
	}
	tlogf("LRU: current min space usage in the cluster: %d%%\n", usedpct)
	var (
		lowwm  = usedpct - 5
		highwm = usedpct - 1
	)
	if int(lowwm) < 10 {
		t.Logf("The current space usage is too low (%d) for the LRU to be tested", lowwm)
		t.Skip()
		return
	}
	oconfig := getConfig(proxyurl+"/"+dfc.Rversion+"/"+dfc.Rdaemon, httpclient, t)
	if t.Failed() {
		return
	}
	//
	// all targets: set new watermarks; restore upon exit
	//
	olruconfig := oconfig["lru_config"].(map[string]interface{})
	operiodic := oconfig["periodic"].(map[string]interface{})
	defer func() {
		setConfig("dont_evict_time", olruconfig["dont_evict_time"].(string), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
		setConfig("capacity_upd_time", olruconfig["capacity_upd_time"].(string), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
		setConfig("highwm", fmt.Sprint(olruconfig["highwm"]), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
		setConfig("lowwm", fmt.Sprint(olruconfig["lowwm"]), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
		for k, di := range smap.Tmap {
			setConfig("highwm", fmt.Sprint(hwms[k]), di.DirectURL+RestAPIDaemonSuffix, httpclient, t)
			setConfig("lowwm", fmt.Sprint(lwms[k]), di.DirectURL+RestAPIDaemonSuffix, httpclient, t)
		}
	}()
	//
	// cluster-wide reduce dont-evict-time
	//
	dontevicttimestr := "30s"
	capacityupdtimestr := "5s"
	sleeptime, err := time.ParseDuration(operiodic["stats_time"].(string)) // to make sure the stats get updated
	if err != nil {
		t.Fatalf("Failed to parse stats_time: %v", err)
	}
	setConfig("dont_evict_time", dontevicttimestr, proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	setConfig("capacity_upd_time", capacityupdtimestr, proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	if t.Failed() {
		return
	}
	setConfig("lowwm", fmt.Sprint(lowwm), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	if t.Failed() {
		return
	}
	setConfig("highwm", fmt.Sprint(highwm), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	if t.Failed() {
		return
	}
	waitProgressBar("LRU: ", sleeptime/2)
	getRandomFiles(0, 1, clibucket, "", t, nil, errch)
	waitProgressBar("LRU: ", sleeptime/2)
	//
	// results
	//
	stats = getClusterStats(httpclient, t)
	testFsPaths := oconfig["test_fspaths"].(map[string]interface{})
	for k, v := range stats.Target {
		bytes := v.Core.Bytesevicted - bytesEvictedOrig[k]
		tlogf("Target %s: evicted %d files - %.2f MB (%dB) total\n",
			k, v.Core.Filesevicted-filesEvictedOrig[k], float64(bytes)/1000/1000, bytes)
		//
		// testingFSPpaths() - cannot reliably verify space utilization by tmpfs
		//
		if testFsPaths["count"].(float64) > 0 {
			continue
		}
		for mpath, c := range v.Capacity {
			if c.Usedpct < lowwm-1 || c.Usedpct > lowwm+1 {
				t.Errorf("Target %s failed to reach lwm %d%%: mpath %s, used space %d%%", k, lowwm, mpath, c.Usedpct)
			}
		}
	}
}

func TestPrefetchList(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	var (
		toprefetch    = make(chan string, numfiles)
		netprefetches = int64(0)
	)

	isCloud := isCloudBucket(t, proxyurl, clibucket)
	if !isCloud {
		t.Skipf("Cannot prefetch from local bucket %s", clibucket)
	}

	// 1. Get initial number of prefetches
	smap := getClusterMap(t)
	for _, v := range smap.Tmap {
		stats := getDaemonStats(httpclient, t, v.DirectURL)
		corestats := stats["core"].(map[string]interface{})
		npf, err := corestats["numprefetch"].(json.Number).Int64()
		if err != nil {
			t.Fatalf("Could not decode target stats: numprefetch")
		}
		netprefetches -= npf
	}

	// 2. Get keys to prefetch
	n := int64(getMatchingKeys(match, clibucket, []chan string{toprefetch}, nil, t))
	close(toprefetch) // to exit for-range
	files := make([]string, 0)
	for i := range toprefetch {
		files = append(files, i)
	}

	// 3. Evict those objects from the cache and prefetch them
	tlogf("Evicting and Prefetching %d objects\n", len(files))
	err := client.EvictList(proxyurl, clibucket, files, true, 0)
	if err != nil {
		t.Error(err)
	}
	err = client.PrefetchList(proxyurl, clibucket, files, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 5. Ensure that all the prefetches occurred.
	for _, v := range smap.Tmap {
		stats := getDaemonStats(httpclient, t, v.DirectURL)
		corestats := stats["core"].(map[string]interface{})
		npf, err := corestats["numprefetch"].(json.Number).Int64()
		if err != nil {
			t.Fatalf("Could not decode target stats: numprefetch")
		}
		netprefetches += npf
	}
	if netprefetches != n {
		t.Errorf("Did not prefetch all files: Missing %d of %d\n", (n - netprefetches), n)
	}
}

func TestDeleteList(t *testing.T) {
	var (
		err    error
		prefix = ListRangeStr + "/tstf-"
		wg     = &sync.WaitGroup{}
		errch  = make(chan error, numfiles)
		files  = make([]string, 0)
	)
	created := createLocalBucketIfNotExists(t, proxyurl, clibucket)

	// 1. Put files to delete:
	for i := 0; i < numfiles; i++ {
		r, err := readers.NewRandReader(fileSize, true /* withHash */)
		checkFatal(err, t)

		keyname := fmt.Sprintf("%s%d", prefix, i)

		wg.Add(1)
		go client.PutAsync(wg, proxyurl, r, clibucket, keyname, errch, !testing.Verbose())
		files = append(files, keyname)

	}
	wg.Wait()
	selectErr(errch, "put", t, true)

	// 2. Delete the objects
	err = client.DeleteList(proxyurl, clibucket, files, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 3. Check to see that all the files have been deleted.
	msg := &dfc.GetMsg{GetPrefix: prefix, GetPageSize: int(pagesize)}
	bktlst, err := client.ListBucket(proxyurl, clibucket, msg, 0)
	if len(bktlst.Entries) != 0 {
		t.Errorf("Incorrect number of remaining files: %d, should be 0", len(bktlst.Entries))
	}

	if created {
		if err = client.DestroyLocalBucket(proxyurl, clibucket); err != nil {
			t.Errorf("Failed to delete local bucket: %v", err)
		}
	}
}

func TestPrefetchRange(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	var (
		netprefetches = int64(0)
		err           error
		rmin, rmax    int64
		re            *regexp.Regexp
	)

	isCloud := isCloudBucket(t, proxyurl, clibucket)
	if !isCloud {
		t.Skipf("Cannot prefetch from local bucket %s", clibucket)
	}

	// 1. Get initial number of prefetches
	smap := getClusterMap(t)
	for _, v := range smap.Tmap {
		stats := getDaemonStats(httpclient, t, v.DirectURL)
		corestats := stats["core"].(map[string]interface{})
		npf, err := corestats["numprefetch"].(json.Number).Int64()
		if err != nil {
			t.Fatalf("Could not decode target stats: numprefetch")
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
	msg := &dfc.GetMsg{GetPrefix: prefetchPrefix, GetPageSize: int(pagesize)}
	objsToFilter := testListBucket(t, clibucket, msg, 0)
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

	// 4. Evict those objects from the cache, and then prefetch them.
	tlogf("Evicting and Prefetching %d objects\n", len(files))
	err = client.EvictRange(proxyurl, clibucket, prefetchPrefix, prefetchRegex, prefetchRange, true, 0)
	if err != nil {
		t.Error(err)
	}
	err = client.PrefetchRange(proxyurl, clibucket, prefetchPrefix, prefetchRegex, prefetchRange, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 5. Ensure that all the prefetches occurred.
	for _, v := range smap.Tmap {
		stats := getDaemonStats(httpclient, t, v.DirectURL)
		corestats := stats["core"].(map[string]interface{})
		npf, err := corestats["numprefetch"].(json.Number).Int64()
		if err != nil {
			t.Fatalf("Could not decode target stats: numprefetch")
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
		errch          = make(chan error, numfiles)
	)

	created := createLocalBucketIfNotExists(t, proxyurl, clibucket)

	// 1. Put files to delete:
	for i := 0; i < numfiles; i++ {
		r, err := readers.NewRandReader(fileSize, true /* withHash */)
		checkFatal(err, t)

		wg.Add(1)
		go client.PutAsync(wg, proxyurl, r, clibucket, fmt.Sprintf("%s%d", prefix, i), errch,
			!testing.Verbose())
	}
	wg.Wait()
	selectErr(errch, "put", t, true)

	// 2. Delete the small range of objects:
	err = client.DeleteRange(proxyurl, clibucket, prefix, regex, smallrange, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 3. Check to see that the correct files have been deleted
	msg := &dfc.GetMsg{GetPrefix: prefix, GetPageSize: int(pagesize)}
	bktlst, err := client.ListBucket(proxyurl, clibucket, msg, 0)
	if len(bktlst.Entries) != numfiles-smallrangesize {
		t.Errorf("Incorrect number of remaining files: %d, should be %d", len(bktlst.Entries), numfiles-smallrangesize)
	}
	filemap := make(map[string]*dfc.BucketEntry)
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

	// 4. Delete the big range of objects:
	err = client.DeleteRange(proxyurl, clibucket, prefix, regex, bigrange, true, 0)
	if err != nil {
		t.Error(err)
	}

	// 5. Check to see that all the files have been deleted
	bktlst, err = client.ListBucket(proxyurl, clibucket, msg, 0)
	if len(bktlst.Entries) != 0 {
		t.Errorf("Incorrect number of remaining files: %d, should be 0", len(bktlst.Entries))
	}

	if created {
		if err = client.DestroyLocalBucket(proxyurl, clibucket); err != nil {
			t.Errorf("Failed to delete local bucket: %v", err)
		}
	}
}

func doRenameRegressionTest(t *testing.T, rtd regressionTestData, numPuts int, filesput chan string) {
	err := client.RenameLocalBucket(proxyurl, rtd.bucket, rtd.renamedBucket)
	checkFatal(err, t)

	buckets, err := client.ListBuckets(proxyurl, true)
	checkFatal(err, t)

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

	objs, err := client.ListObjects(proxyurl, rtd.renamedBucket, "", numPuts+1)
	checkFatal(err, t)

	if len(objs) != numPuts {
		for name := range filesput {
			found := false
			for _, n := range objs {
				if strings.Contains(n, name) || strings.Contains(name, n) {
					found = true
					break
				}
			}
			if !found {
				tlogf("not found: %s\n", name)
			}
		}
		t.Fatalf("wrong number of objects in the bucket %s renamed as %s (before: %d. after: %d)",
			rtd.bucket, rtd.renamedBucket, numPuts, len(objs))
	}
}

func doBucketRegressionTest(t *testing.T, rtd regressionTestData) {
	var (
		numPuts  = 64
		filesput = make(chan string, numPuts)
		errch    = make(chan error, 100)
		wg       = &sync.WaitGroup{}
		sgl      *dfc.SGLIO
		filesize = uint64(1024)
		bucket   = rtd.bucket
	)

	if usingSG {
		sgl = dfc.NewSGLIO(filesize)
		defer sgl.Free()
	}

	putRandomFiles(baseseed+2, filesize, numPuts, bucket, t, nil, errch, filesput, SmokeDir,
		SmokeStr, !testing.Verbose(), sgl)
	close(filesput)
	selectErr(errch, "put", t, true)

	if rtd.rename {
		doRenameRegressionTest(t, rtd, numPuts, filesput)
		tlogf("\nRenamed %s(numobjs=%d) => %s\n", bucket, numPuts, rtd.renamedBucket)
		bucket = rtd.renamedBucket
	}

	getRandomFiles(0, numPuts, bucket, SmokeStr+"/", t, nil, errch)
	selectErr(errch, "get", t, false)
	for fname := range filesput {
		if usingFile {
			err := os.Remove(SmokeDir + "/" + fname)
			if err != nil {
				t.Error(err)
			}
		}

		wg.Add(1)
		go client.Del(proxyurl, bucket, "smoke/"+fname, wg, errch, !testing.Verbose())
	}
	wg.Wait()
	selectErr(errch, "delete", t, abortonerr)
	close(errch)
}

//========
//
// Helpers
//
//========

func waitForRebalanceToComplete(t *testing.T) {
OUTER:
	for {
		// Wait before querying so the rebalance statistics are populated.
		time.Sleep(time.Second * 3)
		tlogln("Waiting for rebalance to complete.")

		rebalanceStats, err := client.GetXactionRebalance(proxyurl)
		if err != nil {
			t.Fatalf("Unable to get rebalance stats. Error: [%v]", err)
		}

		targetsCompleted := 0
		for _, targetStats := range rebalanceStats.TargetStats {
			if len(targetStats.Xactions) > 0 {
				for _, xaction := range targetStats.Xactions {
					if xaction.Status != dfc.XactionStatusCompleted {
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

func waitProgressBar(prefix string, wait time.Duration) {
	const tickerStep = time.Second * 5

	ticker := time.NewTicker(tickerStep)
	tlogf(prefix)
	idx := 1
waitloop:
	for range ticker.C {
		elapsed := tickerStep * time.Duration(idx)
		tlogf("----%d%%", (elapsed * 100 / wait))
		if elapsed >= wait {
			tlogln("")
			break waitloop
		}
		idx++
	}
	ticker.Stop()
}

func getClusterStats(httpclient *http.Client, t *testing.T) (stats dfc.ClusterStats) {
	q := getWhatRawQuery(dfc.GetWhatStats)
	url := fmt.Sprintf("%s?%s", proxyurl+dfc.URLPath(dfc.Rversion, dfc.Rcluster), q)
	resp, err := httpclient.Get(url)
	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body, err = %v", err)
	}

	if resp.StatusCode >= http.StatusBadRequest {
		t.Fatalf("HTTP error = %d, message = %v", resp.StatusCode, string(b))
	}

	err = json.Unmarshal(b, &stats)
	if err != nil {
		t.Fatalf("Failed to unmarshal, err = %v", err)
	}

	return
}

func getDaemonStats(httpclient *http.Client, t *testing.T, URL string) (stats map[string]interface{}) {
	q := getWhatRawQuery(dfc.GetWhatStats)
	url := fmt.Sprintf("%s?%s", URL+RestAPIDaemonSuffix, q)
	resp, err := httpclient.Get(url)
	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body, err = %v", err)
	}

	if resp.StatusCode >= http.StatusBadRequest {
		t.Fatalf("HTTP error = %d, message = %s", err, string(b))
	}

	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	// If this isn't used, json.Unmarshal converts uint32s to floats, losing precision
	err = dec.Decode(&stats)
	if err != nil {
		t.Fatalf("Failed to unmarshal Dfconfig: %v", err)
	}

	return
}

func getClusterMap(t *testing.T) dfc.Smap {
	smap, err := client.GetClusterMap(proxyurl)
	checkFatal(err, t)
	return smap
}

func getConfig(URL string, httpclient *http.Client, t *testing.T) (dfcfg map[string]interface{}) {
	q := getWhatRawQuery(dfc.GetWhatConfig)
	url := fmt.Sprintf("%s?%s", URL, q)
	resp, err := httpclient.Get(url)
	defer func() {
		if resp != nil {
			resp.Body.Close()
		}
	}()

	if err != nil {
		t.Errorf("Get configuration, err = %v", err)
		return
	}

	var b []byte
	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("Failed to read response body, err = %v", err)
		return
	}

	if resp.StatusCode >= http.StatusBadRequest {
		t.Errorf("HTTP request get configuration failed, status = %d, %s",
			resp.StatusCode, string(b))
		return
	}

	err = json.Unmarshal(b, &dfcfg)
	if err != nil {
		t.Errorf("Failed to unmarshal config: %v", err)
	}

	return
}

func setConfig(name, value, URL string, httpclient *http.Client, t *testing.T) {
	SetConfigMsg := dfc.ActionMsg{Action: dfc.ActSetConfig,
		Name:  name,
		Value: value,
	}

	injson, err := json.Marshal(SetConfigMsg)
	if err != nil {
		t.Errorf("Failed to marshal SetConfig Message: %v", err)
		return
	}
	req, err := http.NewRequest("PUT", URL, bytes.NewBuffer(injson))
	if err != nil {
		t.Errorf("Failed to create request: %v", err)
		return
	}
	r, err := httpclient.Do(req)
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()
	if err != nil {
		t.Errorf("Failed to execute SetConfig request: %v", err)
		return
	}
}

//
// FIXME: extract the most basic utility functions (such as the ones below)
//        into a separate file within the dfc_test module
//
func min(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}

func selectErr(errch chan error, verb string, t *testing.T, errisfatal bool) {
	select {
	case err := <-errch:
		if errisfatal {
			t.Fatalf("Failed to %s files: %v", verb, err)
		} else {
			t.Errorf("Failed to %s files: %v", verb, err)
		}
	default:
	}
}

func tlogf(msg string, args ...interface{}) {
	if testing.Verbose() {
		fmt.Fprintf(os.Stdout, msg, args...)
	}
}

func tlogln(msg string) {
	tlogf(msg + "\n")
}

func stringInSlice(str string, values []string) bool {
	for _, v := range values {
		if str == v {
			return true
		}
	}

	return false
}

func getWhatRawQuery(getWhat string) string {
	q := url.Values{}
	q.Add(dfc.URLParamWhat, getWhat)
	return q.Encode()
}
