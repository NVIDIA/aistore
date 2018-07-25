/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

// For how to run tests, see README

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof" // profile
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
	"github.com/NVIDIA/dfcpub/pkg/client/readers"
	"github.com/OneOfOne/xxhash"
)

// worker's result
type workres struct {
	totfiles int
	totbytes int64
}

func Test_download(t *testing.T) {
	if err := client.Tcping(proxyurl); err != nil {
		tlogf("%s: %v\n", proxyurl, err)
		os.Exit(1)
	}

	isCloud := isCloudBucket(t, proxyurl, clibucket)
	if !isCloud {
		t.Skip("Download test is for cloud buckets only")
	}

	// Declare one channel per worker to pass the keyname
	keynameChans := make([]chan string, numworkers)
	resultChans := make([]chan workres, numworkers)
	filesCreated := make(chan string, numfiles)

	defer func() {
		close(filesCreated)
		var err error
		for file := range filesCreated {
			e := os.Remove(LocalDestDir + "/" + file)
			if e != nil {
				err = e
			}
		}
		if err != nil {
			t.Error(err)
		}
	}()

	for i := 0; i < numworkers; i++ {
		// Allow a bunch of messages at a time to be written asynchronously to a channel
		keynameChans[i] = make(chan string, 100)

		// Initialize number of files downloaded
		resultChans[i] = make(chan workres, 100)
	}

	// Start the worker pools
	errch := make(chan error, 100)

	var wg = &sync.WaitGroup{}
	// Get the workers started
	for i := 0; i < numworkers; i++ {
		wg.Add(1)
		// Read the response and write it to a file
		go getAndCopyTmp(i, keynameChans[i], t, wg, errch, resultChans[i], clibucket)
	}

	num := getMatchingKeys(match, clibucket, keynameChans, filesCreated, t)

	t.Logf("Expecting to get %d keys\n", num)

	// Close the channels after the reading is done
	for i := 0; i < numworkers; i++ {
		close(keynameChans[i])
	}

	wg.Wait()

	// Now find the total number of files and data downloaed
	var sumtotfiles int
	var sumtotbytes int64
	for i := 0; i < numworkers; i++ {
		res := <-resultChans[i]
		if res.totfiles == 0 {
			continue
		}
		sumtotbytes += res.totbytes
		sumtotfiles += res.totfiles
		t.Logf("Worker #%d: %d files, size %.2f MB (%d B)",
			i, res.totfiles, float64(res.totbytes/1000/1000), res.totbytes)
	}
	t.Logf("\nSummary: %d workers, %d files, total size %.2f MB (%d B)",
		numworkers, sumtotfiles, float64(sumtotbytes/1000/1000), sumtotbytes)

	if sumtotfiles != num {
		s := fmt.Sprintf("Not all files downloaded. Expected: %d, Downloaded:%d", num, sumtotfiles)
		t.Error(s)
		if errch != nil {
			errch <- errors.New(s)
		}
	}
	select {
	case <-errch:
		t.Fail()
	default:
	}
}

// delete existing objects that match the regex
func Test_matchdelete(t *testing.T) {
	created := createLocalBucketIfNotExists(t, proxyurl, clibucket)

	// Declare one channel per worker to pass the keyname
	keyname_chans := make([]chan string, numworkers)
	for i := 0; i < numworkers; i++ {
		// Allow a bunch of messages at a time to be written asynchronously to a channel
		keyname_chans[i] = make(chan string, 100)
	}
	// Start the worker pools
	errch := make(chan error, 100)
	var wg = &sync.WaitGroup{}
	// Get the workers started
	for i := 0; i < numworkers; i++ {
		wg.Add(1)
		go deleteFiles(keyname_chans[i], t, wg, errch, clibucket)
	}

	// list the bucket
	var msg = &dfc.GetMsg{GetPageSize: int(pagesize)}
	reslist, err := client.ListBucket(proxyurl, clibucket, msg, 0)
	if err != nil {
		t.Error(err)
		return
	}

	re, err := regexp.Compile(match)
	if err != nil {
		t.Errorf("Invalid match expression %s, err = %v", match, err)
		return
	}

	var num int
	for _, entry := range reslist.Entries {
		name := entry.Name
		if !re.MatchString(name) {
			continue
		}
		keyname_chans[num%numworkers] <- name
		if num++; num >= numfiles {
			break
		}
	}
	// Close the channels after the reading is done
	for i := 0; i < numworkers; i++ {
		close(keyname_chans[i])
	}
	wg.Wait()
	select {
	case <-errch:
		t.Fail()
	default:
	}

	if created {
		if err = client.DestroyLocalBucket(proxyurl, clibucket); err != nil {
			t.Errorf("Failed to delete local bucket: %v", err)
		}
	}
}

func Test_putdeleteRange(t *testing.T) {
	if testing.Short() {
		t.Skip("Long run only")
	}

	const (
		numFiles     = 100
		commonPrefix = "tst" // object full name: <bucket>/<commonPrefix>/<generated_name:a-####|b-####>
	)
	var sgl *dfc.SGLIO

	if err := dfc.CreateDir(DeleteDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", DeleteDir, err)
	}
	created := createLocalBucketIfNotExists(t, proxyurl, clibucket)
	errch := make(chan error, numfiles*5)
	filesput := make(chan string, numfiles)
	filesize := uint64(16 * 1024)

	if usingSG {
		sgl = dfc.NewSGLIO(filesize)
		defer sgl.Free()
	}

	filenameList := make([]string, 0, numfiles)
	for i := 0; i < numfiles/2; i++ {
		fname := fmt.Sprintf("a-%04d", i)
		filenameList = append(filenameList, fname)
		fname = fmt.Sprintf("b-%04d", i)
		filenameList = append(filenameList, fname)
	}
	fillWithRandomData(baseseed, filesize, filenameList, clibucket, t, errch, filesput, DeleteDir,
		commonPrefix, !testing.Verbose(), sgl)
	selectErr(errch, "put", t, true /* fatal - if PUT does not work then it makes no sense to continue */)
	close(filesput)

	type testParams struct {
		// title to print out while testing
		name string
		// prefix for object name
		prefix string
		// regular expression object name must match
		regexStr string
		// a range of file IDs
		rangeStr string
		// total number of files expected to delete
		delta int
	}
	tests := []testParams{
		{
			"Trying to delete files with invalid prefix",
			"file/a-", "\\d+", "0:10",
			0,
		},
		{
			"Trying to delete files out of range",
			commonPrefix + "/a-", "\\d+", fmt.Sprintf("%d:%d", numFiles+10, numFiles+110),
			0,
		},
		{
			"Deleting 10 files with prefix 'a-'",
			commonPrefix + "/a-", "\\d+", "10:19",
			10,
		},
		{
			"Deleting 20 files (short range)",
			commonPrefix + "/", "\\d+", "30:39",
			20,
		},
		{
			"Deleting 20 more files (wide range)",
			commonPrefix + "/", "2\\d+", "10:90",
			20,
		},
		{
			"Deleting files with empty range",
			commonPrefix + "/b-", "", "",
			30,
		},
	}

	totalFiles := numFiles
	for idx, test := range tests {
		msg := &dfc.GetMsg{GetPrefix: commonPrefix + "/"}
		tlogf("%d. %s\n    Prefix: [%s], range: [%s], regexp: [%s]\n", idx+1, test.name, test.prefix, test.rangeStr, test.regexStr)

		err := client.DeleteRange(proxyurl, clibucket, test.prefix, test.regexStr, test.rangeStr, true, 0)
		if err != nil {
			t.Error(err)
		}

		totalFiles -= test.delta
		bktlst, err := client.ListBucket(proxyurl, clibucket, msg, 0)
		if err != nil {
			t.Error(err)
		}
		if len(bktlst.Entries) != totalFiles {
			t.Errorf("Incorrect number of remaining files: %d, should be %d", len(bktlst.Entries), totalFiles)
		} else {
			tlogf("  %d files have been deleted\n", test.delta)
		}
	}

	tlogf("Cleaning up remained objects...\n")
	msg := &dfc.GetMsg{GetPrefix: commonPrefix + "/"}
	bktlst, err := client.ListBucket(proxyurl, clibucket, msg, 0)
	if err != nil {
		t.Errorf("Failed to get the list of remained files, err: %v\n", err)
	}
	// cleanup everything at the end
	// Declare one channel per worker to pass the keyname
	keynameChans := make([]chan string, numworkers)
	for i := 0; i < numworkers; i++ {
		// Allow a bunch of messages at a time to be written asynchronously to a channel
		keynameChans[i] = make(chan string, 100)
	}

	// Start the worker pools
	var wg = &sync.WaitGroup{}
	// Get the workers started
	for i := 0; i < numworkers; i++ {
		wg.Add(1)
		go deleteFiles(keynameChans[i], t, wg, errch, clibucket)
	}

	if usingFile {
		for name := range filesput {
			os.Remove(DeleteDir + "/" + name)
		}
	}
	num := 0
	for _, entry := range bktlst.Entries {
		keynameChans[num%numworkers] <- entry.Name
		num++
	}

	// Close the channels after the reading is done
	for i := 0; i < numworkers; i++ {
		close(keynameChans[i])
	}

	wg.Wait()
	selectErr(errch, "delete", t, false)

	if created {
		if err = client.DestroyLocalBucket(proxyurl, clibucket); err != nil {
			t.Errorf("Failed to delete local bucket: %v", err)
		}
	}
}

// PUT, then delete
func Test_putdelete(t *testing.T) {
	if testing.Short() {
		t.Skip("Long run only")
	}

	var sgl *dfc.SGLIO
	if err := dfc.CreateDir(DeleteDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", DeleteDir, err)
	}

	errch := make(chan error, numfiles)
	filesput := make(chan string, numfiles)
	filesize := uint64(512 * 1024)
	created := createLocalBucketIfNotExists(t, proxyurl, clibucket)

	if usingSG {
		sgl = dfc.NewSGLIO(filesize)
		defer sgl.Free()
	}

	putRandomFiles(baseseed, filesize, numfiles, clibucket, t, nil, errch, filesput,
		DeleteDir, DeleteStr, !testing.Verbose(), sgl)
	close(filesput)

	// Declare one channel per worker to pass the keyname
	keynameChans := make([]chan string, numworkers)
	for i := 0; i < numworkers; i++ {
		// Allow a bunch of messages at a time to be written asynchronously to a channel
		keynameChans[i] = make(chan string, 100)
	}

	// Start the worker pools
	var wg = &sync.WaitGroup{}
	// Get the workers started
	for i := 0; i < numworkers; i++ {
		wg.Add(1)
		go deleteFiles(keynameChans[i], t, wg, errch, clibucket)
	}

	num := 0
	for name := range filesput {
		if usingFile {
			os.Remove(DeleteDir + "/" + name)
		}

		keynameChans[num%numworkers] <- DeleteStr + "/" + name
		num++
	}

	// Close the channels after the reading is done
	for i := 0; i < numworkers; i++ {
		close(keynameChans[i])
	}

	wg.Wait()
	selectErr(errch, "delete", t, false)
	if created {
		if err := client.DestroyLocalBucket(proxyurl, clibucket); err != nil {
			t.Errorf("Failed to delete local bucket: %v", err)
		}
	}
}

func listObjects(t *testing.T, msg *dfc.GetMsg, bucket string, objLimit int) (*dfc.BucketList, error) {
	var (
		copy    bool
		file    *os.File
		err     error
		reslist *dfc.BucketList
	)
	tlogf("LIST %s [prefix %s]\n", bucket, msg.GetPrefix)
	fname := LocalDestDir + "/" + bucket
	if copy {
		// Write list to a local filename = bucket
		if err = dfc.CreateDir(LocalDestDir); err != nil {
			t.Errorf("Failed to create dir %s, err: %v", LocalDestDir, err)
			return nil, err
		}
		file, err = os.Create(fname)
		if err != nil {
			t.Errorf("Failed to create file %s, err: %v", fname, err)
			return nil, err
		}
	}

	totalObjs := 0
	for {
		reslist = testListBucket(t, bucket, msg, objLimit)
		if reslist == nil {
			return nil, fmt.Errorf("Failed to list bucket %s", bucket)
		}
		if copy {
			for _, m := range reslist.Entries {
				fmt.Fprintln(file, m)
			}
			t.Logf("ls bucket %s written to %s", bucket, fname)
		} else {
			for _, m := range reslist.Entries {
				if len(m.Checksum) > 8 {
					tlogf("%s %d %s [%s] %s [%v - %s]\n", m.Name, m.Size, m.Ctime, m.Version, m.Checksum[:8]+"...", m.IsCached, m.Atime)
				} else {
					tlogf("%s %d %s [%s] %s [%v - %s]\n", m.Name, m.Size, m.Ctime, m.Version, m.Checksum, m.IsCached, m.Atime)
				}
			}
			totalObjs += len(reslist.Entries)
		}

		if reslist.PageMarker == "" {
			break
		}

		msg.GetPageMarker = reslist.PageMarker
		tlogf("PageMarker for the next page: %s\n", reslist.PageMarker)
	}
	tlogf("-----------------\nTotal objects listed: %v\n", totalObjs)
	return reslist, nil
}

func Test_bucketnames(t *testing.T) {
	var (
		url = proxyurl + "/" + dfc.Rversion + "/" + dfc.Rbuckets + "/" + "*"
		r   *http.Response
		err error
	)
	tlogf("local bucket names:\n")
	urlLocalOnly := fmt.Sprintf("%s?%s=%t", url, dfc.URLParamLocal, true)
	r, err = http.Get(urlLocalOnly)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	printbucketnames(t, r)

	tlogf("all bucket names:\n")
	r, err = http.Get(url)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	printbucketnames(t, r)
}

func printbucketnames(t *testing.T, r *http.Response) {
	defer r.Body.Close()
	if r != nil && r.StatusCode >= http.StatusBadRequest {
		t.Errorf("Failed with HTTP status %d", r.StatusCode)
		return
	}
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		t.Errorf("Failed to read response body: %v", err)
		return
	}
	bucketnames := &dfc.BucketNames{}
	err = json.Unmarshal(b, bucketnames)
	if err != nil {
		t.Errorf("Failed to unmarshal bucket names, err: %v", err)
		return
	}
	pretty, err := json.MarshalIndent(bucketnames, "", "\t")
	if err != nil {
		t.Errorf("Failed to pretty-print bucket names, err: %v", err)
		return
	}
	fmt.Fprintln(os.Stdout, string(pretty))
}

func Test_coldgetmd5(t *testing.T) {
	var (
		numPuts   = 5
		filesput  = make(chan string, numPuts)
		fileslist = make([]string, 0, 100)
		errch     = make(chan error, 100)
		wg        = &sync.WaitGroup{}
		bucket    = clibucket
		totalsize = numPuts * largefilesize
		filesize  = uint64(largefilesize * 1024 * 1024)
		sgl       *dfc.SGLIO
	)

	isCloud := isCloudBucket(t, proxyurl, clibucket)
	if !isCloud {
		t.Skip("Coldgetmd5 test is for cloud buckets only")
	}

	ldir := LocalSrcDir + "/" + ColdValidStr
	if err := dfc.CreateDir(ldir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", ldir, err)
	}

	config := getConfig(proxyurl+"/"+dfc.Rversion+"/"+dfc.Rdaemon, httpclient, t)
	cksumconfig := config["cksum_config"].(map[string]interface{})
	bcoldget := cksumconfig["validate_checksum_cold_get"].(bool)

	if usingSG {
		sgl = dfc.NewSGLIO(filesize)
		defer sgl.Free()
	}

	putRandomFiles(baseseed, filesize, numPuts, bucket, t, nil, errch, filesput, ldir,
		ColdValidStr, true, sgl)
	selectErr(errch, "put", t, false)
	close(filesput) // to exit for-range
	for fname := range filesput {
		fileslist = append(fileslist, ColdValidStr+"/"+fname)
	}
	evictobjects(t, fileslist)
	// Disable Cold Get Validation
	if bcoldget {
		setConfig("validate_checksum_cold_get", strconv.FormatBool(false), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	}
	start := time.Now()
	getfromfilelist(t, bucket, errch, fileslist, false)
	curr := time.Now()
	duration := curr.Sub(start)
	if t.Failed() {
		goto cleanup
	}
	tlogf("GET %d MB without MD5 validation: %v\n", totalsize, duration)
	selectErr(errch, "get", t, false)
	evictobjects(t, fileslist)
	// Enable Cold Get Validation
	setConfig("validate_checksum_cold_get", strconv.FormatBool(true), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	if t.Failed() {
		goto cleanup
	}
	start = time.Now()
	getfromfilelist(t, bucket, errch, fileslist, true)
	curr = time.Now()
	duration = curr.Sub(start)
	tlogf("GET %d MB with MD5 validation:    %v\n", totalsize, duration)
	selectErr(errch, "get", t, false)
cleanup:
	setConfig("validate_checksum_cold_get", strconv.FormatBool(bcoldget), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	for _, fn := range fileslist {
		if usingFile {
			_ = os.Remove(LocalSrcDir + "/" + fn)
		}

		wg.Add(1)
		go client.Del(proxyurl, bucket, fn, wg, errch, !testing.Verbose())
	}
	wg.Wait()
	selectErr(errch, "delete", t, false)
	close(errch)
}

func TestHeadLocalBucket(t *testing.T) {
	err := client.CreateLocalBucket(proxyurl, TestLocalBucketName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		checkFatal(client.DestroyLocalBucket(proxyurl, TestLocalBucketName), t)
	}()

	nextTierURL := "http://foo.com"
	err = client.SetBucketProps(proxyurl, TestLocalBucketName, dfc.BucketProps{
		CloudProvider: dfc.ProviderDfc,
		NextTierURL:   nextTierURL,
		ReadPolicy:    dfc.RWPolicyNextTier,
		WritePolicy:   dfc.RWPolicyNextTier,
	})
	checkFatal(err, t)

	p, err := client.HeadBucket(proxyurl, TestLocalBucketName)
	checkFatal(err, t)

	if p.CloudProvider != dfc.ProviderDfc {
		t.Errorf("Expected cloud provider: %s, received cloud provider: %s", dfc.CloudProvider, p.CloudProvider)
	}
	if p.ReadPolicy != dfc.RWPolicyNextTier {
		t.Errorf("Expected read policy: %s, received read policy: %s", dfc.RWPolicyNextTier, p.ReadPolicy)
	}
	if p.WritePolicy != dfc.RWPolicyNextTier {
		t.Errorf("Expected write policy: %s, received write policy: %s", dfc.RWPolicyNextTier, p.WritePolicy)
	}
	if p.NextTierURL != nextTierURL {
		t.Errorf("Expected next tier URL: %s, received next tier URL: %s", nextTierURL, p.NextTierURL)
	}

}

func TestHeadCloudBucket(t *testing.T) {
	if !isCloudBucket(t, proxyurl, clibucket) {
		t.Skipf("skipping test - bucket: %s is not a cloud bucket", clibucket)
	}

	nextTierURL := "http://foo.com"
	err := client.SetBucketProps(proxyurl, clibucket, dfc.BucketProps{
		CloudProvider: dfc.ProviderAmazon,
		NextTierURL:   nextTierURL,
		ReadPolicy:    dfc.RWPolicyCloud,
		WritePolicy:   dfc.RWPolicyNextTier,
	})
	checkFatal(err, t)
	defer resetBucketProps(clibucket, t)

	p, err := client.HeadBucket(proxyurl, clibucket)
	checkFatal(err, t)

	versionModes := []string{dfc.VersionAll, dfc.VersionCloud, dfc.VersionLocal, dfc.VersionNone}
	if !stringInSlice(p.Versioning, versionModes) {
		t.Errorf("Invalid bucket %s versioning mode: %s [must be one of %s]",
			clibucket, p.Versioning, strings.Join(versionModes, ", "))
	}

	if err = dfc.ValidateCloudProvider(p.CloudProvider, false); err != nil {
		t.Error(err)
	}

	if p.ReadPolicy != dfc.RWPolicyCloud {
		t.Errorf("Expected read policy: %s, received read policy: %s", dfc.RWPolicyCloud, p.ReadPolicy)
	}
	if p.WritePolicy != dfc.RWPolicyNextTier {
		t.Errorf("Expected write policy: %s, received write policy: %s", dfc.RWPolicyNextTier, p.WritePolicy)
	}
	if p.NextTierURL != nextTierURL {
		t.Errorf("Expected next tier URL: %s, received next tier URL: %s", nextTierURL, p.NextTierURL)
	}
}

func TestHeadObject(t *testing.T) {
	if err := client.CreateLocalBucket(proxyurl, TestLocalBucketName); err != nil {
		t.Fatalf("client.CreateLocalBucket failed, err = %v", err)
	}
	defer client.DestroyLocalBucket(proxyurl, TestLocalBucketName)

	fileName := "headobject_test_file"
	fileSize := 1024
	r, frErr := readers.NewRandReader(int64(fileSize), false)
	defer r.Close()

	if frErr != nil {
		t.Fatalf("readers.NewFileReader failed, err = %v", frErr)
	}

	if err := client.Put(proxyurl, r, TestLocalBucketName, fileName, true); err != nil {
		t.Fatalf("client.Put failed, err = %v", err)
	}

	propsExp := &client.ObjectProps{Size: fileSize, Version: "1"}
	props, err := client.HeadObject(proxyurl, TestLocalBucketName, fileName)
	if err != nil {
		t.Errorf("client.HeadObject failed, err = %v", err)
	}

	if !reflect.DeepEqual(props, propsExp) {
		t.Errorf("Returned object props not correct. Expected: %v, actual: %v", propsExp, props)
	}

	props, err = client.HeadObject(proxyurl, TestLocalBucketName, "this_file_should_not_exist")
	if err == nil {
		t.Errorf("Expected non-nil error (404) from client.HeadObject, received nil error")
	}
}

func TestHeadObjectCheckCached(t *testing.T) {
	created := createLocalBucketIfNotExists(t, proxyurl, clibucket)
	fileName := "headobject_check_cached_test_file"
	fileSize := 1024
	r, err := readers.NewRandReader(int64(fileSize), false)
	defer r.Close()

	if err != nil {
		t.Fatalf("readers.NewFileReader failed, err = %v", err)
	}

	err = client.Put(proxyurl, r, clibucket, fileName, true)
	checkFatal(err, t)

	b, err := client.IsCached(proxyurl, clibucket, fileName)
	checkFatal(err, t)
	if !b {
		t.Error("Expected object to be cached, got false from client.IsCached")
	}

	err = client.Del(proxyurl, clibucket, fileName, nil, nil, true)
	checkFatal(err, t)

	b, err = client.IsCached(proxyurl, clibucket, fileName)
	checkFatal(err, t)
	if b {
		t.Error("Expected object to NOT be cached after deleting object, got true from client.IsCached")
	}

	if created {
		if err = client.DestroyLocalBucket(proxyurl, clibucket); err != nil {
			t.Errorf("Failed to delete local bucket: %v", err)
		}
	}
}

func Benchmark_get(b *testing.B) {
	var wg = &sync.WaitGroup{}
	errch := make(chan error, 100)
	for j := 0; j < b.N; j++ {
		for i := 0; i < 10; i++ {
			wg.Add(1)
			keyname := "dir" + strconv.Itoa(i%3+1) + "/a" + strconv.Itoa(i)
			go client.Get(proxyurl, clibucket, keyname, wg, errch, !testing.Verbose(), false /* validate */)
		}
		wg.Wait()
		select {
		case err := <-errch:
			b.Error(err)
		default:
		}
	}
}

func getAndCopyTmp(id int, keynames <-chan string, t *testing.T, wg *sync.WaitGroup,
	errch chan error, resch chan workres, bucket string) {
	geturl := proxyurl + "/" + dfc.Rversion + "/" + dfc.Robjects
	res := workres{0, 0}
	defer func() {
		close(resch)
		wg.Done()
	}()

	for keyname := range keynames {
		url := geturl + "/" + bucket + "/" + keyname
		written, failed := getAndCopyOne(id, t, errch, bucket, keyname, url)
		if failed {
			t.Fail()
			return
		}
		res.totfiles++
		res.totbytes += written
	}
	resch <- res
}

func getAndCopyOne(id int, t *testing.T, errch chan error, bucket, keyname, url string) (written int64, failed bool) {
	var errstr string
	t.Logf("Worker %2d: GET %q", id, url)
	resp, err := http.Get(url)
	if err == nil && resp == nil {
		err = fmt.Errorf("HTTP returned empty response")
	}

	if err != nil {
		errch <- err
		t.Error(err)
		failed = true
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		err = fmt.Errorf("Worker %2d: get key %s from bucket %s http error %d",
			id, keyname, bucket, resp.StatusCode)
		errch <- err
		t.Error(err)
		failed = true
		return
	}

	hdhash := resp.Header.Get(dfc.HeaderDfcChecksumVal)
	hdhashtype := resp.Header.Get(dfc.HeaderDfcChecksumType)

	// Create a local copy
	fname := LocalDestDir + "/" + keyname
	file, err := dfc.CreateFile(fname)
	if err != nil {
		t.Errorf("Worker %2d: Failed to create file, err: %v", id, err)
		failed = true
		return
	}
	defer func() {
		if err := file.Close(); err != nil {
			errstr = fmt.Sprintf("Failed to close file, err: %s", err)
			t.Errorf("Worker %2d: %s", id, errstr)
		}
	}()
	if hdhashtype == dfc.ChecksumXXHash {
		xx := xxhash.New64()
		written, err = dfc.ReceiveAndChecksum(file, resp.Body, nil, xx)
		if err != nil {
			t.Errorf("Worker %2d: failed to write file, err: %v", id, err)
			failed = true
			return
		}
		hashIn64 := xx.Sum64()
		hashInBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(hashInBytes, uint64(hashIn64))
		hash := hex.EncodeToString(hashInBytes)
		if hdhash != hash {
			t.Errorf("Worker %2d: header's %s %s doesn't match the file's %s", id, dfc.ChecksumXXHash, hdhash, hash)
			failed = true
			return
		}
		tlogf("Worker %2d: header's %s checksum %s matches the file's %s\n", id, dfc.ChecksumXXHash, hdhash, hash)
	} else if hdhashtype == dfc.ChecksumMD5 {
		md5 := md5.New()
		written, err = dfc.ReceiveAndChecksum(file, resp.Body, nil, md5)
		if err != nil {
			t.Errorf("Worker %2d: failed to write file, err: %v", id, err)
			return
		}
		hashInBytes := md5.Sum(nil)[:16]
		md5hash := hex.EncodeToString(hashInBytes)
		if errstr != "" {
			t.Errorf("Worker %2d: failed to compute %s, err: %s", id, dfc.ChecksumMD5, errstr)
			failed = true
			return
		}
		if hdhash != md5hash {
			t.Errorf("Worker %2d: header's %s %s doesn't match the file's %s", id, dfc.ChecksumMD5, hdhash, md5hash)
			failed = true
			return
		}
		tlogf("Worker %2d: header's %s checksum %s matches the file's %s\n", id, dfc.ChecksumMD5, hdhash, md5hash)
	} else {
		written, err = dfc.ReceiveAndChecksum(file, resp.Body, nil)
		if err != nil {
			t.Errorf("Worker %2d: failed to write file, err: %v", id, err)
			failed = true
			return
		}
	}
	return
}

func deleteFiles(keynames <-chan string, t *testing.T, wg *sync.WaitGroup, errch chan error, bucket string) {
	defer wg.Done()
	dwg := &sync.WaitGroup{}
	for keyname := range keynames {
		dwg.Add(1)
		go client.Del(proxyurl, bucket, keyname, dwg, errch, !testing.Verbose())
	}
	dwg.Wait()
}

func getMatchingKeys(regexmatch, bucket string, keynameChans []chan string, outputChan chan string, t *testing.T) int {
	var msg = &dfc.GetMsg{GetPageSize: int(pagesize)}
	reslist := testListBucket(t, bucket, msg, 0)
	if reslist == nil {
		return 0
	}

	re, err := regexp.Compile(regexmatch)
	if err != nil {
		t.Errorf("Invalid match expression %s, err = %v", match, err)
		return 0
	}

	num := 0
	numchans := len(keynameChans)
	for _, entry := range reslist.Entries {
		name := entry.Name
		if !re.MatchString(name) {
			continue
		}
		keynameChans[num%numchans] <- name
		if outputChan != nil {
			outputChan <- name
		}
		if num++; num >= numfiles {
			break
		}
	}

	return num
}

func testListBucket(t *testing.T, bucket string, msg *dfc.GetMsg, limit int) *dfc.BucketList {
	url := proxyurl + "/" + dfc.Rversion + "/" + dfc.Rbuckets + "/" + bucket
	tlogf("LIST %q (Number of objects: %d)\n", url, limit)
	reslist, err := client.ListBucket(proxyurl, bucket, msg, limit)
	if err != nil {
		t.Errorf("List bucket %s failed, err = %v", bucket, err)
		return nil
	}

	return reslist
}

// 1.	PUT file
// 2.	Change contents of the file or change XXHash
// 3.	GET file.
// Note: The following test can only work when running on a local setup
// (targets are co-located with where this test is running from, because
// it searches a local oldFileIfo system)
func TestChecksumValidateOnWarmGetForCloudBucket(t *testing.T) {
	var (
		numFiles               = 3
		fileSize        uint64 = 1024
		seed                   = baseseed + 111
		errorChannel           = make(chan error, numFiles*5)
		fileNameChannel        = make(chan string, numfiles)
		sgl             *dfc.SGLIO
		fqn             string
		fileName        string
		oldFileInfo     os.FileInfo
		newFileInfo     os.FileInfo
		errstr          string
		filesList       = make([]string, 0, numFiles)
	)

	isCloud := isCloudBucket(t, proxyurl, clibucket)
	if !isCloud {
		t.Skip("TestRegressionCloudBuckets test is for cloud buckets only")
	}

	if usingSG {
		sgl = dfc.NewSGLIO(fileSize)
		defer sgl.Free()
	}

	tlogf("Creating %d objects\n", numFiles)
	putRandomFiles(seed, fileSize, numFiles, clibucket, t, nil, errorChannel, fileNameChannel, ChecksumWarmValidateDir, ChecksumWarmValidateStr, true, sgl)

	fileName = <-fileNameChannel
	filesList = append(filesList, ChecksumWarmValidateStr+"/"+fileName)
	// Fetch the file from cloud bucket.
	_, _, err := client.Get(proxyurl, clibucket, ChecksumWarmValidateStr+"/"+fileName, nil, nil, false, true)
	if err != nil {
		t.Errorf("Failed while fetching the file from the cloud bucket. Error: [%v]", err)
	}

	fsWalkFunc := func(path string, info os.FileInfo, err error) error {
		if filepath.Base(path) == fileName && strings.Contains(path, clibucket) {
			fqn = path
		}
		return nil
	}

	config := getConfig(proxyurl+"/"+dfc.Rversion+"/"+dfc.Rdaemon, httpclient, t)
	checksumConfig := config["cksum_config"].(map[string]interface{})
	oldWarmGet := checksumConfig["validate_checksum_warm_get"].(bool)
	oldChecksum := checksumConfig["checksum"].(string)
	if !oldWarmGet {
		setConfig("validate_checksum_warm_get", fmt.Sprint("true"), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
		if t.Failed() {
			goto cleanup
		}
	}

	filepath.Walk(rootDir, fsWalkFunc)
	oldFileInfo, err = os.Stat(fqn)
	if err != nil {
		t.Errorf("Failed while reading the bucket from the local file system. Error: [%v]", err)
	}

	// Test when the contents of the file are changed
	tlogf("\nChanging contents of the file [%s]: %s\n", fileName, fqn)
	err = ioutil.WriteFile(fqn, []byte("Contents of this file have been changed."), 0644)
	checkFatal(err, t)
	validateGETUponFileChangeForChecksumValidation(t, fileName, newFileInfo, fqn, oldFileInfo)

	// Test when the xxHash of the file is changed
	fileName = <-fileNameChannel
	filesList = append(filesList, ChecksumWarmValidateStr+"/"+fileName)
	filepath.Walk(rootDir, fsWalkFunc)
	oldFileInfo, err = os.Stat(fqn)
	if err != nil {
		t.Errorf("Failed while reading the bucket from the local file system. Error: [%v]", err)
	}
	tlogf("\nChanging file xattr[%s]: %s\n", fileName, fqn)
	errstr = dfc.Setxattr(fqn, dfc.XattrXXHashVal, []byte("01234abcde"))
	if errstr != "" {
		t.Error(errstr)
	}
	validateGETUponFileChangeForChecksumValidation(t, fileName, newFileInfo, fqn, oldFileInfo)

	// Test for no checksum algo
	fileName = <-fileNameChannel
	filesList = append(filesList, ChecksumWarmValidateStr+"/"+fileName)
	filepath.Walk(rootDir, fsWalkFunc)
	setConfig("checksum", dfc.ChecksumNone, proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	if t.Failed() {
		goto cleanup
	}
	tlogf("\nChanging file xattr[%s]: %s\n", fileName, fqn)
	errstr = dfc.Setxattr(fqn, dfc.XattrXXHashVal, []byte("01234abcde"))
	if errstr != "" {
		t.Error(errstr)
	}
	_, _, err = client.Get(proxyurl, clibucket, ChecksumWarmValidateStr+"/"+fileName, nil, nil, false, true)
	if err != nil {
		t.Errorf("A GET on an object when checksum algo is none should pass. Error: %v", err)
	}

cleanup:
	// Restore old config
	setConfig("checksum", fmt.Sprint(oldChecksum), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	setConfig("validate_checksum_warm_get", fmt.Sprint(oldWarmGet), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	wg := &sync.WaitGroup{}
	for _, fn := range filesList {
		if usingFile {
			_ = os.Remove(LocalSrcDir + "/" + fn)
		}

		wg.Add(1)
		go client.Del(proxyurl, clibucket, fn, wg, errorChannel, !testing.Verbose())
	}
	wg.Wait()
	selectErr(errorChannel, "delete", t, false)
	close(errorChannel)
	close(fileNameChannel)
}

func validateGETUponFileChangeForChecksumValidation(
	t *testing.T, fileName string, newFileInfo os.FileInfo, fqn string,
	oldFileInfo os.FileInfo) {
	// Do a GET to see to check if a cold get was executed by comparing old and new size
	_, _, err := client.Get(proxyurl, clibucket, ChecksumWarmValidateStr+"/"+fileName, nil, nil, false, true)
	if err != nil {
		t.Errorf("Unable to GET file. Error: %v", err)
	}
	newFileInfo, err = os.Stat(fqn)
	if err != nil {
		t.Errorf("Failed while reading the file %s rom the local file system. Error: %v", fqn, err)
	}
	if newFileInfo.Size() != oldFileInfo.Size() {
		t.Errorf("Both files should match in size since a cold get"+"should have been executed. Expected size: %d, Actual Size: %d", oldFileInfo.Size(), newFileInfo.Size())
	}
}

// 1.	PUT file
// 2.	Change contents of the file or change XXHash
// 3.	GET file (first GET should fail with Internal Server Error and the
// 		second should fail with not found).
// Note: The following test can only work when running on a local setup
// (targets are co-located with where this test is running from, because
// it searches a local file system)
func TestChecksumValidateOnWarmGetForLocalBucket(t *testing.T) {
	var (
		numFiles        = 3
		fileNameChannel = make(chan string, numFiles)
		errorChannel    = make(chan error, 100)
		sgl             *dfc.SGLIO
		fileSize        = uint64(1024)
		seed            = int64(111)
		bucketName      = TestLocalBucketName
		fqn             string
		errstr          string
	)

	err := client.CreateLocalBucket(proxyurl, bucketName)
	checkFatal(err, t)

	defer func() {
		err = client.DestroyLocalBucket(proxyurl, bucketName)
		checkFatal(err, t)
	}()

	if usingSG {
		sgl = dfc.NewSGLIO(fileSize)
		defer sgl.Free()
	}

	putRandomFiles(seed, fileSize, numFiles, bucketName, t, nil, errorChannel, fileNameChannel, ChecksumWarmValidateDir, ChecksumWarmValidateStr, true, sgl)
	selectErr(errorChannel, "put", t, false)

	// Get Current Config
	config := getConfig(proxyurl+"/"+dfc.Rversion+"/"+dfc.Rdaemon, httpclient, t)
	checksumConfig := config["cksum_config"].(map[string]interface{})
	oldWarmGet := checksumConfig["validate_checksum_warm_get"].(bool)
	oldChecksum := checksumConfig["checksum"].(string)

	var fileName string
	fsWalkFunc := func(path string, info os.FileInfo, err error) error {
		if info.IsDir() && info.Name() == "cloud" {
			return filepath.SkipDir
		}
		if filepath.Base(path) == fileName && strings.Contains(path, bucketName) {
			fqn = path
		}
		return nil
	}

	if !oldWarmGet {
		setConfig("validate_checksum_warm_get", fmt.Sprint("true"), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
		if t.Failed() {
			goto cleanup
		}
	}

	// Test changing the file content
	fileName = <-fileNameChannel
	filepath.Walk(rootDir, fsWalkFunc)
	tlogf("Changing contents of the file [%s]: %s\n", fileName, fqn)
	err = ioutil.WriteFile(fqn, []byte("Contents of this file have been changed."), 0644)
	checkFatal(err, t)
	executeTwoGETsForChecksumValidation(bucketName, fileName, t)

	// Test changing the file xattr
	fileName = <-fileNameChannel
	filepath.Walk(rootDir, fsWalkFunc)
	tlogf("Changing file xattr[%s]: %s\n", fileName, fqn)
	errstr = dfc.Setxattr(fqn, dfc.XattrXXHashVal, []byte("01234abcde"))
	if errstr != "" {
		t.Error(errstr)
	}
	executeTwoGETsForChecksumValidation(bucketName, fileName, t)

	// Test for none checksum algo
	fileName = <-fileNameChannel
	filepath.Walk(rootDir, fsWalkFunc)
	setConfig("checksum", dfc.ChecksumNone, proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	if t.Failed() {
		goto cleanup
	}
	tlogf("Changing file xattr[%s]: %s\n", fileName, fqn)
	errstr = dfc.Setxattr(fqn, dfc.XattrXXHashVal, []byte("01234abcde"))
	if errstr != "" {
		t.Error(errstr)
	}
	_, _, err = client.Get(proxyurl, bucketName, ChecksumWarmValidateStr+"/"+fileName, nil, nil, false, true)
	if err != nil {
		t.Error("A GET on an object when checksum algo is none should pass")
	}

cleanup:
	// Restore old config
	setConfig("checksum", fmt.Sprint(oldChecksum), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	setConfig("validate_checksum_warm_get", fmt.Sprint(oldWarmGet), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	close(errorChannel)
	close(fileNameChannel)
}

func executeTwoGETsForChecksumValidation(bucket string, fName string, t *testing.T) {
	_, _, err := client.Get(proxyurl, bucket, ChecksumWarmValidateStr+"/"+fName, nil, nil, false, true)
	if err == nil {
		t.Error("Error is nil, expected internal server error on a GET for an object")
	}
	if !strings.Contains(err.Error(), "http status 500") {
		t.Errorf("Expected internal server error on a GET for a corrupted object, got [%s]", err.Error())
	}
	// Execute another GET to make sure that the object is deleted
	_, _, err = client.Get(proxyurl, bucket, ChecksumWarmValidateStr+"/"+fName, nil, nil, false, true)
	if err == nil {
		t.Error("Error is nil, expected not found on a second GET for a corrupted object")
	}
	if !strings.Contains(err.Error(), "http status 404") {
		t.Errorf("Expected Not Found on a second GET for a corrupted object, got [%s]", err.Error())
	}
}

func TestRangeRead(t *testing.T) {
	var (
		numFiles        = 1
		fileNameChannel = make(chan string, numFiles)
		errorChannel    = make(chan error, numFiles)
		sgl             *dfc.SGLIO
		fileSize        = uint64(1024)
		seed            = int64(131)
		bucketName      = clibucket
		fileName        string
	)

	if usingSG {
		sgl = dfc.NewSGLIO(fileSize)
		defer sgl.Free()
	}

	created := createLocalBucketIfNotExists(t, proxyurl, clibucket)
	putRandomFiles(seed, fileSize, numFiles, bucketName, t, nil, errorChannel, fileNameChannel, RangeGetDir, RangeGetStr, false, sgl)
	selectErr(errorChannel, "put", t, false)

	// Get Current Config
	config := getConfig(proxyurl+"/"+dfc.Rversion+"/"+dfc.Rdaemon, httpclient, t)
	checksumConfig := config["cksum_config"].(map[string]interface{})
	oldEnableReadRangeChecksum := checksumConfig["enable_read_range_checksum"].(bool)

	fileName = <-fileNameChannel
	tlogln("Testing valid cases.")
	// Validate entire object checksum is being returned
	if oldEnableReadRangeChecksum {
		setConfig("enable_read_range_checksum", fmt.Sprint(false), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
		if t.Failed() {
			goto cleanup
		}
	}
	testValidCases(fileSize, t, bucketName, fileName, true)

	// Validate only range checksum is being returned
	if !oldEnableReadRangeChecksum {
		setConfig("enable_read_range_checksum", fmt.Sprint(true), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
		if t.Failed() {
			goto cleanup
		}
	}
	testValidCases(fileSize, t, bucketName, fileName, false)

	tlogln("Testing invalid cases.")
	verifyInvalidParams(t, bucketName, fileName, "", "1")
	verifyInvalidParams(t, bucketName, fileName, "1", "")
	verifyInvalidParams(t, bucketName, fileName, "-1", "-1")
	verifyInvalidParams(t, bucketName, fileName, "1", "-1")
	verifyInvalidParams(t, bucketName, fileName, "-1", "1")
	verifyInvalidParams(t, bucketName, fileName, "1", "0")
cleanup:
	tlogln("Cleaning up...")
	wg := &sync.WaitGroup{}

	if usingFile {
		_ = os.Remove(LocalSrcDir + "/" + fileName)
	}

	wg.Add(1)
	go client.Del(proxyurl, clibucket, RangeGetStr+"/"+fileName, wg, errorChannel, !testing.Verbose())
	wg.Wait()
	selectErr(errorChannel, "delete", t, false)
	setConfig("enable_read_range_checksum", fmt.Sprint(oldEnableReadRangeChecksum), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	close(errorChannel)
	close(fileNameChannel)

	if created {
		if err := client.DestroyLocalBucket(proxyurl, clibucket); err != nil {
			t.Errorf("Failed to delete local bucket: %v", err)
		}
	}
}

func testValidCases(fileSize uint64, t *testing.T, bucketName string, fileName string, checkEntireObjCkSum bool) {
	// Read the entire file range by range
	// Read in ranges of 500 to test covered, partially covered and completely
	// uncovered ranges
	byteRange := int64(500)
	iterations := int64(fileSize) / byteRange
	for i := int64(0); i < iterations; i += byteRange {
		verifyValidRanges(t, bucketName, fileName, int64(i), byteRange, byteRange, checkEntireObjCkSum)
	}
	verifyValidRanges(t, bucketName, fileName, byteRange*iterations, byteRange, int64(fileSize)%byteRange, checkEntireObjCkSum)
	verifyValidRanges(t, bucketName, fileName, int64(fileSize)+100, byteRange, 0, checkEntireObjCkSum)
}

func verifyValidRanges(t *testing.T, bucketName string, fileName string,
	offset int64, length int64, expectedLength int64, checkEntireObjCksum bool) {
	var fqn string
	fsWalkFunc := func(path string, info os.FileInfo, err error) error {
		if filepath.Base(path) == fileName && strings.Contains(path, bucketName) {
			fqn = path
		}
		return nil
	}
	filepath.Walk(rootDir, fsWalkFunc)

	q := url.Values{}
	q.Add(dfc.URLParamOffset, strconv.FormatInt(offset, 10))
	q.Add(dfc.URLParamLength, strconv.FormatInt(length, 10))
	var b bytes.Buffer
	w := bufio.NewWriter(&b)
	_, _, err := client.GetFileWithQuery(proxyurl, bucketName, RangeGetStr+"/"+fileName, nil, nil, true, true, w, q)
	if err != nil {
		if !checkEntireObjCksum {
			t.Errorf("Failed to get object %s/%s! Error: %v", bucketName, fileName, err)
		} else {
			if ckErr, ok := err.(client.InvalidCksumError); ok {
				file, err := os.Open(fqn)
				if err != nil {
					t.Fatalf("Unable to open file: %s. Error:  %v", fqn, err)
				}
				defer file.Close()
				hash, errstr := dfc.ComputeXXHash(file, nil)
				if errstr != "" {
					t.Errorf("Unable to compute cksum of file: %s. Error:  %s", fqn, errstr)
				}
				if hash != ckErr.ExpectedHash {
					t.Errorf("Expected entire object checksum [%s], checksum returned in response [%s]", ckErr.ExpectedHash, hash)
				}
			} else {
				t.Errorf("Unexpected error returned [%v].", err)
			}
		}
	}
	err = w.Flush()
	if err != nil {
		t.Errorf("Unable to flush read bytes to buffer. Error:  %v", err)
	}

	file, err := os.Open(fqn)
	if err != nil {
		t.Fatalf("Unable to open file: %s. Error:  %v", fqn, err)
	}
	defer file.Close()
	outputBytes := b.Bytes()
	sectionReader := io.NewSectionReader(file, offset, length)
	expectedBytesBuffer := new(bytes.Buffer)
	_, err = expectedBytesBuffer.ReadFrom(sectionReader)
	if err != nil {
		t.Errorf("Unable to read the file %s, from offset: %d and length: %d. Error: %v", fqn, offset, length, err)
	}
	expectedBytes := expectedBytesBuffer.Bytes()
	if len(outputBytes) != len(expectedBytes) {
		t.Errorf("Bytes length mismatch. Expected bytes: [%d]. Actual bytes: [%d]", len(expectedBytes), len(outputBytes))
	}
	if int64(len(outputBytes)) != expectedLength {
		t.Errorf("Returned bytes don't match expected length. Expected length: [%d]. Output length: [%d]", length, len(outputBytes))
	}
	for i := 0; i < len(expectedBytes); i++ {
		if expectedBytes[i] != outputBytes[i] {
			t.Errorf("Byte mismatch. Expected: %v, Actual: %v", string(expectedBytes), string(outputBytes))
		}
	}
}

func verifyInvalidParams(t *testing.T, bucketName string, fileName string, offset string, length string) {
	q := url.Values{}
	q.Add(dfc.URLParamOffset, offset)
	q.Add(dfc.URLParamLength, length)
	_, _, err := client.GetWithQuery(proxyurl, bucketName, RangeGetStr+"/"+fileName, nil, nil, false, true, q)
	if err == nil {
		t.Errorf("Must fail for invalid offset %s and length %s combination.", offset, length)
	}
}

func Test_checksum(t *testing.T) {
	if testing.Short() {
		t.Skip("Long run only")
	}

	var (
		filesput    = make(chan string, 100)
		fileslist   = make([]string, 0, 100)
		errch       = make(chan error, 100)
		bucket      = clibucket
		start, curr time.Time
		duration    time.Duration
		numPuts     = 5
		filesize    = uint64(largefilesize * 1024 * 1024)
		sgl         *dfc.SGLIO
		totalio     = numPuts * largefilesize
	)

	created := createLocalBucketIfNotExists(t, proxyurl, bucket)
	ldir := LocalSrcDir + "/" + ChksumValidStr
	if err := dfc.CreateDir(ldir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", ldir, err)
	}

	// Get Current Config
	config := getConfig(proxyurl+"/"+dfc.Rversion+"/"+dfc.Rdaemon, httpclient, t)
	cksumconfig := config["cksum_config"].(map[string]interface{})
	ocoldget := cksumconfig["validate_checksum_cold_get"].(bool)
	ochksum := cksumconfig["checksum"].(string)

	if usingSG {
		sgl = dfc.NewSGLIO(filesize)
		defer sgl.Free()
	}

	putRandomFiles(0, filesize, int(numPuts), bucket, t, nil, errch, filesput, ldir,
		ChksumValidStr, true, sgl)
	selectErr(errch, "put", t, false)
	close(filesput) // to exit for-range
	for fname := range filesput {
		if fname != "" {
			fileslist = append(fileslist, ChksumValidStr+"/"+fname)
		}
	}
	// Delete it from cache.
	evictobjects(t, fileslist)
	// Disable checkum
	if ochksum != dfc.ChecksumNone {
		setConfig("checksum", dfc.ChecksumNone, proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	}
	if t.Failed() {
		goto cleanup
	}
	// Disable Cold Get Validation
	if ocoldget {
		setConfig("validate_checksum_cold_get", fmt.Sprint("false"), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	}
	if t.Failed() {
		goto cleanup
	}
	start = time.Now()
	getfromfilelist(t, bucket, errch, fileslist, false)
	curr = time.Now()
	duration = curr.Sub(start)
	if t.Failed() {
		goto cleanup
	}
	tlogf("GET %d MB without any checksum validation: %v\n", totalio, duration)
	selectErr(errch, "get", t, false)
	evictobjects(t, fileslist)
	switch clichecksum {
	case "all":
		setConfig("checksum", dfc.ChecksumXXHash, proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
		setConfig("validate_checksum_cold_get", fmt.Sprint("true"), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
		if t.Failed() {
			goto cleanup
		}
	case dfc.ChecksumXXHash:
		setConfig("checksum", dfc.ChecksumXXHash, proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
		if t.Failed() {
			goto cleanup
		}
	case ColdMD5str:
		setConfig("validate_checksum_cold_get", fmt.Sprint("true"), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
		if t.Failed() {
			goto cleanup
		}
	case dfc.ChecksumNone:
		// do nothing
		tlogf("Checksum validation has been disabled \n")
		goto cleanup
	default:
		fmt.Fprintf(os.Stdout, "Checksum is either not set or invalid\n")
		goto cleanup
	}
	start = time.Now()
	getfromfilelist(t, bucket, errch, fileslist, true)
	curr = time.Now()
	duration = curr.Sub(start)
	tlogf("GET %d MB and validate checksum (%s): %v\n", totalio, clichecksum, duration)
	selectErr(errch, "get", t, false)
cleanup:
	deletefromfilelist(t, bucket, errch, fileslist)
	// restore old config
	setConfig("checksum", fmt.Sprint(ochksum), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	setConfig("validate_checksum_cold_get", fmt.Sprint(ocoldget), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)

	if created {
		if err := client.DestroyLocalBucket(proxyurl, bucket); err != nil {
			t.Errorf("Failed to delete local bucket: %v", err)
		}
	}

	return
}

func deletefromfilelist(t *testing.T, bucket string, errch chan error, fileslist []string) {
	wg := &sync.WaitGroup{}
	// Delete local file and objects from bucket
	for _, fn := range fileslist {
		if usingFile {
			err := os.Remove(LocalSrcDir + "/" + fn)
			if err != nil {
				t.Error(err)
			}
		}

		wg.Add(1)
		go client.Del(proxyurl, bucket, fn, wg, errch, true)
	}

	wg.Wait()
	selectErr(errch, "delete", t, false)
	close(errch)
}

func getfromfilelist(t *testing.T, bucket string, errch chan error, fileslist []string, validate bool) {
	getsGroup := &sync.WaitGroup{}
	for i := 0; i < len(fileslist); i++ {
		if fileslist[i] != "" {
			getsGroup.Add(1)
			go client.Get(proxyurl, bucket, fileslist[i], getsGroup, errch, !testing.Verbose(), validate)
		}
	}
	getsGroup.Wait()
}

func evictobjects(t *testing.T, fileslist []string) {
	err := client.EvictList(proxyurl, clibucket, fileslist, true, 0)
	if err != nil {
		t.Errorf("Evict bucket %s failed, err = %v", clibucket, err)
	}
}

func createLocalBucketIfNotExists(t *testing.T, proxyurl, bucket string) (created bool) {
	buckets, err := client.ListBuckets(proxyurl, false)
	if err != nil {
		t.Fatalf("Failed to read bucket list: %v", err)
	}

	if stringInSlice(bucket, buckets.Local) || stringInSlice(bucket, buckets.Cloud) {
		return false
	}

	err = client.CreateLocalBucket(proxyurl, clibucket)
	if err != nil {
		t.Fatalf("Failed to create local bucket %s: %v", clibucket, err)
	}

	return true
}

func isCloudBucket(t *testing.T, proxyurl, bucket string) bool {
	buckets, err := client.ListBuckets(proxyurl, false)
	if err != nil {
		t.Fatalf("Failed to read bucket names: %v", err)
	}

	return stringInSlice(bucket, buckets.Cloud)
}
