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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/memsys"
	"github.com/NVIDIA/dfcpub/tutils"
	"github.com/OneOfOne/xxhash"
	"github.com/json-iterator/go"
)

// worker's result
type workres struct {
	totfiles int
	totbytes int64
}

func Test_download(t *testing.T) {
	proxyURL := getPrimaryURL(t, proxyURLRO)

	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skip("test requires a Cloud bucket")
	}

	if err := tutils.Tcping(proxyURL); err != nil {
		tutils.Logf("%s: %v\n", proxyURL, err)
		os.Exit(1)
	}

	// Declare one channel per worker to pass the keyname
	keynameChans := make([]chan string, numworkers)
	resultChans := make([]chan workres, numworkers)
	filesCreated := make(chan string, numfiles)

	defer func() {
		close(filesCreated)
		var err error
		for file := range filesCreated {
			e := os.Remove(filepath.Join(LocalDestDir, file))
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
	errCh := make(chan error, 100)

	var wg = &sync.WaitGroup{}
	// Get the workers started
	for i := 0; i < numworkers; i++ {
		wg.Add(1)
		// Read the response and write it to a file
		go getAndCopyTmp(proxyURL, i, keynameChans[i], t, wg, errCh, resultChans[i], clibucket)
	}

	num := getMatchingKeys(proxyURL, match, clibucket, keynameChans, filesCreated, t)

	t.Logf("Expecting to get %d objects\n", num)

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
			i, res.totfiles, float64(res.totbytes)/1000/1000, res.totbytes)
	}
	t.Logf("\nSummary: %d workers, %d objects, total size %.2f MB (%d B)",
		numworkers, sumtotfiles, float64(sumtotbytes)/1000/1000, sumtotbytes)

	if sumtotfiles != num {
		s := fmt.Sprintf("Not all files downloaded. Expected: %d, Downloaded:%d", num, sumtotfiles)
		t.Error(s)
		if errCh != nil {
			errCh <- errors.New(s)
		}
	}
	select {
	case <-errCh:
		t.Fail()
	default:
	}
}

// delete existing objects that match the regex
func Test_matchdelete(t *testing.T) {
	proxyURL := getPrimaryURL(t, proxyURLRO)
	if created := createLocalBucketIfNotExists(t, proxyURL, clibucket); created {
		defer destroyLocalBucket(t, proxyURL, clibucket)
	}

	// Declare one channel per worker to pass the keyname
	keyname_chans := make([]chan string, numworkers)
	for i := 0; i < numworkers; i++ {
		// Allow a bunch of messages at a time to be written asynchronously to a channel
		keyname_chans[i] = make(chan string, 100)
	}
	// Start the worker pools
	errCh := make(chan error, 100)
	var wg = &sync.WaitGroup{}
	// Get the workers started
	for i := 0; i < numworkers; i++ {
		wg.Add(1)
		go deleteFiles(proxyURL, keyname_chans[i], t, wg, errCh, clibucket)
	}

	// list the bucket
	var msg = &cmn.GetMsg{GetPageSize: int(pagesize)}
	baseParams := tutils.BaseAPIParams(proxyURL)
	reslist, err := api.ListBucket(baseParams, clibucket, msg, 0)
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
	case <-errCh:
		t.Fail()
	default:
	}
}

func Test_putdeleteRange(t *testing.T) {
	if numfiles < 10 || numfiles%10 != 0 {
		t.Skip("numfiles must be a positive multiple of 10")
	}

	if testing.Short() {
		t.Skip(skipping)
	}

	const (
		commonPrefix = "tst" // object full name: <bucket>/<commonPrefix>/<generated_name:a-####|b-####>
		objSize      = 16 * 1024
	)
	var sgl *memsys.SGL
	proxyURL := getPrimaryURL(t, proxyURLRO)

	if err := cmn.CreateDir(DeleteDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", DeleteDir, err)
	}
	if created := createLocalBucketIfNotExists(t, proxyURL, clibucket); created {
		defer destroyLocalBucket(t, proxyURL, clibucket)
	}

	errCh := make(chan error, numfiles*5)
	objsPutCh := make(chan string, numfiles)

	if usingSG {
		sgl = tutils.Mem2.NewSGL(objSize)
		defer sgl.Free()
	}
	objList := make([]string, 0, numfiles)
	for i := 0; i < numfiles/2; i++ {
		fname := fmt.Sprintf("a-%04d", i)
		objList = append(objList, fname)
		fname = fmt.Sprintf("b-%04d", i)
		objList = append(objList, fname)
	}
	tutils.PutObjsFromList(proxyURL, clibucket, DeleteDir, readerType, commonPrefix, objSize, objList, errCh, objsPutCh, sgl)
	selectErr(errCh, "put", t, true /* fatal - if PUT does not work then it makes no sense to continue */)
	close(objsPutCh)
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
			commonPrefix + "/a-", "\\d+", fmt.Sprintf("%d:%d", numfiles+10, numfiles+110),
			0,
		},
		{
			fmt.Sprintf("Deleting %d files with prefix 'a-'", numfiles/10),
			commonPrefix + "/a-", "\\d+", fmt.Sprintf("%d:%d", (numfiles-numfiles/5)/2, numfiles/2),
			numfiles / 10,
		},
		{
			fmt.Sprintf("Deleting %d files (short range)", numfiles/5),
			commonPrefix + "/", "\\d+", fmt.Sprintf("%d:%d", 1, numfiles/10),
			numfiles / 5,
		},
		{
			"Deleting files with empty range",
			commonPrefix + "/b-", "", "",
			(numfiles - numfiles/5) / 2,
		},
	}

	totalFiles := numfiles
	baseParams := tutils.BaseAPIParams(proxyURL)
	for idx, test := range tests {
		msg := &cmn.GetMsg{GetPrefix: commonPrefix + "/"}
		tutils.Logf("%d. %s\n    Prefix: [%s], range: [%s], regexp: [%s]\n", idx+1, test.name, test.prefix, test.rangeStr, test.regexStr)

		err := tutils.DeleteRange(proxyURL, clibucket, test.prefix, test.regexStr, test.rangeStr, true, 0)
		if err != nil {
			t.Error(err)
		}

		totalFiles -= test.delta
		bktlst, err := api.ListBucket(baseParams, clibucket, msg, 0)
		if err != nil {
			t.Error(err)
		}
		if len(bktlst.Entries) != totalFiles {
			t.Errorf("Incorrect number of remaining files: %d, should be %d", len(bktlst.Entries), totalFiles)
		} else {
			tutils.Logf("  %d files have been deleted\n", test.delta)
		}
	}

	tutils.Logf("Cleaning up remained objects...\n")
	msg := &cmn.GetMsg{GetPrefix: commonPrefix + "/"}
	bktlst, err := api.ListBucket(baseParams, clibucket, msg, 0)
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
		go deleteFiles(proxyURL, keynameChans[i], t, wg, errCh, clibucket)
	}

	if usingFile {
		for name := range objsPutCh {
			os.Remove(filepath.Join(DeleteDir, name))
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
	selectErr(errCh, "delete", t, false)
}

// PUT, then delete
func Test_putdelete(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	var sgl *memsys.SGL
	if err := cmn.CreateDir(DeleteDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", DeleteDir, err)
	}

	errCh := make(chan error, numfiles)
	filesPutCh := make(chan string, numfiles)
	const filesize = 512 * 1024
	proxyURL := getPrimaryURL(t, proxyURLRO)
	if created := createLocalBucketIfNotExists(t, proxyURL, clibucket); created {
		defer destroyLocalBucket(t, proxyURL, clibucket)
	}

	if usingSG {
		sgl = tutils.Mem2.NewSGL(filesize)
		defer sgl.Free()
	}
	tutils.PutRandObjs(proxyURL, clibucket, DeleteDir, readerType, DeleteStr, filesize, numfiles, errCh, filesPutCh, sgl)
	close(filesPutCh)
	selectErr(errCh, "put", t, true)

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
		go deleteFiles(proxyURL, keynameChans[i], t, wg, errCh, clibucket)
	}

	num := 0
	for name := range filesPutCh {
		if usingFile {
			os.Remove(filepath.Join(DeleteDir, name))
		}

		keynameChans[num%numworkers] <- filepath.Join(DeleteStr, name)
		num++
	}

	// Close the channels after the reading is done
	for i := 0; i < numworkers; i++ {
		close(keynameChans[i])
	}

	wg.Wait()
	selectErr(errCh, "delete", t, false)
}

func listObjects(t *testing.T, proxyURL string, msg *cmn.GetMsg, bucket string, objLimit int) (*cmn.BucketList, error) {
	var (
		copy    bool
		file    *os.File
		err     error
		reslist *cmn.BucketList
	)
	tutils.Logf("LIST %s [prefix %s]\n", bucket, msg.GetPrefix)
	fname := filepath.Join(LocalDestDir, bucket)
	if copy {
		// Write list to a local filename = bucket
		if err = cmn.CreateDir(LocalDestDir); err != nil {
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
		reslist = testListBucket(t, proxyURL, bucket, msg, objLimit)
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
					tutils.Logf("%s %d %s [%s] %s [%v - %s]\n", m.Name, m.Size, m.Ctime, m.Version, m.Checksum[:8]+"...", m.IsCached, m.Atime)
				} else {
					tutils.Logf("%s %d %s [%s] %s [%v - %s]\n", m.Name, m.Size, m.Ctime, m.Version, m.Checksum, m.IsCached, m.Atime)
				}
			}
			totalObjs += len(reslist.Entries)
		}

		if reslist.PageMarker == "" {
			break
		}

		msg.GetPageMarker = reslist.PageMarker
		tutils.Logf("PageMarker for the next page: %s\n", reslist.PageMarker)
	}
	tutils.Logf("-----------------\nTotal objects listed: %v\n", totalObjs)
	return reslist, nil
}

func Test_bucketnames(t *testing.T) {
	var (
		err error
	)
	buckets, err := api.GetBucketNames(tutils.DefaultBaseAPIParams(t), false)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	tutils.Logf("local bucket names:\n")
	printBucketNames(t, buckets.Local)

	tutils.Logf("cloud bucket names:\n")
	printBucketNames(t, buckets.Cloud)
}

func printBucketNames(t *testing.T, bucketNames []string) {
	pretty, err := jsoniter.MarshalIndent(bucketNames, "", " ")
	if err != nil {
		t.Errorf("Failed to pretty-print bucket names, err: %v", err)
		return
	}
	fmt.Fprintln(os.Stdout, string(pretty))
}

func Test_coldgetmd5(t *testing.T) {
	const filesize = largefilesize * 1024 * 1024
	var (
		numPuts    = 5
		filesPutCh = make(chan string, numPuts)
		fileslist  = make([]string, 0, 100)
		errCh      = make(chan error, 100)
		wg         = &sync.WaitGroup{}
		bucket     = clibucket
		totalsize  = numPuts * largefilesize
		sgl        *memsys.SGL
		proxyURL   = getPrimaryURL(t, proxyURLRO)
	)

	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skip("Test_coldgetmd5 requires a cloud bucket")
	}

	ldir := filepath.Join(LocalSrcDir, ColdValidStr)
	if err := cmn.CreateDir(ldir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", ldir, err)
	}

	config := getDaemonConfig(t, proxyURL)
	bcoldget := config.Cksum.ValidateColdGet

	if usingSG {
		sgl = tutils.Mem2.NewSGL(filesize)
		defer sgl.Free()
	}
	tutils.PutRandObjs(proxyURL, bucket, ldir, readerType, ColdValidStr, filesize, numPuts, errCh, filesPutCh, sgl)
	selectErr(errCh, "put", t, false)
	close(filesPutCh) // to exit for-range
	for fname := range filesPutCh {
		fileslist = append(fileslist, filepath.Join(ColdValidStr, fname))
	}
	evictobjects(t, proxyURL, fileslist)
	// Disable Cold Get Validation
	if bcoldget {
		setClusterConfig(t, proxyURL, "validate_checksum_cold_get", false)
	}
	start := time.Now()
	getfromfilelist(t, proxyURL, bucket, errCh, fileslist, false)
	curr := time.Now()
	duration := curr.Sub(start)
	if t.Failed() {
		goto cleanup
	}
	tutils.Logf("GET %d MB without MD5 validation: %v\n", totalsize, duration)
	selectErr(errCh, "get", t, false)
	evictobjects(t, proxyURL, fileslist)
	// Enable Cold Get Validation
	setClusterConfig(t, proxyURL, "validate_checksum_cold_get", true)
	if t.Failed() {
		goto cleanup
	}
	start = time.Now()
	getfromfilelist(t, proxyURL, bucket, errCh, fileslist, true)
	curr = time.Now()
	duration = curr.Sub(start)
	tutils.Logf("GET %d MB with MD5 validation:    %v\n", totalsize, duration)
	selectErr(errCh, "get", t, false)
cleanup:
	setClusterConfig(t, proxyURL, "validate_checksum_cold_get", bcoldget)
	for _, fn := range fileslist {
		if usingFile {
			_ = os.Remove(filepath.Join(LocalSrcDir, fn))
		}

		wg.Add(1)
		go tutils.Del(proxyURL, bucket, fn, wg, errCh, !testing.Verbose())
	}
	wg.Wait()
	selectErr(errCh, "delete", t, false)
	close(errCh)
}

func TestHeadLocalBucket(t *testing.T) {
	var (
		proxyURL   = getPrimaryURL(t, proxyURLRO)
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	createFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer destroyLocalBucket(t, proxyURL, TestLocalBucketName)

	bucketProps := defaultBucketProps()
	bucketProps.ValidateWarmGet = true
	bucketProps.LRUEnabled = true

	err := api.SetBucketProps(baseParams, TestLocalBucketName, bucketProps)
	tutils.CheckFatal(err, t)

	p, err := api.HeadBucket(baseParams, TestLocalBucketName)
	tutils.CheckFatal(err, t)

	validateBucketProps(t, bucketProps, *p)
}

func TestHeadCloudBucket(t *testing.T) {
	var (
		proxyURL   = getPrimaryURL(t, proxyURLRO)
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skip(fmt.Sprintf("%s requires a cloud bucket", t.Name()))
	}
	bucketProps := defaultBucketProps()
	bucketProps.CloudProvider = cmn.ProviderAmazon
	bucketProps.ValidateWarmGet = true
	bucketProps.ValidateColdGet = true
	bucketProps.LRUEnabled = true

	err := api.SetBucketProps(baseParams, clibucket, bucketProps)
	tutils.CheckFatal(err, t)
	defer resetBucketProps(proxyURL, clibucket, t)

	p, err := api.HeadBucket(baseParams, clibucket)
	tutils.CheckFatal(err, t)

	versionModes := []string{cmn.VersionAll, cmn.VersionCloud, cmn.VersionLocal, cmn.VersionNone}
	if !cmn.StringInSlice(p.Versioning, versionModes) {
		t.Errorf("Invalid bucket %s versioning mode: %s [must be one of %s]",
			clibucket, p.Versioning, strings.Join(versionModes, ", "))
	}

	validateBucketProps(t, bucketProps, *p)
}

func TestHeadObject(t *testing.T) {
	var (
		proxyURL = getPrimaryURL(t, proxyURLRO)
		objName  = "headobject_test_obj"
		objSize  = 1024
	)
	createFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer destroyLocalBucket(t, proxyURL, TestLocalBucketName)

	r, rrErr := tutils.NewRandReader(int64(objSize), false)
	if rrErr != nil {
		t.Fatalf("tutils.NewRandReader failed, err = %v", rrErr)
	}
	defer r.Close()
	if err := api.PutObject(tutils.DefaultBaseAPIParams(t), TestLocalBucketName, objName, r.XXHash(), r); err != nil {
		t.Fatalf("api.PutObject failed, err = %v", err)
	}

	propsExp := &cmn.ObjectProps{Size: objSize, Version: "1"}
	props, err := api.HeadObject(tutils.DefaultBaseAPIParams(t), TestLocalBucketName, objName)
	if err != nil {
		t.Errorf("api.HeadObject failed, err = %v", err)
	}

	if !reflect.DeepEqual(props, propsExp) {
		t.Errorf("Returned object props not correct. Expected: %v, actual: %v", propsExp, props)
	}

	_, err = api.HeadObject(tutils.DefaultBaseAPIParams(t), TestLocalBucketName, "this_object_should_not_exist")
	if err == nil {
		t.Errorf("Expected non-nil error (404) from api.HeadObject, received nil error")
	}
}

func TestHeadObjectCheckCached(t *testing.T) {
	var (
		proxyURL = getPrimaryURL(t, proxyURLRO)
		fileName = "headobject_check_cached_test_file"
		fileSize = 1024
	)

	if created := createLocalBucketIfNotExists(t, proxyURL, clibucket); created {
		defer destroyLocalBucket(t, proxyURL, clibucket)
	}
	r, err := tutils.NewRandReader(int64(fileSize), false)
	if err != nil {
		t.Fatalf("tutils.NewRandReader failed, err = %v", err)
	}
	defer r.Close()

	err = api.PutObject(tutils.DefaultBaseAPIParams(t), clibucket, fileName, r.XXHash(), r)
	tutils.CheckFatal(err, t)

	b, err := tutils.IsCached(proxyURL, clibucket, fileName)
	tutils.CheckFatal(err, t)
	if !b {
		t.Error("Expected object to be cached, got false from tutils.IsCached")
	}

	err = tutils.Del(proxyURL, clibucket, fileName, nil, nil, true)
	tutils.CheckFatal(err, t)

	b, err = tutils.IsCached(proxyURL, clibucket, fileName)
	tutils.CheckFatal(err, t)
	if b {
		t.Error("Expected object to NOT be cached after deleting object, got true from tutils.IsCached")
	}
}

func getAndCopyTmp(proxyURL string, id int, keynames <-chan string, t *testing.T, wg *sync.WaitGroup,
	errCh chan error, resch chan workres, bucket string) {
	geturl := proxyURL + cmn.URLPath(cmn.Version, cmn.Objects)
	res := workres{0, 0}
	defer func() {
		close(resch)
		wg.Done()
	}()

	for keyname := range keynames {
		url := geturl + cmn.URLPath(bucket, keyname)
		written, failed := getAndCopyOne(id, t, errCh, bucket, keyname, url)
		if failed {
			t.Fail()
			return
		}
		res.totfiles++
		res.totbytes += written
	}
	resch <- res
}

func getAndCopyOne(id int, t *testing.T, errCh chan error, bucket, keyname, url string) (written int64, failed bool) {
	var errstr string
	t.Logf("Worker %2d: GET %q", id, url)
	resp, err := http.Get(url)
	if err == nil && resp == nil {
		err = fmt.Errorf("HTTP returned empty response")
	}

	if err != nil {
		errCh <- err
		t.Error(err)
		failed = true
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		err = fmt.Errorf("Worker %2d: get key %s from bucket %s http error %d",
			id, keyname, bucket, resp.StatusCode)
		errCh <- err
		t.Error(err)
		failed = true
		return
	}

	hdhash := resp.Header.Get(cmn.HeaderDFCChecksumVal)
	hdhashtype := resp.Header.Get(cmn.HeaderDFCChecksumType)

	// Create a local copy
	fname := filepath.Join(LocalDestDir, keyname)
	file, err := cmn.CreateFile(fname)
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
	if hdhashtype == cmn.ChecksumXXHash {
		xx := xxhash.New64()
		written, err = cmn.ReceiveAndChecksum(file, resp.Body, nil, xx)
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
			t.Errorf("Worker %2d: header's %s %s doesn't match the file's %s", id, cmn.ChecksumXXHash, hdhash, hash)
			failed = true
			return
		}
		tutils.Logf("Worker %2d: header's %s checksum %s matches the file's %s\n", id, cmn.ChecksumXXHash, hdhash, hash)
	} else if hdhashtype == cmn.ChecksumMD5 {
		md5 := md5.New()
		written, err = cmn.ReceiveAndChecksum(file, resp.Body, nil, md5)
		if err != nil {
			t.Errorf("Worker %2d: failed to write file, err: %v", id, err)
			return
		}
		hashInBytes := md5.Sum(nil)[:16]
		md5hash := hex.EncodeToString(hashInBytes)
		if errstr != "" {
			t.Errorf("Worker %2d: failed to compute %s, err: %s", id, cmn.ChecksumMD5, errstr)
			failed = true
			return
		}
		if hdhash != md5hash {
			t.Errorf("Worker %2d: header's %s %s doesn't match the file's %s", id, cmn.ChecksumMD5, hdhash, md5hash)
			failed = true
			return
		}
		tutils.Logf("Worker %2d: header's %s checksum %s matches the file's %s\n", id, cmn.ChecksumMD5, hdhash, md5hash)
	} else {
		written, err = cmn.ReceiveAndChecksum(file, resp.Body, nil)
		if err != nil {
			t.Errorf("Worker %2d: failed to write file, err: %v", id, err)
			failed = true
			return
		}
	}
	return
}

func deleteFiles(proxyURL string, keynames <-chan string, t *testing.T, wg *sync.WaitGroup, errCh chan error, bucket string) {
	defer wg.Done()
	dwg := &sync.WaitGroup{}
	for keyname := range keynames {
		dwg.Add(1)
		go tutils.Del(proxyURL, bucket, keyname, dwg, errCh, true)
	}
	dwg.Wait()
}

func getMatchingKeys(proxyURL string, regexmatch, bucket string, keynameChans []chan string, outputChan chan string, t *testing.T) int {
	var msg = &cmn.GetMsg{GetPageSize: int(pagesize)}
	reslist := testListBucket(t, proxyURL, bucket, msg, 0)
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

func testListBucket(t *testing.T, proxyURL, bucket string, msg *cmn.GetMsg, limit int) *cmn.BucketList {
	tutils.Logf("LIST bucket %s (%s)\n", bucket, proxyURL)
	baseParams := tutils.BaseAPIParams(proxyURL)
	reslist, err := api.ListBucket(baseParams, bucket, msg, limit)
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
	const fileSize = 1024
	var (
		numFiles    = 3
		errCh       = make(chan error, numFiles*5)
		fileNameCh  = make(chan string, numfiles)
		sgl         *memsys.SGL
		fqn         string
		fileName    string
		oldFileInfo os.FileInfo
		errstr      string
		filesList   = make([]string, 0, numFiles)
		proxyURL    = getPrimaryURL(t, proxyURLRO)
	)

	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skip(fmt.Sprintf("test %q requires a cloud bucket", t.Name()))
	}

	if tutils.DockerRunning() {
		t.Skip(fmt.Sprintf("test %q requires Xattributes to be set, doesn't work with docker", t.Name()))
	}

	if usingSG {
		sgl = tutils.Mem2.NewSGL(fileSize)
		defer sgl.Free()
	}

	tutils.Logf("Creating %d objects\n", numFiles)
	tutils.PutRandObjs(proxyURL, clibucket, ChecksumWarmValidateDir, readerType, ChecksumWarmValidateStr, fileSize, numFiles, errCh, fileNameCh, sgl)
	selectErr(errCh, "put", t, false)

	fileName = <-fileNameCh
	filesList = append(filesList, filepath.Join(ChecksumWarmValidateStr, fileName))
	// Fetch the file from cloud bucket.
	_, err := api.GetObjectWithValidation(tutils.DefaultBaseAPIParams(t), clibucket, path.Join(ChecksumWarmValidateStr, fileName))
	if err != nil {
		t.Errorf("Failed while fetching the file from the cloud bucket. Error: [%v]", err)
	}

	fsWalkFunc := func(path string, info os.FileInfo, err error) error {
		if filepath.Base(path) == fileName && strings.Contains(path, filepath.Join(clibucket, ChecksumWarmValidateStr)) {
			fqn = path
		}
		return nil
	}

	config := getDaemonConfig(t, proxyURL)
	oldWarmGet := config.Cksum.ValidateWarmGet
	oldChecksum := config.Cksum.Checksum
	if !oldWarmGet {
		setClusterConfig(t, proxyURL, "validate_checksum_warm_get", true)
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
	tutils.Logf("\nChanging contents of the file [%s]: %s\n", fileName, fqn)
	err = ioutil.WriteFile(fqn, []byte("Contents of this file have been changed."), 0644)
	tutils.CheckFatal(err, t)
	validateGETUponFileChangeForChecksumValidation(t, proxyURL, fileName, fqn, oldFileInfo)

	// Test when the xxHash of the file is changed
	fileName = <-fileNameCh
	filesList = append(filesList, filepath.Join(ChecksumWarmValidateStr, fileName))
	filepath.Walk(rootDir, fsWalkFunc)
	oldFileInfo, err = os.Stat(fqn)
	if err != nil {
		t.Errorf("Failed while reading the bucket from the local file system. Error: [%v]", err)
	}
	tutils.Logf("\nChanging file xattr[%s]: %s\n", fileName, fqn)
	errstr = fs.SetXattr(fqn, cmn.XattrXXHashVal, []byte("01234abcde"))
	if errstr != "" {
		t.Error(errstr)
	}
	validateGETUponFileChangeForChecksumValidation(t, proxyURL, fileName, fqn, oldFileInfo)

	// Test for no checksum algo
	fileName = <-fileNameCh
	filesList = append(filesList, filepath.Join(ChecksumWarmValidateStr, fileName))
	filepath.Walk(rootDir, fsWalkFunc)
	setClusterConfig(t, proxyURL, "checksum", cmn.ChecksumNone)
	if t.Failed() {
		goto cleanup
	}
	tutils.Logf("\nChanging file xattr[%s]: %s\n", fileName, fqn)
	errstr = fs.SetXattr(fqn, cmn.XattrXXHashVal, []byte("01234abcde"))
	if errstr != "" {
		t.Error(errstr)
	}
	_, err = api.GetObject(tutils.DefaultBaseAPIParams(t), clibucket, path.Join(ChecksumWarmValidateStr, fileName))
	if err != nil {
		t.Errorf("A GET on an object when checksum algo is none should pass. Error: %v", err)
	}

cleanup:
	// Restore old config
	setClusterConfig(t, proxyURL, "checksum", oldChecksum)
	setClusterConfig(t, proxyURL, "validate_checksum_warm_get", oldWarmGet)
	wg := &sync.WaitGroup{}
	for _, fn := range filesList {
		if usingFile {
			_ = os.Remove(filepath.Join(LocalSrcDir, fn))
		}

		wg.Add(1)
		go tutils.Del(proxyURL, clibucket, fn, wg, errCh, !testing.Verbose())
	}
	wg.Wait()
	selectErr(errCh, "delete", t, false)
	close(errCh)
	close(fileNameCh)
}

func validateGETUponFileChangeForChecksumValidation(t *testing.T, proxyURL, fileName string, fqn string, oldFileInfo os.FileInfo) {
	// Do a GET to see to check if a cold get was executed by comparing old and new size
	baseParams := tutils.BaseAPIParams(proxyURL)
	_, err := api.GetObjectWithValidation(baseParams, clibucket, path.Join(ChecksumWarmValidateStr, fileName))
	if err != nil {
		t.Errorf("Unable to GET file. Error: %v", err)
	}
	newFileInfo, err := os.Stat(fqn)
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
	const fileSize = 1024
	var (
		numFiles   = 3
		fileNameCh = make(chan string, numFiles)
		errCh      = make(chan error, 100)
		sgl        *memsys.SGL
		bucketName = TestLocalBucketName
		proxyURL   = getPrimaryURL(t, proxyURLRO)
		fqn        string
		errstr     string
		err        error
	)

	if tutils.DockerRunning() {
		t.Skip(fmt.Sprintf("test %q requires Xattributes to be set, doesn't work with docker", t.Name()))
	}

	createFreshLocalBucket(t, proxyURL, bucketName)

	if usingSG {
		sgl = tutils.Mem2.NewSGL(fileSize)
		defer sgl.Free()
	}

	tutils.PutRandObjs(proxyURL, bucketName, ChecksumWarmValidateDir, readerType, ChecksumWarmValidateStr, fileSize, numFiles, errCh, fileNameCh, sgl)
	selectErr(errCh, "put", t, false)

	// Get Current Config
	config := getDaemonConfig(t, proxyURL)
	oldWarmGet := config.Cksum.ValidateWarmGet
	oldChecksum := config.Cksum.Checksum

	var fileName string
	fsWalkFunc := func(path string, info os.FileInfo, err error) error {
		if info.IsDir() && info.Name() == "cloud" {
			return filepath.SkipDir
		}
		if filepath.Base(path) == fileName && strings.Contains(path, filepath.Join(bucketName, ChecksumWarmValidateStr)) {
			fqn = path
		}
		return nil
	}

	if !oldWarmGet {
		setClusterConfig(t, proxyURL, "validate_checksum_warm_get", true)
		if t.Failed() {
			goto cleanup
		}
	}

	// Test changing the file content
	fileName = <-fileNameCh
	filepath.Walk(rootDir, fsWalkFunc)
	tutils.Logf("Changing contents of the file [%s]: %s\n", fileName, fqn)
	err = ioutil.WriteFile(fqn, []byte("Contents of this file have been changed."), 0644)
	tutils.CheckFatal(err, t)
	executeTwoGETsForChecksumValidation(proxyURL, bucketName, fileName, t)

	// Test changing the file xattr
	fileName = <-fileNameCh
	filepath.Walk(rootDir, fsWalkFunc)
	tutils.Logf("Changing file xattr[%s]: %s\n", fileName, fqn)
	errstr = fs.SetXattr(fqn, cmn.XattrXXHashVal, []byte("01234abcde"))
	if errstr != "" {
		t.Error(errstr)
	}
	executeTwoGETsForChecksumValidation(proxyURL, bucketName, fileName, t)

	// Test for none checksum algo
	fileName = <-fileNameCh
	filepath.Walk(rootDir, fsWalkFunc)
	setClusterConfig(t, proxyURL, "checksum", cmn.ChecksumNone)
	if t.Failed() {
		goto cleanup
	}
	tutils.Logf("Changing file xattr[%s]: %s\n", fileName, fqn)
	errstr = fs.SetXattr(fqn, cmn.XattrXXHashVal, []byte("01234abcde"))
	if errstr != "" {
		t.Error(errstr)
	}
	_, err = api.GetObject(tutils.DefaultBaseAPIParams(t), bucketName, path.Join(ChecksumWarmValidateStr, fileName))
	if err != nil {
		t.Error("A GET on an object when checksum algo is none should pass")
	}

cleanup:
	// Restore old config
	destroyLocalBucket(t, proxyURL, bucketName)
	setClusterConfig(t, proxyURL, "checksum", oldChecksum)
	setClusterConfig(t, proxyURL, "validate_checksum_warm_get", oldWarmGet)
	close(errCh)
	close(fileNameCh)
}

func executeTwoGETsForChecksumValidation(proxyURL, bucket string, fName string, t *testing.T) {
	baseParams := tutils.BaseAPIParams(proxyURL)
	_, err := api.GetObjectWithValidation(baseParams, bucket, path.Join(ChecksumWarmValidateStr, fName))
	if err == nil {
		t.Error("Error is nil, expected internal server error on a GET for an object")
	} else if !strings.Contains(err.Error(), "500") {
		t.Errorf("Expected internal server error on a GET for a corrupted object, got [%s]", err.Error())
	}
	// Execute another GET to make sure that the object is deleted
	_, err = api.GetObjectWithValidation(baseParams, bucket, path.Join(ChecksumWarmValidateStr, fName))
	if err == nil {
		t.Error("Error is nil, expected not found on a second GET for a corrupted object")
	} else if !strings.Contains(err.Error(), "404") {
		t.Errorf("Expected Not Found on a second GET for a corrupted object, got [%s]", err.Error())
	}
}

func TestRangeRead(t *testing.T) {
	const fileSize = 1024
	var (
		numFiles   = 1
		fileNameCh = make(chan string, numFiles)
		errCh      = make(chan error, numFiles)
		sgl        *memsys.SGL
		proxyURL   = getPrimaryURL(t, proxyURLRO)
		bucketName = clibucket
		fileName   string
	)

	if usingSG {
		sgl = tutils.Mem2.NewSGL(fileSize)
		defer sgl.Free()
	}

	created := createLocalBucketIfNotExists(t, proxyURL, clibucket)
	tutils.PutRandObjs(proxyURL, bucketName, RangeGetDir, readerType, RangeGetStr, fileSize, numFiles, errCh, fileNameCh, sgl)
	selectErr(errCh, "put", t, false)

	// Get Current Config
	config := getDaemonConfig(t, proxyURL)
	oldEnableReadRangeChecksum := config.Cksum.EnableReadRangeChecksum

	fileName = <-fileNameCh
	tutils.Logln("Testing valid cases.")
	// Validate entire object checksum is being returned
	if oldEnableReadRangeChecksum {
		setClusterConfig(t, proxyURL, "enable_read_range_checksum", false)
		if t.Failed() {
			goto cleanup
		}
	}
	testValidCases(fileSize, t, proxyURL, bucketName, fileName, true, RangeGetStr)

	// Validate only range checksum is being returned
	if !oldEnableReadRangeChecksum {
		setClusterConfig(t, proxyURL, "enable_read_range_checksum", true)
		if t.Failed() {
			goto cleanup
		}
	}
	testValidCases(fileSize, t, proxyURL, bucketName, fileName, false, RangeGetStr)

	tutils.Logln("Testing invalid cases.")
	verifyInvalidParams(t, proxyURL, bucketName, fileName, "", "1")
	verifyInvalidParams(t, proxyURL, bucketName, fileName, "1", "")
	verifyInvalidParams(t, proxyURL, bucketName, fileName, "-1", "-1")
	verifyInvalidParams(t, proxyURL, bucketName, fileName, "1", "-1")
	verifyInvalidParams(t, proxyURL, bucketName, fileName, "-1", "1")
	verifyInvalidParams(t, proxyURL, bucketName, fileName, "1", "0")
cleanup:
	tutils.Logln("Cleaning up...")
	wg := &sync.WaitGroup{}

	if usingFile {
		_ = os.Remove(filepath.Join(LocalSrcDir, fileName))
	}

	wg.Add(1)
	go tutils.Del(proxyURL, clibucket, filepath.Join(RangeGetStr, fileName), wg, errCh, !testing.Verbose())
	wg.Wait()
	selectErr(errCh, "delete", t, false)
	setClusterConfig(t, proxyURL, "enable_read_range_checksum", oldEnableReadRangeChecksum)
	close(errCh)
	close(fileNameCh)

	if created {
		destroyLocalBucket(t, proxyURL, clibucket)
	}
}

func testValidCases(fileSize uint64, t *testing.T, proxyURL, bucketName string, fileName string, checkEntireObjCkSum bool, checkDir string) {
	// Read the entire file range by range
	// Read in ranges of 500 to test covered, partially covered and completely
	// uncovered ranges
	byteRange := int64(500)
	iterations := int64(fileSize) / byteRange
	for i := int64(0); i < iterations; i += byteRange {
		verifyValidRanges(t, proxyURL, bucketName, fileName, int64(i), byteRange, byteRange, checkEntireObjCkSum, checkDir)
	}
	verifyValidRanges(t, proxyURL, bucketName, fileName, byteRange*iterations, byteRange, int64(fileSize)%byteRange, checkEntireObjCkSum, checkDir)
	verifyValidRanges(t, proxyURL, bucketName, fileName, int64(fileSize)+100, byteRange, 0, checkEntireObjCkSum, checkDir)
}

func verifyValidRanges(t *testing.T, proxyURL, bucketName string, fileName string,
	offset int64, length int64, expectedLength int64, checkEntireObjCksum bool,
	checkDir string) {
	var fqn string
	fsWalkFunc := func(path string, info os.FileInfo, err error) error {
		if filepath.Base(path) == fileName && strings.Contains(path, filepath.Join(bucketName, checkDir)) {
			fqn = path
		}
		return nil
	}
	filepath.Walk(rootDir, fsWalkFunc)

	q := url.Values{}
	q.Add(cmn.URLParamOffset, strconv.FormatInt(offset, 10))
	q.Add(cmn.URLParamLength, strconv.FormatInt(length, 10))
	var b bytes.Buffer
	w := bufio.NewWriter(&b)
	baseParams := tutils.BaseAPIParams(proxyURL)
	options := api.GetObjectInput{Writer: w, Query: q}
	_, err := api.GetObjectWithValidation(baseParams, bucketName, path.Join(RangeGetStr, fileName), options)
	if err != nil {
		if !checkEntireObjCksum {
			t.Errorf("Failed to get object %s/%s! Error: %v", bucketName, fileName, err)
		} else {
			if ckErr, ok := err.(cmn.InvalidCksumError); ok {
				file, err := os.Open(fqn)
				if err != nil {
					t.Fatalf("Unable to open file: %s. Error:  %v", fqn, err)
				}
				defer file.Close()
				hash, errstr := cmn.ComputeXXHash(file, nil)
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

func verifyInvalidParams(t *testing.T, proxyURL, bucketName string, fileName string, offset string, length string) {
	q := url.Values{}
	q.Add(cmn.URLParamOffset, offset)
	q.Add(cmn.URLParamLength, length)
	baseParams := tutils.BaseAPIParams(proxyURL)
	options := api.GetObjectInput{Query: q}
	_, err := api.GetObjectWithValidation(baseParams, bucketName, path.Join(RangeGetStr, fileName), options)
	if err == nil {
		t.Errorf("Must fail for invalid offset %s and length %s combination.", offset, length)
	}
}

func Test_checksum(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	const filesize = largefilesize * 1024 * 1024
	var (
		numPuts     = 5
		filesPutCh  = make(chan string, numPuts)
		fileslist   = make([]string, 0, numPuts)
		errCh       = make(chan error, numPuts*2)
		bucket      = clibucket
		start, curr time.Time
		duration    time.Duration
		sgl         *memsys.SGL
		totalio     = numPuts * largefilesize
		proxyURL    = getPrimaryURL(t, proxyURLRO)
	)

	created := createLocalBucketIfNotExists(t, proxyURL, bucket)
	ldir := filepath.Join(LocalSrcDir, ChksumValidStr)
	if err := cmn.CreateDir(ldir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", ldir, err)
	}

	// Get Current Config
	config := getDaemonConfig(t, proxyURL)
	ocoldget := config.Cksum.ValidateColdGet
	ochksum := config.Cksum.Checksum

	if usingSG {
		sgl = tutils.Mem2.NewSGL(filesize)
		defer sgl.Free()
	}
	tutils.PutRandObjs(proxyURL, bucket, ldir, readerType, ChksumValidStr, filesize, int(numPuts), errCh, filesPutCh, sgl)
	selectErr(errCh, "put", t, false)
	close(filesPutCh) // to exit for-range
	for fname := range filesPutCh {
		if fname != "" {
			fileslist = append(fileslist, filepath.Join(ChksumValidStr, fname))
		}
	}
	// Delete it from cache.
	evictobjects(t, proxyURL, fileslist)
	// Disable checkum
	if ochksum != cmn.ChecksumNone {
		setClusterConfig(t, proxyURL, "checksum", cmn.ChecksumNone)
	}
	if t.Failed() {
		goto cleanup
	}
	// Disable Cold Get Validation
	if ocoldget {
		setClusterConfig(t, proxyURL, "validate_checksum_cold_get", false)
	}
	if t.Failed() {
		goto cleanup
	}
	start = time.Now()
	getfromfilelist(t, proxyURL, bucket, errCh, fileslist, false)
	curr = time.Now()
	duration = curr.Sub(start)
	if t.Failed() {
		goto cleanup
	}
	tutils.Logf("GET %d MB without any checksum validation: %v\n", totalio, duration)
	selectErr(errCh, "get", t, false)
	evictobjects(t, proxyURL, fileslist)
	switch clichecksum {
	case "all":
		setClusterConfig(t, proxyURL, "checksum", cmn.ChecksumXXHash)
		setClusterConfig(t, proxyURL, "validate_checksum_cold_get", true)
		if t.Failed() {
			goto cleanup
		}
	case cmn.ChecksumXXHash:
		setClusterConfig(t, proxyURL, "checksum", cmn.ChecksumXXHash)
		if t.Failed() {
			goto cleanup
		}
	case ColdMD5str:
		setClusterConfig(t, proxyURL, "validate_checksum_cold_get", true)
		if t.Failed() {
			goto cleanup
		}
	case cmn.ChecksumNone:
		// do nothing
		tutils.Logf("Checksum validation has been disabled \n")
		goto cleanup
	default:
		tutils.Logf("Checksum is either not set or invalid\n")
		goto cleanup
	}
	start = time.Now()
	getfromfilelist(t, proxyURL, bucket, errCh, fileslist, true)
	curr = time.Now()
	duration = curr.Sub(start)
	tutils.Logf("GET %d MB and validate checksum (%s): %v\n", totalio, clichecksum, duration)
	selectErr(errCh, "get", t, false)
cleanup:
	deletefromfilelist(proxyURL, bucket, errCh, fileslist)
	selectErr(errCh, "delete", t, false)
	close(errCh)
	// restore old config
	setClusterConfig(t, proxyURL, "checksum", ochksum)
	setClusterConfig(t, proxyURL, "validate_checksum_cold_get", ocoldget)

	if created {
		destroyLocalBucket(t, proxyURL, bucket)
	}
}

// deletefromfilelist requires that errCh be twice the size of len(fileslist) as each
// file can produce upwards of two errors.
func deletefromfilelist(proxyURL, bucket string, errCh chan error, fileslist []string) {
	wg := &sync.WaitGroup{}
	// Delete local file and objects from bucket
	for _, fn := range fileslist {
		if usingFile {
			err := os.Remove(filepath.Join(LocalSrcDir, fn))
			if err != nil {
				errCh <- err
			}
		}

		wg.Add(1)
		go tutils.Del(proxyURL, bucket, fn, wg, errCh, true)
	}

	wg.Wait()
}

func getfromfilelist(t *testing.T, proxyURL, bucket string, errCh chan error, fileslist []string, validate bool) {
	getsGroup := &sync.WaitGroup{}
	baseParams := tutils.BaseAPIParams(proxyURL)
	for i := 0; i < len(fileslist); i++ {
		if fileslist[i] != "" {
			getsGroup.Add(1)
			go func(i int) {
				var err error
				if validate {
					_, err = api.GetObjectWithValidation(baseParams, bucket, fileslist[i])
				} else {
					_, err = api.GetObject(baseParams, bucket, fileslist[i])
				}
				if err != nil {
					errCh <- err
				}
				getsGroup.Done()
			}(i)
		}
	}
	getsGroup.Wait()
}

func evictobjects(t *testing.T, proxyURL string, fileslist []string) {
	err := tutils.EvictList(proxyURL, clibucket, fileslist, true, 0)
	if err != nil {
		t.Errorf("Evict bucket %s failed, err = %v", clibucket, err)
	}
}

func createLocalBucketIfNotExists(t *testing.T, proxyURL, bucket string) (created bool) {
	baseParams := tutils.BaseAPIParams(proxyURL)
	buckets, err := api.GetBucketNames(baseParams, false)
	if err != nil {
		t.Fatalf("Failed to read bucket list: %v", err)
	}

	if cmn.StringInSlice(bucket, buckets.Local) || cmn.StringInSlice(bucket, buckets.Cloud) {
		return false
	}

	err = api.CreateLocalBucket(baseParams, bucket)
	if err != nil {
		t.Fatalf("Failed to create local bucket %s: %v", bucket, err)
	}

	return true
}

func isCloudBucket(t *testing.T, proxyURL, bucket string) bool {
	baseParams := tutils.BaseAPIParams(proxyURL)
	buckets, err := api.GetBucketNames(baseParams, false)
	if err != nil {
		t.Fatalf("Failed to read bucket names: %v", err)
	}

	return cmn.StringInSlice(bucket, buckets.Cloud)
}

func getPrimaryURL(t *testing.T, proxyURL string) string {
	url, err := tutils.GetPrimaryProxy(proxyURL)
	if err != nil {
		t.Fatalf("Failed to get primary proxy URL: %v", err)
	}

	return url
}

func validateBucketProps(t *testing.T, expected, actual cmn.BucketProps) {
	if actual.CloudProvider != expected.CloudProvider {
		t.Errorf("Expected cloud provider: %s, received cloud provider: %s",
			expected.CloudProvider, actual.CloudProvider)
	}
	if actual.ReadPolicy != expected.ReadPolicy {
		t.Errorf("Expected read policy: %s, received read policy: %s",
			expected.ReadPolicy, actual.ReadPolicy)
	}
	if actual.WritePolicy != expected.WritePolicy {
		t.Errorf("Expected write policy: %s, received write policy: %s",
			expected.WritePolicy, actual.WritePolicy)
	}
	if actual.NextTierURL != expected.NextTierURL {
		t.Errorf("Expected next tier URL: %s, received next tier URL: %s",
			expected.NextTierURL, actual.NextTierURL)
	}
	if actual.Checksum != expected.Checksum {
		t.Errorf("Expected checksum type: %s, received checksum type: %s",
			expected.Checksum, actual.Checksum)
	}
	if actual.ValidateColdGet != expected.ValidateColdGet {
		t.Errorf("Expected cold GET validation setting: %t, received: %t",
			expected.ValidateColdGet, actual.ValidateColdGet)
	}
	if actual.ValidateWarmGet != expected.ValidateWarmGet {
		t.Errorf("Expected warm GET validation setting: %t, received: %t",
			expected.ValidateWarmGet, actual.ValidateWarmGet)
	}
	if actual.EnableReadRangeChecksum != expected.EnableReadRangeChecksum {
		t.Errorf("Expected byte range validation setting: %t, received: %t",
			expected.EnableReadRangeChecksum, actual.EnableReadRangeChecksum)
	}
	if actual.LowWM != expected.LowWM {
		t.Errorf("Expected LowWM setting: %d, received %d", expected.LowWM, actual.LowWM)
	}
	if actual.HighWM != expected.HighWM {
		t.Errorf("Expected HighWM setting: %d, received %d", expected.HighWM, actual.HighWM)
	}
	if actual.AtimeCacheMax != expected.AtimeCacheMax {
		t.Errorf("Expected AtimeCacheMax setting: %d, received %d",
			expected.AtimeCacheMax, actual.AtimeCacheMax)
	}
	if actual.DontEvictTimeStr != expected.DontEvictTimeStr {
		t.Errorf("Expected DontEvictTimeStr setting: %s, received %s",
			expected.DontEvictTimeStr, actual.DontEvictTimeStr)
	}
	if actual.CapacityUpdTimeStr != expected.CapacityUpdTimeStr {
		t.Errorf("Expected CapacityUpdTimeStr setting: %s, received %s",
			expected.CapacityUpdTimeStr, actual.CapacityUpdTimeStr)
	}
	if actual.LRUEnabled != expected.LRUEnabled {
		t.Errorf("Expected LRU enabled setting: %t, received: %t",
			expected.LRUEnabled, actual.LRUEnabled)
	}
}

func defaultBucketProps() cmn.BucketProps {
	return cmn.BucketProps{
		CloudProvider: cmn.ProviderDFC,
		NextTierURL:   "http://foo.com",
		ReadPolicy:    cmn.RWPolicyNextTier,
		WritePolicy:   cmn.RWPolicyNextTier,
		CksumConf: cmn.CksumConf{
			Checksum:                cmn.ChecksumXXHash,
			ValidateColdGet:         false,
			ValidateWarmGet:         false,
			EnableReadRangeChecksum: false,
		},
		LRUConf: cmn.LRUConf{
			LowWM:              int64(10),
			HighWM:             int64(50),
			AtimeCacheMax:      int64(9999),
			DontEvictTimeStr:   "1m",
			CapacityUpdTimeStr: "2m",
			LRUEnabled:         false,
		},
	}
}
