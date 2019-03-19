// Package ais_test contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais_test

// For how to run tests, see README

import (
	"bufio"
	"bytes"
	"crypto/md5"
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

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tutils"
	jsoniter "github.com/json-iterator/go"
)

// worker's result
type workres struct {
	totfiles int
	totbytes int64
}

func Test_download(t *testing.T) {
	proxyURL := getPrimaryURL(t, proxyURLReadOnly)

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
	proxyURL := getPrimaryURL(t, proxyURLReadOnly)
	if created := createLocalBucketIfNotExists(t, proxyURL, clibucket); created {
		defer tutils.DestroyLocalBucket(t, proxyURL, clibucket)
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
	proxyURL := getPrimaryURL(t, proxyURLReadOnly)

	if err := cmn.CreateDir(DeleteDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", DeleteDir, err)
	}
	if created := createLocalBucketIfNotExists(t, proxyURL, clibucket); created {
		defer tutils.DestroyLocalBucket(t, proxyURL, clibucket)
	}

	errCh := make(chan error, numfiles*5)
	objsPutCh := make(chan string, numfiles)

	sgl := tutils.Mem2.NewSGL(objSize)
	defer sgl.Free()
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

		err := api.DeleteRange(baseParams, clibucket, "", test.prefix, test.regexStr, test.rangeStr, true, 0)
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

	if err := cmn.CreateDir(DeleteDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", DeleteDir, err)
	}

	errCh := make(chan error, numfiles)
	filesPutCh := make(chan string, numfiles)
	const filesize = 512 * 1024
	proxyURL := getPrimaryURL(t, proxyURLReadOnly)
	if created := createLocalBucketIfNotExists(t, proxyURL, clibucket); created {
		defer tutils.DestroyLocalBucket(t, proxyURL, clibucket)
	}

	sgl := tutils.Mem2.NewSGL(filesize)
	defer sgl.Free()
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
			return nil, fmt.Errorf("failed to list bucket %s", bucket)
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
	buckets, err := api.GetBucketNames(tutils.DefaultBaseAPIParams(t), "")
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
func Test_SameLocalAndCloudBckNameValidate(t *testing.T) {
	var (
		bucketName = clibucket
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		baseParams = tutils.BaseAPIParams(proxyURL)
		fileName1  = "mytestobj1.txt"
		fileName2  = "mytestobj2.txt"
		queryLocal = url.Values{}
		queryCloud = url.Values{}
		dataLocal  = []byte("im local")
		dataCloud  = []byte("I'm from the cloud!")
		files      = []string{fileName1, fileName2}
	)

	if !isCloudBucket(t, proxyURL, bucketName) {
		t.Skipf("%s requires a cloud bucket", t.Name())
	}
	queryLocal.Add(cmn.URLParamBckProvider, cmn.LocalBs)
	queryCloud.Add(cmn.URLParamBckProvider, cmn.CloudBs)

	putArgsLocal := api.PutObjectArgs{
		BaseParams:     baseParams,
		Bucket:         bucketName,
		BucketProvider: cmn.LocalBs,
		Object:         fileName1,
		Reader:         tutils.NewBytesReader(dataLocal),
	}

	putArgsCloud := api.PutObjectArgs{
		BaseParams:     baseParams,
		Bucket:         bucketName,
		BucketProvider: cmn.CloudBs,
		Object:         fileName1,
		Reader:         tutils.NewBytesReader(dataCloud),
	}

	//PUT/GET/DEL Without local bucket
	tutils.Logf("Validating responses for non-existent local bucket...\n")
	err := api.PutObject(putArgsLocal)
	if err == nil {
		t.Fatalf("Local bucket %s does not exist: Expected an error.", bucketName)
	}

	_, err = api.GetObject(baseParams, bucketName, fileName1, api.GetObjectInput{Query: queryLocal})
	if err == nil {
		t.Fatalf("Local bucket %s does not exist: Expected an error.", bucketName)
	}

	err = api.DeleteObject(baseParams, bucketName, fileName1, cmn.LocalBs)
	if err == nil {
		t.Fatalf("Local bucket %s does not exist: Expected an error.", bucketName)
	}

	// Prefetch, evict without bprovider=cloud
	tutils.Logf("Validating responses for empty bprovider for cloud bucket operations ...\n")
	err = api.PrefetchList(baseParams, clibucket, "", files, true, 0)
	if err == nil {
		t.Fatalf("PrefetchList without %s=%s. Expected an error.", cmn.URLParamBckProvider, cmn.CloudBs)
	}

	err = api.PrefetchRange(baseParams, clibucket, "", "", "", prefetchRange, true, 0)
	if err == nil {
		t.Fatalf("PrefetchRange without %s=%s. Expected an error.", cmn.URLParamBckProvider, cmn.CloudBs)
	}

	err = api.EvictList(baseParams, bucketName, "", files, true, 0)
	if err == nil {
		t.Fatalf("EvictList without %s=%s. Expected an error.", cmn.URLParamBckProvider, cmn.CloudBs)
	}

	err = api.EvictRange(baseParams, clibucket, "", "", "", prefetchRange, true, 0)
	if err == nil {
		t.Fatalf("EvictRange without %s=%s. Expected an error.", cmn.URLParamBckProvider, cmn.CloudBs)
	}

	tutils.CreateFreshLocalBucket(t, proxyURL, bucketName)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucketName)

	// PUT
	tutils.Logf("Putting %s and %s into buckets...\n", fileName1, fileName2)
	err = api.PutObject(putArgsLocal)
	tutils.CheckFatal(err, t)
	putArgsLocal.Object = fileName2
	err = api.PutObject(putArgsLocal)
	tutils.CheckFatal(err, t)

	err = api.PutObject(putArgsCloud)
	tutils.CheckFatal(err, t)
	putArgsCloud.Object = fileName2
	err = api.PutObject(putArgsCloud)
	tutils.CheckFatal(err, t)

	// Check Local bucket has 2 files
	tutils.Logf("Validating local bucket have %s and %s ...\n", fileName1, fileName2)
	_, err = api.HeadObject(baseParams, bucketName, cmn.LocalBs, fileName1)
	tutils.CheckFatal(err, t)
	_, err = api.HeadObject(baseParams, bucketName, cmn.LocalBs, fileName2)
	tutils.CheckFatal(err, t)

	// Prefetch/Evict should work
	err = api.PrefetchList(baseParams, clibucket, cmn.CloudBs, files, true, 0)
	tutils.CheckFatal(err, t)
	err = api.EvictList(baseParams, bucketName, cmn.CloudBs, files, true, 0)
	tutils.CheckFatal(err, t)

	// Deleting from cloud bucket
	tutils.Logf("Deleting %s and %s from cloud bucket ...\n", fileName1, fileName2)
	api.DeleteList(baseParams, bucketName, cmn.CloudBs, files, true, 0)

	// Deleting from local bucket
	tutils.Logf("Deleting %s and %s from local bucket ...\n", fileName1, fileName2)
	api.DeleteList(baseParams, bucketName, cmn.LocalBs, files, true, 0)

	_, err = api.HeadObject(baseParams, bucketName, cmn.LocalBs, fileName1)
	if !strings.Contains(err.Error(), strconv.Itoa(http.StatusNotFound)) {
		t.Errorf("Local file %s not deleted", fileName1)
	}
	_, err = api.HeadObject(baseParams, bucketName, cmn.LocalBs, fileName2)
	if !strings.Contains(err.Error(), strconv.Itoa(http.StatusNotFound)) {
		t.Errorf("Local file %s not deleted", fileName2)
	}

	_, err = api.HeadObject(baseParams, bucketName, cmn.CloudBs, fileName1)
	if !strings.Contains(err.Error(), strconv.Itoa(http.StatusNotFound)) {
		t.Errorf("Cloud file %s not deleted", fileName1)
	}
	_, err = api.HeadObject(baseParams, bucketName, cmn.CloudBs, fileName2)
	if !strings.Contains(err.Error(), strconv.Itoa(http.StatusNotFound)) {
		t.Errorf("Cloud file %s not deleted", fileName2)
	}

}

func Test_SameLocalAndCloudBucketName(t *testing.T) {
	var (
		bucketName   = clibucket
		proxyURL     = getPrimaryURL(t, proxyURLReadOnly)
		fileName     = "mytestobj1.txt"
		baseParams   = tutils.DefaultBaseAPIParams(t)
		queryLocal   = url.Values{}
		queryCloud   = url.Values{}
		dataLocal    = []byte("im local")
		dataCloud    = []byte("I'm from the cloud!")
		globalProps  cmn.BucketProps
		globalConfig = getDaemonConfig(t, proxyURL)
		msg          = &cmn.GetMsg{GetPageSize: int(pagesize), GetProps: "size,status"}
		found        = false
	)

	if !isCloudBucket(t, proxyURL, bucketName) {
		t.Skipf("%s requires a cloud bucket", t.Name())
	}

	queryLocal.Add(cmn.URLParamBckProvider, cmn.LocalBs)
	queryCloud.Add(cmn.URLParamBckProvider, cmn.CloudBs)

	tutils.CreateFreshLocalBucket(t, proxyURL, bucketName)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucketName)

	//Common
	globalProps.Cksum = globalConfig.Cksum
	globalProps.LRU = testBucketProps(t).LRU

	// Local bucket props
	bucketPropsLocal := defaultBucketProps()
	bucketPropsLocal.Cksum.Type = cmn.ChecksumNone

	// Cloud bucket props
	bucketPropsCloud := defaultBucketProps()
	bucketPropsCloud.CloudProvider = cmn.ProviderAmazon

	// Put
	tutils.Logf("Putting object (%s) into local bucket %s...\n", fileName, bucketName)
	putArgs := api.PutObjectArgs{
		BaseParams:     baseParams,
		Bucket:         bucketName,
		BucketProvider: cmn.LocalBs,
		Object:         fileName,
		Reader:         tutils.NewBytesReader(dataLocal),
	}
	err := api.PutObject(putArgs)
	tutils.CheckFatal(err, t)

	resLocal, err := api.ListBucket(baseParams, bucketName, msg, 0, queryLocal)
	tutils.CheckFatal(err, t)

	tutils.Logf("Putting object (%s) into cloud bucket %s...\n", fileName, bucketName)
	putArgs = api.PutObjectArgs{
		BaseParams:     baseParams,
		Bucket:         bucketName,
		BucketProvider: cmn.CloudBs,
		Object:         fileName,
		Reader:         tutils.NewBytesReader(dataCloud),
	}
	err = api.PutObject(putArgs)
	tutils.CheckFatal(err, t)

	resCloud, err := api.ListBucket(baseParams, bucketName, msg, 0, queryCloud)
	tutils.CheckFatal(err, t)

	if len(resLocal.Entries) != 1 {
		t.Fatalf("Expected number of files in local bucket (%s) does not match: expected %v, got %v", bucketName, 1, len(resLocal.Entries))
	}

	for _, entry := range resCloud.Entries {
		if entry.Name == fileName {
			found = true
			break
		}
	}

	if !found {
		t.Fatalf("File (%s) not found in cloud bucket (%s)", fileName, bucketName)
	}

	// Get
	lenLocal, err := api.GetObject(baseParams, bucketName, fileName, api.GetObjectInput{Query: queryLocal})
	tutils.CheckFatal(err, t)
	lenCloud, err := api.GetObject(baseParams, bucketName, fileName, api.GetObjectInput{Query: queryCloud})
	tutils.CheckFatal(err, t)

	if lenLocal == lenCloud {
		t.Errorf("Local file and cloud file have same size, expected: local (%v) cloud (%v) got: local (%v) cloud (%v)",
			len(dataLocal), len(dataCloud), lenLocal, lenCloud)
	}

	// Delete
	err = api.DeleteObject(baseParams, bucketName, fileName, cmn.CloudBs)
	tutils.CheckFatal(err, t)

	lenLocal, err = api.GetObject(baseParams, bucketName, fileName, api.GetObjectInput{Query: queryLocal})
	tutils.CheckFatal(err, t)

	// Check that local object still exists
	if lenLocal != int64(len(dataLocal)) {
		t.Errorf("Local file %s deleted", fileName)
	}

	// Check that cloud object is deleted using HeadObject
	_, err = api.HeadObject(baseParams, bucketName, cmn.CloudBs, fileName)
	if !strings.Contains(err.Error(), strconv.Itoa(http.StatusNotFound)) {
		t.Errorf("Cloud file %s not deleted", fileName)
	}

	// Set Props Object
	err = api.SetBucketProps(baseParams, bucketName, bucketPropsLocal, queryLocal)
	tutils.CheckFatal(err, t)

	err = api.SetBucketProps(baseParams, bucketName, bucketPropsCloud, queryCloud)
	tutils.CheckFatal(err, t)

	// Validate local bucket props are set
	localProps, err := api.HeadBucket(baseParams, bucketName, queryLocal)
	tutils.CheckFatal(err, t)
	validateBucketProps(t, bucketPropsLocal, *localProps)

	// Validate cloud bucket props are set
	cloudProps, err := api.HeadBucket(baseParams, bucketName, queryCloud)
	tutils.CheckFatal(err, t)
	validateBucketProps(t, bucketPropsCloud, *cloudProps)

	// Reset local bucket props and validate they are reset
	globalProps.CloudProvider = cmn.ProviderAIS
	err = api.ResetBucketProps(baseParams, bucketName, queryLocal)
	tutils.CheckFatal(err, t)
	localProps, err = api.HeadBucket(baseParams, bucketName, queryLocal)
	tutils.CheckFatal(err, t)
	validateBucketProps(t, globalProps, *localProps)

	// Check if cloud bucket props remain the same
	cloudProps, err = api.HeadBucket(baseParams, bucketName, queryCloud)
	tutils.CheckFatal(err, t)
	validateBucketProps(t, bucketPropsCloud, *cloudProps)

	// Reset cloud bucket props
	globalProps.CloudProvider = cmn.ProviderAmazon
	err = api.ResetBucketProps(baseParams, bucketName, queryCloud)
	tutils.CheckFatal(err, t)
	cloudProps, err = api.HeadBucket(baseParams, bucketName, queryCloud)
	tutils.CheckFatal(err, t)
	validateBucketProps(t, globalProps, *cloudProps)

	// Check if local bucket props remain the same
	globalProps.CloudProvider = cmn.ProviderAIS
	localProps, err = api.HeadBucket(baseParams, bucketName, queryLocal)
	tutils.CheckFatal(err, t)
	validateBucketProps(t, globalProps, *localProps)

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
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
	)

	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skipf("%s requires a cloud bucket", t.Name())
	}

	ldir := filepath.Join(LocalSrcDir, ColdValidStr)
	if err := cmn.CreateDir(ldir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", ldir, err)
	}

	config := getDaemonConfig(t, proxyURL)
	bcoldget := config.Cksum.ValidateColdGet

	sgl := tutils.Mem2.NewSGL(filesize)
	defer sgl.Free()
	tutils.PutRandObjs(proxyURL, bucket, ldir, readerType, ColdValidStr, filesize, numPuts, errCh, filesPutCh, sgl)
	selectErr(errCh, "put", t, false)
	close(filesPutCh) // to exit for-range
	for fname := range filesPutCh {
		fileslist = append(fileslist, filepath.Join(ColdValidStr, fname))
	}
	evictobjects(t, proxyURL, cmn.CloudBs, fileslist)
	// Disable Cold Get Validation
	if bcoldget {
		setClusterConfig(t, proxyURL, "cksum.validate_cold_get", false)
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
	evictobjects(t, proxyURL, cmn.CloudBs, fileslist)
	// Enable Cold Get Validation
	setClusterConfig(t, proxyURL, "cksum.validate_cold_get", true)
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
	setClusterConfig(t, proxyURL, "cksum.validate_cold_get", bcoldget)
	for _, fn := range fileslist {
		wg.Add(1)
		go tutils.Del(proxyURL, bucket, fn, "", wg, errCh, !testing.Verbose())
	}
	wg.Wait()
	selectErr(errCh, "delete", t, false)
	close(errCh)
}

func TestHeadLocalBucket(t *testing.T) {
	var (
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	tutils.CreateFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer tutils.DestroyLocalBucket(t, proxyURL, TestLocalBucketName)

	bucketProps := defaultBucketProps()
	bucketProps.Cksum.ValidateWarmGet = true
	bucketProps.LRU.Enabled = true

	err := api.SetBucketProps(baseParams, TestLocalBucketName, bucketProps)
	tutils.CheckFatal(err, t)

	p, err := api.HeadBucket(baseParams, TestLocalBucketName)
	tutils.CheckFatal(err, t)

	validateBucketProps(t, bucketProps, *p)
}

func TestHeadCloudBucket(t *testing.T) {
	var (
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skip(fmt.Sprintf("%s requires a cloud bucket", t.Name()))
	}
	bucketProps := defaultBucketProps()
	bucketProps.CloudProvider = cmn.ProviderAmazon
	bucketProps.Cksum.ValidateWarmGet = true
	bucketProps.Cksum.ValidateColdGet = true
	bucketProps.LRU.Enabled = true

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
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		objName  = "headobject_test_obj"
		objSize  = 1024
	)
	tutils.CreateFreshLocalBucket(t, proxyURL, TestLocalBucketName)
	defer tutils.DestroyLocalBucket(t, proxyURL, TestLocalBucketName)

	r, rrErr := tutils.NewRandReader(int64(objSize), false)
	if rrErr != nil {
		t.Fatalf("tutils.NewRandReader failed, err = %v", rrErr)
	}
	defer r.Close()
	putArgs := api.PutObjectArgs{
		BaseParams: tutils.DefaultBaseAPIParams(t),
		Bucket:     TestLocalBucketName,
		Object:     objName,
		Hash:       r.XXHash(),
		Reader:     r,
	}
	if err := api.PutObject(putArgs); err != nil {
		t.Fatalf("api.PutObject failed, err = %v", err)
	}

	propsExp := &cmn.ObjectProps{Size: objSize, Version: "1"}
	props, err := api.HeadObject(tutils.DefaultBaseAPIParams(t), TestLocalBucketName, "", objName)
	if err != nil {
		t.Errorf("api.HeadObject failed, err = %v", err)
	}

	if !reflect.DeepEqual(props, propsExp) {
		t.Errorf("Returned object props not correct. Expected: %v, actual: %v", propsExp, props)
	}

	_, err = api.HeadObject(tutils.DefaultBaseAPIParams(t), TestLocalBucketName, "", "this_object_should_not_exist")
	if err == nil {
		t.Errorf("Expected non-nil error (404) from api.HeadObject, received nil error")
	}
}

func TestHeadObjectCheckCached(t *testing.T) {
	var (
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		fileName = "headobject_check_cached_test_file"
		fileSize = 1024
	)

	if created := createLocalBucketIfNotExists(t, proxyURL, clibucket); created {
		defer tutils.DestroyLocalBucket(t, proxyURL, clibucket)
	}
	r, err := tutils.NewRandReader(int64(fileSize), false)
	if err != nil {
		t.Fatalf("tutils.NewRandReader failed, err = %v", err)
	}
	defer r.Close()
	putArgs := api.PutObjectArgs{
		BaseParams: tutils.DefaultBaseAPIParams(t),
		Bucket:     clibucket,
		Object:     fileName,
		Hash:       r.XXHash(),
		Reader:     r,
	}
	err = api.PutObject(putArgs)
	tutils.CheckFatal(err, t)

	b, err := tutils.IsCached(proxyURL, clibucket, fileName)
	tutils.CheckFatal(err, t)
	if !b {
		t.Error("Expected object to be cached, got false from tutils.IsCached")
	}

	err = tutils.Del(proxyURL, clibucket, fileName, "", nil, nil, true)
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
	var (
		errstr   string
		cksumVal string
	)

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
		err = fmt.Errorf("worker %2d: get key %s from bucket %s http error %d",
			id, keyname, bucket, resp.StatusCode)
		errCh <- err
		t.Error(err)
		failed = true
		return
	}

	hdhash := resp.Header.Get(cmn.HeaderObjCksumVal)
	hdhashtype := resp.Header.Get(cmn.HeaderObjCksumType)

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
		written, cksumVal, err = cmn.WriteWithHash(file, resp.Body, nil)
		if err != nil {
			t.Errorf("Worker %2d: failed to write file, err: %v", id, err)
			failed = true
			return
		}
		if hdhash != cksumVal {
			t.Errorf("Worker %2d: header's %s %s doesn't match the file's %s", id, cmn.ChecksumXXHash, hdhash, cksumVal)
			failed = true
			return
		}
		tutils.Logf("Worker %2d: header's %s checksum %s matches the file's %s\n", id, cmn.ChecksumXXHash, hdhash, cksumVal)
	} else if hdhashtype == cmn.ChecksumMD5 {
		md5 := md5.New()
		written, err = cmn.ReceiveAndChecksum(file, resp.Body, nil, md5)
		if err != nil {
			t.Errorf("Worker %2d: failed to write file, err: %v", id, err)
			return
		}
		md5hash := cmn.HashToStr(md5)[:16]
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
		go tutils.Del(proxyURL, bucket, keyname, "", dwg, errCh, true)
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

// Tests URL to quickly get all objects in a bucket
func TestQuickObjectList(t *testing.T) {
	const (
		bucket       = "quick-list-bucket"
		numObjs      = 1234 // greater than default PageSize=1000
		objSize      = 1024
		commonPrefix = "quick"
	)
	var (
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
	)

	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)
	sgl := tutils.Mem2.NewSGL(objSize)
	defer sgl.Free()

	tutils.Logf("Creating %d objects in %s bucket\n", numObjs, bucket)
	errCh := make(chan error, numObjs)
	objsPutCh := make(chan string, numObjs)
	objList := make([]string, 0, numObjs)
	for i := 0; i < numObjs; i++ {
		fname := fmt.Sprintf("q-%04d", i)
		objList = append(objList, fname)
	}
	tutils.PutObjsFromList(proxyURL, bucket, DeleteDir, readerType, commonPrefix, objSize, objList, errCh, objsPutCh, sgl)
	selectErr(errCh, "put", t, true /* fatal - if PUT does not work then it makes no sense to continue */)
	close(objsPutCh)

	tutils.Logln("Reading objects...")
	baseParams := tutils.BaseAPIParams(proxyURL)
	reslist, err := api.ListBucketFast(baseParams, bucket, nil)
	if err != nil {
		t.Fatalf("List bucket %s failed, err = %v", bucket, err)
	}
	tutils.Logf("Object count: %d\n", len(reslist.Entries))
	if len(reslist.Entries) != numObjs {
		t.Fatalf("Expected %d objects, received %d objects",
			numObjs, len(reslist.Entries))
	}

	// check that all props are zeros, except name and size
	var empty cmn.BucketEntry
	for _, e := range reslist.Entries {
		if e.Name == "" || e.Size != objSize {
			t.Errorf("Invalid size or name: %#v", *e)
		}
		if e.Ctime != empty.Ctime ||
			e.Checksum != empty.Checksum ||
			e.Type != empty.Type ||
			e.Bucket != empty.Bucket ||
			e.Atime != empty.Atime ||
			e.Version != empty.Version ||
			e.TargetURL != empty.TargetURL ||
			e.Status != empty.Status ||
			e.Copies != empty.Copies ||
			e.IsCached != empty.IsCached {
			t.Errorf("Some fields does have default values: %#v", *e)
		}
	}

	query := make(url.Values)
	prefix := commonPrefix + "/q-009"
	query.Set(cmn.URLParamPrefix, prefix)
	reslist, err = api.ListBucketFast(baseParams, bucket, nil, query)
	if err != nil {
		t.Fatalf("List bucket %s with prefix %s failed, err = %v",
			bucket, prefix, err)
	}
	tutils.Logf("Object count (with prefix %s): %d\n", prefix, len(reslist.Entries))
	// Get should return only objects from q-0090 through q-0099
	if len(reslist.Entries) != 10 {
		t.Fatalf("Expected %d objects with prefix %s, received %d objects",
			numObjs, prefix, len(reslist.Entries))
	}
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
		fqn         string
		fileName    string
		oldFileInfo os.FileInfo
		errstr      string
		filesList   = make([]string, 0, numFiles)
		proxyURL    = getPrimaryURL(t, proxyURLReadOnly)
	)

	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skip(fmt.Sprintf("test %q requires a cloud bucket", t.Name()))
	}

	if tutils.DockerRunning() {
		t.Skip(fmt.Sprintf("test %q requires Xattributes to be set, doesn't work with docker", t.Name()))
	}
	sgl := tutils.Mem2.NewSGL(fileSize)
	defer sgl.Free()

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
	oldChecksum := config.Cksum.Type
	if !oldWarmGet {
		setClusterConfig(t, proxyURL, "cksum.validate_warm_get", true)
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
	errstr = fs.SetXattr(fqn, cmn.XattrXXHash, []byte("01234abcde"))
	if errstr != "" {
		t.Error(errstr)
	}
	validateGETUponFileChangeForChecksumValidation(t, proxyURL, fileName, fqn, oldFileInfo)

	// Test for no checksum algo
	fileName = <-fileNameCh
	filesList = append(filesList, filepath.Join(ChecksumWarmValidateStr, fileName))
	filepath.Walk(rootDir, fsWalkFunc)
	setClusterConfig(t, proxyURL, "cksum.type", cmn.ChecksumNone)
	if t.Failed() {
		goto cleanup
	}
	tutils.Logf("\nChanging file xattr[%s]: %s\n", fileName, fqn)
	errstr = fs.SetXattr(fqn, cmn.XattrXXHash, []byte("01234abcde"))
	if errstr != "" {
		t.Error(errstr)
	}
	_, err = api.GetObject(tutils.DefaultBaseAPIParams(t), clibucket, path.Join(ChecksumWarmValidateStr, fileName))
	if err != nil {
		t.Errorf("A GET on an object when checksum algo is none should pass. Error: %v", err)
	}

cleanup:
	// Restore old config
	setClusterConfig(t, proxyURL, "cksum.type", oldChecksum)
	setClusterConfig(t, proxyURL, "cksum.validate_warm_get", oldWarmGet)
	wg := &sync.WaitGroup{}
	for _, fn := range filesList {
		wg.Add(1)
		go tutils.Del(proxyURL, clibucket, fn, "", wg, errCh, !testing.Verbose())
	}
	wg.Wait()
	selectErr(errCh, "delete", t, false)
	close(errCh)
	close(fileNameCh)
}

func Test_evictCloudBucket(t *testing.T) {
	const filesize = largefilesize * 1024 * 1024
	var (
		numPuts    = 5
		filesPutCh = make(chan string, numPuts)
		fileslist  = make([]string, 0, 100)
		errCh      = make(chan error, 100)
		err        error
		wg         = &sync.WaitGroup{}
		bucket     = clibucket
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		bProps     *cmn.BucketProps
		query      = url.Values{}
	)

	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skipf("%s requires a cloud bucket", t.Name())
	}

	ldir := filepath.Join(LocalSrcDir, EvictCBStr)
	if err = cmn.CreateDir(ldir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", ldir, err)
	}

	defer func() {
		os.RemoveAll(LocalSrcDir)
		//cleanup
		for _, fn := range fileslist {
			wg.Add(1)
			go tutils.Del(proxyURL, bucket, fn, "", wg, errCh, !testing.Verbose())
		}
		wg.Wait()
		selectErr(errCh, "delete", t, false)
		close(errCh)

		resetBucketProps(proxyURL, clibucket, t)
	}()

	sgl := tutils.Mem2.NewSGL(filesize)
	defer sgl.Free()
	tutils.PutRandObjs(proxyURL, bucket, ldir, readerType, EvictCBStr, filesize, numPuts, errCh, filesPutCh, sgl)
	selectErr(errCh, "put", t, false)
	close(filesPutCh) // to exit for-range
	for fname := range filesPutCh {
		fileslist = append(fileslist, filepath.Join(EvictCBStr, fname))
	}
	getfromfilelist(t, proxyURL, bucket, errCh, fileslist, false)
	for _, fname := range fileslist {
		if b, _ := tutils.IsCached(proxyURL, bucket, fname); !b {
			t.Fatalf("Object not cached: %s", fname)
		}
	}

	//test property, mirror is disabled for cloud bucket that hasn't been accessed, even if system config says otherwise
	api.SetBucketProp(tutils.DefaultBaseAPIParams(t), bucket, cmn.HeaderBucketMirrorEnabled, true)
	bProps, err = api.HeadBucket(tutils.DefaultBaseAPIParams(t), bucket)
	tutils.CheckFatal(err, t)
	if !bProps.Mirror.Enabled {
		t.Fatalf("Test property not changed")
	}
	query.Add(cmn.URLParamBckProvider, cmn.CloudBs)
	api.EvictCloudBucket(tutils.DefaultBaseAPIParams(t), bucket, query)

	for _, fname := range fileslist {
		if b, _ := tutils.IsCached(proxyURL, bucket, fname); b {
			t.Fatalf("Object still cached: %s", fname)
		}
	}
	bProps, err = api.HeadBucket(tutils.DefaultBaseAPIParams(t), bucket)
	tutils.CheckFatal(err, t)
	if bProps.Mirror.Enabled {
		t.Fatalf("Test property not reset ")
	}
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
		bucketName = TestLocalBucketName
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		fqn        string
		errstr     string
		err        error
	)

	if tutils.DockerRunning() {
		t.Skip(fmt.Sprintf("test %q requires Xattributes to be set, doesn't work with docker", t.Name()))
	}

	tutils.CreateFreshLocalBucket(t, proxyURL, bucketName)
	sgl := tutils.Mem2.NewSGL(fileSize)
	defer sgl.Free()
	tutils.PutRandObjs(proxyURL, bucketName, ChecksumWarmValidateDir, readerType, ChecksumWarmValidateStr, fileSize, numFiles, errCh, fileNameCh, sgl)
	selectErr(errCh, "put", t, false)

	// Get Current Config
	config := getDaemonConfig(t, proxyURL)
	oldWarmGet := config.Cksum.ValidateWarmGet
	oldChecksum := config.Cksum.Type

	var fileName string
	fsWalkFunc := func(path string, info os.FileInfo, err error) error {
		if info == nil || (info.IsDir() && info.Name() == "cloud") {
			return filepath.SkipDir
		}
		if filepath.Base(path) == fileName && strings.Contains(path, filepath.Join(bucketName, ChecksumWarmValidateStr)) {
			fqn = path
		}
		return nil
	}

	if !oldWarmGet {
		setClusterConfig(t, proxyURL, "cksum.validate_warm_get", true)
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
	errstr = fs.SetXattr(fqn, cmn.XattrXXHash, []byte("01234abcde"))
	if errstr != "" {
		t.Error(errstr)
	}
	executeTwoGETsForChecksumValidation(proxyURL, bucketName, fileName, t)

	// Test for none checksum algo
	fileName = <-fileNameCh
	filepath.Walk(rootDir, fsWalkFunc)
	setClusterConfig(t, proxyURL, "cksum.type", cmn.ChecksumNone)
	if t.Failed() {
		goto cleanup
	}
	tutils.Logf("Changing file xattr[%s]: %s\n", fileName, fqn)
	errstr = fs.SetXattr(fqn, cmn.XattrXXHash, []byte("01234abcde"))
	if errstr != "" {
		t.Error(errstr)
	}
	_, err = api.GetObject(tutils.DefaultBaseAPIParams(t), bucketName, path.Join(ChecksumWarmValidateStr, fileName))
	if err != nil {
		t.Error("A GET on an object when checksum algo is none should pass")
	}

cleanup:
	// Restore old config
	tutils.DestroyLocalBucket(t, proxyURL, bucketName)
	setClusterConfig(t, proxyURL, "cksum.type", oldChecksum)
	setClusterConfig(t, proxyURL, "cksum.validate_warm_get", oldWarmGet)
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
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		bucketName = clibucket
		fileName   string
	)

	sgl := tutils.Mem2.NewSGL(fileSize)
	defer sgl.Free()
	created := createLocalBucketIfNotExists(t, proxyURL, clibucket)
	tutils.PutRandObjs(proxyURL, bucketName, RangeGetDir, readerType, RangeGetStr, fileSize, numFiles, errCh, fileNameCh, sgl)
	selectErr(errCh, "put", t, false)

	// Get Current Config
	config := getDaemonConfig(t, proxyURL)
	oldEnableReadRangeChecksum := config.Cksum.EnableReadRange

	fileName = <-fileNameCh
	tutils.Logln("Testing valid cases.")
	// Validate entire object checksum is being returned
	if oldEnableReadRangeChecksum {
		setClusterConfig(t, proxyURL, "cksum.enable_read_range", false)
		if t.Failed() {
			goto cleanup
		}
	}
	testValidCases(fileSize, t, proxyURL, bucketName, fileName, true, RangeGetStr)

	// Validate only range checksum is being returned
	if !oldEnableReadRangeChecksum {
		setClusterConfig(t, proxyURL, "cksum.enable_read_range", true)
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

	wg.Add(1)
	go tutils.Del(proxyURL, clibucket, filepath.Join(RangeGetStr, fileName), "", wg, errCh, !testing.Verbose())
	wg.Wait()
	selectErr(errCh, "delete", t, false)
	setClusterConfig(t, proxyURL, "cksum.enable_read_range", oldEnableReadRangeChecksum)
	close(errCh)
	close(fileNameCh)

	if created {
		tutils.DestroyLocalBucket(t, proxyURL, clibucket)
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
		totalio     = numPuts * largefilesize
		proxyURL    = getPrimaryURL(t, proxyURLReadOnly)
	)

	created := createLocalBucketIfNotExists(t, proxyURL, bucket)
	ldir := filepath.Join(LocalSrcDir, ChksumValidStr)
	if err := cmn.CreateDir(ldir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", ldir, err)
	}

	// Get Current Config
	config := getDaemonConfig(t, proxyURL)
	ocoldget := config.Cksum.ValidateColdGet
	ochksum := config.Cksum.Type

	sgl := tutils.Mem2.NewSGL(filesize)
	defer sgl.Free()
	tutils.PutRandObjs(proxyURL, bucket, ldir, readerType, ChksumValidStr, filesize, int(numPuts), errCh, filesPutCh, sgl)
	selectErr(errCh, "put", t, false)
	close(filesPutCh) // to exit for-range
	for fname := range filesPutCh {
		if fname != "" {
			fileslist = append(fileslist, filepath.Join(ChksumValidStr, fname))
		}
	}
	// Delete it from cache.
	evictobjects(t, proxyURL, cmn.CloudBs, fileslist)
	// Disable checkum
	if ochksum != cmn.ChecksumNone {
		setClusterConfig(t, proxyURL, "cksum.type", cmn.ChecksumNone)
	}
	if t.Failed() {
		goto cleanup
	}
	// Disable Cold Get Validation
	if ocoldget {
		setClusterConfig(t, proxyURL, "cksum.validate_cold_get", false)
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
	evictobjects(t, proxyURL, cmn.CloudBs, fileslist)
	switch clichecksum {
	case "all":
		setClusterConfig(t, proxyURL, "cksum.type", cmn.ChecksumXXHash)
		setClusterConfig(t, proxyURL, "cksum.validate_cold_get", true)
		if t.Failed() {
			goto cleanup
		}
	case cmn.ChecksumXXHash:
		setClusterConfig(t, proxyURL, "cksum.type", cmn.ChecksumXXHash)
		if t.Failed() {
			goto cleanup
		}
	case ColdMD5str:
		setClusterConfig(t, proxyURL, "cksum.validate_cold_get", true)
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
	setClusterConfig(t, proxyURL, "cksum.type", ochksum)
	setClusterConfig(t, proxyURL, "cksum.validate_cold_get", ocoldget)

	if created {
		tutils.DestroyLocalBucket(t, proxyURL, bucket)
	}
}

// deletefromfilelist requires that errCh be twice the size of len(fileslist) as each
// file can produce upwards of two errors.
func deletefromfilelist(proxyURL, bucket string, errCh chan error, fileslist []string) {
	wg := &sync.WaitGroup{}
	// Delete local file and objects from bucket
	for _, fn := range fileslist {
		wg.Add(1)
		go tutils.Del(proxyURL, bucket, fn, "", wg, errCh, true)
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

func evictobjects(t *testing.T, proxyURL, bckProvider string, fileslist []string) {
	err := api.EvictList(tutils.BaseAPIParams(proxyURL), clibucket, bckProvider, fileslist, true, 0)
	if err != nil {
		t.Errorf("Evict bucket %s failed, err = %v", clibucket, err)
	}
}

func createLocalBucketIfNotExists(t *testing.T, proxyURL, bucket string) (created bool) {
	baseParams := tutils.BaseAPIParams(proxyURL)
	buckets, err := api.GetBucketNames(baseParams, "")
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
	buckets, err := api.GetBucketNames(baseParams, "")
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
	if actual.Cksum.Type != expected.Cksum.Type {
		t.Errorf("Expected checksum type: %s, received checksum type: %s",
			expected.Cksum.Type, actual.Cksum.Type)
	}
	if actual.Cksum.ValidateColdGet != expected.Cksum.ValidateColdGet {
		t.Errorf("Expected cold GET validation setting: %t, received: %t",
			expected.Cksum.ValidateColdGet, actual.Cksum.ValidateColdGet)
	}
	if actual.Cksum.ValidateWarmGet != expected.Cksum.ValidateWarmGet {
		t.Errorf("Expected warm GET validation setting: %t, received: %t",
			expected.Cksum.ValidateWarmGet, actual.Cksum.ValidateWarmGet)
	}
	if actual.Cksum.EnableReadRange != expected.Cksum.EnableReadRange {
		t.Errorf("Expected byte range validation setting: %t, received: %t",
			expected.Cksum.EnableReadRange, actual.Cksum.EnableReadRange)
	}
	if actual.LRU.LowWM != expected.LRU.LowWM {
		t.Errorf("Expected LowWM setting: %d, received %d", expected.LRU.LowWM, actual.LRU.LowWM)
	}
	if actual.LRU.HighWM != expected.LRU.HighWM {
		t.Errorf("Expected HighWM setting: %d, received %d", expected.LRU.HighWM, actual.LRU.HighWM)
	}
	if actual.LRU.AtimeCacheMax != expected.LRU.AtimeCacheMax {
		t.Errorf("Expected AtimeCacheMax setting: %d, received %d",
			expected.LRU.AtimeCacheMax, actual.LRU.AtimeCacheMax)
	}
	if actual.LRU.DontEvictTimeStr != expected.LRU.DontEvictTimeStr {
		t.Errorf("Expected DontEvictTimeStr setting: %s, received %s",
			expected.LRU.DontEvictTimeStr, actual.LRU.DontEvictTimeStr)
	}
	if actual.LRU.CapacityUpdTimeStr != expected.LRU.CapacityUpdTimeStr {
		t.Errorf("Expected CapacityUpdTimeStr setting: %s, received %s",
			expected.LRU.CapacityUpdTimeStr, actual.LRU.CapacityUpdTimeStr)
	}
	if actual.LRU.Enabled != expected.LRU.Enabled {
		t.Errorf("Expected LRU enabled setting: %t, received: %t",
			expected.LRU.Enabled, actual.LRU.Enabled)
	}
}

func defaultBucketProps() cmn.BucketProps {
	return cmn.BucketProps{
		CloudProvider: cmn.ProviderAIS,
		NextTierURL:   "http://foo.com",
		ReadPolicy:    cmn.RWPolicyNextTier,
		WritePolicy:   cmn.RWPolicyNextTier,
		Cksum: cmn.CksumConf{
			Type:            cmn.ChecksumXXHash,
			ValidateColdGet: false,
			ValidateWarmGet: false,
			EnableReadRange: false,
		},
		LRU: cmn.LRUConf{
			LowWM:              int64(10),
			HighWM:             int64(50),
			OOS:                int64(90),
			AtimeCacheMax:      int64(9999),
			DontEvictTimeStr:   "1m",
			CapacityUpdTimeStr: "2m",
			Enabled:            false,
		},
	}
}
