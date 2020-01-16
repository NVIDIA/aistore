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

	"github.com/NVIDIA/aistore/containers"

	"github.com/NVIDIA/aistore/cluster"

	"github.com/NVIDIA/aistore/tutils/tassert"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
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

	err := tutils.PingURL(proxyURL)
	tassert.CheckFatal(t, err)

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
		errCh <- errors.New(s)
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
	if created := createBucketIfNotExists(t, proxyURL, clibucket); created {
		defer tutils.DestroyBucket(t, proxyURL, clibucket)
	}

	// Declare one channel per worker to pass the keyname
	keynameChans := make([]chan string, numworkers)
	for i := 0; i < numworkers; i++ {
		// Allow a bunch of messages at a time to be written asynchronously to a channel
		keynameChans[i] = make(chan string, 100)
	}
	// Start the worker pools
	errCh := make(chan error, 100)
	var wg = &sync.WaitGroup{}
	// Get the workers started
	for i := 0; i < numworkers; i++ {
		wg.Add(1)
		go deleteFiles(proxyURL, keynameChans[i], wg, errCh, clibucket)
	}

	// list the bucket
	var msg = &cmn.SelectMsg{PageSize: int(pagesize)}
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
		keynameChans[num%numworkers] <- name
		num++
		if num >= numfiles {
			break
		}
	}
	// Close the channels after the reading is done
	for i := 0; i < numworkers; i++ {
		close(keynameChans[i])
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

	if testing.Short() && !isCloudBucket(t, proxyURL, clibucket) {
		t.Skipf("don't run when short mode and cloud bucket")
	}

	const (
		commonPrefix = "tst" // object full name: <bucket>/<commonPrefix>/<generated_name:a-####|b-####>
		objSize      = 16 * 1024
	)
	proxyURL := getPrimaryURL(t, proxyURLReadOnly)

	if err := cmn.CreateDir(DeleteDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", DeleteDir, err)
	}
	if created := createBucketIfNotExists(t, proxyURL, clibucket); created {
		defer tutils.DestroyBucket(t, proxyURL, clibucket)
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
		msg := &cmn.SelectMsg{Prefix: commonPrefix + "/"}
		tutils.Logf("%d. %s\n    Prefix: [%s], range: [%s], regexp: [%s]\n", idx+1, test.name, test.prefix, test.rangeStr, test.regexStr)

		err := api.DeleteRange(baseParams, clibucket, "", test.prefix, test.regexStr, test.rangeStr, true, 0)
		if err != nil {
			t.Error(err)
			continue
		}

		totalFiles -= test.delta
		bktlst, err := api.ListBucket(baseParams, clibucket, msg, 0)
		if err != nil {
			t.Error(err)
			continue
		}
		if len(bktlst.Entries) != totalFiles {
			t.Errorf("Incorrect number of remaining files: %d, should be %d", len(bktlst.Entries), totalFiles)
		} else {
			tutils.Logf("  %d files have been deleted\n", test.delta)
		}
	}

	tutils.Logf("Cleaning up remained objects...\n")
	msg := &cmn.SelectMsg{Prefix: commonPrefix + "/"}
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
		go deleteFiles(proxyURL, keynameChans[i], wg, errCh, clibucket)
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
	if testing.Short() && !isCloudBucket(t, proxyURL, clibucket) {
		t.Skipf("don't run when short mode and cloud bucket")
	}

	if err := cmn.CreateDir(DeleteDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", DeleteDir, err)
	}

	errCh := make(chan error, numfiles)
	filesPutCh := make(chan string, numfiles)
	const filesize = 512 * 1024
	proxyURL := getPrimaryURL(t, proxyURLReadOnly)
	if created := createBucketIfNotExists(t, proxyURL, clibucket); created {
		defer tutils.DestroyBucket(t, proxyURL, clibucket)
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
		go deleteFiles(proxyURL, keynameChans[i], wg, errCh, clibucket)
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

func listObjects(t *testing.T, proxyURL string, msg *cmn.SelectMsg, bucket string, objLimit int) (*cmn.BucketList, error) {
	resList := testListBucket(t, proxyURL, bucket, msg, objLimit)
	if resList == nil {
		return nil, fmt.Errorf("failed to list bucket %s", bucket)
	}
	for _, m := range resList.Entries {
		if len(m.Checksum) > 8 {
			tutils.Logf("%s %d [%s] %s [%v - %s]\n", m.Name, m.Size, m.Version, m.Checksum[:8]+"...", m.CheckExists, m.Atime)
		} else {
			tutils.Logf("%s %d [%s] %s [%v - %s]\n", m.Name, m.Size, m.Version, m.Checksum, m.CheckExists, m.Atime)
		}
	}

	tutils.Logln("----------------")
	tutils.Logf("Total objects listed: %v\n", len(resList.Entries))
	return resList, nil
}

func Test_BucketNames(t *testing.T) {
	var (
		baseParams = tutils.DefaultBaseAPIParams(t)
		bucket     = t.Name() + "Bucket"
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
	)
	tutils.CreateFreshBucket(t, proxyURL, bucket)
	defer tutils.DestroyBucket(t, proxyURL, bucket)

	buckets, err := api.GetBucketNames(baseParams, "")
	tassert.CheckFatal(t, err)

	tutils.Logf("cloud bucket names: %s\n", strings.Join(buckets.Cloud, ", "))
	tutils.Logf("ais bucket names:\n")
	printBucketNames(t, buckets.AIS)

	for _, provider := range []string{cmn.ProviderAmazon, cmn.ProviderGoogle} {
		cloudBuckets, err := api.GetBucketNames(baseParams, provider)
		tassert.CheckError(t, err)
		if len(cloudBuckets.Cloud) != len(buckets.Cloud) {
			t.Fatalf("cloud buckets: %d != %d\n", len(cloudBuckets.Cloud), len(buckets.Cloud))
		}
		if len(cloudBuckets.AIS) != 0 {
			t.Fatalf("cloud buckets contain ais: %+v\n", cloudBuckets.AIS)
		}
	}
	aisBuckets, err := api.GetBucketNames(baseParams, cmn.ProviderAIS)
	tassert.CheckError(t, err)
	if len(aisBuckets.AIS) != len(buckets.AIS) {
		t.Fatalf("ais buckets: %d != %d\n", len(aisBuckets.AIS), len(buckets.AIS))
	}
	if len(aisBuckets.Cloud) != 0 {
		t.Fatalf("ais buckets contain cloud: %+v\n", aisBuckets.Cloud)
	}
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
	queryLocal.Add(cmn.URLParamProvider, cmn.ProviderAIS)
	queryCloud.Add(cmn.URLParamProvider, cmn.Cloud)

	putArgsLocal := api.PutObjectArgs{
		BaseParams: baseParams,
		Bucket:     bucketName,
		Provider:   cmn.ProviderAIS,
		Object:     fileName1,
		Reader:     tutils.NewBytesReader(dataLocal),
	}

	putArgsCloud := api.PutObjectArgs{
		BaseParams: baseParams,
		Bucket:     bucketName,
		Provider:   cmn.Cloud,
		Object:     fileName1,
		Reader:     tutils.NewBytesReader(dataCloud),
	}

	// PUT/GET/DEL Without ais bucket
	tutils.Logf("Validating responses for non-existent ais bucket...\n")
	err := api.PutObject(putArgsLocal)
	if err == nil {
		t.Fatalf("ais bucket %s does not exist: Expected an error.", bucketName)
	}

	_, err = api.GetObject(baseParams, bucketName, fileName1, api.GetObjectInput{Query: queryLocal})
	if err == nil {
		t.Fatalf("ais bucket %s does not exist: Expected an error.", bucketName)
	}

	err = api.DeleteObject(baseParams, bucketName, fileName1, cmn.ProviderAIS)
	if err == nil {
		t.Fatalf("ais bucket %s does not exist: Expected an error.", bucketName)
	}

	tutils.Logf("PrefetchList %d\n", len(files))
	err = api.PrefetchList(baseParams, clibucket, "", files, true, 0)
	if err != nil {
		t.Fatalf("%v\n", err)
	}
	tutils.Logf("PrefetchRange\n")
	err = api.PrefetchRange(baseParams, clibucket, "", "r", "", prefetchRange, true, 0)
	if err != nil {
		t.Fatalf("%v\n", err)
	}
	tutils.Logf("EvictList\n")
	err = api.EvictList(baseParams, bucketName, "", files, true, 0)
	if err != nil {
		t.Fatalf("%v\n", err)
	}
	tutils.Logf("EvictRange\n")
	err = api.EvictRange(baseParams, clibucket, "", "", "", prefetchRange, true, 0)
	if err != nil {
		t.Fatalf("%v\n", err)
	}

	tutils.CreateFreshBucket(t, proxyURL, bucketName)
	defer tutils.DestroyBucket(t, proxyURL, bucketName)

	// PUT
	tutils.Logf("Putting %s and %s into buckets...\n", fileName1, fileName2)
	err = api.PutObject(putArgsLocal)
	tassert.CheckFatal(t, err)
	putArgsLocal.Object = fileName2
	err = api.PutObject(putArgsLocal)
	tassert.CheckFatal(t, err)

	err = api.PutObject(putArgsCloud)
	tassert.CheckFatal(t, err)
	putArgsCloud.Object = fileName2
	err = api.PutObject(putArgsCloud)
	tassert.CheckFatal(t, err)

	// Check ais bucket has 2 objects
	tutils.Logf("Validating ais bucket have %s and %s ...\n", fileName1, fileName2)
	_, err = api.HeadObject(baseParams, bucketName, cmn.ProviderAIS, fileName1)
	tassert.CheckFatal(t, err)
	_, err = api.HeadObject(baseParams, bucketName, cmn.ProviderAIS, fileName2)
	tassert.CheckFatal(t, err)

	// Prefetch/Evict should work
	err = api.PrefetchList(baseParams, clibucket, cmn.Cloud, files, true, 0)
	tassert.CheckFatal(t, err)
	err = api.EvictList(baseParams, bucketName, cmn.Cloud, files, true, 0)
	tassert.CheckFatal(t, err)

	// Deleting from cloud bucket
	tutils.Logf("Deleting %s and %s from cloud bucket ...\n", fileName1, fileName2)
	api.DeleteList(baseParams, bucketName, cmn.Cloud, files, true, 0)

	// Deleting from ais bucket
	tutils.Logf("Deleting %s and %s from ais bucket ...\n", fileName1, fileName2)
	api.DeleteList(baseParams, bucketName, cmn.ProviderAIS, files, true, 0)

	_, err = api.HeadObject(baseParams, bucketName, cmn.ProviderAIS, fileName1)
	if !strings.Contains(err.Error(), strconv.Itoa(http.StatusNotFound)) {
		t.Errorf("Local file %s not deleted", fileName1)
	}
	_, err = api.HeadObject(baseParams, bucketName, cmn.ProviderAIS, fileName2)
	if !strings.Contains(err.Error(), strconv.Itoa(http.StatusNotFound)) {
		t.Errorf("Local file %s not deleted", fileName2)
	}

	_, err = api.HeadObject(baseParams, bucketName, cmn.Cloud, fileName1)
	if !strings.Contains(err.Error(), strconv.Itoa(http.StatusNotFound)) {
		t.Errorf("Cloud file %s not deleted", fileName1)
	}
	_, err = api.HeadObject(baseParams, bucketName, cmn.Cloud, fileName2)
	if !strings.Contains(err.Error(), strconv.Itoa(http.StatusNotFound)) {
		t.Errorf("Cloud file %s not deleted", fileName2)
	}
}

func Test_SameAISAndCloudBucketName(t *testing.T) {
	var (
		defLocalProps cmn.BucketPropsToUpdate
		defCloudProps cmn.BucketPropsToUpdate

		bucketName = clibucket
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		fileName   = "mytestobj1.txt"
		baseParams = tutils.DefaultBaseAPIParams(t)
		queryLocal = url.Values{}
		queryCloud = url.Values{}
		dataLocal  = []byte("im local")
		dataCloud  = []byte("I'm from the cloud!")
		msg        = &cmn.SelectMsg{PageSize: int(pagesize), Props: "size,status"}
		found      = false
	)

	if !isCloudBucket(t, proxyURL, bucketName) {
		t.Skipf("%s requires a cloud bucket", t.Name())
	}

	queryLocal.Add(cmn.URLParamProvider, cmn.ProviderAIS)
	queryCloud.Add(cmn.URLParamProvider, cmn.Cloud)

	tutils.CreateFreshBucket(t, proxyURL, bucketName)
	defer tutils.DestroyBucket(t, proxyURL, bucketName)

	bucketPropsLocal := cmn.BucketPropsToUpdate{
		Cksum: &cmn.CksumConfToUpdate{
			Type: api.String(cmn.ChecksumNone),
		},
	}
	bucketPropsCloud := cmn.BucketPropsToUpdate{}

	// Put
	tutils.Logf("Putting object (%s) into ais bucket %s...\n", fileName, bucketName)
	putArgs := api.PutObjectArgs{
		BaseParams: baseParams,
		Bucket:     bucketName,
		Provider:   cmn.ProviderAIS,
		Object:     fileName,
		Reader:     tutils.NewBytesReader(dataLocal),
	}
	err := api.PutObject(putArgs)
	tassert.CheckFatal(t, err)

	resLocal, err := api.ListBucket(baseParams, bucketName, msg, 0, queryLocal)
	tassert.CheckFatal(t, err)

	tutils.Logf("Putting object (%s) into cloud bucket %s...\n", fileName, bucketName)
	putArgs = api.PutObjectArgs{
		BaseParams: baseParams,
		Bucket:     bucketName,
		Provider:   cmn.Cloud,
		Object:     fileName,
		Reader:     tutils.NewBytesReader(dataCloud),
	}
	err = api.PutObject(putArgs)
	tassert.CheckFatal(t, err)

	resCloud, err := api.ListBucket(baseParams, bucketName, msg, 0, queryCloud)
	tassert.CheckFatal(t, err)

	if len(resLocal.Entries) != 1 {
		t.Fatalf("Expected number of files in ais bucket (%s) does not match: expected %v, got %v", bucketName, 1, len(resLocal.Entries))
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
	tassert.CheckFatal(t, err)
	lenCloud, err := api.GetObject(baseParams, bucketName, fileName, api.GetObjectInput{Query: queryCloud})
	tassert.CheckFatal(t, err)

	if lenLocal == lenCloud {
		t.Errorf("Local file and cloud file have same size, expected: local (%v) cloud (%v) got: local (%v) cloud (%v)",
			len(dataLocal), len(dataCloud), lenLocal, lenCloud)
	}

	// Delete
	err = api.DeleteObject(baseParams, bucketName, fileName, cmn.Cloud)
	tassert.CheckFatal(t, err)

	lenLocal, err = api.GetObject(baseParams, bucketName, fileName, api.GetObjectInput{Query: queryLocal})
	tassert.CheckFatal(t, err)

	// Check that local object still exists
	if lenLocal != int64(len(dataLocal)) {
		t.Errorf("Local file %s deleted", fileName)
	}

	// Check that cloud object is deleted using HeadObject
	_, err = api.HeadObject(baseParams, bucketName, cmn.Cloud, fileName)
	if !strings.Contains(err.Error(), strconv.Itoa(http.StatusNotFound)) {
		t.Errorf("Cloud file %s not deleted", fileName)
	}

	// Set Props Object
	err = api.SetBucketProps(baseParams, bucketName, bucketPropsLocal, queryLocal)
	tassert.CheckFatal(t, err)

	err = api.SetBucketProps(baseParams, bucketName, bucketPropsCloud, queryCloud)
	tassert.CheckFatal(t, err)

	// Validate ais bucket props are set
	localProps, err := api.HeadBucket(baseParams, bucketName, queryLocal)
	tassert.CheckFatal(t, err)
	validateBucketProps(t, bucketPropsLocal, localProps)

	// Validate cloud bucket props are set
	cloudProps, err := api.HeadBucket(baseParams, bucketName, queryCloud)
	tassert.CheckFatal(t, err)
	validateBucketProps(t, bucketPropsCloud, cloudProps)

	// Reset ais bucket props and validate they are reset
	err = api.ResetBucketProps(baseParams, bucketName, queryLocal)
	tassert.CheckFatal(t, err)
	localProps, err = api.HeadBucket(baseParams, bucketName, queryLocal)
	tassert.CheckFatal(t, err)
	validateBucketProps(t, defLocalProps, localProps)

	// Check if cloud bucket props remain the same
	cloudProps, err = api.HeadBucket(baseParams, bucketName, queryCloud)
	tassert.CheckFatal(t, err)
	validateBucketProps(t, bucketPropsCloud, cloudProps)

	// Reset cloud bucket props
	err = api.ResetBucketProps(baseParams, bucketName, queryCloud)
	tassert.CheckFatal(t, err)
	cloudProps, err = api.HeadBucket(baseParams, bucketName, queryCloud)
	tassert.CheckFatal(t, err)
	validateBucketProps(t, defCloudProps, cloudProps)

	// Check if ais bucket props remain the same
	localProps, err = api.HeadBucket(baseParams, bucketName, queryLocal)
	tassert.CheckFatal(t, err)
	validateBucketProps(t, defLocalProps, localProps)
}

func Test_coldgetmd5(t *testing.T) {
	var (
		numPuts    = 5
		filesPutCh = make(chan string, numPuts)
		filesList  = make([]string, 0, 100)
		errCh      = make(chan error, 100)
		wg         = &sync.WaitGroup{}
		bucket     = clibucket
		totalSize  = int64(numPuts * largeFileSize)
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
	)

	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skipf("%s requires a cloud bucket", t.Name())
	}

	ldir := filepath.Join(LocalSrcDir, ColdValidStr)
	if err := cmn.CreateDir(ldir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", ldir, err)
	}

	config := getClusterConfig(t, proxyURL)
	bcoldget := config.Cksum.ValidateColdGet

	sgl := tutils.Mem2.NewSGL(largeFileSize)
	defer sgl.Free()
	tutils.PutRandObjs(proxyURL, bucket, ldir, readerType, ColdValidStr, largeFileSize, numPuts, errCh, filesPutCh, sgl)
	selectErr(errCh, "put", t, false)
	close(filesPutCh) // to exit for-range
	for fname := range filesPutCh {
		filesList = append(filesList, filepath.Join(ColdValidStr, fname))
	}
	tutils.EvictObjects(t, proxyURL, filesList, clibucket)
	// Disable Cold Get Validation
	if bcoldget {
		setClusterConfig(t, proxyURL, cmn.SimpleKVs{"cksum.validate_cold_get": "false"})
	}
	start := time.Now()
	getFromObjList(proxyURL, bucket, errCh, filesList, false)
	curr := time.Now()
	duration := curr.Sub(start)
	if t.Failed() {
		goto cleanup
	}
	tutils.Logf("GET %s without MD5 validation: %v\n", cmn.B2S(totalSize, 0), duration)
	selectErr(errCh, "get", t, false)
	tutils.EvictObjects(t, proxyURL, filesList, clibucket)
	// Enable Cold Get Validation
	setClusterConfig(t, proxyURL, cmn.SimpleKVs{"cksum.validate_cold_get": "true"})
	if t.Failed() {
		goto cleanup
	}
	start = time.Now()
	getFromObjList(proxyURL, bucket, errCh, filesList, true)
	curr = time.Now()
	duration = curr.Sub(start)
	tutils.Logf("GET %s with MD5 validation:    %v\n", cmn.B2S(totalSize, 0), duration)
	selectErr(errCh, "get", t, false)
cleanup:
	setClusterConfig(t, proxyURL, cmn.SimpleKVs{"cksum.validate_cold_get": fmt.Sprintf("%v", bcoldget)})
	for _, fn := range filesList {
		wg.Add(1)
		go tutils.Del(proxyURL, bucket, fn, "", wg, errCh, !testing.Verbose())
	}
	wg.Wait()
	selectErr(errCh, "delete", t, false)
	close(errCh)
}

func TestHeadBucket(t *testing.T) {
	var (
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	tutils.CreateFreshBucket(t, proxyURL, TestBucketName)
	defer tutils.DestroyBucket(t, proxyURL, TestBucketName)

	bckPropsToUpdate := cmn.BucketPropsToUpdate{
		Cksum: &cmn.CksumConfToUpdate{
			ValidateWarmGet: api.Bool(true),
		},
		LRU: &cmn.LRUConfToUpdate{
			Enabled: api.Bool(true),
		},
	}
	err := api.SetBucketProps(baseParams, TestBucketName, bckPropsToUpdate)
	tassert.CheckFatal(t, err)

	p, err := api.HeadBucket(baseParams, TestBucketName)
	tassert.CheckFatal(t, err)

	validateBucketProps(t, bckPropsToUpdate, p)
}

func TestHeadCloudBucket(t *testing.T) {
	var (
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skip(fmt.Sprintf("%s requires a cloud bucket", t.Name()))
	}

	bckPropsToUpdate := cmn.BucketPropsToUpdate{
		Cksum: &cmn.CksumConfToUpdate{
			ValidateWarmGet: api.Bool(true),
			ValidateColdGet: api.Bool(true),
		},
		LRU: &cmn.LRUConfToUpdate{
			Enabled: api.Bool(true),
		},
	}
	err := api.SetBucketProps(baseParams, clibucket, bckPropsToUpdate)
	tassert.CheckFatal(t, err)
	defer resetBucketProps(proxyURL, clibucket, t)

	p, err := api.HeadBucket(baseParams, clibucket)
	tassert.CheckFatal(t, err)
	validateBucketProps(t, bckPropsToUpdate, p)
}

func TestHeadNonexistentBucket(t *testing.T) {
	var (
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	bucket, err := tutils.GenerateNonexistentBucketName("head", baseParams)
	tassert.CheckFatal(t, err)

	_, err = api.HeadBucket(baseParams, bucket)
	if err == nil {
		t.Fatalf("Expected an error, but go no errors.")
	}
	httpErr, ok := err.(*cmn.HTTPError)
	if !ok {
		t.Fatalf("Expected an error of type *cmn.HTTPError, but got: %T.", err)
	}
	if httpErr.Status != http.StatusNotFound {
		t.Errorf("Expected status: %d, got: %d.", http.StatusNotFound, httpErr.Status)
	}
}

func TestHeadObject(t *testing.T) {
	var (
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		objName  = "headobject_test_obj"
		objSize  = int64(1024)

		putTime time.Time
	)
	tutils.CreateFreshBucket(t, proxyURL, TestBucketName)
	defer tutils.DestroyBucket(t, proxyURL, TestBucketName)

	r, rrErr := tutils.NewRandReader(objSize, true)
	if rrErr != nil {
		t.Fatalf("tutils.NewRandReader failed, err = %v", rrErr)
	}
	defer r.Close()
	hash := r.XXHash()
	putArgs := api.PutObjectArgs{
		BaseParams: tutils.DefaultBaseAPIParams(t),
		Bucket:     TestBucketName,
		Object:     objName,
		Hash:       hash,
		Reader:     r,
	}

	putTime, _ = time.Parse(time.RFC822, time.Now().Format(time.RFC822)) // Get the time in the same format as atime (with milliseconds truncated)
	if err := api.PutObject(putArgs); err != nil {
		t.Fatalf("api.PutObject failed, err = %v", err)
	}

	propsExp := &cmn.ObjectProps{Size: objSize, Version: "1", NumCopies: 1, Checksum: hash, Present: true, Provider: cmn.ProviderAIS}
	props, err := api.HeadObject(tutils.DefaultBaseAPIParams(t), TestBucketName, "", objName)
	if err != nil {
		t.Errorf("api.HeadObject failed, err = %v", err)
	}

	if props.Size != propsExp.Size {
		t.Errorf("Returned `Size` not correct. Expected: %v, actual: %v", propsExp.Size, props.Size)
	}
	if props.Version != propsExp.Version {
		t.Errorf("Returned `Version` not correct. Expected: %v, actual: %v", propsExp.Version, props.Version)
	}
	if props.Provider != propsExp.Provider {
		t.Errorf("Returned `Provider` not correct. Expected: %v, actual: %v", propsExp.Provider, props.Provider)
	}
	if props.NumCopies != propsExp.NumCopies {
		t.Errorf("Returned `Number` of copies not correct. Expected: %v, actual: %v", propsExp.NumCopies, props.NumCopies)
	}
	if props.Checksum != propsExp.Checksum {
		t.Errorf("Returned `Checksum` not correct. Expected: %v, actual: %v", propsExp.Checksum, props.Checksum)
	}
	if props.Present != propsExp.Present {
		t.Errorf("Returned `Present` not correct. Expected: %v, actual: %v", propsExp.Present, props.Present)
	}
	if props.Atime.IsZero() {
		t.Fatalf("Returned `Atime` (%s) is zero.", props.Atime)
	}
	if props.Atime.Before(putTime) {
		t.Errorf("Returned `Atime` (%s) not correct - expected `atime` after `put` time (%s)", props.Atime, putTime.Format(time.RFC822))
	}
}

func TestHeadNonexistentObject(t *testing.T) {
	var (
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		objName  = "this_object_should_not_exist"
	)
	tutils.CreateFreshBucket(t, proxyURL, TestBucketName)
	defer tutils.DestroyBucket(t, proxyURL, TestBucketName)

	_, err := api.HeadObject(tutils.DefaultBaseAPIParams(t), TestBucketName, "", objName)
	if err == nil {
		t.Errorf("Expected non-nil error (404) from api.HeadObject, received nil error")
	}
}

func TestHeadObjectCheckExists(t *testing.T) {
	var (
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		fileName = "headobject_check_cached_test_file"
		fileSize = 1024
	)

	if created := createBucketIfNotExists(t, proxyURL, clibucket); created {
		defer tutils.DestroyBucket(t, proxyURL, clibucket)
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
	tassert.CheckFatal(t, err)

	b, err := tutils.CheckExists(proxyURL, clibucket, fileName)
	tassert.CheckFatal(t, err)
	if !b {
		t.Error("Expected object to be cached, got false from tutils.CheckExists")
	}

	err = tutils.Del(proxyURL, clibucket, fileName, "", nil, nil, true)
	tassert.CheckFatal(t, err)

	b, err = tutils.CheckExists(proxyURL, clibucket, fileName)
	tassert.CheckFatal(t, err)
	if b {
		t.Error("Expected object to NOT be cached after deleting object, got true from tutils.CheckExists")
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

	t.Logf("%2d: GET %q", id, url)
	resp, err := http.Get(url)
	if err == nil && resp == nil {
		err = fmt.Errorf("empty response")
	}

	if err != nil {
		errCh <- err
		t.Error(err)
		failed = true
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		err = fmt.Errorf("%2d: Get %s from bucket %s http error %d", id, keyname, bucket, resp.StatusCode)
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
		t.Errorf("%2d: Failed to create object, err: %v", id, err)
		failed = true
		return
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("%2d: %v", id, err)
		}
	}()
	if hdhashtype == cmn.ChecksumXXHash {
		written, cksumVal, err = cmn.WriteWithHash(file, resp.Body, nil)
		if err != nil {
			t.Errorf("%2d: failed to write object, err: %v", id, err)
			failed = true
			return
		}
		if hdhash != cksumVal {
			t.Errorf("%2d: header's %s %s doesn't match object's %s", id, cmn.ChecksumXXHash, hdhash, cksumVal)
			failed = true
			return
		}
		tutils.Logf("%2d: header's %s %s matches object's %s\n", id, cmn.ChecksumXXHash, hdhash, cksumVal)
	} else if hdhashtype == cmn.ChecksumMD5 {
		md5 := md5.New()
		written, err = cmn.ReceiveAndChecksum(file, resp.Body, nil, md5)
		if err != nil {
			t.Errorf("%2d: failed to write object, err: %v", id, err)
			return
		}
		md5hash := cmn.HashToStr(md5)[:16]
		if errstr != "" {
			t.Errorf("%2d: failed to compute %s, err: %s", id, cmn.ChecksumMD5, errstr)
			failed = true
			return
		}
		if hdhash != md5hash {
			t.Errorf("%2d: header's %s %s doesn't match object's %s", id, cmn.ChecksumMD5, hdhash, md5hash)
			failed = true
			return
		}
		tutils.Logf("%2d: header's %s %s matches object's %s\n", id, cmn.ChecksumMD5, hdhash, md5hash)
	} else {
		written, err = cmn.ReceiveAndChecksum(file, resp.Body, nil)
		if err != nil {
			t.Errorf("%2d: failed to write object, err: %v", id, err)
			failed = true
			return
		}
	}
	return
}

func deleteFiles(proxyURL string, keynames <-chan string, wg *sync.WaitGroup, errCh chan error, bucket string) {
	defer wg.Done()
	dwg := &sync.WaitGroup{}
	for keyname := range keynames {
		dwg.Add(1)
		go tutils.Del(proxyURL, bucket, keyname, "", dwg, errCh, true)
	}
	dwg.Wait()
}

func getMatchingKeys(proxyURL string, regexmatch, bucket string, keynameChans []chan string, outputChan chan string, t *testing.T) int {
	var msg = &cmn.SelectMsg{PageSize: int(pagesize)}
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
		num++
		if num >= numfiles {
			break
		}
	}

	return num
}

func testListBucket(t *testing.T, proxyURL, bucket string, msg *cmn.SelectMsg, limit int) *cmn.BucketList {
	tutils.Logf("LIST bucket %s [fast: %v, prefix: %q, page_size: %d, marker: %q]\n", bucket, msg.Fast, msg.Prefix, msg.PageSize, msg.PageMarker)
	baseParams := tutils.BaseAPIParams(proxyURL)
	resList, err := api.ListBucket(baseParams, bucket, msg, limit)
	if err != nil {
		t.Errorf("List bucket %s failed, err = %v", bucket, err)
		return nil
	}

	return resList
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
		filesList   = make([]string, 0, numFiles)
		proxyURL    = getPrimaryURL(t, proxyURLReadOnly)
		tMock       = cluster.NewTargetMock(cluster.NewBaseBownerMock(TestBucketName))
	)

	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skip(fmt.Sprintf("test %q requires a cloud bucket", t.Name()))
	}

	if containers.DockerRunning() {
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

	config := getClusterConfig(t, proxyURL)
	oldWarmGet := config.Cksum.ValidateWarmGet
	oldChecksum := config.Cksum.Type
	if !oldWarmGet {
		setClusterConfig(t, proxyURL, cmn.SimpleKVs{"cksum.validate_warm_get": "true"})
		if t.Failed() {
			goto cleanup
		}
	}

	filepath.Walk(rootDir, fsWalkFunc)
	tutils.CheckPathExists(t, fqn, false /*dir*/)
	oldFileInfo, _ = os.Stat(fqn)

	// Test when the contents of the file are changed
	tutils.Logf("\nChanging contents of the file [%s]: %s\n", fileName, fqn)
	err = ioutil.WriteFile(fqn, []byte("Contents of this file have been changed."), 0644)
	tassert.CheckFatal(t, err)
	validateGETUponFileChangeForChecksumValidation(t, proxyURL, fileName, fqn, oldFileInfo)

	// Test when the xxHash of the file is changed
	fileName = <-fileNameCh
	filesList = append(filesList, filepath.Join(ChecksumWarmValidateStr, fileName))
	filepath.Walk(rootDir, fsWalkFunc)
	tutils.CheckPathExists(t, fqn, false /*dir*/)
	oldFileInfo, _ = os.Stat(fqn)

	tutils.Logf("\nChanging file xattr[%s]: %s\n", fileName, fqn)
	err = tutils.SetXattrCksum(fqn, cmn.NewCksum(cmn.ChecksumXXHash, "01234"), tMock)
	tassert.CheckError(t, err)
	validateGETUponFileChangeForChecksumValidation(t, proxyURL, fileName, fqn, oldFileInfo)

	// Test for no checksum algo
	fileName = <-fileNameCh
	filesList = append(filesList, filepath.Join(ChecksumWarmValidateStr, fileName))
	filepath.Walk(rootDir, fsWalkFunc)
	setClusterConfig(t, proxyURL, cmn.SimpleKVs{"cksum.type": cmn.ChecksumNone})
	if t.Failed() {
		goto cleanup
	}
	tutils.Logf("\nChanging file xattr[%s]: %s\n", fileName, fqn)
	err = tutils.SetXattrCksum(fqn, cmn.NewCksum(cmn.ChecksumXXHash, "01234abcde"), tMock)
	tassert.CheckError(t, err)

	_, err = api.GetObject(tutils.DefaultBaseAPIParams(t), clibucket, path.Join(ChecksumWarmValidateStr, fileName))
	tassert.Errorf(t, err == nil, "A GET on an object when checksum algo is none should pass. Error: %v", err)

cleanup:
	// Restore old config
	setClusterConfig(t, proxyURL, cmn.SimpleKVs{
		"cksum.type":              oldChecksum,
		"cksum.validate_warm_get": fmt.Sprintf("%v", oldWarmGet),
	})

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
	var (
		err error

		numPuts    = 5
		filesPutCh = make(chan string, numPuts)
		filesList  = make([]string, 0, 100)
		errCh      = make(chan error, 100)
		wg         = &sync.WaitGroup{}
		bucket     = clibucket
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		baseParams = tutils.DefaultBaseAPIParams(t)
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
		// Cleanup
		for _, fn := range filesList {
			wg.Add(1)
			go tutils.Del(proxyURL, bucket, fn, "", wg, errCh, !testing.Verbose())
		}
		wg.Wait()
		selectErr(errCh, "delete", t, false)
		close(errCh)

		resetBucketProps(proxyURL, clibucket, t)
	}()

	sgl := tutils.Mem2.NewSGL(largeFileSize)
	defer sgl.Free()
	tutils.PutRandObjs(proxyURL, bucket, ldir, readerType, EvictCBStr, largeFileSize, numPuts, errCh, filesPutCh, sgl)
	selectErr(errCh, "put", t, false)
	close(filesPutCh) // to exit for-range
	for fname := range filesPutCh {
		filesList = append(filesList, filepath.Join(EvictCBStr, fname))
	}
	getFromObjList(proxyURL, bucket, errCh, filesList, false)
	for _, fname := range filesList {
		if b, _ := tutils.CheckExists(proxyURL, bucket, fname); !b {
			t.Fatalf("Object not cached: %s", fname)
		}
	}

	// Test property, mirror is disabled for cloud bucket that hasn't been accessed,
	// even if system config says otherwise
	err = api.SetBucketProps(baseParams, bucket, cmn.BucketPropsToUpdate{
		Mirror: &cmn.MirrorConfToUpdate{Enabled: api.Bool(true)},
	})
	tassert.CheckFatal(t, err)
	bProps, err := api.HeadBucket(baseParams, bucket)
	tassert.CheckFatal(t, err)
	if !bProps.Mirror.Enabled {
		t.Fatalf("Test property hasn't changed")
	}
	err = api.EvictCloudBucket(baseParams, bucket)
	tassert.CheckFatal(t, err)

	for _, fname := range filesList {
		if b, _ := tutils.CheckExists(proxyURL, bucket, fname); b {
			t.Errorf("%s remains cached", fname)
		}
	}
	bProps, err = api.HeadBucket(baseParams, bucket)
	tassert.CheckFatal(t, err)
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
	tutils.CheckPathExists(t, fqn, false /*dir*/)
	newFileInfo, _ := os.Stat(fqn)
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
func TestChecksumValidateOnWarmGetForBucket(t *testing.T) {
	const fileSize = 1024
	var (
		numFiles   = 3
		fileNameCh = make(chan string, numFiles)
		errCh      = make(chan error, 100)
		bucketName = TestBucketName
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		fqn        string
		err        error
		tMock      = cluster.NewTargetMock(cluster.NewBaseBownerMock(TestBucketName))
	)

	if containers.DockerRunning() {
		t.Skip(fmt.Sprintf("test %q requires Xattributes to be set, doesn't work with docker", t.Name()))
	}

	tutils.CreateFreshBucket(t, proxyURL, bucketName)
	sgl := tutils.Mem2.NewSGL(fileSize)
	defer sgl.Free()
	tutils.PutRandObjs(proxyURL, bucketName, ChecksumWarmValidateDir, readerType, ChecksumWarmValidateStr, fileSize, numFiles, errCh, fileNameCh, sgl)
	selectErr(errCh, "put", t, false)

	// Get Current Config
	config := getClusterConfig(t, proxyURL)
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
		setClusterConfig(t, proxyURL, cmn.SimpleKVs{"cksum.validate_warm_get": "true"})
		if t.Failed() {
			goto cleanup
		}
	}

	// Test changing the file content
	fileName = <-fileNameCh
	filepath.Walk(rootDir, fsWalkFunc)
	tutils.Logf("Changing contents of the file [%s]: %s\n", fileName, fqn)
	err = ioutil.WriteFile(fqn, []byte("Contents of this file have been changed."), 0644)
	tassert.CheckFatal(t, err)
	executeTwoGETsForChecksumValidation(proxyURL, bucketName, fileName, t)

	// Test changing the file xattr
	fileName = <-fileNameCh
	filepath.Walk(rootDir, fsWalkFunc)
	tutils.Logf("Changing file xattr[%s]: %s\n", fileName, fqn)
	err = tutils.SetXattrCksum(fqn, cmn.NewCksum(cmn.ChecksumXXHash, "01234abcde"), tMock)
	tassert.CheckError(t, err)
	executeTwoGETsForChecksumValidation(proxyURL, bucketName, fileName, t)

	// Test for none checksum algo
	fileName = <-fileNameCh
	filepath.Walk(rootDir, fsWalkFunc)
	setClusterConfig(t, proxyURL, cmn.SimpleKVs{"cksum.type": cmn.ChecksumNone})
	if t.Failed() {
		goto cleanup
	}
	tutils.Logf("Changing file xattr[%s]: %s\n", fileName, fqn)
	err = tutils.SetXattrCksum(fqn, cmn.NewCksum(cmn.ChecksumXXHash, "01234abcde"), tMock)
	tassert.CheckError(t, err)
	_, err = api.GetObject(tutils.DefaultBaseAPIParams(t), bucketName, path.Join(ChecksumWarmValidateStr, fileName))
	if err != nil {
		t.Error("A GET on an object when checksum algo is none should pass")
	}

cleanup:
	// Restore old config
	tutils.DestroyBucket(t, proxyURL, bucketName)
	setClusterConfig(t, proxyURL, cmn.SimpleKVs{
		"cksum.type":              oldChecksum,
		"cksum.validate_warm_get": fmt.Sprintf("%v", oldWarmGet),
	})
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
	created := createBucketIfNotExists(t, proxyURL, clibucket)
	tutils.PutRandObjs(proxyURL, bucketName, RangeGetDir, readerType, RangeGetStr, fileSize, numFiles, errCh, fileNameCh, sgl, true)
	selectErr(errCh, "put", t, false)

	// Get Current Config
	config := getClusterConfig(t, proxyURL)
	oldEnableReadRangeChecksum := config.Cksum.EnableReadRange

	fileName = <-fileNameCh
	tutils.Logln("Testing valid cases.")
	// Validate entire object checksum is being returned
	if oldEnableReadRangeChecksum {
		setClusterConfig(t, proxyURL, cmn.SimpleKVs{"cksum.enable_read_range": "false"})
		if t.Failed() {
			goto cleanup
		}
	}
	testValidCases(fileSize, t, proxyURL, bucketName, fileName, true, RangeGetStr)

	// Validate only range checksum is being returned
	if !oldEnableReadRangeChecksum {
		setClusterConfig(t, proxyURL, cmn.SimpleKVs{"cksum.enable_read_range": "true"})
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
	setClusterConfig(t, proxyURL, cmn.SimpleKVs{"cksum.enable_read_range": fmt.Sprintf("%v", oldEnableReadRangeChecksum)})
	close(errCh)
	close(fileNameCh)

	if created {
		tutils.DestroyBucket(t, proxyURL, clibucket)
	}
}

func testValidCases(fileSize uint64, t *testing.T, proxyURL, bucketName string, fileName string, checkEntireObjCkSum bool, checkDir string) {
	// Read the entire file range by range
	// Read in ranges of 500 to test covered, partially covered and completely
	// uncovered ranges
	byteRange := int64(500)
	iterations := int64(fileSize) / byteRange
	for i := int64(0); i < iterations; i += byteRange {
		verifyValidRanges(t, proxyURL, bucketName, fileName, i, byteRange, byteRange, checkEntireObjCkSum, checkDir)
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
				hash, err := cmn.ComputeXXHash(file, nil)
				if err != nil {
					t.Errorf("Unable to compute cksum of file: %s. Error:  %s", fqn, err)
				}
				if hash != ckErr.Expected() {
					t.Errorf("Expected entire object checksum [%s], checksum returned in response [%s]",
						ckErr.Expected(), hash)
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
		t.Errorf("Returned bytes don't match expected length. Expected length: [%d]. Output length: [%d]", expectedLength, len(outputBytes))
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
		t.Skip(tutils.SkipMsg)
	}

	var (
		numPuts     = 5
		filesPutCh  = make(chan string, numPuts)
		filesList   = make([]string, 0, numPuts)
		errCh       = make(chan error, numPuts*2)
		bucket      = clibucket
		start, curr time.Time
		duration    time.Duration
		totalSize   = int64(numPuts * largeFileSize)
		proxyURL    = getPrimaryURL(t, proxyURLReadOnly)
	)

	if !isCloudBucket(t, proxyURL, clibucket) {
		t.Skipf("%s requires a cloud bucket", t.Name())
	}
	ldir := filepath.Join(LocalSrcDir, ChksumValidStr)
	if err := cmn.CreateDir(ldir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", ldir, err)
	}

	// Get Current Config
	config := getClusterConfig(t, proxyURL)
	ocoldget := config.Cksum.ValidateColdGet
	ochksum := config.Cksum.Type

	sgl := tutils.Mem2.NewSGL(largeFileSize)
	defer sgl.Free()
	tutils.PutRandObjs(proxyURL, bucket, ldir, readerType, ChksumValidStr, largeFileSize, numPuts, errCh, filesPutCh, sgl)
	selectErr(errCh, "put", t, false)
	close(filesPutCh) // to exit for-range
	for fname := range filesPutCh {
		if fname != "" {
			filesList = append(filesList, filepath.Join(ChksumValidStr, fname))
		}
	}
	// Delete it from cache.
	tutils.EvictObjects(t, proxyURL, filesList, clibucket)
	// Disable checkum
	if ochksum != cmn.ChecksumNone {
		setClusterConfig(t, proxyURL, cmn.SimpleKVs{"cksum.type": cmn.ChecksumNone})
	}
	if t.Failed() {
		goto cleanup
	}
	// Disable Cold Get Validation
	if ocoldget {
		setClusterConfig(t, proxyURL, cmn.SimpleKVs{"cksum.validate_cold_get": "false"})
	}
	if t.Failed() {
		goto cleanup
	}
	start = time.Now()
	getFromObjList(proxyURL, bucket, errCh, filesList, false)
	curr = time.Now()
	duration = curr.Sub(start)
	if t.Failed() {
		goto cleanup
	}
	tutils.Logf("GET %s without any checksum validation: %v\n", cmn.B2S(totalSize, 0), duration)
	selectErr(errCh, "get", t, false)
	tutils.EvictObjects(t, proxyURL, filesList, clibucket)
	switch clichecksum {
	case "all":
		setClusterConfig(t, proxyURL, cmn.SimpleKVs{
			"cksum.type":              cmn.ChecksumXXHash,
			"cksum.validate_cold_get": "true",
		})
		if t.Failed() {
			goto cleanup
		}
	case cmn.ChecksumXXHash:
		setClusterConfig(t, proxyURL, cmn.SimpleKVs{"cksum.type": cmn.ChecksumXXHash})
		if t.Failed() {
			goto cleanup
		}
	case ColdMD5str:
		setClusterConfig(t, proxyURL, cmn.SimpleKVs{"cksum.validate_cold_get": "true"})
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
	getFromObjList(proxyURL, bucket, errCh, filesList, true)
	curr = time.Now()
	duration = curr.Sub(start)
	tutils.Logf("GET %s and validate checksum (%s): %v\n", cmn.B2S(totalSize, 0), clichecksum, duration)
	selectErr(errCh, "get", t, false)
cleanup:
	deleteFromFileList(proxyURL, bucket, errCh, filesList)
	selectErr(errCh, "delete", t, false)
	close(errCh)
	// restore old config
	setClusterConfig(t, proxyURL, cmn.SimpleKVs{
		"cksum.type":              ochksum,
		"cksum.validate_cold_get": fmt.Sprintf("%v", ocoldget),
	})
}

// deleteFromFileList requires that errCh be twice the size of len(filesList) as each
// file can produce upwards of two errors.
func deleteFromFileList(proxyURL, bucket string, errCh chan error, filesList []string) {
	wg := &sync.WaitGroup{}
	// Delete local file and objects from bucket
	for _, fn := range filesList {
		wg.Add(1)
		go tutils.Del(proxyURL, bucket, fn, "", wg, errCh, true)
	}

	wg.Wait()
}

func getFromObjList(proxyURL, bucket string, errCh chan error, filesList []string, validate bool) {
	getsGroup := &sync.WaitGroup{}
	baseParams := tutils.BaseAPIParams(proxyURL)
	for i := 0; i < len(filesList); i++ {
		if filesList[i] != "" {
			getsGroup.Add(1)
			go func(i int) {
				var err error
				if validate {
					_, err = api.GetObjectWithValidation(baseParams, bucket, filesList[i])
				} else {
					_, err = api.GetObject(baseParams, bucket, filesList[i])
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

func createBucketIfNotExists(t *testing.T, proxyURL, bucket string) (created bool) {
	baseParams := tutils.BaseAPIParams(proxyURL)
	buckets, err := api.GetBucketNames(baseParams, "")
	if err != nil {
		t.Fatalf("Failed to read bucket list: %v", err)
	}

	if cmn.StringInSlice(bucket, buckets.AIS) || cmn.StringInSlice(bucket, buckets.Cloud) {
		return false
	}

	err = api.CreateBucket(baseParams, bucket)
	if err != nil {
		t.Fatalf("Failed to create ais bucket %s: %v", bucket, err)
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
	primary, err := tutils.GetPrimaryProxy(proxyURL)
	if err != nil {
		t.Fatalf("Failed to get primary proxy URL: %v", err)
	}
	return primary.URL(cmn.NetworkPublic)
}

func validateBucketProps(t *testing.T, expected cmn.BucketPropsToUpdate, actual cmn.BucketProps) {
	// Apply changes on props that we have received. If after applying anything
	// has changed it means that the props were not applied.
	tmpProps := *actual.Clone()
	tmpProps.Apply(expected)
	if !reflect.DeepEqual(tmpProps, actual) {
		t.Errorf("bucket props are not equal, expected: %+v, got: %+v", tmpProps, actual)
	}
}

func resetBucketProps(proxyURL, bucket string, t *testing.T) {
	baseParams := tutils.BaseAPIParams(proxyURL)
	if err := api.ResetBucketProps(baseParams, bucket); err != nil {
		t.Errorf("bucket: %s props not reset, err: %v", clibucket, err)
	}
}
