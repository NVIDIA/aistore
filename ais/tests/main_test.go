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
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/containers"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
	jsoniter "github.com/json-iterator/go"
)

// worker's result
type workres struct {
	totfiles int
	totbytes int64
}

func Test_download(t *testing.T) {
	var (
		bck = api.Bck{
			Name:     clibucket,
			Provider: cmn.Cloud,
		}
		proxyURL = tutils.GetPrimaryURL()
	)

	if !isCloudBucket(t, proxyURL, bck) {
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
		go getAndCopyTmp(t, proxyURL, bck, i, keynameChans[i], wg, errCh, resultChans[i])
	}

	num := getMatchingKeys(t, proxyURL, bck, match, keynameChans, filesCreated)

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
	var (
		bck = api.Bck{
			Name:     clibucket,
			Provider: cmn.Cloud,
		}
		proxyURL = tutils.GetPrimaryURL()
	)

	if created := createBucketIfNotExists(t, proxyURL, bck); created {
		defer tutils.DestroyBucket(t, proxyURL, bck)
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
		go deleteFiles(proxyURL, bck, keynameChans[i], wg, errCh)
	}

	// list the bucket
	var msg = &cmn.SelectMsg{PageSize: int(pagesize)}
	baseParams := tutils.BaseAPIParams(proxyURL)
	reslist, err := api.ListBucket(baseParams, bck, msg, 0)
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
	var (
		bck      = api.Bck{Name: clibucket}
		proxyURL = tutils.GetPrimaryURL()
	)

	if numfiles < 10 || numfiles%10 != 0 {
		t.Skip("numfiles must be a positive multiple of 10")
	}

	if testing.Short() && isCloudBucket(t, proxyURL, bck) {
		t.Skipf("don't run when short mode and cloud bucket")
	}

	const (
		commonPrefix = "tst" // object full name: <bucket>/<commonPrefix>/<generated_name:a-####|b-####>
		objSize      = 16 * 1024
	)

	if err := cmn.CreateDir(DeleteDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", DeleteDir, err)
	}
	if created := createBucketIfNotExists(t, proxyURL, bck); created {
		defer tutils.DestroyBucket(t, proxyURL, bck)
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
	tutils.PutObjsFromList(proxyURL, bck, DeleteDir, readerType, commonPrefix, objSize, objList, errCh, objsPutCh, sgl)
	tassert.SelectErr(t, errCh, "put", true /* fatal - if PUT does not work then it makes no sense to continue */)
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
		tutils.Logf("%d. %s\n    Prefix: [%s], range: [%s], regexp: [%s]\n",
			idx+1, test.name, test.prefix, test.rangeStr, test.regexStr)

		err := api.DeleteRange(baseParams, bck, test.prefix, test.regexStr, test.rangeStr, true, 0)
		if err != nil {
			t.Error(err)
			continue
		}

		totalFiles -= test.delta
		bktlst, err := api.ListBucket(baseParams, bck, msg, 0)
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
	bckList, err := api.ListBucket(baseParams, bck, msg, 0)
	if err != nil {
		t.Errorf("Failed to get the list of remained files, err: %v\n", err)
	}
	// cleanup everything at the end
	// Declare one channel per worker to pass the keyname
	nameChans := make([]chan string, numworkers)
	for i := 0; i < numworkers; i++ {
		// Allow a bunch of messages at a time to be written asynchronously to a channel
		nameChans[i] = make(chan string, 100)
	}

	// Start the worker pools
	var wg = &sync.WaitGroup{}
	// Get the workers started
	for i := 0; i < numworkers; i++ {
		wg.Add(1)
		go deleteFiles(proxyURL, bck, nameChans[i], wg, errCh)
	}

	num := 0
	for _, entry := range bckList.Entries {
		nameChans[num%numworkers] <- entry.Name
		num++
	}

	// Close the channels after the reading is done
	for i := 0; i < numworkers; i++ {
		close(nameChans[i])
	}

	wg.Wait()
	tassert.SelectErr(t, errCh, "delete", false)
}

// PUT, then delete
func Test_putdelete(t *testing.T) {
	const fileSize = 512 * 1024

	var (
		proxyURL = tutils.GetPrimaryURL()
		bck      = api.Bck{Name: clibucket}
	)

	if testing.Short() && isCloudBucket(t, proxyURL, bck) {
		t.Skipf("don't run when short mode and cloud bucket")
	}

	if err := cmn.CreateDir(DeleteDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", DeleteDir, err)
	}

	var (
		errCh      = make(chan error, numfiles)
		filesPutCh = make(chan string, numfiles)
		sgl        = tutils.Mem2.NewSGL(fileSize)
	)

	if created := createBucketIfNotExists(t, proxyURL, bck); created {
		defer tutils.DestroyBucket(t, proxyURL, bck)
	}

	defer sgl.Free()
	tutils.PutRandObjs(proxyURL, bck, DeleteDir, readerType, DeleteStr, fileSize, numfiles, errCh, filesPutCh, sgl)
	close(filesPutCh)
	tassert.SelectErr(t, errCh, "put", true)

	// Declare one channel per worker to pass the keyname
	nameChans := make([]chan string, numworkers)
	for i := 0; i < numworkers; i++ {
		// Allow a bunch of messages at a time to be written asynchronously to a channel
		nameChans[i] = make(chan string, 100)
	}

	// Start the worker pools
	var wg = &sync.WaitGroup{}
	// Get the workers started
	for i := 0; i < numworkers; i++ {
		wg.Add(1)
		go deleteFiles(proxyURL, bck, nameChans[i], wg, errCh)
	}

	num := 0
	for name := range filesPutCh {
		nameChans[num%numworkers] <- filepath.Join(DeleteStr, name)
		num++
	}

	// Close the channels after the reading is done
	for i := 0; i < numworkers; i++ {
		close(nameChans[i])
	}

	wg.Wait()
	tassert.SelectErr(t, errCh, "delete", false)
}

func listObjects(t *testing.T, proxyURL string, bck api.Bck, msg *cmn.SelectMsg, objLimit int) (*cmn.BucketList, error) {
	resList := testListBucket(t, proxyURL, bck, msg, objLimit)
	if resList == nil {
		return nil, fmt.Errorf("failed to list bucket %s", bck)
	}
	for _, m := range resList.Entries {
		if len(m.Checksum) > 8 {
			tutils.Logf("%s %d [%s] %s [%v - %s]\n",
				m.Name, m.Size, m.Version, m.Checksum[:8]+"...", m.CheckExists, m.Atime)
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
		bck = api.Bck{
			Name:     t.Name() + "Bucket",
			Provider: cmn.ProviderAIS,
		}
		proxyURL   = tutils.GetPrimaryURL()
		baseParams = tutils.DefaultBaseAPIParams(t)
	)
	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	buckets, err := api.GetBucketNames(baseParams, api.Bck{})
	tassert.CheckFatal(t, err)

	tutils.Logf("ais bucket names:\n")
	printBucketNames(t, buckets.AIS)
	tutils.Logf("cloud bucket names:\n")
	printBucketNames(t, buckets.Cloud)

	for _, provider := range []string{cmn.ProviderAmazon, cmn.ProviderGoogle} {
		cloudBuckets, err := api.GetBucketNames(baseParams, api.Bck{Provider: provider})
		tassert.CheckError(t, err)
		if len(cloudBuckets.Cloud) != len(buckets.Cloud) {
			t.Fatalf("cloud buckets: %d != %d\n", len(cloudBuckets.Cloud), len(buckets.Cloud))
		}
		if len(cloudBuckets.AIS) != 0 {
			t.Fatalf("cloud buckets contain ais: %+v\n", cloudBuckets.AIS)
		}
	}
	aisBuckets, err := api.GetBucketNames(baseParams, api.Bck{Provider: cmn.ProviderAIS})
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
		proxyURL   = tutils.GetPrimaryURL()
		baseParams = tutils.DefaultBaseAPIParams(t)
		bckLocal   = api.Bck{
			Name:     clibucket,
			Provider: cmn.ProviderAIS,
		}
		bckCloud = api.Bck{
			Name:     clibucket,
			Provider: cmn.Cloud,
		}
		fileName1 = "mytestobj1.txt"
		fileName2 = "mytestobj2.txt"
		dataLocal = []byte("im local")
		dataCloud = []byte("I'm from the cloud!")
		files     = []string{fileName1, fileName2}
	)

	if !isCloudBucket(t, proxyURL, bckCloud) {
		t.Skipf("%s requires a cloud bucket", t.Name())
	}

	putArgsLocal := api.PutObjectArgs{
		BaseParams: baseParams,
		Bck:        bckLocal,
		Object:     fileName1,
		Reader:     tutils.NewBytesReader(dataLocal),
	}

	putArgsCloud := api.PutObjectArgs{
		BaseParams: baseParams,
		Bck:        bckCloud,
		Object:     fileName1,
		Reader:     tutils.NewBytesReader(dataCloud),
	}

	// PUT/GET/DEL Without ais bucket
	tutils.Logf("Validating responses for non-existent ais bucket...\n")
	err := api.PutObject(putArgsLocal)
	if err == nil {
		t.Fatalf("ais bucket %s does not exist: Expected an error.", bckLocal)
	}

	_, err = api.GetObject(baseParams, bckLocal, fileName1)
	if err == nil {
		t.Fatalf("ais bucket %s does not exist: Expected an error.", bckLocal)
	}

	err = api.DeleteObject(baseParams, bckLocal, fileName1)
	if err == nil {
		t.Fatalf("ais bucket %s does not exist: Expected an error.", bckLocal)
	}

	tutils.Logf("PrefetchList %d\n", len(files))
	err = api.PrefetchList(baseParams, bckCloud, files, true, 0)
	if err != nil {
		t.Fatalf("%v\n", err)
	}
	tutils.Logf("PrefetchRange\n")
	err = api.PrefetchRange(baseParams, bckCloud, "r", "", prefetchRange, true, 0)
	if err != nil {
		t.Fatalf("%v\n", err)
	}
	tutils.Logf("EvictList\n")
	err = api.EvictList(baseParams, bckCloud, files, true, 0)
	if err != nil {
		t.Fatalf("%v\n", err)
	}
	tutils.Logf("EvictRange\n")
	err = api.EvictRange(baseParams, bckCloud, "", "", prefetchRange, true, 0)
	if err != nil {
		t.Fatalf("%v\n", err)
	}

	tutils.CreateFreshBucket(t, proxyURL, bckLocal)
	defer tutils.DestroyBucket(t, proxyURL, bckLocal)

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
	_, err = api.HeadObject(baseParams, bckLocal, fileName1)
	tassert.CheckFatal(t, err)
	_, err = api.HeadObject(baseParams, bckLocal, fileName2)
	tassert.CheckFatal(t, err)

	// Prefetch/Evict should work
	err = api.PrefetchList(baseParams, bckCloud, files, true, 0)
	tassert.CheckFatal(t, err)
	err = api.EvictList(baseParams, bckCloud, files, true, 0)
	tassert.CheckFatal(t, err)

	// Deleting from cloud bucket
	tutils.Logf("Deleting %s and %s from cloud bucket ...\n", fileName1, fileName2)
	api.DeleteList(baseParams, bckCloud, files, true, 0)

	// Deleting from ais bucket
	tutils.Logf("Deleting %s and %s from ais bucket ...\n", fileName1, fileName2)
	api.DeleteList(baseParams, bckLocal, files, true, 0)

	_, err = api.HeadObject(baseParams, bckLocal, fileName1)
	if !strings.Contains(err.Error(), strconv.Itoa(http.StatusNotFound)) {
		t.Errorf("Local file %s not deleted", fileName1)
	}
	_, err = api.HeadObject(baseParams, bckLocal, fileName2)
	if !strings.Contains(err.Error(), strconv.Itoa(http.StatusNotFound)) {
		t.Errorf("Local file %s not deleted", fileName2)
	}

	_, err = api.HeadObject(baseParams, bckCloud, fileName1)
	if !strings.Contains(err.Error(), strconv.Itoa(http.StatusNotFound)) {
		t.Errorf("Cloud file %s not deleted", fileName1)
	}
	_, err = api.HeadObject(baseParams, bckCloud, fileName2)
	if !strings.Contains(err.Error(), strconv.Itoa(http.StatusNotFound)) {
		t.Errorf("Cloud file %s not deleted", fileName2)
	}
}

func Test_SameAISAndCloudBucketName(t *testing.T) {
	var (
		defLocalProps cmn.BucketPropsToUpdate
		defCloudProps cmn.BucketPropsToUpdate

		bckLocal = api.Bck{
			Name:     clibucket,
			Provider: cmn.ProviderAIS,
		}
		bckCloud = api.Bck{
			Name:     clibucket,
			Provider: cmn.Cloud,
		}
		proxyURL   = tutils.GetPrimaryURL()
		fileName   = "mytestobj1.txt"
		baseParams = tutils.DefaultBaseAPIParams(t)
		dataLocal  = []byte("im local")
		dataCloud  = []byte("I'm from the cloud!")
		msg        = &cmn.SelectMsg{PageSize: int(pagesize), Props: "size,status"}
		found      = false
	)

	if !isCloudBucket(t, proxyURL, bckCloud) {
		t.Skipf("%s requires a cloud bucket", t.Name())
	}

	tutils.CreateFreshBucket(t, proxyURL, bckLocal)
	defer tutils.DestroyBucket(t, proxyURL, bckLocal)

	bucketPropsLocal := cmn.BucketPropsToUpdate{
		Cksum: &cmn.CksumConfToUpdate{
			Type: api.String(cmn.ChecksumNone),
		},
	}
	bucketPropsCloud := cmn.BucketPropsToUpdate{}

	// Put
	tutils.Logf("Putting object (%s) into ais bucket %s...\n", fileName, bckLocal)
	putArgs := api.PutObjectArgs{
		BaseParams: baseParams,
		Bck:        bckLocal,
		Object:     fileName,
		Reader:     tutils.NewBytesReader(dataLocal),
	}
	err := api.PutObject(putArgs)
	tassert.CheckFatal(t, err)

	resLocal, err := api.ListBucket(baseParams, bckLocal, msg, 0)
	tassert.CheckFatal(t, err)

	tutils.Logf("Putting object (%s) into cloud bucket %s...\n", fileName, bckLocal)
	putArgs = api.PutObjectArgs{
		BaseParams: baseParams,
		Bck:        bckCloud,
		Object:     fileName,
		Reader:     tutils.NewBytesReader(dataCloud),
	}
	err = api.PutObject(putArgs)
	tassert.CheckFatal(t, err)

	resCloud, err := api.ListBucket(baseParams, bckCloud, msg, 0)
	tassert.CheckFatal(t, err)

	if len(resLocal.Entries) != 1 {
		t.Fatalf("Expected number of files in ais bucket (%s) does not match: expected %v, got %v",
			bckCloud, 1, len(resLocal.Entries))
	}

	for _, entry := range resCloud.Entries {
		if entry.Name == fileName {
			found = true
			break
		}
	}

	if !found {
		t.Fatalf("File (%s) not found in cloud bucket (%s)", fileName, bckCloud)
	}

	// Get
	lenLocal, err := api.GetObject(baseParams, bckLocal, fileName)
	tassert.CheckFatal(t, err)
	lenCloud, err := api.GetObject(baseParams, bckCloud, fileName)
	tassert.CheckFatal(t, err)

	if lenLocal == lenCloud {
		t.Errorf("Local file and cloud file have same size, expected: local (%v) cloud (%v) got: local (%v) cloud (%v)",
			len(dataLocal), len(dataCloud), lenLocal, lenCloud)
	}

	// Delete
	err = api.DeleteObject(baseParams, bckCloud, fileName)
	tassert.CheckFatal(t, err)

	lenLocal, err = api.GetObject(baseParams, bckLocal, fileName)
	tassert.CheckFatal(t, err)

	// Check that local object still exists
	if lenLocal != int64(len(dataLocal)) {
		t.Errorf("Local file %s deleted", fileName)
	}

	// Check that cloud object is deleted using HeadObject
	_, err = api.HeadObject(baseParams, bckCloud, fileName)
	if !strings.Contains(err.Error(), strconv.Itoa(http.StatusNotFound)) {
		t.Errorf("Cloud file %s not deleted", fileName)
	}

	// Set Props Object
	err = api.SetBucketProps(baseParams, bckLocal, bucketPropsLocal)
	tassert.CheckFatal(t, err)

	err = api.SetBucketProps(baseParams, bckCloud, bucketPropsCloud)
	tassert.CheckFatal(t, err)

	// Validate ais bucket props are set
	localProps, err := api.HeadBucket(baseParams, bckLocal)
	tassert.CheckFatal(t, err)
	validateBucketProps(t, bucketPropsLocal, localProps)

	// Validate cloud bucket props are set
	cloudProps, err := api.HeadBucket(baseParams, bckCloud)
	tassert.CheckFatal(t, err)
	validateBucketProps(t, bucketPropsCloud, cloudProps)

	// Reset ais bucket props and validate they are reset
	err = api.ResetBucketProps(baseParams, bckLocal)
	tassert.CheckFatal(t, err)
	localProps, err = api.HeadBucket(baseParams, bckLocal)
	tassert.CheckFatal(t, err)
	validateBucketProps(t, defLocalProps, localProps)

	// Check if cloud bucket props remain the same
	cloudProps, err = api.HeadBucket(baseParams, bckCloud)
	tassert.CheckFatal(t, err)
	validateBucketProps(t, bucketPropsCloud, cloudProps)

	// Reset cloud bucket props
	err = api.ResetBucketProps(baseParams, bckCloud)
	tassert.CheckFatal(t, err)
	cloudProps, err = api.HeadBucket(baseParams, bckCloud)
	tassert.CheckFatal(t, err)
	validateBucketProps(t, defCloudProps, cloudProps)

	// Check if ais bucket props remain the same
	localProps, err = api.HeadBucket(baseParams, bckLocal)
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
		bck        = api.Bck{
			Name:     clibucket,
			Provider: cmn.Cloud,
		}
		totalSize = int64(numPuts * largeFileSize)
		proxyURL  = tutils.GetPrimaryURL()
	)

	if !isCloudBucket(t, proxyURL, bck) {
		t.Skipf("%s requires a cloud bucket", t.Name())
	}

	ldir := filepath.Join(LocalSrcDir, ColdValidStr)
	if err := cmn.CreateDir(ldir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", ldir, err)
	}

	config := tutils.GetClusterConfig(t)
	bcoldget := config.Cksum.ValidateColdGet

	sgl := tutils.Mem2.NewSGL(largeFileSize)
	defer sgl.Free()
	tutils.PutRandObjs(proxyURL, bck, ldir, readerType, ColdValidStr, largeFileSize, numPuts, errCh, filesPutCh, sgl)
	tassert.SelectErr(t, errCh, "put", false)
	close(filesPutCh) // to exit for-range
	for fname := range filesPutCh {
		filesList = append(filesList, filepath.Join(ColdValidStr, fname))
	}
	tutils.EvictObjects(t, proxyURL, bck, filesList)
	// Disable Cold Get Validation
	if bcoldget {
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"cksum.validate_cold_get": "false"})
	}
	start := time.Now()
	getFromObjList(proxyURL, bck, errCh, filesList, false)
	curr := time.Now()
	duration := curr.Sub(start)
	if t.Failed() {
		goto cleanup
	}
	tutils.Logf("GET %s without MD5 validation: %v\n", cmn.B2S(totalSize, 0), duration)
	tassert.SelectErr(t, errCh, "get", false)
	tutils.EvictObjects(t, proxyURL, bck, filesList)
	// Enable Cold Get Validation
	tutils.SetClusterConfig(t, cmn.SimpleKVs{"cksum.validate_cold_get": "true"})
	if t.Failed() {
		goto cleanup
	}
	start = time.Now()
	getFromObjList(proxyURL, bck, errCh, filesList, true)
	curr = time.Now()
	duration = curr.Sub(start)
	tutils.Logf("GET %s with MD5 validation:    %v\n", cmn.B2S(totalSize, 0), duration)
	tassert.SelectErr(t, errCh, "get", false)
cleanup:
	tutils.SetClusterConfig(t, cmn.SimpleKVs{"cksum.validate_cold_get": fmt.Sprintf("%v", bcoldget)})
	for _, fn := range filesList {
		wg.Add(1)
		go tutils.Del(proxyURL, bck, fn, wg, errCh, !testing.Verbose())
	}
	wg.Wait()
	tassert.SelectErr(t, errCh, "delete", false)
	close(errCh)
}

func TestHeadBucket(t *testing.T) {
	var (
		proxyURL   = tutils.GetPrimaryURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		bck        = api.Bck{
			Name:     TestBucketName,
			Provider: cmn.ProviderAIS,
		}
	)

	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	bckPropsToUpdate := cmn.BucketPropsToUpdate{
		Cksum: &cmn.CksumConfToUpdate{
			ValidateWarmGet: api.Bool(true),
		},
		LRU: &cmn.LRUConfToUpdate{
			Enabled: api.Bool(true),
		},
	}
	err := api.SetBucketProps(baseParams, bck, bckPropsToUpdate)
	tassert.CheckFatal(t, err)

	p, err := api.HeadBucket(baseParams, bck)
	tassert.CheckFatal(t, err)

	validateBucketProps(t, bckPropsToUpdate, p)
}

func TestHeadCloudBucket(t *testing.T) {
	var (
		proxyURL   = tutils.GetPrimaryURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		bck        = api.Bck{
			Name:     clibucket,
			Provider: cmn.Cloud,
		}
	)

	if !isCloudBucket(t, proxyURL, bck) {
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
	err := api.SetBucketProps(baseParams, bck, bckPropsToUpdate)
	tassert.CheckFatal(t, err)
	defer resetBucketProps(proxyURL, bck, t)

	p, err := api.HeadBucket(baseParams, bck)
	tassert.CheckFatal(t, err)
	validateBucketProps(t, bckPropsToUpdate, p)
}

func TestHeadNonexistentBucket(t *testing.T) {
	var (
		proxyURL   = tutils.GetPrimaryURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	bucket, err := tutils.GenerateNonexistentBucketName("head", baseParams)
	tassert.CheckFatal(t, err)

	bck := api.Bck{
		Name:     bucket,
		Provider: cmn.ProviderAIS,
	}

	_, err = api.HeadBucket(baseParams, bck)
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
		proxyURL = tutils.GetPrimaryURL()
		objName  = "headobject_test_obj"
		objSize  = int64(1024)
		bck      = api.Bck{
			Name:     TestBucketName,
			Provider: cmn.ProviderAIS,
		}

		putTime time.Time
	)
	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	r, rrErr := tutils.NewRandReader(objSize, true)
	if rrErr != nil {
		t.Fatalf("tutils.NewRandReader failed, err = %v", rrErr)
	}
	defer r.Close()
	hash := r.XXHash()
	putArgs := api.PutObjectArgs{
		BaseParams: tutils.DefaultBaseAPIParams(t),
		Bck:        bck,
		Object:     objName,
		Hash:       hash,
		Reader:     r,
	}

	// Get the time in the same format as atime (with milliseconds truncated)
	putTime, _ = time.Parse(time.RFC822, time.Now().Format(time.RFC822))
	if err := api.PutObject(putArgs); err != nil {
		t.Fatalf("api.PutObject failed, err = %v", err)
	}

	propsExp := &cmn.ObjectProps{Size: objSize, Version: "1", NumCopies: 1, Checksum: hash,
		Present: true, Provider: cmn.ProviderAIS}
	props, err := api.HeadObject(tutils.DefaultBaseAPIParams(t), bck, objName)
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
		t.Errorf("Returned `Number` of copies not correct. Expected: %v, actual: %v",
			propsExp.NumCopies, props.NumCopies)
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
		t.Errorf("Returned `Atime` (%s) not correct - expected `atime` after `put` time (%s)",
			props.Atime, putTime.Format(time.RFC822))
	}
}

func TestHeadNonexistentObject(t *testing.T) {
	var (
		bck = api.Bck{
			Name:     TestBucketName,
			Provider: cmn.ProviderAIS,
		}
		objName  = "this_object_should_not_exist"
		proxyURL = tutils.GetPrimaryURL()
	)
	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	_, err := api.HeadObject(tutils.DefaultBaseAPIParams(t), bck, objName)
	if err == nil {
		t.Errorf("Expected non-nil error (404) from api.HeadObject, received nil error")
	}
}

func TestHeadObjectCheckExists(t *testing.T) {
	var (
		bck = api.Bck{
			Name: clibucket,
		}
		fileName = "headobject_check_cached_test_file"
		fileSize = 1024
		proxyURL = tutils.GetPrimaryURL()
	)

	if created := createBucketIfNotExists(t, proxyURL, bck); created {
		defer tutils.DestroyBucket(t, proxyURL, bck)
	}
	r, err := tutils.NewRandReader(int64(fileSize), false)
	if err != nil {
		t.Fatalf("tutils.NewRandReader failed, err = %v", err)
	}
	defer r.Close()
	putArgs := api.PutObjectArgs{
		BaseParams: tutils.DefaultBaseAPIParams(t),
		Bck:        bck,
		Object:     fileName,
		Hash:       r.XXHash(),
		Reader:     r,
	}
	err = api.PutObject(putArgs)
	tassert.CheckFatal(t, err)

	b, err := tutils.CheckExists(proxyURL, bck, fileName)
	tassert.CheckFatal(t, err)
	if !b {
		t.Error("Expected object to be cached, got false from tutils.CheckExists")
	}

	err = tutils.Del(proxyURL, bck, fileName, nil, nil, true)
	tassert.CheckFatal(t, err)

	b, err = tutils.CheckExists(proxyURL, bck, fileName)
	tassert.CheckFatal(t, err)
	if b {
		t.Error("Expected object to NOT be cached after deleting object, got true from tutils.CheckExists")
	}
}

func getAndCopyTmp(t *testing.T, proxyURL string, bck api.Bck, id int, keynames <-chan string, wg *sync.WaitGroup,
	errCh chan error, resch chan workres) {
	geturl := proxyURL + cmn.URLPath(cmn.Version, cmn.Objects)
	res := workres{0, 0}
	defer func() {
		close(resch)
		wg.Done()
	}()

	for keyname := range keynames {
		url := geturl + cmn.URLPath(bck.Name, keyname)
		written, failed := getAndCopyOne(t, url, bck, id, errCh, keyname)
		if failed {
			t.Fail()
			return
		}
		res.totfiles++
		res.totbytes += written
	}
	resch <- res
}

func getAndCopyOne(t *testing.T, url string, bck api.Bck, id int, errCh chan error,
	keyname string) (written int64, failed bool) {
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
		err = fmt.Errorf("%2d: Get %s from bucket %s http error %d", id, keyname, bck, resp.StatusCode)
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

func deleteFiles(proxyURL string, bck api.Bck, keynames <-chan string, wg *sync.WaitGroup, errCh chan error) {
	defer wg.Done()
	dwg := &sync.WaitGroup{}
	for keyname := range keynames {
		dwg.Add(1)
		go tutils.Del(proxyURL, bck, keyname, dwg, errCh, true)
	}
	dwg.Wait()
}

func getMatchingKeys(t *testing.T, proxyURL string, bck api.Bck, regexmatch string,
	keynameChans []chan string, outputChan chan string) int {
	var msg = &cmn.SelectMsg{PageSize: int(pagesize)}
	reslist := testListBucket(t, proxyURL, bck, msg, 0)
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

func testListBucket(t *testing.T, proxyURL string, bck api.Bck, msg *cmn.SelectMsg, limit int) *cmn.BucketList {
	tutils.Logf("LIST bucket %s [fast: %v, prefix: %q, page_size: %d, marker: %q]\n",
		bck, msg.Fast, msg.Prefix, msg.PageSize, msg.PageMarker)
	baseParams := tutils.BaseAPIParams(proxyURL)
	resList, err := api.ListBucket(baseParams, bck, msg, limit)
	if err != nil {
		t.Errorf("List bucket %s failed, err = %v", bck, err)
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
		proxyURL    = tutils.GetPrimaryURL()
		bmdMock     = cluster.NewBaseBownerMock()
		tMock       = cluster.NewTargetMock(bmdMock)
		bck         = api.Bck{
			Name:     clibucket,
			Provider: cmn.Cloud,
		}
	)
	bmdMock.Add(&cluster.Bck{Name: TestBucketName, Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal,
		Props: &cmn.BucketProps{Cksum: cmn.CksumConf{Type: cmn.ChecksumXXHash}}})

	if !isCloudBucket(t, proxyURL, bck) {
		t.Skip(fmt.Sprintf("test %q requires a cloud bucket", t.Name()))
	}

	if containers.DockerRunning() {
		t.Skip(fmt.Sprintf("test %q requires Xattributes to be set, doesn't work with docker", t.Name()))
	}
	sgl := tutils.Mem2.NewSGL(fileSize)
	defer sgl.Free()

	tutils.Logf("Creating %d objects\n", numFiles)
	tutils.PutRandObjs(proxyURL, bck, ChecksumWarmValidateDir, readerType, ChecksumWarmValidateStr, fileSize,
		numFiles, errCh, fileNameCh, sgl)
	tassert.SelectErr(t, errCh, "put", false)

	fileName = <-fileNameCh
	filesList = append(filesList, filepath.Join(ChecksumWarmValidateStr, fileName))
	// Fetch the file from cloud bucket.
	_, err := api.GetObjectWithValidation(tutils.DefaultBaseAPIParams(t), bck, path.Join(ChecksumWarmValidateStr, fileName))
	if err != nil {
		t.Errorf("Failed while fetching the file from the cloud bucket. Error: [%v]", err)
	}

	fsWalkFunc := func(path string, info os.FileInfo, err error) error {
		if filepath.Base(path) == fileName && strings.Contains(path, filepath.Join(bck.Name, ChecksumWarmValidateStr)) {
			fqn = path
		}
		return nil
	}

	config := tutils.GetClusterConfig(t)
	oldWarmGet := config.Cksum.ValidateWarmGet
	oldChecksum := config.Cksum.Type
	if !oldWarmGet {
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"cksum.validate_warm_get": "true"})
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
	tutils.SetClusterConfig(t, cmn.SimpleKVs{"cksum.type": cmn.ChecksumNone})
	if t.Failed() {
		goto cleanup
	}
	tutils.Logf("\nChanging file xattr[%s]: %s\n", fileName, fqn)
	err = tutils.SetXattrCksum(fqn, cmn.NewCksum(cmn.ChecksumXXHash, "01234abcde"), tMock)
	tassert.CheckError(t, err)

	_, err = api.GetObject(tutils.DefaultBaseAPIParams(t), bck, path.Join(ChecksumWarmValidateStr, fileName))
	tassert.Errorf(t, err == nil, "A GET on an object when checksum algo is none should pass. Error: %v", err)

cleanup:
	// Restore old config
	tutils.SetClusterConfig(t, cmn.SimpleKVs{
		"cksum.type":              oldChecksum,
		"cksum.validate_warm_get": fmt.Sprintf("%v", oldWarmGet),
	})

	wg := &sync.WaitGroup{}
	for _, fn := range filesList {
		wg.Add(1)
		go tutils.Del(proxyURL, bck, fn, wg, errCh, !testing.Verbose())
	}
	wg.Wait()
	tassert.SelectErr(t, errCh, "delete", false)
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
		bck        = api.Bck{
			Name:     clibucket,
			Provider: cmn.Cloud,
		}
		proxyURL   = tutils.GetPrimaryURL()
		baseParams = tutils.DefaultBaseAPIParams(t)
	)

	if !isCloudBucket(t, proxyURL, bck) {
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
			go tutils.Del(proxyURL, bck, fn, wg, errCh, !testing.Verbose())
		}
		wg.Wait()
		tassert.SelectErr(t, errCh, "delete", false)
		close(errCh)

		resetBucketProps(proxyURL, bck, t)
	}()

	sgl := tutils.Mem2.NewSGL(largeFileSize)
	defer sgl.Free()
	tutils.PutRandObjs(proxyURL, bck, ldir, readerType, EvictCBStr, largeFileSize, numPuts, errCh, filesPutCh, sgl)
	tassert.SelectErr(t, errCh, "put", false)
	close(filesPutCh) // to exit for-range
	for fname := range filesPutCh {
		filesList = append(filesList, filepath.Join(EvictCBStr, fname))
	}
	getFromObjList(proxyURL, bck, errCh, filesList, false)
	for _, fname := range filesList {
		if b, _ := tutils.CheckExists(proxyURL, bck, fname); !b {
			t.Fatalf("Object not cached: %s", fname)
		}
	}

	// Test property, mirror is disabled for cloud bucket that hasn't been accessed,
	// even if system config says otherwise
	err = api.SetBucketProps(baseParams, bck, cmn.BucketPropsToUpdate{
		Mirror: &cmn.MirrorConfToUpdate{Enabled: api.Bool(true)},
	})
	tassert.CheckFatal(t, err)
	bProps, err := api.HeadBucket(baseParams, bck)
	tassert.CheckFatal(t, err)
	if !bProps.Mirror.Enabled {
		t.Fatalf("Test property hasn't changed")
	}
	err = api.EvictCloudBucket(baseParams, bck)
	tassert.CheckFatal(t, err)

	for _, fname := range filesList {
		if b, _ := tutils.CheckExists(proxyURL, bck, fname); b {
			t.Errorf("%s remains cached", fname)
		}
	}
	bProps, err = api.HeadBucket(baseParams, bck)
	tassert.CheckFatal(t, err)
	if bProps.Mirror.Enabled {
		t.Fatalf("Test property not reset ")
	}
}

func validateGETUponFileChangeForChecksumValidation(t *testing.T, proxyURL, fileName string, fqn string,
	oldFileInfo os.FileInfo) {
	// Do a GET to see to check if a cold get was executed by comparing old and new size
	var (
		baseParams = tutils.BaseAPIParams(proxyURL)
		bck        = api.Bck{
			Name:     clibucket,
			Provider: cmn.Cloud,
		}
	)
	_, err := api.GetObjectWithValidation(baseParams, bck, path.Join(ChecksumWarmValidateStr, fileName))
	if err != nil {
		t.Errorf("Unable to GET file. Error: %v", err)
	}
	tutils.CheckPathExists(t, fqn, false /*dir*/)
	newFileInfo, _ := os.Stat(fqn)
	if newFileInfo.Size() != oldFileInfo.Size() {
		t.Errorf("Expected size: %d, Actual Size: %d", oldFileInfo.Size(), newFileInfo.Size())
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
		fqn string
		err error

		numFiles   = 3
		fileNameCh = make(chan string, numFiles)
		errCh      = make(chan error, 100)
		proxyURL   = tutils.GetPrimaryURL()
		bmdMock    = cluster.NewBaseBownerMock()
		tMock      = cluster.NewTargetMock(bmdMock)
		bck        = api.Bck{
			Name:     TestBucketName,
			Provider: cmn.ProviderAIS,
		}
	)
	bmdMock.Add(&cluster.Bck{Name: TestBucketName, Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal,
		Props: &cmn.BucketProps{Cksum: cmn.CksumConf{Type: cmn.ChecksumXXHash}}})

	if containers.DockerRunning() {
		t.Skip(fmt.Sprintf("test %q requires Xattributes to be set, doesn't work with docker", t.Name()))
	}

	tutils.CreateFreshBucket(t, proxyURL, bck)
	sgl := tutils.Mem2.NewSGL(fileSize)
	defer sgl.Free()
	tutils.PutRandObjs(proxyURL, bck, ChecksumWarmValidateDir, readerType, ChecksumWarmValidateStr,
		fileSize, numFiles, errCh, fileNameCh, sgl)
	tassert.SelectErr(t, errCh, "put", false)

	// Get Current Config
	config := tutils.GetClusterConfig(t)
	oldWarmGet := config.Cksum.ValidateWarmGet
	oldChecksum := config.Cksum.Type

	var fileName string
	fsWalkFunc := func(path string, info os.FileInfo, err error) error {
		if info == nil {
			return filepath.SkipDir
		}
		if filepath.Base(path) == fileName && strings.Contains(path, filepath.Join(bck.Name, ChecksumWarmValidateStr)) {
			fqn = path
		}
		return nil
	}

	if !oldWarmGet {
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"cksum.validate_warm_get": "true"})
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
	executeTwoGETsForChecksumValidation(proxyURL, bck, fileName, t)

	// Test changing the file xattr
	fileName = <-fileNameCh
	filepath.Walk(rootDir, fsWalkFunc)
	tutils.Logf("Changing file xattr[%s]: %s\n", fileName, fqn)
	err = tutils.SetXattrCksum(fqn, cmn.NewCksum(cmn.ChecksumXXHash, "01234abcde"), tMock)
	tassert.CheckError(t, err)
	executeTwoGETsForChecksumValidation(proxyURL, bck, fileName, t)

	// Test for none checksum algo
	fileName = <-fileNameCh
	filepath.Walk(rootDir, fsWalkFunc)
	tutils.SetClusterConfig(t, cmn.SimpleKVs{"cksum.type": cmn.ChecksumNone})
	if t.Failed() {
		goto cleanup
	}
	tutils.Logf("Changing file xattr[%s]: %s\n", fileName, fqn)
	err = tutils.SetXattrCksum(fqn, cmn.NewCksum(cmn.ChecksumXXHash, "01234abcde"), tMock)
	tassert.CheckError(t, err)
	_, err = api.GetObject(tutils.DefaultBaseAPIParams(t), bck, path.Join(ChecksumWarmValidateStr, fileName))
	if err != nil {
		t.Error("A GET on an object when checksum algo is none should pass")
	}

cleanup:
	// Restore old config
	tutils.DestroyBucket(t, proxyURL, bck)
	tutils.SetClusterConfig(t, cmn.SimpleKVs{
		"cksum.type":              oldChecksum,
		"cksum.validate_warm_get": fmt.Sprintf("%v", oldWarmGet),
	})
	close(errCh)
	close(fileNameCh)
}

func executeTwoGETsForChecksumValidation(proxyURL string, bck api.Bck, fName string, t *testing.T) {
	baseParams := tutils.BaseAPIParams(proxyURL)
	_, err := api.GetObjectWithValidation(baseParams, bck, path.Join(ChecksumWarmValidateStr, fName))
	if err == nil {
		t.Error("Error is nil, expected internal server error on a GET for an object")
	} else if !strings.Contains(err.Error(), "500") {
		t.Errorf("Expected internal server error on a GET for a corrupted object, got [%s]", err.Error())
	}
	// Execute another GET to make sure that the object is deleted
	_, err = api.GetObjectWithValidation(baseParams, bck, path.Join(ChecksumWarmValidateStr, fName))
	if err == nil {
		t.Error("Error is nil, expected not found on a second GET for a corrupted object")
	} else if !strings.Contains(err.Error(), "404") {
		t.Errorf("Expected Not Found on a second GET for a corrupted object, got [%s]", err.Error())
	}
}

func TestRangeRead(t *testing.T) {
	const fileSize = 1024
	var (
		fileName string

		numFiles   = 1
		fileNameCh = make(chan string, numFiles)
		errCh      = make(chan error, numFiles)
		bck        = api.Bck{
			Name: clibucket,
		}
		proxyURL = tutils.GetPrimaryURL()
	)

	sgl := tutils.Mem2.NewSGL(fileSize)
	defer sgl.Free()
	created := createBucketIfNotExists(t, proxyURL, bck)
	tutils.PutRandObjs(proxyURL, bck, RangeGetDir, readerType, RangeGetStr, fileSize, numFiles, errCh, fileNameCh, sgl, true)
	tassert.SelectErr(t, errCh, "put", false)

	// Get Current Config
	config := tutils.GetClusterConfig(t)
	oldEnableReadRangeChecksum := config.Cksum.EnableReadRange

	fileName = <-fileNameCh
	tutils.Logln("Testing valid cases.")
	// Validate entire object checksum is being returned
	if oldEnableReadRangeChecksum {
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"cksum.enable_read_range": "false"})
		if t.Failed() {
			goto cleanup
		}
	}
	testValidCases(t, proxyURL, bck, fileSize, fileName, true, RangeGetStr)

	// Validate only range checksum is being returned
	if !oldEnableReadRangeChecksum {
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"cksum.enable_read_range": "true"})
		if t.Failed() {
			goto cleanup
		}
	}
	testValidCases(t, proxyURL, bck, fileSize, fileName, false, RangeGetStr)

	tutils.Logln("Testing invalid cases.")
	verifyInvalidParams(t, proxyURL, bck, fileName, "", "1")
	verifyInvalidParams(t, proxyURL, bck, fileName, "1", "")
	verifyInvalidParams(t, proxyURL, bck, fileName, "-1", "-1")
	verifyInvalidParams(t, proxyURL, bck, fileName, "1", "-1")
	verifyInvalidParams(t, proxyURL, bck, fileName, "-1", "1")
	verifyInvalidParams(t, proxyURL, bck, fileName, "1", "0")
cleanup:
	tutils.Logln("Cleaning up...")
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go tutils.Del(proxyURL, bck, filepath.Join(RangeGetStr, fileName), wg, errCh, !testing.Verbose())
	wg.Wait()
	tassert.SelectErr(t, errCh, "delete", false)
	tutils.SetClusterConfig(t, cmn.SimpleKVs{"cksum.enable_read_range": fmt.Sprintf("%v", oldEnableReadRangeChecksum)})
	close(errCh)
	close(fileNameCh)

	if created {
		tutils.DestroyBucket(t, proxyURL, bck)
	}
}

func testValidCases(t *testing.T, proxyURL string, bck api.Bck, fileSize uint64, fileName string, checkEntireObjCkSum bool, checkDir string) {
	// Read the entire file range by range
	// Read in ranges of 500 to test covered, partially covered and completely
	// uncovered ranges
	byteRange := int64(500)
	iterations := int64(fileSize) / byteRange
	for i := int64(0); i < iterations; i += byteRange {
		verifyValidRanges(t, proxyURL, bck, fileName, i, byteRange, byteRange, checkEntireObjCkSum, checkDir)
	}
	verifyValidRanges(t, proxyURL, bck, fileName, byteRange*iterations, byteRange, int64(fileSize)%byteRange, checkEntireObjCkSum, checkDir)
	verifyValidRanges(t, proxyURL, bck, fileName, int64(fileSize)+100, byteRange, 0, checkEntireObjCkSum, checkDir)
}

func verifyValidRanges(t *testing.T, proxyURL string, bck api.Bck, fileName string,
	offset, length, expectedLength int64, checkEntireObjCksum bool, checkDir string) {
	var fqn string
	fsWalkFunc := func(path string, info os.FileInfo, err error) error {
		if filepath.Base(path) == fileName && strings.Contains(path, filepath.Join(bck.Name, checkDir)) {
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
	_, err := api.GetObjectWithValidation(baseParams, bck, path.Join(RangeGetStr, fileName), options)
	if err != nil {
		if !checkEntireObjCksum {
			t.Errorf("Failed to get object %s/%s! Error: %v", bck, fileName, err)
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
		t.Errorf("Returned bytes don't match expected length. Expected length: [%d]. Output length: [%d]",
			expectedLength, len(outputBytes))
	}
	for i := 0; i < len(expectedBytes); i++ {
		if expectedBytes[i] != outputBytes[i] {
			t.Errorf("Byte mismatch. Expected: %v, Actual: %v", string(expectedBytes), string(outputBytes))
		}
	}
}

func verifyInvalidParams(t *testing.T, proxyURL string, bck api.Bck, fileName string, offset string, length string) {
	q := url.Values{}
	q.Add(cmn.URLParamOffset, offset)
	q.Add(cmn.URLParamLength, length)
	baseParams := tutils.BaseAPIParams(proxyURL)
	options := api.GetObjectInput{Query: q}
	_, err := api.GetObjectWithValidation(baseParams, bck, path.Join(RangeGetStr, fileName), options)
	if err == nil {
		t.Errorf("Must fail for invalid offset %s and length %s combination.", offset, length)
	}
}

func Test_checksum(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		start, curr time.Time
		duration    time.Duration

		numPuts = 5
		bck     = api.Bck{
			Name:     clibucket,
			Provider: cmn.Cloud,
		}

		filesPutCh = make(chan string, numPuts)
		filesList  = make([]string, 0, numPuts)
		errCh      = make(chan error, numPuts*2)
		totalSize  = int64(numPuts * largeFileSize)
		proxyURL   = tutils.GetPrimaryURL()
	)

	if !isCloudBucket(t, proxyURL, bck) {
		t.Skipf("%s requires a cloud bucket", t.Name())
	}
	ldir := filepath.Join(LocalSrcDir, ChksumValidStr)
	if err := cmn.CreateDir(ldir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", ldir, err)
	}

	// Get Current Config
	config := tutils.GetClusterConfig(t)
	ocoldget := config.Cksum.ValidateColdGet
	ochksum := config.Cksum.Type

	sgl := tutils.Mem2.NewSGL(largeFileSize)
	defer sgl.Free()
	tutils.PutRandObjs(proxyURL, bck, ldir, readerType, ChksumValidStr, largeFileSize, numPuts, errCh, filesPutCh, sgl)
	tassert.SelectErr(t, errCh, "put", false)
	close(filesPutCh) // to exit for-range
	for fname := range filesPutCh {
		if fname != "" {
			filesList = append(filesList, filepath.Join(ChksumValidStr, fname))
		}
	}
	// Delete it from cache.
	tutils.EvictObjects(t, proxyURL, bck, filesList)
	// Disable checkum
	if ochksum != cmn.ChecksumNone {
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"cksum.type": cmn.ChecksumNone})
	}
	if t.Failed() {
		goto cleanup
	}
	// Disable Cold Get Validation
	if ocoldget {
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"cksum.validate_cold_get": "false"})
	}
	if t.Failed() {
		goto cleanup
	}
	start = time.Now()
	getFromObjList(proxyURL, bck, errCh, filesList, false)
	curr = time.Now()
	duration = curr.Sub(start)
	if t.Failed() {
		goto cleanup
	}
	tutils.Logf("GET %s without any checksum validation: %v\n", cmn.B2S(totalSize, 0), duration)
	tassert.SelectErr(t, errCh, "get", false)
	tutils.EvictObjects(t, proxyURL, bck, filesList)
	switch clichecksum {
	case "all":
		tutils.SetClusterConfig(t, cmn.SimpleKVs{
			"cksum.type":              cmn.ChecksumXXHash,
			"cksum.validate_cold_get": "true",
		})
		if t.Failed() {
			goto cleanup
		}
	case cmn.ChecksumXXHash:
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"cksum.type": cmn.ChecksumXXHash})
		if t.Failed() {
			goto cleanup
		}
	case ColdMD5str:
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"cksum.validate_cold_get": "true"})
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
	getFromObjList(proxyURL, bck, errCh, filesList, true)
	curr = time.Now()
	duration = curr.Sub(start)
	tutils.Logf("GET %s and validate checksum (%s): %v\n", cmn.B2S(totalSize, 0), clichecksum, duration)
	tassert.SelectErr(t, errCh, "get", false)
cleanup:
	deleteFromFileList(proxyURL, bck, errCh, filesList)
	tassert.SelectErr(t, errCh, "delete", false)
	close(errCh)
	// restore old config
	tutils.SetClusterConfig(t, cmn.SimpleKVs{
		"cksum.type":              ochksum,
		"cksum.validate_cold_get": fmt.Sprintf("%v", ocoldget),
	})
}

// deleteFromFileList requires that errCh be twice the size of len(filesList) as each
// file can produce upwards of two errors.
func deleteFromFileList(proxyURL string, bck api.Bck, errCh chan error, filesList []string) {
	wg := &sync.WaitGroup{}
	// Delete local file and objects from bucket
	for _, fn := range filesList {
		wg.Add(1)
		go tutils.Del(proxyURL, bck, fn, wg, errCh, true)
	}

	wg.Wait()
}

func getFromObjList(proxyURL string, bck api.Bck, errCh chan error, filesList []string, validate bool) {
	getsGroup := &sync.WaitGroup{}
	baseParams := tutils.BaseAPIParams(proxyURL)
	for i := 0; i < len(filesList); i++ {
		if filesList[i] != "" {
			getsGroup.Add(1)
			go func(i int) {
				var err error
				if validate {
					_, err = api.GetObjectWithValidation(baseParams, bck, filesList[i])
				} else {
					_, err = api.GetObject(baseParams, bck, filesList[i])
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

func createBucketIfNotExists(t *testing.T, proxyURL string, bck api.Bck) (created bool) {
	baseParams := tutils.BaseAPIParams(proxyURL)
	buckets, err := api.GetBucketNames(baseParams, api.Bck{})
	if err != nil {
		t.Fatalf("Failed to read bucket list: %v", err)
	}

	if cmn.StringInSlice(bck.Name, buckets.AIS) || cmn.StringInSlice(bck.Name, buckets.Cloud) {
		return false
	}

	err = api.CreateBucket(baseParams, bck)
	if err != nil {
		t.Fatalf("Failed to create ais bucket %s: %v", bck, err)
	}

	return true
}

func isCloudBucket(t *testing.T, proxyURL string, bck api.Bck) bool {
	baseParams := tutils.BaseAPIParams(proxyURL)
	buckets, err := api.GetBucketNames(baseParams, bck)
	if err != nil {
		t.Fatalf("Failed to read bucket names: %v", err)
	}

	return cmn.StringInSlice(bck.Name, buckets.Cloud)
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

func resetBucketProps(proxyURL string, bck api.Bck, t *testing.T) {
	baseParams := tutils.BaseAPIParams(proxyURL)
	if err := api.ResetBucketProps(baseParams, bck); err != nil {
		t.Errorf("bucket: %s props not reset, err: %v", clibucket, err)
	}
}
