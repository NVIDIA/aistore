// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
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

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/containers"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/readers"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func TestCloudBucketObject(t *testing.T) {
	const (
		getOP = "get"
		putOP = "put"
	)

	var (
		baseParams = tutils.BaseAPIParams()
		bck        = cmn.Bck{
			Name:     clibucket,
			Provider: cmn.AnyCloud,
		}
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true, Cloud: true, Bck: bck})

	tests := []struct {
		ty     string
		exists bool
	}{
		{putOP, false},
		{putOP, true},
		{getOP, false},
		{getOP, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s:%v", test.ty, test.exists), func(t *testing.T) {
			var (
				object = cmn.RandString(10)
			)
			if !test.exists {
				bck.Name = cmn.RandString(10)
			} else {
				bck.Name = clibucket
			}

			reader, err := readers.NewRandReader(cmn.KiB, false /* withHash */)
			tassert.CheckFatal(t, err)

			defer api.DeleteObject(baseParams, bck, object)

			switch test.ty {
			case putOP:
				err = api.PutObject(api.PutObjectArgs{
					BaseParams: baseParams,
					Bck:        bck,
					Object:     object,
					Reader:     reader,
				})
			case getOP:
				if test.exists {
					err = api.PutObject(api.PutObjectArgs{
						BaseParams: baseParams,
						Bck:        bck,
						Object:     object,
						Reader:     reader,
					})
					tassert.CheckFatal(t, err)
				}

				_, err = api.GetObjectWithValidation(baseParams, bck, object)
			default:
				t.Fail()
			}

			if !test.exists {
				if err == nil {
					t.Errorf("expected error when doing %s on non existing %q bucket", test.ty, bck)
				} else if errAsHTTPError, ok := err.(*cmn.HTTPError); !ok {
					t.Errorf("invalid error returned")
				} else if errAsHTTPError.Status != http.StatusNotFound {
					t.Errorf("returned status %d is incorrect", errAsHTTPError.Status)
				}
			} else {
				if err != nil {
					t.Errorf("expected no error when doing %s on existing %q bucket", test.ty, bck)
				}
			}
		})
	}
}

func TestAppendObject(t *testing.T) {
	var (
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     TestBucketName,
			Provider: cmn.ProviderAIS,
		}
		objHead = "1111111111"
		objBody = "222222222222222"
		objTail = "333333333"
		objName = "test/obj1"
		content = objHead + objBody + objTail
		objSize = len(content)
	)
	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	args := api.AppendArgs{
		BaseParams: baseParams,
		Bck:        bck,
		Object:     objName,
		Reader:     cmn.NewByteHandle([]byte(objHead)),
		Size:       int64(objSize),
	}
	// First call with empty `handle` to start writing the object
	handle, err := api.AppendObject(args)
	tassert.CheckFatal(t, err)
	// Use the handle returned by the first call (it never changes)
	args.Handle = handle
	args.Reader = cmn.NewByteHandle([]byte(objBody))
	_, err = api.AppendObject(args)
	tassert.CheckFatal(t, err)
	args.Reader = cmn.NewByteHandle([]byte(objTail))
	_, err = api.AppendObject(args)
	tassert.CheckFatal(t, err)
	// Flush object to make it persistent one in the bucket
	err = api.FlushObject(args)
	tassert.CheckFatal(t, err)

	// Read the object from the bucket
	buf := make([]byte, 0, objSize*2)
	writer := bytes.NewBuffer(buf)
	getArgs := api.GetObjectInput{Writer: writer}
	n, err := api.GetObject(baseParams, bck, objName, getArgs)
	tassert.CheckFatal(t, err)
	if writer.String() != content {
		t.Errorf("Invalid object content [%d]%q, expected [%d]%q",
			n, writer.String(), objSize, content)
	}
}

// PUT, then delete
func Test_putdelete(t *testing.T) {
	const fileSize = 512 * cmn.KiB

	var (
		proxyURL = tutils.RandomProxyURL()
	)

	runProviderTests(t, func(t *testing.T, bck cmn.Bck) {
		var (
			errCh      = make(chan error, numfiles)
			filesPutCh = make(chan string, numfiles)
		)

		tutils.PutRandObjs(proxyURL, bck, DeleteStr, fileSize, numfiles, errCh, filesPutCh)
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
	})
}

func listObjects(t *testing.T, proxyURL string, bck cmn.Bck, msg *cmn.SelectMsg, objLimit int) (*cmn.BucketList, error) {
	resList := testListObjects(t, proxyURL, bck, msg, objLimit)
	if resList == nil {
		return nil, fmt.Errorf("failed to list_objects %s", bck)
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

// delete existing objects that match the regex
func Test_matchdelete(t *testing.T) {
	var (
		proxyURL = tutils.RandomProxyURL()
	)

	runProviderTests(t, func(t *testing.T, bck cmn.Bck) {
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
		reslist, err := api.ListObjects(baseParams, bck, msg, 0)
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
	})
}

func Test_putdeleteRange(t *testing.T) {
	if numfiles < 10 || numfiles%10 != 0 {
		t.Fatal("numfiles must be a positive multiple of 10")
	}

	const (
		commonPrefix = "tst" // object full name: <bucket>/<commonPrefix>/<generated_name:a-####|b-####>
		objSize      = 16 * 1024
	)
	var (
		proxyURL = tutils.RandomProxyURL()
	)

	runProviderTests(t, func(t *testing.T, bck cmn.Bck) {
		errCh := make(chan error, numfiles*5)
		objsPutCh := make(chan string, numfiles)

		objList := make([]string, 0, numfiles)
		for i := 0; i < numfiles/2; i++ {
			fname := fmt.Sprintf("a-%04d", i)
			objList = append(objList, fname)
			fname = fmt.Sprintf("b-%04d", i)
			objList = append(objList, fname)
		}
		tutils.PutObjsFromList(proxyURL, bck, commonPrefix, objSize, objList, errCh, objsPutCh)
		tassert.SelectErr(t, errCh, "put", true /* fatal - if PUT does not work then it makes no sense to continue */)
		close(objsPutCh)

		tests := []struct {
			// title to print out while testing
			name string
			// a range of file IDs
			rangeStr string
			// total number of files expected to delete
			delta int
		}{
			{
				"Trying to delete files with invalid prefix",
				"file/a-{0..10}",
				0,
			},
			{
				"Trying to delete files out of range",
				commonPrefix + "/a-" + fmt.Sprintf("{%d..%d}", numfiles+10, numfiles+110),
				0,
			},
			{
				fmt.Sprintf("Deleting %d files with prefix 'a-'", numfiles/10),
				commonPrefix + "/a-" + fmt.Sprintf("{%04d..%04d}", (numfiles-numfiles/5)/2, numfiles/2),
				numfiles / 10,
			},
			{
				fmt.Sprintf("Deleting %d files (short range)", numfiles/5),
				commonPrefix + "/b-" + fmt.Sprintf("{%04d..%04d}", 1, numfiles/5),
				numfiles / 5,
			},
			{
				"Deleting files with empty range",
				commonPrefix + "/b-",
				numfiles/2 - numfiles/5,
			},
		}

		totalFiles := numfiles
		baseParams := tutils.BaseAPIParams(proxyURL)
		xactArgs := api.XactReqArgs{Kind: cmn.ActDelete, Bck: bck, Timeout: rebalanceTimeout}
		for idx, test := range tests {
			msg := &cmn.SelectMsg{Prefix: commonPrefix + "/"}
			tutils.Logf("%d. %s\n  Range: [%s]\n",
				idx+1, test.name, test.rangeStr)

			err := api.DeleteRange(baseParams, bck, test.rangeStr)
			if err != nil {
				t.Error(err)
				continue
			}
			if err := api.WaitForXaction(baseParams, xactArgs); err != nil {
				t.Error(err)
				continue
			}

			totalFiles -= test.delta
			bktlst, err := api.ListObjects(baseParams, bck, msg, 0)
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
		bckList, err := api.ListObjects(baseParams, bck, msg, 0)
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
	})
}

func Test_SameLocalAndCloudBckNameValidate(t *testing.T) {
	var (
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		bckLocal   = cmn.Bck{
			Name:     clibucket,
			Provider: cmn.ProviderAIS,
		}
		bckCloud = cmn.Bck{
			Name:     clibucket,
			Provider: cmn.AnyCloud,
		}
		fileName1 = "mytestobj1.txt"
		fileName2 = "mytestobj2.txt"
		dataLocal = []byte("im local")
		dataCloud = []byte("I'm from the cloud!")
		files     = []string{fileName1, fileName2}
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{Cloud: true, Bck: bckCloud})

	putArgsLocal := api.PutObjectArgs{
		BaseParams: baseParams,
		Bck:        bckLocal,
		Object:     fileName1,
		Reader:     readers.NewBytesReader(dataLocal),
	}

	putArgsCloud := api.PutObjectArgs{
		BaseParams: baseParams,
		Bck:        bckCloud,
		Object:     fileName1,
		Reader:     readers.NewBytesReader(dataCloud),
	}

	xactArgsPrefetch := api.XactReqArgs{Kind: cmn.ActPrefetch, Bck: bckCloud, Timeout: rebalanceTimeout}
	xactArgsEvict := api.XactReqArgs{Kind: cmn.ActEvictObjects, Bck: bckCloud, Timeout: rebalanceTimeout}
	xactArgsDelete := api.XactReqArgs{Kind: cmn.ActDelete, Bck: bckCloud, Timeout: rebalanceTimeout}

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
	err = api.PrefetchList(baseParams, bckCloud, files)
	tassert.CheckFatal(t, err)
	err = api.WaitForXaction(baseParams, xactArgsPrefetch)
	tassert.CheckFatal(t, err)

	tutils.Logf("PrefetchRange\n")
	err = api.PrefetchRange(baseParams, bckCloud, "r"+prefetchRange)
	tassert.CheckFatal(t, err)
	err = api.WaitForXaction(baseParams, xactArgsPrefetch)
	tassert.CheckFatal(t, err)

	tutils.Logf("EvictList\n")
	err = api.EvictList(baseParams, bckCloud, files)
	tassert.CheckFatal(t, err)
	err = api.WaitForXaction(baseParams, xactArgsEvict)
	tassert.CheckFatal(t, err)

	tutils.Logf("EvictRange\n")
	err = api.EvictRange(baseParams, bckCloud, prefetchRange)
	tassert.CheckFatal(t, err)
	err = api.WaitForXaction(baseParams, xactArgsEvict)
	tassert.CheckFatal(t, err)

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
	err = api.PrefetchList(baseParams, bckCloud, files)
	tassert.CheckFatal(t, err)
	err = api.WaitForXaction(baseParams, xactArgsPrefetch)
	tassert.CheckFatal(t, err)

	err = api.EvictList(baseParams, bckCloud, files)
	tassert.CheckFatal(t, err)
	err = api.WaitForXaction(baseParams, xactArgsEvict)
	tassert.CheckFatal(t, err)

	// Deleting from cloud bucket
	tutils.Logf("Deleting %s and %s from cloud bucket ...\n", fileName1, fileName2)
	err = api.DeleteList(baseParams, bckCloud, files)
	tassert.CheckFatal(t, err)
	err = api.WaitForXaction(baseParams, xactArgsDelete)
	tassert.CheckFatal(t, err)

	// Deleting from ais bucket
	tutils.Logf("Deleting %s and %s from ais bucket ...\n", fileName1, fileName2)
	err = api.DeleteList(baseParams, bckLocal, files)
	tassert.CheckFatal(t, err)
	err = api.WaitForXaction(baseParams, xactArgsDelete)
	tassert.CheckFatal(t, err)

	_, err = api.HeadObject(baseParams, bckLocal, fileName1)
	if err == nil || !strings.Contains(err.Error(), strconv.Itoa(http.StatusNotFound)) {
		t.Errorf("Local file %s not deleted", fileName1)
	}
	_, err = api.HeadObject(baseParams, bckLocal, fileName2)
	if err == nil || !strings.Contains(err.Error(), strconv.Itoa(http.StatusNotFound)) {
		t.Errorf("Local file %s not deleted", fileName2)
	}

	_, err = api.HeadObject(baseParams, bckCloud, fileName1)
	if err == nil || !strings.Contains(err.Error(), strconv.Itoa(http.StatusNotFound)) {
		t.Errorf("Cloud file %s not deleted", fileName1)
	}
	_, err = api.HeadObject(baseParams, bckCloud, fileName2)
	if err == nil || !strings.Contains(err.Error(), strconv.Itoa(http.StatusNotFound)) {
		t.Errorf("Cloud file %s not deleted", fileName2)
	}
}

func Test_SameAISAndCloudBucketName(t *testing.T) {
	var (
		defLocalProps cmn.BucketPropsToUpdate
		defCloudProps cmn.BucketPropsToUpdate

		bckLocal = cmn.Bck{
			Name:     clibucket,
			Provider: cmn.ProviderAIS,
		}
		bckCloud = cmn.Bck{
			Name:     clibucket,
			Provider: cmn.AnyCloud,
		}
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		fileName   = "mytestobj1.txt"
		dataLocal  = []byte("im local")
		dataCloud  = []byte("I'm from the cloud!")
		msg        = &cmn.SelectMsg{PageSize: int(pagesize), Props: "size,status", Prefix: "my"}
		found      = false
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{Cloud: true, Bck: bckCloud})

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
		Reader:     readers.NewBytesReader(dataLocal),
	}
	err := api.PutObject(putArgs)
	tassert.CheckFatal(t, err)

	resLocal, err := api.ListObjects(baseParams, bckLocal, msg, 0)
	tassert.CheckFatal(t, err)

	tutils.Logf("Putting object (%s) into cloud bucket %s...\n", fileName, bckCloud)
	putArgs = api.PutObjectArgs{
		BaseParams: baseParams,
		Bck:        bckCloud,
		Object:     fileName,
		Reader:     readers.NewBytesReader(dataCloud),
	}
	err = api.PutObject(putArgs)
	tassert.CheckFatal(t, err)

	resCloud, err := api.ListObjects(baseParams, bckCloud, msg, 0)
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
		bck        = cmn.Bck{
			Name:     clibucket,
			Provider: cmn.AnyCloud,
		}
		totalSize = int64(numPuts * largeFileSize)
		proxyURL  = tutils.RandomProxyURL()
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{Cloud: true, Bck: bck})

	config := tutils.GetClusterConfig(t)
	bcoldget := config.Cksum.ValidateColdGet

	tutils.PutRandObjs(proxyURL, bck, ColdValidStr, largeFileSize, numPuts, errCh, filesPutCh)
	tassert.SelectErr(t, errCh, "put", false)
	close(filesPutCh) // to exit for-range
	for fname := range filesPutCh {
		filesList = append(filesList, filepath.Join(ColdValidStr, fname))
	}
	tutils.EvictObjects(t, proxyURL, bck, filesList)
	// Disable Cold Get Validation
	if bcoldget {
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"checksum.validate_cold_get": "false"})
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
	tutils.SetClusterConfig(t, cmn.SimpleKVs{"checksum.validate_cold_get": "true"})
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
	tutils.SetClusterConfig(t, cmn.SimpleKVs{"checksum.validate_cold_get": fmt.Sprintf("%v", bcoldget)})
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
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
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
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     clibucket,
			Provider: cmn.AnyCloud,
		}
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{Cloud: true, Bck: bck})

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
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	bucket, err := tutils.GenerateNonexistentBucketName("head", baseParams)
	tassert.CheckFatal(t, err)

	bck := cmn.Bck{
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

func deleteFiles(proxyURL string, bck cmn.Bck, keynames <-chan string, wg *sync.WaitGroup, errCh chan error) {
	defer wg.Done()
	for keyname := range keynames {
		tutils.Del(proxyURL, bck, keyname, nil, errCh, true)
	}
}

func getMatchingKeys(t *testing.T, proxyURL string, bck cmn.Bck, regexmatch string,
	keynameChans []chan string, outputChan chan string) int {
	var msg = &cmn.SelectMsg{PageSize: int(pagesize)}
	reslist := testListObjects(t, proxyURL, bck, msg, 0)
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

func testListObjects(t *testing.T, proxyURL string, bck cmn.Bck, msg *cmn.SelectMsg, limit int) *cmn.BucketList {
	tutils.Logf("LIST objects %s [fast: %v, prefix: %q, page_size: %d, marker: %q]\n",
		bck, msg.Fast, msg.Prefix, msg.PageSize, msg.PageMarker)
	baseParams := tutils.BaseAPIParams(proxyURL)
	resList, err := api.ListObjects(baseParams, bck, msg, limit)
	if err != nil {
		t.Errorf("List objects %s failed, err = %v", bck, err)
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
		fileName    string
		oldFileInfo os.FileInfo
		filesList   = make([]string, 0, numFiles)
		proxyURL    = tutils.RandomProxyURL()
		baseParams  = tutils.BaseAPIParams(proxyURL)
		bmdMock     = cluster.NewBaseBownerMock()
		tMock       = cluster.NewTargetMock(bmdMock)
		bck         = cmn.Bck{
			Name:     clibucket,
			Provider: cmn.AnyCloud,
		}
	)
	props := &cmn.BucketProps{Cksum: cmn.CksumConf{Type: cmn.ChecksumXXHash}}
	bmdMock.Add(cluster.NewBck(TestBucketName, cmn.ProviderAIS, cmn.NsGlobal, props))

	tutils.CheckSkip(t, tutils.SkipTestArgs{Cloud: true, Bck: bck})

	if containers.DockerRunning() {
		t.Skip(fmt.Sprintf("test %q requires Xattributes to be set, doesn't work with docker", t.Name()))
	}

	tutils.Logf("Creating %d objects\n", numFiles)
	tutils.PutRandObjs(proxyURL, bck, ChecksumWarmValidateStr, fileSize, numFiles, errCh, fileNameCh)
	tassert.SelectErr(t, errCh, "put", false)

	fileName = <-fileNameCh
	filesList = append(filesList, filepath.Join(ChecksumWarmValidateStr, fileName))
	// Fetch the file from cloud bucket.
	_, err := api.GetObjectWithValidation(baseParams, bck, filepath.Join(ChecksumWarmValidateStr, fileName))
	if err != nil {
		t.Errorf("Failed while fetching the file from the cloud bucket. Error: [%v]", err)
	}

	config := tutils.GetClusterConfig(t)
	oldWarmGet := config.Cksum.ValidateWarmGet
	oldChecksum := config.Cksum.Type
	if !oldWarmGet {
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"checksum.validate_warm_get": "true"})
	}

	objName := filepath.Join(ChecksumWarmValidateStr, fileName)
	fqn := findObjOnDisk(bck, objName)
	tutils.CheckPathExists(t, fqn, false /*dir*/)
	oldFileInfo, _ = os.Stat(fqn)

	// Test when the contents of the file are changed
	tutils.Logf("Changing contents of the file [%s]: %s\n", fileName, fqn)
	err = ioutil.WriteFile(fqn, []byte("Contents of this file have been changed."), 0644)
	tassert.CheckFatal(t, err)
	validateGETUponFileChangeForChecksumValidation(t, proxyURL, fileName, fqn, oldFileInfo)

	// Test when the xxHash of the file is changed
	fileName = <-fileNameCh
	filesList = append(filesList, filepath.Join(ChecksumWarmValidateStr, fileName))
	fqn = findObjOnDisk(bck, objName)
	tutils.CheckPathExists(t, fqn, false /*dir*/)
	oldFileInfo, _ = os.Stat(fqn)

	tutils.Logf("Changing file xattr[%s]: %s\n", fileName, fqn)
	err = tutils.SetXattrCksum(fqn, cmn.NewCksum(cmn.ChecksumXXHash, "01234"), tMock)
	tassert.CheckError(t, err)
	validateGETUponFileChangeForChecksumValidation(t, proxyURL, fileName, fqn, oldFileInfo)

	// Test for no checksum algo
	fileName = <-fileNameCh
	filesList = append(filesList, filepath.Join(ChecksumWarmValidateStr, fileName))
	fqn = findObjOnDisk(bck, objName)
	tutils.SetClusterConfig(t, cmn.SimpleKVs{"checksum.type": cmn.ChecksumNone})
	tutils.Logf("Changing file xattr[%s]: %s\n", fileName, fqn)
	err = tutils.SetXattrCksum(fqn, cmn.NewCksum(cmn.ChecksumXXHash, "01234abcde"), tMock)
	tassert.CheckError(t, err)

	_, err = api.GetObject(baseParams, bck, filepath.Join(ChecksumWarmValidateStr, fileName))
	tassert.Errorf(t, err == nil, "A GET on an object when checksum algo is none should pass. Error: %v", err)

	// Restore old config
	tutils.SetClusterConfig(t, cmn.SimpleKVs{
		"checksum.type":              oldChecksum,
		"checksum.validate_warm_get": fmt.Sprintf("%v", oldWarmGet),
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
		bck        = cmn.Bck{
			Name:     clibucket,
			Provider: cmn.AnyCloud,
		}
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{Cloud: true, Bck: bck})

	defer func() {
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

	tutils.PutRandObjs(proxyURL, bck, EvictCBStr, largeFileSize, numPuts, errCh, filesPutCh)
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

func validateGETUponFileChangeForChecksumValidation(t *testing.T, proxyURL, fileName, fqn string,
	oldFileInfo os.FileInfo) {
	// Do a GET to see to check if a cold get was executed by comparing old and new size
	var (
		baseParams = tutils.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     clibucket,
			Provider: cmn.AnyCloud,
		}
	)
	_, err := api.GetObjectWithValidation(baseParams, bck, filepath.Join(ChecksumWarmValidateStr, fileName))
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
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		bmdMock    = cluster.NewBaseBownerMock()
		tMock      = cluster.NewTargetMock(bmdMock)
		bck        = cmn.Bck{
			Name:     TestBucketName,
			Provider: cmn.ProviderAIS,
		}
	)
	props := &cmn.BucketProps{Cksum: cmn.CksumConf{Type: cmn.ChecksumXXHash}}
	bmdMock.Add(cluster.NewBck(TestBucketName, cmn.ProviderAIS, cmn.NsGlobal, props))

	if containers.DockerRunning() {
		t.Skip(fmt.Sprintf("test %q requires Xattributes to be set, doesn't work with docker", t.Name()))
	}

	tutils.CreateFreshBucket(t, proxyURL, bck)
	tutils.PutRandObjs(proxyURL, bck, ChecksumWarmValidateStr, fileSize, numFiles, errCh, fileNameCh)
	tassert.SelectErr(t, errCh, "put", false)

	// Get Current Config
	config := tutils.GetClusterConfig(t)
	oldWarmGet := config.Cksum.ValidateWarmGet
	oldChecksum := config.Cksum.Type

	if !oldWarmGet {
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"checksum.validate_warm_get": "true"})
	}

	// Test changing the file content
	objName := filepath.Join(ChecksumWarmValidateStr, <-fileNameCh)
	fqn = findObjOnDisk(bck, objName)
	tutils.Logf("Changing contents of the file [%s]: %s\n", objName, fqn)
	err = ioutil.WriteFile(fqn, []byte("Contents of this file have been changed."), 0644)
	tassert.CheckFatal(t, err)
	executeTwoGETsForChecksumValidation(proxyURL, bck, objName, t)

	// Test changing the file xattr
	objName = filepath.Join(ChecksumWarmValidateStr, <-fileNameCh)
	fqn = findObjOnDisk(bck, objName)
	tutils.Logf("Changing file xattr[%s]: %s\n", objName, fqn)
	err = tutils.SetXattrCksum(fqn, cmn.NewCksum(cmn.ChecksumXXHash, "01234abcde"), tMock)
	tassert.CheckError(t, err)
	executeTwoGETsForChecksumValidation(proxyURL, bck, objName, t)

	// Test for none checksum algo
	objName = filepath.Join(ChecksumWarmValidateStr, <-fileNameCh)
	fqn = findObjOnDisk(bck, objName)
	tutils.SetClusterConfig(t, cmn.SimpleKVs{"checksum.type": cmn.ChecksumNone})
	tutils.Logf("Changing file xattr[%s]: %s\n", objName, fqn)
	err = tutils.SetXattrCksum(fqn, cmn.NewCksum(cmn.ChecksumXXHash, "01234abcde"), tMock)
	tassert.CheckError(t, err)
	_, err = api.GetObject(baseParams, bck, objName)
	if err != nil {
		t.Error("A GET on an object when checksum algo is none should pass")
	}

	// Restore old config
	tutils.DestroyBucket(t, proxyURL, bck)
	tutils.SetClusterConfig(t, cmn.SimpleKVs{
		"checksum.type":              oldChecksum,
		"checksum.validate_warm_get": fmt.Sprintf("%v", oldWarmGet),
	})
	close(errCh)
	close(fileNameCh)
}

func executeTwoGETsForChecksumValidation(proxyURL string, bck cmn.Bck, objName string, t *testing.T) {
	baseParams := tutils.BaseAPIParams(proxyURL)
	_, err := api.GetObjectWithValidation(baseParams, bck, objName)
	if err == nil {
		t.Error("Error is nil, expected internal server error on a GET for an object")
	} else if !strings.Contains(err.Error(), "500") {
		t.Errorf("Expected internal server error on a GET for a corrupted object, got [%s]", err.Error())
	}
	// Execute another GET to make sure that the object is deleted
	_, err = api.GetObjectWithValidation(baseParams, bck, objName)
	if err == nil {
		t.Error("Error is nil, expected not found on a second GET for a corrupted object")
	} else if !strings.Contains(err.Error(), "404") {
		t.Errorf("Expected Not Found on a second GET for a corrupted object, got [%s]", err.Error())
	}
}

func TestRangeRead(t *testing.T) {
	const (
		numFiles = 1
		fileSize = 1024
	)

	runProviderTests(t, func(t *testing.T, bck cmn.Bck) {
		var (
			fileName   string
			fileNameCh = make(chan string, numFiles)
			errCh      = make(chan error, numFiles)
			proxyURL   = tutils.RandomProxyURL()
			baseParams = tutils.BaseAPIParams(proxyURL)
		)

		tutils.PutRandObjs(proxyURL, bck, RangeGetStr, fileSize, numFiles, errCh, fileNameCh, true)
		tassert.SelectErr(t, errCh, "put", false)

		// Get Current Config
		config := tutils.GetClusterConfig(t)
		oldEnableReadRangeChecksum := config.Cksum.EnableReadRange

		defer func() {
			tutils.Logln("Cleaning up...")
			err := api.DeleteObject(baseParams, bck, filepath.Join(RangeGetStr, fileName))
			tassert.CheckError(t, err)
			tutils.SetClusterConfig(t, cmn.SimpleKVs{"checksum.enable_read_range": fmt.Sprintf("%v", oldEnableReadRangeChecksum)})
			close(errCh)
			close(fileNameCh)
		}()

		fileName = <-fileNameCh
		tutils.Logln("Testing valid cases.")
		// Validate entire object checksum is being returned
		if oldEnableReadRangeChecksum {
			tutils.SetClusterConfig(t, cmn.SimpleKVs{"checksum.enable_read_range": "false"})
			if t.Failed() {
				t.FailNow()
			}
		}
		testValidCases(t, proxyURL, bck, fileSize, fileName, true, RangeGetStr)

		// Validate only range checksum is being returned
		if !oldEnableReadRangeChecksum {
			tutils.SetClusterConfig(t, cmn.SimpleKVs{"checksum.enable_read_range": "true"})
			if t.Failed() {
				t.FailNow()
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
	})
}

func testValidCases(t *testing.T, proxyURL string, bck cmn.Bck, fileSize uint64, fileName string, checkEntireObjCkSum bool, checkDir string) {
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

func verifyValidRanges(t *testing.T, proxyURL string, bck cmn.Bck, fileName string,
	offset, length, expectedLength int64, checkEntireObjCksum bool, checkDir string) {
	fqn := findObjOnDisk(bck, filepath.Join(checkDir, fileName))

	q := url.Values{}
	q.Add(cmn.URLParamOffset, strconv.FormatInt(offset, 10))
	q.Add(cmn.URLParamLength, strconv.FormatInt(length, 10))
	var b bytes.Buffer
	w := bufio.NewWriter(&b)
	baseParams := tutils.BaseAPIParams(proxyURL)
	options := api.GetObjectInput{Writer: w, Query: q}
	_, err := api.GetObjectWithValidation(baseParams, bck, filepath.Join(RangeGetStr, fileName), options)
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

func verifyInvalidParams(t *testing.T, proxyURL string, bck cmn.Bck, fileName, offset, length string) {
	q := url.Values{}
	q.Add(cmn.URLParamOffset, offset)
	q.Add(cmn.URLParamLength, length)
	baseParams := tutils.BaseAPIParams(proxyURL)
	options := api.GetObjectInput{Query: q}
	_, err := api.GetObjectWithValidation(baseParams, bck, filepath.Join(RangeGetStr, fileName), options)
	if err == nil {
		t.Errorf("Must fail for invalid offset %s and length %s combination.", offset, length)
	}
}

func Test_checksum(t *testing.T) {
	var (
		start, curr time.Time
		duration    time.Duration

		numPuts = 5
		bck     = cmn.Bck{
			Name:     clibucket,
			Provider: cmn.AnyCloud,
		}

		filesPutCh = make(chan string, numPuts)
		filesList  = make([]string, 0, numPuts)
		errCh      = make(chan error, numPuts*2)
		totalSize  = int64(numPuts * largeFileSize)
		proxyURL   = tutils.RandomProxyURL()
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true, Cloud: true, Bck: bck})

	// Get Current Config
	config := tutils.GetClusterConfig(t)
	ocoldget := config.Cksum.ValidateColdGet
	ochksum := config.Cksum.Type

	tutils.PutRandObjs(proxyURL, bck, ChksumValidStr, largeFileSize, numPuts, errCh, filesPutCh)
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
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"checksum.type": cmn.ChecksumNone})
	}
	if t.Failed() {
		goto cleanup
	}
	// Disable Cold Get Validation
	if ocoldget {
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"checksum.validate_cold_get": "false"})
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
			"checksum.type":              cmn.ChecksumXXHash,
			"checksum.validate_cold_get": "true",
		})
		if t.Failed() {
			goto cleanup
		}
	case cmn.ChecksumXXHash:
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"checksum.type": cmn.ChecksumXXHash})
		if t.Failed() {
			goto cleanup
		}
	case ColdMD5str:
		tutils.SetClusterConfig(t, cmn.SimpleKVs{"checksum.validate_cold_get": "true"})
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
		"checksum.type":              ochksum,
		"checksum.validate_cold_get": fmt.Sprintf("%v", ocoldget),
	})
}

// deleteFromFileList requires that errCh be twice the size of len(filesList) as each
// file can produce upwards of two errors.
func deleteFromFileList(proxyURL string, bck cmn.Bck, errCh chan error, filesList []string) {
	wg := &sync.WaitGroup{}
	// Delete local file and objects from bucket
	for _, fn := range filesList {
		wg.Add(1)
		go tutils.Del(proxyURL, bck, fn, wg, errCh, true)
	}

	wg.Wait()
}

func getFromObjList(proxyURL string, bck cmn.Bck, errCh chan error, filesList []string, validate bool) {
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

func validateBucketProps(t *testing.T, expected cmn.BucketPropsToUpdate, actual cmn.BucketProps) {
	// Apply changes on props that we have received. If after applying anything
	// has changed it means that the props were not applied.
	tmpProps := *actual.Clone()
	tmpProps.Apply(expected)
	if !reflect.DeepEqual(tmpProps, actual) {
		t.Errorf("bucket props are not equal, expected: %+v, got: %+v", tmpProps, actual)
	}
}

func resetBucketProps(proxyURL string, bck cmn.Bck, t *testing.T) {
	baseParams := tutils.BaseAPIParams(proxyURL)
	if err := api.ResetBucketProps(baseParams, bck); err != nil {
		t.Errorf("bucket: %s props not reset, err: %v", clibucket, err)
	}
}

func findObjOnDisk(bck cmn.Bck, objName string) string {
	var fqn string
	fsWalkFunc := func(path string, info os.FileInfo, err error) error {
		if strings.HasSuffix(path, "/"+objName) && strings.Contains(path, "/"+bck.Name+"/") {
			fqn = path
		}
		return nil
	}
	filepath.Walk(rootDir, fsWalkFunc)
	return fqn
}
