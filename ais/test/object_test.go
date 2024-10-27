// Package integration_test.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/ais/backend"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/docker"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact"
)

const (
	httpBucketURL           = "http://storage.googleapis.com/minikube/"
	httpObjectName          = "minikube-0.6.iso.sha256"
	httpObjectURL           = httpBucketURL + httpObjectName
	httpAnotherObjectName   = "minikube-0.7.iso.sha256"
	httpAnotherObjectURL    = httpBucketURL + httpAnotherObjectName
	httpObjectOutput        = "ff0f444f4a01f0ec7925e6bb0cb05e84156cff9cc8de6d03102d8b3df35693e2"
	httpAnotherObjectOutput = "aadc8b6f5720d5a493a36e1f07f71bffb588780c76498d68cd761793d2ca344e"
)

const (
	getOP = "GET"
	putOP = "PUT"
)

func TestObjectInvalidName(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams()
		bck        = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
		}
	)

	tests := []struct {
		op      string
		objName string
	}{
		{op: putOP, objName: "."},
		{op: putOP, objName: ".."},
		{op: putOP, objName: "../smth.txt"},
		{op: putOP, objName: "../."},
		{op: putOP, objName: "../.."},
		{op: putOP, objName: "///"},
		{op: putOP, objName: ""},
		{op: putOP, objName: "1\x00"},
		{op: putOP, objName: "\n"},

		{op: getOP, objName: ""},
		{op: getOP, objName: ".."},
		{op: getOP, objName: "///"},
		{op: getOP, objName: "...../log/aisnode.INFO"},
		{op: getOP, objName: "/...../log/aisnode.INFO"},
		{op: getOP, objName: "/\\../\\../\\../\\../log/aisnode.INFO"},
		{op: getOP, objName: "\\../\\../\\../\\../log/aisnode.INFO"},
		{op: getOP, objName: "/../../../../log/aisnode.INFO"},
		{op: getOP, objName: "/././../../../../log/aisnode.INFO"},
	}

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	for _, test := range tests {
		t.Run(test.op, func(t *testing.T) {
			switch test.op {
			case putOP:
				reader, err := readers.NewRand(cos.KiB, cos.ChecksumNone)
				tassert.CheckFatal(t, err)
				_, err = api.PutObject(&api.PutArgs{
					BaseParams: baseParams,
					Bck:        bck,
					ObjName:    test.objName,
					Reader:     reader,
				})
				tassert.Errorf(t, err != nil, "expected error to occur (object name: %q)", test.objName)
			case getOP:
				_, err := api.GetObjectWithValidation(baseParams, bck, test.objName, nil)
				tassert.Errorf(t, err != nil, "expected error to occur (object name: %q)", test.objName)
			default:
				panic(test.op)
			}
		})
	}
}

func TestRemoteBucketObject(t *testing.T) {
	var (
		baseParams = tools.BaseAPIParams()
		bck        = cliBck
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true, RemoteBck: true, Bck: bck})

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
			object := trand.String(10)
			if !test.exists {
				bck.Name = trand.String(10)
			} else {
				bck.Name = cliBck.Name
			}

			reader, err := readers.NewRand(cos.KiB, cos.ChecksumNone)
			tassert.CheckFatal(t, err)

			defer api.DeleteObject(baseParams, bck, object)

			switch test.ty {
			case putOP:
				var oah api.ObjAttrs
				oah, err = api.PutObject(&api.PutArgs{
					BaseParams: baseParams,
					Bck:        bck,
					ObjName:    object,
					Reader:     reader,
				})
				if err == nil {
					oa := oah.Attrs()
					tlog.Logf("PUT(%s) attrs %s\n", bck.Cname(object), oa.String())
				}
			case getOP:
				if test.exists {
					_, err = api.PutObject(&api.PutArgs{
						BaseParams: baseParams,
						Bck:        bck,
						ObjName:    object,
						Reader:     reader,
					})
					tassert.CheckFatal(t, err)
				}

				_, err = api.GetObjectWithValidation(baseParams, bck, object, nil)
			default:
				t.Fail()
			}

			if !test.exists {
				if err == nil {
					t.Errorf("expected error when doing %s on non existing %q bucket", test.ty, bck)
				}
			} else if err != nil {
				t.Errorf("expected no error when executing %s on existing %q bucket(err = %v)",
					test.ty, bck, err)
			}
		})
	}
}

func TestHttpProviderObjectGet(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true}) // NOTE: ht:// is now conditionally linked, requires 'ht' build tag
	var (
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
		hbo, _     = cmn.NewHTTPObjPath(httpObjectURL)
		w          = bytes.NewBuffer(nil)
		getArgs    = api.GetArgs{Writer: w}
	)
	t.Cleanup(func() {
		tools.DestroyBucket(t, proxyURL, hbo.Bck)
	})

	// get using the HTTP API
	getArgs.Query = make(url.Values, 1)
	getArgs.Query.Set(apc.QparamOrigURL, httpObjectURL)
	_, err := api.GetObject(baseParams, hbo.Bck, httpObjectName, &getArgs)

	if err != nil && strings.Contains(err.Error(), "backend is missing in the cluster configuration") {
		t.Skipf("test %q requires 'ht://' backend ('aisnode' build with build tag 'ht')", t.Name())
	}

	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, strings.TrimSpace(w.String()) == httpObjectOutput, "bad content (expected:%s got:%s)",
		httpObjectOutput, w.String())

	// get another object using /v1/objects/bucket-name/object-name endpoint
	w.Reset()
	getArgs.Query = make(url.Values, 1)
	getArgs.Query.Set(apc.QparamOrigURL, httpAnotherObjectURL)
	_, err = api.GetObject(baseParams, hbo.Bck, httpAnotherObjectName, &getArgs)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, strings.TrimSpace(w.String()) == httpAnotherObjectOutput, "bad content (expected:%s got:%s)",
		httpAnotherObjectOutput, w.String())

	// list object should contain both the objects
	reslist, err := api.ListObjects(baseParams, hbo.Bck, &apc.LsoMsg{}, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(reslist.Entries) == 2, "should have exactly 2 entries in bucket")

	matchCount := 0
	for _, en := range reslist.Entries {
		if en.Name == httpAnotherObjectName || en.Name == httpObjectName {
			matchCount++
		}
	}
	tassert.Errorf(t, matchCount == 2, "objects %s and %s should be present in %s",
		httpObjectName, httpAnotherObjectName, hbo.Bck)
}

func TestAppendObject(t *testing.T) {
	for _, cksumType := range cos.SupportedChecksums() {
		t.Run(cksumType, func(t *testing.T) {
			var (
				proxyURL   = tools.RandomProxyURL(t)
				baseParams = tools.BaseAPIParams(proxyURL)
				bck        = cmn.Bck{
					Name:     trand.String(10),
					Provider: apc.AIS,
				}
				objName = "test/obj1"

				objHead = "1111111111"
				objBody = "222222222222222"
				objTail = "333333333"
				content = objHead + objBody + objTail
				objSize = len(content)
			)
			tools.CreateBucket(t, proxyURL, bck,
				&cmn.BpropsToSet{Cksum: &cmn.CksumConfToSet{Type: apc.Ptr(cksumType)}},
				true, /*cleanup*/
			)

			var (
				err    error
				handle string
				cksum  = cos.NewCksumHash(cksumType)
			)
			for _, body := range []string{objHead, objBody, objTail} {
				args := api.AppendArgs{
					BaseParams: baseParams,
					Bck:        bck,
					Object:     objName,
					Handle:     handle,
					Reader:     cos.NewByteHandle([]byte(body)),
				}
				handle, err = api.AppendObject(&args)
				tassert.CheckFatal(t, err)

				_, err = cksum.H.Write([]byte(body))
				tassert.CheckFatal(t, err)
			}

			// Flush object with cksum to make it persistent in the bucket.
			cksum.Finalize()
			err = api.FlushObject(&api.FlushArgs{
				BaseParams: baseParams,
				Bck:        bck,
				Object:     objName,
				Handle:     handle,
				Cksum:      cksum.Clone(),
			})
			tassert.CheckFatal(t, err)

			// Read the object from the bucket.
			writer := bytes.NewBuffer(nil)
			getArgs := api.GetArgs{Writer: writer}
			oah, err := api.GetObjectWithValidation(baseParams, bck, objName, &getArgs)
			if !cksum.IsEmpty() {
				tassert.CheckFatal(t, err)
			}
			tassert.Errorf(
				t, writer.String() == content,
				"invalid object content: [%d](%s), expected: [%d](%s)",
				oah.Size(), writer.String(), objSize, content,
			)
		})
		time.Sleep(3 * time.Second)
	}
}

func TestSameBucketName(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bckLocal   = cmn.Bck{
			Name:     cliBck.Name,
			Provider: apc.AIS,
		}
		bckRemote  = cliBck
		fileName1  = "mytestobj1.txt"
		fileName2  = "mytestobj2.txt"
		objRange   = "mytestobj{1..2}.txt"
		dataLocal  = []byte("I'm from ais:// bucket")
		dataRemote = []byte("I'm from the cloud!")
		files      = []string{fileName1, fileName2}
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: bckRemote})

	putArgsLocal := api.PutArgs{
		BaseParams: baseParams,
		Bck:        bckLocal,
		ObjName:    fileName1,
		Reader:     readers.NewBytes(dataLocal),
	}

	putArgsRemote := api.PutArgs{
		BaseParams: baseParams,
		Bck:        bckRemote,
		ObjName:    fileName1,
		Reader:     readers.NewBytes(dataRemote),
	}

	// PUT/GET/DEL Without ais bucket
	tlog.Logf("Validating responses for non-existent ais bucket...\n")
	_, err := api.PutObject(&putArgsLocal)
	if err == nil {
		t.Fatalf("ais bucket %s does not exist: Expected an error.", bckLocal.String())
	}

	// PUT -> remote
	_, err = api.PutObject(&putArgsRemote)
	tassert.CheckFatal(t, err)
	putArgsRemote.ObjName = fileName2
	_, err = api.PutObject(&putArgsRemote)
	tassert.CheckFatal(t, err)
	putArgsRemote.ObjName = fileName1

	_, err = api.GetObject(baseParams, bckLocal, fileName1, nil)
	if err == nil {
		t.Fatalf("ais bucket %s does not exist: Expected an error.", bckLocal.String())
	}

	err = api.DeleteObject(baseParams, bckLocal, fileName1)
	if err == nil {
		t.Fatalf("ais bucket %s does not exist: Expected an error.", bckLocal.String())
	}

	tlog.Logf("PrefetchList num=%d\n", len(files))
	{
		var msg apc.PrefetchMsg
		msg.ObjNames = files
		prefetchListID, err := api.Prefetch(baseParams, bckRemote, msg)
		tassert.CheckFatal(t, err)
		args := xact.ArgsMsg{ID: prefetchListID, Kind: apc.ActPrefetchObjects, Timeout: tools.RebalanceTimeout}
		_, err = api.WaitForXactionIC(baseParams, &args)
		tassert.CheckFatal(t, err)
	}

	tlog.Logf("PrefetchRange %s\n", objRange)
	{
		var msg apc.PrefetchMsg
		msg.Template = objRange
		prefetchRangeID, err := api.Prefetch(baseParams, bckRemote, msg)
		tassert.CheckFatal(t, err)
		args := xact.ArgsMsg{ID: prefetchRangeID, Kind: apc.ActPrefetchObjects, Timeout: tools.RebalanceTimeout}
		_, err = api.WaitForXactionIC(baseParams, &args)
		tassert.CheckFatal(t, err)
	}

	// delete one obj from remote, and check evictions (below)
	err = api.DeleteObject(baseParams, bckRemote, fileName1)
	tassert.CheckFatal(t, err)

	tlog.Logf("EvictList %v\n", files)
	evictListID, err := api.EvictMultiObj(baseParams, bckRemote, files, "" /*template*/)
	tassert.CheckFatal(t, err)
	args := xact.ArgsMsg{ID: evictListID, Kind: apc.ActEvictObjects, Timeout: tools.RebalanceTimeout}
	status, err := api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, status.ErrMsg != "", "expecting errors when not finding listed objects")

	tlog.Logf("EvictRange\n")
	evictRangeID, err := api.EvictMultiObj(baseParams, bckRemote, nil /*lst objnames*/, objRange)
	tassert.CheckFatal(t, err)
	args = xact.ArgsMsg{ID: evictRangeID, Kind: apc.ActEvictObjects, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	tools.CreateBucket(t, proxyURL, bckLocal, nil, true /*cleanup*/)

	// PUT
	tlog.Logf("PUT %s and %s -> both buckets...\n", fileName1, fileName2)
	_, err = api.PutObject(&putArgsLocal)
	tassert.CheckFatal(t, err)
	putArgsLocal.ObjName = fileName2
	_, err = api.PutObject(&putArgsLocal)
	tassert.CheckFatal(t, err)

	_, err = api.PutObject(&putArgsRemote)
	tassert.CheckFatal(t, err)
	putArgsRemote.ObjName = fileName2
	_, err = api.PutObject(&putArgsRemote)
	tassert.CheckFatal(t, err)

	hargs := api.HeadArgs{FltPresence: apc.FltPresent}

	// Check that ais bucket has 2 objects
	tlog.Logf("Validating that ais bucket contains %s and %s ...\n", fileName1, fileName2)
	_, err = api.HeadObject(baseParams, bckLocal, fileName1, hargs)
	tassert.CheckFatal(t, err)
	_, err = api.HeadObject(baseParams, bckLocal, fileName2, hargs)
	tassert.CheckFatal(t, err)

	// Prefetch/Evict should work
	{
		var msg apc.PrefetchMsg
		msg.ObjNames = files
		prefetchListID, err := api.Prefetch(baseParams, bckRemote, msg)
		tassert.CheckFatal(t, err)
		args = xact.ArgsMsg{ID: prefetchListID, Kind: apc.ActPrefetchObjects, Timeout: tools.RebalanceTimeout}
		_, err = api.WaitForXactionIC(baseParams, &args)
		tassert.CheckFatal(t, err)
	}

	evictListID, err = api.EvictMultiObj(baseParams, bckRemote, files, "" /*template*/)
	tassert.CheckFatal(t, err)
	args = xact.ArgsMsg{ID: evictListID, Kind: apc.ActEvictObjects, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	// Delete from cloud bucket
	tlog.Logf("Deleting %s and %s from cloud bucket ...\n", fileName1, fileName2)
	deleteID, err := api.DeleteMultiObj(baseParams, bckRemote, files, "" /*template*/)
	tassert.CheckFatal(t, err)
	args = xact.ArgsMsg{ID: deleteID, Kind: apc.ActDeleteObjects, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	// Delete from ais bucket
	tlog.Logf("Deleting %s and %s from ais bucket ...\n", fileName1, fileName2)
	deleteID, err = api.DeleteMultiObj(baseParams, bckLocal, files, "" /*template*/)
	tassert.CheckFatal(t, err)
	args = xact.ArgsMsg{ID: deleteID, Kind: apc.ActDeleteObjects, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	_, err = api.HeadObject(baseParams, bckLocal, fileName1, hargs)
	if err == nil || !strings.Contains(err.Error(), strconv.Itoa(http.StatusNotFound)) {
		t.Errorf("Local file %s not deleted", fileName1)
	}
	_, err = api.HeadObject(baseParams, bckLocal, fileName2, hargs)
	if err == nil || !strings.Contains(err.Error(), strconv.Itoa(http.StatusNotFound)) {
		t.Errorf("Local file %s not deleted", fileName2)
	}

	hargsRemote := api.HeadArgs{FltPresence: apc.FltExists}
	_, err = api.HeadObject(baseParams, bckRemote, fileName1, hargsRemote)
	if err == nil {
		t.Errorf("remote file %s not deleted", fileName1)
	}
	_, err = api.HeadObject(baseParams, bckRemote, fileName2, hargsRemote)
	if err == nil {
		t.Errorf("remote file %s not deleted", fileName2)
	}
}

func Test_SameAISAndRemoteBucketName(t *testing.T) {
	var (
		defLocalProps  cmn.BpropsToSet
		defRemoteProps cmn.BpropsToSet

		bckLocal = cmn.Bck{
			Name:     cliBck.Name,
			Provider: apc.AIS,
		}
		bckRemote  = cliBck
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		fileName   = "mytestobj1.txt"
		dataLocal  = []byte("im local")
		dataRemote = []byte("I'm from the cloud!")
		msg        = &apc.LsoMsg{Props: "size,status", Prefix: "my"}
		found      = false
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: bckRemote})

	tools.CreateBucket(t, proxyURL, bckLocal, nil, true /*cleanup*/)

	bucketPropsLocal := &cmn.BpropsToSet{
		Cksum: &cmn.CksumConfToSet{
			Type: apc.Ptr(cos.ChecksumNone),
		},
	}
	bucketPropsRemote := &cmn.BpropsToSet{}

	// Put
	tlog.Logf("PUT %s => %s\n", fileName, bckLocal)
	putArgs := api.PutArgs{
		BaseParams: baseParams,
		Bck:        bckLocal,
		ObjName:    fileName,
		Reader:     readers.NewBytes(dataLocal),
	}
	_, err := api.PutObject(&putArgs)
	tassert.CheckFatal(t, err)

	resLocal, err := api.ListObjects(baseParams, bckLocal, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)

	tlog.Logf("PUT %s => %s\n", fileName, bckRemote)
	putArgs = api.PutArgs{
		BaseParams: baseParams,
		Bck:        bckRemote,
		ObjName:    fileName,
		Reader:     readers.NewBytes(dataRemote),
	}
	_, err = api.PutObject(&putArgs)
	tassert.CheckFatal(t, err)

	resRemote, err := api.ListObjects(baseParams, bckRemote, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)

	if len(resLocal.Entries) != 1 {
		t.Fatalf("Expected number of files in ais bucket (%s) does not match: expected %v, got %v",
			bckRemote.String(), 1, len(resLocal.Entries))
	}

	for _, en := range resRemote.Entries {
		if en.Name == fileName {
			found = true
			break
		}
	}

	if !found {
		t.Fatalf("File (%s) not found in cloud bucket (%s)", fileName, bckRemote.String())
	}

	// Get
	oahLocal, err := api.GetObject(baseParams, bckLocal, fileName, nil)
	tassert.CheckFatal(t, err)
	lenLocal := oahLocal.Size()
	oahRemote, err := api.GetObject(baseParams, bckRemote, fileName, nil)
	tassert.CheckFatal(t, err)
	lenRemote := oahRemote.Size()

	if lenLocal == lenRemote {
		t.Errorf("Local file and cloud file have same size, expected: local (%v) cloud (%v) got: local (%v) cloud (%v)",
			len(dataLocal), len(dataRemote), lenLocal, lenRemote)
	}

	// Delete
	err = api.DeleteObject(baseParams, bckRemote, fileName)
	tassert.CheckFatal(t, err)

	oahLocal, err = api.GetObject(baseParams, bckLocal, fileName, nil)
	tassert.CheckFatal(t, err)
	lenLocal = oahLocal.Size()

	// Check that local object still exists
	if lenLocal != int64(len(dataLocal)) {
		t.Errorf("Local file %s deleted", fileName)
	}

	// Check that cloud object is deleted
	_, err = api.HeadObject(baseParams, bckRemote, fileName, api.HeadArgs{FltPresence: apc.FltExistsOutside})
	if !strings.Contains(err.Error(), strconv.Itoa(http.StatusNotFound)) {
		t.Errorf("Remote file %s not deleted", fileName)
	}

	// Set Props Object
	_, err = api.SetBucketProps(baseParams, bckLocal, bucketPropsLocal)
	tassert.CheckFatal(t, err)

	_, err = api.SetBucketProps(baseParams, bckRemote, bucketPropsRemote)
	tassert.CheckFatal(t, err)

	// Validate ais bucket props are set
	localProps, err := api.HeadBucket(baseParams, bckLocal, true /* don't add */)
	tassert.CheckFatal(t, err)
	validateBucketProps(t, bucketPropsLocal, localProps)

	// Validate cloud bucket props are set
	cloudProps, err := api.HeadBucket(baseParams, bckRemote, true /* don't add */)
	tassert.CheckFatal(t, err)
	validateBucketProps(t, bucketPropsRemote, cloudProps)

	// Reset ais bucket props and validate they are reset
	_, err = api.ResetBucketProps(baseParams, bckLocal)
	tassert.CheckFatal(t, err)
	localProps, err = api.HeadBucket(baseParams, bckLocal, true /* don't add */)
	tassert.CheckFatal(t, err)
	validateBucketProps(t, &defLocalProps, localProps)

	// Check if cloud bucket props remain the same
	cloudProps, err = api.HeadBucket(baseParams, bckRemote, true /* don't add */)
	tassert.CheckFatal(t, err)
	validateBucketProps(t, bucketPropsRemote, cloudProps)

	// Reset cloud bucket props
	_, err = api.ResetBucketProps(baseParams, bckRemote)
	tassert.CheckFatal(t, err)
	cloudProps, err = api.HeadBucket(baseParams, bckRemote, true /* don't add */)
	tassert.CheckFatal(t, err)
	validateBucketProps(t, &defRemoteProps, cloudProps)

	// Check if ais bucket props remain the same
	localProps, err = api.HeadBucket(baseParams, bckLocal, true /* don't add */)
	tassert.CheckFatal(t, err)
	validateBucketProps(t, &defLocalProps, localProps)
}

func Test_coldgetmd5(t *testing.T) {
	var (
		m = ioContext{
			t:        t,
			bck:      cliBck,
			num:      5,
			fileSize: largeFileSize,
			prefix:   "md5/obj-",
		}
		totalSize  = int64(uint64(m.num) * m.fileSize)
		proxyURL   = tools.RandomProxyURL(t)
		propsToSet *cmn.BpropsToSet
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: m.bck})

	m.initAndSaveState(true /*cleanup*/)

	baseParams := tools.BaseAPIParams(proxyURL)
	p, err := api.HeadBucket(baseParams, m.bck, false /* don't add */)
	tassert.CheckFatal(t, err)

	t.Cleanup(func() {
		propsToSet = &cmn.BpropsToSet{
			Cksum: &cmn.CksumConfToSet{
				ValidateColdGet: apc.Ptr(p.Cksum.ValidateColdGet),
			},
		}
		_, err = api.SetBucketProps(baseParams, m.bck, propsToSet)
		tassert.CheckError(t, err)
		m.del()
	})

	m.remotePuts(true /*evict*/)

	// Disable Cold Get Validation
	if p.Cksum.ValidateColdGet {
		propsToSet = &cmn.BpropsToSet{
			Cksum: &cmn.CksumConfToSet{
				ValidateColdGet: apc.Ptr(false),
			},
		}
		_, err = api.SetBucketProps(baseParams, m.bck, propsToSet)
		tassert.CheckFatal(t, err)
	}

	start := time.Now()
	m.gets(nil /*api.GetArgs*/, false /*withValidation*/)
	tlog.Logf("GET %s without MD5 validation: %v\n", cos.ToSizeIEC(totalSize, 0), time.Since(start))

	m.evict()

	// Enable cold get validation.
	propsToSet = &cmn.BpropsToSet{
		Cksum: &cmn.CksumConfToSet{
			ValidateColdGet: apc.Ptr(true),
		},
	}
	_, err = api.SetBucketProps(baseParams, m.bck, propsToSet)
	tassert.CheckFatal(t, err)

	start = time.Now()
	m.gets(nil /*api.GetArgs*/, true /*withValidation*/)
	tlog.Logf("GET %s with MD5 validation:    %v\n", cos.ToSizeIEC(totalSize, 0), time.Since(start))
}

func TestHeadBucket(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     testBucketName,
			Provider: apc.AIS,
		}
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	bckPropsToSet := &cmn.BpropsToSet{
		Cksum: &cmn.CksumConfToSet{
			ValidateWarmGet: apc.Ptr(true),
		},
		LRU: &cmn.LRUConfToSet{
			Enabled: apc.Ptr(true),
		},
	}
	_, err := api.SetBucketProps(baseParams, bck, bckPropsToSet)
	tassert.CheckFatal(t, err)

	p, err := api.HeadBucket(baseParams, bck, false /* don't add */)
	tassert.CheckFatal(t, err)

	validateBucketProps(t, bckPropsToSet, p)
}

func TestHeadRemoteBucket(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cliBck
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: bck})

	bckPropsToSet := &cmn.BpropsToSet{
		Cksum: &cmn.CksumConfToSet{
			ValidateWarmGet: apc.Ptr(true),
			ValidateColdGet: apc.Ptr(true),
		},
		LRU: &cmn.LRUConfToSet{
			Enabled: apc.Ptr(true),
		},
	}
	_, err := api.SetBucketProps(baseParams, bck, bckPropsToSet)
	tassert.CheckFatal(t, err)
	defer resetBucketProps(t, proxyURL, bck)

	p, err := api.HeadBucket(baseParams, bck, true /* don't add */)
	tassert.CheckFatal(t, err)
	validateBucketProps(t, bckPropsToSet, p)
}

func TestHeadNonexistentBucket(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	bucket, err := tools.GenerateNonexistentBucketName("head", baseParams)
	tassert.CheckFatal(t, err)

	bck := cmn.Bck{
		Name:     bucket,
		Provider: apc.AIS,
	}

	_, err = api.HeadBucket(baseParams, bck, true /* don't add */)
	if err == nil {
		t.Fatalf("Expected an error, but go no errors.")
	}

	status := api.HTTPStatus(err)
	if status != http.StatusNotFound {
		t.Errorf("Expected status %d, got %d", http.StatusNotFound, status)
	}
}

// 1.	PUT file
// 2.	Change contents of the file or change XXHash
// 3.	GET file.
// NOTE: The following test can only work when running on a local setup
// (targets are co-located with where this test is running from, because
// it searches a local oldFileIfo system).
func TestChecksumValidateOnWarmGetForRemoteBucket(t *testing.T) {
	var (
		m = ioContext{
			t:        t,
			bck:      cliBck,
			num:      3,
			fileSize: cos.KiB,
			prefix:   "cksum/obj-",
		}

		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	m.init(true /*cleanup*/)

	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: m.bck})
	if docker.IsRunning() {
		t.Skipf("test %q requires xattrs to be set, doesn't work with docker", t.Name())
	}

	p, err := api.HeadBucket(baseParams, m.bck, false /* don't add */)
	tassert.CheckFatal(t, err)

	_ = mock.NewTarget(mock.NewBaseBownerMock(
		meta.NewBck(
			m.bck.Name, m.bck.Provider, cmn.NsGlobal,
			&cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumXXHash}, Extra: p.Extra, BID: 0xa73b9f11},
		),
	))

	initMountpaths(t, proxyURL)

	m.puts()

	t.Cleanup(func() {
		propsToSet := &cmn.BpropsToSet{
			Cksum: &cmn.CksumConfToSet{
				Type:            apc.Ptr(p.Cksum.Type),
				ValidateWarmGet: apc.Ptr(p.Cksum.ValidateWarmGet),
			},
		}
		_, err = api.SetBucketProps(baseParams, m.bck, propsToSet)
		tassert.CheckError(t, err)
		m.del()
	})

	objName := m.nextObjName()
	_, err = api.GetObjectWithValidation(baseParams, m.bck, objName, nil)
	tassert.CheckError(t, err)

	if !p.Cksum.ValidateWarmGet {
		propsToSet := &cmn.BpropsToSet{
			Cksum: &cmn.CksumConfToSet{
				ValidateWarmGet: apc.Ptr(true),
			},
		}
		_, err = api.SetBucketProps(baseParams, m.bck, propsToSet)
		tassert.CheckFatal(t, err)
	}

	fqn := findObjOnDisk(m.bck, objName)
	tools.CheckPathExists(t, fqn, false /*dir*/)
	oldFileInfo, _ := os.Stat(fqn)

	// Test when the contents of the file are changed
	tlog.Logf("Changing contents of the file [%s]: %s\n", objName, fqn)
	err = os.WriteFile(fqn, []byte("Contents of this file have been changed."), cos.PermRWR)
	tassert.CheckFatal(t, err)
	validateGETUponFileChangeForChecksumValidation(t, proxyURL, objName, fqn, oldFileInfo)

	// Test when the xxHash of the file is changed
	objName = m.nextObjName()
	fqn = findObjOnDisk(m.bck, objName)
	tools.CheckPathExists(t, fqn, false /*dir*/)
	oldFileInfo, _ = os.Stat(fqn)

	tlog.Logf("Changing file xattr[%s]: %s\n", objName, fqn)
	err = tools.SetXattrCksum(fqn, m.bck, cos.NewCksum(cos.ChecksumXXHash, "01234"))
	tassert.CheckError(t, err)
	validateGETUponFileChangeForChecksumValidation(t, proxyURL, objName, fqn, oldFileInfo)

	// Test for no checksum algo
	objName = m.nextObjName()
	fqn = findObjOnDisk(m.bck, objName)

	if p.Cksum.Type != cos.ChecksumNone {
		propsToSet := &cmn.BpropsToSet{
			Cksum: &cmn.CksumConfToSet{
				Type: apc.Ptr(cos.ChecksumNone),
			},
		}
		_, err = api.SetBucketProps(baseParams, m.bck, propsToSet)
		tassert.CheckFatal(t, err)
	}

	tlog.Logf("Changing file xattr[%s]: %s\n", objName, fqn)
	err = tools.SetXattrCksum(fqn, m.bck, cos.NewCksum(cos.ChecksumXXHash, "01234abcde"))
	tassert.CheckError(t, err)

	_, err = api.GetObject(baseParams, m.bck, objName, nil)
	tassert.Errorf(t, err == nil, "A GET on an object when checksum algo is none should pass. Error: %v", err)
}

func TestValidateOnWarmGetRemoteBucket(t *testing.T) {
	var (
		m = ioContext{
			t:        t,
			bck:      cliBck,
			num:      10,
			fileSize: cos.KiB,
			prefix:   "validate/obj-",
		}

		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	m.init(true /*cleanup*/)

	tools.CheckSkip(t, &tools.SkipTestArgs{CloudBck: true, Bck: m.bck})
	if docker.IsRunning() {
		t.Skipf("test %q requires xattrs to be set, doesn't work with docker", t.Name())
	}

	p, err := api.HeadBucket(baseParams, m.bck, false /*don't add*/)
	tassert.CheckFatal(t, err)

	tMock := mock.NewTarget(mock.NewBaseBownerMock(
		meta.NewBck(
			m.bck.Name, m.bck.Provider, cmn.NsGlobal,
			&cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumXXHash}, Extra: p.Extra, BID: 0xa73b9f11},
		),
	))

	// add mock backend
	var mockBackend core.Backend
	switch m.bck.Provider {
	case apc.AWS:
		mockBackend, _ = backend.NewAWS(tMock, mock.NewStatsTracker())
	case apc.GCP:
		mockBackend, _ = backend.NewGCP(tMock, mock.NewStatsTracker())
	case apc.Azure:
		mockBackend, _ = backend.NewAzure(tMock, mock.NewStatsTracker())
	default:
		t.Fatalf("unexpected backend provider %q", m.bck.Provider)
	}
	tMock.Backends = map[string]core.Backend{m.bck.Provider: mockBackend}

	initMountpaths(t, proxyURL)

	m.puts()

	if !p.Cksum.ValidateWarmGet {
		propsToSet := &cmn.BpropsToSet{
			Versioning: &cmn.VersionConfToSet{ValidateWarmGet: apc.Ptr(true)},
			Cksum: &cmn.CksumConfToSet{
				Type:            apc.Ptr(cos.ChecksumXXHash),
				ValidateWarmGet: apc.Ptr(true),
			},
		}
		_, err = api.SetBucketProps(baseParams, m.bck, propsToSet)
		tassert.CheckFatal(t, err)
	}
	t.Cleanup(func() {
		propsToSet := &cmn.BpropsToSet{
			Versioning: &cmn.VersionConfToSet{
				ValidateWarmGet: apc.Ptr(p.Versioning.ValidateWarmGet),
			},
			Cksum: &cmn.CksumConfToSet{
				Type:            apc.Ptr(p.Cksum.Type),
				ValidateWarmGet: apc.Ptr(p.Cksum.ValidateWarmGet),
			},
		}
		_, err = api.SetBucketProps(baseParams, m.bck, propsToSet)
		tassert.CheckError(t, err)
		m.del()
	})

	// FIXME: We should also have tests for `ColdGetEnabled`.

	t.Run("ColdGetDisabled", func(t *testing.T) {
		propsToSet := &cmn.BpropsToSet{Features: apc.Ptr(feat.DisableColdGET)}
		_, err = api.SetBucketProps(baseParams, m.bck, propsToSet)
		tassert.CheckFatal(t, err)
		t.Cleanup(func() {
			propsToSet := &cmn.BpropsToSet{Features: apc.Ptr(p.Features)}
			_, err = api.SetBucketProps(baseParams, m.bck, propsToSet)
			tassert.CheckError(t, err)
		})

		for name, test := range map[string]struct {
			beforeTest func(*testing.T)
			modify     func(*testing.T, *core.LOM)
		}{
			"checksum": {modify: func(_ *testing.T, lom *core.LOM) {
				lom.SetCksum(cos.NewCksum(cos.ChecksumXXHash, "01234"))
			}},
			"local_content": {modify: func(t *testing.T, lom *core.LOM) {
				err := os.WriteFile(lom.FQN, []byte("modified"), cos.PermRWR)
				tassert.CheckFatal(t, err)
			}},
			"remote_content": {
				modify: func(t *testing.T, lom *core.LOM) {
					if tMock.Backend(lom.Bck()) == nil {
						t.Skipf(`Skipping because tests are not built with '-tags="%s"' option`, lom.Bck().Provider)
					}

					r := io.NopCloser(bytes.NewReader([]byte("modified")))
					_, err := tMock.Backend(lom.Bck()).PutObj(r, lom, nil)
					tassert.CheckFatal(t, err)
				},
			},
			"remote_version": {
				beforeTest: func(t *testing.T) {
					if !p.Versioning.Enabled {
						t.Skip("Validation is not enabled for this bucket")
					}
				},
				modify: func(t *testing.T, lom *core.LOM) {
					// We override remote object with the same content - it should
					// bump the version without changing the other metadata.
					data, err := os.ReadFile(lom.FQN)
					tassert.CheckFatal(t, err)
					r := io.NopCloser(bytes.NewReader(data))
					backend := tMock.Backend(lom.Bck())

					if backend == nil {
						t.Skipf("Warning: mock backend is nil, most likely reason: missing build tag\n"+
							"(try running this test with -tags=%q)\n\n", "debug,aws,gcp,azure")
					}

					_, err = backend.PutObj(r, lom, nil)
					tassert.CheckFatal(t, err)
				},
			},
		} {
			t.Run(name, func(t *testing.T) {
				if test.beforeTest != nil {
					test.beforeTest(t)
				}

				objName := m.nextObjName()

				// Modify local version.
				fqn := findObjOnDisk(m.bck, objName)
				tools.ModifyLOM(t, fqn, m.bck, test.modify)

				// Make sure that object is not returned.
				_, err = api.GetObjectWithValidation(baseParams, m.bck, objName, nil)
				tassert.Fatalf(t, err != nil, "Expected error to occur")
			})
		}
	})
}

func TestEvictRemoteBucket(t *testing.T) {
	t.Run("Cloud/KeepMD", func(t *testing.T) { testEvictRemoteBucket(t, cliBck, true) })
	t.Run("Cloud/DeleteMD", func(t *testing.T) { testEvictRemoteBucket(t, cliBck, false) })
	t.Run("RemoteAIS", testEvictRemoteAISBucket)
}

func testEvictRemoteAISBucket(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiresRemoteCluster: true})
	bck := cmn.Bck{
		Name:     trand.String(10),
		Provider: apc.AIS,
		Ns: cmn.Ns{
			UUID: tools.RemoteCluster.UUID,
		},
	}
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	testEvictRemoteBucket(t, bck, false)
}

func testEvictRemoteBucket(t *testing.T, bck cmn.Bck, keepMD bool) {
	var (
		m = ioContext{
			t:        t,
			bck:      bck,
			num:      10,
			fileSize: largeFileSize,
			prefix:   "evict/obj-",
		}
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: m.bck})
	m.initAndSaveState(true /*cleanup*/)

	t.Cleanup(func() {
		m.del()
		resetBucketProps(t, proxyURL, m.bck)
	})

	m.remotePuts(false /*evict*/)

	// Test property, mirror is disabled for cloud bucket that hasn't been accessed,
	// even if system config says otherwise
	_, err := api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{
		Mirror: &cmn.MirrorConfToSet{Enabled: apc.Ptr(true)},
	})
	tassert.CheckFatal(t, err)

	bProps, err := api.HeadBucket(baseParams, m.bck, true /* don't add */)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, bProps.Mirror.Enabled, "test property hasn't changed")

	// Wait for async mirroring to finish
	flt := xact.ArgsMsg{Kind: apc.ActMakeNCopies, Bck: m.bck}
	api.WaitForXactionIdle(baseParams, &flt)
	time.Sleep(time.Second)

	err = api.EvictRemoteBucket(baseParams, m.bck, keepMD)
	tassert.CheckFatal(t, err)

	for _, objName := range m.objNames {
		exists := tools.CheckObjIsPresent(proxyURL, m.bck, objName)
		tassert.Errorf(t, !exists, "object remains cached: %s", objName)
	}
	bProps, err = api.HeadBucket(baseParams, m.bck, true /* don't add */)
	tassert.CheckFatal(t, err)
	if keepMD {
		tassert.Fatalf(t, bProps.Mirror.Enabled, "test property was reset")
	} else {
		tassert.Fatalf(t, !bProps.Mirror.Enabled, "test property not reset")
	}
}

func validateGETUponFileChangeForChecksumValidation(t *testing.T, proxyURL, objName, fqn string,
	oldFileInfo os.FileInfo) {
	// Do a GET to see to check if a cold get was executed by comparing old and new size
	var (
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cliBck
	)

	_, err := api.GetObjectWithValidation(baseParams, bck, objName, nil)
	tassert.CheckError(t, err)

	tools.CheckPathExists(t, fqn, false /*dir*/)
	newFileInfo, _ := os.Stat(fqn)
	if newFileInfo.Size() != oldFileInfo.Size() {
		t.Errorf("Expected size: %d, Actual Size: %d", oldFileInfo.Size(), newFileInfo.Size())
	}
}

//  1. PUT file
//  2. Change contents of the file or change XXHash
//  3. GET file (first GET should fail with Internal Server Error and the
//     second should fail with not found).
//
// Note: The following test can only work when running on a local setup
// (targets are co-located with where this test is running from, because
// it searches a local file system)
func TestChecksumValidateOnWarmGetForBucket(t *testing.T) {
	var (
		m = ioContext{
			t: t,
			bck: cmn.Bck{
				Provider: apc.AIS,
				Name:     trand.String(15),
				Props:    &cmn.Bprops{BID: 2},
			},
			num:      3,
			fileSize: cos.KiB,
		}

		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		_          = mock.NewTarget(mock.NewBaseBownerMock(
			meta.NewBck(
				m.bck.Name, apc.AIS, cmn.NsGlobal,
				&cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumXXHash}, BID: 1},
			),
			meta.CloneBck(&m.bck),
		))
		cksumConf = m.bck.DefaultProps(initialClusterConfig).Cksum
	)

	m.init(true /*cleanup*/)

	if docker.IsRunning() {
		t.Skipf("test %q requires write access to xattrs, doesn't work with docker", t.Name())
	}

	initMountpaths(t, proxyURL)

	tools.CreateBucket(t, proxyURL, m.bck, nil, true /*cleanup*/)

	m.puts()

	if !cksumConf.ValidateWarmGet {
		propsToSet := &cmn.BpropsToSet{
			Cksum: &cmn.CksumConfToSet{
				ValidateWarmGet: apc.Ptr(true),
			},
		}
		_, err := api.SetBucketProps(baseParams, m.bck, propsToSet)
		tassert.CheckFatal(t, err)
	}

	// Test changing the file content.
	objName := m.objNames[0]
	fqn := findObjOnDisk(m.bck, objName)
	tlog.Logf("Changing contents of the file [%s]: %s\n", objName, fqn)
	err := os.WriteFile(fqn, []byte("Contents of this file have been changed."), cos.PermRWR)
	tassert.CheckFatal(t, err)
	executeTwoGETsForChecksumValidation(proxyURL, m.bck, objName, t)

	// Test changing the file xattr.
	objName = m.objNames[1]
	fqn = findObjOnDisk(m.bck, objName)
	tlog.Logf("Changing file xattr[%s]: %s\n", objName, fqn)
	err = tools.SetXattrCksum(fqn, m.bck, cos.NewCksum(cos.ChecksumXXHash, "01234abcde"))
	tassert.CheckError(t, err)
	executeTwoGETsForChecksumValidation(proxyURL, m.bck, objName, t)

	// Test for none checksum algorithm.
	objName = m.objNames[2]
	fqn = findObjOnDisk(m.bck, objName)

	if cksumConf.Type != cos.ChecksumNone {
		propsToSet := &cmn.BpropsToSet{
			Cksum: &cmn.CksumConfToSet{
				Type: apc.Ptr(cos.ChecksumNone),
			},
		}
		_, err = api.SetBucketProps(baseParams, m.bck, propsToSet)
		tassert.CheckFatal(t, err)
	}

	tlog.Logf("Changing file xattr[%s]: %s\n", objName, fqn)
	err = tools.SetXattrCksum(fqn, m.bck, cos.NewCksum(cos.ChecksumXXHash, "01234abcde"))
	tassert.CheckError(t, err)
	_, err = api.GetObject(baseParams, m.bck, objName, nil)
	tassert.CheckError(t, err)
}

func executeTwoGETsForChecksumValidation(proxyURL string, bck cmn.Bck, objName string, t *testing.T) {
	baseParams := tools.BaseAPIParams(proxyURL)
	_, err := api.GetObjectWithValidation(baseParams, bck, objName, nil)
	if err == nil {
		t.Error("Error is nil, expected internal server error on a GET for an object")
	} else if !strings.Contains(err.Error(), "ErrBadCksum") {
		t.Errorf("Expected bad checksum error on a GET for a corrupted object, got [%v]", err)
	}
	// Execute another GET to make sure that the object is deleted
	_, err = api.GetObjectWithValidation(baseParams, bck, objName, nil)
	if err == nil {
		t.Error("Error is nil, expected not found on a second GET for a corrupted object")
	} else if !strings.Contains(err.Error(), "ErrNotFound") {
		t.Errorf("Expected Not Found on a second GET for a corrupted object, got [%v]", err)
	}
}

// TODO: validate range checksums
func TestRangeRead(t *testing.T) {
	initMountpaths(t, tools.RandomProxyURL(t)) // to run findObjOnDisk() and validate range

	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		var (
			m = ioContext{
				t:         t,
				bck:       bck.Clone(),
				num:       5,
				fileSize:  5271,
				fixedSize: true,
			}
			proxyURL   = tools.RandomProxyURL(t)
			baseParams = tools.BaseAPIParams(proxyURL)
			cksumProps = bck.CksumConf()
		)

		m.init(true /*cleanup*/)
		m.puts()
		if m.bck.IsRemote() {
			defer m.del()
		}
		objName := m.objNames[0]

		defer func() {
			tlog.Logln("Cleaning up...")
			propsToSet := &cmn.BpropsToSet{
				Cksum: &cmn.CksumConfToSet{
					EnableReadRange: apc.Ptr(cksumProps.EnableReadRange),
				},
			}
			_, err := api.SetBucketProps(baseParams, m.bck, propsToSet)
			tassert.CheckError(t, err)
		}()

		tlog.Logln("Valid range with object checksum...")
		// Validate that entire object checksum is being returned
		if cksumProps.EnableReadRange {
			propsToSet := &cmn.BpropsToSet{
				Cksum: &cmn.CksumConfToSet{
					EnableReadRange: apc.Ptr(false),
				},
			}
			_, err := api.SetBucketProps(baseParams, m.bck, propsToSet)
			tassert.CheckFatal(t, err)
		}
		testValidCases(t, proxyURL, bck.Clone(), cksumProps.Type, m.fileSize, objName, true)

		// Validate only that range checksum is being returned
		tlog.Logln("Valid range with range checksum...")
		if !cksumProps.EnableReadRange {
			propsToSet := &cmn.BpropsToSet{
				Cksum: &cmn.CksumConfToSet{
					EnableReadRange: apc.Ptr(true),
				},
			}
			_, err := api.SetBucketProps(baseParams, bck.Clone(), propsToSet)
			tassert.CheckFatal(t, err)
		}
		testValidCases(t, proxyURL, m.bck, cksumProps.Type, m.fileSize, objName, false)

		tlog.Logln("Valid range query...")
		verifyValidRangesQuery(t, proxyURL, m.bck, objName, "bytes=-1", 1)
		verifyValidRangesQuery(t, proxyURL, m.bck, objName, "bytes=0-", int64(m.fileSize))
		verifyValidRangesQuery(t, proxyURL, m.bck, objName, "bytes=10-", int64(m.fileSize-10))
		verifyValidRangesQuery(t, proxyURL, m.bck, objName, fmt.Sprintf("bytes=-%d", m.fileSize), int64(m.fileSize))
		verifyValidRangesQuery(t, proxyURL, m.bck, objName, fmt.Sprintf("bytes=-%d", m.fileSize+2), int64(m.fileSize))

		tlog.Logln("======================================================================")
		tlog.Logln("Invalid range query:")
		verifyInvalidRangesQuery(t, proxyURL, m.bck, objName, "potatoes=0-1")
		verifyInvalidRangesQuery(t, proxyURL, m.bck, objName, "bytes")
		verifyInvalidRangesQuery(t, proxyURL, m.bck, objName, fmt.Sprintf("bytes=%d-", m.fileSize+1))
		verifyInvalidRangesQuery(t, proxyURL, m.bck, objName, fmt.Sprintf("bytes=%d-%d", m.fileSize+1, m.fileSize+2))
		verifyInvalidRangesQuery(t, proxyURL, m.bck, objName, "bytes=0--1")
		verifyInvalidRangesQuery(t, proxyURL, m.bck, objName, "bytes=1-0")
		verifyInvalidRangesQuery(t, proxyURL, m.bck, objName, "bytes=-1-0")
		verifyInvalidRangesQuery(t, proxyURL, m.bck, objName, "bytes=-1-2")
		verifyInvalidRangesQuery(t, proxyURL, m.bck, objName, "bytes=10--1")
		verifyInvalidRangesQuery(t, proxyURL, m.bck, objName, "bytes=1-2,4-6")
		verifyInvalidRangesQuery(t, proxyURL, m.bck, objName, "bytes=--1")
	})
}

// TODO: validate range checksum if enabled
func testValidCases(t *testing.T, proxyURL string, bck cmn.Bck, cksumType string, fileSize uint64, objName string,
	checkEntireObjCksum bool) {
	// Range-Read the entire file in 500 increments
	byteRange := int64(500)
	iterations := int64(fileSize) / byteRange
	for i := int64(0); i < iterations; i += byteRange {
		verifyValidRanges(t, proxyURL, bck, cksumType, objName, i, byteRange, byteRange, checkEntireObjCksum)
	}

	verifyValidRanges(t, proxyURL, bck, cksumType, objName, byteRange*iterations, byteRange,
		int64(fileSize)%byteRange, checkEntireObjCksum)
}

func verifyValidRanges(t *testing.T, proxyURL string, bck cmn.Bck, cksumType, objName string,
	offset, length, expectedLength int64, checkEntireObjCksum bool) {
	var (
		w          = bytes.NewBuffer(nil)
		rng        = cmn.MakeRangeHdr(offset, length)
		hdr        = http.Header{cos.HdrRange: []string{rng}}
		baseParams = tools.BaseAPIParams(proxyURL)
		fqn        = findObjOnDisk(bck, objName)
		args       = api.GetArgs{Writer: w, Header: hdr}
	)

	oah, err := api.GetObjectWithValidation(baseParams, bck, objName, &args)
	if err != nil {
		if !checkEntireObjCksum {
			t.Errorf("Failed to GET %s: %v", bck.Cname(objName), err)
		} else {
			if ckErr, ok := err.(*cmn.ErrInvalidCksum); ok {
				file, err := os.Open(fqn)
				if err != nil {
					t.Fatalf("Unable to open file: %s. Error:  %v", fqn, err)
				}
				defer file.Close()
				_, cksum, err := cos.CopyAndChecksum(io.Discard, file, nil, cksumType)
				if err != nil {
					t.Errorf("Unable to compute cksum of file: %s. Error:  %s", fqn, err)
				}
				if cksum.Value() != ckErr.Expected() {
					t.Errorf("Expected entire object checksum [%s], checksum returned in response [%s]",
						ckErr.Expected(), cksum)
				}
			} else {
				t.Errorf("Unexpected error returned [%v].", err)
			}
		}
	} else if oah.Size() != expectedLength {
		t.Errorf("number of bytes received (%d) is different from expected (%d)", oah.Size(), expectedLength)
	}

	file, err := os.Open(fqn)
	if err != nil {
		t.Fatalf("Unable to open file: %s. Error:  %v", fqn, err)
	}
	defer file.Close()
	outputBytes := w.Bytes()
	sectionReader := io.NewSectionReader(file, offset, length)
	expectedBytesBuffer := bytes.NewBuffer(nil)
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
	for i := range expectedBytes {
		if expectedBytes[i] != outputBytes[i] {
			t.Errorf("Byte mismatch. Expected: %v, Actual: %v", string(expectedBytes), string(outputBytes))
		}
	}
}

func verifyValidRangesQuery(t *testing.T, proxyURL string, bck cmn.Bck, objName, rangeQuery string, expectedLength int64) {
	var (
		baseParams = tools.BaseAPIParams(proxyURL)
		hdr        = http.Header{cos.HdrRange: {rangeQuery}}
		args       = api.GetArgs{Header: hdr}
	)
	oah, err := api.GetObject(baseParams, bck, objName, &args)
	tassert.CheckFatal(t, err)

	tlog.Logf("rangeQuery=%s, n=%d\n", rangeQuery, oah.Size()) // DEBUG

	// check size
	tassert.Errorf(t, oah.Size() == expectedLength, "expected range length %d, got %d",
		expectedLength, oah.Size())

	// check read range response headers
	respHeader := oah.RespHeader()
	acceptRanges := respHeader.Get(cos.HdrAcceptRanges)
	tassert.Errorf(t, acceptRanges == "bytes", "%q header is not set correctly: %s",
		cos.HdrAcceptRanges, acceptRanges)
	contentRange := respHeader.Get(cos.HdrContentRange)
	tassert.Errorf(t, contentRange != "", "%q header should be set", cos.HdrContentRange)
}

func verifyInvalidRangesQuery(t *testing.T, proxyURL string, bck cmn.Bck, objName, rangeQuery string) {
	var (
		baseParams = tools.BaseAPIParams(proxyURL)
		hdr        = http.Header{cos.HdrRange: {rangeQuery}}
		args       = api.GetArgs{Header: hdr}
	)
	_, err := api.GetObjectWithValidation(baseParams, bck, objName, &args)
	tassert.Errorf(t, err != nil, "must fail for %q combination", rangeQuery)
}

func Test_checksum(t *testing.T) {
	var (
		m = ioContext{
			t:        t,
			bck:      cliBck,
			num:      5,
			fileSize: largeFileSize,
		}
		totalSize  = int64(uint64(m.num) * m.fileSize)
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true, RemoteBck: true, Bck: m.bck})

	p, err := api.HeadBucket(baseParams, m.bck, false /* don't add */)
	tassert.CheckFatal(t, err)

	t.Cleanup(func() {
		propsToSet := &cmn.BpropsToSet{
			Cksum: &cmn.CksumConfToSet{
				Type:            apc.Ptr(p.Cksum.Type),
				ValidateColdGet: apc.Ptr(p.Cksum.ValidateColdGet),
			},
		}
		_, err = api.SetBucketProps(baseParams, m.bck, propsToSet)
		tassert.CheckError(t, err)
		m.del()
	})

	m.remotePuts(true /*evict*/)

	// Disable checkum.
	if p.Cksum.Type != cos.ChecksumNone {
		propsToSet := &cmn.BpropsToSet{
			Cksum: &cmn.CksumConfToSet{
				Type: apc.Ptr(cos.ChecksumNone),
			},
		}
		_, err = api.SetBucketProps(baseParams, m.bck, propsToSet)
		tassert.CheckFatal(t, err)
	}

	// Disable cold get validation.
	if p.Cksum.ValidateColdGet {
		propsToSet := &cmn.BpropsToSet{
			Cksum: &cmn.CksumConfToSet{
				ValidateColdGet: apc.Ptr(false),
			},
		}
		_, err = api.SetBucketProps(baseParams, m.bck, propsToSet)
		tassert.CheckFatal(t, err)
	}

	start := time.Now()
	m.gets(nil /*api.GetArgs*/, false /*withValidate*/)
	tlog.Logf("GET %s without any checksum validation: %v\n", cos.ToSizeIEC(totalSize, 0), time.Since(start))

	m.evict()

	propsToSet := &cmn.BpropsToSet{
		Cksum: &cmn.CksumConfToSet{
			Type:            apc.Ptr(cos.ChecksumXXHash),
			ValidateColdGet: apc.Ptr(true),
		},
	}
	_, err = api.SetBucketProps(baseParams, m.bck, propsToSet)
	tassert.CheckFatal(t, err)

	start = time.Now()
	m.gets(nil /*api.GetArgs*/, true /*withValidate*/)
	tlog.Logf("GET %s and validate checksum: %v\n", cos.ToSizeIEC(totalSize, 0), time.Since(start))
}

func validateBucketProps(t *testing.T, expected *cmn.BpropsToSet, actual *cmn.Bprops) {
	// Apply changes on props that we have received. If after applying anything
	// has changed it means that the props were not applied.
	tmpProps := actual.Clone()
	tmpProps.Apply(expected)
	if !reflect.DeepEqual(tmpProps, actual) {
		t.Errorf("bucket props are not equal, expected: %+v, got: %+v", tmpProps, actual)
	}
}

func resetBucketProps(t *testing.T, proxyURL string, bck cmn.Bck) {
	baseParams := tools.BaseAPIParams(proxyURL)
	_, err := api.ResetBucketProps(baseParams, bck)
	tassert.CheckError(t, err)
}

func corruptSingleBitInFile(t *testing.T, bck cmn.Bck, objName string) {
	var (
		fqn     = findObjOnDisk(bck, objName)
		fi, err = os.Stat(fqn)
		b       = []byte{0}
	)
	tassert.CheckFatal(t, err)
	off := rand.Int64N(fi.Size())
	file, err := os.OpenFile(fqn, os.O_RDWR, cos.PermRWR)
	tassert.CheckFatal(t, err)
	_, err = file.Seek(off, 0)
	tassert.CheckFatal(t, err)
	_, err = file.Read(b)
	tassert.CheckFatal(t, err)
	bit := rand.IntN(8)
	b[0] ^= 1 << bit
	_, err = file.Seek(off, 0)
	tassert.CheckFatal(t, err)
	_, err = file.Write(b)
	tassert.CheckFatal(t, err)
	file.Close()
}

func TestPutObjectWithChecksum(t *testing.T) {
	var (
		proxyURL     = tools.RandomProxyURL(t)
		baseParams   = tools.BaseAPIParams(proxyURL)
		bck          = cliBck
		basefileName = "mytestobj.txt"
		objData      = []byte("I am object data")
		badCksumVal  = "badchecksum"
	)

	if bck.IsAIS() {
		tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	} else {
		t.Cleanup(func() {
			m := ioContext{
				t:   t,
				bck: bck,
			}
			m.del(-1 /*delete all*/, 0 /* lsmsg.Flags */)
		})
	}

	bprops, err := api.HeadBucket(baseParams, bck, false /* don't add */)
	tassert.CheckFatal(t, err)

	// Enable Cold Get Validation
	if !bprops.Cksum.ValidateColdGet {
		propsToSet := &cmn.BpropsToSet{
			Cksum: &cmn.CksumConfToSet{
				ValidateColdGet: apc.Ptr(true),
			},
		}
		_, err = api.SetBucketProps(baseParams, bck, propsToSet)
		tassert.CheckFatal(t, err)

		t.Cleanup(func() {
			propsToSet.Cksum.ValidateColdGet = apc.Ptr(false)
			_, err = api.SetBucketProps(baseParams, bck, propsToSet)
			tassert.CheckError(t, err)
		})
	}

	putArgs := api.PutArgs{
		BaseParams: baseParams,
		Bck:        bck,
		Reader:     readers.NewBytes(objData),
	}
	for _, cksumType := range cos.SupportedChecksums() {
		if cksumType == cos.ChecksumNone {
			continue
		}
		fileName := basefileName + cksumType
		hasher := cos.NewCksumHash(cksumType)
		hasher.H.Write(objData)
		cksumValue := hex.EncodeToString(hasher.H.Sum(nil))
		putArgs.Cksum = cos.NewCksum(cksumType, badCksumVal)
		putArgs.ObjName = fileName

		_, err := api.PutObject(&putArgs)
		if err == nil {
			t.Errorf("Bad checksum provided by the user, Expected an error")
		}

		_, err = api.HeadObject(baseParams, bck, fileName, api.HeadArgs{FltPresence: apc.FltExists})
		if err == nil || !strings.Contains(err.Error(), strconv.Itoa(http.StatusNotFound)) {
			t.Errorf("Object %s exists despite bad checksum", fileName)
		}
		putArgs.Cksum = cos.NewCksum(cksumType, cksumValue)
		oah, err := api.PutObject(&putArgs)
		if err != nil {
			t.Errorf("Correct checksum provided, Err encountered %v", err)
		}
		op, err := api.HeadObject(baseParams, bck, fileName, api.HeadArgs{FltPresence: apc.FltPresent})
		if err != nil {
			t.Errorf("Object %s does not exist despite correct checksum", fileName)
		}
		attrs1 := oah.Attrs()
		attrs2 := op.ObjAttrs
		tassert.Errorf(t, attrs1.CheckEq(&attrs2) == nil, "PUT(obj) attrs %s != %s HEAD\n", attrs1.String(), attrs2.String())
	}
}

func TestOperationsWithRanges(t *testing.T) {
	const (
		objCnt  = 50 // NOTE: must by a multiple of 10
		objSize = cos.KiB
	)
	proxyURL := tools.RandomProxyURL(t)

	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		for _, evict := range []bool{false, true} {
			t.Run(fmt.Sprintf("evict=%t", evict), func(t *testing.T) {
				if evict && bck.IsAIS() {
					t.Skip("operation `evict` is not applicable to AIS buckets")
				}

				var (
					objList   = make([]string, 0, objCnt)
					cksumType = bck.Props.Cksum.Type
				)

				for i := range objCnt / 2 {
					objList = append(objList,
						fmt.Sprintf("test/a-%04d", i),
						fmt.Sprintf("test/b-%04d", i),
					)
				}
				for _, objName := range objList {
					r, _ := readers.NewRand(objSize, cksumType)
					_, err := api.PutObject(&api.PutArgs{
						BaseParams: baseParams,
						Bck:        bck.Clone(),
						ObjName:    objName,
						Reader:     r,
						Size:       objSize,
					})
					tassert.CheckFatal(t, err)
				}

				tests := []struct {
					// Title to print out while testing.
					name string
					// A range of objects.
					rangeStr string
					// Total number of objects expected to be deleted/evicted.
					delta int
				}{
					{
						"Trying to delete/evict objects with invalid prefix",
						"file/a-{0..10}",
						0,
					},
					{
						"Trying to delete/evict objects out of range",
						"test/a-" + fmt.Sprintf("{%d..%d}", objCnt+10, objCnt+110),
						0,
					},
					{
						fmt.Sprintf("Deleting/Evicting %d objects with prefix 'a-'", objCnt/10),
						"test/a-" + fmt.Sprintf("{%04d..%04d}", (objCnt-objCnt/5)/2, objCnt/2),
						objCnt / 10,
					},
					{
						fmt.Sprintf("Deleting/Evicting %d objects (short range)", objCnt/5),
						"test/b-" + fmt.Sprintf("{%04d..%04d}", 1, objCnt/5),
						objCnt / 5,
					},
					{
						"Deleting/Evicting objects with empty range",
						"test/b-",
						objCnt/2 - objCnt/5,
					},
				}

				var (
					totalFiles  = objCnt
					baseParams  = tools.BaseAPIParams(proxyURL)
					waitTimeout = 10 * time.Second
					b           = bck.Clone()
				)

				if bck.IsRemote() {
					waitTimeout = 40 * time.Second
				}

				for idx, test := range tests {
					tlog.Logf("%d. %s; range: [%s]\n", idx+1, test.name, test.rangeStr)

					var (
						err  error
						xid  string
						kind string
						msg  = &apc.LsoMsg{Prefix: "test/"}
					)
					if evict {
						xid, err = api.EvictMultiObj(baseParams, b, nil /*lst objnames*/, test.rangeStr)
						msg.Flags = apc.LsObjCached
						kind = apc.ActEvictObjects
					} else {
						xid, err = api.DeleteMultiObj(baseParams, b, nil /*lst objnames*/, test.rangeStr)
						kind = apc.ActDeleteObjects
					}
					if err != nil {
						t.Error(err)
						continue
					}

					args := xact.ArgsMsg{ID: xid, Kind: kind, Timeout: waitTimeout}
					_, err = api.WaitForXactionIC(baseParams, &args)
					tassert.CheckFatal(t, err)

					totalFiles -= test.delta
					objList, err := api.ListObjects(baseParams, b, msg, api.ListArgs{})
					if err != nil {
						t.Error(err)
						continue
					}
					if len(objList.Entries) != totalFiles {
						t.Errorf("Incorrect number of remaining objects: %d, should be %d",
							len(objList.Entries), totalFiles)
						continue
					}

					tlog.Logf("  %d objects have been deleted/evicted\n", test.delta)
				}

				msg := &apc.LsoMsg{Prefix: "test/"}
				lst, err := api.ListObjects(baseParams, b, msg, api.ListArgs{})
				tassert.CheckFatal(t, err)

				tlog.Logf("Cleaning up remaining objects...\n")
				for _, obj := range lst.Entries {
					err := tools.Del(proxyURL, b, obj.Name, nil, nil, true /*silent*/)
					tassert.CheckError(t, err)
				}
			})
		}
	})
}
