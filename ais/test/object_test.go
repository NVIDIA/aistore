// Package integration_test.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/NVIDIA/aistore/ais/backend"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/sys"
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
				reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: cos.KiB, CksumType: cos.ChecksumNone})
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

			reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: cos.KiB, CksumType: cos.ChecksumNone})
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
					tlog.Logfln("PUT(%s) attrs %s", bck.Cname(object), oa.String())
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
					t.Errorf("expected error when doing %s on non existing %q bucket", test.ty, bck.String())
				}
			} else if err != nil {
				t.Errorf("expected no error when executing %s on existing %q bucket(err = %v)",
					test.ty, bck.String(), err)
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
		httpObjectName, httpAnotherObjectName, hbo.Bck.String())
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
					Reader:     cos.NewByteReader([]byte(body)),
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
			if !cos.NoneH(cksum) {
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

func TestCopyObject(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bckFrom    = cmn.Bck{
			Name:     "obj-cp-from-" + trand.String(5),
			Provider: apc.AIS,
		}
		bckTo = cmn.Bck{
			Name:     "obj-cp-to-" + trand.String(5),
			Provider: apc.AIS,
		}
		objFrom = "test-from-obj"
		objTo   = "test-to-obj"
		data    = []byte("this is test data for copy object")
	)

	// Create bucket and PUT source object
	tools.CreateBucket(t, proxyURL, bckFrom, nil, true /*cleanup*/)
	tools.CreateBucket(t, proxyURL, bckTo, nil, true /*cleanup*/)
	putArgs := api.PutArgs{
		BaseParams: baseParams,
		Bck:        bckFrom,
		ObjName:    objFrom,
		Reader:     readers.NewBytes(data),
	}
	_, err := api.PutObject(&putArgs)
	tassert.CheckFatal(t, err)

	err = api.CopyObject(baseParams, &api.CopyArgs{
		FromBck:     bckFrom,
		FromObjName: objFrom,
		ToBck:       bckTo,
		ToObjName:   objTo,
	})
	tassert.CheckFatal(t, err)

	// GET destination object to validate content
	writer := bytes.NewBuffer(nil)
	getArgs := api.GetArgs{Writer: writer}
	_, err = api.GetObject(baseParams, bckTo, objTo, &getArgs)
	tassert.CheckFatal(t, err)

	// Compare content
	tassert.Errorf(t, bytes.Equal(writer.Bytes(), data), "copied object content mismatch: expected %q, got %q",
		string(data), writer.String())

	// HEAD both source and destination to ensure existence
	hargs := api.HeadArgs{FltPresence: apc.FltPresent}
	_, err = api.HeadObject(baseParams, bckFrom, objFrom, hargs)
	tassert.CheckFatal(t, err)
	_, err = api.HeadObject(baseParams, bckTo, objTo, hargs)
	tassert.CheckFatal(t, err)

	// Attempt to copy to a nonexistent bucket
	bckNonexistent := cmn.Bck{
		Name:     "nonexistent-bucket-" + trand.String(5),
		Provider: apc.AIS,
	}
	err = api.CopyObject(baseParams, &api.CopyArgs{
		FromBck:     bckFrom,
		FromObjName: objFrom,
		ToBck:       bckNonexistent,
		ToObjName:   objTo,
	})
	tassert.Errorf(t, err != nil, "expected error when copying to nonexistent bucket")
	if err != nil {
		status := api.HTTPStatus(err)
		tassert.Errorf(t, status == http.StatusNotFound, "expected status %d, got %d", http.StatusNotFound, status)
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
	tlog.Logfln("Validating responses for non-existent ais bucket...")
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

	tlog.Logfln("PrefetchList num=%d", len(files))
	{
		var msg apc.PrefetchMsg
		msg.ObjNames = files
		prefetchListID, err := api.Prefetch(baseParams, bckRemote, &msg)
		tassert.CheckFatal(t, err)
		args := xact.ArgsMsg{ID: prefetchListID, Kind: apc.ActPrefetchObjects, Timeout: tools.RebalanceTimeout}
		_, err = api.WaitForXactionIC(baseParams, &args)
		tassert.CheckFatal(t, err)
	}

	tlog.Logfln("PrefetchRange %s", objRange)
	{
		var msg apc.PrefetchMsg
		msg.Template = objRange
		prefetchRangeID, err := api.Prefetch(baseParams, bckRemote, &msg)
		tassert.CheckFatal(t, err)
		args := xact.ArgsMsg{ID: prefetchRangeID, Kind: apc.ActPrefetchObjects, Timeout: tools.RebalanceTimeout}
		_, err = api.WaitForXactionIC(baseParams, &args)
		tassert.CheckFatal(t, err)
	}

	// Verify both objects are cached locally before deletion
	for _, objName := range files {
		exists := tools.CheckObjIsPresent(proxyURL, bckRemote, objName)
		if !exists {
			t.Fatalf("Object %s should be cached locally after prefetch", objName)
		}
	}

	// delete one obj from remote, and check evictions (below)
	err = api.DeleteObject(baseParams, bckRemote, fileName1)
	tassert.CheckFatal(t, err)

	tlog.Logfln("EvictList %v", files)
	evdListMsg := &apc.EvdMsg{ListRange: apc.ListRange{ObjNames: files}}
	evictListID, err := api.EvictMultiObj(baseParams, bckRemote, evdListMsg)
	tassert.CheckFatal(t, err)

	// Wait for xaction to complete
	args := xact.ArgsMsg{ID: evictListID, Kind: apc.ActEvictObjects, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForSnaps(baseParams, &args, args.NotRunning())
	tassert.CheckFatal(t, err)

	// Query the xaction snaps directly to get errors
	snaps, err := api.QueryXactionSnaps(baseParams, &args)
	tassert.CheckFatal(t, err)

	// Extract error from snapshots
	var statusErr string
	for _, snapList := range snaps {
		for _, snap := range snapList {
			if snap.Err != "" {
				statusErr = snap.Err
				break
			}
		}
		if statusErr != "" {
			break
		}
	}

	tlog.Logfln("Received expected evict status error: %s", statusErr)
	tassert.Errorf(t, statusErr != "", "expecting errors when not finding listed objects")

	tlog.Logfln("EvictRange")
	evdRangeMsg := &apc.EvdMsg{ListRange: apc.ListRange{ObjNames: nil, Template: objRange}}
	evictRangeID, err := api.EvictMultiObj(baseParams, bckRemote, evdRangeMsg)
	tassert.CheckFatal(t, err)
	args = xact.ArgsMsg{ID: evictRangeID, Kind: apc.ActEvictObjects, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	tools.CreateBucket(t, proxyURL, bckLocal, nil, true /*cleanup*/)

	// PUT
	tlog.Logfln("PUT %s and %s -> both buckets...", fileName1, fileName2)
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
	tlog.Logfln("Validating that ais bucket contains %s and %s ...", fileName1, fileName2)
	_, err = api.HeadObject(baseParams, bckLocal, fileName1, hargs)
	tassert.CheckFatal(t, err)
	_, err = api.HeadObject(baseParams, bckLocal, fileName2, hargs)
	tassert.CheckFatal(t, err)

	// Prefetch/Evict should work
	{
		var msg apc.PrefetchMsg
		msg.ObjNames = files
		prefetchListID, err := api.Prefetch(baseParams, bckRemote, &msg)
		tassert.CheckFatal(t, err)
		args = xact.ArgsMsg{ID: prefetchListID, Kind: apc.ActPrefetchObjects, Timeout: tools.RebalanceTimeout}
		_, err = api.WaitForXactionIC(baseParams, &args)
		tassert.CheckFatal(t, err)
	}

	evictListID, err = api.EvictMultiObj(baseParams, bckRemote, evdListMsg)
	tassert.CheckFatal(t, err)
	args = xact.ArgsMsg{ID: evictListID, Kind: apc.ActEvictObjects, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	// Delete from cloud bucket
	tlog.Logfln("Deleting %s and %s from cloud bucket ...", fileName1, fileName2)
	deleteID, err := api.DeleteMultiObj(baseParams, bckRemote, evdListMsg)
	tassert.CheckFatal(t, err)
	args = xact.ArgsMsg{ID: deleteID, Kind: apc.ActDeleteObjects, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	// Delete from ais bucket
	tlog.Logfln("Deleting %s and %s from ais bucket ...", fileName1, fileName2)
	deleteID, err = api.DeleteMultiObj(baseParams, bckLocal, evdListMsg)
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
	tlog.Logfln("PUT %s => %s", fileName, bckLocal.String())
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

	tlog.Logfln("PUT %s => %s", fileName, bckRemote.String())
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
	tlog.Logfln("GET %s without MD5 validation: %v", cos.IEC(totalSize, 0), time.Since(start))

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
	tlog.Logfln("GET %s with MD5 validation:    %v", cos.IEC(totalSize, 0), time.Since(start))
}

func TestColdGetChunked(t *testing.T) {
	const (
		chunkSize   = 16 * cos.MiB
		maxMonoSize = 1 * cos.GiB
	)

	tests := []struct {
		name          string
		objSize       uint64 // object size
		sourceChunked bool   // use chunksConf when provisioning source
		evict         bool   // evict after provision
		longOnly      bool   // run only with long tests
	}{
		// Cold GET scenarios (evicted): chunking based on size limit
		{name: "chunked-src-evict-exceeds", objSize: 1 * cos.GiB, sourceChunked: true, evict: true, longOnly: true},     // exceeds limit => chunks
		{name: "chunked-src-evict-small", objSize: 48 * cos.MiB, sourceChunked: true, evict: true, longOnly: true},      // doesn't exceed => monolithic
		{name: "monolithic-src-evict-exceeds", objSize: 1 * cos.GiB, sourceChunked: false, evict: true, longOnly: true}, // exceeds limit => chunks
		{name: "monolithic-src-evict-small", objSize: 48 * cos.MiB, sourceChunked: false, evict: true, longOnly: true},  // doesn't exceed => monolithic

		// Warm GET scenarios (not evicted): object stays as provisioned
		{name: "chunked-src-no-evict-exceeds", objSize: 1 * cos.GiB, sourceChunked: true, evict: false, longOnly: true},     // source chunked => chunks
		{name: "chunked-src-no-evict-small", objSize: 48 * cos.MiB, sourceChunked: true, evict: false, longOnly: true},      // source chunked => chunks
		{name: "monolithic-src-no-evict-exceeds", objSize: 1 * cos.GiB, sourceChunked: false, evict: false, longOnly: true}, // source monolithic => monolithic
		{name: "monolithic-src-no-evict-small", objSize: 48 * cos.MiB, sourceChunked: false, evict: false, longOnly: true},  // source monolithic => monolithic
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				numObjs  = 1
				proxyURL = tools.RandomProxyURL(t)
			)

			m := ioContext{
				t:             t,
				bck:           cliBck,
				num:           numObjs,
				fileSizeRange: [2]uint64{tt.objSize + 1, tt.objSize + chunkSize - 1}, // range within chunkSize for predictable chunk count
				prefix:        "coldget-chunked/" + tt.name + trand.String(5),
				getErrIsFatal: true,
			}

			tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: m.bck, Long: tt.longOnly})

			// Configure source provisioning
			if tt.sourceChunked {
				m.chunksConf = &ioCtxChunksConf{
					numChunks: int(tt.objSize / chunkSize),
					multipart: true,
				}
			}

			m.init(true /*cleanup*/)
			initMountpaths(t, proxyURL)
			baseParams := tools.BaseAPIParams(proxyURL)

			// Provision objects to remote backend
			tlog.Logfln("Provisioning %d objects (chunked=%v, size=%s, evict=%v)...", numObjs, tt.sourceChunked, cos.ToSizeIEC(int64(tt.objSize), 0), tt.evict)
			m.remotePuts(tt.evict)

			// Save original bucket props for cleanup
			p, err := api.HeadBucket(baseParams, m.bck, false)
			tassert.CheckFatal(t, err)

			// Configure bucket chunking properties
			_, err = api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{
				Chunks: &cmn.ChunksConfToSet{
					MaxMonolithicSize: apc.Ptr(cos.SizeIEC(maxMonoSize)),
					ChunkSize:         apc.Ptr(cos.SizeIEC(chunkSize)),
				},
			})
			tassert.CheckFatal(t, err)

			t.Cleanup(func() {
				_, err = api.SetBucketProps(baseParams, m.bck, &cmn.BpropsToSet{
					Chunks: &cmn.ChunksConfToSet{
						MaxMonolithicSize: apc.Ptr(p.Chunks.MaxMonolithicSize),
						ChunkSize:         apc.Ptr(p.Chunks.ChunkSize),
					},
				})
				tassert.CheckError(t, err)
			})

			if tt.evict {
				tlog.Logln("Performing cold GET...")
			} else {
				tlog.Logln("Performing warm GET...")
			}
			m.gets(nil, true)

			// Determine expected chunking outcome
			// Cold GET (evicted): chunks if size > limit
			// Warm GET (not evicted): stays as provisioned
			expectChunked := (tt.evict && tt.objSize > maxMonoSize) || (!tt.evict && tt.sourceChunked)

			// Verify chunks on disk only if expected to be chunked
			if expectChunked {
				tlog.Logfln("Verifying objects are chunked")
				ls, err := api.ListObjects(baseParams, m.bck, &apc.LsoMsg{Prefix: m.prefix, Props: apc.GetPropsChunked}, api.ListArgs{})
				tassert.CheckFatal(t, err)
				tassert.Fatalf(t, len(ls.Entries) == m.num, "expected %d objects, got %d", m.num, len(ls.Entries))

				tlog.Logfln("Verifying chunks are persisted on disk...")
				expectedChunkCount := int(tt.objSize / chunkSize)
				for _, objName := range m.objNames {
					if tt.sourceChunked && !tt.evict {
						// stay as provisioned
						expectedChunkCount = m.chunksConf.numChunks - 1
					}
					chunks := m.findObjChunksOnDisk(m.bck, objName)
					tassert.Fatalf(t, len(chunks) == expectedChunkCount, "expected %d chunk files for %s, found %d", expectedChunkCount, objName, len(chunks))
				}
			}

			tlog.Logfln("Test case '%s' completed successfully!", tt.name)
		})
	}
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
		t.Fatal("Expected an error, but go no errors.")
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
			&cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumCesXxh}, Extra: p.Extra, BID: 0xa73b9f11},
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

	fqn := m.findObjOnDisk(m.bck, objName)
	tools.CheckPathExists(t, fqn, false /*dir*/)
	oldFileInfo, _ := os.Stat(fqn)

	// Test when the contents of the file are changed
	tlog.Logfln("Changing contents of the file [%s]: %s", objName, fqn)
	err = os.WriteFile(fqn, []byte("Contents of this file have been changed."), cos.PermRWR)
	tassert.CheckFatal(t, err)
	validateGETUponFileChangeForChecksumValidation(t, proxyURL, objName, fqn, oldFileInfo)

	// Test when the xxHash of the file is changed
	objName = m.nextObjName()
	fqn = m.findObjOnDisk(m.bck, objName)
	tools.CheckPathExists(t, fqn, false /*dir*/)
	oldFileInfo, _ = os.Stat(fqn)

	tlog.Logfln("Changing file xattr[%s]: %s", objName, fqn)
	err = tools.SetXattrCksum(fqn, m.bck, cos.NewCksum(cos.ChecksumCesXxh, "deadbeefcafebabe"))
	tassert.CheckError(t, err)
	validateGETUponFileChangeForChecksumValidation(t, proxyURL, objName, fqn, oldFileInfo)

	// Test for no checksum algo
	objName = m.nextObjName()
	fqn = m.findObjOnDisk(m.bck, objName)

	if p.Cksum.Type != cos.ChecksumNone {
		propsToSet := &cmn.BpropsToSet{
			Cksum: &cmn.CksumConfToSet{
				Type: apc.Ptr(cos.ChecksumNone),
			},
		}
		_, err = api.SetBucketProps(baseParams, m.bck, propsToSet)
		tassert.CheckFatal(t, err)
	}

	tlog.Logfln("Changing file xattr[%s]: %s", objName, fqn)
	err = tools.SetXattrCksum(fqn, m.bck, cos.NewCksum(cos.ChecksumCesXxh, "badc0ffee0ddf00d"))
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
	if m.chunksConf != nil {
		t.Skipf("skipping %s: relies on 'initMountpaths/findObjOnDisk' to modify content and cannot work with chunked objects", t.Name())
	}

	p, err := api.HeadBucket(baseParams, m.bck, false /*don't add*/)
	tassert.CheckFatal(t, err)

	tMock := mock.NewTarget(mock.NewBaseBownerMock(
		meta.NewBck(
			m.bck.Name, m.bck.Provider, cmn.NsGlobal,
			&cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumCesXxh}, Extra: p.Extra, BID: 0xa73b9f11},
		),
	))

	// add mock backend
	var mockBackend core.Backend
	switch m.bck.Provider {
	case apc.AWS:
		mockBackend, _ = backend.NewAWS(tMock, mock.NewStatsTracker(), false /*starting up*/)
	case apc.GCP:
		mockBackend, _ = backend.NewGCP(tMock, mock.NewStatsTracker(), false /*starting up*/)
	case apc.Azure:
		mockBackend, _ = backend.NewAzure(tMock, mock.NewStatsTracker(), false /*starting up*/)
	case apc.OCI:
		mockBackend, _ = backend.NewOCI(tMock, mock.NewStatsTracker(), false /*starting up*/)
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
				Type:            apc.Ptr(cos.ChecksumCesXxh),
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
				lom.SetCksum(cos.NewCksum(cos.ChecksumCesXxh, "deadbeefcafebabe"))
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
					_, err := tMock.Backend(lom.Bck()).PutObj(t.Context(), r, lom, nil)
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

					_, err = backend.PutObj(t.Context(), r, lom, nil)
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
				fqn := m.findObjOnDisk(m.bck, objName)
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
	t.Run("MultiObject", testEvictMultiObject)
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
	api.WaitForSnapsIdle(baseParams, &flt)
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

func testEvictMultiObject(t *testing.T) {
	var (
		bck = cliBck
		m   = ioContext{
			t:        t,
			bck:      bck,
			num:      0, // Manual object creation
			fileSize: cos.KiB,
		}
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: bck})
	m.initAndSaveState(true /*cleanup*/)

	t.Cleanup(func() {
		m.del()
	})

	// Different eviction scenarios
	testObjects := map[string][]byte{
		"obj1.txt":             []byte("obj1 content"),
		"obj2.txt":             []byte("obj2 content"),
		"test_prefix_1.txt":    []byte("prefix content 1"),
		"test_prefix_2.txt":    []byte("prefix content 2"),
		"single_obj.txt":       []byte("single content"),
		"template_001.txt":     []byte("template content 1"),
		"template_002.txt":     []byte("template content 2"),
		"keepmd_test.txt":      []byte("keepmd content"),
		"nonrec/file.txt":      []byte("nonrecursive content"),
		"nonrec/deep/file.txt": []byte("deeper content"),
		"nested/deep/file.txt": []byte("nested content"),
	}

	// PUT all test objects
	for objName, content := range testObjects {
		putArgs := api.PutArgs{
			BaseParams: baseParams,
			Bck:        bck,
			ObjName:    objName,
			Reader:     readers.NewBytes(content),
		}
		_, err := api.PutObject(&putArgs)
		tassert.CheckFatal(t, err)
	}

	for objName := range testObjects {
		exists := tools.CheckObjIsPresent(proxyURL, bck, objName)
		tassert.Fatalf(t, exists, "object should be cached: %s", objName)
	}

	// Evict specific objects using list
	tlog.Logln("Testing evict with object list...")
	evdListMsg := &apc.EvdMsg{ListRange: apc.ListRange{ObjNames: []string{"obj1.txt", "obj2.txt"}}}
	evictListID, err := api.EvictMultiObj(baseParams, bck, evdListMsg)
	tassert.CheckFatal(t, err)
	time.Sleep(time.Second)
	args := xact.ArgsMsg{ID: evictListID, Kind: apc.ActEvictObjects, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	for _, objName := range []string{"obj1.txt", "obj2.txt"} {
		exists := tools.CheckObjIsPresent(proxyURL, bck, objName)
		tassert.Errorf(t, !exists, "object should be evicted: %s", objName)
	}

	// Evict with prefix (template without ranges)
	tlog.Logln("Testing evict with prefix...")
	evdPrefixMsg := &apc.EvdMsg{ListRange: apc.ListRange{Template: "test_prefix_"}}
	evictPrefixID, err := api.EvictMultiObj(baseParams, bck, evdPrefixMsg)
	tassert.CheckFatal(t, err)
	time.Sleep(time.Second)
	args = xact.ArgsMsg{ID: evictPrefixID, Kind: apc.ActEvictObjects, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	for _, objName := range []string{"test_prefix_1.txt", "test_prefix_2.txt"} {
		exists := tools.CheckObjIsPresent(proxyURL, bck, objName)
		tassert.Errorf(t, !exists, "object should be evicted: %s", objName)
	}

	// Single object eviction with --keep-md (equivalent)
	tlog.Logln("Testing single object evict...")
	evdSingleMsg := &apc.EvdMsg{ListRange: apc.ListRange{ObjNames: []string{"keepmd_test.txt"}}}
	evictSingleID, err := api.EvictMultiObj(baseParams, bck, evdSingleMsg)
	tassert.CheckFatal(t, err)
	time.Sleep(time.Second)
	args = xact.ArgsMsg{ID: evictSingleID, Kind: apc.ActEvictObjects, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	exists := tools.CheckObjIsPresent(proxyURL, bck, "keepmd_test.txt")
	tassert.Errorf(t, !exists, "object should be evicted: keepmd_test.txt")

	// Template-based eviction with brace expansion
	tlog.Logln("Testing evict with template...")
	evdTemplateMsg := &apc.EvdMsg{ListRange: apc.ListRange{Template: "template_{001..002}.txt"}}
	evictTemplateID, err := api.EvictMultiObj(baseParams, bck, evdTemplateMsg)
	tassert.CheckFatal(t, err)
	time.Sleep(time.Second)
	args = xact.ArgsMsg{ID: evictTemplateID, Kind: apc.ActEvictObjects, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	for _, objName := range []string{"template_001.txt", "template_002.txt"} {
		exists := tools.CheckObjIsPresent(proxyURL, bck, objName)
		tassert.Errorf(t, !exists, "object should be evicted: %s", objName)
	}

	// Non-recursive prefix eviction
	tlog.Logln("Testing non-recursive evict...")
	evdNonRecMsg := &apc.EvdMsg{
		ListRange: apc.ListRange{Template: "nonrec/"},
		NonRecurs: true,
	}
	evictNonRecID, err := api.EvictMultiObj(baseParams, bck, evdNonRecMsg)
	tassert.CheckFatal(t, err)
	args = xact.ArgsMsg{ID: evictNonRecID, Kind: apc.ActEvictObjects, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)
	time.Sleep(time.Second)

	exists = tools.CheckObjIsPresent(proxyURL, bck, "nonrec/file.txt")
	tassert.Errorf(t, !exists, "direct file should be evicted: nonrec/file.txt")
	exists = tools.CheckObjIsPresent(proxyURL, bck, "nonrec/deep/file.txt")
	tassert.Fatalf(t, exists, "deeper file should remain cached: nonrec/deep/file.txt")

	// Recursive prefix eviction (default behavior)
	tlog.Logln("Testing recursive evict...")
	evdRecMsg := &apc.EvdMsg{ListRange: apc.ListRange{Template: "nested/"}}
	evictRecID, err := api.EvictMultiObj(baseParams, bck, evdRecMsg)
	tassert.CheckFatal(t, err)
	time.Sleep(time.Second)
	args = xact.ArgsMsg{ID: evictRecID, Kind: apc.ActEvictObjects, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	exists = tools.CheckObjIsPresent(proxyURL, bck, "nested/deep/file.txt")
	tassert.Errorf(t, !exists, "nested object should be evicted: nested/deep/file.txt")

	// Verify remaining objects still exist
	remainingObjects := []string{"single_obj.txt", "nonrec/deep/file.txt"}
	for _, objName := range remainingObjects {
		exists := tools.CheckObjIsPresent(proxyURL, bck, objName)
		tassert.Fatalf(t, exists, "object should still be cached: %s", objName)
	}

	tlog.Logf("Multi-object eviction test completed successfully")
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
				&cmn.Bprops{Cksum: cmn.CksumConf{Type: cos.ChecksumCesXxh}, BID: 1},
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
	var (
		objName   = m.objNames[0]
		fqn       string
		isChunked = m.chunksConf != nil && m.chunksConf.multipart
	)
	if isChunked {
		fqns := m.findObjChunksOnDisk(m.bck, objName)
		tassert.Fatalf(t, len(fqns) > 0, "object should have chunks: %s", objName)
		fqn = fqns[0]
	} else {
		fqn = m.findObjOnDisk(m.bck, objName)
	}

	tlog.Logfln("Changing contents of the file [%s]: %s", objName, fqn)
	err := os.WriteFile(fqn, []byte("Contents of this file have been changed."), cos.PermRWR)
	tassert.CheckFatal(t, err)
	executeTwoGETsForChecksumValidation(proxyURL, m.bck, objName, isChunked, t)

	// Skip xattr tests for chunked/multipart objects
	if !isChunked {
		// Test changing the file xattr.
		objName = m.objNames[1]
		fqn = m.findObjOnDisk(m.bck, objName)
		tlog.Logfln("Changing file xattr[%s]: %s", objName, fqn)
		err = tools.SetXattrCksum(fqn, m.bck, cos.NewCksum(cos.ChecksumCesXxh, "deadbeefcafebabe"))
		tassert.CheckError(t, err)
		executeTwoGETsForChecksumValidation(proxyURL, m.bck, objName, isChunked, t)

		// Test for none checksum algorithm.
		objName = m.objNames[2]
		fqn = m.findObjOnDisk(m.bck, objName)

		if cksumConf.Type != cos.ChecksumNone {
			propsToSet := &cmn.BpropsToSet{
				Cksum: &cmn.CksumConfToSet{
					Type: apc.Ptr(cos.ChecksumNone),
				},
			}
			_, err = api.SetBucketProps(baseParams, m.bck, propsToSet)
			tassert.CheckFatal(t, err)
		}

		tlog.Logfln("Changing file xattr[%s]: %s", objName, fqn)
		err = tools.SetXattrCksum(fqn, m.bck, cos.NewCksum(cos.ChecksumCesXxh, "deadbeefcafebabe"))
		tassert.CheckError(t, err)
		_, err = api.GetObject(baseParams, m.bck, objName, nil)
		tassert.CheckError(t, err)
	}
}

func executeTwoGETsForChecksumValidation(proxyURL string, bck cmn.Bck, objName string, isChunked bool, t *testing.T) {
	baseParams := tools.BaseAPIParams(proxyURL)
	_, err := api.GetObjectWithValidation(baseParams, bck, objName, nil)
	herr := cmn.AsErrHTTP(err)
	switch {
	case herr == nil:
		t.Error("Error is nil, expected internal server error on a GET for an object")
	case isChunked && !strings.Contains(herr.Message, "truncated"): // TODO: create a new error type for it and replace it with generic `IsErr*` check
		t.Errorf("Expected chunk truncated error on a GET for a corrupted object, got [%v]", herr)
	case !isChunked && !cos.IsErrBadCksum(herr):
		t.Errorf("Expected bad checksum error on a GET for a corrupted object, got [%v]", herr)
	}

	// Execute another GET to make sure that the object is deleted
	_, err = api.GetObjectWithValidation(baseParams, bck, objName, nil)
	herr = cmn.AsErrHTTP(err)
	switch {
	case herr == nil:
		t.Error("Error is nil, expected not found on a second GET for a corrupted object")
	case isChunked && !strings.Contains(herr.Message, "truncated"):
		t.Errorf("Expected chunk truncated error on a GET for a corrupted object, got [%v]", herr)
	case !isChunked && !cos.IsErrNotFound(herr):
		t.Errorf("Expected Not Found on a second GET for a corrupted object, got [%v]", herr)
	}
}

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

		t.Cleanup(func() {
			tlog.Logln("Cleaning up...")
			propsToSet := &cmn.BpropsToSet{
				Cksum: &cmn.CksumConfToSet{
					EnableReadRange: apc.Ptr(cksumProps.EnableReadRange),
				},
			}
			_, err := api.SetBucketProps(baseParams, m.bck, propsToSet)
			tassert.CheckError(t, err)
		})

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
		testValidCases(&m, bck.Clone(), cksumProps, m.fileSize, objName)

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
		testValidCases(&m, m.bck, cksumProps, m.fileSize, objName)

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

func testValidCases(m *ioContext, bck cmn.Bck, cksumConf *cmn.CksumConf, fileSize uint64, objName string) {
	const byteRange = int64(500)
	iterations := int64(fileSize) / byteRange
	for j := range iterations {
		off := j * byteRange
		verifyValidRanges(m, bck, cksumConf, objName, off, byteRange, byteRange)
	}
	if rem := int64(fileSize) % byteRange; rem != 0 {
		verifyValidRanges(m, bck, cksumConf, objName, iterations*byteRange, byteRange, rem)
	}
}

func verifyValidRanges(m *ioContext, bck cmn.Bck, cksumConf *cmn.CksumConf, objName string, offset, length, expectedLength int64) {
	var (
		w          = bytes.NewBuffer(nil)
		rng        = cmn.MakeRangeHdr(offset, length)
		hdr        = http.Header{cos.HdrRange: []string{rng}}
		baseParams = tools.BaseAPIParams(m.proxyURL)
		args       = api.GetArgs{Writer: w, Header: hdr}
		oah        api.ObjAttrs
		err        error
	)
	isrng := offset > 0 || length != 0
	if isrng && !cksumConf.EnableReadRange {
		oah, err = api.GetObject(baseParams, bck, objName, &args)
	} else {
		oah, err = api.GetObjectWithValidation(baseParams, bck, objName, &args)
	}
	tassert.CheckError(m.t, err)
	if oah.Size() != expectedLength {
		m.t.Errorf("number of bytes received (%d) is different from expected (%d)", oah.Size(), expectedLength)
	}

	fqn := m.findObjOnDisk(bck, objName)

	file, err := os.Open(fqn)
	if err != nil {
		m.t.Fatalf("Unable to open file: %s. Error:  %v", fqn, err)
	}
	defer file.Close()
	outputBytes := w.Bytes()
	sectionReader := io.NewSectionReader(file, offset, length)
	expectedBytesBuffer := bytes.NewBuffer(nil)
	_, err = expectedBytesBuffer.ReadFrom(sectionReader)
	if err != nil {
		m.t.Errorf("Unable to read the file %s, from offset: %d and length: %d. Error: %v", fqn, offset, length, err)
	}
	expectedBytes := expectedBytesBuffer.Bytes()
	if len(outputBytes) != len(expectedBytes) {
		m.t.Errorf("Bytes length mismatch. Expected bytes: [%d]. Actual bytes: [%d]", len(expectedBytes), len(outputBytes))
	}
	if int64(len(outputBytes)) != expectedLength {
		m.t.Errorf("Returned bytes don't match expected length. Expected length: [%d]. Output length: [%d]",
			expectedLength, len(outputBytes))
	}
	for i := range expectedBytes {
		if expectedBytes[i] != outputBytes[i] {
			m.t.Errorf("Byte mismatch. Expected: %v, Actual: %v", string(expectedBytes), string(outputBytes))
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

	// Disable checksum.
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
	tlog.Logfln("GET %s without any checksum validation: %v", cos.IEC(totalSize, 0), time.Since(start))

	m.evict()

	propsToSet := &cmn.BpropsToSet{
		Cksum: &cmn.CksumConfToSet{
			Type:            apc.Ptr(cos.ChecksumCesXxh),
			ValidateColdGet: apc.Ptr(true),
		},
	}
	_, err = api.SetBucketProps(baseParams, m.bck, propsToSet)
	tassert.CheckFatal(t, err)

	start = time.Now()
	m.gets(nil /*api.GetArgs*/, true /*withValidate*/)
	tlog.Logfln("GET %s and validate checksum: %v", cos.IEC(totalSize, 0), time.Since(start))
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
		cksumValue := cos.ChecksumB2S(objData, cksumType)

		putArgs.Cksum = cos.NewCksum(cksumType, badCksumVal)
		putArgs.ObjName = fileName

		_, err := api.PutObject(&putArgs)
		if err == nil {
			t.Error("Bad checksum provided by the user, Expected an error")
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

func TestMultipartUpload(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
		}
		objName = "test-multipart-object"

		// Test data to upload in 3 parts
		part1Data = []byte("This is the first part of the multipart upload test. ")
		part2Data = []byte("This is the second part containing more test data. ")
		part3Data = []byte("This is the final third part to complete the upload.")

		// Complete expected content
		expectedContent = append(append(part1Data, part2Data...), part3Data...)
		partNumbers     = make([]int, 0, 3)
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	tlog.Logfln("multipart upload: %s/%s", bck.Name, objName)

	// Step 1: Create multipart upload
	uploadID, err := api.CreateMultipartUpload(baseParams, bck, objName)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, uploadID != "", "upload ID should not be empty")

	// Step 2: Upload three parts
	testParts := []struct {
		partNum int
		data    []byte
	}{
		{1, part1Data},
		{2, part2Data},
		{3, part3Data},
	}

	for _, part := range testParts {
		putPartArgs := &api.PutPartArgs{
			PutArgs: api.PutArgs{
				BaseParams: baseParams,
				Bck:        bck,
				ObjName:    objName,
				Reader:     readers.NewBytes(part.data),
				Size:       uint64(len(part.data)),
			},
			UploadID:   uploadID,
			PartNumber: part.partNum,
		}

		err = api.UploadPart(putPartArgs)
		tassert.CheckFatal(t, err)

		partNumbers = append(partNumbers, part.partNum)
	}

	// Step 3: Complete multipart upload
	err = api.CompleteMultipartUpload(baseParams, bck, objName, uploadID, partNumbers)
	tassert.CheckFatal(t, err)

	// Step 4: Verify the uploaded object

	// Check object exists
	hargs := api.HeadArgs{FltPresence: apc.FltPresent}
	objAttrs, err := api.HeadObject(baseParams, bck, objName, hargs)
	tassert.CheckFatal(t, err)

	// Verify object size matches expected content
	expectedSize := int64(len(expectedContent))
	tassert.Errorf(t, objAttrs.Size == expectedSize,
		"object size mismatch: expected %d, got %d", expectedSize, objAttrs.Size)

	// Download and verify content
	writer := bytes.NewBuffer(nil)
	getArgs := api.GetArgs{Writer: writer}
	_, err = api.GetObject(baseParams, bck, objName, &getArgs)
	tassert.CheckFatal(t, err)

	downloadedContent := writer.Bytes()
	tassert.Errorf(t, bytes.Equal(downloadedContent, expectedContent),
		"content mismatch: expected %q, got %q", string(expectedContent), string(downloadedContent))
}

func TestMultipartUploadParallel(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
		}
		objName = "test-multipart-parallel-object"

		// Test data to upload in 5 parts
		part1Data = []byte("Part 1: This is the first chunk of data. ")
		part2Data = []byte("Part 2: This is the second chunk with more content. ")
		part3Data = []byte("Part 3: Here is the middle section of our test. ")
		part4Data = []byte("Part 4: Almost done with the parallel upload. ")
		part5Data = []byte("Part 5: Final part to complete our test object.")

		// Complete expected content in proper order
		expectedContent = append(append(append(append(part1Data, part2Data...), part3Data...), part4Data...), part5Data...)
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	tlog.Logfln("multipart upload (parallel): %s/%s", bck.Name, objName)

	// Step 1: Create multipart upload
	uploadID, err := api.CreateMultipartUpload(baseParams, bck, objName)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, uploadID != "", "upload ID should not be empty")

	// Step 2: Upload five parts in parallel with shuffled order
	testParts := []struct {
		partNum int
		data    []byte
	}{
		{1, part1Data},
		{2, part2Data},
		{3, part3Data},
		{4, part4Data},
		{5, part5Data},
	}

	shuffledOrder := []int{3, 1, 5, 2, 4} // Upload parts in shuffled order

	g := errgroup.Group{}
	for _, partIdx := range shuffledOrder {
		part := testParts[partIdx-1] // Convert to 0-based index

		g.Go(func() error {
			putPartArgs := &api.PutPartArgs{
				PutArgs: api.PutArgs{
					BaseParams: baseParams,
					Bck:        bck,
					ObjName:    objName,
					Reader:     readers.NewBytes(part.data),
					Size:       uint64(len(part.data)),
				},
				UploadID:   uploadID,
				PartNumber: part.partNum,
			}

			return api.UploadPart(putPartArgs)
		})
	}

	// Wait for all goroutines to complete and check for errors
	err = g.Wait()
	tassert.CheckFatal(t, err)

	// Step 3: Complete multipart upload with parts in correct order
	partNumbers := []int{1, 2, 3, 4, 5} // Parts must be completed in correct order
	err = api.CompleteMultipartUpload(baseParams, bck, objName, uploadID, partNumbers)
	tassert.CheckFatal(t, err)

	// Step 4: Verify the uploaded object
	hargs := api.HeadArgs{FltPresence: apc.FltPresent}
	objAttrs, err := api.HeadObject(baseParams, bck, objName, hargs)
	tassert.CheckFatal(t, err)

	expectedSize := int64(len(expectedContent))
	tassert.Errorf(t, objAttrs.Size == expectedSize,
		"object size mismatch: expected %d, got %d", expectedSize, objAttrs.Size)

	writer := bytes.NewBuffer(nil)
	getArgs := api.GetArgs{Writer: writer}
	_, err = api.GetObject(baseParams, bck, objName, &getArgs)
	tassert.CheckFatal(t, err)

	downloadedContent := writer.Bytes()
	tassert.Errorf(t, bytes.Equal(downloadedContent, expectedContent),
		"content mismatch: expected %q, got %q", string(expectedContent), string(downloadedContent))

	tlog.Logfln("parallel multipart upload completed successfully with correct content ordering")
}

func TestMultipartMaxChunks(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
		}
		miniPartData = []byte("x") // Minimal 1-byte data per part
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	t.Run("exceed-limit", func(t *testing.T) {
		var (
			objName  = "test-multipart-exceed-limit"
			numParts = core.MaxChunkCount + 100 // Exceed limit by 100
		)

		uploadID, err := api.CreateMultipartUpload(baseParams, bck, objName)
		tassert.CheckFatal(t, err)

		err = uploadPartsInParallel(objName, uploadID, numParts, bck, miniPartData)

		// We expect an error because we're exceeding MaxChunkCount
		tassert.Fatalf(t, err != nil, "expected error when exceeding MaxChunkCount, but upload succeeded")
		herr := cmn.AsErrHTTP(err)
		tassert.Fatalf(t, herr != nil, "expected ErrHTTP, got %v", err)
		tassert.Fatalf(t, strings.Contains(herr.Message, "exceeds the maximum allowed"),
			"expected error message to contain 'exceeds the maximum allowed', got: %v", err)

		tlog.Logfln("multipart upload correctly rejected when exceeding MaxChunkCount (%d)", core.MaxChunkCount)

		// Cleanup: abort the upload
		_ = api.AbortMultipartUpload(baseParams, bck, objName, uploadID)
	})

	t.Run("equal-to-limit", func(t *testing.T) {
		var (
			objName  = "test-multipart-at-limit"
			numParts = core.MaxChunkCount // Exactly at the limit
		)

		uploadID, err := api.CreateMultipartUpload(baseParams, bck, objName)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, uploadID != "", "upload ID should not be empty")

		// Upload parts in parallel - all should succeed
		err = uploadPartsInParallel(objName, uploadID, numParts, bck, miniPartData)
		tassert.CheckFatal(t, err)

		// Complete multipart upload
		partNumbers := make([]int, numParts)
		for i := range numParts {
			partNumbers[i] = i + 1
		}
		err = api.CompleteMultipartUpload(baseParams, bck, objName, uploadID, partNumbers)
		tassert.CheckFatal(t, err)

		tlog.Logfln("multipart upload completed successfully with %d parts at MaxChunkCount", numParts)

		// Verify the uploaded object
		hargs := api.HeadArgs{FltPresence: apc.FltPresent}
		objAttrs, err := api.HeadObject(baseParams, bck, objName, hargs)
		tassert.CheckFatal(t, err)

		expectedSize := int64(numParts * len(miniPartData))
		tassert.Fatalf(t, objAttrs.Size == expectedSize, "object size mismatch: expected %d, got %d", expectedSize, objAttrs.Size)

		// GET the object and validate content
		writer := bytes.NewBuffer(nil)
		getArgs := api.GetArgs{Writer: writer}
		_, err = api.GetObject(baseParams, bck, objName, &getArgs)
		tassert.CheckFatal(t, err)

		downloadedContent := writer.Bytes()
		tassert.Errorf(t, len(downloadedContent) == numParts,
			"content length mismatch: expected %d bytes, got %d", numParts, len(downloadedContent))

		// Validate all bytes are 'x'
		for i, b := range downloadedContent {
			if b != 'x' {
				t.Fatalf("byte at position %d is %q, expected 'x'", i, b)
			}
		}

		tlog.Logfln("object content validated successfully: %d bytes, all 'x'", len(downloadedContent))
	})
}

func uploadPartsInParallel(objName, uploadID string, numParts int, bck cmn.Bck, data []byte) error {
	g := errgroup.Group{}
	g.SetLimit(sys.MaxParallelism())

	for partNum := 1; partNum <= numParts; partNum++ {
		pn := partNum

		g.Go(func() error {
			putPartArgs := &api.PutPartArgs{
				PutArgs: api.PutArgs{
					BaseParams: baseParams,
					Bck:        bck,
					ObjName:    objName,
					Reader:     readers.NewBytes(data),
					Size:       uint64(len(data)),
				},
				UploadID:   uploadID,
				PartNumber: pn,
			}

			return api.UploadPart(putPartArgs)
		})
	}

	return g.Wait()
}

func TestMultipartUploadAbort(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
		}
		objName = "test-multipart-abort-object"

		// Test data to upload in parts
		part1Data = []byte("Part 1: This is the first part before abort. ")
		part2Data = []byte("Part 2: This is the second part before abort. ")
		part3Data = []byte("Part 3: This part should fail after abort. ")
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	tlog.Logfln("multipart upload abort test: %s/%s", bck.Name, objName)

	// Step 1: Create multipart upload
	uploadID, err := api.CreateMultipartUpload(baseParams, bck, objName)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, uploadID != "", "upload ID should not be empty")

	// Step 2: Upload first two parts successfully
	testParts := []struct {
		partNum int
		data    []byte
	}{
		{1, part1Data},
		{2, part2Data},
	}

	for _, part := range testParts {
		putPartArgs := &api.PutPartArgs{
			PutArgs: api.PutArgs{
				BaseParams: baseParams,
				Bck:        bck,
				ObjName:    objName,
				Reader:     readers.NewBytes(part.data),
				Size:       uint64(len(part.data)),
			},
			UploadID:   uploadID,
			PartNumber: part.partNum,
		}

		err = api.UploadPart(putPartArgs)
		tassert.CheckFatal(t, err)
		tlog.Logfln("successfully uploaded part %d before abort", part.partNum)
	}

	// Step 3: Abort the multipart upload
	tlog.Logfln("aborting multipart upload with ID: %s", uploadID)
	err = api.AbortMultipartUpload(baseParams, bck, objName, uploadID)
	tassert.CheckFatal(t, err)

	// Negative tests below

	// Step 4: Try to upload another part after abort - should fail
	tlog.Logfln("attempting to upload part after abort (should fail)")
	putPartArgs := &api.PutPartArgs{
		PutArgs: api.PutArgs{
			BaseParams: baseParams,
			Bck:        bck,
			ObjName:    objName,
			Reader:     readers.NewBytes(part3Data),
			Size:       uint64(len(part3Data)),
		},
		UploadID:   uploadID,
		PartNumber: 3,
	}

	err = api.UploadPart(putPartArgs)
	tassert.Errorf(t, err != nil, "upload part after abort should fail, but succeeded")
	if err != nil {
		tlog.Logfln("correctly failed to upload part after abort: %v", err)
	}

	// Step 5: Try to complete multipart upload after abort - should fail
	tlog.Logfln("attempting to complete upload after abort (should fail)")
	partNumbers := []int{1, 2}
	err = api.CompleteMultipartUpload(baseParams, bck, objName, uploadID, partNumbers)
	tassert.Errorf(t, err != nil, "complete multipart upload after abort should fail, but succeeded")
	if err != nil {
		tlog.Logfln("correctly failed to complete upload after abort: %v", err)
	}

	// Step 6: Try to abort again - should be idempotent or fail gracefully
	tlog.Logfln("attempting to abort again (should be idempotent)")
	err = api.AbortMultipartUpload(baseParams, bck, objName, uploadID)
	// This may succeed (idempotent) or fail gracefully, both are acceptable
	if err != nil {
		tlog.Logfln("second abort failed as expected: %v", err)
	} else {
		tlog.Logfln("second abort succeeded (idempotent behavior)")
	}

	// Step 7: Verify that no object was created
	tlog.Logfln("verifying that object was not created after abort")
	hargs := api.HeadArgs{FltPresence: apc.FltPresent}
	_, err = api.HeadObject(baseParams, bck, objName, hargs)
	tassert.Errorf(t, err != nil, "object should not exist after abort, but HEAD succeeded")
	if err != nil {
		tlog.Logfln("correctly confirmed object does not exist after abort")
	}

	// Step 8: Test abort workflow with immediate abort (no parts uploaded)
	tlog.Logfln("testing immediate abort without uploading parts")
	objName2 := "test-multipart-immediate-abort"
	uploadID2, err := api.CreateMultipartUpload(baseParams, bck, objName2)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, uploadID2 != "", "upload ID should not be empty")

	// Immediately abort without uploading any parts
	err = api.AbortMultipartUpload(baseParams, bck, objName2, uploadID2)
	tassert.CheckFatal(t, err)
	tlog.Logfln("successfully aborted upload immediately without any parts")

	// Verify this object also doesn't exist
	_, err = api.HeadObject(baseParams, bck, objName2, hargs)
	tassert.Errorf(t, err != nil, "immediately aborted object should not exist")

	tlog.Logfln("multipart upload abort test completed successfully")
}

func TestMultipartUploadAndCopyBucket(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
		}
		dstBck = cmn.Bck{
			Name:     trand.String(10),
			Provider: apc.AIS,
		}

		// Create 30 multipart objects
		numObjects = 30

		// Test data to upload in 3 parts per object
		part1Data = []byte("This is the first part of the multipart upload test. ")
		part2Data = []byte("This is the second part containing more test data. ")
		part3Data = []byte("This is the final third part to complete the upload.")

		// Complete expected content per object
		expectedContent = append(append(part1Data, part2Data...), part3Data...)

		// Track all created objects
		createdObjects = make([]string, numObjects)
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	tlog.Logfln("Creating %d multipart objects in bucket: %s", numObjects, bck.Name)

	// Step 1: Create 30 multipart objects
	for i := range numObjects {
		objName := fmt.Sprintf("test-multipart-object-%d", i)
		createdObjects[i] = objName

		// Create multipart upload
		uploadID, err := api.CreateMultipartUpload(baseParams, bck, objName)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, uploadID != "", "upload ID should not be empty for object %d", i)

		// Upload three parts
		testParts := []struct {
			partNum int
			data    []byte
		}{
			{1, part1Data},
			{2, part2Data},
			{3, part3Data},
		}

		var partNumbers []int
		for _, part := range testParts {
			putPartArgs := &api.PutPartArgs{
				PutArgs: api.PutArgs{
					BaseParams: baseParams,
					Bck:        bck,
					ObjName:    objName,
					Reader:     readers.NewBytes(part.data),
					Size:       uint64(len(part.data)),
				},
				UploadID:   uploadID,
				PartNumber: part.partNum,
			}

			err = api.UploadPart(putPartArgs)
			tassert.CheckFatal(t, err)

			partNumbers = append(partNumbers, part.partNum)
		}

		// Complete multipart upload
		err = api.CompleteMultipartUpload(baseParams, bck, objName, uploadID, partNumbers)
		tassert.CheckFatal(t, err)
	}

	tlog.Logfln("Successfully created %d multipart objects", numObjects)

	// Step 2: Copy bucket
	tlog.Logfln("Copying bucket with %d chunked files: %s -> %s", numObjects, bck.Name, dstBck.Name)
	xid, err := api.CopyBucket(baseParams, bck, dstBck, &apc.TCBMsg{CopyBckMsg: apc.CopyBckMsg{}, ContinueOnError: true})
	if err != nil && ensurePrevRebalanceIsFinished(baseParams, err) {
		// can retry
		xid, err = api.CopyBucket(baseParams, bck, dstBck, &apc.TCBMsg{CopyBckMsg: apc.CopyBckMsg{}, ContinueOnError: true})
	}
	tassert.CheckFatal(t, err)

	t.Cleanup(func() {
		tools.DestroyBucket(t, proxyURL, dstBck)
	})

	// Wait for copy to complete
	args := xact.ArgsMsg{ID: xid, Kind: apc.ActCopyBck, Timeout: 3 * time.Minute}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	// Step 3: Verify all objects were copied correctly
	tlog.Logfln("Verifying all %d copied objects in destination bucket", numObjects)

	expectedSize := int64(len(expectedContent))
	hargs := api.HeadArgs{FltPresence: apc.FltPresent}

	for i, objName := range createdObjects {
		// Check object exists in destination
		objAttrs, err := api.HeadObject(baseParams, dstBck, objName, hargs)
		tassert.CheckFatal(t, err)

		// Verify size
		tassert.Errorf(t, objAttrs.Size == expectedSize,
			"object %d size mismatch: expected %d, got %d", i, expectedSize, objAttrs.Size)

		// Download and verify content
		writer := bytes.NewBuffer(nil)
		getArgs := api.GetArgs{Writer: writer}
		_, err = api.GetObject(baseParams, dstBck, objName, &getArgs)
		tassert.CheckFatal(t, err)

		downloadedContent := writer.Bytes()
		tassert.Errorf(t, bytes.Equal(downloadedContent, expectedContent),
			"object %d content mismatch", i)
	}

	tlog.Logfln("SUCCESS: All %d multipart chunked objects copied correctly", numObjects)
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
					r, _ := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cksumType})
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
					tlog.Logfln("%d. %s; range: [%s]", idx+1, test.name, test.rangeStr)

					var (
						err    error
						xid    string
						kind   string
						lsmsg  = &apc.LsoMsg{Prefix: "test/"}
						evdMsg = &apc.EvdMsg{ListRange: apc.ListRange{ObjNames: nil, Template: test.rangeStr}}
					)
					if evict {
						xid, err = api.EvictMultiObj(baseParams, b, evdMsg)
						lsmsg.Flags = apc.LsCached
						kind = apc.ActEvictObjects
					} else {
						xid, err = api.DeleteMultiObj(baseParams, b, evdMsg)
						kind = apc.ActDeleteObjects
					}
					if err != nil {
						t.Error(err)
						continue
					}

					time.Sleep(time.Second)
					args := xact.ArgsMsg{ID: xid, Kind: kind, Timeout: waitTimeout}
					_, err = api.WaitForXactionIC(baseParams, &args)
					tassert.CheckFatal(t, err)

					totalFiles -= test.delta
					objList, err := api.ListObjects(baseParams, b, lsmsg, api.ListArgs{})
					if err != nil {
						t.Error(err)
						continue
					}
					if len(objList.Entries) != totalFiles {
						t.Errorf("Incorrect number of remaining objects: %d, should be %d",
							len(objList.Entries), totalFiles)
						continue
					}

					tlog.Logfln("  %d objects have been deleted/evicted", test.delta)
				}

				msg := &apc.LsoMsg{Prefix: "test/"}
				lst, err := api.ListObjects(baseParams, b, msg, api.ListArgs{})
				tassert.CheckFatal(t, err)

				tlog.Logfln("Cleaning up remaining objects...")
				for _, obj := range lst.Entries {
					err := tools.Del(proxyURL, b, obj.Name, nil, nil, true /*silent*/)
					tassert.CheckError(t, err)
				}
			})
		}
	})
}
