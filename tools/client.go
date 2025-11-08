// Package tools provides common tools and utilities for all unit and integration tests
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package tools

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact"

	"golang.org/x/sync/errgroup"
)

const (
	// This value is holds the input of 'proxyURLFlag' from init_tests.go.
	// It is used in BaseAPIParams to determine if the cluster is running
	// on a
	// 	1. local instance (no docker) 	- works
	//	2. local docker instance		- works
	// 	3. AWS-deployed cluster 		- not tested (but runs mainly with Ansible)
	MockDaemonID = "MOCK"
)

// times and timeouts
const (
	WaitClusterStartup    = 20 * time.Second
	RebalanceStartTimeout = 10 * time.Second
	MaxCplaneTimeout      = 10 * time.Second

	CopyBucketTimeout     = 3 * time.Minute
	MultiProxyTestTimeout = 3 * time.Minute

	DsortFinishTimeout   = 6 * time.Minute
	RebalanceTimeout     = 2 * time.Minute
	EvictPrefetchTimeout = 2 * time.Minute
	BucketCleanupTimeout = time.Minute

	xactPollSleep     = time.Second
	controlPlaneSleep = 2 * time.Second
)

type PutObjectsArgs struct {
	ProxyURL           string
	Bck                cmn.Bck
	ObjPath            string
	CksumType          string
	ObjSize            uint64
	ObjSizeRange       [2]uint64
	MultipartNumChunks int
	ObjCnt             int
	ObjNameLn          int
	WorkerCnt          int
	FixedSize          bool
	Ordered            bool // true - object names make sequence, false - names are random
	IgnoreErr          bool
	SkipVC             bool           // skip loading existing object's metadata (see also: apc.QparamSkipVC and api.PutArgs.SkipVC)
	Reader             readers.Reader // optional reader for multipart uploads - when provided, uploads sequentially
}

// TODO: need to be replaced with a global syncPool of random objects:
// - the `cos.NowRand()` might not be thread-safe, but is currently used across goroutines in several tests
// - performance-wise, syncPool is much faster than creating a new random object for each call
var fileSizeRnd = cos.NowRand()

func GetRandSize(objSizeRange [2]uint64, objSize uint64, fixedSize bool) (size uint64) {
	switch {
	case objSizeRange[0] > 0 && objSizeRange[1] > 0:
		// randomly sample a size within [args.ObjSizeRange[0], args.ObjSizeRange[1]]
		size = objSizeRange[0] + fileSizeRnd.Uint64N(objSizeRange[1]-objSizeRange[0]+1)
	case objSize == 0:
		size = (fileSizeRnd.Uint64N(cos.KiB) + 1) * cos.KiB
	case !fixedSize:
		size = objSize + fileSizeRnd.Uint64N(cos.KiB)
	default:
		size = objSize
	}
	return size
}

func Del(proxyURL string, bck cmn.Bck, object string, wg *sync.WaitGroup, errCh chan error, silent bool) error {
	if wg != nil {
		defer wg.Done()
	}
	if !silent {
		tlog.Logfln("DEL: %s", object)
	}
	bp := BaseAPIParams(proxyURL)
	err := api.DeleteObject(bp, bck, object)
	if err != nil && errCh != nil {
		errCh <- err
	}
	return err
}

func CheckObjIsPresent(proxyURL string, bck cmn.Bck, objName string) bool {
	bp := BaseAPIParams(proxyURL)
	hargs := api.HeadArgs{FltPresence: apc.FltPresent, Silent: true}
	_, err := api.HeadObject(bp, bck, objName, hargs)
	return err == nil
}

// Put sends a PUT request to the given URL
func Put(proxyURL string, bck cmn.Bck, objName string, reader readers.Reader, size uint64, numChunks int, errCh chan error) {
	var err error
	if numChunks > 0 {
		err = PutMultipartObject(BaseAPIParams(proxyURL), bck, objName, size, &PutObjectsArgs{Reader: reader, MultipartNumChunks: numChunks})
	} else {
		bp := BaseAPIParams(proxyURL)
		putArgs := api.PutArgs{
			BaseParams: bp,
			Bck:        bck,
			ObjName:    objName,
			Cksum:      reader.Cksum(),
			Reader:     reader,
			Size:       size,
		}
		_, err = api.PutObject(&putArgs)
	}

	if err == nil {
		return
	}
	if errCh == nil {
		tlog.Logfln("Failed to PUT %s: %v (nil error channel)", bck.Cname(objName), err)
	} else {
		errCh <- err
	}
}

// PutObject sends a PUT request to the given URL.
func PutObject(t *testing.T, bck cmn.Bck, objName string, reader readers.Reader, size uint64) {
	var (
		proxyURL = RandomProxyURL()
		errCh    = make(chan error, 1)
	)
	Put(proxyURL, bck, objName, reader, size, 0 /*numChunks*/, errCh)
	tassert.SelectErr(t, errCh, "put", true)
}

// ListObjectNames returns a slice of object names of all objects that match the prefix in a bucket
func ListObjectNames(proxyURL string, bck cmn.Bck, prefix string, objectCountLimit int64, cached bool) ([]string, error) {
	var (
		bp  = BaseAPIParams(proxyURL)
		msg = &apc.LsoMsg{Prefix: prefix}
	)
	if cached {
		msg.Flags = apc.LsCached
	}
	data, err := api.ListObjects(bp, bck, msg, api.ListArgs{Limit: objectCountLimit})
	if err != nil {
		return nil, err
	}

	objs := make([]string, 0, len(data.Entries))
	for _, obj := range data.Entries {
		objs = append(objs, obj.Name)
	}
	return objs, nil
}

func GetPrimaryURL() string {
	primary, err := GetPrimaryProxy(proxyURLReadOnly)
	if err == nil {
		return primary.URL(cmn.NetPublic)
	}
	tlog.Logfln("Warning: GetPrimaryProxy [%v] - retrying once...", err)
	if currSmap == nil {
		time.Sleep(time.Second)
		primary, err = GetPrimaryProxy(proxyURLReadOnly)
	} else if proxyURL := currSmap.Primary.URL(cmn.NetPublic); proxyURL != proxyURLReadOnly {
		primary, err = GetPrimaryProxy(proxyURL)
	} else {
		var psi *meta.Snode
		if psi, err = currSmap.GetRandProxy(true /*exclude primary*/); err == nil {
			primary, err = GetPrimaryProxy(psi.URL(cmn.NetPublic))
		}
	}
	if err != nil {
		tlog.Logfln("Warning: GetPrimaryProxy [%v] - returning global %q", err, proxyURLReadOnly)
		return proxyURLReadOnly
	}
	return primary.URL(cmn.NetPublic)
}

// GetPrimaryProxy returns the primary proxy
func GetPrimaryProxy(proxyURL string) (*meta.Snode, error) {
	bp := BaseAPIParams(proxyURL)
	smap, err := api.GetClusterMap(bp)
	if err != nil {
		return nil, err
	}
	if currSmap == nil || currSmap.Version < smap.Version {
		currSmap = smap
	}
	return smap.Primary, err
}

func GetProxyReadiness(proxyURL string) error {
	return api.GetProxyReadiness(BaseAPIParams(proxyURL))
}

func CreateBucket(tb testing.TB, proxyURL string, bck cmn.Bck, props *cmn.BpropsToSet, cleanup bool) {
	bp := BaseAPIParams(proxyURL)
	err := api.CreateBucket(bp, bck, props)
	tassert.CheckFatal(tb, err)
	if cleanup {
		tb.Cleanup(func() {
			DestroyBucket(tb, proxyURL, bck)
		})
	}
}

// is usually called to cleanup (via tb.Cleanup)
func DestroyBucket(tb testing.TB, proxyURL string, bck cmn.Bck) {
	bp := BaseAPIParams(proxyURL)
	exists, err := api.QueryBuckets(bp, cmn.QueryBcks(bck), apc.FltExists)
	tassert.CheckFatal(tb, err)
	if exists {
		err = api.DestroyBucket(bp, bck)
		if err == nil {
			return
		}
		herr := cmn.AsErrHTTP(err)
		if herr == nil || herr.Status != http.StatusNotFound {
			tassert.CheckFatal(tb, err)
		}
	}
}

func EvictRemoteBucket(tb testing.TB, proxyURL string, bck cmn.Bck, keepMD bool) {
	if backend := bck.Backend(); backend != nil {
		bck.Copy(backend)
	}
	err := api.EvictRemoteBucket(BaseAPIParams(proxyURL), bck, keepMD)
	tassert.CheckFatal(tb, err)
}

func CleanupRemoteBucket(t *testing.T, proxyURL string, bck cmn.Bck, prefix string) {
	if !bck.IsRemote() {
		return
	}

	toDelete, err := ListObjectNames(proxyURL, bck, prefix, 0, false /*cached*/)
	tassert.CheckFatal(t, err)
	defer EvictRemoteBucket(t, proxyURL, bck, false /*keepMD*/)

	if len(toDelete) == 0 {
		return
	}

	bp := BaseAPIParams(proxyURL)
	msg := &apc.EvdMsg{ListRange: apc.ListRange{ObjNames: toDelete}}
	xid, err := api.DeleteMultiObj(bp, bck, msg)
	tassert.CheckFatal(t, err)
	args := xact.ArgsMsg{ID: xid, Kind: apc.ActDeleteObjects, Timeout: BucketCleanupTimeout}
	_, err = api.WaitForXactionIC(bp, &args)
	tassert.CheckFatal(t, err)
}

func SetBackendBck(t *testing.T, bp api.BaseParams, srcBck, dstBck cmn.Bck) {
	// find out real provider of the bucket
	p, err := api.HeadBucket(bp, dstBck, false /* don't add to cluster MD */)
	tassert.CheckFatal(t, err)

	_, err = api.SetBucketProps(bp, srcBck, &cmn.BpropsToSet{
		BackendBck: &cmn.BackendBckToSet{
			Name:     apc.Ptr(dstBck.Name),
			Provider: apc.Ptr(p.Provider),
		},
	})
	tassert.CheckFatal(t, err)
}

func RmTargetSkipRebWait(t *testing.T, proxyURL string, smap *meta.Smap) (*meta.Smap, *meta.Snode) {
	var (
		removeTarget, _ = smap.GetRandTarget()
		origTgtCnt      = smap.CountActiveTs()
		args            = &apc.ActValRmNode{DaemonID: removeTarget.ID(), SkipRebalance: true}
	)
	_, err := api.StartMaintenance(BaseAPIParams(proxyURL), args)
	tassert.CheckFatal(t, err)
	newSmap, err := WaitForClusterState(
		proxyURL,
		"target is gone",
		smap.Version,
		smap.CountActivePs(),
		origTgtCnt-1,
	)
	tassert.CheckFatal(t, err)
	newTgtCnt := newSmap.CountActiveTs()
	tassert.Fatalf(t, newTgtCnt == origTgtCnt-1,
		"new smap expected to have 1 target less: %d (v%d) vs %d (v%d)", newTgtCnt, origTgtCnt,
		newSmap.Version, smap.Version)
	return newSmap, removeTarget
}

// Internal API to remove a node from Smap: use it to unregister MOCK targets/proxies.
// Use `JoinCluster` to attach node back.
func RemoveNodeUnsafe(proxyURL, sid string) error {
	return _removeNodeFromSmap(proxyURL, sid, MaxCplaneTimeout)
}

func WaitForObjectToBeDowloaded(bp api.BaseParams, bck cmn.Bck, objName string, timeout time.Duration) error {
	maxTime := time.Now().Add(timeout)
	for {
		if time.Now().After(maxTime) {
			return fmt.Errorf("timed out (%v) waiting for %s download", timeout, bck.Cname(objName))
		}
		reslist, err := api.ListObjects(bp, bck, &apc.LsoMsg{}, api.ListArgs{})
		if err != nil {
			return err
		}
		for _, obj := range reslist.Entries {
			if obj.Name == objName {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func EnsureObjectsExist(t *testing.T, params api.BaseParams, bck cmn.Bck, objectsNames ...string) {
	for _, objName := range objectsNames {
		_, err := api.GetObject(params, bck, objName, nil)
		if err != nil {
			t.Errorf("Unexpected GetObject(%s) error: %v.", objName, err)
		}
	}
}

func putMultipartObjectSequential(bp api.BaseParams, bck cmn.Bck, objName, uploadID string, size uint64, args *PutObjectsArgs) error {
	partNumbers := make([]int, args.MultipartNumChunks)

	for i := range args.MultipartNumChunks {
		var (
			partNum    = i + 1
			offset     = uint64(partNum-1) * size / uint64(args.MultipartNumChunks)
			nextOffset = uint64(partNum) * size / uint64(args.MultipartNumChunks)
			partSize   = nextOffset - offset
		)

		// Read chunk from input reader
		buf := make([]byte, partSize)
		n, err := args.Reader.Read(buf)
		if err != nil && err != io.EOF {
			if abortErr := api.AbortMultipartUpload(bp, bck, objName, uploadID); abortErr != nil {
				return fmt.Errorf("failed to read part %d and failed to abort %s: read error: %w, abort error: %v", partNum, objName, err, abortErr)
			}
			return fmt.Errorf("failed to read part %d of %s: %w", partNum, objName, err)
		}

		// Create reader for this part
		partReader := readers.NewBytes(buf[:n])

		putPartArgs := &api.PutPartArgs{
			PutArgs: api.PutArgs{
				BaseParams: bp,
				Bck:        bck,
				ObjName:    objName,
				Cksum:      partReader.Cksum(),
				Reader:     partReader,
				Size:       uint64(n),
				SkipVC:     args.SkipVC,
			},
			UploadID:   uploadID,
			PartNumber: partNum,
		}

		if err := api.UploadPart(putPartArgs); err != nil {
			if abortErr := api.AbortMultipartUpload(bp, bck, objName, uploadID); abortErr != nil {
				return fmt.Errorf("failed to upload part %d and failed to abort %s: upload err: %w, abort err: %v",
					partNum, objName, err, abortErr)
			}
			return fmt.Errorf("failed to upload part %d of %s: %w", partNum, objName, err)
		}
		partNumbers[partNum-1] = partNum
	}

	if err := api.CompleteMultipartUpload(bp, bck, objName, uploadID, partNumbers); err != nil {
		if abortErr := api.AbortMultipartUpload(bp, bck, objName, uploadID); abortErr != nil {
			return fmt.Errorf("failed to complete multipart upload and failed to abort %s: complete err: %w, abort err: %v",
				objName, err, abortErr)
		}
		return fmt.Errorf("failed to complete multipart upload for %s: %w", objName, err)
	}
	return nil
}

func uploadPart(bp api.BaseParams, bck cmn.Bck, objName, uploadID string,
	partNum int, partSize uint64, args *PutObjectsArgs,
	mu *sync.Mutex, partNumbers []int) error {
	partReader, err := readers.New(&readers.Arg{
		Type:      readers.Rand,
		Size:      int64(partSize),
		CksumType: args.CksumType,
	})
	if err != nil {
		return fmt.Errorf("failed to create reader for part %d of %s: %w", partNum, objName, err)
	}
	defer partReader.Close()

	putPartArgs := &api.PutPartArgs{
		PutArgs: api.PutArgs{
			BaseParams: bp,
			Bck:        bck,
			ObjName:    objName,
			Cksum:      partReader.Cksum(),
			Reader:     partReader,
			Size:       partSize,
			SkipVC:     args.SkipVC,
		},
		UploadID:   uploadID,
		PartNumber: partNum,
	}

	if err := api.UploadPart(putPartArgs); err != nil {
		return fmt.Errorf("failed to upload part %d of %s: %w", partNum, objName, err)
	}

	mu.Lock()
	partNumbers[partNum-1] = partNum
	mu.Unlock()

	return nil
}

func PutMultipartObject(bp api.BaseParams, bck cmn.Bck, objName string, size uint64, args *PutObjectsArgs) error {
	uploadID, err := api.CreateMultipartUpload(bp, bck, objName)
	if err != nil {
		return fmt.Errorf("failed to create multipart upload for %s: %w", objName, err)
	}

	// If reader is provided in args, upload parts sequentially
	if args.Reader != nil {
		return putMultipartObjectSequential(bp, bck, objName, uploadID, size, args)
	}

	// Otherwise, upload parts in parallel
	var (
		partNumbers = make([]int, args.MultipartNumChunks)
		mu          = &sync.Mutex{}
		group       = &errgroup.Group{}
	)

	for i := range args.MultipartNumChunks {
		var (
			partNum    = i + 1
			offset     = uint64(partNum-1) * size / uint64(args.MultipartNumChunks)
			nextOffset = uint64(partNum) * size / uint64(args.MultipartNumChunks)
			partSize   = nextOffset - offset
		)
		group.Go(func() error {
			return uploadPart(bp, bck, objName, uploadID, partNum, partSize, args, mu, partNumbers)
		})
	}

	// Wait for all parts to complete
	if err := group.Wait(); err != nil {
		if abortErr := api.AbortMultipartUpload(bp, bck, objName, uploadID); abortErr != nil {
			return fmt.Errorf("failed to upload parts and failed to abort upload %s: upload error: %w, abort error: %v", objName, err, abortErr)
		}
		return fmt.Errorf("failed to upload parts of %s: %w", objName, err)
	}

	// Complete multipart upload
	if err := api.CompleteMultipartUpload(bp, bck, objName, uploadID, partNumbers); err != nil {
		if abortErr := api.AbortMultipartUpload(bp, bck, objName, uploadID); abortErr != nil {
			return fmt.Errorf("failed to complete multipart upload and failed to abort %s: complete error: %w, abort error: %v", objName, err, abortErr)
		}
		return fmt.Errorf("failed to complete multipart upload for %s: %w", objName, err)
	}
	return nil
}

//nolint:gocritic // need a copy of PutObjectsArgs
func PutRandObjs(args PutObjectsArgs) ([]string, int, error) {
	var (
		errCnt = atomic.NewInt32(0)
		putCnt = atomic.NewInt32(0)

		workerCnt = 40 // Default worker count.
		group     = &errgroup.Group{}
		objNames  = make([]string, 0, args.ObjCnt)
		bp        = BaseAPIParams(args.ProxyURL)
	)

	if args.WorkerCnt > 0 {
		workerCnt = args.WorkerCnt
	}
	workerCnt = min(workerCnt, args.ObjCnt)

	for i := range args.ObjCnt {
		if args.Ordered {
			objNames = append(objNames, path.Join(args.ObjPath, strconv.Itoa(i)))
		} else {
			nameLen := cos.NonZero(args.ObjNameLn, 16)
			objNames = append(objNames, path.Join(args.ObjPath, trand.String(nameLen)))
		}
	}
	chunkSize := (len(objNames) + workerCnt - 1) / workerCnt
	for i := 0; i < len(objNames); i += chunkSize {
		end := min(i+chunkSize, len(objNames))
		group.Go(func() error {
			return putObjectBatch(bp, objNames, i, end, &args, putCnt, errCnt)
		})
	}

	err := group.Wait()
	cos.Assert(err != nil || len(objNames) == int(putCnt.Load()))
	return objNames, int(errCnt.Load()), err
}

func putSingleObject(bp api.BaseParams, objName string, size uint64, args *PutObjectsArgs,
	putCnt, errCnt *atomic.Int32) error {
	reader, err := readers.New(&readers.Arg{
		Type:      readers.Rand,
		Size:      int64(size),
		CksumType: args.CksumType,
	})
	if err != nil {
		return err
	}
	defer reader.Close()

	// We could PUT while creating files, but that makes it
	// begin all the puts immediately (because creating random files is fast
	// compared to the list objects call that getRandomFiles does)
	_, err = api.PutObject(&api.PutArgs{
		BaseParams: bp,
		Bck:        args.Bck,
		ObjName:    objName,
		Cksum:      reader.Cksum(),
		Reader:     reader,
		Size:       size,
		SkipVC:     args.SkipVC,
	})

	putCnt.Inc()
	if err != nil {
		if args.IgnoreErr {
			errCnt.Inc()
			return nil
		}
		return err
	}
	return nil
}

func putObjectBatch(bp api.BaseParams, objNames []string, start, end int,
	args *PutObjectsArgs, putCnt, errCnt *atomic.Int32) error {
	for _, objName := range objNames[start:end] {
		size := GetRandSize(args.ObjSizeRange, args.ObjSize, args.FixedSize)

		if args.CksumType == "" {
			args.CksumType = cos.ChecksumNone
		}

		var err error
		if args.MultipartNumChunks > 0 {
			err = PutMultipartObject(bp, args.Bck, objName, size, args)
			putCnt.Inc()
			if err != nil {
				if args.IgnoreErr {
					errCnt.Inc()
					continue
				}
				return err
			}
		} else {
			if err := putSingleObject(bp, objName, size, args, putCnt, errCnt); err != nil {
				return err
			}
		}
	}
	return nil
}

// Put an object into a cloud bucket and evict it afterwards - can be used to test cold GET
func PutObjectInRemoteBucketWithoutCachingLocally(t *testing.T, bck cmn.Bck, object string, objContent cos.ReadOpenCloser) {
	bp := BaseAPIParams()

	_, err := api.PutObject(&api.PutArgs{
		BaseParams: bp,
		Bck:        bck,
		ObjName:    object,
		Reader:     objContent,
	})
	tassert.CheckFatal(t, err)

	err = api.EvictObject(bp, bck, object)
	tassert.CheckFatal(t, err)
}

func GetObjectAtime(t *testing.T, bp api.BaseParams, bck cmn.Bck, object, timeFormat string) (time.Time, string) {
	msg := &apc.LsoMsg{Props: apc.GetPropsAtime, TimeFormat: timeFormat, Prefix: object}
	bucketList, err := api.ListObjects(bp, bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)

	for _, entry := range bucketList.Entries {
		if entry.Name == object {
			atime, err := time.Parse(timeFormat, entry.Atime)
			tassert.CheckFatal(t, err)
			return atime, entry.Atime
		}
	}

	tassert.Fatalf(t, false, "Cannot find %s in bucket %s", object, bck.String())
	return time.Time{}, ""
}

// WaitForDsortToFinish waits until all dSorts jobs finished without failure or
// all jobs abort.
func WaitForDsortToFinish(proxyURL, managerUUID string) (allAborted bool, err error) {
	tlog.Logfln("waiting for dsort[%s]", managerUUID)

	bp := BaseAPIParams(proxyURL)
	deadline := time.Now().Add(DsortFinishTimeout)
	for time.Now().Before(deadline) {
		all, err := api.MetricsDsort(bp, managerUUID)
		if err != nil {
			return false, err
		}

		allAborted := true
		allFinished := true
		for _, jmetrics := range all {
			m := jmetrics.Metrics
			allAborted = allAborted && m.Aborted.Load()
			allFinished = allFinished &&
				!m.Aborted.Load() &&
				m.Extraction.Finished &&
				m.Sorting.Finished &&
				m.Creation.Finished
		}
		if allAborted {
			return true, nil
		}
		if allFinished {
			return false, nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false, errors.New("deadline exceeded")
}

func BaseAPIParams(urls ...string) api.BaseParams {
	var u string
	if len(urls) > 0 && urls[0] != "" {
		u = urls[0]
	} else {
		u = RandomProxyURL()
	}
	return api.BaseParams{Client: gctx.Client, URL: u, Token: LoggedUserToken, UA: "tools/test"}
}

func EvictObjects(t *testing.T, proxyURL string, bck cmn.Bck, prefix string) {
	bp := BaseAPIParams(proxyURL)
	msg := &apc.EvdMsg{ListRange: apc.ListRange{Template: prefix}}
	tlog.Logfln("evicting %s", bck.Cname(prefix))
	xid, err := api.EvictMultiObj(bp, bck, msg)
	if err != nil {
		t.Errorf("Evict bucket %s failed: %v", bck.String(), err)
	}

	args := xact.ArgsMsg{ID: xid, Kind: apc.ActEvictObjects, Timeout: EvictPrefetchTimeout}
	if _, err := api.WaitForXactionIC(bp, &args); err != nil {
		t.Errorf("Wait for xaction to finish failed, err = %v", err)
	}
}

// TODO -- FIXME: revise and rewrite
//
// Waits for both resilver and rebalance to complete.
// If they were not started, this function treats them as completed
// and returns. If timeout set, if any of rebalances doesn't complete before timeout
// the function ends with fatal.
func WaitForRebalAndResil(t testing.TB, bp api.BaseParams, timeouts ...time.Duration) {
	var (
		wg    = &sync.WaitGroup{}
		errCh = make(chan error, 2)
	)
	smap, err := api.GetClusterMap(bp)
	tassert.CheckFatal(t, err)

	if nat := smap.CountActiveTs(); nat < 1 {
		// NOTE in re nat == 1: single remaining target vs. graceful shutdown and such
		s := "No targets"
		tlog.Logfln("%s, %s - cannot rebalance", s, smap)
		_waitResil(t, bp, controlPlaneSleep)
		return
	}

	_waitReToStart(bp)
	timeout := RebalanceTimeout
	if len(timeouts) > 0 {
		timeout = timeouts[0]
	}
	tlog.Logfln("Waiting for rebalance and resilver to complete (timeout %v)", timeout)
	wg.Add(2)
	go func() {
		defer wg.Done()
		xargs := xact.ArgsMsg{Kind: apc.ActRebalance, OnlyRunning: true, Timeout: timeout}
		if _, err := api.WaitForXactionIC(bp, &xargs); err != nil {
			if cmn.IsStatusNotFound(err) {
				return
			}
			errCh <- err
		}
	}()

	go func() {
		defer wg.Done()
		xargs := xact.ArgsMsg{Kind: apc.ActResilver, OnlyRunning: true, Timeout: timeout}
		if _, err := api.WaitForXactionIC(bp, &xargs); err != nil {
			if cmn.IsStatusNotFound(err) {
				return
			}
			errCh <- err
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		tassert.CheckFatal(t, err)
		return
	}
}

// compare w/ `tools.WaitForResilvering`
func _waitResil(t testing.TB, bp api.BaseParams, timeout time.Duration) {
	xargs := xact.ArgsMsg{Kind: apc.ActResilver, OnlyRunning: true, Timeout: timeout}
	_, err := api.WaitForXactionIC(bp, &xargs)
	if err == nil {
		return
	}
	if herr, ok := err.(*cmn.ErrHTTP); ok {
		if herr.Status == http.StatusNotFound { // double check iff not found
			time.Sleep(xactPollSleep)
			_, err = api.WaitForXactionIC(bp, &xargs)
		}
	}
	if err == nil {
		return
	}
	if herr, ok := err.(*cmn.ErrHTTP); ok {
		if herr.Status == http.StatusNotFound {
			err = nil
		}
	}
	tassert.CheckError(t, err)
}

func WaitForRebalanceByID(t *testing.T, bp api.BaseParams, rebID string, timeouts ...time.Duration) {
	if rebID == "" {
		return
	}
	tassert.Fatalf(t, xact.IsValidRebID(rebID), "invalid reb ID %q", rebID)
	timeout := RebalanceTimeout
	if len(timeouts) > 0 {
		timeout = timeouts[0]
	}
	tlog.Logfln("Wait for rebalance %s", rebID)
	xargs := xact.ArgsMsg{ID: rebID, Kind: apc.ActRebalance, OnlyRunning: true, Timeout: timeout}
	_, err := api.WaitForXactionIC(bp, &xargs)
	tassert.CheckFatal(t, err)
}

func _waitReToStart(bp api.BaseParams) {
	var (
		kinds   = []string{apc.ActRebalance, apc.ActResilver}
		timeout = max(10*xactPollSleep, MaxCplaneTimeout)
		retries = int(timeout / xactPollSleep)
	)
	for range retries {
		for _, kind := range kinds {
			args := xact.ArgsMsg{Timeout: xactPollSleep, OnlyRunning: true, Kind: kind}
			status, err := api.GetOneXactionStatus(bp, &args)
			if err == nil {
				if !status.IsFinished() {
					return
				}
			}
		}
		time.Sleep(xactPollSleep)
	}
	tlog.Logfln("Warning: timed out (%v) waiting for rebalance or resilver to start", timeout)
}

func GetClusterStats(t *testing.T, proxyURL string) stats.Cluster {
	bp := BaseAPIParams(proxyURL)
	scs, err := api.GetClusterStats(bp)
	tassert.CheckFatal(t, err)
	return scs
}

func GetNamedStatsVal(ds *stats.Node, name string) int64 {
	v, ok := ds.Tracker[name]
	if !ok {
		return 0
	}
	return v.Value
}

func GetDaemonConfig(t *testing.T, node *meta.Snode) *cmn.Config {
	bp := BaseAPIParams()
	config, err := api.GetDaemonConfig(bp, node)
	tassert.CheckFatal(t, err)
	return config
}

func GetClusterMap(tb testing.TB, url string) *meta.Smap {
	smap, err := waitForStartup(BaseAPIParams(url), tb)
	if err == nil && (currSmap == nil || currSmap.Version < smap.Version) {
		currSmap = smap
	}
	return smap
}

func getClusterConfig() (config *cmn.Config, err error) {
	proxyURL := GetPrimaryURL()
	primary, err := GetPrimaryProxy(proxyURL)
	if err != nil {
		return nil, err
	}
	return api.GetDaemonConfig(BaseAPIParams(proxyURL), primary)
}

func GetClusterConfig(t *testing.T) (config *cmn.Config) {
	config, err := getClusterConfig()
	tassert.CheckError(t, err)
	return config
}

func SetClusterConfig(t *testing.T, nvs cos.StrKVs) {
	proxyURL := GetPrimaryURL()
	bp := BaseAPIParams(proxyURL)
	err := api.SetClusterConfig(bp, nvs, false /*transient*/)
	tassert.CheckError(t, err)
}

func SetClusterConfigUsingMsg(t *testing.T, toUpdate *cmn.ConfigToSet) {
	proxyURL := GetPrimaryURL()
	bp := BaseAPIParams(proxyURL)
	err := api.SetClusterConfigUsingMsg(bp, toUpdate, false /*transient*/)
	tassert.CheckFatal(t, err)
}

func EnableRebalance(t *testing.T) {
	proxyURL := GetPrimaryURL()
	bp := BaseAPIParams(proxyURL)
	err := api.EnableRebalance(bp)
	tassert.CheckError(t, err)
}

func DisableRebalance(t *testing.T) {
	proxyURL := GetPrimaryURL()
	bp := BaseAPIParams(proxyURL)
	err := api.DisableRebalance(bp)
	tassert.CheckError(t, err)
}

func SetRemAisConfig(t *testing.T, nvs cos.StrKVs) {
	remoteBP := BaseAPIParams(RemoteCluster.URL)
	err := api.SetClusterConfig(remoteBP, nvs, false /*transient*/)
	tassert.CheckError(t, err)
}

func CheckErrIsNotFound(t *testing.T, err error) {
	if err == nil {
		t.Fatal("expected error")
		return
	}
	herr, ok := err.(*cmn.ErrHTTP)
	tassert.Fatalf(t, ok, "expected an error of the type *cmn.ErrHTTP, got %v(%T)", err, err)
	tassert.Fatalf(
		t, herr.Status == http.StatusNotFound,
		"expected status: %d, got: %d.", http.StatusNotFound, herr.Status,
	)
}

func waitForStartup(bp api.BaseParams, ts ...testing.TB) (*meta.Smap, error) {
	for {
		smap, err := api.GetClusterMap(bp)
		if err != nil {
			if api.HTTPStatus(err) == http.StatusServiceUnavailable {
				tlog.Logln("Waiting for the cluster to start up...")
				time.Sleep(WaitClusterStartup)
				continue
			}

			tlog.Logfln("Unable to get usable cluster map: %v", err)
			if len(ts) > 0 {
				tassert.CheckFatal(ts[0], err)
			}
			return nil, err
		}
		return smap, nil
	}
}
