// Package integration_test.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"testing"

	s3compat "github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact"
)

const (
	chunkPolicyLimit = 64 * cos.KiB
	chunkPolicySize  = int64(cmn.ChunkSizeMin)
	coldGetOP        = "COLD-GET"
	blobOP           = "BLOB"
)

func TestPutObjectAutoChunks(t *testing.T) {
	proxyURL := tools.RandomProxyURL(t)
	bck := cmn.Bck{Name: "put-auto-chunk-" + trand.String(8), Provider: apc.AIS}
	tools.CreateBucket(t, proxyURL, bck, &cmn.BpropsToSet{Chunks: &cmn.ChunksConfToSet{
		ObjSizeLimit: apc.Ptr(cos.SizeIEC(chunkPolicyLimit)), ChunkSize: apc.Ptr(cos.SizeIEC(chunkPolicySize)),
	}}, true /*cleanup*/)

	t.Run("below-limit", func(t *testing.T) {
		putObjectsAndCheckPolicy(t, bck, chunkPolicyLimit-1, false)
	})
	t.Run("at-limit", func(t *testing.T) {
		putObjectsAndCheckPolicy(t, bck, chunkPolicyLimit, true)
	})
	t.Run("above-limit", func(t *testing.T) {
		putObjectsAndCheckPolicy(t, bck, chunkPolicyLimit+1, true)
	})
}

func TestRebalanceAutoChunks(t *testing.T) {
	const multipartChunkSize = 48 * cos.KiB
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 2, Long: true})
	proxyURL := tools.RandomProxyURL(t)
	bp := tools.BaseAPIParams(proxyURL)
	bck := cmn.Bck{Name: "rebalance-auto-chunk-" + trand.String(8), Provider: apc.AIS}
	tools.CreateBucket(t, proxyURL, bck, &cmn.BpropsToSet{
		Chunks: &cmn.ChunksConfToSet{
			ObjSizeLimit: apc.Ptr(cos.SizeIEC(chunkPolicyLimit)),
			ChunkSize:    apc.Ptr(cos.SizeIEC(chunkPolicySize)),
		},
	}, true /*cleanup*/)
	objects := ioContext{
		t: t, bck: bck, num: 10, fileSize: 2 * multipartChunkSize, fixedSize: true,
		chunksConf: &ioCtxChunksConf{multipart: true, numChunks: 2},
	}
	objects.initAndSaveState(true /*cleanup*/)
	objects.puts()
	smap := objects.smap
	smap.InitDigests()
	owner, err := smap.HrwName2T(meta.CloneBck(&bck).MakeUname(objects.objNames[0]))
	tassert.CheckFatal(t, err)
	for _, objName := range objects.objNames {
		checkObjectChunked(t, bp, bck, objName, multipartChunkSize)
	}

	args := &apc.ActValRmNode{DaemonID: owner.ID()}
	rebID, err := startMaintenanceRetry(t, bp, args)
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		_, err := stopMaintenance(t, bp, args, proxyURL, smap.Version, smap.CountActivePs(), smap.CountActiveTs())
		tassert.CheckError(t, err)
	})
	tassert.Fatalf(t, rebID != "", "expected maintenance to start rebalance")
	tools.WaitForRebalanceByID(t, bp, rebID)
	updated, err := tools.WaitForClusterState(proxyURL, "target in maintenance", smap.Version, smap.CountActivePs(), smap.CountActiveTs()-1, owner.ID())
	tassert.CheckFatal(t, err)
	updated.InitDigests()
	moved := 0
	// Only objects owned by the maintenance target must move to another target.
	for _, objName := range objects.objNames {
		uname := meta.CloneBck(&bck).MakeUname(objName)
		previous, err := smap.HrwName2T(uname)
		tassert.CheckFatal(t, err)
		if previous.ID() != owner.ID() {
			// Unmoved objects retain the client's two 48KiB parts.
			checkObjectChunked(t, bp, bck, objName, multipartChunkSize)
			continue
		}
		// Moved objects must use the bucket's three 32KiB chunks.
		receiver, err := updated.HrwName2T(uname)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, receiver.ID() != owner.ID(), "expected %s to move off %s", objName, owner.ID())
		checkObjectChunked(t, bp, bck, objName, chunkPolicySize)
		moved++
	}
	tassert.Fatalf(t, moved > 0, "expected at least one object to move")
	objects.gets(nil, true /*withValidation*/)
	objects.ensureNoGetErrors()
}

func TestColdGetAutoChunks(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: cliBck})
	proxyURL := tools.RandomProxyURL(t)
	bp := tools.BaseAPIParams(proxyURL)
	p, err := api.HeadBucket(bp, cliBck, false /*dontAddRemote*/)
	tassert.CheckFatal(t, err)
	orig := p.Chunks
	t.Cleanup(func() {
		_, err := api.SetBucketProps(bp, cliBck, &cmn.BpropsToSet{Chunks: &cmn.ChunksConfToSet{
			ObjSizeLimit:      apc.Ptr(orig.ObjSizeLimit),
			MaxMonolithicSize: apc.Ptr(orig.MaxMonolithicSize),
			ChunkSize:         apc.Ptr(orig.ChunkSize),
		}})
		tassert.CheckError(t, err)
	})

	t.Run("below-limit", func(t *testing.T) {
		coldGetObjectsAndCheckPolicy(t, bp, cliBck, chunkPolicyLimit-1, false)
	})
	t.Run("at-limit", func(t *testing.T) {
		coldGetObjectsAndCheckPolicy(t, bp, cliBck, chunkPolicyLimit, true)
	})
	t.Run("above-limit", func(t *testing.T) {
		coldGetObjectsAndCheckPolicy(t, bp, cliBck, chunkPolicyLimit+1, true)
	})
}

// TestRemoteChunkedWriteFailureCleanup forces the cloud backend to reject AIS-generated
// multipart uploads and verifies that PUT and copy remove their partial local state.
func TestRemoteChunkedWriteFailureCleanup(t *testing.T) {
	const (
		chunkSize = int64(cmn.ChunkSizeMin)
		objSize   = 2*chunkSize + 1
	)
	tools.CheckSkip(t, &tools.SkipTestArgs{
		Bck: cliBck, RequiredCloudProvider: apc.AWS, RequiredDeployment: tools.ClusterTypeLocal,
	})

	proxyURL := tools.RandomProxyURL(t)
	bp := tools.BaseAPIParams(proxyURL)
	initMountpaths(t, proxyURL)
	orig := setBucketChunkPolicy(t, bp, cliBck, 0, chunkSize)
	t.Cleanup(func() {
		_, err := api.SetBucketProps(bp, cliBck, &cmn.BpropsToSet{Chunks: &cmn.ChunksConfToSet{
			ObjSizeLimit:      apc.Ptr(orig.ObjSizeLimit),
			MaxMonolithicSize: apc.Ptr(orig.MaxMonolithicSize),
			ChunkSize:         apc.Ptr(orig.ChunkSize),
		}})
		tassert.CheckError(t, err)
	})

	prefix := "chunk-backend-failure/" + trand.String(8) + "/"
	copySrc := prefix + "copy-source"
	copyDst := tools.GenerateObjectNameForTarget(copySrc, prefix+"copy-", cliBck,
		tools.GetClusterMap(t, proxyURL), true /*same target*/)
	_, err := api.PutObject(&api.PutArgs{
		BaseParams: bp, Bck: cliBck, ObjName: copySrc,
		Reader: readers.NewBytes(make([]byte, objSize)), Size: uint64(objSize),
	})
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		for _, objName := range []string{copySrc, prefix + "put", copyDst} {
			api.DeleteObject(bp, cliBck, objName)
		}
	})
	setBucketChunkPolicy(t, bp, cliBck, 1, chunkSize)

	t.Run("put", func(t *testing.T) {
		objName := prefix + "put"
		_, err := api.PutObject(&api.PutArgs{
			BaseParams: bp, Bck: cliBck, ObjName: objName,
			Reader: readers.NewBytes(make([]byte, objSize)), Size: uint64(objSize),
		})
		tassert.Fatalf(t, err != nil && strings.Contains(err.Error(), "EntityTooSmall"),
			"expected EntityTooSmall, got %v", err)
		checkFailedChunkedWriteCleanup(t, bp, cliBck, objName)
	})

	t.Run("copy", func(t *testing.T) {
		err := api.CopyObject(bp, &api.CopyArgs{
			FromBck: cliBck, FromObjName: copySrc, ToBck: cliBck, ToObjName: copyDst,
		})
		tassert.Fatalf(t, err != nil && strings.Contains(err.Error(), "EntityTooSmall"),
			"expected EntityTooSmall, got %v", err)
		checkFailedChunkedWriteCleanup(t, bp, cliBck, copyDst)
	})
}

// TestChunkingRWStress PUTs remote objects, races cache and copy operations on them, and validates content and layout.
func TestChunkingRWStress(t *testing.T) {
	const (
		objSize   = 6 * cos.MiB
		chunkSize = int64(cloudMinPartSize)
	)
	var (
		objCount = 2
		rounds   = 3
	)
	if !testing.Short() {
		objCount, rounds = 6, 8
	}
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: cliBck, MinTargets: 2})

	proxyURL := tools.RandomProxyURL(t)
	bp := tools.BaseAPIParams(proxyURL)
	orig := setBucketChunkPolicy(t, bp, cliBck, chunkPolicyLimit, chunkSize)
	t.Cleanup(func() {
		_, err := api.SetBucketProps(bp, cliBck, &cmn.BpropsToSet{Chunks: &cmn.ChunksConfToSet{
			ObjSizeLimit:      apc.Ptr(orig.ObjSizeLimit),
			MaxMonolithicSize: apc.Ptr(orig.MaxMonolithicSize),
			ChunkSize:         apc.Ptr(orig.ChunkSize),
		}})
		tassert.CheckError(t, err)
	})

	objects := ioContext{
		t: t, bck: cliBck, num: objCount, prefix: "chunking-stress/" + trand.String(8) + "/",
		fileSize: objSize, fixedSize: true, ordered: true,
	}
	objects.init(true /*cleanup*/)
	// Exercise regular PUT once; all concurrent operations below reuse these objects.
	objects.remotePuts(false /*evict*/)

	// Every round races GET, COPY, cold GET, blob download, and eviction on every source object.
	ops := [...]string{getOP, copyOP, coldGetOP, blobOP, apc.ActEvictObjects}
	numOps := rounds * objCount * len(ops)
	results := make(chan opRes, numOps)
	wg := cos.NewLimitedWaitGroup(40, 0)
	for i := range numOps {
		idx := (i / len(ops)) % objCount
		op := ops[i%len(ops)]
		wg.Add(1)
		go func(idx int, op string) {
			defer wg.Done()
			var (
				src = objects.objNames[idx]
				dst = src + "-copy"
				err error
			)
			switch op {
			case getOP:
				err = objects.get(bp, idx, 0, nil, true /*validate*/)
			case copyOP:
				err = api.CopyObject(bp, &api.CopyArgs{
					FromBck: objects.bck, FromObjName: src, ToBck: objects.bck, ToObjName: dst,
				})
			case coldGetOP:
				err = api.EvictObject(bp, objects.bck, src)
				if err == nil || api.HTTPStatus(err) == http.StatusNotFound {
					err = objects.get(bp, idx, 0, nil, true /*validate*/)
				}
			case blobOP:
				err = api.EvictObject(bp, objects.bck, src)
				if err == nil || api.HTTPStatus(err) == http.StatusNotFound {
					var xid string
					xid, err = api.BlobDownload(bp, objects.bck, src, &apc.BlobMsg{
						ChunkSize: chunkSize, FullSize: objSize,
					})
					if err == nil && xid != "" {
						err = api.WaitForXaction(bp, &xact.ArgsMsg{
							ID: xid, Kind: apc.ActBlobDl, Timeout: tools.EvictPrefetchTimeout,
						})
					}
				}
			case apc.ActEvictObjects:
				err = api.EvictObject(bp, objects.bck, src)
				if api.HTTPStatus(err) == http.StatusNotFound {
					err = nil
				}
			}
			if herr := cmn.AsErrHTTP(err); herr != nil && herr.TypeCode == "ErrBusy" {
				err = nil // expected when conflicting operations race on the same object
			}
			results <- opRes{op: op, err: err}
		}(idx, op)
	}
	wg.Wait()
	close(results)

	for result := range results {
		if result.err != nil {
			t.Fatalf("%s failed: %v", result.op, result.err)
		}
	}

	// Recover every source with cold GET, copy it, and verify content, layout, and locks.
	for idx, src := range objects.objNames {
		dst := src + "-copy"
		err := api.EvictObject(bp, objects.bck, src)
		tassert.Fatalf(t, err == nil || api.HTTPStatus(err) == http.StatusNotFound,
			"evict %s: %v", objects.bck.Cname(src), err)
		tassert.CheckFatal(t, objects.get(bp, idx, 0, nil, true /*validate*/))
		err = api.CopyObject(bp, &api.CopyArgs{
			FromBck: objects.bck, FromObjName: src, ToBck: objects.bck, ToObjName: dst,
		})
		tassert.CheckFatal(t, err)
		tassert.CheckFatal(t, sameObjectContent(bp, objects.bck, src, dst))
		checkObjectChunked(t, bp, objects.bck, src, chunkSize)
		checkObjectChunked(t, bp, objects.bck, dst, chunkSize)
		lock, err := api.CheckObjectLock(bp, objects.bck, src)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, lock == apc.LockNone, "%s: expected released lock, got %d", objects.bck.Cname(src), lock)
		lock, err = api.CheckObjectLock(bp, objects.bck, dst)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, lock == apc.LockNone, "%s: expected released lock, got %d", objects.bck.Cname(dst), lock)
	}
}

func sameObjectContent(bp api.BaseParams, bck cmn.Bck, src, dst string) error {
	expected, expectedSize, err := api.GetObjectReader(bp, bck, src, nil)
	if err != nil {
		return err
	}
	defer cos.Close(expected)
	actual, actualSize, err := api.GetObjectReader(bp, bck, dst, nil)
	if err != nil {
		return err
	}
	defer cos.Close(actual)
	if expectedSize != actualSize {
		return fmt.Errorf("%s: expected size %d, got %d", bck.Cname(dst), expectedSize, actualSize)
	}
	if !tools.ReaderEqual(expected, actual) {
		return fmt.Errorf("%s: content differs from %s", bck.Cname(dst), bck.Cname(src))
	}
	return nil
}

func checkFailedChunkedWriteCleanup(t *testing.T, bp api.BaseParams, bck cmn.Bck, objName string) {
	t.Helper()
	_, err := api.HeadObjectV2(bp, bck, objName, apc.GetPropsName, api.HeadArgs{FltPresence: apc.FltPresent, Silent: true})
	tassert.Fatalf(t, isErrNotFound(err), "%s: expected no local object, got %v", bck.Cname(objName), err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, bp.URL+apc.URLPathS3.Join(bck.Name)+"?"+apc.QparamMptUploads, http.NoBody)
	tassert.CheckFatal(t, err)
	api.SetAuxHeaders(req, &bp)
	resp, err := bp.Client.Do(req)
	tassert.CheckFatal(t, err)
	defer resp.Body.Close()
	tassert.Fatalf(t, resp.StatusCode == http.StatusOK, "list multipart uploads returned status %d", resp.StatusCode)
	var uploads s3compat.ListMptUploadsResult
	tassert.CheckFatal(t, xml.NewDecoder(resp.Body).Decode(&uploads))
	for _, upload := range uploads.Uploads {
		tassert.Fatalf(t, upload.Key != objName, "%s: upload %q remains active", bck.Cname(objName), upload.UploadID)
	}
}

// putObjectsAndCheckPolicy issues regular, non-multipart PUTs and verifies the
// stored layout on the requested side of chunks.objsize_limit.
func putObjectsAndCheckPolicy(t *testing.T, bck cmn.Bck, objSize int64, chunked bool) {
	t.Helper()
	m := ioContext{
		t: t, bck: bck, num: 5, prefix: t.Name() + "/" + trand.String(5) + "/",
		fileSize: uint64(objSize), fixedSize: true, ordered: true, chunksConf: &ioCtxChunksConf{},
	}
	m.init(true /*cleanup*/)
	m.puts()
	m.gets(nil, true /*withValidation*/)
	bp := tools.BaseAPIParams(m.proxyURL)
	for _, objName := range m.objNames {
		if chunked {
			checkObjectChunked(t, bp, bck, objName, chunkPolicySize)
		} else {
			checkObjectMonolithic(t, bp, bck, objName)
		}
	}
}

// coldGetObjectsAndCheckPolicy provisions monolithic remote objects, evicts
// them, and verifies that cold GET stores them according to the bucket policy.
func coldGetObjectsAndCheckPolicy(t *testing.T, bp api.BaseParams, bck cmn.Bck, objSize int64, chunked bool) {
	t.Helper()
	m := ioContext{
		t: t, bck: bck, num: 5, prefix: t.Name() + "/" + trand.String(5) + "/",
		fileSize: uint64(objSize), fixedSize: true, ordered: true, getErrIsFatal: true,
	}
	m.init(true /*cleanup*/)
	setBucketChunkPolicy(t, bp, bck, 0, chunkPolicySize)
	m.remotePuts(true /*evict*/)
	setBucketChunkPolicy(t, bp, bck, chunkPolicyLimit, chunkPolicySize)
	m.gets(nil, true /*withValidation*/)
	for _, objName := range m.objNames {
		if chunked {
			checkObjectChunked(t, bp, bck, objName, chunkPolicySize)
		} else {
			checkObjectMonolithic(t, bp, bck, objName)
		}
	}
}

func setBucketChunkPolicy(t *testing.T, bp api.BaseParams, bck cmn.Bck, sizeLimit, chunkSize int64) cmn.ChunksConf {
	t.Helper()
	p, err := api.HeadBucket(bp, bck, false /*dontAddRemote*/)
	tassert.CheckFatal(t, err)
	_, err = api.SetBucketProps(bp, bck, &cmn.BpropsToSet{Chunks: &cmn.ChunksConfToSet{
		ObjSizeLimit: apc.Ptr(cos.SizeIEC(sizeLimit)), ChunkSize: apc.Ptr(cos.SizeIEC(chunkSize)),
	}})
	tassert.CheckFatal(t, err)
	return p.Chunks
}

func checkObjectMonolithic(t *testing.T, bp api.BaseParams, bck cmn.Bck, objName string) {
	t.Helper()
	op, err := api.HeadObjectV2(bp, bck, objName, apc.JoinProps(apc.GetPropsSize, apc.GetPropsChunked), api.HeadArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, op.Chunks != nil && op.Chunks.ChunkCount == 0,
		"expected %s to be monolithic, got %+v", bck.Cname(objName), op.Chunks)
}

func checkObjectChunked(t *testing.T, bp api.BaseParams, bck cmn.Bck, objName string, chunkSize int64) {
	t.Helper()
	op, err := api.HeadObjectV2(bp, bck, objName, apc.JoinProps(apc.GetPropsSize, apc.GetPropsChunked), api.HeadArgs{})
	tassert.CheckFatal(t, err)
	wantCount := int(cos.DivCeil(op.Size, chunkSize))
	wantMaxSize := min(op.Size, chunkSize)
	tassert.Fatalf(t, op.Chunks != nil && op.Chunks.ChunkCount == wantCount && op.Chunks.MaxChunkSize == wantMaxSize,
		"%s: expected %d chunks of up to %s, got %+v",
		bck.Cname(objName), wantCount, cos.ToSizeIEC(wantMaxSize, 0), op.Chunks)
}
