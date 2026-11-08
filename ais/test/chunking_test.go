// Package integration_test.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	s3compat "github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
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
	cbck := meta.CloneBck(&bck)
	owner, err := smap.HrwObjName2T(cbck, objects.objNames[0])
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
		previous, err := smap.HrwObjName2T(cbck, objName)
		tassert.CheckFatal(t, err)
		if previous.ID() != owner.ID() {
			// Unmoved objects retain the client's two 48KiB parts.
			checkObjectChunked(t, bp, bck, objName, multipartChunkSize)
			continue
		}
		// Moved objects must use the bucket's three 32KiB chunks.
		receiver, err := updated.HrwObjName2T(cbck, objName)
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

func TestColdGetRangesRemoteDelete(t *testing.T) {
	const (
		objSize   = 64 * cos.MiB
		chunkSize = 4 * cos.MiB
	)
	tools.CheckSkip(t, &tools.SkipTestArgs{
		Long: true, Bck: cliBck, RequiredCloudProvider: apc.GCP, RequiredDeployment: tools.ClusterTypeLocal,
	})
	proxyURL := tools.RandomProxyURL(t)
	bp := tools.BaseAPIParams(proxyURL)
	initMountpaths(t, proxyURL)
	for _, tc := range []struct {
		name   string
		chunks int
	}{
		{name: "first-ranges", chunks: 1},
		{name: "later-ranges", chunks: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bck := cmn.Bck{Name: "cold-range-delete-" + trand.String(8), Provider: apc.AIS}
			tools.CreateBucket(t, proxyURL, bck, &cmn.BpropsToSet{
				Chunks: &cmn.ChunksConfToSet{ObjSizeLimit: apc.Ptr(cos.SizeIEC(chunkSize)), ChunkSize: apc.Ptr(cos.SizeIEC(chunkSize))},
				Cksum:  &cmn.CksumConfToSet{ValidateColdGet: apc.Ptr(false)},
			}, true /*cleanup*/)
			tools.SetBackendBck(t, bp, bck, cliBck)
			objects := ioContext{
				t: t, bck: cliBck, num: 1, fileSize: objSize, fixedSize: true,
				prefix: t.Name() + "/" + trand.String(8) + "/", skipRemoteEvict: true,
				chunksConf: &ioCtxChunksConf{},
			}
			objects.init(false)
			objects.remotePuts(false /*evict*/)
			objName := objects.objNames[0]
			t.Cleanup(func() { tassert.CheckError(t, api.DeleteObject(bp, cliBck, objName)) })
			tlog.Logfln("delete during parallel cold GET: %s", cliBck.Cname(objName))
			get := ioContext{t: t, bck: bck, num: 1}
			get.init(false)
			get.objNames = objects.objNames
			get.otherTasksToTrigger = 1
			get.controlCh = make(chan struct{}, 1)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			finished := false
			t.Cleanup(func() {
				cancel()
				if !finished {
					<-get.controlCh
				}
			})
			go get.gets(&api.GetArgs{Context: ctx}, false)
			err := tools.WaitForCondition(func() bool {
				return len(get.findObjChunksOnDisk(bck, objName)) >= tc.chunks
			}, tools.WaitRetryOpts{MaxRetries: 1500, Interval: 20 * time.Millisecond})
			tassert.CheckFatal(t, err)
			lock, err := api.CheckObjectLock(bp, bck, objName)
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, lock == apc.LockWrite, "expected active cold GET, got lock %d", lock)
			// The backend bucket has a separate lock, so deletion can interrupt pinned ranges.
			tassert.CheckFatal(t, api.DeleteObject(bp, cliBck, objName))
			select {
			case <-get.controlCh:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			finished = true
			tassert.Fatalf(t, get.numGetErrs.Load() == 1, "expected remote deletion to fail the cold GET")
			lock, err = api.CheckObjectLock(bp, bck, objName)
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, lock == apc.LockNone, "expected released lock, got %d", lock)
			_, err = api.HeadObjectV2(bp, bck, objName, apc.GetPropsName, api.HeadArgs{FltPresence: apc.FltPresent})
			tassert.Fatalf(t, api.HTTPStatus(err) == http.StatusNotFound, "failed GET cached an object: %v", err)
			get.validateChunksOnDisk(bck, objName, 0)
			_, err = api.GetObject(bp, bck, objName, nil)
			tassert.Fatalf(t, api.HTTPStatus(err) == http.StatusNotFound, "expected deleted remote object, got %v", err)

			replacement := bytes.Repeat([]byte{'r'}, cos.KiB)
			tools.PutObject(t, cliBck, objName, readers.NewBytes(replacement), uint64(len(replacement)))
			var out bytes.Buffer
			_, err = api.GetObjectWithValidation(bp, bck, objName, &api.GetArgs{Writer: &out})
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, bytes.Equal(out.Bytes(), replacement), "recovery GET returned stale or incomplete data")
		})
	}
}

func TestRemaisColdGetRanges(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiresRemoteCluster: true, RequiredDeployment: tools.ClusterTypeLocal})
	tools.WaitForRebalAndResil(t, tools.BaseAPIParams(tools.RandomProxyURL(t)))
	// give proxies time to refresh the restored route after the last subtest
	var restoreDelay time.Duration
	t.Cleanup(func() { time.Sleep(restoreDelay) })

	const chunkSize = 4 * cos.MiB
	tests := []struct {
		name, cksum, fault string
		remoteCksum        string
		size, chunk        int64
		parallel           bool
		validate, noPin    bool
		latest, refetch    bool
		validateWarm, sync bool
		checkLatest        bool
		code               int
	}{
		{name: "crc32c", cksum: cos.ChecksumOneXxh, remoteCksum: cos.ChecksumNone, parallel: true},
		{name: "remote-checksum", cksum: cos.ChecksumSHA256, parallel: true, checkLatest: true},
		{name: "remote-sha256", cksum: cos.ChecksumOneXxh, remoteCksum: cos.ChecksumSHA256, parallel: true, checkLatest: true},
		{name: "none", cksum: cos.ChecksumNone, parallel: true},
		{name: "long-name", cksum: cos.ChecksumOneXxh, parallel: true},
		{name: "small", cksum: cos.ChecksumOneXxh, size: 2*chunkSize + 17},
		{name: "one-chunk", cksum: cos.ChecksumOneXxh, chunk: 64 * cos.MiB},
		{name: "validate", cksum: cos.ChecksumOneXxh, validate: true},
		{name: "latest", cksum: cos.ChecksumOneXxh, latest: true, parallel: true},
		{name: "validate-warm-get", cksum: cos.ChecksumOneXxh, validateWarm: true, parallel: true},
		{name: "synchronize", cksum: cos.ChecksumOneXxh, sync: true, parallel: true},
		{name: "no-pin", cksum: cos.ChecksumOneXxh, noPin: true},
		// remote change mid-fill: the same GET refetches the new revision sequentially
		{name: "version-change", cksum: cos.ChecksumOneXxh, parallel: true, fault: "overwrite", refetch: true},
		{name: "custom-version", cksum: cos.ChecksumOneXxh, parallel: true, fault: "overwrite", refetch: true},
		{name: "ignored-range", cksum: cos.ChecksumNone, parallel: true, fault: "ignore", code: http.StatusBadGateway},
		{name: "short-range", cksum: cos.ChecksumOneXxh, parallel: true, fault: "short", code: http.StatusInternalServerError},
		// remote 429 without rate_limit.backend: sequential refetch, then backoff (keep it last)
		{name: "throttled", cksum: cos.ChecksumOneXxh, parallel: true, fault: "throttle"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				proxyURL    = tools.RandomProxyURL(t)
				bp          = tools.BaseAPIParams(proxyURL)
				remoteBP    = tools.BaseAPIParams(tools.RemoteCluster.URL)
				remoteBck   = cmn.Bck{Name: "crange-" + trand.String(8), Provider: apc.AIS}
				bck         = cmn.Bck{Name: remoteBck.Name, Provider: apc.AIS, Ns: cmn.Ns{UUID: tools.RemoteCluster.UUID}}
				objName     = "object"
				size        = tc.size
				chunk       = tc.chunk
				remoteCksum = tc.remoteCksum
			)
			if remoteCksum == "" {
				remoteCksum = cos.ChecksumOneXxh
			}
			if size == 0 {
				size = 8*chunkSize + 17
			}
			if chunk == 0 {
				chunk = chunkSize
			}
			if tc.name == "long-name" {
				objName = strings.Repeat("x", 280)
			}
			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i/chunkSize + 1)
			}
			replacement := bytes.Repeat([]byte{'z'}, len(data))
			tools.CreateBucket(t, remoteBP.URL, remoteBck, &cmn.BpropsToSet{
				Versioning: &cmn.VersionConfToSet{Enabled: apc.Ptr(!tc.noPin)},
				Cksum:      &cmn.CksumConfToSet{Type: &remoteCksum},
			}, true /*cleanup*/)
			put := func(data []byte) error {
				args := &api.PutArgs{BaseParams: remoteBP, Bck: remoteBck, ObjName: objName,
					Reader: readers.NewBytes(data), Size: uint64(len(data))}
				if tc.name == "custom-version" {
					args.Header = http.Header{apc.HdrObjCustomMD: []string{cmn.VersionObjMD + "=custom"}}
				}
				_, err := api.PutObject(args)
				return err
			}
			tassert.CheckFatal(t, put(data))
			_, err := api.HeadBucket(bp, bck, false)
			tassert.CheckFatal(t, err)
			t.Cleanup(func() { tassert.CheckError(t, api.EvictRemoteBucket(bp, bck, false)) })
			_, err = api.SetBucketProps(bp, bck, &cmn.BpropsToSet{
				Chunks:     &cmn.ChunksConfToSet{ObjSizeLimit: apc.Ptr(cos.SizeIEC(chunkSize)), ChunkSize: apc.Ptr(cos.SizeIEC(chunk))},
				Cksum:      &cmn.CksumConfToSet{Type: &tc.cksum, ValidateColdGet: &tc.validate, ValidateWarmGet: apc.Ptr(false)},
				Versioning: &cmn.VersionConfToSet{ValidateWarmGet: &tc.validateWarm, Sync: &tc.sync},
			})
			tassert.CheckFatal(t, err)
			initMountpaths(t, proxyURL)

			remoteURL, err := url.Parse(remoteBP.URL)
			tassert.CheckFatal(t, err)
			forward := httputil.NewSingleHostReverseProxy(remoteURL)
			forward.Transport = remoteBP.Client.Transport
			var full, heads, ranges, active, peak atomic.Int64
			var overwrite sync.Once
			var allowRange atomic.Bool
			gate := make(chan struct{})
			// match the remote scheme (see TestRemaisBlobDownloadVersionPin)
			newBridge := httptest.NewServer
			if cos.IsHTTPS(remoteBP.URL) {
				newBridge = httptest.NewTLSServer
			}
			bridge := newBridge(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if !strings.HasSuffix(req.URL.Path, "/"+remoteBck.Name+"/"+objName) {
					forward.ServeHTTP(w, req)
					return
				}
				if req.Method == http.MethodHead {
					heads.Add(1)
				}
				if req.Method != http.MethodGet || req.Header.Get(cos.HdrRange) == "" {
					if req.Method == http.MethodGet {
						full.Add(1)
					}
					forward.ServeHTTP(w, req)
					return
				}
				n := active.Add(1)
				defer active.Add(-1)
				for prev := peak.Load(); n > prev; prev = peak.Load() {
					if peak.CompareAndSwap(prev, n) {
						break
					}
				}
				if ranges.Add(1) == 2 {
					close(gate)
				}
				if tc.fault == "" {
					// hold the first range request until a second reader reaches the bridge
					select {
					case <-gate:
					case <-req.Context().Done():
						return
					case <-time.After(15 * time.Second):
						http.Error(w, "parallel readers did not arrive", http.StatusGatewayTimeout)
						return
					}
				}
				switch tc.fault {
				case "overwrite":
					overwrite.Do(func() {
						if err := put(replacement); err != nil {
							t.Errorf("overwrite: %v", err)
						}
					})
				case "ignore":
					w.Header().Set(cos.HdrContentLength, strconv.FormatInt(size, 10))
					w.Header().Set(apc.HdrObjVersion, "1")
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write(data)
					return
				case "throttle":
					if !allowRange.Load() {
						http.Error(w, "slow down", http.StatusTooManyRequests)
						return
					}
				case "short":
					var off, end int64
					_, _ = fmt.Sscanf(req.Header.Get(cos.HdrRange), "bytes=%d-%d", &off, &end)
					w.Header().Set(cos.HdrContentLength, strconv.FormatInt(end-off+1, 10))
					w.Header().Set(apc.HdrObjVersion, "1")
					w.WriteHeader(http.StatusPartialContent)
					_, _ = w.Write(data[:min(1024, end-off)])
					return
				}
				forward.ServeHTTP(w, req)
			}))
			t.Cleanup(bridge.Close)
			config, err := api.GetClusterConfig(bp)
			tassert.CheckFatal(t, err)
			backend := config.Backend
			backend.Conf = maps.Clone(backend.Conf)
			backend.Set(apc.AIS, cmn.BackendConfAIS{"crange-" + trand.String(8): []string{bridge.URL}})
			t.Cleanup(func() {
				err := api.SetClusterConfigUsingMsg(bp, &cmn.ConfigToSet{Backend: &config.Backend}, false)
				tassert.CheckError(t, err)
				if err == nil {
					restoreDelay = 2 * config.Timeout.CplaneOperation.D()
				}
			})
			tassert.CheckFatal(t, api.SetClusterConfigUsingMsg(bp, &cmn.ConfigToSet{Backend: &backend}, false))

			var out bytes.Buffer
			args := &api.GetArgs{Writer: &out}
			if tc.latest {
				args.Query = url.Values{apc.QparamLatestVer: []string{"true"}}
			}
			_, err = api.GetObject(bp, bck, objName, args)
			m := ioContext{t: t, bck: bck}
			if tc.refetch {
				tassert.CheckFatal(t, err)
				tassert.Fatalf(t, bytes.Equal(out.Bytes(), replacement), "refetch returned mixed or stale revisions")
				tassert.Fatalf(t, full.Load() == 2 && heads.Load() == 0, "expected two full GETs and no HEAD: GET=%d HEAD=%d",
					full.Load(), heads.Load())
				m.validateChunksOnDisk(bck, objName, int(cos.DivCeil(size, chunk)))
				tlog.Logfln("%s: full GETs=%d, ranges=%d", tc.name, full.Load(), ranges.Load())
				return
			}
			if tc.fault == "throttle" {
				// Wait out target-wide backoff even if an assertion fails.
				resumeAt := time.Now().Add(config.Timeout.ColdRangeBackoff())
				t.Cleanup(func() { time.Sleep(time.Until(resumeAt)) })
				tassert.CheckFatal(t, err)
				tassert.Fatalf(t, bytes.Equal(out.Bytes(), data), "throttled cold GET returned incorrect bytes")
				tassert.Fatalf(t, full.Load() == 2 && heads.Load() == 0, "expected two full GETs and no HEAD: GET=%d HEAD=%d",
					full.Load(), heads.Load())
				m.validateChunksOnDisk(bck, objName, int(cos.DivCeil(size, chunk)))

				// The next cold GET is sequential during backoff.
				tassert.CheckFatal(t, api.EvictObject(bp, bck, objName))
				nranges := ranges.Load()
				out.Reset()
				_, err = api.GetObject(bp, bck, objName, args)
				tassert.CheckFatal(t, err)
				tassert.Fatalf(t, bytes.Equal(out.Bytes(), data), "cold GET during backoff returned incorrect bytes")
				tassert.Fatalf(t, full.Load() == 3 && ranges.Load() == nranges,
					"expected a sequential cold GET during backoff: GET=%d, ranges=%d (before %d)", full.Load(), ranges.Load(), nranges)
				// Once backoff expires, the next cold GET uses range reads again.
				allowRange.Store(true)
				time.Sleep(time.Until(resumeAt))
				tassert.CheckFatal(t, api.EvictObject(bp, bck, objName))
				out.Reset()
				_, err = api.GetObject(bp, bck, objName, args)
				tassert.CheckFatal(t, err)
				tassert.Fatalf(t, bytes.Equal(out.Bytes(), data), "cold GET after backoff returned incorrect bytes")
				tassert.Fatalf(t, full.Load() == 4 && ranges.Load() > nranges,
					"expected parallel cold GET after backoff: GET=%d, ranges=%d (before %d)", full.Load(), ranges.Load(), nranges)
				return
			}
			if tc.code != 0 {
				tassert.Fatalf(t, err != nil, "expected HTTP %d", tc.code)
				tassert.Fatalf(t, cmn.AsErrHTTP(err).Status == tc.code, "expected HTTP %d, got %v", tc.code, err)
				tassert.Fatalf(t, !tools.CheckObjIsPresent(proxyURL, bck, objName), "failed fill published a cached object")
				m.validateChunksOnDisk(bck, objName, 0)
				return
			}
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, bytes.Equal(out.Bytes(), data), "cold GET returned incorrect bytes")
			tassert.Fatalf(t, full.Load() == 1 && heads.Load() == 0, "unexpected initial requests: GET=%d HEAD=%d", full.Load(), heads.Load())
			if tc.parallel {
				n := peak.Load()
				tassert.Fatalf(t, n >= 2 && n <= 4, "expected two to four concurrent readers, got %d", n)
				tassert.Fatalf(t, ranges.Load() == cos.DivCeil(size, chunk)-1, "unexpected range count %d", ranges.Load())
				op, err := api.HeadObject(bp, bck, objName, api.HeadArgs{FltPresence: apc.FltPresent})
				tassert.CheckFatal(t, err)
				if tc.cksum == cos.ChecksumNone {
					tassert.Fatalf(t, cos.NoneC(op.Cksum), "checksum=none stored %v", op.Cksum)
				} else {
					cksumType := remoteCksum
					if cksumType == cos.ChecksumNone {
						cksumType = cos.ChecksumCRC32C
					}
					cksum, err := cos.ChecksumBytes(data, cksumType)
					tassert.CheckFatal(t, err)
					tassert.Fatalf(t, cksum.Equal(op.Cksum), "expected whole-object %v, got %v", cksum, op.Cksum)
				}
			} else {
				tassert.Fatalf(t, ranges.Load() == 0, "fallback issued %d range requests", ranges.Load())
			}
			diskName := objName
			if fs.IsFntl(objName) {
				diskName = fs.ShortenFntl(objName)
			}
			m.validateChunksOnDisk(bck, diskName, int(cos.DivCeil(size, chunk)))
			before := full.Load() + ranges.Load()
			out.Reset()
			_, err = api.GetObject(bp, bck, objName, args)
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, bytes.Equal(out.Bytes(), data), "warm GET returned incorrect bytes")
			tassert.Fatalf(t, full.Load()+ranges.Load() == before, "warm GET downloaded the object again")
			if tc.checkLatest {
				// A latest-version check must recognize the preserved remote checksum.
				nheads := heads.Load()
				args.Query = url.Values{apc.QparamLatestVer: []string{"true"}}
				out.Reset()
				_, err = api.GetObject(bp, bck, objName, args)
				tassert.CheckFatal(t, err)
				tassert.Fatalf(t, bytes.Equal(out.Bytes(), data), "latest GET returned incorrect bytes")
				tassert.Fatalf(t, heads.Load() > nheads, "latest GET did not check remote metadata")
				tassert.Fatalf(t, full.Load()+ranges.Load() == before, "latest GET downloaded unchanged content again")
			}
			tlog.Logfln("%s: full GETs=%d, ranges=%d, peak=%d", tc.name, full.Load(), ranges.Load(), peak.Load())
		})
	}
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
