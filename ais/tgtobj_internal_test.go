// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"flag"
	"io"
	"net/http"
	"os"
	"path"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
)

const (
	testMountpath = "/tmp/ais-test-mpath" // mpath is created and deleted during the test
	testBucket    = "bck"
)

var (
	t *target

	// interface guard
	_ http.ResponseWriter = (*discardRW)(nil)
)

type (
	discardRW struct {
		w io.Writer
	}
	failingTestROC struct {
		err error
	}
)

func newDiscardRW() *discardRW {
	return &discardRW{
		w: io.Discard,
	}
}

func (drw *discardRW) Write(p []byte) (int, error) { return drw.w.Write(p) }
func (*discardRW) Header() http.Header             { return make(http.Header) }
func (*discardRW) WriteHeader(int)                 {}

func (r *failingTestROC) Read([]byte) (int, error) { return 0, r.err }
func (*failingTestROC) Close() error               { return nil }
func (r *failingTestROC) Open() (cos.ReadOpenCloser, error) {
	return &failingTestROC{err: r.err}, nil
}

func TestMain(m *testing.M) {
	flag.Parse()

	// file system
	cos.CreateDir(testMountpath)
	defer os.RemoveAll(testMountpath)
	fs.TestNew(nil)

	// target
	config := cmn.GCO.Get()
	config.Log.Level = "3"
	co := newConfigOwner(config)
	t = newTarget(co)
	t.initPhase1(config)
	tid, _ := initTID(config)
	t.si.Init(tid, apc.Target)

	fs.Add(testMountpath, t.SID())

	t.htrun.initPhase2(config)
	t.ups.t = t

	t.statsT = mock.NewStatsTracker()
	core.Tinit(t, config, false)

	bck := meta.NewBck(testBucket, apc.AIS, cmn.NsGlobal)
	bmd := newBucketMD()
	bmd.add(bck, &cmn.Bprops{
		Cksum: cmn.CksumConf{
			Type: cos.ChecksumNone,
		},
	})
	t.owner.bmd.putPersist(bmd, nil)
	fs.CreateBucket(bck.Bucket(), false /*nilbmd*/)

	m.Run()
}

func TestPutObjectChunks(tst *testing.T) {
	tests := []struct {
		name          string
		dataSize      int64
		chunkSize     int64
		expectedParts int
	}{
		{
			name:          "single chunk",
			dataSize:      100,
			chunkSize:     200,
			expectedParts: 1,
		},
		{
			name:          "exact multiple chunks",
			dataSize:      600,
			chunkSize:     200,
			expectedParts: 3,
		},
		{
			name:          "partial last chunk",
			dataSize:      550,
			chunkSize:     200,
			expectedParts: 3,
		},
		{
			name:          "many small chunks",
			dataSize:      1000,
			chunkSize:     100,
			expectedParts: 10,
		},
	}

	for _, tt := range tests {
		tst.Run(tt.name, func(test *testing.T) {
			// Create a random reader
			reader, _ := readers.New(&readers.Arg{Type: readers.Rand, Size: tt.dataSize, CksumType: cos.ChecksumNone})

			// Set up LOM (using global testBucket that was created in TestMain)
			lom := core.AllocLOM("test-chunked-obj-" + tt.name)
			defer core.FreeLOM(lom)
			err := lom.InitBck(&meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
			tassert.CheckFatal(test, err)
			defer lom.RemoveMain()

			// Create putOI instance using global 't' (target) from TestMain
			config := cmn.GCO.Get()
			poi := &putOI{
				atime:   time.Now().UnixNano(),
				t:       t, // Reuse global target initialized in TestMain
				lom:     lom,
				r:       reader,
				oreq:    &http.Request{Header: make(http.Header)},
				workFQN: path.Join(testMountpath, "test-chunked-obj.work"),
				config:  config,
				size:    tt.dataSize,
			}

			_, err = poi.chunk(tt.chunkSize)
			tassert.CheckFatal(test, err)

			// Verify the object was created
			err = lom.Load(false, false)
			tassert.CheckFatal(test, err)

			lom.Lock(false)
			defer lom.Unlock(false)

			// Verify the object size matches
			tassert.Fatalf(test, lom.Lsize() == tt.dataSize, "object size mismatch: expected %d, got %d", tt.dataSize, lom.Lsize())

			// Verify object is marked as chunked
			tassert.Fatalf(test, lom.IsChunked(), "expected object to be marked as chunked")

			// Verify chunk count
			manifest, err := core.NewUfest("", lom, true /*must-exist*/)
			tassert.CheckFatal(test, err)
			err = manifest.LoadCompleted(lom)
			tassert.CheckFatal(test, err)
			actualChunks := manifest.Count()
			tassert.Fatalf(test, actualChunks == tt.expectedParts, "chunk count mismatch: expected %d parts, got %d", tt.expectedParts, actualChunks)
		})
	}
}

func TestCopyReaderPreservesRemoteSourceContext(tst *testing.T) {
	src := core.AllocLOM("copy-remote-source")
	defer core.FreeLOM(src)
	err := src.InitBck(&meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
	tassert.CheckFatal(tst, err)

	dst := core.AllocLOM("copy-remote-dst")
	defer core.FreeLOM(dst)
	err = dst.InitBck(&meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
	tassert.CheckFatal(tst, err)
	defer dst.RemoveMain()

	remoteSrc := &core.RemoteSource{
		Provider: "s3",
		Bucket:   "s3://remote-bck",
		Object:   src.ObjName,
	}
	c := &coi{
		Config: cmn.GCO.Get(),
		OWT:    cmn.OwtCopy,
		GetROC: func(*core.LOM, bool, bool, *core.ETLArgs) core.ReadResp {
			return core.ReadResp{
				R:         &failingTestROC{err: io.ErrUnexpectedEOF},
				OAH:       &cmn.ObjAttrs{Size: 1024, Atime: time.Now().UnixNano(), Cksum: cos.NewCksum(cos.ChecksumNone, "")},
				RemoteSrc: remoteSrc,
				Remote:    true,
			}
		},
	}

	res := c._reader(t, nil /*dm*/, src, dst, nil /*args*/)
	if !errors.Is(res.Err, io.ErrUnexpectedEOF) {
		tst.Fatalf("expected unexpected EOF cause, got %v", res.Err)
	}
	if res.RemoteSrc == nil {
		tst.Fatal("expected remote source context")
	}
	if res.RemoteSrc.Provider != "s3" || res.RemoteSrc.Bucket != "s3://remote-bck" || res.RemoteSrc.Object != src.ObjName {
		tst.Fatalf("unexpected remote source context: %+v", res.RemoteSrc)
	}
}

func BenchmarkObjPut(b *testing.B) {
	benches := []struct {
		fileSize int64
	}{
		{cos.KiB},
		{512 * cos.KiB},
		{cos.MiB},
		{2 * cos.MiB},
		{4 * cos.MiB},
		{8 * cos.MiB},
		{16 * cos.MiB},
	}
	for _, bench := range benches {
		b.Run(cos.IEC(bench.fileSize, 2), func(b *testing.B) {
			lom := core.AllocLOM("objname")
			defer core.FreeLOM(lom)
			err := lom.InitBck(&meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
			if err != nil {
				b.Fatal(err)
			}

			for b.Loop() {
				b.StopTimer()
				r, _ := readers.New(&readers.Arg{Type: readers.Rand, Size: bench.fileSize, CksumType: cos.ChecksumNone})
				poi := &putOI{
					atime:   time.Now().UnixNano(),
					t:       t,
					lom:     lom,
					r:       r,
					workFQN: path.Join(testMountpath, "objname.work"),
					config:  cmn.GCO.Get(),
				}
				lom.RemoveMain()
				b.StartTimer()

				_, err := poi.putObject()
				if err != nil {
					b.Fatal(err)
				}
			}
			lom.RemoveMain()
		})
	}
}

func BenchmarkObjAppend(b *testing.B) {
	benches := []struct {
		fileSize int64
	}{
		{fileSize: cos.KiB},
		{fileSize: 512 * cos.KiB},
		{fileSize: cos.MiB},
		{fileSize: 2 * cos.MiB},
		{fileSize: 4 * cos.MiB},
		{fileSize: 8 * cos.MiB},
		{fileSize: 16 * cos.MiB},
	}

	buf := make([]byte, 16*cos.KiB)
	for _, bench := range benches {
		b.Run(cos.IEC(bench.fileSize, 2), func(b *testing.B) {
			lom := core.AllocLOM("objname")
			defer core.FreeLOM(lom)
			err := lom.InitBck(&meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
			if err != nil {
				b.Fatal(err)
			}

			var hdl aoHdl
			for b.Loop() {
				b.StopTimer()
				r, _ := readers.New(&readers.Arg{Type: readers.Rand, Size: bench.fileSize, CksumType: cos.ChecksumNone})
				aoi := &apndOI{
					started: time.Now().UnixNano(),
					t:       t,
					lom:     lom,
					r:       r,
					op:      apc.AppendOp,
					hdl:     hdl,
				}
				lom.RemoveMain()
				b.StartTimer()

				newHandle, err := aoi.apnd(buf)
				if err != nil {
					b.Fatal(err)
				}
				err = aoi.parse(newHandle)
				if err != nil {
					b.Fatal(err)
				}
			}
			lom.RemoveMain()
			os.Remove(hdl.workFQN)
		})
	}
}

func BenchmarkObjGetDiscard(b *testing.B) {
	benches := []struct {
		fileSize int64
		chunked  bool
	}{
		{fileSize: cos.KiB, chunked: false},
		{fileSize: 512 * cos.KiB, chunked: false},
		{fileSize: cos.MiB, chunked: false},
		{fileSize: 2 * cos.MiB, chunked: false},
		{fileSize: 4 * cos.MiB, chunked: false},
		{fileSize: 16 * cos.MiB, chunked: false},

		{fileSize: cos.KiB, chunked: true},
		{fileSize: 512 * cos.KiB, chunked: true},
		{fileSize: cos.MiB, chunked: true},
		{fileSize: 2 * cos.MiB, chunked: true},
		{fileSize: 4 * cos.MiB, chunked: true},
		{fileSize: 16 * cos.MiB, chunked: true},
	}

	for _, bench := range benches {
		benchName := cos.IEC(bench.fileSize, 2)
		if bench.chunked {
			benchName += "-chunked"
		}
		b.Run(benchName, func(b *testing.B) {
			lom := core.AllocLOM("objname")
			defer core.FreeLOM(lom)
			err := lom.InitBck(&meta.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
			if err != nil {
				b.Fatal(err)
			}

			r, _ := readers.New(&readers.Arg{Type: readers.Rand, Size: bench.fileSize, CksumType: cos.ChecksumNone})
			poi := &putOI{
				atime:   time.Now().UnixNano(),
				t:       t,
				lom:     lom,
				r:       r,
				workFQN: path.Join(testMountpath, "objname.work"),
				config:  cmn.GCO.Get(),
			}
			_, err = poi.putObject()
			if err != nil {
				b.Fatal(err)
			}

			if err := lom.Load(false, false); err != nil {
				b.Fatal(err)
			}

			w := newDiscardRW()
			goi := &getOI{
				atime:   time.Now().UnixNano(),
				t:       t,
				lom:     lom,
				w:       w,
				chunked: bench.chunked,
			}

			for b.Loop() {
				_, err := goi.getObject()
				if err != nil {
					b.Fatal(err)
				}
			}

			lom.RemoveMain()
		})
	}
}
