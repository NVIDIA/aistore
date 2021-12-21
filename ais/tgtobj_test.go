// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"flag"
	"io"
	"net/http"
	"os"
	"path"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/mock"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/readers"
	"github.com/NVIDIA/aistore/fs"
)

const (
	testMountpath = "/tmp/ais-test-mpath" // mpath is created and deleted during the test
	testBucket    = "bck"
)

var (
	t *targetrunner

	// interface guard
	_ http.ResponseWriter = (*discardRW)(nil)
)

type (
	discardRW struct {
		w io.Writer
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

func TestMain(m *testing.M) {
	flag.Parse()

	// file system
	cos.CreateDir(testMountpath)
	defer os.RemoveAll(testMountpath)
	fs.TestNew(nil)
	fs.TestDisableValidation()
	_ = fs.CSM.RegisterContentType(fs.ObjectType, &fs.ObjectContentResolver{})
	_ = fs.CSM.RegisterContentType(fs.WorkfileType, &fs.WorkfileContentResolver{})

	// target
	config := cmn.GCO.Get()
	co := newConfigOwner(config)
	t = newTarget(co)
	t.name = cmn.Target
	t.initNetworks()
	tid, _ := initTID(config)
	t.si.Init(tid, cmn.Target)

	fs.Add(testMountpath, t.si.ID())

	t.httprunner.init(config)

	t.statsT = mock.NewStatsTracker()
	cluster.Init(t)

	bck := cluster.NewBck(testBucket, cmn.ProviderAIS, cmn.NsGlobal)
	bmd := newBucketMD()
	bmd.add(bck, &cmn.BucketProps{
		Cksum: cmn.CksumConf{
			Type: cos.ChecksumNone,
		},
	})
	t.owner.bmd.putPersist(bmd, nil)
	fs.CreateBucket("test", bck.Bck, false /*nilbmd*/)

	m.Run()
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
		b.Run(cos.B2S(bench.fileSize, 2), func(b *testing.B) {
			lom := cluster.AllocLOM("objname")
			defer cluster.FreeLOM(lom)
			err := lom.Init(cmn.Bck{Name: testBucket, Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal})
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				r, _ := readers.NewRandReader(bench.fileSize, cos.ChecksumNone)
				poi := &putObjInfo{
					atime:   time.Now(),
					t:       t,
					lom:     lom,
					r:       r,
					workFQN: path.Join(testMountpath, "objname.work"),
				}
				os.Remove(lom.FQN)
				b.StartTimer()

				_, err := poi.putObject()
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			os.Remove(lom.FQN)
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

	for _, bench := range benches {
		b.Run(cos.B2S(bench.fileSize, 2), func(b *testing.B) {
			lom := cluster.AllocLOM("objname")
			defer cluster.FreeLOM(lom)
			err := lom.Init(cmn.Bck{Name: testBucket, Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal})
			if err != nil {
				b.Fatal(err)
			}

			var hi handleInfo
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				r, _ := readers.NewRandReader(bench.fileSize, cos.ChecksumNone)
				aoi := &appendObjInfo{
					started: time.Now(),
					t:       t,
					lom:     lom,
					r:       r,
					op:      cmn.AppendOp,
					hi:      hi,
				}
				os.Remove(lom.FQN)
				b.StartTimer()

				newHandle, _, err := aoi.appendObject()
				if err != nil {
					b.Fatal(err)
				}
				hi, err = parseAppendHandle(newHandle)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			os.Remove(lom.FQN)
			os.Remove(hi.filePath)
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
		benchName := cos.B2S(bench.fileSize, 2)
		if bench.chunked {
			benchName += "-chunked"
		}
		b.Run(benchName, func(b *testing.B) {
			lom := cluster.AllocLOM("objname")
			defer cluster.FreeLOM(lom)
			err := lom.Init(cmn.Bck{Name: testBucket, Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal})
			if err != nil {
				b.Fatal(err)
			}

			r, _ := readers.NewRandReader(bench.fileSize, cos.ChecksumNone)
			poi := &putObjInfo{
				atime:   time.Now(),
				t:       t,
				lom:     lom,
				r:       r,
				workFQN: path.Join(testMountpath, "objname.work"),
			}
			_, err = poi.putObject()
			if err != nil {
				b.Fatal(err)
			}

			if err := lom.Load(false, false); err != nil {
				b.Fatal(err)
			}

			w := io.Discard
			if !bench.chunked {
				w = newDiscardRW()
			}

			goi := &getObjInfo{
				started: time.Now(),
				t:       t,
				lom:     lom,
				w:       w,
				chunked: bench.chunked,
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := goi.getObject()
				if err != nil {
					b.Fatal(err)
				}
			}

			b.StopTimer()
			os.Remove(lom.FQN)
		})
	}
}
