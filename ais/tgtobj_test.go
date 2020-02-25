// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"flag"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/tutils"
)

const (
	testMountpath = "/tmp"
	testBucket    = "bck"
)

var (
	t *targetrunner

	// interface guard
	_ http.ResponseWriter = &discardRW{}
)

type (
	discardRW struct {
		w io.Writer
	}
)

func newDiscardRW() *discardRW {
	return &discardRW{
		w: ioutil.Discard,
	}
}

func (drw *discardRW) Write(p []byte) (int, error) { return drw.w.Write(p) }
func (drw *discardRW) Header() http.Header         { return make(http.Header) }
func (drw *discardRW) WriteHeader(statusCode int)  {}

func TestMain(m *testing.M) {
	flag.Parse()

	// file system
	fs.Mountpaths = fs.NewMountedFS()
	fs.Mountpaths.DisableFsIDCheck()
	fs.Mountpaths.Add(testMountpath)
	_ = fs.CSM.RegisterContentType(fs.ObjectType, &fs.ObjectContentResolver{})
	_ = fs.CSM.RegisterContentType(fs.WorkfileType, &fs.WorkfileContentResolver{})

	// memory
	daemon.gmm = memsys.DefaultPageMM()

	// target
	t = &targetrunner{}
	t.initSI(cmn.Target)
	t.init(nil, cmn.GCO.Get())
	t.owner.bmd = newBMDOwnerTgt()

	bmd := newBucketMD()
	bmd.add(cluster.NewBck(testBucket, cmn.ProviderAIS, cmn.NsGlobal), &cmn.BucketProps{
		Cksum: cmn.CksumConf{
			Type: cmn.ChecksumNone,
		},
	})
	t.owner.bmd.put(bmd)
	t.statsT = stats.NewTrackerMock()

	os.Exit(m.Run())
}

func BenchmarkObjPut(b *testing.B) {
	benches := []struct {
		fileSize int64
	}{
		{cmn.KiB},
		{512 * cmn.KiB},
		{cmn.MiB},
		{2 * cmn.MiB},
		{4 * cmn.MiB},
		{8 * cmn.MiB},
		{16 * cmn.MiB},
	}

	for _, bench := range benches {
		b.Run(cmn.B2S(bench.fileSize, 2), func(b *testing.B) {
			lom := &cluster.LOM{T: t, Objname: "objname"}
			err := lom.Init(cmn.Bck{Name: testBucket, Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal})
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				r, _ := tutils.NewRandReader(bench.fileSize, false)
				poi := &putObjInfo{
					started: time.Now(),
					t:       t,
					lom:     lom,
					r:       r,
					workFQN: path.Join(testMountpath, "objname.work"),
				}
				os.Remove(lom.FQN)
				b.StartTimer()

				err, _ := poi.putObject()
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
		{cmn.KiB},
		{512 * cmn.KiB},
		{cmn.MiB},
		{2 * cmn.MiB},
		{4 * cmn.MiB},
		{8 * cmn.MiB},
		{16 * cmn.MiB},
	}

	for _, bench := range benches {
		b.Run(cmn.B2S(bench.fileSize, 2), func(b *testing.B) {
			lom := &cluster.LOM{T: t, Objname: "objname"}
			err := lom.Init(cmn.Bck{Name: testBucket, Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal})
			if err != nil {
				b.Fatal(err)
			}

			var filePath string
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				r, _ := tutils.NewRandReader(bench.fileSize, false)
				aoi := &appendObjInfo{
					started:  time.Now(),
					t:        t,
					lom:      lom,
					r:        r,
					op:       cmn.AppendOp,
					filePath: filePath,
				}
				os.Remove(lom.FQN)
				b.StartTimer()

				filePath, err, _ = aoi.appendObject()
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			os.Remove(lom.FQN)
			os.Remove(filePath)
		})
	}
}

func BenchmarkObjGetDiscard(b *testing.B) {
	benches := []struct {
		fileSize int64
		chunked  bool
	}{
		{fileSize: cmn.KiB, chunked: false},
		{fileSize: 512 * cmn.KiB, chunked: false},
		{fileSize: cmn.MiB, chunked: false},
		{fileSize: 2 * cmn.MiB, chunked: false},
		{fileSize: 4 * cmn.MiB, chunked: false},
		{fileSize: 16 * cmn.MiB, chunked: false},

		{fileSize: cmn.KiB, chunked: true},
		{fileSize: 512 * cmn.KiB, chunked: true},
		{fileSize: cmn.MiB, chunked: true},
		{fileSize: 2 * cmn.MiB, chunked: true},
		{fileSize: 4 * cmn.MiB, chunked: true},
		{fileSize: 16 * cmn.MiB, chunked: true},
	}

	for _, bench := range benches {
		benchName := cmn.B2S(bench.fileSize, 2)
		if bench.chunked {
			benchName += "-chunked"
		}
		b.Run(benchName, func(b *testing.B) {
			lom := &cluster.LOM{T: t, Objname: "objname"}
			err := lom.Init(cmn.Bck{Name: testBucket, Provider: cmn.ProviderAIS, Ns: cmn.NsGlobal})
			if err != nil {
				b.Fatal(err)
			}

			r, _ := tutils.NewRandReader(bench.fileSize, false)
			poi := &putObjInfo{
				started: time.Now(),
				t:       t,
				lom:     lom,
				r:       r,
				workFQN: path.Join(testMountpath, "objname.work"),
			}
			err, _ = poi.putObject()
			if err != nil {
				b.Fatal(err)
			}

			// load lom
			if err := lom.Load(); err != nil {
				b.Fatal(err)
			}

			w := ioutil.Discard
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
				err, _ := goi.getObject()
				if err != nil {
					b.Fatal(err)
				}
			}

			b.StopTimer()
			os.Remove(lom.FQN)
		})
	}
}
