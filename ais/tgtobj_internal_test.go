// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
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

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/core/mock"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/tools/readers"
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
	fs.CSM.Reg(fs.ObjectType, &fs.ObjectContentResolver{}, true)
	fs.CSM.Reg(fs.WorkfileType, &fs.WorkfileContentResolver{}, true)

	// target
	config := cmn.GCO.Get()
	config.Log.Level = "3"
	co := newConfigOwner(config)
	t = newTarget(co)
	t.initSnode(config)
	tid, _ := initTID(config)
	t.si.Init(tid, apc.Target)

	fs.Add(testMountpath, t.SID())

	t.htrun.init(config)

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
		b.Run(cos.ToSizeIEC(bench.fileSize, 2), func(b *testing.B) {
			lom := core.AllocLOM("objname")
			defer core.FreeLOM(lom)
			err := lom.InitBck(&cmn.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				r, _ := readers.NewRand(bench.fileSize, cos.ChecksumNone)
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
			b.StopTimer()
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
		b.Run(cos.ToSizeIEC(bench.fileSize, 2), func(b *testing.B) {
			lom := core.AllocLOM("objname")
			defer core.FreeLOM(lom)
			err := lom.InitBck(&cmn.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
			if err != nil {
				b.Fatal(err)
			}

			var hdl aoHdl
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				r, _ := readers.NewRand(bench.fileSize, cos.ChecksumNone)
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

				newHandle, _, err := aoi.apnd(buf)
				if err != nil {
					b.Fatal(err)
				}
				err = aoi.parse(newHandle)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
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
		benchName := cos.ToSizeIEC(bench.fileSize, 2)
		if bench.chunked {
			benchName += "-chunked"
		}
		b.Run(benchName, func(b *testing.B) {
			lom := core.AllocLOM("objname")
			defer core.FreeLOM(lom)
			err := lom.InitBck(&cmn.Bck{Name: testBucket, Provider: apc.AIS, Ns: cmn.NsGlobal})
			if err != nil {
				b.Fatal(err)
			}

			r, _ := readers.NewRand(bench.fileSize, cos.ChecksumNone)
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

			b.ResetTimer()
			for range b.N {
				_, err := goi.getObject()
				if err != nil {
					b.Fatal(err)
				}
			}

			b.StopTimer()
			lom.RemoveMain()
		})
	}
}
