// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"io/ioutil"
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
)

func init() {
	// file system
	fs.Mountpaths = fs.NewMountedFS()
	fs.Mountpaths.DisableFsIDCheck()
	fs.Mountpaths.Add(testMountpath)
	_ = fs.CSM.RegisterFileType(fs.ObjectType, &fs.ObjectContentResolver{})
	_ = fs.CSM.RegisterFileType(fs.WorkfileType, &fs.WorkfileContentResolver{})

	// memory
	mem := &memsys.Mem2{MinPctTotal: 4, MinFree: cmn.GiB * 2} // free mem: try to maintain at least the min of these two
	_ = mem.Init(false)                                       // don't ignore init-time errors
	gmem2 = mem

	// target
	t = &targetrunner{}
	t.initSI(cmn.Target)
	t.init(nil)

	bmd := newBucketMD()
	bmd.add(testBucket, true, &cmn.BucketProps{
		Cksum: cmn.CksumConf{
			Type: cmn.ChecksumNone,
		},
	})
	t.bmdowner.put(bmd)
	t.statsif = stats.NewTrackerMock()
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
			lom, errstr := cluster.LOM{T: t, Objname: "objname", Bucket: testBucket, BucketProvider: cmn.LocalBs}.Init()
			if errstr != "" {
				b.Fatal(errstr)
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

func BenchmarkObjGetDiscard(b *testing.B) {
	benches := []struct {
		fileSize int64
	}{
		{cmn.KiB},
		{512 * cmn.KiB},
		{cmn.MiB},
		{2 * cmn.MiB},
		{4 * cmn.MiB},
		{16 * cmn.MiB},
	}

	for _, bench := range benches {
		b.Run(cmn.B2S(bench.fileSize, 2), func(b *testing.B) {
			lom, errstr := cluster.LOM{T: t, Objname: "objname", Bucket: testBucket, BucketProvider: cmn.LocalBs}.Init()
			if errstr != "" {
				b.Fatal(errstr)
			}

			r, _ := tutils.NewRandReader(bench.fileSize, false)
			poi := &putObjInfo{
				started: time.Now(),
				t:       t,
				lom:     lom,
				r:       r,
				workFQN: path.Join(testMountpath, "objname.work"),
			}
			err, _ := poi.putObject()
			if err != nil {
				b.Fatal(err)
			}

			// load lom
			if _, errstr := lom.Load(true); errstr != "" {
				b.Fatal(errstr)
			}

			w := ioutil.Discard
			goi := &getObjInfo{
				started: time.Now(),
				t:       t,
				lom:     lom,
				w:       w,
				ctx:     nil,
				offset:  0,
				length:  0,
				gfn:     false,
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
