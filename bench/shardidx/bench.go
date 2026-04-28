// Package main benchmarks GET archpath with vs without a stored shard index.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"archive/tar"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tarch"
	"github.com/NVIDIA/aistore/xact"
)

var (
	endpoint  = flag.String("endpoint", "http://localhost:8080", "AIStore endpoint")
	bucket    = flag.String("bucket", "bench-shardidx", "Bucket name")
	entries   = flag.Int("entries", 512, "Number of entries in generated TAR")
	entrySize = flag.Int64("entrysize", cos.MiB, "Size of each generated entry")
	iters     = flag.Int("iters", 100, "Random extractions per phase")
)

func main() {
	flag.Parse()
	bp := api.BaseParams{URL: *endpoint, Client: cmn.NewClient(cmn.TransportArgs{Timeout: 10 * time.Minute})}
	bck := cmn.Bck{Name: *bucket, Provider: apc.AIS}
	if err := api.CreateBucket(bp, bck, nil); err != nil && !cmn.IsErrBucketAlreadyExists(err) {
		panic(err)
	}

	// generate one local TAR with predictable names
	tmp, err := os.MkdirTemp("", "shardidx-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmp)
	localTar := filepath.Join(tmp, "bench.tar")
	names := make([]string, *entries)
	for i := range *entries {
		names[i] = fmt.Sprintf("%06d.bin", i)
	}
	if err := tarch.CreateArchRandomFiles(localTar, tar.FormatGNU, archive.ExtTar,
		*entries, int(*entrySize), nil, names, false, false, true); err != nil {
		panic(err)
	}
	st, _ := os.Stat(localTar)
	fmt.Printf("TAR: %s (%d entries x %s)\n\n", cos.ToSizeIEC(st.Size(), 0), *entries, cos.ToSizeIEC(*entrySize, 0))

	// Phase A: PUT, fetch+measure (no index), remove
	objA := fmt.Sprintf("bench-%d.tar", time.Now().UnixNano())
	putTar(bp, bck, objA, localTar)
	dA := extract(bp, bck, objA, names, "no-index")
	api.DeleteObject(bp, bck, objA)

	// Phase B: PUT (fresh copy, no cross-phase page-cache reuse), build index, fetch+measure, remove
	objB := fmt.Sprintf("bench-%d.tar", time.Now().UnixNano())
	putTar(bp, bck, objB, localTar)
	xid, err := api.IndexBucketShards(bp, bck, &apc.IndexShardMsg{})
	if err != nil {
		panic(err)
	}
	if _, err := api.WaitForXactionIC(bp, &xact.ArgsMsg{ID: xid, Kind: apc.ActIndexShard, Bck: bck, Timeout: 5 * time.Minute}); err != nil {
		panic(err)
	}
	dB := extract(bp, bck, objB, names, "with-index")
	api.DeleteObject(bp, bck, objB)

	fmt.Printf("\nSpeedup: %.2fx\n", dA.Seconds()/dB.Seconds())
}

func putTar(bp api.BaseParams, bck cmn.Bck, objName, localTar string) {
	st, _ := os.Stat(localTar)
	r, err := readers.New(&readers.Arg{
		Type: readers.File, Path: filepath.Dir(localTar), Name: filepath.Base(localTar),
		Size: readers.ExistingFileSize, CksumType: cos.ChecksumOneXxh,
	})
	if err != nil {
		panic(err)
	}
	defer r.Close()
	if _, err := api.PutObject(&api.PutArgs{
		BaseParams: bp, Bck: bck, ObjName: objName,
		Cksum: r.Cksum(), Reader: r, Size: uint64(st.Size()),
	}); err != nil {
		panic(err)
	}
}

func extract(bp api.BaseParams, bck cmn.Bck, objName string, names []string, label string) time.Duration {
	rng := rand.New(rand.NewPCG(42, 0))
	var bytes int64
	start := time.Now()
	for range *iters {
		oah, err := api.GetObject(bp, bck, objName, &api.GetArgs{
			Query:  url.Values{apc.QparamArchpath: []string{names[rng.IntN(len(names))]}},
			Writer: io.Discard,
		})
		if err != nil {
			panic(err)
		}
		bytes += oah.Size()
	}
	d := time.Since(start)
	fmt.Printf("%-12s %.3fs  %s  %.2f MiB/s\n", label, d.Seconds(), cos.ToSizeIEC(bytes, 0), float64(bytes)/d.Seconds()/1048576)
	return d
}
