// Package main benchmarks MultipartDownload vs GetObjectReader.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/xact"
)

var (
	endpoint   = flag.String("endpoint", "http://localhost:8080", "AIStore endpoint")
	bucket     = flag.String("bucket", "bench-mpd", "Bucket name")
	size       = flag.Int64("size", 1*cos.GiB, "Object size")
	workers    = flag.Int("workers", 16, "Multipart download workers")
	chunk      = flag.Int64("chunk", 8*cos.MiB, "Chunk size")
	buffer     = flag.Int64("buffer", 0, "MultipartDownloadStream ring buffer size (0 = default)")
	iterations = flag.Int("n", 3, "Iterations")
)

func main() {
	flag.Parse()

	bp := api.BaseParams{URL: *endpoint, Client: cmn.NewClient(cmn.TransportArgs{Timeout: 10 * time.Minute})}
	bck := cmn.Bck{Name: *bucket, Provider: apc.AIS}

	api.CreateBucket(bp, bck, nil)

	// create test object
	objName := fmt.Sprintf("bench-%d.bin", time.Now().UnixNano())
	reader, _ := readers.New(&readers.Arg{Type: readers.Rand, Size: *size, CksumType: cos.ChecksumNone})
	api.PutObject(&api.PutArgs{BaseParams: bp, Bck: bck, ObjName: objName, Reader: reader, Size: uint64(*size)})
	defer api.DeleteObject(bp, bck, objName)

	// rechunk the object with the specified chunk size
	xid, err := api.RechunkBucket(bp, bck, &apc.RechunkMsg{ChunkSize: *chunk, ObjSizeLimit: *size - 1})
	if err != nil {
		fmt.Printf("rechunk failed: %v\n", err)
		return
	}
	api.WaitForXactionIC(bp, &xact.ArgsMsg{ID: xid})

	fmt.Printf("Object: %s, Workers: %d, Chunk: %s\n\n", cos.ToSizeIEC(*size, 0), *workers, cos.ToSizeIEC(*chunk, 0))

	// benchmark GetObjectReader
	fmt.Println("GetObjectReader:")
	var singleTotal time.Duration
	for i := range *iterations {
		file, _ := os.CreateTemp("", "single-*.bin")
		start := time.Now()
		r, _, _ := api.GetObjectReader(bp, bck, objName, nil)
		io.Copy(file, r)
		r.Close()
		elapsed := time.Since(start)
		file.Close()
		os.Remove(file.Name())
		singleTotal += elapsed
		fmt.Printf("  %d: %.2f MiB/s\n", i+1, float64(*size)/elapsed.Seconds()/1048576)
	}
	singleAvg := singleTotal / time.Duration(*iterations)

	// benchmark MultipartDownload
	fmt.Println("\nMultipartDownload:")
	var multiTotal time.Duration
	for i := range *iterations {
		file, _ := os.CreateTemp("", "multi-*.bin")
		file.Truncate(*size)
		start := time.Now()
		api.MultipartDownload(bp, bck, objName, &api.MultipartDownloadArgs{
			Writer: file, NumWorkers: *workers, ChunkSize: *chunk, ObjectSize: *size,
		})
		elapsed := time.Since(start)
		file.Close()
		os.Remove(file.Name())
		multiTotal += elapsed
		fmt.Printf("  %d: %.2f MiB/s\n", i+1, float64(*size)/elapsed.Seconds()/1048576)
	}
	multiAvg := multiTotal / time.Duration(*iterations)

	// benchmark MultipartDownloadStream
	fmt.Println("\nMultipartDownloadStream:")
	var streamTotal time.Duration
	for i := range *iterations {
		file, _ := os.CreateTemp("", "stream-*.bin")
		start := time.Now()
		r, _, err := api.MultipartDownloadStream(bp, bck, objName, &api.MpdStreamArgs{
			NumWorkers: *workers,
			ChunkSize:  *chunk,
			ObjectSize: *size,
			BufferSize: *buffer,
		})
		if err != nil {
			fmt.Printf("stream open failed: %v\n", err)
			file.Close()
			os.Remove(file.Name())
			return
		}
		_, err = io.Copy(file, r)
		r.Close()
		if err != nil {
			fmt.Printf("stream copy failed: %v\n", err)
			file.Close()
			os.Remove(file.Name())
			return
		}
		elapsed := time.Since(start)
		file.Close()
		os.Remove(file.Name())
		streamTotal += elapsed
		fmt.Printf("  %d: %.2f MiB/s\n", i+1, float64(*size)/elapsed.Seconds()/1048576)
	}
	streamAvg := streamTotal / time.Duration(*iterations)

	fmt.Printf("\nAvg: GetObjectReader=%.2f MiB/s, MultipartDownload=%.2f MiB/s, MultipartDownloadStream=%.2f MiB/s, Speedup(mpd)=%.2fx, Speedup(stream)=%.2fx\n",
		float64(*size)/singleAvg.Seconds()/1048576,
		float64(*size)/multiAvg.Seconds()/1048576,
		float64(*size)/streamAvg.Seconds()/1048576,
		singleAvg.Seconds()/multiAvg.Seconds(),
		singleAvg.Seconds()/streamAvg.Seconds())
}
