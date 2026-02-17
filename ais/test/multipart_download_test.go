// Package integration_test.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	"crypto/md5"
	"io"
	"os"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
)

// TestMultipartDownload tests the api.MultipartDownload API.
// It validates:
// 1. Objects can be downloaded via concurrent range requests
// 2. Downloaded content matches the original (MD5 verification)
// 3. Various worker counts and chunk sizes work correctly
func TestMultipartDownload(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
		objName    = "mpd-test-" + trand.String(6)
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	tests := []struct {
		name      string
		size      int64
		workers   int
		chunkSize int64
	}{
		{"small-1worker", 64 * cos.KiB, 1, 16 * cos.KiB},
		{"small-4workers", 256 * cos.KiB, 4, 32 * cos.KiB},
		{"medium-8workers", 8 * cos.MiB, 8, cos.MiB},
		{"large-16workers", 64 * cos.MiB, 16, 4 * cos.MiB},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			name := objName + "-" + tc.name
			tlog.Logfln("Testing %s: size=%s, workers=%d, chunk=%s",
				tc.name, cos.ToSizeIEC(tc.size, 0), tc.workers, cos.ToSizeIEC(tc.chunkSize, 0))

			// Create object with random content
			reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: tc.size, CksumType: cos.ChecksumNone})
			tassert.CheckFatal(t, err)

			_, err = api.PutObject(&api.PutArgs{
				BaseParams: baseParams,
				Bck:        bck,
				ObjName:    name,
				Reader:     reader,
				Size:       uint64(tc.size),
			})
			tassert.CheckFatal(t, err)
			defer api.DeleteObject(baseParams, bck, name)

			// Get original content for comparison
			var origBuf bytes.Buffer
			_, err = api.GetObject(baseParams, bck, name, &api.GetArgs{Writer: &origBuf})
			tassert.CheckFatal(t, err)
			origMD5 := md5.Sum(origBuf.Bytes())

			// Download via MultipartDownload
			file, err := os.CreateTemp(t.TempDir(), "mpd-test-*.bin")
			tassert.CheckFatal(t, err)

			tassert.CheckFatal(t, file.Truncate(tc.size))

			err = api.MultipartDownload(baseParams, bck, name, &api.MultipartDownloadArgs{
				Writer:     file,
				NumWorkers: tc.workers,
				ChunkSize:  tc.chunkSize,
				ObjectSize: tc.size,
			})
			tassert.CheckFatal(t, err)
			file.Close()

			// Verify content
			downloaded, err := os.ReadFile(file.Name())
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, int64(len(downloaded)) == tc.size,
				"size mismatch: got %d, expected %d", len(downloaded), tc.size)

			downloadedMD5 := md5.Sum(downloaded)
			tassert.Fatalf(t, origMD5 == downloadedMD5, "content mismatch: MD5 differs")

			tlog.Logfln("  OK: content verified")
		})
	}
}

// TestMultipartDownloadStream tests the api.MultipartDownloadStream API.
func TestMultipartDownloadStream(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
		objName    = "mpds-test-" + trand.String(6)
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	tests := []struct {
		name       string
		size       int64
		workers    int
		chunkSize  int64
		bufferSize int64 // 0 = default (numWorkers * chunkSize)
	}{
		{"small-1worker", 64 * cos.KiB, 1, 16 * cos.KiB, 0},
		{"small-4workers", 256 * cos.KiB, 4, 32 * cos.KiB, 0},
		{"medium-8workers", 8 * cos.MiB, 8, cos.MiB, 0},
		// Backpressure: buffer holds only 2 chunks, fewer slots than workers
		{"backpressure-2slots", 8 * cos.MiB, 8, cos.MiB, 2 * cos.MiB},
		// Minimal: buffer = 1 chunk, effectively sequential
		{"minimal-buffer", 4 * cos.MiB, 4, cos.MiB, cos.MiB},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			name := objName + "-" + tc.name
			tlog.Logfln("Testing %s: size=%s, workers=%d, chunk=%s, buf=%s",
				tc.name, cos.ToSizeIEC(tc.size, 0), tc.workers,
				cos.ToSizeIEC(tc.chunkSize, 0), cos.ToSizeIEC(tc.bufferSize, 0))

			// Create object with random content
			reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: tc.size, CksumType: cos.ChecksumNone})
			tassert.CheckFatal(t, err)

			_, err = api.PutObject(&api.PutArgs{
				BaseParams: baseParams,
				Bck:        bck,
				ObjName:    name,
				Reader:     reader,
				Size:       uint64(tc.size),
			})
			tassert.CheckFatal(t, err)
			defer api.DeleteObject(baseParams, bck, name)

			// Get original content for comparison
			var origBuf bytes.Buffer
			_, err = api.GetObject(baseParams, bck, name, &api.GetArgs{Writer: &origBuf})
			tassert.CheckFatal(t, err)
			origMD5 := md5.Sum(origBuf.Bytes())

			// Download via MultipartDownloadStream
			streamReader, oah, err := api.MultipartDownloadStream(baseParams, bck, name, &api.MpdStreamArgs{
				NumWorkers: tc.workers,
				ChunkSize:  tc.chunkSize,
				ObjectSize: tc.size,
				BufferSize: tc.bufferSize,
			})
			tassert.CheckFatal(t, err)

			var downloadBuf bytes.Buffer
			_, err = downloadBuf.ReadFrom(streamReader)
			tassert.CheckFatal(t, err)
			streamReader.Close()

			tassert.Fatalf(t, int64(downloadBuf.Len()) == tc.size,
				"size mismatch: got %d, expected %d", downloadBuf.Len(), tc.size)

			downloadedMD5 := md5.Sum(downloadBuf.Bytes())
			tassert.Fatalf(t, origMD5 == downloadedMD5, "content mismatch: MD5 differs")

			// Validate returned checksum (if any)
			returnedCksum := oah.Attrs().Cksum
			if !cos.NoneC(returnedCksum) {
				computed := cos.ChecksumB2S(downloadBuf.Bytes(), returnedCksum.Type())
				tassert.Fatalf(t, computed == returnedCksum.Value(),
					"returned checksum mismatch: expected %s, got %s (type %s)", returnedCksum.Value(), computed, returnedCksum.Type())
			}

			tlog.Logfln("  OK: content verified")
		})
	}
}

// TestMultipartDownloadEdgeCases tests edge cases for api.MultipartDownload.
func TestMultipartDownloadEdgeCases(t *testing.T) {
	const (
		objSize = 10 * cos.MiB
	)
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
		objName    = "mpd-edge-" + trand.String(6)
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	// Create test object
	tlog.Logfln("Creating test object: %s (%s)", objName, cos.ToSizeIEC(objSize, 0))
	reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cos.ChecksumNone})
	tassert.CheckFatal(t, err)

	_, err = api.PutObject(&api.PutArgs{
		BaseParams: baseParams,
		Bck:        bck,
		ObjName:    objName,
		Reader:     reader,
		Size:       uint64(objSize),
	})
	tassert.CheckFatal(t, err)
	defer api.DeleteObject(baseParams, bck, objName)

	// Get original for comparison
	var origBuf bytes.Buffer
	_, err = api.GetObject(baseParams, bck, objName, &api.GetArgs{Writer: &origBuf})
	tassert.CheckFatal(t, err)
	origMD5 := md5.Sum(origBuf.Bytes())

	t.Run("chunk-larger-than-object", func(t *testing.T) {
		tlog.Logfln("Testing chunk size larger than object")
		file, err := os.CreateTemp(t.TempDir(), "mpd-*.bin")
		tassert.CheckFatal(t, err)

		tassert.CheckFatal(t, file.Truncate(objSize))

		err = api.MultipartDownload(baseParams, bck, objName, &api.MultipartDownloadArgs{
			Writer:     file,
			NumWorkers: 4,
			ChunkSize:  objSize * 2, // chunk larger than object
			ObjectSize: objSize,
		})
		tassert.CheckFatal(t, err)
		file.Close()

		downloaded, err := os.ReadFile(file.Name())
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, md5.Sum(downloaded) == origMD5, "content mismatch")
	})

	t.Run("single-worker", func(t *testing.T) {
		tlog.Logfln("Testing single worker (sequential)")
		file, err := os.CreateTemp(t.TempDir(), "mpd-*.bin")
		tassert.CheckFatal(t, err)

		tassert.CheckFatal(t, file.Truncate(objSize))

		err = api.MultipartDownload(baseParams, bck, objName, &api.MultipartDownloadArgs{
			Writer:     file,
			NumWorkers: 1,
			ChunkSize:  cos.MiB,
			ObjectSize: objSize,
		})
		tassert.CheckFatal(t, err)
		file.Close()

		downloaded, err := os.ReadFile(file.Name())
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, md5.Sum(downloaded) == origMD5, "content mismatch")
	})

	t.Run("auto-detect-size", func(t *testing.T) {
		tlog.Logfln("Testing auto-detect object size via HEAD")
		file, err := os.CreateTemp(t.TempDir(), "mpd-*.bin")
		tassert.CheckFatal(t, err)

		tassert.CheckFatal(t, file.Truncate(objSize))

		err = api.MultipartDownload(baseParams, bck, objName, &api.MultipartDownloadArgs{
			Writer:     file,
			NumWorkers: 4,
			// ChunkSize and ObjectSize not set - should auto-detect via HEAD
		})
		tassert.CheckFatal(t, err)
		file.Close()

		downloaded, err := os.ReadFile(file.Name())
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, md5.Sum(downloaded) == origMD5, "content mismatch")
	})
}

// TestMultipartDownloadStreamClose tests early Close() behavior on the stream reader.
func TestMultipartDownloadStreamClose(t *testing.T) {
	const objSize = 8 * cos.MiB

	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
		objName    = "mpds-close-" + trand.String(6)
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	// Create test object
	reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: objSize, CksumType: cos.ChecksumNone})
	tassert.CheckFatal(t, err)
	_, err = api.PutObject(&api.PutArgs{
		BaseParams: baseParams, Bck: bck, ObjName: objName, Reader: reader, Size: uint64(objSize),
	})
	tassert.CheckFatal(t, err)
	defer api.DeleteObject(baseParams, bck, objName)

	streamArgs := &api.MpdStreamArgs{
		NumWorkers: 4, ChunkSize: cos.MiB, ObjectSize: objSize,
	}

	t.Run("close-immediately", func(t *testing.T) {
		tlog.Logfln("Open stream and close immediately")
		r, _, err := api.MultipartDownloadStream(baseParams, bck, objName, streamArgs)
		tassert.CheckFatal(t, err)
		tassert.CheckFatal(t, r.Close())
	})

	t.Run("close-after-partial-read", func(t *testing.T) {
		tlog.Logfln("Read partial content then close")
		r, _, err := api.MultipartDownloadStream(baseParams, bck, objName, streamArgs)
		tassert.CheckFatal(t, err)

		buf := make([]byte, 2*cos.MiB)
		n, err := io.ReadFull(r, buf)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, n == 2*cos.MiB, "expected 2MiB, got %d", n)

		tassert.CheckFatal(t, r.Close())
		tlog.Logfln("  OK: read %d bytes, closed cleanly", n)
	})

	t.Run("close-while-reading", func(t *testing.T) {
		tlog.Logfln("Close from another goroutine while reading")
		r, _, err := api.MultipartDownloadStream(baseParams, bck, objName, streamArgs)
		tassert.CheckFatal(t, err)

		// Let the reader start consuming
		buf := make([]byte, cos.MiB)
		_, err = io.ReadFull(r, buf)
		tassert.CheckFatal(t, err)

		// Close from another goroutine after a brief delay
		go func() {
			time.Sleep(50 * time.Millisecond)
			r.Close()
		}()

		// Keep reading — expect io.ErrClosedPipe eventually
		for {
			_, err = r.Read(buf)
			if err != nil {
				break
			}
		}
		tassert.Fatalf(t, err == io.ErrClosedPipe || err == io.EOF,
			"expected ErrClosedPipe or EOF, got: %v", err)
		tlog.Logfln("  OK: got expected error: %v", err)
	})
}
