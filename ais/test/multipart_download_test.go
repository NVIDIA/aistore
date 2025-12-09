// Package integration_test.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	"crypto/md5"
	"os"
	"testing"

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
			ChunkSize:  cos.MiB,
			// ObjectSize not set - should auto-detect via HEAD
		})
		tassert.CheckFatal(t, err)
		file.Close()

		downloaded, err := os.ReadFile(file.Name())
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, md5.Sum(downloaded) == origMD5, "content mismatch")
	})
}
