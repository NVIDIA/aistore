// Package main for the `ishard` executable.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package ishard_test

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/ishard"
	"github.com/NVIDIA/aistore/tools/ishard/config"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tarch"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/trand"
)

func TestNoRecordsSplit(t *testing.T) {
	testCases := []struct {
		numRecords    int
		numExtensions int
		fileSize      int64
		collapse      bool
	}{
		{numRecords: 50, numExtensions: 2, fileSize: 32 * cos.KiB, collapse: false},
		{numRecords: 200, numExtensions: 4, fileSize: 48 * cos.KiB, collapse: true},
		{numRecords: 50, numExtensions: 2, fileSize: 32 * cos.KiB, collapse: false},
		{numRecords: 200, numExtensions: 4, fileSize: 48 * cos.KiB, collapse: true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Records:%d/Extensions:%d/FileSize:%d/Collapse:%v", tc.numRecords, tc.numExtensions, tc.fileSize, tc.collapse), func(t *testing.T) {
			var (
				cfg = &config.Config{
					SrcBck: cmn.Bck{
						Name:     trand.String(15),
						Provider: apc.AIS,
					},
					DstBck: cmn.Bck{
						Name:     trand.String(15),
						Provider: apc.AIS,
					},
					IshardConfig: config.IshardConfig{
						MaxShardSize: 102400,
						Collapse:     tc.collapse,
						StartIdx:     0,
						IdxDigits:    4,
						Ext:          ".tar",
						Prefix:       "shard-",
					},
					ClusterConfig: config.DefaultConfig.ClusterConfig,
				}
				baseParams = api.BaseParams{
					URL:    cfg.URL,
					Client: cmn.NewClient(cmn.TransportArgs{UseHTTPProxyEnv: true}),
				}
			)

			t.Logf("using %s as input bucket\n", cfg.SrcBck)
			tools.CreateBucket(t, cfg.URL, cfg.SrcBck, nil, true /*cleanup*/)
			t.Logf("using %s as output bucket\n", cfg.DstBck)
			tools.CreateBucket(t, cfg.URL, cfg.DstBck, nil, true /*cleanup*/)
			tassert.CheckError(t, generateNestedStructure(baseParams, cfg.SrcBck, tc.numRecords, tc.numExtensions, tc.fileSize))

			tassert.CheckError(t, ishard.Init(cfg))
			tassert.CheckError(t, ishard.Start())

			time.Sleep(time.Second * 3) // wait for api.ArchiveMultiObj to complete

			shardContents, err := getShardContents(baseParams, cfg.DstBck)
			tassert.CheckError(t, err)

			recordToTarballs := make(map[string]string)

			totalFileNum := 0
			for tarball, files := range shardContents {
				for _, file := range files {
					totalFileNum++
					record := getRecordName(file)
					existingTarball, exists := recordToTarballs[record]
					tassert.Fatalf(t, !exists || existingTarball == tarball, "Found split records")
					recordToTarballs[record] = tarball
				}
			}
			tassert.Fatalf(t, totalFileNum == tc.numRecords*tc.numExtensions, "The total number of files in output shards doesn't match to the initially generated amount")
		})
	}
}

func TestMaxShardSize(t *testing.T) {
	testCases := []struct {
		numRecords   int
		fileSize     int64
		maxShardSize int64
	}{
		{numRecords: 50, fileSize: 32 * cos.KiB, maxShardSize: 128 * cos.KiB},
		{numRecords: 100, fileSize: 96 * cos.KiB, maxShardSize: 256 * cos.KiB},
		{numRecords: 200, fileSize: 24 * cos.KiB, maxShardSize: 16 * cos.KiB},
		{numRecords: 2000, fileSize: 24 * cos.KiB, maxShardSize: 160000 * cos.KiB},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Records:%d/FileSize:%d/MaxShardSize:%d", tc.numRecords, tc.fileSize, tc.maxShardSize), func(t *testing.T) {
			var (
				cfg = &config.Config{
					SrcBck: cmn.Bck{
						Name:     trand.String(15),
						Provider: apc.AIS,
					},
					DstBck: cmn.Bck{
						Name:     trand.String(15),
						Provider: apc.AIS,
					},
					IshardConfig: config.IshardConfig{
						MaxShardSize: tc.maxShardSize,
						Collapse:     true,
						StartIdx:     0,
						IdxDigits:    4,
						Ext:          ".tar",
						Prefix:       "shard-",
					},
					ClusterConfig: config.DefaultConfig.ClusterConfig,
				}
				baseParams = api.BaseParams{
					URL:    cfg.URL,
					Client: cmn.NewClient(cmn.TransportArgs{UseHTTPProxyEnv: true}),
				}
				numExtensions = 3
			)

			t.Logf("using %s as input bucket\n", cfg.SrcBck)
			tools.CreateBucket(t, cfg.URL, cfg.SrcBck, nil, true /*cleanup*/)
			t.Logf("using %s as output bucket\n", cfg.DstBck)
			tools.CreateBucket(t, cfg.URL, cfg.DstBck, nil, true /*cleanup*/)
			tassert.CheckError(t, generateNestedStructure(baseParams, cfg.SrcBck, tc.numRecords, numExtensions, tc.fileSize))

			tassert.CheckError(t, ishard.Init(cfg))
			tassert.CheckError(t, ishard.Start())
			time.Sleep(time.Second * 3) // wait for api.ArchiveMultiObj to complete

			tarballs, err := api.ListObjects(baseParams, cfg.DstBck, &apc.LsoMsg{}, api.ListArgs{})
			tassert.CheckError(t, err)

			for i, en := range tarballs.Entries {
				// With collapse enabled, all output shards should reach `maxShardSize`,
				// except the last one, which may contain only the remaining data.
				if i < len(tarballs.Entries)-1 {
					tassert.Fatalf(t, en.Size > tc.maxShardSize, "The output shard size doesn't reach to the desired amount. en.Size: %d, tc.maxShardSize: %d, i: %d, len(tarballs.Entries): %d", en.Size, tc.maxShardSize, i, len(tarballs.Entries))
				}
			}
		})
	}
}

// Helper function to generate a nested directory structure
func generateNestedStructure(baseParams api.BaseParams, bucket cmn.Bck, numRecords, numExtensions int, fileSize int64) error {
	extensions := make([]string, 0, numExtensions)

	for range numExtensions {
		extensions = append(extensions, "."+trand.String(3))
	}

	randomFilePath := func() string {
		levels := rand.IntN(3) + 1 // Random number of subdirectory levels (1-3)
		parts := make([]string, levels)
		for i := range levels {
			parts[i] = trand.String(3)
		}
		return filepath.Join(parts...)
	}

	basePath := randomFilePath()
	for range numRecords {
		dice := rand.IntN(5)
		if dice == 0 { // 1/5 chance to change to a new directory
			basePath = randomFilePath()
		} else if dice < 2 { // 1/5 chance to extend current directory
			basePath += randomFilePath()
		}
		baseName := trand.String(5)
		for _, ext := range extensions {
			objectName := filepath.Join(basePath, baseName+ext)
			size := rand.Int64N(fileSize)
			r, _ := readers.NewRand(size, cos.ChecksumNone)
			if _, err := api.PutObject(&api.PutArgs{
				BaseParams: baseParams,
				Bck:        bucket,
				ObjName:    objectName,
				Reader:     r,
				Size:       uint64(size),
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func getShardContents(baseParams api.BaseParams, bucket cmn.Bck) (map[string][]string, error) {
	msg := &apc.LsoMsg{}
	objList, err := api.ListObjects(baseParams, bucket, msg, api.ListArgs{})
	if err != nil {
		return nil, err
	}

	shardContents := make(map[string][]string)
	for _, en := range objList.Entries {
		var buffer bytes.Buffer
		_, err := api.GetObject(baseParams, bucket, en.Name, &api.GetArgs{Writer: &buffer})
		if err != nil {
			return nil, err
		}
		files, err := tarch.GetFilesFromArchBuffer(".tar", buffer, ".tar")
		if err != nil {
			return nil, err
		}
		for _, file := range files {
			shardContents[en.Name] = append(shardContents[en.Name], file.Name)
		}
	}

	return shardContents, nil
}

func getRecordName(filePath string) string {
	base := filePath[strings.LastIndex(filePath, "/")+1:]
	return base[:strings.LastIndex(base, ".")]
}
