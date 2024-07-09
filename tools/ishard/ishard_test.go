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
	"regexp"
	"strings"
	"sync"
	"testing"

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
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
)

func runIshardTest(t *testing.T, cfg *config.Config, baseParams api.BaseParams, numRecords, numExtensions int, fileSize int64) {
	tools.CreateBucket(t, cfg.URL, cfg.SrcBck, nil, true /*cleanup*/)
	tools.CreateBucket(t, cfg.URL, cfg.DstBck, nil, true /*cleanup*/)

	totalSize, err := generateNestedStructure(baseParams, cfg.SrcBck, numRecords, numExtensions, fileSize)
	tassert.CheckError(t, err)

	isharder, err := ishard.NewISharder(cfg)
	tassert.CheckError(t, err)

	tlog.Logf("starting ishard, from %s to %s\n", cfg.SrcBck, cfg.DstBck)

	err = isharder.Start()
	tassert.CheckError(t, err)

	checkOutputShards(t, baseParams, cfg.DstBck, numRecords*numExtensions, totalSize)
}

func TestIshardNoRecordsSplit(t *testing.T) {
	testCases := []struct {
		numRecords    int
		numExtensions int
		fileSize      int64
		collapse      bool
	}{
		{numRecords: 500, numExtensions: 2, fileSize: 32 * cos.KiB, collapse: false},
		{numRecords: 2000, numExtensions: 4, fileSize: 48 * cos.KiB, collapse: true},
		{numRecords: 500, numExtensions: 2, fileSize: 32 * cos.KiB, collapse: false},
		{numRecords: 2000, numExtensions: 4, fileSize: 48 * cos.KiB, collapse: true},
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
						MaxShardSize:  102400,
						Collapse:      tc.collapse,
						ShardTemplate: "shard-%d",
						Ext:           ".tar",
					},
					ClusterConfig: config.DefaultConfig.ClusterConfig,
				}
				baseParams = api.BaseParams{
					URL:    cfg.URL,
					Client: cmn.NewClient(cmn.TransportArgs{UseHTTPProxyEnv: true}),
				}
			)

			if testing.Short() {
				tc.numRecords /= 10
			}

			runIshardTest(t, cfg, baseParams, tc.numRecords, tc.numExtensions, tc.fileSize)
		})
	}
}

func TestIshardMaxShardSize(t *testing.T) {
	testCases := []struct {
		numRecords   int
		fileSize     int64
		maxShardSize int64
	}{
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
						MaxShardSize:  tc.maxShardSize,
						Collapse:      true,
						ShardTemplate: "shard-%d",
						Ext:           ".tar",
					},
					ClusterConfig: config.DefaultConfig.ClusterConfig,
				}
				baseParams = api.BaseParams{
					URL:    cfg.URL,
					Client: cmn.NewClient(cmn.TransportArgs{UseHTTPProxyEnv: true}),
				}
				numExtensions = 3
			)

			if testing.Short() {
				tc.numRecords /= 10
			}

			runIshardTest(t, cfg, baseParams, tc.numRecords, numExtensions, tc.fileSize)

			tarballs, err := api.ListObjects(baseParams, cfg.DstBck, &apc.LsoMsg{}, api.ListArgs{})
			tassert.CheckError(t, err)

			// With collapse enabled, only one output shard is allowed to have size that doesn't reach to `maxShardSize`,
			// which may contain only the remaining data.
			var foundIncompleteShard bool
			for _, en := range tarballs.Entries {
				if en.Size < tc.maxShardSize {
					tassert.Fatalf(t, !foundIncompleteShard, "The output shard size doesn't reach to maxShardSize. en.Name: %s, en.Size: %d, tc.maxShardSize: %d, len(tarballs.Entries): %d", en.Name, en.Size, tc.maxShardSize, len(tarballs.Entries))
					foundIncompleteShard = true
				}
			}
		})
	}
}

func TestIshardTemplate(t *testing.T) {
	testCases := []struct {
		numRecords    int
		fileSize      int64
		maxShardSize  int64
		shardTemplate string
	}{
		{numRecords: 50, fileSize: 32 * cos.KiB, maxShardSize: 128 * cos.KiB, shardTemplate: "prefix{0000..9999}-suffix"},
		{numRecords: 100, fileSize: 96 * cos.KiB, maxShardSize: 256 * cos.KiB, shardTemplate: "prefix-%06d-suffix"},
		{numRecords: 200, fileSize: 24 * cos.KiB, maxShardSize: 16 * cos.KiB, shardTemplate: "prefix-@00001-gap-@100-suffix"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Records:%d/FileSize:%d/MaxShardSize:%d/Template:%s", tc.numRecords, tc.fileSize, tc.maxShardSize, tc.shardTemplate), func(t *testing.T) {
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
						MaxShardSize:  tc.maxShardSize,
						Collapse:      true,
						ShardTemplate: tc.shardTemplate,
						Ext:           ".tar",
					},
					ClusterConfig: config.DefaultConfig.ClusterConfig,
				}
				baseParams = api.BaseParams{
					URL:    cfg.URL,
					Client: cmn.NewClient(cmn.TransportArgs{UseHTTPProxyEnv: true}),
				}
				numExtensions = 3
			)

			runIshardTest(t, cfg, baseParams, tc.numRecords, numExtensions, tc.fileSize)

			tarballs, err := api.ListObjects(baseParams, cfg.DstBck, &apc.LsoMsg{}, api.ListArgs{})
			tassert.CheckError(t, err)

			for _, en := range tarballs.Entries {
				var expectedFormat string
				switch tc.shardTemplate {
				case "prefix{0000..9999}-suffix":
					expectedFormat = `^prefix\d{4}-suffix\.tar$`
				case "prefix-%06d-suffix":
					expectedFormat = `^prefix-\d{6}-suffix\.tar$`
				case "prefix-@00001-gap-@100-suffix":
					expectedFormat = `^prefix-\d+-gap-\d+-suffix\.tar$`
				default:
					t.Fatalf("Unsupported shard template: %s", tc.shardTemplate)
				}

				re := regexp.MustCompile(expectedFormat)
				tassert.Fatalf(t, re.MatchString(en.Name), fmt.Sprintf("expected %s to match %s", en.Name, expectedFormat))
			}
		})
	}
}

func TestIshardParallel(t *testing.T) {
	var ishardsCount = 5

	wg := &sync.WaitGroup{}
	for i := range ishardsCount {
		var (
			bckNameSuffix = trand.String(10)
			cfg           = &config.Config{
				SrcBck: cmn.Bck{
					Name:     fmt.Sprintf("src-%d-%s", i, bckNameSuffix),
					Provider: apc.AIS,
				},
				DstBck: cmn.Bck{
					Name:     fmt.Sprintf("dst-%d-%s", i, bckNameSuffix),
					Provider: apc.AIS,
				},
				IshardConfig: config.IshardConfig{
					MaxShardSize:  64 * cos.MiB,
					Collapse:      true,
					ShardTemplate: "shard-%09d",
					Ext:           ".tar",
				},
				ClusterConfig: config.DefaultConfig.ClusterConfig,
			}
			baseParams = api.BaseParams{
				URL:    cfg.URL,
				Client: cmn.NewClient(cmn.TransportArgs{UseHTTPProxyEnv: true}),
			}
			numRecords    = 10000
			numExtensions = 5
			fileSize      = 32 * cos.KiB
		)

		if testing.Short() {
			numRecords /= 100
		}

		wg.Add(1)
		go func(cfg config.Config, baseParams api.BaseParams) {
			defer wg.Done()
			runIshardTest(t, &cfg, baseParams, numRecords, numExtensions, int64(fileSize))
		}(*cfg, baseParams)
	}
	wg.Wait()
}

func TestIshardChain(t *testing.T) {
	var ishardsCount = 5

	for i := range ishardsCount {
		var (
			bckNameSuffix = trand.String(10)
			cfg           = &config.Config{
				SrcBck: cmn.Bck{
					Name:     fmt.Sprintf("src-%d-%s", i, bckNameSuffix),
					Provider: apc.AIS,
				},
				DstBck: cmn.Bck{
					Name:     fmt.Sprintf("dst-%d-%s", i, bckNameSuffix),
					Provider: apc.AIS,
				},
				IshardConfig: config.IshardConfig{
					MaxShardSize:  64 * cos.MiB,
					Collapse:      true,
					ShardTemplate: "shard-%09d",
					Ext:           ".tar",
				},
				ClusterConfig: config.DefaultConfig.ClusterConfig,
			}
			baseParams = api.BaseParams{
				URL:    cfg.URL,
				Client: cmn.NewClient(cmn.TransportArgs{UseHTTPProxyEnv: true}),
			}
			numRecords    = 50000
			numExtensions = 5
			fileSize      = 32 * cos.KiB
		)

		if testing.Short() {
			numRecords /= 100
		}

		runIshardTest(t, cfg, baseParams, numRecords, numExtensions, int64(fileSize))
	}
}

func TestIshardLargeBucket(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	var (
		cfg = &config.Config{
			SrcBck: cmn.Bck{
				Name:     trand.String(10),
				Provider: apc.AIS,
			},
			DstBck: cmn.Bck{
				Name:     trand.String(10),
				Provider: apc.AIS,
			},
			IshardConfig: config.IshardConfig{
				MaxShardSize:  64 * cos.MiB,
				Collapse:      true,
				ShardTemplate: "shard-%09d",
				Ext:           ".tar",
			},
			ClusterConfig: config.DefaultConfig.ClusterConfig,
		}
		baseParams = api.BaseParams{
			URL:    cfg.URL,
			Client: cmn.NewClient(cmn.TransportArgs{UseHTTPProxyEnv: true}),
		}
		numRecords    = 100000
		numExtensions = 3
		fileSize      = 32 * cos.KiB
	)

	runIshardTest(t, cfg, baseParams, numRecords, numExtensions, int64(fileSize))
}

func TestIshardLargeFiles(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	var (
		cfg = &config.Config{
			SrcBck: cmn.Bck{
				Name:     trand.String(10),
				Provider: apc.AIS,
			},
			DstBck: cmn.Bck{
				Name:     trand.String(10),
				Provider: apc.AIS,
			},
			IshardConfig: config.IshardConfig{
				MaxShardSize:  5 * cos.GiB,
				Collapse:      true,
				ShardTemplate: "shard-%09d",
				Ext:           ".tar",
			},
			ClusterConfig: config.DefaultConfig.ClusterConfig,
		}
		baseParams = api.BaseParams{
			URL:    cfg.URL,
			Client: cmn.NewClient(cmn.TransportArgs{UseHTTPProxyEnv: true}),
		}
		numRecords    = 5
		numExtensions = 3
		fileSize      = 2 * cos.GiB
	)

	runIshardTest(t, cfg, baseParams, numRecords, numExtensions, int64(fileSize))
}

// Helper function to generate a nested directory structure
func generateNestedStructure(baseParams api.BaseParams, bucket cmn.Bck, numRecords, numExtensions int, fileSize int64) (totalSize int64, _ error) {
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
			totalSize += size
			r, _ := readers.NewRand(size, cos.ChecksumNone)
			if _, err := api.PutObject(&api.PutArgs{
				BaseParams: baseParams,
				Bck:        bucket,
				ObjName:    objectName,
				Reader:     r,
				Size:       uint64(size),
			}); err != nil {
				return -1, err
			}
		}
	}
	tlog.Logf("generated %d records in %s bucket\n", numRecords, bucket)
	return totalSize, nil
}

func checkOutputShards(t *testing.T, baseParams api.BaseParams, bucket cmn.Bck, expectedNumFiles int, totalSize int64) {
	shardContents, err := getShardContents(baseParams, bucket)
	tassert.CheckError(t, err)

	recordToTarballs := make(map[string]string)

	totalFileNum := 0
	for tarball, files := range shardContents {
		for _, file := range files {
			totalFileNum++
			record := getRecordName(file)
			existingTarball, exists := recordToTarballs[record]
			tassert.Fatalf(t, !exists || existingTarball == tarball, "Found split record: %s in output shards: %s and %s", record, existingTarball, tarball)
			recordToTarballs[record] = tarball
		}
	}
	tassert.Fatalf(t, totalFileNum == expectedNumFiles, "The total number of files in output shards (%d) doesn't match to the initially generated amount (%d)", totalFileNum, expectedNumFiles)
	tlog.Logf("finished ishard, archived %d files with total size %s\n", expectedNumFiles, cos.ToSizeIEC(totalSize, 2))
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
