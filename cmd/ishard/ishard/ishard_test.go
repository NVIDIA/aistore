// Package main for the `ishard` executable.
/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ishard_test

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/ishard/ishard"
	"github.com/NVIDIA/aistore/cmd/ishard/ishard/config"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ext/dsort/shard"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tarch"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/trand"
)

func runIshardTest(t *testing.T, cfg *config.Config, baseParams api.BaseParams, numRecords, numExtensions int,
	fileSize int64, sampleKeyPattern config.SampleKeyPattern, randomize bool) {
	tools.CreateBucket(t, cfg.URL, cfg.SrcBck, nil, true /*cleanup*/)
	tools.CreateBucket(t, cfg.URL, cfg.DstBck, nil, true /*cleanup*/)

	extensions := make([]string, 0, numExtensions)
	for range numExtensions {
		extensions = append(extensions, "."+trand.String(3))
	}

	totalSize, err := generateNestedStructure(baseParams, cfg.SrcBck, numRecords, "", extensions, fileSize, randomize, false)
	tassert.CheckFatal(t, err)

	isharder, err := ishard.NewISharder(cfg)
	tassert.CheckFatal(t, err)

	fmt.Printf("starting ishard, from %s to %s\n", cfg.SrcBck.String(), cfg.DstBck.String())

	err = isharder.Start()
	tassert.CheckFatal(t, err)

	checkOutputShards(t, baseParams, cfg.DstBck, numRecords*numExtensions, totalSize, sampleKeyPattern, false)
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
						ShardSize:        config.ShardSize{Size: 102400},
						Collapse:         tc.collapse,
						ShardTemplate:    "shard-%d",
						Ext:              ".tar",
						SampleKeyPattern: config.BaseFileNamePattern,
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

			runIshardTest(t, cfg, baseParams, tc.numRecords, tc.numExtensions, tc.fileSize, config.BaseFileNamePattern, false /*randomize*/)
		})
	}
}

func TestIshardShardSize(t *testing.T) {
	type tc struct {
		numRecords int
		fileSize   int64
		shardSize  int64
		isCount    bool
	}
	testCases := []tc{
		{numRecords: 100, fileSize: 96 * cos.KiB, shardSize: 256 * cos.KiB, isCount: false},
		{numRecords: 200, fileSize: 24 * cos.KiB, shardSize: 20, isCount: true},
		{numRecords: 2000, fileSize: 24 * cos.KiB, shardSize: 160000 * cos.KiB, isCount: false},
	}

	if !testing.Short() {
		testCases = append(testCases,
			tc{numRecords: 100, fileSize: 96 * cos.KiB, shardSize: 10, isCount: true},
			tc{numRecords: 200, fileSize: 24 * cos.KiB, shardSize: 16 * cos.KiB, isCount: false},
			tc{numRecords: 2000, fileSize: 24 * cos.KiB, shardSize: 200, isCount: true},
		)
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Records:%d/FileSize:%d/ShardSize:%d", tc.numRecords, tc.fileSize, tc.shardSize), func(t *testing.T) {
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
						ShardSize: config.ShardSize{
							IsCount: tc.isCount,
							Size:    tc.shardSize,
							Count:   int(tc.shardSize),
						},
						Collapse:         true,
						ShardTemplate:    "shard-%d",
						Ext:              ".tar",
						SampleKeyPattern: config.BaseFileNamePattern,
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

			runIshardTest(t, cfg, baseParams, tc.numRecords, numExtensions, tc.fileSize, config.BaseFileNamePattern, false /*randomize*/)

			lsmsg := &apc.LsoMsg{}
			lsmsg.SetFlag(apc.LsNameSize)
			if tc.isCount {
				lsmsg.SetFlag(apc.LsArchDir)
				recs := shard.NewRecords(tc.numRecords)

				lst, err := api.ListObjects(baseParams, cfg.DstBck, lsmsg, api.ListArgs{})
				tassert.CheckFatal(t, err)

				for _, en := range lst.Entries {
					// Only counts objects inside archive
					if !en.IsAnyFlagSet(apc.EntryInArch) {
						continue
					}
					ext := filepath.Ext(en.Name)
					fullname := strings.TrimSuffix(en.Name, ext)
					recs.Insert(&shard.Record{
						Key:  fullname,
						Name: fullname,
						Objects: []*shard.RecordObj{{
							Size:      en.Size,
							Extension: ext,
						}},
					})
				}

				var foundIncompleteShard bool
				for _, record := range recs.All() {
					if len(record.Objects) > int(tc.shardSize)*numExtensions {
						tassert.Fatalf(t, foundIncompleteShard, "number of records in a shard exceeds the configured limit count, have len(record.Objects)=%d v.s. int(tc.shardSize)*numExtensions=%d", len(record.Objects), int(tc.shardSize)*numExtensions)
						foundIncompleteShard = true
					}
				}
			} else {
				tarballs, err := api.ListObjects(baseParams, cfg.DstBck, lsmsg, api.ListArgs{})
				tassert.CheckFatal(t, err)

				// With collapse enabled, only one output shard is allowed to have size that doesn't reach to `shardSize`,
				// which may contain only the remaining data.
				var foundIncompleteShard bool
				for _, en := range tarballs.Entries {
					if en.Size < tc.shardSize {
						tassert.Fatalf(t, !foundIncompleteShard, "The output shard size doesn't reach to shardSize. en.Name: %s, en.Size: %d, tc.shardSize: %d, len(tarballs.Entries): %d", en.Name, en.Size, tc.shardSize, len(tarballs.Entries))
						foundIncompleteShard = true
					}
				}
			}
		})
	}
}

func TestIshardPrefix(t *testing.T) {
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
				ShardSize:        config.ShardSize{Size: 102400},
				Collapse:         false,
				ShardTemplate:    "shard-%d",
				Ext:              ".tar",
				SampleKeyPattern: config.BaseFileNamePattern,
			},
			ClusterConfig: config.DefaultConfig.ClusterConfig,
			SrcPrefix:     "matched_prefix_",
		}
		baseParams = api.BaseParams{
			URL:    cfg.URL,
			Client: cmn.NewClient(cmn.TransportArgs{UseHTTPProxyEnv: true}),
		}
		numRecords    = 50
		numExtensions = 3
		fileSize      = 32 * cos.KiB
	)

	tools.CreateBucket(t, cfg.URL, cfg.SrcBck, nil, true /*cleanup*/)
	tools.CreateBucket(t, cfg.URL, cfg.DstBck, nil, true /*cleanup*/)

	extensions := make([]string, numExtensions)
	for i := range numExtensions {
		extensions[i] = "." + trand.String(3)
	}

	totalSize, err := generateNestedStructure(baseParams, cfg.SrcBck, numRecords, "matched_prefix_", extensions, int64(fileSize), false, false)
	tassert.CheckFatal(t, err)
	_, err = generateNestedStructure(baseParams, cfg.SrcBck, numRecords/2, "unmatched_prefix_", extensions, int64(fileSize), false, false)
	tassert.CheckFatal(t, err)

	isharder, err := ishard.NewISharder(cfg)
	tassert.CheckFatal(t, err)

	fmt.Printf("starting ishard, from %s to %s\n", cfg.SrcBck.String(), cfg.DstBck.String())

	err = isharder.Start()
	tassert.CheckFatal(t, err)

	// Only files with `matched_prefix_` counts
	checkOutputShards(t, baseParams, cfg.DstBck, numRecords*numExtensions, totalSize, config.BaseFileNamePattern, false)
}

func TestIshardTemplate(t *testing.T) {
	testCases := []struct {
		numRecords    int
		fileSize      int64
		shardSize     int64
		shardTemplate string
	}{
		{numRecords: 50, fileSize: 32 * cos.KiB, shardSize: 128 * cos.KiB, shardTemplate: "prefix{0000..9999}-suffix"},
		{numRecords: 50, fileSize: 96 * cos.KiB, shardSize: 256 * cos.KiB, shardTemplate: "prefix-%06d-suffix"},
		{numRecords: 50, fileSize: 24 * cos.KiB, shardSize: 16 * cos.KiB, shardTemplate: "prefix-@00001-gap-@100-suffix"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Records:%d/FileSize:%d/ShardSize:%d/Template:%s", tc.numRecords, tc.fileSize, tc.shardSize, tc.shardTemplate), func(t *testing.T) {
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
						ShardSize:        config.ShardSize{Size: tc.shardSize},
						Collapse:         true,
						ShardTemplate:    tc.shardTemplate,
						Ext:              ".tar",
						SampleKeyPattern: config.BaseFileNamePattern,
					},
					ClusterConfig: config.DefaultConfig.ClusterConfig,
				}
				baseParams = api.BaseParams{
					URL:    cfg.URL,
					Client: cmn.NewClient(cmn.TransportArgs{UseHTTPProxyEnv: true}),
				}
				numExtensions = 3
			)

			runIshardTest(t, cfg, baseParams, tc.numRecords, numExtensions, tc.fileSize, config.BaseFileNamePattern, false /*randomize*/)

			tarballs, err := api.ListObjects(baseParams, cfg.DstBck, &apc.LsoMsg{}, api.ListArgs{})
			tassert.CheckFatal(t, err)

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
				tassert.Fatalf(t, re.MatchString(en.Name), "expected %s to match %s", en.Name, expectedFormat)
			}
		})
	}
}

func TestIshardSampleKeyPattern(t *testing.T) {
	type tc struct {
		collapse bool
		pattern  config.SampleKeyPattern
	}
	testCases := []tc{
		{pattern: config.FullNamePattern, collapse: false},
		{pattern: config.BaseFileNamePattern, collapse: false},
		{pattern: config.CollapseAllDirPattern, collapse: false},
	}

	// If long test, extend to test cases
	if !testing.Short() {
		testCases = append(testCases,
			tc{pattern: config.FullNamePattern, collapse: true},
			tc{pattern: config.BaseFileNamePattern, collapse: true},
			tc{pattern: config.CollapseAllDirPattern, collapse: true},
		)
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Pattern:%v/Collapse:%v", tc.pattern, tc.collapse), func(t *testing.T) {
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
						ShardSize:        config.ShardSize{Size: 102400},
						Collapse:         tc.collapse,
						ShardTemplate:    "shard-%d",
						Ext:              ".tar",
						SampleKeyPattern: tc.pattern,
					},
					ClusterConfig: config.DefaultConfig.ClusterConfig,
				}
				baseParams = api.BaseParams{
					URL:    cfg.URL,
					Client: cmn.NewClient(cmn.TransportArgs{UseHTTPProxyEnv: true}),
				}
				numRecords    = 50
				numExtensions = 5
				fileSize      = 32 * cos.KiB
			)

			runIshardTest(t, cfg, baseParams, numRecords, numExtensions, int64(fileSize), tc.pattern, true /*randomize*/)
		})
	}
}

func TestIshardMissingExtension(t *testing.T) {
	var (
		cfg = &config.Config{
			IshardConfig: config.IshardConfig{
				ShardSize:        config.ShardSize{Size: 102400},
				Collapse:         true,
				ShardTemplate:    "shard-%d",
				Ext:              ".tar",
				SampleKeyPattern: config.BaseFileNamePattern,
			},
			ClusterConfig: config.DefaultConfig.ClusterConfig,
		}
		baseParams = api.BaseParams{
			URL:    cfg.URL,
			Client: cmn.NewClient(cmn.TransportArgs{UseHTTPProxyEnv: true}),
		}
		numRecords = 500
		fileSize   = 32 * cos.KiB
	)

	if testing.Short() {
		numRecords /= 10
	}

	t.Run("TestIshardMissingExtension/action=abort", func(t *testing.T) {
		cfg.SrcBck = cmn.Bck{
			Name:     trand.String(15),
			Provider: apc.AIS,
		}
		cfg.DstBck = cmn.Bck{
			Name:     trand.String(15),
			Provider: apc.AIS,
		}

		tools.CreateBucket(t, cfg.URL, cfg.SrcBck, nil, true /*cleanup*/)
		tools.CreateBucket(t, cfg.URL, cfg.DstBck, nil, true /*cleanup*/)

		var err error
		expectedExts := []string{".jpeg", ".cls", ".json"}
		cfg.MExtMgr, err = config.NewMissingExtManager("abort", expectedExts)
		tassert.CheckFatal(t, err)

		_, err = generateNestedStructure(baseParams, cfg.SrcBck, numRecords, "", expectedExts, int64(fileSize), true /*randomize*/, true /*dropout*/)
		tassert.CheckFatal(t, err)

		isharder, err := ishard.NewISharder(cfg)
		tassert.CheckFatal(t, err)

		fmt.Printf("starting ishard, from %s to %s\n", cfg.SrcBck.String(), cfg.DstBck.String())

		err = isharder.Start() // error is expected to occur since `dropout` is set, ishard should abort
		if err == nil || !strings.HasPrefix(err.Error(), config.ErrorPrefix) {
			tassert.Fatalf(t, false, "expected error with 'missing extension:', but got: %v", err)
		}
	})

	t.Run("TestIshardMissingExtension/action=exclude", func(t *testing.T) {
		cfg.SrcBck = cmn.Bck{
			Name:     trand.String(15),
			Provider: apc.AIS,
		}
		cfg.DstBck = cmn.Bck{
			Name:     trand.String(15),
			Provider: apc.AIS,
		}
		tools.CreateBucket(t, cfg.URL, cfg.SrcBck, nil, true /*cleanup*/)
		tools.CreateBucket(t, cfg.URL, cfg.DstBck, nil, true /*cleanup*/)

		expectedExts := []string{".jpeg", ".cls", ".json"}
		var err error
		cfg.MExtMgr, err = config.NewMissingExtManager("exclude", expectedExts)
		tassert.CheckFatal(t, err)

		_, err = generateNestedStructure(baseParams, cfg.SrcBck, numRecords, "", expectedExts, int64(fileSize), true /*randomize*/, true /*dropout*/)
		tassert.CheckFatal(t, err)

		isharder, err := ishard.NewISharder(cfg)
		tassert.CheckFatal(t, err)

		fmt.Printf("starting ishard, from %s to %s\n", cfg.SrcBck.String(), cfg.DstBck.String())

		err = isharder.Start()
		tassert.CheckFatal(t, err)

		re := regexp.MustCompile(cfg.SampleKeyPattern.Regex)

		// all incomplete samples should be excluded
		shardContents, err := getShardContents(baseParams, cfg.DstBck)
		for _, files := range shardContents {
			// map to store the set of extensions for each base filename
			baseFiles := make(map[string]map[string]struct{})
			for _, file := range files {
				base := strings.TrimSuffix(file, filepath.Ext(file))
				ext := filepath.Ext(file)
				base = re.ReplaceAllString(base, cfg.SampleKeyPattern.CaptureGroup)

				if _, exists := baseFiles[base]; !exists {
					baseFiles[base] = make(map[string]struct{})
				}
				baseFiles[base][ext] = struct{}{}
			}

			for base, exts := range baseFiles {
				for _, desiredExt := range expectedExts {
					if _, exists := exts[desiredExt]; !exists {
						tassert.Fatalf(t, false, "Base file %s is missing extension %s\n", base, desiredExt)
					}
				}
			}
		}
		tassert.CheckFatal(t, err)
	})

	t.Run("TestIshardMissingExtension/action=warn", func(t *testing.T) {
		cfg.SrcBck = cmn.Bck{
			Name:     trand.String(15),
			Provider: apc.AIS,
		}
		cfg.DstBck = cmn.Bck{
			Name:     trand.String(15),
			Provider: apc.AIS,
		}
		tools.CreateBucket(t, cfg.URL, cfg.SrcBck, nil, true /*cleanup*/)
		tools.CreateBucket(t, cfg.URL, cfg.DstBck, nil, true /*cleanup*/)

		expectedExts := []string{".jpeg", ".cls", ".json"}
		var err error
		cfg.MExtMgr, err = config.NewMissingExtManager("warn", expectedExts)
		tassert.CheckFatal(t, err)

		_, err = generateNestedStructure(baseParams, cfg.SrcBck, numRecords, "", expectedExts, int64(fileSize), true /*randomize*/, true /*dropout*/)
		tassert.CheckFatal(t, err)

		isharder, err := ishard.NewISharder(cfg)
		tassert.CheckFatal(t, err)

		fmt.Printf("starting ishard, from %s to %s\n", cfg.SrcBck.String(), cfg.DstBck.String())

		// Capture stdout output to verify warning messages
		r, w, _ := os.Pipe()
		stdout := os.Stdout
		defer func() { os.Stdout = stdout }() // Restore original stdout at the end
		os.Stdout = w

		err = isharder.Start()
		w.Close()

		var outputBuffer bytes.Buffer
		_, _ = outputBuffer.ReadFrom(r)
		output := outputBuffer.String()

		if !strings.HasPrefix(output, config.WarningPrefix) {
			t.Errorf("Expected warning message not found in output: %s", output)
		}

		tassert.CheckFatal(t, err)
	})
}

func TestIshardEKM(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

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
				ShardSize:        config.ShardSize{Size: 102400},
				ShardTemplate:    "shard-%d",
				Ext:              ".tar",
				SampleKeyPattern: config.BaseFileNamePattern,
			},
			ClusterConfig: config.DefaultConfig.ClusterConfig,
		}
		baseParams = api.BaseParams{
			URL:    cfg.URL,
			Client: cmn.NewClient(cmn.TransportArgs{UseHTTPProxyEnv: true}),
		}
		numRecords    = 50
		numExtensions = 5
		fileSize      = 32 * cos.KiB
		dsortedBck    = cmn.Bck{
			Name:     cfg.DstBck.Name + "-sorted",
			Provider: apc.AIS,
		}
	)

	tools.CreateBucket(t, cfg.URL, cfg.SrcBck, nil, true /*cleanup*/)
	tools.CreateBucket(t, cfg.URL, cfg.DstBck, nil, true /*cleanup*/)
	tools.CreateBucket(t, cfg.URL, dsortedBck, nil, true /*cleanup*/)

	extensions := make([]string, 0, numExtensions)
	for range numExtensions {
		extensions = append(extensions, "."+trand.String(3))
	}
	_, err := generateNestedStructure(baseParams, cfg.SrcBck, numRecords, "", extensions, int64(fileSize), false, false)
	tassert.CheckFatal(t, err)

	fmt.Println("building and configuring EKM as JSON string")
	var builder strings.Builder
	builder.WriteString("{")
	for i, letter := range "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" {
		if i > 0 {
			builder.WriteString(",")
		}
		fmt.Fprintf(&builder, "\n    \"%c-%%d.tar\": [\"^%c.*\"]", letter, letter)
	}
	builder.WriteString("\n}")
	cfg.EKMFlag.Set(builder.String())

	fmt.Printf("starting ishard, from %s to %s\n", cfg.SrcBck.String(), cfg.DstBck.String())
	isharder, err := ishard.NewISharder(cfg)
	tassert.CheckFatal(t, err)

	err = isharder.Start()
	tassert.CheckFatal(t, err)

	shardContents, err := getShardContents(baseParams, dsortedBck)
	tassert.CheckFatal(t, err)

	var fileCount int
	for tarball, files := range shardContents {
		for _, fileName := range files {
			fileCount++
			tassert.Fatalf(
				t, tarball[0] == fileName[0],
				"fail to categorize by the first character in file names, tarball name %s != file name %s", tarball, fileName,
			)
		}
	}
	tassert.Fatalf(t, fileCount == numRecords*numExtensions, "created file count doesn't match, want %d have %d", numRecords*numExtensions, fileCount)
}

func TestIshardParallel(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

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
					ShardSize:        config.ShardSize{Size: 64 * cos.MiB},
					Collapse:         true,
					ShardTemplate:    "shard-%09d",
					Ext:              ".tar",
					SampleKeyPattern: config.BaseFileNamePattern,
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

		wg.Add(1)
		go func(cfg config.Config, baseParams api.BaseParams) {
			defer wg.Done()
			runIshardTest(t, &cfg, baseParams, numRecords, numExtensions, int64(fileSize), config.BaseFileNamePattern, false /*randomize*/)
		}(*cfg, baseParams)
	}
	wg.Wait()
}

func TestIshardChain(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

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
					ShardSize:        config.ShardSize{Size: 64 * cos.MiB},
					Collapse:         true,
					ShardTemplate:    "shard-%09d",
					Ext:              ".tar",
					SampleKeyPattern: config.BaseFileNamePattern,
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

		runIshardTest(t, cfg, baseParams, numRecords, numExtensions, int64(fileSize), config.BaseFileNamePattern, false /*randomize*/)
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
				ShardSize:        config.ShardSize{Size: 128 * cos.MiB},
				Collapse:         true,
				ShardTemplate:    "shard-%09d",
				Ext:              ".tar",
				SampleKeyPattern: config.BaseFileNamePattern,
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

	runIshardTest(t, cfg, baseParams, numRecords, numExtensions, int64(fileSize), config.BaseFileNamePattern, false /*randomize*/)
}

func TestIshardLargeFiles(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	var (
		cfg = &config.Config{
			IshardConfig: config.IshardConfig{
				ShardSize:        config.ShardSize{Size: 5 * cos.GiB},
				Collapse:         true,
				ShardTemplate:    "shard-%09d",
				Ext:              ".tar",
				SampleKeyPattern: config.FullNamePattern,
			},
			ClusterConfig: config.DefaultConfig.ClusterConfig,
			SrcBck: cmn.Bck{
				Name:     trand.String(10),
				Provider: apc.AIS,
			},
			DstBck: cmn.Bck{
				Name:     trand.String(10),
				Provider: apc.AIS,
			},
		}
		baseParams = api.BaseParams{
			URL:    cfg.URL,
			Client: cmn.NewClient(cmn.TransportArgs{UseHTTPProxyEnv: true}),
		}
		numRecords    = 5
		numExtensions = 3
		fileSize      = 2 * cos.GiB
	)

	runIshardTest(t, cfg, baseParams, numRecords, numExtensions, int64(fileSize), config.FullNamePattern, false /*randomize*/)
}

// Helper function to generate a nested directory structure
func generateNestedStructure(baseParams api.BaseParams, bucket cmn.Bck, numRecords int, prefix string, extensions []string, fileSize int64, randomize, dropout bool) (totalSize int64, _ error) {
	randomFilePath := func() string {
		levels := rand.IntN(3) + 1 // Random number of subdirectory levels (1-3)
		parts := make([]string, levels)
		for i := range levels {
			parts[i] = trand.String(3)
		}
		return filepath.Join(parts...)
	}

	// Queue to hold objects temporarily to random insertion
	randomizeQueue := make([]string, 0)
	dropOnce := dropout

	basePath := randomFilePath()
	for range numRecords {
		// Randomly change or extend the current directory
		dice := rand.IntN(5)
		if dice == 0 {
			basePath = randomFilePath()
		} else if dice < 2 {
			basePath += randomFilePath()
		}

		// Randomly pop an object from the queue and generate it from the current base path
		if randomize && len(randomizeQueue) > 0 && rand.IntN(10) == 0 {
			baseNameWithExt := randomizeQueue[0]
			randomizeQueue = randomizeQueue[1:]
			size := rand.Int64N(fileSize)
			totalSize += size
			r, _ := readers.NewRand(size, cos.ChecksumNone)
			if _, err := api.PutObject(&api.PutArgs{
				BaseParams: baseParams,
				Bck:        bucket,
				ObjName:    filepath.Join(prefix, basePath, baseNameWithExt),
				Reader:     r,
				Size:       uint64(size),
			}); err != nil {
				return -1, err
			}
		}

		baseName := trand.String(5)
		for _, ext := range extensions {
			// Ensure to drop an extension at least once
			if dropOnce {
				dropOnce = false
				continue
			}

			// Randomly skip an extension if dropout is set
			if dropout && rand.IntN(10) == 0 {
				continue
			}

			// Randomly push to queue instead of generating
			if randomize && rand.IntN(10) == 0 {
				randomizeQueue = append(randomizeQueue, baseName+ext)
				continue
			}

			// Generate the object normally
			objectName := filepath.Join(basePath, baseName+ext)
			size := rand.Int64N(fileSize)
			totalSize += size
			r, _ := readers.NewRand(size, cos.ChecksumNone)
			if _, err := api.PutObject(&api.PutArgs{
				BaseParams: baseParams,
				Bck:        bucket,
				ObjName:    filepath.Join(prefix, objectName),
				Reader:     r,
				Size:       uint64(size),
			}); err != nil {
				return -1, err
			}
		}
	}

	// Generate any remaining objects in the queue without base path (at root level)
	for _, objectName := range randomizeQueue {
		size := rand.Int64N(fileSize)
		totalSize += size
		r, _ := readers.NewRand(size, cos.ChecksumNone)
		if _, err := api.PutObject(&api.PutArgs{
			BaseParams: baseParams,
			Bck:        bucket,
			ObjName:    filepath.Join(prefix, objectName),
			Reader:     r,
			Size:       uint64(size),
		}); err != nil {
			return -1, err
		}
	}

	fmt.Printf("generated %d records in %s bucket\n", numRecords, bucket.String())
	return totalSize, nil
}

func checkOutputShards(t *testing.T, baseParams api.BaseParams, bucket cmn.Bck, expectedNumFiles int, totalSize int64, sampleKeyPattern config.SampleKeyPattern, dropout bool) {
	shardContents, err := getShardContents(baseParams, bucket)
	tassert.CheckFatal(t, err)

	sampleKey2Tarballs := make(map[string]string)
	re := regexp.MustCompile(sampleKeyPattern.Regex)

	totalFileNum := 0
	for tarball, files := range shardContents {
		for _, fileName := range files {
			totalFileNum++

			ext := filepath.Ext(fileName)
			base := strings.TrimSuffix(fileName, ext)
			sampleKey := re.ReplaceAllString(base, sampleKeyPattern.CaptureGroup)

			existingTarball, exists := sampleKey2Tarballs[sampleKey]
			if existingTarball != tarball {
				tassert.Fatalf(t, !exists, "Found split sampleKey: %s in output shards: %s and %s", sampleKey, existingTarball, tarball)
			}
			sampleKey2Tarballs[sampleKey] = tarball
		}
	}
	if !dropout {
		tassert.Fatalf(t, totalFileNum == expectedNumFiles, "The total number of files in output shards (%d) doesn't match to the initially generated amount (%d)", totalFileNum, expectedNumFiles)
	}
	fmt.Printf("finished ishard, archived %d files with total size %s\n", expectedNumFiles, cos.ToSizeIEC(totalSize, 2))
}

func getShardContents(baseParams api.BaseParams, bucket cmn.Bck) (map[string][]string, error) {
	msg := &apc.LsoMsg{}
	lst, err := api.ListObjects(baseParams, bucket, msg, api.ListArgs{})
	if err != nil {
		return nil, err
	}

	shardContents := make(map[string][]string)
	for _, en := range lst.Entries {
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
