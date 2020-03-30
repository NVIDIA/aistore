// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/containers"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/dsort/extract"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

const (
	dsortDescAllPrefix = cmn.DSortNameLowercase + "-test-integration"

	scopeConfig = "config"
	scopeSpec   = "spec"
)

var (
	dsortDescCurPrefix = fmt.Sprintf("%s-%d-", dsortDescAllPrefix, os.Getpid())

	dsorterTypes       = []string{dsort.DSorterGeneralType, dsort.DSorterMemType}
	dsortPhases        = []string{dsort.ExtractionPhase, dsort.SortingPhase, dsort.CreationPhase}
	dsortSettingScopes = []string{scopeConfig, scopeSpec}
)

type (
	dsortTestSpec struct {
		p         bool // determines if the tests should be ran in parallel mode
		types     []string
		phases    []string
		reactions []string
		scopes    []string
	}

	// nolint:maligned // no performance critical code
	dsortFramework struct {
		m *ioContext

		dsorterType string

		outputBck    cmn.Bck
		inputPrefix  string
		outputPrefix string

		inputTempl            string
		outputTempl           string
		orderFileURL          string
		tarballCnt            int
		tarballCntToSkip      int
		fileInTarballCnt      int
		fileInTarballSize     int
		tarballSize           int
		outputShardCnt        int
		recordDuplicationsCnt int

		extension       string
		algorithm       *dsort.SortAlgorithm
		missingKeys     bool
		outputShardSize string
		maxMemUsage     string
		dryRun          bool

		missingShards     string
		duplicatedRecords string

		baseParams  api.BaseParams
		managerUUID string
	}

	shardRecords struct {
		name        string
		recordNames []string
	}
)

func generateDSortDesc() string {
	return dsortDescCurPrefix + time.Now().Format(time.RFC3339Nano)
}

func runDSortTest(t *testing.T, dts dsortTestSpec, f interface{}) {
	if dts.p {
		t.Parallel()
	}

	for _, dsorterType := range dts.types {
		dsorterType := dsorterType // pin
		t.Run(dsorterType, func(t *testing.T) {
			if dts.p {
				t.Parallel()
			}

			if len(dts.phases) > 0 {
				g := f.(func(dsorterType, phase string, t *testing.T))
				for _, phase := range dts.phases {
					phase := phase // pin
					t.Run(phase, func(t *testing.T) {
						if dts.p {
							t.Parallel()
						}
						g(dsorterType, phase, t)
					})
				}
			} else if len(dts.reactions) > 0 {
				for _, reaction := range dts.reactions {
					reaction := reaction // pin
					t.Run(reaction, func(t *testing.T) {
						if dts.p {
							t.Parallel()
						}

						if len(dts.scopes) > 0 {
							for _, scope := range dts.scopes {
								scope := scope // pin
								t.Run(scope, func(t *testing.T) {
									if dts.p {
										t.Parallel()
									}

									g := f.(func(dsorterType, reaction, scope string, t *testing.T))
									g(dsorterType, reaction, scope, t)
								})
							}
						} else {
							g := f.(func(dsorterType, reaction string, t *testing.T))
							g(dsorterType, reaction, t)
						}
					})
				}
			} else {
				g := f.(func(dsorterType string, t *testing.T))
				g(dsorterType, t)
			}
		})
	}
}

func (df *dsortFramework) init() {
	if df.inputTempl == "" {
		df.inputTempl = fmt.Sprintf("input-{0..%d}", df.tarballCnt-1)
	}
	if df.outputTempl == "" {
		df.outputTempl = "output-{00000..10000}"
	}
	if df.extension == "" {
		df.extension = cmn.ExtTar
	}

	// Assumption is that all prefixes end with dash: "-"
	df.inputPrefix = df.inputTempl[:strings.Index(df.inputTempl, "-")+1]
	df.outputPrefix = df.outputTempl[:strings.Index(df.outputTempl, "-")+1]

	if df.fileInTarballSize == 0 {
		df.fileInTarballSize = cmn.KiB
	}

	if df.maxMemUsage == "" {
		df.maxMemUsage = "99%"
	}

	df.tarballSize = df.fileInTarballCnt * df.fileInTarballSize
	outputShardSize := int64(10 * df.fileInTarballCnt * df.fileInTarballSize)
	df.outputShardSize = cmn.B2S(outputShardSize, 0)
	df.outputShardCnt = (df.tarballCnt * df.tarballSize) / int(outputShardSize)

	if df.algorithm == nil {
		df.algorithm = &dsort.SortAlgorithm{}
	}

	df.baseParams = tutils.BaseAPIParams(df.m.proxyURL)
}

func (df *dsortFramework) gen() dsort.RequestSpec {
	return dsort.RequestSpec{
		Description:      generateDSortDesc(),
		Bucket:           df.m.bck.Name,
		Provider:         df.m.bck.Provider,
		OutputBucket:     df.outputBck.Name,
		OutputProvider:   df.outputBck.Provider,
		Extension:        df.extension,
		InputFormat:      df.inputTempl,
		OutputFormat:     df.outputTempl,
		OutputShardSize:  df.outputShardSize,
		Algorithm:        *df.algorithm,
		OrderFileURL:     df.orderFileURL,
		ExtractConcLimit: 10,
		CreateConcLimit:  10,
		MaxMemUsage:      df.maxMemUsage,
		DSorterType:      df.dsorterType,
		DryRun:           df.dryRun,

		DSortConf: cmn.DSortConf{
			MissingShards:     df.missingShards,
			DuplicatedRecords: df.duplicatedRecords,
		},
	}
}

func (df *dsortFramework) start() {
	var (
		err error
		rs  = df.gen()
	)

	df.managerUUID, err = api.StartDSort(df.baseParams, rs)
	tassert.CheckFatal(df.m.t, err)
}

func (df *dsortFramework) createInputShards() {
	const tmpDir = "/tmp"
	var (
		err error
	)
	tutils.Logf("creating %d tarballs...\n", df.tarballCnt)
	wg := &sync.WaitGroup{}
	errCh := make(chan error, df.tarballCnt)
	for i := df.tarballCntToSkip; i < df.tarballCnt; i++ {
		wg.Add(1)
		go func(i int) {
			duplication := i < df.recordDuplicationsCnt

			path := fmt.Sprintf("%s/%s/%s%d", tmpDir, df.m.bck.Name, df.inputPrefix, i)
			if df.algorithm.Kind == dsort.SortKindContent {
				err = tutils.CreateTarWithCustomFiles(path, df.fileInTarballCnt, df.fileInTarballSize, df.algorithm.FormatType, df.algorithm.Extension, df.missingKeys)
			} else if df.extension == cmn.ExtTar {
				err = tutils.CreateTarWithRandomFiles(path, false, df.fileInTarballCnt, df.fileInTarballSize, duplication)
			} else if df.extension == cmn.ExtTarTgz {
				err = tutils.CreateTarWithRandomFiles(path, true, df.fileInTarballCnt, df.fileInTarballSize, duplication)
			} else if df.extension == cmn.ExtZip {
				err = tutils.CreateZipWithRandomFiles(path, df.fileInTarballCnt, df.fileInTarballSize)
			} else {
				df.m.t.Fail()
			}
			tassert.CheckFatal(df.m.t, err)

			fqn := path + df.extension
			defer os.Remove(fqn)

			reader, err := tutils.NewFileReaderFromFile(fqn, false)
			tassert.CheckFatal(df.m.t, err)

			tutils.PutAsync(wg, df.m.proxyURL, df.m.bck, filepath.Base(fqn), reader, errCh)
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		tassert.CheckFatal(df.m.t, err)
	}
	tutils.Logln("done creating tarballs")
}

func (df *dsortFramework) checkOutputShards(zeros int) {
	tutils.Logln("checking if files are sorted...")

	lastName := ""
	var lastValue interface{}

	gzipped := false
	if df.extension != cmn.ExtTar {
		gzipped = true
	}

	inversions := 0
	baseParams := tutils.BaseAPIParams(df.m.proxyURL)
	for i := 0; i < df.outputShardCnt; i++ {
		shardName := fmt.Sprintf("%s%0*d%s", df.outputPrefix, zeros, i, df.extension)
		var buffer bytes.Buffer
		getOptions := api.GetObjectInput{
			Writer: &buffer,
		}

		bucket := df.m.bck
		if df.outputBck.Name != "" {
			bucket = df.outputBck
		}

		_, err := api.GetObject(baseParams, bucket, shardName, getOptions)
		if err != nil && df.extension == ".zip" && i > df.outputShardCnt/2 {
			// We estimated too much output shards to be produced - zip compression
			// was so good that we could fit more files inside the shard.
			//
			// Sanity check to make sure that error did not occur before half
			// of the shards estimated (zip compression should be THAT good).
			break
		}
		tassert.CheckFatal(df.m.t, err)

		if df.algorithm.Kind == dsort.SortKindContent {
			files, err := tutils.GetFilesFromTarBuffer(buffer, df.algorithm.Extension)
			tassert.CheckFatal(df.m.t, err)
			for _, file := range files {
				if file.Ext == df.algorithm.Extension {
					if strings.TrimSuffix(file.Name, filepath.Ext(file.Name)) != strings.TrimSuffix(lastName, filepath.Ext(lastName)) {
						// custom files should be AFTER the regular files
						df.m.t.Fatalf("names are not in correct order (shard: %s, lastName: %s, curName: %s)", shardName, lastName, file.Name)
					}

					switch df.algorithm.FormatType {
					case extract.FormatTypeInt:
						intValue, err := strconv.ParseInt(string(file.Content), 10, 64)
						tassert.CheckFatal(df.m.t, err)
						if lastValue != nil && intValue < lastValue.(int64) {
							df.m.t.Fatalf("int values are not in correct order (shard: %s, lastIntValue: %d, curIntValue: %d)", shardName, lastValue.(int64), intValue)
						}
						lastValue = intValue
					case extract.FormatTypeFloat:
						floatValue, err := strconv.ParseFloat(string(file.Content), 64)
						tassert.CheckFatal(df.m.t, err)
						if lastValue != nil && floatValue < lastValue.(float64) {
							df.m.t.Fatalf("string values are not in correct order (shard: %s, lastStringValue: %f, curStringValue: %f)", shardName, lastValue.(float64), floatValue)
						}
						lastValue = floatValue
					case extract.FormatTypeString:
						stringValue := string(file.Content)
						if lastValue != nil && stringValue < lastValue.(string) {
							df.m.t.Fatalf("string values are not in correct order (shard: %s, lastStringValue: %s, curStringValue: %s)", shardName, lastValue.(string), stringValue)
						}
						lastValue = stringValue
					default:
						df.m.t.Fail()
					}
				} else {
					lastName = file.Name
				}
			}
		} else {
			var (
				files []os.FileInfo
			)

			if df.extension == cmn.ExtTar || df.extension == cmn.ExtTarTgz {
				files, err = tutils.GetFileInfosFromTarBuffer(buffer, gzipped)
			} else if df.extension == cmn.ExtZip {
				files, err = tutils.GetFileInfosFromZipBuffer(buffer)
			}

			tassert.CheckFatal(df.m.t, err)
			if len(files) == 0 {
				df.m.t.Fatal("number of files inside shard is 0")
			}

			for _, file := range files {
				if df.algorithm.Kind == "" {
					if lastName > file.Name() {
						df.m.t.Fatalf("names are not in correct order (shard: %s, lastName: %s, curName: %s)", shardName, lastName, file.Name())
					}
				} else if df.algorithm.Kind == dsort.SortKindShuffle {
					if lastName > file.Name() {
						inversions++
					}
				}
				if file.Size() != int64(df.fileInTarballSize) {
					df.m.t.Fatalf("file sizes has changed (expected: %d, got: %d)", df.fileInTarballSize, file.Size())
				}
				lastName = file.Name()
			}
		}
	}

	if df.algorithm.Kind == dsort.SortKindShuffle {
		if inversions == 0 {
			df.m.t.Fatal("shuffle sorting did not create any inversions")
		}
	}
}

func (df *dsortFramework) checkReactionResult(reaction string, expectedProblemsCnt int) {
	tutils.Logln("checking metrics and reaction results...")
	allMetrics, err := api.MetricsDSort(df.baseParams, df.managerUUID)
	tassert.CheckFatal(df.m.t, err)
	if len(allMetrics) != df.m.originalTargetCount {
		df.m.t.Errorf("number of metrics %d is not same as number of targets %d", len(allMetrics), df.m.originalTargetCount)
	}

	switch reaction {
	case cmn.IgnoreReaction:
		for target, metrics := range allMetrics {
			if len(metrics.Warnings) != 0 {
				df.m.t.Errorf("target %q has %s warnings: %s", target, cmn.DSortName, metrics.Warnings)
			}
			if len(metrics.Errors) != 0 {
				df.m.t.Errorf("target %q has %s errors: %s", target, cmn.DSortName, metrics.Errors)
			}
		}
	case cmn.WarnReaction:
		totalWarnings := 0
		for target, metrics := range allMetrics {
			totalWarnings += len(metrics.Warnings)

			if len(metrics.Errors) != 0 {
				df.m.t.Errorf("target %q has %s errors: %s", target, cmn.DSortName, metrics.Errors)
			}
		}

		if totalWarnings != expectedProblemsCnt {
			df.m.t.Errorf("number of total warnings %d is different than number of deleted shards: %d", totalWarnings, expectedProblemsCnt)
		}
	case cmn.AbortReaction:
		totalErrors := 0
		for target, metrics := range allMetrics {
			if !metrics.Aborted.Load() {
				df.m.t.Errorf("%s was not aborted by target: %s", cmn.DSortName, target)
			}
			totalErrors += len(metrics.Errors)
		}

		if totalErrors == 0 {
			df.m.t.Error("expected errors on abort, got nothing")
		}
	}
}

func (df *dsortFramework) getRecordNames(bck cmn.Bck) []shardRecords {
	var (
		allShardRecords = make([]shardRecords, 0, 10)
	)

	list, err := api.ListObjectsFast(df.baseParams, bck, nil)
	tassert.CheckFatal(df.m.t, err)

	if len(list.Entries) == 0 {
		df.m.t.Errorf("number of objects in bucket %q is 0", bck)
	}

	for _, obj := range list.Entries {
		var buffer bytes.Buffer
		getOptions := api.GetObjectInput{
			Writer: &buffer,
		}
		_, err := api.GetObject(df.baseParams, bck, obj.Name, getOptions)
		tassert.CheckFatal(df.m.t, err)

		files, err := tutils.GetFileInfosFromTarBuffer(buffer, false)
		tassert.CheckFatal(df.m.t, err)

		shard := shardRecords{
			name:        obj.Name,
			recordNames: make([]string, len(files)),
		}
		for idx, file := range files {
			shard.recordNames[idx] = file.Name()
		}
		allShardRecords = append(allShardRecords, shard)
	}

	return allShardRecords
}

func (df *dsortFramework) checkMetrics(expectAbort bool) map[string]*dsort.Metrics {
	tutils.Logln("checking metrics...")
	allMetrics, err := api.MetricsDSort(df.baseParams, df.managerUUID)
	tassert.CheckFatal(df.m.t, err)
	if len(allMetrics) != df.m.originalTargetCount {
		df.m.t.Errorf("number of metrics %d is not same as number of targets %d", len(allMetrics), df.m.originalTargetCount)
	}

	for target, metrics := range allMetrics {
		if expectAbort && !metrics.Aborted.Load() {
			df.m.t.Errorf("%s was not aborted by target: %s", cmn.DSortName, target)
		} else if !expectAbort && metrics.Aborted.Load() {
			df.m.t.Errorf("%s was aborted by target: %s", cmn.DSortName, target)
		}
	}

	return allMetrics
}

// helper for dispatching i-th dSort job
func dispatchDSortJob(m *ioContext, dsorterType string, i int) {
	var (
		df = &dsortFramework{
			m:                m,
			dsorterType:      dsorterType,
			inputTempl:       fmt.Sprintf("input%d-{0..999}", i),
			outputTempl:      fmt.Sprintf("output%d-{00000..01000}", i),
			tarballCnt:       1000,
			fileInTarballCnt: 50,
			maxMemUsage:      "99%",
		}
	)

	df.init()
	df.createInputShards()

	tutils.Logln("starting distributed sort...")
	df.start()

	_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
	tassert.CheckFatal(m.t, err)
	tutils.Logln("finished distributed sort")

	df.checkMetrics(false /* expectAbort */)
	df.checkOutputShards(5)
}

func waitForDSortPhase(t *testing.T, proxyURL, managerUUID, phaseName string, callback func()) {
	tutils.Logf("waiting for %s phase...\n", phaseName)
	baseParams := tutils.BaseAPIParams(proxyURL)
	for {
		allMetrics, err := api.MetricsDSort(baseParams, managerUUID)
		if err != nil { // in case of error call callback anyway
			t.Error(err)
			callback()
			break
		}

		phase := true
		for _, metrics := range allMetrics {
			switch phaseName {
			case dsort.ExtractionPhase:
				phase = phase && (metrics.Extraction.Running || metrics.Extraction.Finished)
			case dsort.SortingPhase:
				phase = phase && (metrics.Sorting.Running || metrics.Sorting.Finished)
			case dsort.CreationPhase:
				phase = phase && (metrics.Creation.Running || metrics.Creation.Finished)
			default:
				t.Fatal(phaseName)
			}
		}

		if phase {
			callback()
			break
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func TestDistributedSort(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	runDSortTest(
		// Include empty ("") type - in this case type must be selected automatically.
		t, dsortTestSpec{p: true, types: append(dsorterTypes, "")},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:                m,
					dsorterType:      dsorterType,
					tarballCnt:       1000,
					fileInTarballCnt: 100,
					maxMemUsage:      "99%",
				}
			)

			// Initialize ioContext
			m.saveClusterState()
			if m.originalTargetCount < 3 {
				t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
			}

			// Create ais bucket
			tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
			defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

			df.init()
			df.createInputShards()

			tutils.Logln("starting distributed sort...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tutils.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(5)
		},
	)
}

func TestDistributedSortWithNonExistingBuckets(t *testing.T) {
	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:           m,
					dsorterType: dsorterType,
					outputBck: cmn.Bck{
						Name:     tutils.GenRandomString(15),
						Provider: cmn.ProviderAIS,
					},
					tarballCnt:       1000,
					fileInTarballCnt: 100,
					maxMemUsage:      "99%",
				}
			)

			// Initialize ioContext
			m.saveClusterState()
			if m.originalTargetCount < 3 {
				t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
			}

			df.init()

			// Create local output bucket
			tutils.CreateFreshBucket(t, m.proxyURL, df.outputBck)

			tutils.Logln("starting distributed sort...")
			rs := df.gen()
			if _, err := api.StartDSort(df.baseParams, rs); err == nil {
				t.Errorf("expected %s job to fail when input bucket does not exist", cmn.DSortName)
			}

			// Now destroy output bucket and create input bucket
			tutils.DestroyBucket(t, m.proxyURL, df.outputBck)
			tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
			defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

			tutils.Logln("starting second distributed sort...")
			if _, err := api.StartDSort(df.baseParams, rs); err == nil {
				t.Errorf("expected %s job to fail when output bucket does not exist", cmn.DSortName)
			}
		},
	)
}

func TestDistributedSortWithEmptyBucket(t *testing.T) {
	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes, reactions: cmn.SupportedReactions},
		func(dsorterType, reaction string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:                m,
					dsorterType:      dsorterType,
					tarballCnt:       100,
					fileInTarballCnt: 10,
					maxMemUsage:      "99%",
					missingShards:    reaction,
				}
			)

			// Initialize ioContext
			m.saveClusterState()
			if m.originalTargetCount < 3 {
				t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
			}

			tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
			defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

			df.init()

			tutils.Logln("starting distributed sort...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tutils.Logln("finished distributed sort")

			df.checkMetrics(reaction == cmn.AbortReaction /*expectAbort*/)
			df.checkReactionResult(reaction, df.tarballCnt)
		},
	)
}

func TestDistributedSortWithOutputBucket(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:           m,
					dsorterType: dsorterType,
					outputBck: cmn.Bck{
						Name:     tutils.GenRandomString(15),
						Provider: cmn.ProviderAIS,
					},
					tarballCnt:       1000,
					fileInTarballCnt: 100,
					maxMemUsage:      "99%",
				}
			)

			// Initialize ioContext
			m.saveClusterState()
			if m.originalTargetCount < 3 {
				t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
			}

			// Create ais buckets
			tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
			defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

			// Create local output bucket
			tutils.CreateFreshBucket(t, m.proxyURL, df.outputBck)
			defer tutils.DestroyBucket(t, m.proxyURL, df.outputBck)

			df.init()
			df.createInputShards()

			tutils.Logln("starting distributed sort...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tutils.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(5)
		},
	)
}

// TestDistributedSortParallel runs multiple dSorts in parallel
func TestDistributedSortParallel(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				dSortsCount = 5
			)

			// Initialize ioContext
			m.saveClusterState()
			if m.originalTargetCount < 3 {
				t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
			}

			tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
			defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

			wg := &sync.WaitGroup{}
			for i := 0; i < dSortsCount; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					dispatchDSortJob(m, dsorterType, i)
				}(i)
			}
			wg.Wait()
		},
	)
}

// TestDistributedSortChain runs multiple dSorts one after another
func TestDistributedSortChain(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				dSortsCount = 5
			)

			// Initialize ioContext
			m.saveClusterState()
			if m.originalTargetCount < 3 {
				t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
			}

			// Create ais bucket
			tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
			defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

			for i := 0; i < dSortsCount; i++ {
				dispatchDSortJob(m, dsorterType, i)
			}
		},
	)
}

func TestDistributedSortShuffle(t *testing.T) {
	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:                m,
					dsorterType:      dsorterType,
					algorithm:        &dsort.SortAlgorithm{Kind: dsort.SortKindShuffle},
					tarballCnt:       1000,
					fileInTarballCnt: 10,
					maxMemUsage:      "99%",
				}
			)

			// Initialize ioContext
			m.saveClusterState()
			if m.originalTargetCount < 3 {
				t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
			}

			// Create ais bucket
			tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
			defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

			df.init()
			df.createInputShards()

			tutils.Logln("starting distributed sort...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tutils.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(5)
		},
	)
}

func TestDistributedSortWithDisk(t *testing.T) {
	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:                m,
					dsorterType:      dsorterType,
					outputTempl:      "output-{0..1000}",
					tarballCnt:       100,
					fileInTarballCnt: 10,
					maxMemUsage:      "1KB",
				}
			)

			// Initialize ioContext
			m.saveClusterState()
			if m.originalTargetCount < 3 {
				t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
			}

			// Create ais bucket
			tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
			defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

			df.init()
			df.createInputShards()

			tutils.Logln("starting distributed sort with spilling to disk...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tutils.Logln("finished distributed sort")

			allMetrics := df.checkMetrics(false /* expectAbort */)
			for target, metrics := range allMetrics {
				if metrics.Extraction.ExtractedToDiskCnt == 0 && metrics.Extraction.ExtractedCnt > 0 {
					t.Errorf("target %s did not extract any files do disk", target)
				}
			}

			df.checkOutputShards(0)
		},
	)
}

func TestDistributedSortWithCompressionAndDisk(t *testing.T) {
	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:                m,
					dsorterType:      dsorterType,
					tarballCnt:       200,
					fileInTarballCnt: 50,
					extension:        cmn.ExtTarTgz,
					maxMemUsage:      "1KB",
				}
			)

			// Initialize ioContext
			m.saveClusterState()
			if m.originalTargetCount < 3 {
				t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
			}

			// Create ais bucket
			tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
			defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

			df.init()
			df.createInputShards()

			tutils.Logln("starting distributed sort with compression (.tar.gz)...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tutils.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(5)
		},
	)
}

func TestDistributedSortWithMemoryAndDisk(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		m = &ioContext{
			t: t,
		}
		df = &dsortFramework{
			m:                 m,
			dsorterType:       dsort.DSorterGeneralType,
			tarballCnt:        500,
			fileInTarballSize: cmn.MiB,
			fileInTarballCnt:  5,
		}
	)

	// Initialize ioContext
	m.saveClusterState()
	if m.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create ais bucket
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	df.init()
	df.createInputShards()

	// Try to free all memory to get estimated actual used memory size
	cmn.FreeMemToOS()
	time.Sleep(time.Second)

	// Get current memory
	mem, err := sys.Mem()
	tassert.CheckFatal(t, err)
	df.maxMemUsage = cmn.UnsignedB2S(mem.ActualUsed+500*cmn.MiB, 2)

	tutils.Logf("starting distributed sort with using memory and disk (max mem usage: %s)...\n", df.maxMemUsage)
	df.start()

	_, err = tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
	tassert.CheckFatal(t, err)
	tutils.Logln("finished distributed sort")

	allMetrics := df.checkMetrics(false /* expectAbort */)
	var (
		extractedToDisk int64
		extractedTotal  int64
	)
	for _, metrics := range allMetrics {
		extractedToDisk += metrics.Extraction.ExtractedToDiskCnt
		extractedTotal += metrics.Extraction.ExtractedCnt
	}

	if extractedToDisk == 0 {
		t.Error("all extractions by all targets were done exclusively into memory")
	}
	if extractedToDisk == extractedTotal {
		t.Error("all extractions by all targets were done exclusively into disk")
	}

	df.checkOutputShards(5)
}

func TestDistributedSortWithMemoryAndDiskAndCompression(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	var (
		m = &ioContext{
			t: t,
		}
		df = &dsortFramework{
			m:                 m,
			dsorterType:       dsort.DSorterGeneralType,
			tarballCnt:        400,
			fileInTarballSize: cmn.MiB,
			fileInTarballCnt:  3,
			extension:         ".tar.gz",
		}
	)

	// Initialize ioContext
	m.saveClusterState()
	if m.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create ais bucket
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
	defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

	df.init()
	df.createInputShards()

	// Try to free all memory to get estimated actual used memory size
	cmn.FreeMemToOS()
	time.Sleep(time.Second)

	// Get current memory
	mem, err := sys.Mem()
	tassert.CheckFatal(t, err)
	df.maxMemUsage = cmn.UnsignedB2S(mem.ActualUsed+300*cmn.MiB, 2)

	tutils.Logf("starting distributed sort with using memory, disk and compression (max mem usage: %s)...\n", df.maxMemUsage)
	df.start()

	_, err = tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
	tassert.CheckFatal(t, err)
	tutils.Logln("finished distributed sort")

	allMetrics := df.checkMetrics(false /* expectAbort */)
	var (
		extractedToDisk int64
		extractedTotal  int64
	)
	for _, metrics := range allMetrics {
		extractedToDisk += metrics.Extraction.ExtractedToDiskCnt
		extractedTotal += metrics.Extraction.ExtractedCnt
	}

	if extractedToDisk == 0 {
		t.Error("all extractions by all targets were done exclusively into memory")
	}
	if extractedToDisk == extractedTotal {
		t.Error("all extractions by all targets were done exclusively into disk")
	}

	df.checkOutputShards(5)
}

func TestDistributedSortZip(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				err error
				m   = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:                m,
					dsorterType:      dsorterType,
					tarballCnt:       1000,
					fileInTarballCnt: 100,
					extension:        ".zip",
					maxMemUsage:      "99%",
				}
			)

			// Initialize ioContext
			m.saveClusterState()
			if m.originalTargetCount < 3 {
				t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
			}

			// Create ais bucket
			tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
			defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

			df.init()
			df.createInputShards()

			tutils.Logln("starting distributed sort with compression (.zip)...")
			df.start()

			_, err = tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tutils.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(5)
		},
	)
}

func TestDistributedSortWithCompression(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				err error
				m   = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:                m,
					dsorterType:      dsorterType,
					tarballCnt:       1000,
					fileInTarballCnt: 50,
					extension:        cmn.ExtTarTgz,
					maxMemUsage:      "99%",
				}
			)

			// Initialize ioContext
			m.saveClusterState()
			if m.originalTargetCount < 3 {
				t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
			}

			// Create ais bucket
			tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
			defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

			df.init()
			df.createInputShards()

			tutils.Logln("starting distributed sort with compression (.tar.gz)...")
			df.start()

			_, err = tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tutils.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(5)
		},
	)
}

func TestDistributedSortWithContent(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				cases = []struct {
					extension   string
					formatType  string
					missingKeys bool
				}{
					{".loss", extract.FormatTypeInt, false},
					{".cls", extract.FormatTypeFloat, false},
					{".smth", extract.FormatTypeString, false},

					{".loss", extract.FormatTypeInt, true},
					{".cls", extract.FormatTypeFloat, true},
					{".smth", extract.FormatTypeString, true},
				}
			)

			for _, entry := range cases {
				entry := entry // pin
				test := fmt.Sprintf("%s-%v", entry.formatType, entry.missingKeys)
				t.Run(test, func(t *testing.T) {
					t.Parallel()

					var (
						m = &ioContext{
							t: t,
						}
						df = &dsortFramework{
							m:           m,
							dsorterType: dsorterType,
							algorithm: &dsort.SortAlgorithm{
								Kind:       dsort.SortKindContent,
								Extension:  entry.extension,
								FormatType: entry.formatType,
							},
							missingKeys:      entry.missingKeys,
							tarballCnt:       1000,
							fileInTarballCnt: 100,
							maxMemUsage:      "90%",
						}
					)

					// Initialize ioContext
					m.saveClusterState()
					if m.originalTargetCount < 3 {
						t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
					}

					// Create ais bucket
					tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
					defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

					df.init()
					df.createInputShards()

					tutils.Logln("starting distributed sort...")
					df.start()

					aborted, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
					tassert.CheckFatal(t, err)
					if entry.missingKeys && !aborted {
						t.Errorf("%s was not aborted", cmn.DSortName)
					}

					tutils.Logln("checking metrics...")
					allMetrics, err := api.MetricsDSort(df.baseParams, df.managerUUID)
					tassert.CheckFatal(t, err)
					if len(allMetrics) != m.originalTargetCount {
						t.Errorf("number of metrics %d is not same as number of targets %d", len(allMetrics), m.originalTargetCount)
					}

					for target, metrics := range allMetrics {
						if entry.missingKeys && !metrics.Aborted.Load() {
							t.Errorf("%s was not aborted by target: %s", target, cmn.DSortName)
						}
					}

					if !entry.missingKeys {
						df.checkOutputShards(5)
					}
				})
			}
		},
	)
}

func TestDistributedSortAbort(t *testing.T) {
	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				err error
				m   = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:                m,
					dsorterType:      dsorterType,
					tarballCnt:       1000,
					fileInTarballCnt: 10,
				}
			)

			// Initialize ioContext
			m.saveClusterState()
			if m.originalTargetCount < 3 {
				t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
			}

			// Create ais bucket
			tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
			defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

			df.init()
			df.createInputShards()

			tutils.Logln("starting distributed sort...")
			df.start()

			tutils.Logln("aborting distributed sort...")
			err = api.AbortDSort(df.baseParams, df.managerUUID)
			tassert.CheckFatal(t, err)

			tutils.Logln("waiting for distributed sort to finish up...")
			_, err = tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)

			df.checkMetrics(true /* expectAbort */)
		},
	)
}

func TestDistributedSortAbortDuringPhases(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes, phases: dsortPhases},
		func(dsorterType, phase string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:                m,
					dsorterType:      dsorterType,
					tarballCnt:       1000,
					fileInTarballCnt: 200,
				}
			)

			// Initialize ioContext
			m.saveClusterState()
			if m.originalTargetCount < 3 {
				t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
			}

			// Create ais bucket
			tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
			defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

			df.init()
			df.createInputShards()

			tutils.Logf("starting distributed sort (abort on: %s)...\n", phase)
			df.start()

			waitForDSortPhase(t, m.proxyURL, df.managerUUID, phase, func() {
				tutils.Logln("aborting distributed sort...")
				err := api.AbortDSort(df.baseParams, df.managerUUID)
				tassert.CheckFatal(t, err)
			})

			tutils.Logln("waiting for distributed sort to finish up...")
			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)

			df.checkMetrics(true /* expectAbort */)
		},
	)
}

func TestDistributedSortKillTargetDuringPhases(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	runDSortTest(
		t, dsortTestSpec{p: false, types: dsorterTypes, phases: dsortPhases},
		func(dsorterType, phase string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:                m,
					dsorterType:      dsorterType,
					outputTempl:      "output-{0..100000}",
					tarballCnt:       1000,
					fileInTarballCnt: 200,
				}
			)

			// Initialize ioContext
			m.saveClusterState()
			if m.originalTargetCount < 3 {
				t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
			}
			targets := tutils.ExtractTargetNodes(m.smap)
			idx := rand.Intn(len(targets))

			df.init()

			// Create ais bucket
			tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
			defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

			df.createInputShards()

			tutils.Logf("starting distributed sort (abort on: %s)...\n", phase)
			df.start()

			waitForDSortPhase(t, m.proxyURL, df.managerUUID, phase, func() {
				tutils.Logln("killing target...")
				err := tutils.UnregisterNode(m.proxyURL, targets[idx].ID())
				tassert.CheckFatal(t, err)
			})

			tutils.Logln("waiting for distributed sort to finish up...")
			aborted, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckError(t, err)
			if !aborted {
				t.Errorf("%s was not aborted", cmn.DSortName)
			}

			tutils.Logln("checking metrics...")
			allMetrics, err := api.MetricsDSort(df.baseParams, df.managerUUID)
			tassert.CheckError(t, err)
			if len(allMetrics) == m.originalTargetCount {
				t.Errorf("number of metrics %d is same as number of original targets %d", len(allMetrics), m.originalTargetCount)
			}

			for target, metrics := range allMetrics {
				if !metrics.Aborted.Load() {
					t.Errorf("%s was not aborted by target: %s", cmn.DSortName, target)
				}
			}

			m.reregisterTarget(targets[idx])
			time.Sleep(time.Second)
		},
	)
}

func TestDistributedSortManipulateMountpathDuringPhases(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	const newMountpath = "/tmp/ais/mountpath"

	runDSortTest(
		t, dsortTestSpec{p: false, types: dsorterTypes, phases: dsortPhases},
		func(dsorterType, phase string, t *testing.T) {
			for _, adding := range []bool{false, true} {
				t.Run(fmt.Sprintf("%v", adding), func(t *testing.T) {
					var (
						m = &ioContext{
							t: t,
						}
						df = &dsortFramework{
							m:                m,
							dsorterType:      dsorterType,
							outputTempl:      "output-{0..100000}",
							tarballCnt:       2000,
							fileInTarballCnt: 200,
						}

						mountpaths = make(map[*cluster.Snode]string)
					)

					// Initialize ioContext
					m.saveClusterState()
					if m.originalTargetCount < 3 {
						t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
					}

					// Initialize `df.baseParams`
					df.init()

					targets := tutils.ExtractTargetNodes(m.smap)
					for idx, target := range targets {
						if adding {
							mpath := fmt.Sprintf("%s-%d", newMountpath, idx)
							if containers.DockerRunning() {
								err := containers.DockerCreateMpathDir(0, mpath)
								tassert.CheckFatal(t, err)
							} else {
								err := cmn.CreateDir(mpath)
								tassert.CheckFatal(t, err)
							}

							mountpaths[target] = mpath
						} else {
							targetMountpaths, err := api.GetMountpaths(df.baseParams, target)
							tassert.CheckFatal(t, err)
							mountpaths[target] = targetMountpaths.Available[0]
						}
					}

					defer func() {
						for target, mpath := range mountpaths {
							if adding {
								tutils.Logf("removing mountpath %q to %s...\n", mpath, target.ID())
								err := api.RemoveMountpath(df.baseParams, target.ID(), mpath)
								tassert.CheckError(t, err)
								err = os.RemoveAll(mpath)
								tassert.CheckError(t, err)
							} else {
								tutils.Logf("adding mountpath %q to %s...\n", mpath, target.ID())
								err := api.AddMountpath(df.baseParams, target, mpath)
								tassert.CheckError(t, err)
							}
						}
					}()

					tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
					defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

					df.createInputShards()

					tutils.Logf("starting distributed sort (abort on: %s)...\n", phase)
					df.start()

					waitForDSortPhase(t, m.proxyURL, df.managerUUID, phase, func() {
						for target, mpath := range mountpaths {
							if adding {
								tutils.Logf("adding new mountpath %q to %s...\n", mpath, target.ID())
								err := api.AddMountpath(df.baseParams, target, mpath)
								tassert.CheckFatal(t, err)
							} else {
								tutils.Logf("removing mountpath %q from %s...\n", mpath, target.ID())
								err := api.RemoveMountpath(df.baseParams, target.ID(), mpath)
								tassert.CheckFatal(t, err)
							}
						}
					})

					tutils.Logln("waiting for distributed sort to finish up...")
					_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
					tassert.CheckError(t, err)

					df.checkMetrics(true /* expectAbort */)
				})
			}
		},
	)
}

func TestDistributedSortAddTarget(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	runDSortTest(
		t, dsortTestSpec{p: false, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:                m,
					dsorterType:      dsorterType,
					outputTempl:      "output-{0..100000}",
					tarballCnt:       4000,
					fileInTarballCnt: 200,
				}
			)

			// Initialize ioContext
			m.saveClusterState()
			if m.originalTargetCount < 3 {
				t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
			}
			targets := tutils.ExtractTargetNodes(m.smap)

			df.init()

			tutils.Logln("killing target...")
			err := tutils.UnregisterNode(m.proxyURL, targets[0].ID())
			tassert.CheckFatal(t, err)

			// Create ais bucket
			tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
			defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

			df.createInputShards()

			tutils.Logln("starting distributed sort...")
			df.start()

			waitForDSortPhase(t, m.proxyURL, df.managerUUID, dsort.ExtractionPhase, func() {
				// Reregister target 0
				m.reregisterTarget(targets[0])
				tutils.Logln("reregistering complete")
			})

			tutils.Logln("waiting for distributed sort to finish up...")
			aborted, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			if !aborted {
				t.Errorf("%s was not aborted", cmn.DSortName)
			}

			tutils.Logln("checking metrics...")
			allMetrics, err := api.MetricsDSort(df.baseParams, df.managerUUID)
			tassert.CheckFatal(t, err)
			if len(allMetrics) != m.originalTargetCount-1 {
				t.Errorf("number of metrics %d is different than number of targets when %s started %d", len(allMetrics), cmn.DSortName, m.originalTargetCount-1)
			}
		},
	)
}

func TestDistributedSortMetricsAfterFinish(t *testing.T) {
	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:                m,
					dsorterType:      dsorterType,
					outputTempl:      "output-{0..1000}",
					tarballCnt:       50,
					fileInTarballCnt: 10,
				}
			)

			// Initialize ioContext
			m.saveClusterState()
			if m.originalTargetCount < 3 {
				t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
			}

			// Create ais bucket
			tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
			defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

			df.init()
			df.createInputShards()

			tutils.Logln("starting distributed sort...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tutils.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(0)

			tutils.Logln("checking if metrics are still accessible after some time..")
			time.Sleep(2 * time.Second)

			// Check if metrics can be fetched after some time
			df.checkMetrics(false /* expectAbort */)
		},
	)
}

func TestDistributedSortSelfAbort(t *testing.T) {
	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:                m,
					dsorterType:      dsorterType,
					tarballCnt:       1000,
					fileInTarballCnt: 100,
					missingShards:    cmn.AbortReaction,
				}
			)

			// Initialize ioContext
			m.saveClusterState()
			if m.originalTargetCount < 3 {
				t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
			}

			// Create ais bucket
			tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
			defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

			df.init()

			tutils.Logln("starting distributed sort without any files generated...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tutils.Logln("finished distributed sort")

			// Wait a while for all targets to abort
			time.Sleep(2 * time.Second)

			df.checkMetrics(true /* expectAbort */)
		},
	)
}

func TestDistributedSortOnOOM(t *testing.T) {
	t.Skip("test can take more than couple minutes, run it only when necessary")

	runDSortTest(
		t, dsortTestSpec{p: false, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:                 m,
					dsorterType:       dsorterType,
					fileInTarballCnt:  200,
					fileInTarballSize: 10 * cmn.MiB,
					maxMemUsage:       "80%",
				}
			)

			mem, err := sys.Mem()
			tassert.CheckFatal(t, err)

			// Calculate number of shards to cause OOM and overestimate it to make sure
			// that if dSort doesn't prevent it, it will happen. Notice that maxMemUsage
			// is 80% so dSort should never go above this number in memory usage.
			df.tarballCnt = int(float64(mem.ActualFree/uint64(df.fileInTarballSize)/uint64(df.fileInTarballCnt)) * 1.4)

			// Initialize ioContext
			m.saveClusterState()
			if m.originalTargetCount < 3 {
				t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
			}

			// Create ais bucket
			tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
			defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

			df.init()
			df.createInputShards()

			tutils.Logln("starting distributed sort...")
			df.start()

			_, err = tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tutils.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(5)
		},
	)
}

func TestDistributedSortMissingShards(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	runDSortTest(
		t, dsortTestSpec{p: false, types: dsorterTypes, reactions: cmn.SupportedReactions, scopes: dsortSettingScopes},
		func(dsorterType, reaction, scope string, t *testing.T) {
			if scope != scopeConfig {
				t.Parallel()
			}

			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:                m,
					dsorterType:      dsorterType,
					outputTempl:      "output-{0..100000}",
					tarballCnt:       1000,
					tarballCntToSkip: 50,
					fileInTarballCnt: 200,
					extension:        ".tar",
				}
			)

			// Initialize ioContext
			m.saveClusterState()
			if m.originalTargetCount < 3 {
				t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
			}

			tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
			defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

			switch scope {
			case scopeConfig:
				defer tutils.SetClusterConfig(t, cmn.SimpleKVs{"distributed_sort.missing_shards": cmn.IgnoreReaction})
				tutils.SetClusterConfig(t, cmn.SimpleKVs{"distributed_sort.missing_shards": reaction})

				tutils.Logf("changed `missing_shards` config to: %s\n", reaction)
			case scopeSpec:
				df.missingShards = reaction
				tutils.Logf("set `missing_shards` in request spec to: %s\n", reaction)
			default:
				cmn.AssertMsg(false, scope)
			}

			df.init()
			df.createInputShards()

			tutils.Logln("starting distributed sort...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tutils.Logln("finished distributed sort")

			df.checkReactionResult(reaction, df.tarballCntToSkip)
		},
	)
}

func TestDistributedSortDuplications(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	runDSortTest(
		t, dsortTestSpec{p: false, types: dsorterTypes, reactions: cmn.SupportedReactions, scopes: dsortSettingScopes},
		func(dsorterType, reaction, scope string, t *testing.T) {
			if scope != scopeConfig {
				t.Parallel()
			}

			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:                     m,
					dsorterType:           dsorterType,
					outputTempl:           "output-{0..100000}",
					tarballCnt:            1000,
					fileInTarballCnt:      200,
					recordDuplicationsCnt: 50,
					extension:             ".tar",
				}
			)

			// Initialize ioContext
			m.saveClusterState()
			if m.originalTargetCount < 3 {
				t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
			}

			tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
			defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

			switch scope {
			case scopeConfig:
				defer tutils.SetClusterConfig(t, cmn.SimpleKVs{"distributed_sort.duplicated_records": cmn.AbortReaction})
				tutils.SetClusterConfig(t, cmn.SimpleKVs{"distributed_sort.duplicated_records": reaction})

				tutils.Logf("changed `duplicated_records` config to: %s\n", reaction)
			case scopeSpec:
				df.duplicatedRecords = reaction
				tutils.Logf("set `duplicated_records` in request spec to: %s\n", reaction)
			default:
				cmn.AssertMsg(false, scope)
			}

			df.init()
			df.createInputShards()

			tutils.Logln("starting distributed sort...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tutils.Logln("finished distributed sort")

			df.checkReactionResult(reaction, df.recordDuplicationsCnt)
		},
	)
}

func TestDistributedSortOrderFile(t *testing.T) {
	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				err error
				m   = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:           m,
					dsorterType: dsorterType,
					outputBck: cmn.Bck{
						Name:     tutils.GenRandomString(15),
						Provider: cmn.ProviderAIS,
					},
					tarballCnt:       100,
					fileInTarballCnt: 10,
				}

				orderFileName = "orderFileName"
				ekm           = make(map[string]string, 10)
				shardFmts     = []string{
					"shard-%d-suf",
					"input-%d-pref",
					"smth-%d",
				}
				proxyURL = tutils.GetPrimaryURL()
			)

			// Initialize ioContext
			m.saveClusterState()
			if m.originalTargetCount < 3 {
				t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
			}
			baseParams := tutils.BaseAPIParams(proxyURL)

			// Set URL for order file (points to the object in cluster)
			df.orderFileURL = fmt.Sprintf("%s/%s/%s/%s/%s", proxyURL, cmn.Version, cmn.Objects, m.bck.Name, orderFileName)

			df.init()

			// Create ais bucket
			tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
			defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

			// Create local output bucket
			tutils.CreateFreshBucket(t, m.proxyURL, df.outputBck)
			defer tutils.DestroyBucket(t, m.proxyURL, df.outputBck)

			df.createInputShards()

			// Generate content for the orderFile
			tutils.Logln("generating and putting order file into cluster...")
			var (
				buffer       bytes.Buffer
				shardRecords = df.getRecordNames(m.bck)
			)
			for _, shard := range shardRecords {
				for idx, recordName := range shard.recordNames {
					buffer.WriteString(fmt.Sprintf("%s\t%s\n", recordName, shardFmts[idx%len(shardFmts)]))
					ekm[recordName] = shardFmts[idx%len(shardFmts)]
				}
			}
			args := api.PutObjectArgs{BaseParams: baseParams, Bck: m.bck, Object: orderFileName, Reader: tutils.NewBytesReader(buffer.Bytes())}
			err = api.PutObject(args)
			tassert.CheckFatal(t, err)

			tutils.Logln("starting distributed sort...")
			rs := df.gen()
			managerUUID, err := api.StartDSort(baseParams, rs)
			tassert.CheckFatal(t, err)

			_, err = tutils.WaitForDSortToFinish(m.proxyURL, managerUUID)
			tassert.CheckFatal(t, err)
			tutils.Logln("finished distributed sort")

			allMetrics, err := api.MetricsDSort(baseParams, managerUUID)
			tassert.CheckFatal(t, err)
			if len(allMetrics) != m.originalTargetCount {
				t.Errorf("number of metrics %d is not same as number of targets %d", len(allMetrics), m.originalTargetCount)
			}

			tutils.Logln("checking if all records are in specified shards...")
			shardRecords = df.getRecordNames(df.outputBck)
			for _, shard := range shardRecords {
				for _, recordName := range shard.recordNames {
					match := false
					// Some shard with specified format contains the record
					for i := 0; i < 30; i++ {
						match = match || fmt.Sprintf(ekm[recordName], i) == shard.name
					}
					if !match {
						t.Errorf("record %q was not part of any shard with format %q but was in shard %q", recordName, ekm[recordName], shard.name)
					}
				}
			}
		},
	)
}

func TestDistributedSortDryRun(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:                m,
					dsorterType:      dsorterType,
					tarballCnt:       1000,
					fileInTarballCnt: 100,
					dryRun:           true,
				}
			)

			// Initialize ioContext
			m.saveClusterState()
			if m.originalTargetCount < 3 {
				t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
			}

			// Create ais bucket
			tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
			defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

			df.init()
			df.createInputShards()

			tutils.Logln("starting distributed sort...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tutils.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
		},
	)
}

func TestDistributedSortDryRunDisk(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:                m,
					dsorterType:      dsorterType,
					tarballCnt:       1000,
					fileInTarballCnt: 100,
					dryRun:           true,
				}
			)

			// Initialize ioContext
			m.saveClusterState()
			if m.originalTargetCount < 3 {
				t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
			}

			// Create ais bucket
			tutils.CreateFreshBucket(t, m.proxyURL, m.bck)
			defer tutils.DestroyBucket(t, m.proxyURL, m.bck)

			df.init()
			df.createInputShards()

			tutils.Logln("starting distributed sort...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tutils.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
		},
	)
}
