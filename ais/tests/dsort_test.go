// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/containers"
	"github.com/NVIDIA/aistore/devtools/archive"
	"github.com/NVIDIA/aistore/devtools/readers"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tlog"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/dsort/extract"
	"github.com/NVIDIA/aistore/sys"
)

const (
	dsortDescAllPrefix = dsort.DSortNameLowercase + "-test-integration"

	scopeConfig = "config"
	scopeSpec   = "spec"
)

var (
	dsortDescCurPrefix = fmt.Sprintf("%s-%d-", dsortDescAllPrefix, os.Getpid())

	dsorterTypes       = []string{dsort.DSorterGeneralType, dsort.DSorterMemType}
	dsortPhases        = []string{dsort.ExtractionPhase, dsort.SortingPhase, dsort.CreationPhase}
	dsortAlgorithms    = []string{dsort.SortKindAlphanumeric, dsort.SortKindShuffle}
	dsortSettingScopes = []string{scopeConfig, scopeSpec}
)

type (
	dsortTestSpec struct {
		p          bool // determines if the tests should be ran in parallel mode
		types      []string
		phases     []string
		reactions  []string
		scopes     []string
		algorithms []string
	}

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
		recordExts            []string

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
			} else if len(dts.algorithms) > 0 {
				g := f.(func(dsorterType, algorithm string, t *testing.T))
				for _, algorithm := range dts.algorithms {
					algorithm := algorithm // pin
					t.Run(algorithm, func(t *testing.T) {
						if dts.p {
							t.Parallel()
						}
						g(dsorterType, algorithm, t)
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
		df.extension = cos.ExtTar
	}

	// Assumption is that all prefixes end with dash: "-"
	df.inputPrefix = df.inputTempl[:strings.Index(df.inputTempl, "-")+1]
	df.outputPrefix = df.outputTempl[:strings.Index(df.outputTempl, "-")+1]

	if df.fileInTarballSize == 0 {
		df.fileInTarballSize = cos.KiB
	}

	if df.maxMemUsage == "" {
		df.maxMemUsage = "99%"
	}

	df.tarballSize = df.fileInTarballCnt * df.fileInTarballSize
	if df.outputShardSize == "-1" {
		df.outputShardSize = ""
		pt, err := cos.ParseBashTemplate(df.outputTempl)
		cos.AssertNoErr(err)
		df.outputShardCnt = int(pt.Count())
	} else {
		outputShardSize := int64(10 * df.fileInTarballCnt * df.fileInTarballSize)
		df.outputShardSize = cos.B2S(outputShardSize, 0)
		df.outputShardCnt = (df.tarballCnt * df.tarballSize) / int(outputShardSize)
	}

	if df.algorithm == nil {
		df.algorithm = &dsort.SortAlgorithm{}
	}

	df.baseParams = tutils.BaseAPIParams(df.m.proxyURL)
}

func (df *dsortFramework) gen() dsort.RequestSpec {
	return dsort.RequestSpec{
		Description:         generateDSortDesc(),
		Bck:                 df.m.bck,
		OutputBck:           df.outputBck,
		Extension:           df.extension,
		InputFormat:         df.inputTempl,
		OutputFormat:        df.outputTempl,
		OutputShardSize:     df.outputShardSize,
		Algorithm:           *df.algorithm,
		OrderFileURL:        df.orderFileURL,
		ExtractConcMaxLimit: 10,
		CreateConcMaxLimit:  10,
		MaxMemUsage:         df.maxMemUsage,
		DSorterType:         df.dsorterType,
		DryRun:              df.dryRun,

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
		wg    = cos.NewLimitedWaitGroup(40)
		errCh = make(chan error, df.tarballCnt)
	)

	tlog.Logf("creating %d tarballs...\n", df.tarballCnt)
	for i := df.tarballCntToSkip; i < df.tarballCnt; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var (
				err         error
				duplication = i < df.recordDuplicationsCnt
				path        = fmt.Sprintf("%s/%s/%s%d", tmpDir, df.m.bck.Name, df.inputPrefix, i)
				tarName     string
			)
			if df.algorithm.Kind == dsort.SortKindContent {
				tarName = path + cos.ExtTar
			} else {
				tarName = path + df.extension
			}
			if df.algorithm.Kind == dsort.SortKindContent {
				err = archive.CreateTarWithCustomFiles(tarName, df.fileInTarballCnt, df.fileInTarballSize, df.algorithm.FormatType, df.algorithm.Extension, df.missingKeys)
			} else if df.extension == cos.ExtTar {
				err = archive.CreateTarWithRandomFiles(tarName, df.fileInTarballCnt, df.fileInTarballSize, duplication, df.recordExts, nil)
			} else if df.extension == cos.ExtTarTgz {
				err = archive.CreateTarWithRandomFiles(tarName, df.fileInTarballCnt, df.fileInTarballSize, duplication, nil, nil)
			} else if df.extension == cos.ExtZip {
				err = archive.CreateZipWithRandomFiles(tarName, df.fileInTarballCnt, df.fileInTarballSize, nil)
			} else {
				df.m.t.Fail()
			}
			tassert.CheckFatal(df.m.t, err)

			fqn := path + df.extension
			defer os.Remove(fqn)

			reader, err := readers.NewFileReaderFromFile(fqn, cos.ChecksumNone)
			tassert.CheckFatal(df.m.t, err)

			tutils.Put(df.m.proxyURL, df.m.bck, filepath.Base(fqn), reader, errCh)
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		tassert.CheckFatal(df.m.t, err)
	}
	tlog.Logln("done creating tarballs")
}

func (df *dsortFramework) checkOutputShards(zeros int) {
	tlog.Logln("checking if files are sorted...")

	lastName := ""
	var lastValue interface{}

	gzipped := false
	if df.extension != cos.ExtTar {
		gzipped = true
	}

	var (
		inversions = 0
		baseParams = tutils.BaseAPIParams(df.m.proxyURL)

		idx     = 0
		records = make(map[string]int, 100)
	)
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
			files, err := archive.GetFilesFromTarBuffer(buffer, df.algorithm.Extension)
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
			var files []os.FileInfo

			if df.extension == cos.ExtTar || df.extension == cos.ExtTarTgz {
				files, err = archive.GetFileInfosFromTarBuffer(buffer, gzipped)
			} else if df.extension == cos.ExtZip {
				files, err = archive.GetFileInfosFromZipBuffer(buffer)
			}

			tassert.CheckFatal(df.m.t, err)
			if len(files) == 0 {
				df.m.t.Fatal("number of files inside shard is 0")
			}

			for _, file := range files {
				if df.algorithm.Kind == "" || df.algorithm.Kind == dsort.SortKindAlphanumeric {
					if lastName > file.Name() && canonicalName(lastName) != canonicalName(file.Name()) {
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

				// For each record object see if they we weren't split (they should
				// be one after another).
				recordCanonicalName := canonicalName(file.Name())
				prevIdx, ok := records[recordCanonicalName]
				if ok && prevIdx != idx-1 {
					df.m.t.Errorf("record object %q was splitted", file.Name())
				}
				records[recordCanonicalName] = idx

				// Check if the record objects are in the correct order.
				if len(df.recordExts) > 0 {
					ext := extract.Ext(file.Name())
					expectedExt := df.recordExts[idx%len(df.recordExts)]
					if ext != expectedExt {
						df.m.t.Errorf(
							"record objects %q order has been disrupted: %s != %s",
							file.Name(), ext, expectedExt,
						)
					}
				}

				idx++
			}
		}
	}

	if df.algorithm.Kind == dsort.SortKindShuffle {
		if inversions == 0 {
			df.m.t.Fatal("shuffle sorting did not create any inversions")
		}
	}
}

func canonicalName(recordName string) string {
	return strings.TrimSuffix(recordName, extract.Ext(recordName))
}

func (df *dsortFramework) checkReactionResult(reaction string, expectedProblemsCnt int) {
	tlog.Logln("checking metrics and reaction results...")
	allMetrics, err := api.MetricsDSort(df.baseParams, df.managerUUID)
	tassert.CheckFatal(df.m.t, err)
	if len(allMetrics) != df.m.originalTargetCount {
		df.m.t.Errorf("number of metrics %d is not same as number of targets %d", len(allMetrics), df.m.originalTargetCount)
	}

	switch reaction {
	case cmn.IgnoreReaction:
		for target, metrics := range allMetrics {
			if len(metrics.Warnings) != 0 {
				df.m.t.Errorf("target %q has %s warnings: %s", target, dsort.DSortName, metrics.Warnings)
			}
			if len(metrics.Errors) != 0 {
				df.m.t.Errorf("target %q has %s errors: %s", target, dsort.DSortName, metrics.Errors)
			}
		}
	case cmn.WarnReaction:
		totalWarnings := 0
		for target, metrics := range allMetrics {
			totalWarnings += len(metrics.Warnings)

			if len(metrics.Errors) != 0 {
				df.m.t.Errorf("target %q has %s errors: %s", target, dsort.DSortName, metrics.Errors)
			}
		}

		if totalWarnings != expectedProblemsCnt {
			df.m.t.Errorf("number of total warnings %d is different than number of deleted shards: %d", totalWarnings, expectedProblemsCnt)
		}
	case cmn.AbortReaction:
		totalErrors := 0
		for target, metrics := range allMetrics {
			if !metrics.Aborted.Load() {
				df.m.t.Errorf("%s was not aborted by target: %s", dsort.DSortName, target)
			}
			totalErrors += len(metrics.Errors)
		}

		if totalErrors == 0 {
			df.m.t.Error("expected errors on abort, got nothing")
		}
	}
}

func (df *dsortFramework) getRecordNames(bck cmn.Bck) []shardRecords {
	allShardRecords := make([]shardRecords, 0, 10)

	list, err := api.ListObjects(df.baseParams, bck, nil, 0)
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

		files, err := archive.GetFileInfosFromTarBuffer(buffer, false)
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
	tlog.Logln("checking metrics...")
	allMetrics, err := api.MetricsDSort(df.baseParams, df.managerUUID)
	tassert.CheckFatal(df.m.t, err)
	if len(allMetrics) != df.m.originalTargetCount {
		df.m.t.Errorf("number of metrics %d is not same as number of targets %d", len(allMetrics), df.m.originalTargetCount)
	}

	for target, metrics := range allMetrics {
		if expectAbort && !metrics.Aborted.Load() {
			df.m.t.Errorf("%s was not aborted by target: %s", dsort.DSortName, target)
		} else if !expectAbort && metrics.Aborted.Load() {
			df.m.t.Errorf("%s was aborted by target: %s", dsort.DSortName, target)
		}
	}

	return allMetrics
}

// helper for dispatching i-th dSort job
func dispatchDSortJob(m *ioContext, dsorterType string, i int) {
	df := &dsortFramework{
		m:                m,
		dsorterType:      dsorterType,
		inputTempl:       fmt.Sprintf("input%d-{0..999}", i),
		outputTempl:      fmt.Sprintf("output%d-{00000..01000}", i),
		tarballCnt:       500,
		fileInTarballCnt: 50,
		maxMemUsage:      "99%",
	}

	df.init()
	df.createInputShards()

	tlog.Logln("starting distributed sort...")
	df.start()

	_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
	tassert.CheckFatal(m.t, err)
	tlog.Logln("finished distributed sort")

	df.checkMetrics(false /* expectAbort */)
	df.checkOutputShards(5)
}

func waitForDSortPhase(t *testing.T, proxyURL, managerUUID, phaseName string, callback func()) {
	tlog.Logf("waiting for %s phase...\n", phaseName)
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
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

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
					tarballCnt:       500,
					fileInTarballCnt: 100,
					maxMemUsage:      "99%",
				}
			)

			// Initialize ioContext
			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			// Create ais bucket
			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			tlog.Logln("starting distributed sort...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

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
						Name:     cos.RandString(15),
						Provider: apc.ProviderAIS,
					},
					tarballCnt:       500,
					fileInTarballCnt: 100,
					maxMemUsage:      "99%",
				}
			)

			// Initialize ioContext
			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			df.init()

			// Create local output bucket
			tutils.CreateBucketWithCleanup(t, m.proxyURL, df.outputBck, nil)

			tlog.Logln("starting distributed sort...")
			rs := df.gen()
			if _, err := api.StartDSort(df.baseParams, rs); err == nil {
				t.Errorf("expected %s job to fail when input bucket does not exist", dsort.DSortName)
			}

			// Now destroy output bucket and create input bucket
			tutils.DestroyBucket(t, m.proxyURL, df.outputBck)
			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			tlog.Logln("starting second distributed sort...")
			if _, err := api.StartDSort(df.baseParams, rs); err == nil {
				t.Errorf("expected %s job to fail when output bucket does not exist", dsort.DSortName)
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
			m.initWithCleanupAndSaveState()
			m.expectTargets(3)
			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()

			tlog.Logln("starting distributed sort...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

			df.checkMetrics(reaction == cmn.AbortReaction /*expectAbort*/)
			df.checkReactionResult(reaction, df.tarballCnt)
		},
	)
}

func TestDistributedSortWithOutputBucket(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

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
						Name:     cos.RandString(15),
						Provider: apc.ProviderAIS,
					},
					tarballCnt:       500,
					fileInTarballCnt: 100,
					maxMemUsage:      "99%",
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)
			// Create ais buckets
			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			// Create local output bucket
			tutils.CreateBucketWithCleanup(t, m.proxyURL, df.outputBck, nil)

			df.init()
			df.createInputShards()

			tlog.Logln("starting distributed sort...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(5)
		},
	)
}

// TestDistributedSortParallel runs multiple dSorts in parallel
func TestDistributedSortParallel(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	runDSortTest(
		t, dsortTestSpec{p: false, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				dSortsCount = 5
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)
			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

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
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				dSortsCount = 5
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)
			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

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
					tarballCnt:       500,
					fileInTarballCnt: 10,
					maxMemUsage:      "99%",
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)
			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			tlog.Logln("starting distributed sort...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

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
					outputTempl:      "output-%d",
					tarballCnt:       100,
					fileInTarballCnt: 10,
					maxMemUsage:      "1KB",
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)
			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			tlog.Logln("starting distributed sort with spilling to disk...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

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
					extension:        cos.ExtTarTgz,
					maxMemUsage:      "1KB",
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)
			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			tlog.Logln("starting distributed sort with compression (.tar.gz)...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(5)
		},
	)
}

func TestDistributedSortWithMemoryAndDisk(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	var (
		m = &ioContext{
			t: t,
		}
		df = &dsortFramework{
			m:                 m,
			dsorterType:       dsort.DSorterGeneralType,
			tarballCnt:        500,
			fileInTarballSize: cos.MiB,
			fileInTarballCnt:  5,
		}
		mem sys.MemStat
	)

	m.initWithCleanupAndSaveState()
	m.expectTargets(3)
	tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	df.init()
	df.createInputShards()

	// Try to free all memory to get estimated actual used memory size
	cos.FreeMemToOS()
	time.Sleep(time.Second)

	// Get current memory
	err := mem.Get()
	tassert.CheckFatal(t, err)
	df.maxMemUsage = cos.UnsignedB2S(mem.ActualUsed+500*cos.MiB, 2)

	tlog.Logf("starting distributed sort with using memory and disk (max mem usage: %s)...\n", df.maxMemUsage)
	df.start()

	_, err = tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
	tassert.CheckFatal(t, err)
	tlog.Logln("finished distributed sort")

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
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	var (
		m = &ioContext{
			t: t,
		}
		df = &dsortFramework{
			m:                 m,
			dsorterType:       dsort.DSorterGeneralType,
			tarballCnt:        400,
			fileInTarballSize: cos.MiB,
			fileInTarballCnt:  5,
			extension:         cos.ExtTarTgz,
		}
		mem sys.MemStat
	)

	m.initWithCleanupAndSaveState()
	m.expectTargets(3)
	tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	df.init()
	df.createInputShards()

	// Try to free all memory to get estimated actual used memory size
	cos.FreeMemToOS()
	time.Sleep(time.Second)

	// Get current memory
	err := mem.Get()
	tassert.CheckFatal(t, err)
	df.maxMemUsage = cos.UnsignedB2S(mem.ActualUsed+300*cos.MiB, 2)

	tlog.Logf("starting distributed sort with using memory, disk and compression (max mem usage: %s)...\n", df.maxMemUsage)
	df.start()

	_, err = tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
	tassert.CheckFatal(t, err)
	tlog.Logln("finished distributed sort")

	allMetrics := df.checkMetrics(false /*expectAbort*/)
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
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

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
					tarballCnt:       500,
					fileInTarballCnt: 100,
					extension:        ".zip",
					maxMemUsage:      "99%",
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)
			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			tlog.Logln("starting distributed sort with compression (.zip)...")
			df.start()

			_, err = tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(5)
		},
	)
}

func TestDistributedSortWithCompression(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

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
					tarballCnt:       500,
					fileInTarballCnt: 50,
					extension:        cos.ExtTarTgz,
					maxMemUsage:      "99%",
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)
			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			tlog.Logln("starting distributed sort with compression (.tar.gz)...")
			df.start()

			_, err = tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(5)
		},
	)
}

func TestDistributedSortWithContent(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			cases := []struct {
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
							tarballCnt:       500,
							fileInTarballCnt: 100,
							maxMemUsage:      "90%",
						}
					)

					m.initWithCleanupAndSaveState()
					m.expectTargets(3)
					tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

					df.init()
					df.createInputShards()

					tlog.Logln("starting distributed sort...")
					df.start()

					aborted, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
					tassert.CheckFatal(t, err)
					if entry.missingKeys && !aborted {
						t.Errorf("%s was not aborted", dsort.DSortName)
					}

					tlog.Logln("checking metrics...")
					allMetrics, err := api.MetricsDSort(df.baseParams, df.managerUUID)
					tassert.CheckFatal(t, err)
					if len(allMetrics) != m.originalTargetCount {
						t.Errorf("number of metrics %d is not same as number of targets %d", len(allMetrics), m.originalTargetCount)
					}

					for target, metrics := range allMetrics {
						if entry.missingKeys && !metrics.Aborted.Load() {
							t.Errorf("%s was not aborted by target: %s", target, dsort.DSortName)
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
					tarballCnt:       500,
					fileInTarballCnt: 10,
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)
			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			tlog.Logln("starting distributed sort...")
			df.start()

			tlog.Logln("aborting distributed sort...")
			err = api.AbortDSort(df.baseParams, df.managerUUID)
			tassert.CheckFatal(t, err)

			tlog.Logln("waiting for distributed sort to finish up...")
			_, err = tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)

			df.checkMetrics(true /* expectAbort */)
		},
	)
}

func TestDistributedSortAbortDuringPhases(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

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
					tarballCnt:       500,
					fileInTarballCnt: 200,
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			tlog.Logf("starting distributed sort (abort on: %s)...\n", phase)
			df.start()

			waitForDSortPhase(t, m.proxyURL, df.managerUUID, phase, func() {
				tlog.Logln("aborting distributed sort...")
				err := api.AbortDSort(df.baseParams, df.managerUUID)
				tassert.CheckFatal(t, err)
			})

			tlog.Logln("waiting for distributed sort to finish up...")
			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)

			df.checkMetrics(true /* expectAbort */)
		},
	)
}

func TestDistributedSortKillTargetDuringPhases(t *testing.T) {
	t.Skip("test is flaky, run it only when necessary")

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
					fileInTarballCnt: 500,
				}
				target *cluster.Snode
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			df.init()

			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.createInputShards()

			tlog.Logf("starting distributed sort (abort on: %s)...\n", phase)
			df.start()

			waitForDSortPhase(t, m.proxyURL, df.managerUUID, phase, func() {
				// It may require calling AbortXaction(rebalance) &
				// WaitForRebalAndResil() before unregistering
				target = m.startMaintenanceNoRebalance()
			})

			tlog.Logln("waiting for distributed sort to finish up...")
			aborted, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckError(t, err)
			if !aborted {
				t.Errorf("%s was not aborted", dsort.DSortName)
			}

			tlog.Logln("checking metrics...")
			allMetrics, err := api.MetricsDSort(df.baseParams, df.managerUUID)
			tassert.CheckError(t, err)
			if len(allMetrics) == m.originalTargetCount {
				t.Errorf("number of metrics %d is same as number of original targets %d", len(allMetrics), m.originalTargetCount)
			}

			for target, metrics := range allMetrics {
				if !metrics.Aborted.Load() {
					t.Errorf("%s was not aborted by target: %s", dsort.DSortName, target)
				}
			}

			rebID := m.stopMaintenance(target)
			tutils.WaitForRebalanceByID(t, -1 /*orig target cnt*/, df.baseParams, rebID)
		},
	)
}

func TestDistributedSortManipulateMountpathDuringPhases(t *testing.T) {
	t.Skipf("skipping %s", t.Name())

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
							tarballCnt:       500,
							fileInTarballCnt: 200,
						}

						mountpaths = make(map[*cluster.Snode]string)
					)

					m.initWithCleanupAndSaveState()
					m.expectTargets(3)

					// Initialize `df.baseParams`
					df.init()

					targets := m.smap.Tmap.ActiveNodes()
					for idx, target := range targets {
						if adding {
							mpath := fmt.Sprintf("%s-%d", testMpath, idx)
							if containers.DockerRunning() {
								err := containers.DockerCreateMpathDir(0, mpath)
								tassert.CheckFatal(t, err)
							} else {
								err := cos.CreateDir(mpath)
								tassert.CheckFatal(t, err)
							}

							mountpaths[target] = mpath
						} else {
							targetMountpaths, err := api.GetMountpaths(df.baseParams, target)
							tassert.CheckFatal(t, err)
							mountpaths[target] = targetMountpaths.Available[0]
						}
					}

					t.Cleanup(func() {
						// Wait for any resilver that might be still running.
						tutils.WaitForResilvering(t, df.baseParams, nil)

						for target, mpath := range mountpaths {
							if adding {
								tlog.Logf("removing mountpath %q from %s...\n", mpath, target.ID())
								err := api.DetachMountpath(df.baseParams, target, mpath, true /*dont-resil*/)
								tassert.CheckError(t, err)
								err = os.RemoveAll(mpath)
								tassert.CheckError(t, err)
							} else {
								tlog.Logf("adding mountpath %q to %s...\n", mpath, target.ID())
								err := api.AttachMountpath(df.baseParams, target, mpath, true /*force*/)
								tassert.CheckError(t, err)
							}
						}

						tutils.WaitForResilvering(t, df.baseParams, nil)
					})

					tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

					df.createInputShards()

					tlog.Logf("starting distributed sort (abort on: %s)...\n", phase)
					df.start()

					waitForDSortPhase(t, m.proxyURL, df.managerUUID, phase, func() {
						for target, mpath := range mountpaths {
							if adding {
								tlog.Logf("adding new mountpath %q to %s...\n", mpath, target.ID())
								err := api.AttachMountpath(df.baseParams, target, mpath, true /*force*/)
								tassert.CheckFatal(t, err)
							} else {
								tlog.Logf("removing mountpath %q from %s...\n", mpath, target.ID())
								err := api.DetachMountpath(df.baseParams, target, mpath, false /*dont-resil*/)
								tassert.CheckFatal(t, err)
							}
						}
					})

					tlog.Logln("waiting for distributed sort to finish up...")
					_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
					tassert.CheckError(t, err)

					df.checkMetrics(true /*expectAbort*/)
				})
			}
		},
	)
}

func TestDistributedSortAddTarget(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

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
					tarballCnt:       1000,
					fileInTarballCnt: 200,
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			df.init()

			target := m.startMaintenanceNoRebalance()

			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.createInputShards()

			tlog.Logln("starting distributed sort...")
			df.start()

			defer tutils.WaitForRebalAndResil(t, df.baseParams)

			waitForDSortPhase(t, m.proxyURL, df.managerUUID, dsort.ExtractionPhase, func() {
				m.stopMaintenance(target)
			})

			tlog.Logln("waiting for distributed sort to finish up...")
			aborted, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			if !aborted {
				t.Errorf("%s was not aborted", dsort.DSortName)
			}

			tlog.Logln("checking metrics...")
			allMetrics, err := api.MetricsDSort(df.baseParams, df.managerUUID)
			tassert.CheckFatal(t, err)
			if len(allMetrics) != m.originalTargetCount-1 {
				t.Errorf("number of metrics %d is different than number of targets when %s started %d", len(allMetrics), dsort.DSortName, m.originalTargetCount-1)
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

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			tlog.Logln("starting distributed sort...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(0)

			tlog.Logln("checking if metrics are still accessible after some time..")
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
					tarballCnt:       500,
					fileInTarballCnt: 100,
					missingShards:    cmn.AbortReaction,
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()

			tlog.Logln("starting distributed sort without any files generated...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

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
					fileInTarballSize: 10 * cos.MiB,
					maxMemUsage:       "80%",
				}
				mem sys.MemStat
			)

			err := mem.Get()
			tassert.CheckFatal(t, err)

			// Calculate number of shards to cause OOM and overestimate it to make sure
			// that if dSort doesn't prevent it, it will happen. Notice that maxMemUsage
			// is 80% so dSort should never go above this number in memory usage.
			df.tarballCnt = int(float64(mem.ActualFree/uint64(df.fileInTarballSize)/uint64(df.fileInTarballCnt)) * 1.4)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			tlog.Logln("starting distributed sort...")
			df.start()

			_, err = tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(5)
		},
	)
}

func TestDistributedSortMissingShards(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

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
					tarballCnt:       500,
					tarballCntToSkip: 50,
					fileInTarballCnt: 200,
					extension:        cos.ExtTar,
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			switch scope {
			case scopeConfig:
				defer tutils.SetClusterConfig(t, cos.SimpleKVs{"distributed_sort.missing_shards": cmn.IgnoreReaction})
				tutils.SetClusterConfig(t, cos.SimpleKVs{"distributed_sort.missing_shards": reaction})

				tlog.Logf("changed `missing_shards` config to: %s\n", reaction)
			case scopeSpec:
				df.missingShards = reaction
				tlog.Logf("set `missing_shards` in request spec to: %s\n", reaction)
			default:
				cos.AssertMsg(false, scope)
			}

			df.init()
			df.createInputShards()

			tlog.Logln("starting distributed sort...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

			df.checkReactionResult(reaction, df.tarballCntToSkip)
		},
	)
}

func TestDistributedSortDuplications(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

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
					tarballCnt:            500,
					fileInTarballCnt:      200,
					recordDuplicationsCnt: 50,
					extension:             cos.ExtTar,
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			switch scope {
			case scopeConfig:
				defer tutils.SetClusterConfig(t, cos.SimpleKVs{"distributed_sort.duplicated_records": cmn.AbortReaction})
				tutils.SetClusterConfig(t, cos.SimpleKVs{"distributed_sort.duplicated_records": reaction})

				tlog.Logf("changed `duplicated_records` config to: %s\n", reaction)
			case scopeSpec:
				df.duplicatedRecords = reaction
				tlog.Logf("set `duplicated_records` in request spec to: %s\n", reaction)
			default:
				cos.AssertMsg(false, scope)
			}

			df.init()
			df.createInputShards()

			tlog.Logln("starting distributed sort...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

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
						Name:     cos.RandString(15),
						Provider: apc.ProviderAIS,
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
				proxyURL   = tutils.RandomProxyURL()
				baseParams = tutils.BaseAPIParams(proxyURL)
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			// Set URL for order file (points to the object in cluster).
			df.orderFileURL = fmt.Sprintf(
				"%s/%s/%s/%s/%s?%s=%s",
				proxyURL, apc.Version, apc.Objects, m.bck.Name, orderFileName,
				apc.QparamProvider, apc.ProviderAIS,
			)

			df.init()

			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			// Create local output bucket
			tutils.CreateBucketWithCleanup(t, m.proxyURL, df.outputBck, nil)

			df.createInputShards()

			// Generate content for the orderFile
			tlog.Logln("generating and putting order file into cluster...")
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
			args := api.PutObjectArgs{BaseParams: baseParams, Bck: m.bck, Object: orderFileName, Reader: readers.NewBytesReader(buffer.Bytes())}
			err = api.PutObject(args)
			tassert.CheckFatal(t, err)

			tlog.Logln("starting distributed sort...")
			rs := df.gen()
			managerUUID, err := api.StartDSort(baseParams, rs)
			tassert.CheckFatal(t, err)

			_, err = tutils.WaitForDSortToFinish(m.proxyURL, managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

			allMetrics, err := api.MetricsDSort(baseParams, managerUUID)
			tassert.CheckFatal(t, err)
			if len(allMetrics) != m.originalTargetCount {
				t.Errorf("number of metrics %d is not same as number of targets %d", len(allMetrics), m.originalTargetCount)
			}

			tlog.Logln("checking if all records are in specified shards...")
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
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

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
					tarballCnt:       500,
					fileInTarballCnt: 100,
					dryRun:           true,
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			tlog.Logln("starting distributed sort...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
		},
	)
}

func TestDistributedSortDryRunDisk(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

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
					tarballCnt:       500,
					fileInTarballCnt: 100,
					dryRun:           true,
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			tlog.Logln("starting distributed sort...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
		},
	)
}

func TestDistributedSortWithLongerExt(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes, algorithms: dsortAlgorithms},
		func(dsorterType, algorithm string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:                m,
					dsorterType:      dsorterType,
					outputTempl:      "output-%05d",
					tarballCnt:       200,
					fileInTarballCnt: 10,
					maxMemUsage:      "99%",
					algorithm:        &dsort.SortAlgorithm{Kind: algorithm},
					recordExts:       []string{".txt", ".json.info", ".info", ".json"},
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			tlog.Logln("starting distributed sort...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

			df.checkMetrics(false /*expectAbort*/)
			df.checkOutputShards(5)
		},
	)
}

func TestDistributedSortAutomaticallyCalculateOutputShards(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

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
					fileInTarballCnt: 10,
					maxMemUsage:      "99%",
					outputShardSize:  "-1",
					outputTempl:      "output-{0..10}",
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			tutils.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			tlog.Logln("starting distributed sort...")
			df.start()

			_, err := tutils.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

			df.checkMetrics(false /*expectAbort*/)
			df.checkOutputShards(0)
		},
	)
}
