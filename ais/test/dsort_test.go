// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"archive/tar"
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
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ext/dsort"
	"github.com/NVIDIA/aistore/ext/dsort/extract"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/docker"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tarch"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
	jsoniter "github.com/json-iterator/go"
)

const (
	dsortDescAllPrefix = dsort.DSortName + "-test-integration"

	scopeConfig = "config"
	scopeSpec   = "spec"
)

var (
	dsortDescCurPrefix = fmt.Sprintf("%s-%d-", dsortDescAllPrefix, os.Getpid())

	dsorterTypes       = []string{dsort.DSorterGeneralType, dsort.DSorterMemType}
	dsortPhases        = []string{dsort.ExtractionPhase, dsort.SortingPhase, dsort.CreationPhase}
	dsortAlgorithms    = []string{dsort.Alphanumeric, dsort.Shuffle}
	dsortSettingScopes = []string{scopeConfig, scopeSpec}
)

type (
	dsortTestSpec struct {
		p          bool // determines if the tests should be ran in parallel mode
		types      []string
		tarFormats []tar.Format
		phases     []string
		reactions  []string
		scopes     []string
		algs       []string
	}

	dsortFramework struct {
		m *ioContext

		dsorterType string

		outputBck    cmn.Bck
		inputPrefix  string
		outputPrefix string

		inputTempl            apc.ListRange
		outputTempl           string
		orderFileURL          string
		shardCnt              int
		shardCntToSkip        int
		filesPerShard         int
		fileSz                int // in a shard
		shardSize             int
		outputShardCnt        int
		recordDuplicationsCnt int
		recordExts            []string

		inputShards []string

		tarFormat       tar.Format
		extension       string
		alg             *dsort.Algorithm
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

func runDSortTest(t *testing.T, dts dsortTestSpec, f any) {
	if dts.p {
		t.Parallel()
	}

	for _, dsorterType := range dts.types {
		dsorterType := dsorterType // pin
		t.Run(dsorterType, func(t *testing.T) {
			if dts.p {
				t.Parallel()
			}

			if len(dts.tarFormats) > 0 {
				g := f.(func(dsorterType string, tarFormat tar.Format, t *testing.T))
				for _, tf := range dts.tarFormats {
					tarFormat := tf // pin
					t.Run("format-"+tarFormat.String(), func(t *testing.T) {
						if dts.p {
							t.Parallel()
						}
						g(dsorterType, tarFormat, t)
					})
				}
			} else if len(dts.phases) > 0 {
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
			} else if len(dts.algs) > 0 {
				g := f.(func(dsorterType, alg string, t *testing.T))
				for _, alg := range dts.algs {
					alg := alg // pin
					t.Run(alg, func(t *testing.T) {
						if dts.p {
							t.Parallel()
						}
						g(dsorterType, alg, t)
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
	if df.inputTempl.Template == "" {
		df.inputTempl = apc.ListRange{Template: fmt.Sprintf("input-{0..%d}", df.shardCnt-1)}
	}
	if df.outputTempl == "" {
		df.outputTempl = "output-{00000..10000}"
	}
	// NOTE: default extension/format/MIME
	if df.extension == "" {
		df.extension = archive.ExtTar
	}

	// Assumption is that all prefixes end with dash: "-"
	df.inputPrefix = df.inputTempl.Template[:strings.Index(df.inputTempl.Template, "-")+1]
	df.outputPrefix = df.outputTempl[:strings.Index(df.outputTempl, "-")+1]

	if df.fileSz == 0 {
		df.fileSz = cos.KiB
	}

	if df.maxMemUsage == "" {
		df.maxMemUsage = "99%"
	}

	df.shardSize = df.filesPerShard * df.fileSz
	if df.outputShardSize == "-1" {
		df.outputShardSize = ""
		pt, err := cos.ParseBashTemplate(df.outputTempl)
		cos.AssertNoErr(err)
		df.outputShardCnt = int(pt.Count())
	} else {
		outputShardSize := int64(10 * df.filesPerShard * df.fileSz)
		df.outputShardSize = cos.ToSizeIEC(outputShardSize, 0)
		df.outputShardCnt = (df.shardCnt * df.shardSize) / int(outputShardSize)
	}

	if df.alg == nil {
		df.alg = &dsort.Algorithm{}
	}

	df.baseParams = tools.BaseAPIParams(df.m.proxyURL)
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
		Algorithm:           *df.alg,
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
		wg    = cos.NewLimitedWaitGroup(40, 0)
		errCh = make(chan error, df.shardCnt)

		mu = &sync.Mutex{} // to collect inputShards (obj names)
	)
	debug.Assert(len(df.inputShards) == 0)

	tlog.Logf("creating %d shards...\n", df.shardCnt)
	for i := df.shardCntToSkip; i < df.shardCnt; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var (
				err         error
				duplication = i < df.recordDuplicationsCnt
				path        = fmt.Sprintf("%s/%s/%s%d", tmpDir, df.m.bck.Name, df.inputPrefix, i)
				tarName     string
			)
			if df.alg.Kind == dsort.Content {
				tarName = path + archive.ExtTar
			} else {
				tarName = path + df.extension
			}
			if df.alg.Kind == dsort.Content {
				err = tarch.CreateArchCustomFiles(tarName, df.tarFormat, df.extension, df.filesPerShard,
					df.fileSz, df.alg.ContentKeyType, df.alg.Ext, df.missingKeys)
			} else if df.extension == archive.ExtTar {
				err = tarch.CreateArchRandomFiles(tarName, df.tarFormat, df.extension, df.filesPerShard,
					df.fileSz, duplication, df.recordExts, nil)
			} else if df.extension == archive.ExtTarGz || df.extension == archive.ExtZip || df.extension == archive.ExtTarLz4 {
				err = tarch.CreateArchRandomFiles(tarName, df.tarFormat, df.extension, df.filesPerShard,
					df.fileSz, duplication, nil, nil)
			} else {
				df.m.t.Fail()
			}
			tassert.CheckFatal(df.m.t, err)

			reader, err := readers.NewFileReaderFromFile(tarName, cos.ChecksumNone)
			tassert.CheckFatal(df.m.t, err)

			objName := filepath.Base(tarName)
			tools.Put(df.m.proxyURL, df.m.bck, objName, reader, errCh)

			mu.Lock()
			df.inputShards = append(df.inputShards, objName)
			mu.Unlock()

			os.Remove(tarName)
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		tassert.CheckFatal(df.m.t, err)
	}
	tlog.Logln("done creating shards")
}

func (df *dsortFramework) checkOutputShards(zeros int) {
	var (
		lastValue  any
		lastName   string
		inversions int
		idx        int
		baseParams = tools.BaseAPIParams(df.m.proxyURL)
		records    = make(map[string]int, 100)
	)
	tlog.Logln("checking if files are sorted...")
	for i := 0; i < df.outputShardCnt; i++ {
		var (
			buffer    bytes.Buffer
			shardName = fmt.Sprintf("%s%0*d%s", df.outputPrefix, zeros, i, df.extension)
			getArgs   = api.GetArgs{Writer: &buffer}
			bucket    = df.m.bck
		)
		if df.outputBck.Name != "" {
			bucket = df.outputBck
		}

		_, err := api.GetObject(baseParams, bucket, shardName, &getArgs)
		if err != nil && df.extension == archive.ExtZip && i > df.outputShardCnt/2 {
			// We estimated too much output shards to be produced - zip compression
			// was so good that we could fit more files inside the shard.
			//
			// Sanity check to make sure that error did not occur before half
			// of the shards estimated (zip compression should be THAT good).
			break
		}
		tassert.CheckFatal(df.m.t, err)

		if df.alg.Kind == dsort.Content {
			files, err := tarch.GetFilesFromArchBuffer(cos.Ext(shardName), buffer, df.alg.Ext)
			tassert.CheckFatal(df.m.t, err)
			for _, file := range files {
				if file.Ext == df.alg.Ext {
					if strings.TrimSuffix(file.Name, filepath.Ext(file.Name)) !=
						strings.TrimSuffix(lastName, filepath.Ext(lastName)) {
						// custom files should go AFTER the regular files
						df.m.t.Fatalf("names are not in the correct order (shard: %s, lastName: %s, curName: %s)",
							shardName, lastName, file.Name)
					}

					switch df.alg.ContentKeyType {
					case extract.ContentKeyInt:
						intValue, err := strconv.ParseInt(string(file.Content), 10, 64)
						tassert.CheckFatal(df.m.t, err)
						if lastValue != nil && intValue < lastValue.(int64) {
							df.m.t.Fatalf("int values are not in correct order (shard: %s, lastIntValue: %d, curIntValue: %d)", shardName, lastValue.(int64), intValue)
						}
						lastValue = intValue
					case extract.ContentKeyFloat:
						floatValue, err := strconv.ParseFloat(string(file.Content), 64)
						tassert.CheckFatal(df.m.t, err)
						if lastValue != nil && floatValue < lastValue.(float64) {
							df.m.t.Fatalf("string values are not in correct order (shard: %s, lastStringValue: %f, curStringValue: %f)", shardName, lastValue.(float64), floatValue)
						}
						lastValue = floatValue
					case extract.ContentKeyString:
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
			files, err := tarch.GetFileInfosFromArchBuffer(buffer, df.extension)
			tassert.CheckFatal(df.m.t, err)
			if len(files) == 0 {
				df.m.t.Fatal("number of files inside shard is 0")
			}

			for _, file := range files {
				if df.alg.Kind == "" || df.alg.Kind == dsort.Alphanumeric {
					if lastName > file.Name() && canonicalName(lastName) != canonicalName(file.Name()) {
						df.m.t.Fatalf("names are not in correct order (shard: %s, lastName: %s, curName: %s)",
							shardName, lastName, file.Name())
					}
				} else if df.alg.Kind == dsort.Shuffle {
					if lastName > file.Name() {
						inversions++
					}
				}
				if file.Size() != int64(df.fileSz) {
					df.m.t.Fatalf("file sizes has changed (expected: %d, got: %d)", df.fileSz, file.Size())
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
					ext := cos.Ext(file.Name())
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

	if df.alg.Kind == dsort.Shuffle {
		if inversions == 0 {
			df.m.t.Fatal("shuffle sorting did not create any inversions")
		}
	}
}

func canonicalName(recordName string) string {
	return strings.TrimSuffix(recordName, cos.Ext(recordName))
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

	list, err := api.ListObjects(df.baseParams, bck, nil, api.ListArgs{})
	tassert.CheckFatal(df.m.t, err)

	if len(list.Entries) == 0 {
		df.m.t.Errorf("number of objects in bucket %q is 0", bck)
	}
	for _, obj := range list.Entries {
		var (
			buffer  bytes.Buffer
			getArgs = api.GetArgs{Writer: &buffer}
		)
		_, err := api.GetObject(df.baseParams, bck, obj.Name, &getArgs)
		tassert.CheckFatal(df.m.t, err)

		files, err := tarch.GetFileInfosFromArchBuffer(buffer, archive.ExtTar)
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
		m:             m,
		dsorterType:   dsorterType,
		inputTempl:    apc.ListRange{Template: fmt.Sprintf("input%d-{0..999}", i)},
		outputTempl:   fmt.Sprintf("output%d-{00000..01000}", i),
		shardCnt:      500,
		filesPerShard: 50,
		maxMemUsage:   "99%",
	}

	df.init()
	df.createInputShards()

	tlog.Logln("starting distributed sort...")
	df.start()

	_, err := tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
	tassert.CheckFatal(m.t, err)
	tlog.Logln("finished distributed sort")

	df.checkMetrics(false /* expectAbort */)
	df.checkOutputShards(5)
}

func waitForDSortPhase(t *testing.T, proxyURL, managerUUID, phaseName string, callback func()) {
	tlog.Logf("waiting for %s phase...\n", phaseName)
	baseParams := tools.BaseAPIParams(proxyURL)
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
	for _, ext := range []string{archive.ExtTar, archive.ExtTarLz4, archive.ExtZip} {
		for _, lr := range []string{"list", "range"} {
			t.Run(ext+"/"+lr, func(t *testing.T) {
				testDsort(t, ext, lr)
			})
		}
	}
}

func testDsort(t *testing.T, ext, lr string) {
	runDSortTest(
		// Include empty ("") type - in this case type must be selected automatically.
		t, dsortTestSpec{p: true, types: append(dsorterTypes, "")},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:             m,
					extension:     ext,
					dsorterType:   dsorterType,
					shardCnt:      500,
					filesPerShard: 100,
					maxMemUsage:   "99%",
				}
			)
			if testing.Short() {
				df.shardCnt /= 10
			}

			// Initialize ioContext
			m.initWithCleanupAndSaveState()
			m.expectTargets(1)

			// Create ais bucket
			tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			if lr == "list" {
				// iterate list
				df.inputTempl.ObjNames = df.inputShards
				df.inputTempl.Template = ""
				df.missingShards = cmn.AbortReaction // (when shards are explicitly enumerated...)
			}

			tlog.Logln("starting distributed sort ...")
			df.start()

			_, err := tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(5)
		},
	)
}

func TestDistributedSortNonExistingBuckets(t *testing.T) {
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
						Name:     trand.String(15),
						Provider: apc.AIS,
					},
					shardCnt:      500,
					filesPerShard: 100,
					maxMemUsage:   "99%",
				}
			)

			// Initialize ioContext
			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			df.init()

			// Create local output bucket
			tools.CreateBucketWithCleanup(t, m.proxyURL, df.outputBck, nil)

			tlog.Logln("starting distributed sort...")
			rs := df.gen()
			if _, err := api.StartDSort(df.baseParams, rs); err == nil {
				t.Errorf("expected %s job to fail when input bucket does not exist", dsort.DSortName)
			}

			// Now destroy output bucket and create input bucket
			tools.DestroyBucket(t, m.proxyURL, df.outputBck)
			tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			tlog.Logln("starting second distributed sort...")
			if _, err := api.StartDSort(df.baseParams, rs); err == nil {
				t.Errorf("expected %s job to fail when output bucket does not exist", dsort.DSortName)
			}
		},
	)
}

func TestDistributedSortEmptyBucket(t *testing.T) {
	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes, reactions: cmn.SupportedReactions},
		func(dsorterType, reaction string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:             m,
					dsorterType:   dsorterType,
					shardCnt:      100,
					filesPerShard: 10,
					maxMemUsage:   "99%",
					missingShards: reaction,
				}
			)

			// Initialize ioContext
			m.initWithCleanupAndSaveState()
			m.expectTargets(3)
			tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()

			tlog.Logln("starting distributed sort...")
			df.start()

			_, err := tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

			df.checkMetrics(reaction == cmn.AbortReaction /*expectAbort*/)
			df.checkReactionResult(reaction, df.shardCnt)
		},
	)
}

func TestDistributedSortOutputBucket(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

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
						Name:     trand.String(15),
						Provider: apc.AIS,
					},
					shardCnt:      500,
					filesPerShard: 100,
					maxMemUsage:   "99%",
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)
			// Create ais buckets
			tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			// Create local output bucket
			tools.CreateBucketWithCleanup(t, m.proxyURL, df.outputBck, nil)

			df.init()
			df.createInputShards()

			tlog.Logf("starting distributed sort: %d/%d\n", df.shardCnt, df.filesPerShard)
			df.start()

			_, err := tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(5)
		},
	)
}

// TestDistributedSortParallel runs multiple dSorts in parallel
func TestDistributedSortParallel(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

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
			tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

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
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

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
			tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

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
					m:             m,
					dsorterType:   dsorterType,
					alg:           &dsort.Algorithm{Kind: dsort.Shuffle},
					shardCnt:      500,
					filesPerShard: 10,
					maxMemUsage:   "99%",
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)
			tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			tlog.Logf("starting distributed sort: %d/%d\n", df.shardCnt, df.filesPerShard)
			df.start()

			_, err := tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(5)
		},
	)
}

func TestDistributedSortDisk(t *testing.T) {
	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:             m,
					dsorterType:   dsorterType,
					outputTempl:   "output-%d",
					shardCnt:      100,
					filesPerShard: 10,
					maxMemUsage:   "1KB",
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)
			tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()
			tlog.Logf("starting distributed sort with spilling to disk... (%d/%d)\n", df.shardCnt, df.filesPerShard)
			df.start()

			_, err := tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
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

func TestDistributedSortCompressionDisk(t *testing.T) {
	for _, ext := range []string{archive.ExtTarGz, archive.ExtTarLz4} {
		t.Run(ext, func(t *testing.T) {
			runDSortTest(
				t, dsortTestSpec{p: true, types: dsorterTypes},
				func(dsorterType string, t *testing.T) {
					var (
						m = &ioContext{
							t: t,
						}
						df = &dsortFramework{
							m:             m,
							dsorterType:   dsorterType,
							shardCnt:      200,
							filesPerShard: 50,
							extension:     ext,
							maxMemUsage:   "1KB",
						}
					)

					m.initWithCleanupAndSaveState()
					m.expectTargets(3)
					tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

					df.init()
					df.createInputShards()

					tlog.Logf("starting distributed sort: %d/%d, %s\n",
						df.shardCnt, df.filesPerShard, df.extension)
					df.start()

					_, err := tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
					tassert.CheckFatal(t, err)
					tlog.Logln("finished distributed sort")

					df.checkMetrics(false /* expectAbort */)
					df.checkOutputShards(5)
				},
			)
		})
	}
}

func TestDistributedSortMemDisk(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	var (
		m = &ioContext{
			t: t,
		}
		df = &dsortFramework{
			m:             m,
			dsorterType:   dsort.DSorterGeneralType,
			shardCnt:      500,
			fileSz:        cos.MiB,
			filesPerShard: 5,
		}
		mem sys.MemStat
	)

	m.initWithCleanupAndSaveState()
	m.expectTargets(3)
	tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

	df.init()
	df.createInputShards()

	// Try to free all memory to get estimated actual used memory size
	cos.FreeMemToOS()
	time.Sleep(time.Second)

	// Get current memory
	err := mem.Get()
	tassert.CheckFatal(t, err)
	df.maxMemUsage = cos.ToSizeIEC(int64(mem.ActualUsed+500*cos.MiB), 2)

	tlog.Logf("starting distributed sort with memory and disk (max mem usage: %s)... (%d/%d)\n", df.maxMemUsage,
		df.shardCnt, df.filesPerShard)
	df.start()

	_, err = tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
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

func TestDistributedSortMemDiskTarCompression(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})
	for _, ext := range []string{archive.ExtTarGz, archive.ExtTarLz4} {
		t.Run(ext, func(t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:             m,
					dsorterType:   dsort.DSorterGeneralType,
					shardCnt:      400,
					fileSz:        cos.MiB,
					filesPerShard: 5,
					extension:     ext,
				}
				mem sys.MemStat
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)
			tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			// Try to free all memory to get estimated actual used memory size
			cos.FreeMemToOS()
			time.Sleep(time.Second)

			// Get current memory
			err := mem.Get()
			tassert.CheckFatal(t, err)
			df.maxMemUsage = cos.ToSizeIEC(int64(mem.ActualUsed+300*cos.MiB), 2)

			tlog.Logf("starting distributed sort with memory, disk, and compression (max mem usage: %s) ... %d/%d, %s\n",
				df.maxMemUsage, df.shardCnt, df.filesPerShard, df.extension)
			df.start()

			_, err = tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
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
		})
	}
}

func TestDistributedSortZipLz4(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	for _, ext := range []string{archive.ExtZip, archive.ExtTarLz4} {
		t.Run(ext, func(t *testing.T) {
			runDSortTest(
				t, dsortTestSpec{p: true, types: dsorterTypes},
				func(dsorterType string, t *testing.T) {
					var (
						err error
						m   = &ioContext{
							t: t,
						}
						df = &dsortFramework{
							m:             m,
							dsorterType:   dsorterType,
							shardCnt:      500,
							filesPerShard: 100,
							extension:     ext,
							maxMemUsage:   "99%",
						}
					)

					m.initWithCleanupAndSaveState()
					m.expectTargets(3)
					tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

					df.init()
					df.createInputShards()

					tlog.Logf("starting distributed sort: %d/%d, %s\n",
						df.shardCnt, df.filesPerShard, df.extension)
					df.start()

					_, err = tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
					tassert.CheckFatal(t, err)
					tlog.Logln("finished distributed sort")

					df.checkMetrics(false /* expectAbort */)
					df.checkOutputShards(5)
				},
			)
		})
	}
}

func TestDistributedSortTarCompression(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})
	for _, ext := range []string{archive.ExtTarGz, archive.ExtTarLz4} {
		t.Run(ext, func(t *testing.T) {
			runDSortTest(
				t, dsortTestSpec{p: true, types: dsorterTypes},
				func(dsorterType string, t *testing.T) {
					var (
						err error
						m   = &ioContext{
							t: t,
						}
						df = &dsortFramework{
							m:             m,
							dsorterType:   dsorterType,
							shardCnt:      500,
							filesPerShard: 50,
							extension:     ext,
							maxMemUsage:   "99%",
						}
					)

					m.initWithCleanupAndSaveState()
					m.expectTargets(3)
					tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

					df.init()
					df.createInputShards()

					tlog.Logf("starting distributed sort: %d/%d, %s\n",
						df.shardCnt, df.filesPerShard, df.extension)
					df.start()

					_, err = tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
					tassert.CheckFatal(t, err)
					tlog.Logln("finished distributed sort")

					df.checkMetrics(false /* expectAbort */)
					df.checkOutputShards(5)
				},
			)
		})
	}
}

func TestDistributedSortContent(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			cases := []struct {
				extension      string
				contentKeyType string
				missingKeys    bool
			}{
				{".loss", extract.ContentKeyInt, false},
				{".cls", extract.ContentKeyFloat, false},
				{".smth", extract.ContentKeyString, false},

				{".loss", extract.ContentKeyInt, true},
				{".cls", extract.ContentKeyFloat, true},
				{".smth", extract.ContentKeyString, true},
			}

			for _, entry := range cases {
				entry := entry // pin
				test := fmt.Sprintf("%s-%v", entry.contentKeyType, entry.missingKeys)
				t.Run(test, func(t *testing.T) {
					t.Parallel()

					var (
						m = &ioContext{
							t: t,
						}
						df = &dsortFramework{
							m:           m,
							dsorterType: dsorterType,
							alg: &dsort.Algorithm{
								Kind:           dsort.Content,
								Ext:            entry.extension,
								ContentKeyType: entry.contentKeyType,
							},
							missingKeys:   entry.missingKeys,
							shardCnt:      500,
							filesPerShard: 100,
							maxMemUsage:   "90%",
						}
					)

					m.initWithCleanupAndSaveState()
					m.expectTargets(3)
					tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

					df.init()
					df.createInputShards()

					tlog.Logf("starting distributed sort: %d/%d\n", df.shardCnt, df.filesPerShard)
					df.start()

					aborted, err := tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
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
					m:             m,
					dsorterType:   dsorterType,
					shardCnt:      500,
					filesPerShard: 10,
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)
			tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			tlog.Logf("starting distributed sort: %d/%d\n", df.shardCnt, df.filesPerShard)
			df.start()

			tlog.Logln("aborting distributed sort...")
			err = api.AbortDSort(df.baseParams, df.managerUUID)
			tassert.CheckFatal(t, err)

			tlog.Logln("waiting for distributed sort to finish up...")
			_, err = tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)

			df.checkMetrics(true /* expectAbort */)
		},
	)
}

func TestDistributedSortAbortDuringPhases(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes, phases: dsortPhases},
		func(dsorterType, phase string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:             m,
					dsorterType:   dsorterType,
					shardCnt:      500,
					filesPerShard: 200,
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

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
			_, err := tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
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
					m:             m,
					dsorterType:   dsorterType,
					outputTempl:   "output-{0..100000}",
					shardCnt:      1000,
					filesPerShard: 500,
				}
				target *meta.Snode
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			df.init()

			tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.createInputShards()

			tlog.Logf("starting distributed sort (abort on: %s)...\n", phase)
			df.start()

			waitForDSortPhase(t, m.proxyURL, df.managerUUID, phase, func() {
				// It may require calling AbortXaction(rebalance) &
				// WaitForRebalAndResil() before unregistering
				target = m.startMaintenanceNoRebalance()
			})

			tlog.Logln("waiting for distributed sort to finish up...")
			aborted, err := tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckError(t, err)
			if !aborted {
				t.Errorf("%s was not aborted", dsort.DSortName)
			}

			tlog.Logln("checking metrics...")
			allMetrics, err := api.MetricsDSort(df.baseParams, df.managerUUID)
			tassert.CheckError(t, err)
			if len(allMetrics) == m.originalTargetCount {
				t.Errorf("number of metrics %d is same as number of original targets %d",
					len(allMetrics), m.originalTargetCount)
			}

			for target, metrics := range allMetrics {
				if !metrics.Aborted.Load() {
					t.Errorf("%s was not aborted by target: %s", dsort.DSortName, target)
				}
			}

			rebID := m.stopMaintenance(target)
			tools.WaitForRebalanceByID(t, df.baseParams, rebID)
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
							m:             m,
							dsorterType:   dsorterType,
							outputTempl:   "output-{0..100000}",
							shardCnt:      500,
							filesPerShard: 200,
						}

						mountpaths = make(map[*meta.Snode]string)
					)

					m.initWithCleanupAndSaveState()
					m.expectTargets(3)

					// Initialize `df.baseParams`
					df.init()

					targets := m.smap.Tmap.ActiveNodes()
					for idx, target := range targets {
						if adding {
							mpath := fmt.Sprintf("%s-%d", testMpath, idx)
							if docker.IsRunning() {
								err := docker.CreateMpathDir(0, mpath)
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
						tools.WaitForResilvering(t, df.baseParams, nil)

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

						tools.WaitForResilvering(t, df.baseParams, nil)
					})

					tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

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
					_, err := tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
					tassert.CheckError(t, err)

					df.checkMetrics(true /*expectAbort*/)
				})
			}
		},
	)
}

func TestDistributedSortAddTarget(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	runDSortTest(
		t, dsortTestSpec{p: false, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:             m,
					dsorterType:   dsorterType,
					outputTempl:   "output-{0..100000}",
					shardCnt:      1000,
					filesPerShard: 200,
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			df.init()

			target := m.startMaintenanceNoRebalance()

			tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.createInputShards()

			tlog.Logf("starting distributed sort: %d/%d\n", df.shardCnt, df.filesPerShard)
			df.start()

			defer tools.WaitForRebalAndResil(t, df.baseParams)

			waitForDSortPhase(t, m.proxyURL, df.managerUUID, dsort.ExtractionPhase, func() {
				m.stopMaintenance(target)
			})

			tlog.Logln("waiting for distributed sort to finish up...")
			aborted, err := tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
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
					m:             m,
					dsorterType:   dsorterType,
					outputTempl:   "output-{0..1000}",
					shardCnt:      50,
					filesPerShard: 10,
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			tlog.Logf("starting distributed sort: %d/%d\n", df.shardCnt, df.filesPerShard)
			df.start()

			_, err := tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
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
					m:             m,
					dsorterType:   dsorterType,
					shardCnt:      500,
					filesPerShard: 100,
					missingShards: cmn.AbortReaction,
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()

			tlog.Logf("starting distributed sort: %d/%d\n", df.shardCnt, df.filesPerShard)
			df.start()

			_, err := tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
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
					m:             m,
					dsorterType:   dsorterType,
					filesPerShard: 200,
					fileSz:        10 * cos.MiB,
					maxMemUsage:   "80%",
				}
				mem sys.MemStat
			)

			err := mem.Get()
			tassert.CheckFatal(t, err)

			// Calculate number of shards to cause OOM and overestimate it to make sure
			// that if dSort doesn't prevent it, it will happen. Notice that maxMemUsage
			// is 80% so dSort should never go above this number in memory usage.
			df.shardCnt = int(float64(mem.ActualFree/uint64(df.fileSz)/uint64(df.filesPerShard)) * 1.4)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			tlog.Logf("starting distributed sort: %d/%d\n", df.shardCnt, df.filesPerShard)
			df.start()

			_, err = tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(5)
		},
	)
}

func TestDistributedSortMissingShards(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})
	for _, ext := range []string{archive.ExtTar, archive.ExtTarLz4} {
		t.Run(ext, func(t *testing.T) {
			runDSortTest(
				t, dsortTestSpec{
					p:         false,
					types:     dsorterTypes,
					reactions: cmn.SupportedReactions,
					scopes:    dsortSettingScopes,
				},
				func(dsorterType, reaction, scope string, t *testing.T) {
					if scope != scopeConfig {
						t.Parallel()
					}

					var (
						m = &ioContext{
							t: t,
						}
						df = &dsortFramework{
							m:              m,
							dsorterType:    dsorterType,
							outputTempl:    "output-{0..100000}",
							shardCnt:       500,
							shardCntToSkip: 50,
							filesPerShard:  200,
							extension:      ext,
						}
					)

					m.initWithCleanupAndSaveState()
					m.expectTargets(3)

					tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

					switch scope {
					case scopeConfig:
						defer tools.SetClusterConfig(t, cos.StrKVs{"distributed_sort.missing_shards": cmn.IgnoreReaction})
						tools.SetClusterConfig(t, cos.StrKVs{"distributed_sort.missing_shards": reaction})

						tlog.Logf("changed `missing_shards` config to: %s\n", reaction)
					case scopeSpec:
						df.missingShards = reaction
						tlog.Logf("set `missing_shards` in request spec to: %s\n", reaction)
					default:
						cos.AssertMsg(false, scope)
					}

					df.init()
					df.createInputShards()

					tlog.Logf("starting distributed sort: %d/%d, %s\n",
						df.shardCnt, df.filesPerShard, df.extension)
					df.start()

					_, err := tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
					tassert.CheckFatal(t, err)
					tlog.Logln("finished distributed sort")

					df.checkReactionResult(reaction, df.shardCntToSkip)
				},
			)
		})
	}
}

func TestDistributedSortDuplications(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})
	for _, ext := range []string{archive.ExtTar} { // TODO: currently, fails archive.ExtTarLz4 and archive.ExtTarGz, both
		t.Run(ext, func(t *testing.T) {
			runDSortTest(
				t, dsortTestSpec{
					p:         false,
					types:     dsorterTypes,
					reactions: cmn.SupportedReactions,
					scopes:    dsortSettingScopes,
				},
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
							shardCnt:              500,
							filesPerShard:         200,
							recordDuplicationsCnt: 50,
							extension:             ext,
						}
					)
					m.initWithCleanupAndSaveState()
					m.expectTargets(3)
					tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

					switch scope {
					case scopeConfig:
						defer tools.SetClusterConfig(t,
							cos.StrKVs{"distributed_sort.duplicated_records": cmn.AbortReaction})
						tools.SetClusterConfig(t, cos.StrKVs{"distributed_sort.duplicated_records": reaction})

						tlog.Logf("changed `duplicated_records` config to: %s\n", reaction)
					case scopeSpec:
						df.duplicatedRecords = reaction
						tlog.Logf("set `duplicated_records` in request spec to: %s\n", reaction)
					default:
						cos.AssertMsg(false, scope)
					}

					df.init()
					df.createInputShards()

					tlog.Logf("starting distributed sort: %d/%d, %s\n",
						df.shardCnt, df.filesPerShard, df.extension)
					df.start()

					_, err := tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
					tassert.CheckFatal(t, err)
					tlog.Logln("finished distributed sort")

					df.checkReactionResult(reaction, df.recordDuplicationsCnt)
				},
			)
		})
	}
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
						Name:     trand.String(15),
						Provider: apc.AIS,
					},
					shardCnt:      100,
					filesPerShard: 10,
				}

				orderFileName = "orderFileName"
				ekm           = make(map[string]string, 10)
				shardFmts     = []string{
					"shard-%d-suf",
					"input-%d-pref",
					"smth-%d",
				}
				proxyURL   = tools.RandomProxyURL()
				baseParams = tools.BaseAPIParams(proxyURL)
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			// Set URL for order file (points to the object in cluster).
			df.orderFileURL = fmt.Sprintf(
				"%s/%s/%s/%s/%s?%s=%s",
				proxyURL, apc.Version, apc.Objects, m.bck.Name, orderFileName,
				apc.QparamProvider, apc.AIS,
			)

			df.init()

			tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			// Create local output bucket
			tools.CreateBucketWithCleanup(t, m.proxyURL, df.outputBck, nil)

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
			args := api.PutArgs{
				BaseParams: baseParams,
				Bck:        m.bck,
				ObjName:    orderFileName,
				Reader:     readers.NewBytesReader(buffer.Bytes()),
			}
			_, err = api.PutObject(args)
			tassert.CheckFatal(t, err)

			tlog.Logln("starting distributed sort...")
			rs := df.gen()
			managerUUID, err := api.StartDSort(baseParams, rs)
			tassert.CheckFatal(t, err)

			_, err = tools.WaitForDSortToFinish(m.proxyURL, managerUUID)
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

func TestDistributedSortOrderJSONFile(t *testing.T) {
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
						Name:     trand.String(15),
						Provider: apc.AIS,
					},
					shardCnt:      100,
					filesPerShard: 10,
				}

				orderFileName = "order_file_name.json"
				ekm           = make(map[string]string, 10)
				shardFmts     = []string{
					"shard-%d-suf",
					"input-%d-pref",
					"smth-%d",
				}
				proxyURL   = tools.RandomProxyURL()
				baseParams = tools.BaseAPIParams(proxyURL)
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			// Set URL for order file (points to the object in cluster).
			df.orderFileURL = fmt.Sprintf(
				"%s/%s/%s/%s/%s?%s=%s",
				proxyURL, apc.Version, apc.Objects, m.bck.Name, orderFileName,
				apc.QparamProvider, apc.AIS,
			)

			df.init()

			tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			// Create local output bucket
			tools.CreateBucketWithCleanup(t, m.proxyURL, df.outputBck, nil)

			df.createInputShards()

			// Generate content for the orderFile
			tlog.Logln("generating and putting order file into cluster...")
			var (
				content      = make(map[string][]string, 10)
				shardRecords = df.getRecordNames(m.bck)
			)
			for _, shard := range shardRecords {
				for idx, recordName := range shard.recordNames {
					shardFmt := shardFmts[idx%len(shardFmts)]
					content[shardFmt] = append(content[shardFmt], recordName)
					ekm[recordName] = shardFmts[idx%len(shardFmts)]
				}
			}
			jsonBytes, err := jsoniter.Marshal(content)
			tassert.CheckFatal(t, err)
			args := api.PutArgs{
				BaseParams: baseParams,
				Bck:        m.bck,
				ObjName:    orderFileName,
				Reader:     readers.NewBytesReader(jsonBytes),
			}
			_, err = api.PutObject(args)
			tassert.CheckFatal(t, err)

			tlog.Logln("starting distributed sort...")
			rs := df.gen()
			managerUUID, err := api.StartDSort(baseParams, rs)
			tassert.CheckFatal(t, err)

			_, err = tools.WaitForDSortToFinish(m.proxyURL, managerUUID)
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
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:             m,
					dsorterType:   dsorterType,
					shardCnt:      500,
					filesPerShard: 100,
					dryRun:        true,
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			tlog.Logln("starting distributed sort...")
			df.start()

			_, err := tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
		},
	)
}

func TestDistributedSortDryRunDisk(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:             m,
					dsorterType:   dsorterType,
					shardCnt:      500,
					filesPerShard: 100,
					dryRun:        true,
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			tlog.Logln("starting distributed sort...")
			df.start()

			_, err := tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
		},
	)
}

func TestDistributedSortLongerExt(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes, algs: dsortAlgorithms},
		func(dsorterType, alg string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:             m,
					dsorterType:   dsorterType,
					outputTempl:   "output-%05d",
					shardCnt:      200,
					filesPerShard: 10,
					maxMemUsage:   "99%",
					alg:           &dsort.Algorithm{Kind: alg},
					recordExts:    []string{".txt", ".json.info", ".info", ".json"},
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			tlog.Logln("starting distributed sort...")
			df.start()

			_, err := tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

			df.checkMetrics(false /*expectAbort*/)
			df.checkOutputShards(5)
		},
	)
}

func TestDistributedSortAutomaticallyCalculateOutputShards(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	runDSortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:               m,
					dsorterType:     dsorterType,
					shardCnt:        200,
					filesPerShard:   10,
					maxMemUsage:     "99%",
					outputShardSize: "-1",
					outputTempl:     "output-{0..10}",
				}
			)

			m.initWithCleanupAndSaveState()
			m.expectTargets(3)

			tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			tlog.Logln("starting distributed sort...")
			df.start()

			_, err := tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

			df.checkMetrics(false /*expectAbort*/)
			df.checkOutputShards(0)
		},
	)
}

func TestDistributedSortWithTarFormats(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	runDSortTest(
		// Include empty ("") type - in this case type must be selected automatically.
		t, dsortTestSpec{p: true, types: append(dsorterTypes, ""),
			tarFormats: []tar.Format{tar.FormatUnknown, tar.FormatGNU, tar.FormatPAX}},
		func(dsorterType string, tarFormat tar.Format, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:             m,
					dsorterType:   dsorterType,
					shardCnt:      500,
					filesPerShard: 100,
					maxMemUsage:   "1B",
					tarFormat:     tarFormat,
					recordExts:    []string{".txt"},
				}
			)

			// Initialize ioContext
			m.initWithCleanupAndSaveState()
			m.expectTargets(1)

			// Create ais bucket
			tools.CreateBucketWithCleanup(t, m.proxyURL, m.bck, nil)

			df.init()
			df.createInputShards()

			tlog.Logln("starting distributed sort...")
			df.start()

			_, err := tools.WaitForDSortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logln("finished distributed sort")

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(5)
		},
	)
}
