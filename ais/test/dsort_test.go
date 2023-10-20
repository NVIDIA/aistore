// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"archive/tar"
	"bytes"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	rdebug "runtime/debug"
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
	"github.com/NVIDIA/aistore/ext/dsort/shard"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/docker"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tarch"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact"
	jsoniter "github.com/json-iterator/go"
)

const (
	dsortDescAllPrefix = apc.ActDsort + "-test-integration"

	scopeConfig = "config"
	scopeSpec   = "spec"
)

const (
	startingDS = "starting dsort"
)

var (
	dsortDescCurPrefix = fmt.Sprintf("%s-%d-", dsortDescAllPrefix, os.Getpid())

	dsorterTypes       = []string{dsort.GeneralType, dsort.MemType}
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
		inputExt        string
		outputExt       string
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

func generateDsortDesc() string {
	return dsortDescCurPrefix + time.Now().Format(time.RFC3339Nano)
}

func runDsortTest(t *testing.T, dts dsortTestSpec, f any) {
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

////////////////////
// dsortFramework //
////////////////////

func (df *dsortFramework) job() string {
	if df.managerUUID == "" {
		return "dsort[-]"
	}
	return "dsort[" + df.managerUUID + "]"
}

func (df *dsortFramework) init() {
	if df.inputTempl.Template == "" {
		df.inputTempl = apc.ListRange{Template: fmt.Sprintf("input-{0..%d}", df.shardCnt-1)}
	}
	if df.outputTempl == "" {
		df.outputTempl = "output-{00000..10000}"
	}
	if df.inputExt == "" {
		df.inputExt = dsort.DefaultExt
	}

	// Assumption is that all prefixes end with dash: "-"
	df.inputPrefix = df.inputTempl.Template[:strings.Index(df.inputTempl.Template, "-")+1]
	df.outputPrefix = df.outputTempl[:strings.Index(df.outputTempl, "-")+1]

	if df.fileSz == 0 {
		df.fileSz = cos.KiB
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
		Description:         generateDsortDesc(),
		InputBck:            df.m.bck,
		OutputBck:           df.outputBck,
		InputExtension:      df.inputExt,
		OutputExtension:     df.outputExt,
		InputFormat:         df.inputTempl,
		OutputFormat:        df.outputTempl,
		OutputShardSize:     df.outputShardSize,
		Algorithm:           *df.alg,
		OrderFileURL:        df.orderFileURL,
		ExtractConcMaxLimit: 10,
		CreateConcMaxLimit:  10,
		MaxMemUsage:         df.maxMemUsage,
		DsorterType:         df.dsorterType,
		DryRun:              df.dryRun,

		Config: cmn.DsortConf{
			MissingShards:     df.missingShards,
			DuplicatedRecords: df.duplicatedRecords,
		},
	}
}

func (df *dsortFramework) start() {
	var (
		err  error
		spec = df.gen()
	)
	df.managerUUID, err = api.StartDsort(df.baseParams, &spec)
	tassert.CheckFatal(df.m.t, err)
}

func (df *dsortFramework) createInputShards() {
	const tmpDir = "/tmp"
	var (
		wg    = cos.NewLimitedWaitGroup(sys.NumCPU(), 0)
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
				tarName = path + df.inputExt
			}
			if df.alg.Kind == dsort.Content {
				err = tarch.CreateArchCustomFiles(tarName, df.tarFormat, df.inputExt, df.filesPerShard,
					df.fileSz, df.alg.ContentKeyType, df.alg.Ext, df.missingKeys)
			} else if df.inputExt == archive.ExtTar {
				err = tarch.CreateArchRandomFiles(tarName, df.tarFormat, df.inputExt, df.filesPerShard,
					df.fileSz, duplication, df.recordExts, nil)
			} else {
				err = tarch.CreateArchRandomFiles(tarName, df.tarFormat, df.inputExt, df.filesPerShard,
					df.fileSz, duplication, nil, nil)
			}
			tassert.CheckFatal(df.m.t, err)

			reader, err := readers.NewExistingFile(tarName, cos.ChecksumNone)
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
	tlog.Logf("%s: done creating shards\n", df.job())
}

func (df *dsortFramework) checkOutputShards(zeros int) {
	var (
		lastValue  any
		lastName   string
		inversions int
		idx        int
		baseParams = tools.BaseAPIParams(df.m.proxyURL)
		records    = make(map[string]int, 100)

		realOutputShardCnt int
		skipped            int
	)
	tlog.Logf("%s: checking that files are sorted...\n", df.job())
outer:
	for i := 0; i < df.outputShardCnt; i++ {
		var (
			buffer    bytes.Buffer
			shardName = fmt.Sprintf("%s%0*d%s", df.outputPrefix, zeros, i, df.inputExt)
			getArgs   = api.GetArgs{Writer: &buffer}
			bucket    = df.m.bck
		)
		if df.outputBck.Name != "" {
			bucket = df.outputBck
		}

		_, err := api.GetObject(baseParams, bucket, shardName, &getArgs)
		if err != nil {
			herr, ok := err.(*cmn.ErrHTTP)
			if ok && herr.Status == http.StatusNotFound && shard.IsCompressed(df.inputExt) && i > 0 {
				// check for NotFound a few more, and break; see also 'skipped == 0' check below
				switch skipped {
				case 0:
					tlog.Logf("%s: computed output shard count (%d) vs compression: [%s] is the first not-found\n",
						df.job(), df.outputShardCnt, shardName)
					fallthrough
				case 1, 2, 3:
					skipped++
					continue
				default:
					break outer
				}
			}
			tassert.CheckFatal(df.m.t, err)
		}

		tassert.Fatalf(df.m.t, skipped == 0, "%s: got out of order shard %s (not-found >= %d)", df.job(), shardName, skipped)

		realOutputShardCnt++

		if df.alg.Kind == dsort.Content {
			files, err := tarch.GetFilesFromArchBuffer(cos.Ext(shardName), buffer, df.alg.Ext)
			tassert.CheckFatal(df.m.t, err)
			for _, file := range files {
				if file.Ext == df.alg.Ext {
					if strings.TrimSuffix(file.Name, filepath.Ext(file.Name)) !=
						strings.TrimSuffix(lastName, filepath.Ext(lastName)) {
						// custom files should go AFTER the regular files
						df.m.t.Fatalf("%s: names out of order (shard: %s, lastName: %s, curName: %s)",
							df.job(), shardName, lastName, file.Name)
					}

					switch df.alg.ContentKeyType {
					case shard.ContentKeyInt:
						intValue, err := strconv.ParseInt(string(file.Content), 10, 64)
						tassert.CheckFatal(df.m.t, err)
						if lastValue != nil && intValue < lastValue.(int64) {
							df.m.t.Fatalf("%s: int values are not in correct order (shard: %s, lastIntValue: %d, curIntValue: %d)", df.job(), shardName, lastValue.(int64), intValue)
						}
						lastValue = intValue
					case shard.ContentKeyFloat:
						floatValue, err := strconv.ParseFloat(string(file.Content), 64)
						tassert.CheckFatal(df.m.t, err)
						if lastValue != nil && floatValue < lastValue.(float64) {
							df.m.t.Fatalf("%s: string values are not in correct order (shard: %s, lastStringValue: %f, curStringValue: %f)", df.job(), shardName, lastValue.(float64), floatValue)
						}
						lastValue = floatValue
					case shard.ContentKeyString:
						stringValue := string(file.Content)
						if lastValue != nil && stringValue < lastValue.(string) {
							df.m.t.Fatalf("%s: string values are not in correct order (shard: %s, lastStringValue: %s, curStringValue: %s)", df.job(), shardName, lastValue.(string), stringValue)
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
			files, err := tarch.GetFileInfosFromArchBuffer(buffer, df.inputExt)
			tassert.CheckFatal(df.m.t, err)
			if len(files) == 0 {
				df.m.t.Fatalf("%s: number of files inside shard is 0", df.job())
			}

			for _, file := range files {
				if df.alg.Kind == "" || df.alg.Kind == dsort.Alphanumeric {
					if lastName > file.Name() && canonicalName(lastName) != canonicalName(file.Name()) {
						df.m.t.Fatalf("%s: names out of order (shard: %s, lastName: %s, curName: %s)",
							df.job(), shardName, lastName, file.Name())
					}
				} else if df.alg.Kind == dsort.Shuffle {
					if lastName > file.Name() {
						inversions++
					}
				}
				if file.Size() != int64(df.fileSz) {
					df.m.t.Fatalf("%s: file sizes has changed (expected: %d, got: %d)",
						df.job(), df.fileSz, file.Size())
				}
				lastName = file.Name()

				// For each record object see if they we weren't split (they should
				// be one after another).
				recordCanonicalName := canonicalName(file.Name())
				prevIdx, ok := records[recordCanonicalName]
				if ok && prevIdx != idx-1 {
					df.m.t.Errorf("%s: record object %q was splitted", df.job(), file.Name())
				}
				records[recordCanonicalName] = idx

				// Check if the record objects are in the correct order.
				if len(df.recordExts) > 0 {
					ext := cos.Ext(file.Name())
					expectedExt := df.recordExts[idx%len(df.recordExts)]
					if ext != expectedExt {
						df.m.t.Errorf("%s: record objects %q order has been disrupted: %s != %s",
							df.job(), file.Name(), ext, expectedExt,
						)
					}
				}
				idx++
			}
		}
	}

	if shard.IsCompressed(df.inputExt) {
		tlog.Logf("%s: computed output shard count (%d) vs resulting compressed (%d)\n",
			df.job(), df.outputShardCnt, realOutputShardCnt)
	}
	if df.alg.Kind == dsort.Shuffle {
		if inversions == 0 {
			df.m.t.Fatalf("%s: shuffle sorting did not create any inversions", df.job())
		}
	}
}

func canonicalName(recordName string) string {
	return strings.TrimSuffix(recordName, cos.Ext(recordName))
}

func (df *dsortFramework) checkReactionResult(reaction string, expectedProblemsCnt int) {
	tlog.Logf("%s: checking metrics and \"reaction\"\n", df.job())
	all, err := api.MetricsDsort(df.baseParams, df.managerUUID)
	tassert.CheckFatal(df.m.t, err)
	if len(all) != df.m.originalTargetCount {
		df.m.t.Errorf("%s: number of metrics %d is not same as number of targets %d", df.job(),
			len(all), df.m.originalTargetCount)
	}

	switch reaction {
	case cmn.IgnoreReaction:
		for target, jmetrics := range all {
			metrics := jmetrics.Metrics
			if len(metrics.Warnings) != 0 {
				df.m.t.Errorf("%s: target %q has %s warnings: %s", df.job(), target, apc.ActDsort, metrics.Warnings)
			}
			if len(metrics.Errors) != 0 {
				df.m.t.Errorf("%s: target %q has %s errors: %s", df.job(), target, apc.ActDsort, metrics.Errors)
			}
		}
	case cmn.WarnReaction:
		totalWarnings := 0
		for target, jmetrics := range all {
			metrics := jmetrics.Metrics
			totalWarnings += len(metrics.Warnings)

			if len(metrics.Errors) != 0 {
				df.m.t.Errorf("%s: target %q has %s errors: %s", df.job(), target, apc.ActDsort, metrics.Errors)
			}
		}

		if totalWarnings != expectedProblemsCnt {
			df.m.t.Errorf("%s: number of total warnings %d is different than number of deleted shards: %d", df.job(), totalWarnings, expectedProblemsCnt)
		}
	case cmn.AbortReaction:
		totalErrors := 0
		for target, jmetrics := range all {
			metrics := jmetrics.Metrics
			if !metrics.Aborted.Load() {
				df.m.t.Errorf("%s: %s was not aborted by target: %s", df.job(), apc.ActDsort, target)
			}
			totalErrors += len(metrics.Errors)
		}

		if totalErrors == 0 {
			df.m.t.Errorf("%s: expected errors on abort, got nothing", df.job())
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

func (df *dsortFramework) checkMetrics(expectAbort bool) map[string]*dsort.JobInfo {
	tlog.Logf("%s: checking metrics\n", df.job())
	all, err := api.MetricsDsort(df.baseParams, df.managerUUID)
	tassert.CheckFatal(df.m.t, err)
	if len(all) != df.m.originalTargetCount {
		df.m.t.Errorf("%s: number of metrics %d is not same as number of targets %d",
			df.job(), len(all), df.m.originalTargetCount)
	}
	for target, jmetrics := range all {
		m := jmetrics.Metrics
		if expectAbort && !m.Aborted.Load() {
			df.m.t.Errorf("%s: %s was not aborted by target: %s", df.job(), apc.ActDsort, target)
		} else if !expectAbort && m.Aborted.Load() {
			df.m.t.Errorf("%s: %s was aborted by target: %s", df.job(), apc.ActDsort, target)
		}
	}
	return all
}

// helper for dispatching i-th dsort job
func dispatchDsortJob(m *ioContext, dsorterType string, i int) {
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

	tlog.Logln(startingDS)
	df.start()

	_, err := tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
	tassert.CheckFatal(m.t, err)
	tlog.Logf("%s: finished\n", df.job())

	df.checkMetrics(false /* expectAbort */)
	df.checkOutputShards(5)
}

func waitForDsortPhase(t *testing.T, proxyURL, managerUUID, phaseName string, callback func()) {
	tlog.Logf("waiting for %s phase...\n", phaseName)
	baseParams := tools.BaseAPIParams(proxyURL)
	for {
		all, err := api.MetricsDsort(baseParams, managerUUID)
		if err != nil { // in case of error call callback anyway
			t.Error(err)
			callback()
			break
		}

		phase := true
		for _, jmetrics := range all {
			metrics := jmetrics.Metrics
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

//
// tests
//

func TestDsort(t *testing.T) {
	for _, ext := range []string{archive.ExtTar, archive.ExtTarLz4, archive.ExtZip} {
		for _, lr := range []string{"list", "range"} {
			t.Run(ext+"/"+lr, func(t *testing.T) {
				testDsort(t, ext, lr)
			})
		}
	}
}

func testDsort(t *testing.T, ext, lr string) {
	runDsortTest(
		// Include empty ("") type - in this case type must be selected automatically.
		t, dsortTestSpec{p: true, types: append(dsorterTypes, "")},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				df = &dsortFramework{
					m:             m,
					inputExt:      ext,
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
			m.initAndSaveState(true /*cleanup*/)
			m.expectTargets(1)

			// Create ais bucket
			tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

			df.init()
			df.createInputShards()

			if lr == "list" {
				// iterate list
				df.inputTempl.ObjNames = df.inputShards
				df.inputTempl.Template = ""
				df.missingShards = cmn.AbortReaction // (when shards are explicitly enumerated...)
			}

			tlog.Logln(startingDS)
			df.start()

			_, err := tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logf("%s: finished\n", df.job())

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(5)
		},
	)
}

func TestDsortNonExistingBuckets(t *testing.T) {
	runDsortTest(
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
			m.initAndSaveState(true /*cleanup*/)
			m.expectTargets(3)

			df.init()

			// Create ais:// output
			tools.CreateBucket(t, m.proxyURL, df.outputBck, nil, true /*cleanup*/)

			tlog.Logln(startingDS)
			spec := df.gen()
			tlog.Logf("dsort %s(-) => %s\n", m.bck, df.outputBck)
			if _, err := api.StartDsort(df.baseParams, &spec); err == nil {
				t.Error("expected dsort to fail when input bucket doesn't exist")
			}

			// Now destroy output bucket and create input bucket
			tools.DestroyBucket(t, m.proxyURL, df.outputBck)
			tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

			tlog.Logf("dsort %s => %s(-)\n", m.bck, df.outputBck)
			if _, err := api.StartDsort(df.baseParams, &spec); err != nil {
				t.Errorf("expected dsort to create output bucket on the fly, got: %v", err)
			}
		},
	)
}

func TestDsortEmptyBucket(t *testing.T) {
	runDsortTest(
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
			m.initAndSaveState(true /*cleanup*/)
			m.expectTargets(3)
			tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

			df.init()

			tlog.Logln(startingDS)
			df.start()

			_, err := tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logf("%s: finished\n", df.job())

			df.checkMetrics(reaction == cmn.AbortReaction /*expectAbort*/)
			df.checkReactionResult(reaction, df.shardCnt)
		},
	)
}

func TestDsortOutputBucket(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	runDsortTest(
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

			m.initAndSaveState(true /*cleanup*/)
			m.expectTargets(3)
			// Create ais buckets
			tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

			// Create local output bucket
			tools.CreateBucket(t, m.proxyURL, df.outputBck, nil, true /*cleanup*/)

			df.init()
			df.createInputShards()

			tlog.Logf("starting dsort: %d/%d\n", df.shardCnt, df.filesPerShard)
			df.start()

			_, err := tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logf("%s: finished\n", df.job())

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(5)
		},
	)
}

// TestDsortParallel runs multiple dSorts in parallel
func TestDsortParallel(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	runDsortTest(
		t, dsortTestSpec{p: false, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				dSortsCount = 5
			)

			m.initAndSaveState(true /*cleanup*/)
			m.expectTargets(3)
			tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

			wg := &sync.WaitGroup{}
			for i := 0; i < dSortsCount; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					dispatchDsortJob(m, dsorterType, i)
				}(i)
			}
			wg.Wait()
		},
	)
}

// TestDsortChain runs multiple dSorts one after another
func TestDsortChain(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	runDsortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			var (
				m = &ioContext{
					t: t,
				}
				dSortsCount = 5
			)

			m.initAndSaveState(true /*cleanup*/)
			m.expectTargets(3)
			tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

			for i := 0; i < dSortsCount; i++ {
				dispatchDsortJob(m, dsorterType, i)
			}
		},
	)
}

func TestDsortShuffle(t *testing.T) {
	runDsortTest(
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

			m.initAndSaveState(true /*cleanup*/)
			m.expectTargets(3)
			tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

			df.init()
			df.createInputShards()

			tlog.Logf("starting dsort: %d/%d\n", df.shardCnt, df.filesPerShard)
			df.start()

			_, err := tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logf("%s: finished\n", df.job())

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(5)
		},
	)
}

func TestDsortDisk(t *testing.T) {
	runDsortTest(
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

			m.initAndSaveState(true /*cleanup*/)
			m.expectTargets(3)
			tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

			df.init()
			df.createInputShards()
			tlog.Logf("starting dsort with spilling to disk... (%d/%d)\n", df.shardCnt, df.filesPerShard)
			df.start()

			_, err := tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logf("%s: finished\n", df.job())

			all := df.checkMetrics(false /* expectAbort */)
			for target, jmetrics := range all {
				metrics := jmetrics.Metrics
				if metrics.Extraction.ExtractedToDiskCnt == 0 && metrics.Extraction.ExtractedCnt > 0 {
					t.Errorf("target %s did not extract any files do disk", target)
				}
			}

			df.checkOutputShards(0)
		},
	)
}

func TestDsortCompressionDisk(t *testing.T) {
	for _, ext := range []string{archive.ExtTgz, archive.ExtTarLz4, archive.ExtZip} {
		t.Run(ext, func(t *testing.T) {
			runDsortTest(
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
							inputExt:      ext,
							maxMemUsage:   "1KB",
						}
					)

					m.initAndSaveState(true /*cleanup*/)
					m.expectTargets(3)
					tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

					df.init()
					df.createInputShards()

					tlog.Logf("starting dsort: %d/%d, %s\n",
						df.shardCnt, df.filesPerShard, df.inputExt)
					df.start()

					_, err := tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
					tassert.CheckFatal(t, err)
					tlog.Logf("%s: finished\n", df.job())

					df.checkMetrics(false /* expectAbort */)
					df.checkOutputShards(5)
				},
			)
		})
	}
}

func TestDsortMemDisk(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	var (
		m = &ioContext{
			t: t,
		}
		df = &dsortFramework{
			m:             m,
			dsorterType:   dsort.GeneralType,
			shardCnt:      500,
			fileSz:        cos.MiB,
			filesPerShard: 5,
		}
		mem sys.MemStat
	)

	m.initAndSaveState(true /*cleanup*/)
	m.expectTargets(3)
	tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

	df.init()
	df.createInputShards()

	// Try to free all memory to get estimated actual used memory size
	rdebug.FreeOSMemory()

	// Get current memory
	err := mem.Get()
	tassert.CheckFatal(t, err)
	df.maxMemUsage = cos.ToSizeIEC(int64(mem.ActualUsed+500*cos.MiB), 2)

	tlog.Logf("starting dsort with memory and disk (max mem usage: %s)... (%d/%d)\n", df.maxMemUsage,
		df.shardCnt, df.filesPerShard)
	df.start()

	_, err = tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
	tassert.CheckFatal(t, err)
	tlog.Logf("%s: finished\n", df.job())

	all := df.checkMetrics(false /* expectAbort */)
	var (
		extractedToDisk int64
		extractedTotal  int64
	)
	for _, jmetrics := range all {
		metrics := jmetrics.Metrics
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

func TestDsortMinMemCompression(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})
	for _, ext := range []string{archive.ExtTarGz, archive.ExtTarLz4, archive.ExtZip} {
		for _, maxMem := range []string{"10%", "1%"} {
			t.Run(ext+"/mem="+maxMem, func(t *testing.T) {
				minMemCompression(t, ext, maxMem)
			})
		}
	}
}

func minMemCompression(t *testing.T, ext, maxMem string) {
	var (
		m = &ioContext{
			t: t,
		}
		df = &dsortFramework{
			m:             m,
			dsorterType:   dsort.GeneralType,
			shardCnt:      500,
			fileSz:        cos.MiB,
			filesPerShard: 5,
			inputExt:      ext,
			maxMemUsage:   maxMem,
		}
		mem sys.MemStat
	)

	m.initAndSaveState(true /*cleanup*/)
	m.expectTargets(3)
	tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

	df.init()
	df.createInputShards()

	// Try to free all memory to get estimated actual used memory size
	rdebug.FreeOSMemory()

	// Get current memory
	err := mem.Get()
	tassert.CheckFatal(t, err)
	df.maxMemUsage = cos.ToSizeIEC(int64(mem.ActualUsed+300*cos.MiB), 2)

	tlog.Logf("starting dsort with memory, disk, and compression (max mem usage: %s) ... %d/%d, %s\n",
		df.maxMemUsage, df.shardCnt, df.filesPerShard, df.inputExt)
	df.start()

	_, err = tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
	tassert.CheckFatal(t, err)
	tlog.Logf("%s: finished\n", df.job())

	all := df.checkMetrics(false /*expectAbort*/)
	var (
		extractedToDisk int64
		extractedTotal  int64
	)
	for _, jmetrics := range all {
		metrics := jmetrics.Metrics
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

func TestDsortZipLz4(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	for _, ext := range []string{archive.ExtZip, archive.ExtTarLz4} {
		t.Run(ext, func(t *testing.T) {
			runDsortTest(
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
							inputExt:      ext,
							maxMemUsage:   "99%",
						}
					)

					m.initAndSaveState(true /*cleanup*/)
					m.expectTargets(3)
					tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

					df.init()
					df.createInputShards()

					tlog.Logf("starting dsort: %d/%d, %s\n", df.shardCnt, df.filesPerShard, df.inputExt)
					df.start()

					_, err = tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
					tassert.CheckFatal(t, err)
					tlog.Logf("%s: finished\n", df.job())

					df.checkMetrics(false /* expectAbort */)
					df.checkOutputShards(5)
				},
			)
		})
	}
}

func TestDsortMaxMemCompression(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})
	for _, ext := range []string{archive.ExtTgz, archive.ExtTarLz4, archive.ExtZip} {
		t.Run(ext, func(t *testing.T) {
			runDsortTest(
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
							inputExt:      ext,
							maxMemUsage:   "99%",
						}
					)

					m.initAndSaveState(true /*cleanup*/)
					m.expectTargets(3)
					tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

					df.init()
					df.createInputShards()

					tlog.Logf("starting dsort: %d/%d, %s\n", df.shardCnt, df.filesPerShard, df.inputExt)
					df.start()

					_, err = tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
					tassert.CheckFatal(t, err)
					tlog.Logf("%s: finished\n", df.job())

					df.checkMetrics(false /* expectAbort */)
					df.checkOutputShards(5)
				},
			)
		})
	}
}

func TestDsortContent(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	runDsortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			cases := []struct {
				extension      string
				contentKeyType string
				missingKeys    bool
			}{
				{".loss", shard.ContentKeyInt, false},
				{".cls", shard.ContentKeyFloat, false},
				{".smth", shard.ContentKeyString, false},

				{".loss", shard.ContentKeyInt, true},
				{".cls", shard.ContentKeyFloat, true},
				{".smth", shard.ContentKeyString, true},
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

					m.initAndSaveState(true /*cleanup*/)
					m.expectTargets(3)
					tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

					df.init()
					df.createInputShards()

					tlog.Logf("starting dsort: %d/%d\n", df.shardCnt, df.filesPerShard)
					df.start()

					aborted, err := tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
					tassert.CheckFatal(t, err)
					if entry.missingKeys && !aborted {
						t.Errorf("%s was not aborted", apc.ActDsort)
					}

					tlog.Logf("%s: checking metrics\n", df.job())
					all, err := api.MetricsDsort(df.baseParams, df.managerUUID)
					tassert.CheckFatal(t, err)
					if len(all) != m.originalTargetCount {
						t.Errorf("number of metrics %d is not same as the number of targets %d",
							len(all), m.originalTargetCount)
					}

					for target, jmetrics := range all {
						metrics := jmetrics.Metrics
						if entry.missingKeys && !metrics.Aborted.Load() {
							t.Errorf("%s was not aborted by target: %s", target, apc.ActDsort)
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

func TestDsortAbort(t *testing.T) {
	runDsortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes},
		func(dsorterType string, t *testing.T) {
			for _, asXaction := range []bool{false, true} {
				test := dsorterType + "/" + fmt.Sprintf("as-xaction=%t", asXaction)
				t.Run(test, func(t *testing.T) {
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

					m.initAndSaveState(false /*cleanup*/)
					m.expectTargets(3)
					tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

					df.init()
					df.createInputShards()

					tlog.Logf("starting dsort: %d/%d\n", df.shardCnt, df.filesPerShard)
					df.start()

					if asXaction {
						tlog.Logf("aborting dsort[%s] via api.AbortXaction\n", df.managerUUID)
						err = api.AbortXaction(df.baseParams, xact.ArgsMsg{ID: df.managerUUID})
					} else {
						tlog.Logf("aborting dsort[%s] via api.AbortDsort\n", df.managerUUID)
						err = api.AbortDsort(df.baseParams, df.managerUUID)
					}
					tassert.CheckFatal(t, err)

					_, err = tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
					tassert.CheckFatal(t, err)

					df.checkMetrics(true /* expectAbort */)
				})
			}
		},
	)
}

func TestDsortAbortDuringPhases(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	runDsortTest(
		t, dsortTestSpec{p: true, types: dsorterTypes, phases: dsortPhases},
		func(dsorterType, phase string, t *testing.T) {
			for _, asXaction := range []bool{false, true} {
				test := dsorterType + "/" + fmt.Sprintf("as-xaction=%t", asXaction)
				t.Run(test, func(t *testing.T) {
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

					if phase == dsort.SortingPhase && asXaction {
						t.Skipf("skipping %s", t.Name()) // TODO -- FIXME: remove
					}

					m.initAndSaveState(true /*cleanup*/)
					m.expectTargets(3)

					tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

					df.init()
					df.createInputShards()

					tlog.Logf("starting dsort (abort on: %s)...\n", phase)
					df.start()

					waitForDsortPhase(t, m.proxyURL, df.managerUUID, phase, func() {
						var err error
						if asXaction {
							tlog.Logf("aborting dsort[%s] via api.AbortXaction\n", df.managerUUID)
							err = api.AbortXaction(df.baseParams, xact.ArgsMsg{ID: df.managerUUID})
						} else {
							tlog.Logf("aborting dsort[%s] via api.AbortDsort\n", df.managerUUID)
							err = api.AbortDsort(df.baseParams, df.managerUUID)
						}
						tassert.CheckFatal(t, err)
					})

					_, err := tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
					tassert.CheckFatal(t, err)

					df.checkMetrics(true /* expectAbort */)
				})
			}
		},
	)
}

func TestDsortKillTargetDuringPhases(t *testing.T) {
	t.Skip("test is flaky, run it only when necessary")

	runDsortTest(
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

			m.initAndSaveState(true /*cleanup*/)
			m.expectTargets(3)

			df.init()

			tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

			df.createInputShards()

			tlog.Logf("starting dsort (abort on: %s)...\n", phase)
			df.start()

			waitForDsortPhase(t, m.proxyURL, df.managerUUID, phase, func() {
				// It may require calling AbortXaction(rebalance) &
				// WaitForRebalAndResil() before unregistering
				target = m.startMaintenanceNoRebalance()
			})

			aborted, err := tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckError(t, err)
			if !aborted {
				t.Errorf("%s was not aborted", apc.ActDsort)
			}

			tlog.Logf("%s: checking metrics\n", df.job())
			all, err := api.MetricsDsort(df.baseParams, df.managerUUID)
			tassert.CheckError(t, err)
			if len(all) == m.originalTargetCount {
				t.Errorf("number of metrics %d is same as number of original targets %d",
					len(all), m.originalTargetCount)
			}

			for target, jmetrics := range all {
				metrics := jmetrics.Metrics
				if !metrics.Aborted.Load() {
					t.Errorf("%s was not aborted by target: %s", apc.ActDsort, target)
				}
			}

			rebID := m.stopMaintenance(target)
			tools.WaitForRebalanceByID(t, df.baseParams, rebID)
		},
	)
}

func TestDsortManipulateMountpathDuringPhases(t *testing.T) {
	t.Skipf("skipping %s", t.Name())

	runDsortTest(
		t, dsortTestSpec{p: false, types: dsorterTypes, phases: dsortPhases},
		func(dsorterType, phase string, t *testing.T) {
			for _, adding := range []bool{false, true} {
				t.Run(strconv.FormatBool(adding), func(t *testing.T) {
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

					m.initAndSaveState(true /*cleanup*/)
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

					tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

					df.createInputShards()

					tlog.Logf("starting dsort (abort on: %s)...\n", phase)
					df.start()

					waitForDsortPhase(t, m.proxyURL, df.managerUUID, phase, func() {
						for target, mpath := range mountpaths {
							if adding {
								tlog.Logf("adding new mountpath %q to %s...\n", mpath, target.ID())
								err := api.AttachMountpath(df.baseParams, target, mpath, true /*force*/)
								tassert.CheckFatal(t, err)
							} else {
								tlog.Logf("removing mountpath %q from %s...\n", mpath, target.ID())
								err := api.DetachMountpath(df.baseParams, target,
									mpath, false /*dont-resil*/)
								tassert.CheckFatal(t, err)
							}
						}
					})

					_, err := tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
					tassert.CheckError(t, err)

					df.checkMetrics(true /*expectAbort*/)
				})
			}
		},
	)
}

func TestDsortAddTarget(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	runDsortTest(
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

			m.initAndSaveState(true /*cleanup*/)
			m.expectTargets(3)

			df.init()

			target := m.startMaintenanceNoRebalance()

			tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

			df.createInputShards()

			tlog.Logf("starting dsort: %d/%d\n", df.shardCnt, df.filesPerShard)
			df.start()

			defer tools.WaitForRebalAndResil(t, df.baseParams)

			waitForDsortPhase(t, m.proxyURL, df.managerUUID, dsort.ExtractionPhase, func() {
				m.stopMaintenance(target)
			})

			aborted, err := tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			if !aborted {
				t.Errorf("%s was not aborted", apc.ActDsort)
			}

			tlog.Logf("%s: checking metrics\n", df.job())
			allMetrics, err := api.MetricsDsort(df.baseParams, df.managerUUID)
			tassert.CheckFatal(t, err)
			if len(allMetrics) != m.originalTargetCount-1 {
				t.Errorf("number of metrics %d is different than number of targets when %s started %d",
					len(allMetrics), apc.ActDsort, m.originalTargetCount-1)
			}
		},
	)
}

func TestDsortMetricsAfterFinish(t *testing.T) {
	runDsortTest(
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

			m.initAndSaveState(true /*cleanup*/)
			m.expectTargets(3)

			tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

			df.init()
			df.createInputShards()

			tlog.Logf("starting dsort: %d/%d\n", df.shardCnt, df.filesPerShard)
			df.start()

			_, err := tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logf("%s: finished\n", df.job())

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(0)

			tlog.Logln("checking if metrics are still accessible after some time..")
			time.Sleep(2 * time.Second)

			// Check if metrics can be fetched after some time
			df.checkMetrics(false /* expectAbort */)
		},
	)
}

func TestDsortSelfAbort(t *testing.T) {
	runDsortTest(
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

			m.initAndSaveState(true /*cleanup*/)
			m.expectTargets(3)

			tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

			df.init()

			tlog.Logf("starting dsort: %d/%d\n", df.shardCnt, df.filesPerShard)
			df.start()

			_, err := tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logf("%s: finished\n", df.job())

			// Wait a while for all targets to abort
			time.Sleep(2 * time.Second)

			df.checkMetrics(true /* expectAbort */)
		},
	)
}

func TestDsortOnOOM(t *testing.T) {
	t.Skip("test can take more than couple minutes, run it only when necessary")

	runDsortTest(
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
			// that if dsort doesn't prevent it, it will happen. Notice that maxMemUsage
			// is 80% so dsort should never go above this number in memory usage.
			df.shardCnt = int(float64(mem.ActualFree/uint64(df.fileSz)/uint64(df.filesPerShard)) * 1.4)

			m.initAndSaveState(true /*cleanup*/)
			m.expectTargets(3)

			tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

			df.init()
			df.createInputShards()

			tlog.Logf("starting dsort: %d/%d\n", df.shardCnt, df.filesPerShard)
			df.start()

			_, err = tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logf("%s: finished\n", df.job())

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(5)
		},
	)
}

func TestDsortMissingShards(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})
	for _, ext := range []string{archive.ExtTar, archive.ExtTarLz4} {
		t.Run(ext, func(t *testing.T) {
			runDsortTest(
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
							inputExt:       ext,
						}
					)

					m.initAndSaveState(true /*cleanup*/)
					m.expectTargets(3)

					tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

					switch scope {
					case scopeConfig:
						defer tools.SetClusterConfig(t,
							cos.StrKVs{"distributed_sort.missing_shards": cmn.IgnoreReaction})
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

					tlog.Logf("starting dsort: %d/%d, %s\n", df.shardCnt, df.filesPerShard, df.inputExt)
					df.start()

					_, err := tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
					tassert.CheckFatal(t, err)
					tlog.Logf("%s: finished\n", df.job())

					df.checkReactionResult(reaction, df.shardCntToSkip)
				},
			)
		})
	}
}

func TestDsortDuplications(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})
	for _, ext := range []string{archive.ExtTar, archive.ExtTarLz4, archive.ExtTarGz, archive.ExtZip} { // all supported formats
		t.Run(ext, func(t *testing.T) {
			runDsortTest(
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
							inputExt:              ext,
						}
					)
					m.initAndSaveState(false /*cleanup*/)
					m.expectTargets(3)
					tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

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

					tlog.Logf("starting dsort: %d/%d, %s\n", df.shardCnt, df.filesPerShard, df.inputExt)
					df.start()

					_, err := tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
					tassert.CheckFatal(t, err)
					tlog.Logf("%s: finished\n", df.job())

					df.checkReactionResult(reaction, df.recordDuplicationsCnt)
				},
			)
		})
	}
}

func TestDsortOrderFile(t *testing.T) {
	runDsortTest(
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

			m.initAndSaveState(true /*cleanup*/)
			m.expectTargets(3)

			// Set URL for order file (points to the object in cluster).
			df.orderFileURL = fmt.Sprintf(
				"%s/%s/%s/%s/%s?%s=%s",
				proxyURL, apc.Version, apc.Objects, m.bck.Name, orderFileName,
				apc.QparamProvider, apc.AIS,
			)

			df.init()

			tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

			// Create local output bucket
			tools.CreateBucket(t, m.proxyURL, df.outputBck, nil, true /*cleanup*/)

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
				Reader:     readers.NewBytes(buffer.Bytes()),
			}
			_, err = api.PutObject(args)
			tassert.CheckFatal(t, err)

			tlog.Logln(startingDS)
			spec := df.gen()
			managerUUID, err := api.StartDsort(baseParams, &spec)
			tassert.CheckFatal(t, err)

			_, err = tools.WaitForDsortToFinish(m.proxyURL, managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logf("%s: finished\n", df.job())

			allMetrics, err := api.MetricsDsort(baseParams, managerUUID)
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
						t.Errorf("record %q was not part of any shard with format %q but was in shard %q",
							recordName, ekm[recordName], shard.name)
					}
				}
			}
		},
	)
}

func TestDsortOrderJSONFile(t *testing.T) {
	runDsortTest(
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

			m.initAndSaveState(true /*cleanup*/)
			m.expectTargets(3)

			// Set URL for order file (points to the object in cluster).
			df.orderFileURL = fmt.Sprintf(
				"%s/%s/%s/%s/%s?%s=%s",
				proxyURL, apc.Version, apc.Objects, m.bck.Name, orderFileName,
				apc.QparamProvider, apc.AIS,
			)

			df.init()

			tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

			// Create local output bucket
			tools.CreateBucket(t, m.proxyURL, df.outputBck, nil, true /*cleanup*/)

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
				Reader:     readers.NewBytes(jsonBytes),
			}
			_, err = api.PutObject(args)
			tassert.CheckFatal(t, err)

			tlog.Logln(startingDS)
			spec := df.gen()
			managerUUID, err := api.StartDsort(baseParams, &spec)
			tassert.CheckFatal(t, err)

			_, err = tools.WaitForDsortToFinish(m.proxyURL, managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logf("%s: finished\n", df.job())

			allMetrics, err := api.MetricsDsort(baseParams, managerUUID)
			tassert.CheckFatal(t, err)
			if len(allMetrics) != m.originalTargetCount {
				t.Errorf("number of metrics %d is not same as number of targets %d",
					len(allMetrics), m.originalTargetCount)
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
						t.Errorf("record %q was not part of any shard with format %q but was in shard %q",
							recordName, ekm[recordName], shard.name)
					}
				}
			}
		},
	)
}

func TestDsortDryRun(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	runDsortTest(
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

			m.initAndSaveState(true /*cleanup*/)
			m.expectTargets(3)

			tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

			df.init()
			df.createInputShards()

			tlog.Logln(startingDS)
			df.start()

			_, err := tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logf("%s: finished\n", df.job())

			df.checkMetrics(false /* expectAbort */)
		},
	)
}

func TestDsortDryRunDisk(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	runDsortTest(
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

			m.initAndSaveState(true /*cleanup*/)
			m.expectTargets(3)

			tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

			df.init()
			df.createInputShards()

			tlog.Logln(startingDS)
			df.start()

			_, err := tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logf("%s: finished\n", df.job())

			df.checkMetrics(false /* expectAbort */)
		},
	)
}

func TestDsortLongerExt(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	runDsortTest(
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

			m.initAndSaveState(true /*cleanup*/)
			m.expectTargets(3)

			tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

			df.init()
			df.createInputShards()

			tlog.Logln(startingDS)
			df.start()

			_, err := tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logf("%s: finished\n", df.job())

			df.checkMetrics(false /*expectAbort*/)
			df.checkOutputShards(5)
		},
	)
}

func TestDsortAutomaticallyCalculateOutputShards(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	runDsortTest(
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

			m.initAndSaveState(true /*cleanup*/)
			m.expectTargets(3)

			tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

			df.init()
			df.createInputShards()

			tlog.Logln(startingDS)
			df.start()

			_, err := tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logf("%s: finished\n", df.job())

			df.checkMetrics(false /*expectAbort*/)
			df.checkOutputShards(0)
		},
	)
}

func TestDsortWithTarFormats(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	runDsortTest(
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
			m.initAndSaveState(true /*cleanup*/)
			m.expectTargets(1)

			// Create ais bucket
			tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

			df.init()
			df.createInputShards()

			tlog.Logln(startingDS)
			df.start()

			_, err := tools.WaitForDsortToFinish(m.proxyURL, df.managerUUID)
			tassert.CheckFatal(t, err)
			tlog.Logf("%s: finished\n", df.job())

			df.checkMetrics(false /* expectAbort */)
			df.checkOutputShards(5)
		},
	)
}
