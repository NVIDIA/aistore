// Package integration_test.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	cryptorand "crypto/rand"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tetl"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact"

	"github.com/NVIDIA/go-tfdata/tfdata/core"
	onexxh "github.com/OneOfOne/xxhash"
	corev1 "k8s.io/api/core/v1"
)

const (
	tar2tfIn  = "data/small-mnist-3.tar"
	tar2tfOut = "data/small-mnist-3.record"

	tar2tfFiltersIn  = "data/single-png-cls.tar"
	tar2tfFiltersOut = "data/single-png-cls-transformed.tfrecord"
)

type (
	transformFunc  func(r io.Reader) io.Reader
	filesEqualFunc func(f1, f2 string) (bool, error)

	testObjConfig struct {
		transformer string
		comm        string
		inPath      string         // optional
		outPath     string         // optional
		transform   transformFunc  // optional
		filesEqual  filesEqualFunc // optional
		onlyLong    bool           // run only with long tests
	}

	// from-ais/to-ais => [create src, populate src, [run, destroy dst], destroy src]
	// from-remote/to-ais => [populate src, [run, destroy dst], delete src]
	// from-evicted-remote/to-ais => [populate src, [evict src, run, destroy dst], delete src]
	// from-ais/to-remote => [create src, populate src, [run, delete dst], destroy src]
	testBucketConfig struct {
		srcRemote      bool
		evictRemoteSrc bool
		dstRemote      bool
	}

	testCloudObjConfig struct {
		cached   bool
		onlyLong bool
	}
)

func (tc *testObjConfig) Name() string {
	return fmt.Sprintf("%s/%s", tc.transformer, strings.TrimSuffix(tc.comm, "://"))
}

// setupBckFrom creates source bucket and populates it with random objects
func (tbc *testBucketConfig) setupBckFrom(t *testing.T, prefix string, objCnt int) (bckFrom cmn.Bck, tname string) {
	if tbc.srcRemote {
		bckFrom = cliBck
		if tbc.evictRemoteSrc {
			tname += "from-evicted-remote"
		} else {
			tname += "from-remote"
		}

		t.Cleanup(func() {
			srcm := ioContext{t: t, bck: bckFrom, prefix: prefix}
			srcm.del(objCnt)
		})
	} else {
		debug.Assert(!tbc.evictRemoteSrc)
		bckFrom = cmn.Bck{Name: "etlsrc_" + cos.GenUUID(), Provider: apc.AIS}
		tname += "from-ais"
		tools.CreateBucket(t, proxyURL, bckFrom, nil, true /*cleanup*/)
	}

	tlog.Logfln("populating source bucket %s", bckFrom.Cname(prefix))
	var (
		wg    = cos.NewLimitedWaitGroup(sys.MaxParallelism(), 0)
		errCh = make(chan error, objCnt)
	)
	p, err := api.HeadBucket(baseParams, bckFrom, false /* don't add */)
	tassert.CheckFatal(t, err)
	for i := range objCnt {
		objName := fmt.Sprintf("%s/%04d", prefix, i)
		wg.Add(1)
		go func(objName string) {
			defer wg.Done()
			r, err := readers.NewRand(int64(fileSize), p.Cksum.Type)
			tassert.CheckFatal(t, err)
			tools.Put(proxyURL, bckFrom, objName, r, fileSize, 0 /*numChunks*/, errCh)
		}(objName)
	}
	wg.Wait()
	tassert.SelectErr(t, errCh, "put", true)

	if tbc.dstRemote {
		tname += "/to-remote"
	} else {
		tname += "/to-ais"
	}

	return bckFrom, tname
}

// setupBckTo registers cleanup function to destroy destination local/remote bucket when the given test finishes
func (tbc *testBucketConfig) setupBckTo(t *testing.T, prefix string, objCnt int) (bckTo cmn.Bck) {
	if tbc.dstRemote {
		bckTo = cliBck
		dstm := ioContext{t: t, bck: bckTo, prefix: prefix}
		dstm.del(objCnt)
		t.Cleanup(func() { dstm.del(objCnt) })
	} else {
		bckTo = cmn.Bck{Name: "etldst_" + cos.GenUUID(), Provider: apc.AIS}
		// NOTE: ais will create dst bucket on the fly
		t.Cleanup(func() {
			tlog.Logfln("destroy destination bucket %s", bckTo.Cname(prefix))
			tools.DestroyBucket(t, proxyURL, bckTo)
		})
	}
	return bckTo
}

// TODO: This should be a part of go-tfdata.
// This function is necessary, as the same TFRecords can be different byte-wise.
// This is caused by the fact that order of TFExamples is can de different,
// as well as ordering of elements of a single TFExample can be different.
func tfDataEqual(n1, n2 string) (bool, error) {
	examples1, err := readExamples(n1)
	if err != nil {
		return false, err
	}
	examples2, err := readExamples(n2)
	if err != nil {
		return false, err
	}

	if len(examples1) != len(examples2) {
		return false, nil
	}
	return tfRecordsEqual(examples1, examples2)
}

func tfRecordsEqual(examples1, examples2 []*core.TFExample) (bool, error) {
	sort.SliceStable(examples1, func(i, j int) bool {
		return examples1[i].GetFeature("__key__").String() < examples1[j].GetFeature("__key__").String()
	})
	sort.SliceStable(examples2, func(i, j int) bool {
		return examples2[i].GetFeature("__key__").String() < examples2[j].GetFeature("__key__").String()
	})

	for i := range examples1 {
		if !reflect.DeepEqual(examples1[i].ProtoReflect(), examples2[i].ProtoReflect()) {
			return false, nil
		}
	}
	return true, nil
}

func readExamples(fileName string) (examples []*core.TFExample, err error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return core.NewTFRecordReader(f).ReadAllExamples()
}

func testETLObject(t *testing.T, etlName string, args any, inPath, outPath string, fTransform transformFunc, fEq filesEqualFunc, pipeline ...string) {
	var (
		inputFilePath          string
		expectedOutputFilePath string

		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)

		bck            = cmn.Bck{Provider: apc.AIS, Name: "etl-test"}
		objName        = fmt.Sprintf("%s-%s-object", etlName, trand.String(5))
		outputFileName = filepath.Join(t.TempDir(), objName+".out")
	)

	buf := make([]byte, 256)
	_, err := cryptorand.Read(buf)
	tassert.CheckFatal(t, err)
	r := bytes.NewReader(buf)

	if inPath != "" {
		inputFilePath = inPath
	} else {
		inputFilePath = tools.CreateFileFromReader(t, "object.in", r)
	}
	if outPath != "" {
		expectedOutputFilePath = outPath
	} else {
		_, err := r.Seek(0, io.SeekStart)
		tassert.CheckFatal(t, err)

		r := fTransform(r)
		expectedOutputFilePath = tools.CreateFileFromReader(t, "object.out", r)
	}

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	tlog.Logln("PUT object")
	reader, err := readers.NewExistingFile(inputFilePath, cos.ChecksumNone)
	tassert.CheckFatal(t, err)
	tools.PutObject(t, bck, objName, reader, 0 /*size*/)

	fho, err := cos.CreateFile(outputFileName)
	tassert.CheckFatal(t, err)
	defer fho.Close()

	etl := &api.ETL{ETLName: etlName, TransformArgs: args}
	for _, nextETL := range pipeline {
		etl = etl.Chain(&api.ETL{ETLName: nextETL})
	}

	oah, err := api.ETLObject(baseParams, etl, bck, objName, fho)
	tassert.CheckFatal(t, err)

	stat, _ := fho.Stat()
	tassert.Fatalf(t, stat.Size() == oah.Size(), "expected %d bytes, got %d", oah.Size(), stat.Size())

	tlog.Logln("Compare output")
	same, err := fEq(outputFileName, expectedOutputFilePath)
	tassert.CheckError(t, err)
	tassert.Errorf(t, same, "file contents after transformation differ")
}

func testETLObjectCloud(t *testing.T, bck cmn.Bck, etlName, args string, onlyLong, cached bool) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		prefix     = "prefix-" + cos.GenTie()
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{Long: onlyLong})

	// TODO: PUT and then transform many objects

	objName := fmt.Sprintf("%s/%s-%s-object", prefix, etlName, trand.String(5))
	tlog.Logln("PUT object")
	reader, err := readers.NewRand(cos.KiB, cos.ChecksumNone)
	tassert.CheckFatal(t, err)

	_, err = api.PutObject(&api.PutArgs{
		BaseParams: baseParams,
		Bck:        bck,
		ObjName:    objName,
		Reader:     reader,
	})
	tassert.CheckFatal(t, err)

	if !cached {
		tlog.Logfln("Evicting object %s", bck.Cname(objName))
		err := api.EvictObject(baseParams, bck, objName)
		tassert.CheckFatal(t, err)
	}

	defer func() {
		// Could bucket is not destroyed, remove created object instead.
		err := api.DeleteObject(baseParams, bck, objName)
		tassert.CheckError(t, err)
	}()

	bf := bytes.NewBuffer(nil)
	tlog.Logfln("Use ETL[%s] to read transformed object", etlName)
	etl := &api.ETL{ETLName: etlName, TransformArgs: args}
	_, err = api.ETLObject(baseParams, etl, bck, objName, bf)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, bf.Len() == cos.KiB, "Expected %d bytes, got %d", cos.KiB, bf.Len())
}

func testETLAllErrors(t *testing.T, err error, expectedErrSubstrs ...string) {
	tassert.Fatal(t, err != nil, "No error occurred during test")
	for _, e := range expectedErrSubstrs {
		tassert.Fatalf(t, strings.Contains(err.Error(), e), "Error mismatch: expect [%q] appears in %q", e, err.Error())
	}
}

func testETLAnyErrors(t *testing.T, err error, expectedErrSubstrs ...string) {
	tassert.Fatal(t, err != nil, "No error occurred during test")
	for _, e := range expectedErrSubstrs {
		if strings.Contains(err.Error(), e) {
			return
		}
	}
	tassert.Fatalf(t, false, "Error mismatch: expect at least one of %v appears in %q", expectedErrSubstrs, err.Error())
}

// NOTE: BytesCount references number of bytes *before* the transformation.
func checkETLStats(t *testing.T, xid string, expectedObjCnt int, expectedBytesCnt uint64, skipByteStats bool) {
	snaps, err := api.QueryXactionSnaps(baseParams, &xact.ArgsMsg{ID: xid})
	tassert.CheckFatal(t, err)

	localObjs, outObjs, inObjs := snaps.ObjCounts(xid)

	if localObjs+outObjs != int64(expectedObjCnt) {
		tlog.Logfln("Warning: expected %d objects, got %d (where sent %d, received %d)", expectedObjCnt, localObjs, outObjs, inObjs)
	}
	if outObjs != inObjs {
		tlog.Logfln("Warning: (sent objects) %d != %d (received objects)", outObjs, inObjs)
	} else {
		tlog.Logfln("Num sent/received objects: %d, local objects: %d", outObjs, localObjs)
	}

	if skipByteStats {
		return // don't know the size
	}
	bytes, outBytes, inBytes := snaps.ByteCounts(xid)

	// TODO -- FIXME: validate transformed bytes as well, make sure `expectedBytesCnt` is correct

	tlog.Logfln("Byte counts: expected %d, got (original %d, sent %d, received %d)", expectedBytesCnt,
		bytes, outBytes, inBytes)
}

func TestETLObject(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	noopTransform := func(r io.Reader) io.Reader { return r }
	tests := []testObjConfig{
		{transformer: tetl.Echo, comm: etl.Hpull, transform: noopTransform, filesEqual: tools.FilesEqual, onlyLong: true},
		{transformer: tetl.Echo, comm: etl.Hpush, transform: noopTransform, filesEqual: tools.FilesEqual, onlyLong: true},
		{tetl.Tar2TF, etl.Hpull, tar2tfIn, tar2tfOut, nil, tfDataEqual, true},
		{tetl.Tar2TF, etl.Hpush, tar2tfIn, tar2tfOut, nil, tfDataEqual, true},
		{tetl.Tar2tfFilters, etl.Hpull, tar2tfFiltersIn, tar2tfFiltersOut, nil, tfDataEqual, false},
		{tetl.Tar2tfFilters, etl.Hpush, tar2tfFiltersIn, tar2tfFiltersOut, nil, tfDataEqual, false},
	}

	for _, test := range tests {
		t.Run(test.Name(), func(t *testing.T) {
			tools.CheckSkip(t, &tools.SkipTestArgs{Long: test.onlyLong})

			msg := tetl.InitSpec(t, baseParams, test.transformer, test.comm, etl.ArgTypeDefault)
			t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, msg.Name()) })

			testETLObject(t, msg.Name(), "", test.inPath, test.outPath, test.transform, test.filesEqual)
		})
	}
}

func TestETLObjectCloud(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Bck: cliBck, RequiredDeployment: tools.ClusterTypeK8s, RemoteBck: true})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	tcs := map[string][]*testCloudObjConfig{
		etl.Hpull: {
			{cached: true, onlyLong: false},
			{cached: false, onlyLong: false},
		},
		etl.Hpush: {
			{cached: true, onlyLong: false},
			{cached: false, onlyLong: false},
		},
	}

	for comm, configs := range tcs {
		t.Run(comm, func(t *testing.T) {
			// TODO: currently, Echo transformation only - add other transforms
			msg := tetl.InitSpec(t, baseParams, tetl.Echo, comm, etl.ArgTypeDefault)
			t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, msg.Name()) })

			for _, conf := range configs {
				t.Run(fmt.Sprintf("cached=%t", conf.cached), func(t *testing.T) {
					testETLObjectCloud(t, cliBck, msg.Name(), "", conf.onlyLong, conf.cached)
				})
			}
		})
	}
}

// TODO: initial impl - revise and add many more tests
func TestETLInline(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)

		bck = cmn.Bck{Provider: apc.AIS, Name: "etl-test"}

		tests = []testObjConfig{
			{transformer: tetl.MD5, comm: etl.Hpush},
			{transformer: tetl.MD5, comm: etl.Hpull},
		}
	)

	for _, test := range tests {
		t.Run(test.Name(), func(t *testing.T) {
			tools.CheckSkip(t, &tools.SkipTestArgs{Long: test.onlyLong})

			msg := tetl.InitSpec(t, baseParams, test.transformer, test.comm, etl.ArgTypeDefault)
			t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, msg.Name()) })

			tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

			tlog.Logln("PUT object")
			objNames, _, err := tools.PutRandObjs(tools.PutObjectsArgs{
				ProxyURL: proxyURL,
				Bck:      bck,
				ObjCnt:   1,
				ObjSize:  cos.MiB,
			})
			tassert.CheckFatal(t, err)
			objName := objNames[0]

			tlog.Logln("GET transformed object")
			outObject := bytes.NewBuffer(nil)
			_, err = api.GetObject(baseParams, bck, objName, &api.GetArgs{
				Writer: outObject,
				Query:  url.Values{apc.QparamETLName: {msg.Name()}},
			})
			tassert.CheckFatal(t, err)

			matchesMD5 := regexp.MustCompile("^[a-fA-F0-9]{32}$").MatchReader(outObject)
			tassert.Fatalf(t, matchesMD5, "expected transformed object to be md5 checksum")
		})
	}
}

func TestETLInlineMD5SingleObj(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)

		bck         = cmn.Bck{Provider: apc.AIS, Name: "etl-test"}
		transformer = tetl.MD5
		comm        = etl.Hpush
	)
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	msg := tetl.InitSpec(t, baseParams, transformer, comm, etl.ArgTypeDefault)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, msg.Name()) })

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	tlog.Logln("PUT object")
	objName := trand.String(10)
	reader, err := readers.NewRand(cos.MiB, cos.ChecksumMD5)
	tassert.CheckFatal(t, err)

	_, err = api.PutObject(&api.PutArgs{
		BaseParams: baseParams,
		Bck:        bck,
		ObjName:    objName,
		Reader:     reader,
	})
	tassert.CheckFatal(t, err)

	tlog.Logln("GET transformed object")
	outObject := memsys.PageMM().NewSGL(0)
	defer outObject.Free()

	_, err = api.GetObject(baseParams, bck, objName, &api.GetArgs{
		Writer: outObject,
		Query:  url.Values{apc.QparamETLName: {msg.Name()}},
	})
	tassert.CheckFatal(t, err)

	exp, got := reader.Cksum().Val(), string(outObject.Bytes())
	tassert.Errorf(t, exp == got, "expected transformed object to be md5 checksum %s, got %s", exp,
		got[:min(len(got), 16)])
}

func TestETLInlineObjWithArgs(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	var (
		proxyURL    = tools.RandomProxyURL(t)
		baseParams  = tools.BaseAPIParams(proxyURL)
		transformer = tetl.HashWithArgs

		tests = []struct {
			name        string
			commType    string
			transformer string
			onlyLong    bool
		}{
			{name: "etl-args-hpush", commType: etl.Hpush, transformer: tetl.HashWithArgs},
			{name: "etl-args-hpull", commType: etl.Hpull, transformer: tetl.HashWithArgs},
		}
	)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			msg := tetl.InitSpec(t, baseParams, transformer, test.commType, etl.ArgTypeDefault)
			t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, msg.Name()) })

			var seed = rand.Uint64N(1000)

			testETLObject(t, msg.Name(), seed, "", "",
				func(r io.Reader) io.Reader {
					// Read the object and calculate the hash using the same hash algorithm as the ETL (same random seed).
					// The results should match because the hash is calculated in the same way.
					data, _ := io.ReadAll(r)
					hash := onexxh.Checksum64S(data, seed)
					hashHex := fmt.Sprintf("%016x", hash)

					return bytes.NewReader([]byte(hashHex))
				}, tools.FilesEqual,
			)
		})
	}
}

func TestETLBucketTransformParallel(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	var (
		proxyURL    = tools.RandomProxyURL(t)
		baseParams  = tools.BaseAPIParams(proxyURL)
		transformer = tetl.Echo // TODO: add more
		parallel    = 3
	)

	tests := []struct {
		commType string
		onlyLong bool
	}{
		{commType: etl.Hpush},
		{commType: etl.Hpull},
		{commType: etl.WebSocket},
	}

	for _, test := range tests {
		t.Run("etl_bucket_transform_parallel__"+test.commType, func(t *testing.T) {
			tools.CheckSkip(t, &tools.SkipTestArgs{Long: test.onlyLong})
			msg := tetl.InitSpec(t, baseParams, transformer, test.commType, etl.ArgTypeDefault)
			t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, msg.Name()) })

			wg := &sync.WaitGroup{}
			for i := range parallel {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()

					var (
						bckFrom = cmn.Bck{Name: "etlsrc_" + cos.GenTie(), Provider: apc.AIS}
						bckTo   = cmn.Bck{Name: "etldst_" + cos.GenTie(), Provider: apc.AIS}
						m       = ioContext{
							t:         t,
							num:       100,
							fileSize:  512,
							fixedSize: true,
							bck:       bckFrom,
						}
					)
					m.init(true /*cleanup*/)
					tools.CreateBucket(t, proxyURL, m.bck, nil, true /*cleanup*/)
					m.puts()

					msg := &apc.TCBMsg{
						Transform: apc.Transform{
							Name:    msg.Name(),
							Timeout: cos.Duration(30 * time.Second),
						},
						CopyBckMsg: apc.CopyBckMsg{Force: true},
					}

					tlog.Logfln("dispatch %dth ETL bucket transform, from %s to %s", i, bckFrom.Cname(""), bckTo.Cname(""))
					tetl.ETLBucketWithCmp(t, baseParams, bckFrom, bckTo, msg, tools.ReaderEqual)
				}(i)
			}
			wg.Wait()
		})
	}
}

func TestETLAnyToAnyBucket(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		objCnt     = 1000
		fileSize   = 512
		prefix     = "prefix-" + cos.GenUUID()

		bcktests = []testBucketConfig{{false, false, false}}

		tests = []testObjConfig{
			{transformer: tetl.MD5, comm: etl.Hpush, transform: tetl.MD5Transform},
			{transformer: tetl.MD5, comm: etl.WebSocket, transform: tetl.MD5Transform},
			{transformer: tetl.Echo, comm: etl.Hpull, transform: tetl.EchoTransform},
		}
	)

	if cliBck.IsRemote() {
		bcktests = append(bcktests,
			testBucketConfig{true, false, false},
			testBucketConfig{true, true, false},
			testBucketConfig{false, false, true},
		)
	}
	if testing.Short() {
		objCnt /= 10
	}

	for _, bcktest := range bcktests {
		bckFrom, tname := bcktest.setupBckFrom(t, prefix, objCnt) // setup source bucket & cleanup

		t.Run(tname, func(t *testing.T) {
			for _, test := range tests {
				etlName := test.transformer
				t.Run(fmt.Sprintf("%s-%s", test.transformer, strings.TrimSuffix(test.comm, "://")), func(t *testing.T) {
					msg := tetl.InitSpec(t, baseParams, etlName, test.comm, etl.ArgTypeDefault)
					testETLBucket(t, baseParams, msg.Name(), prefix, bckFrom, objCnt, fileSize, time.Minute*3, false, bcktest, test.transform)
				})
			}
		})
	}
}

// also responsible for cleanup: ETL xaction, ETL containers, destination bucket.
func testETLBucket(t *testing.T, bp api.BaseParams, etlName, prefix string, bckFrom cmn.Bck, objCnt, fileSize int, timeout time.Duration,
	skipByteStats bool, bcktest testBucketConfig, transform transformFunc, pipeline ...string) {
	var (
		xid, kind      string
		err            error
		requestTimeout = 30 * time.Second
	)

	t.Cleanup(func() { tetl.StopAndDeleteETL(t, bp, etlName) })

	numWorkersTest := []int{0, 4}

	if !testing.Short() {
		numWorkersTestLong := []int{-1, 16}
		numWorkersTest = append(numWorkersTest, numWorkersTestLong...)
	}

	for _, numWorkers := range numWorkersTest {
		t.Run(fmt.Sprintf("numWorkers=%d", numWorkers), func(t *testing.T) {
			// setup destination bucket & cleanup
			bckTo := bcktest.setupBckTo(t, prefix, objCnt)
			msg := &apc.TCBMsg{
				Transform: apc.Transform{
					Name:     etlName,
					Pipeline: pipeline,
					Timeout:  cos.Duration(requestTimeout),
				},
				CopyBckMsg: apc.CopyBckMsg{Force: true, Prefix: prefix},
				NumWorkers: numWorkers,
			}

			tlog.Logfln("Start ETL[%s]: %s => %s ...", etlName, bckFrom.Cname(""), bckTo.Cname(""))
			if bcktest.evictRemoteSrc {
				tools.EvictObjects(t, proxyURL, bckFrom, prefix)
				kind = apc.ActETLObjects // TODO -- FIXME: remove/simplify-out the reliance on x-kind
				xid, err = api.ETLBucket(bp, bckFrom, bckTo, msg, apc.FltExists)
			} else {
				kind = apc.ActETLBck
				xid, err = api.ETLBucket(bp, bckFrom, bckTo, msg)
			}
			tassert.CheckFatal(t, err)

			tlog.Logfln("ETL[%s]: running %s => %s x-etl[%s]", etlName, bckFrom.Cname(""), bckTo.Cname(""), xid)

			err = tetl.WaitForFinished(bp, xid, kind, timeout)
			tassert.CheckFatal(t, err)

			snaps, err := api.QueryXactionSnaps(baseParams, &xact.ArgsMsg{ID: xid, Kind: kind, Timeout: timeout})
			tassert.CheckFatal(t, err)
			total, err := snaps.TotalRunningTime(xid)
			tassert.CheckFatal(t, err)
			tlog.Logfln("Transforming bucket %s with %d workers took %v", bckFrom.Cname(""), numWorkers, total)

			err = tetl.ListObjectsWithRetry(bp, bckTo, prefix, objCnt, tools.WaitRetryOpts{MaxRetries: 5, Interval: time.Second * 3})
			tassert.CheckFatal(t, err)

			// only verify object content in long test mode (takes long time to complete)
			if !testing.Short() && transform != nil {
				tlog.Logfln("Verifying %d object content", objCnt)
				objeList, err := api.ListObjects(bp, bckTo, &apc.LsoMsg{Prefix: prefix}, api.ListArgs{})
				tassert.CheckFatal(t, err)

				for i, en := range objeList.Entries { // TODO: introudce goroutines to speed up the content check
					if i > 0 && i%max(1, objCnt/10) == 0 {
						tlog.Logfln("Verified %d/%d objects", i, objCnt)
					}
					err := tools.WaitForCondition(func() bool {
						r1, _, err := api.GetObjectReader(bp, bckFrom, en.Name, &api.GetArgs{})
						if err != nil {
							return false
						}
						defer r1.Close()

						r2, _, err := api.GetObjectReader(bp, bckTo, en.Name, &api.GetArgs{})
						if err != nil {
							return false
						}
						defer r2.Close()

						return tools.ReaderEqual(transform(r1), r2)
					}, tools.WaitRetryOpts{MaxRetries: 5, Interval: time.Second})
					tassert.Fatalf(t, err == nil, "object content mismatch after retries: %s vs %s", bckFrom.Cname(en.Name), bckTo.Cname(en.Name))
				}
			}
			checkETLStats(t, xid, objCnt, uint64(fileSize*objCnt), skipByteStats)
		})
	}
}

func TestETLFQN(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		prefix     = "prefix-" + cos.GenTie()

		m = ioContext{
			t:         t,
			num:       10,
			fileSize:  512,
			fixedSize: true,
			prefix:    prefix,
			bck:       cmn.Bck{Name: "etl_" + trand.String(5), Provider: apc.AIS},
		}

		tests = []struct {
			transformer string
			commType    string
			transform   transformFunc
		}{
			{transformer: tetl.Echo, commType: etl.Hpush, transform: tetl.EchoTransform},
			{transformer: tetl.Echo, commType: etl.Hpull, transform: tetl.EchoTransform},
			{transformer: tetl.MD5, commType: etl.Hpush, transform: tetl.MD5Transform},
			{transformer: tetl.MD5, commType: etl.WebSocket, transform: tetl.MD5Transform},
		}
	)

	tools.CreateBucket(t, proxyURL, m.bck, nil, true /*cleanup*/)

	m.init(true /*cleanup*/)

	m.puts()

	for _, testType := range []string{"etl_object", "etl_bucket"} {
		for _, test := range tests {
			t.Run(testType+"__"+test.transformer, func(t *testing.T) {
				msg := tetl.InitSpec(t, baseParams, test.transformer, test.commType, etl.ArgTypeFQN)

				switch testType {
				case "etl_object":
					t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, msg.Name()) })
					testETLObject(t, msg.Name(), "", "", "", test.transform, tools.FilesEqual)
				case "etl_bucket":
					testETLBucket(t, baseParams, msg.Name(), prefix, m.bck, m.num, int(m.fileSize), time.Minute,
						false /*skip checking byte counts*/, testBucketConfig{false, false, false} /* remote src evicted */, test.transform)
				default:
					panic(testType)
				}
			})
		}
	}
}

func TestETLBucketDryRun(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)

		bckFrom = cmn.Bck{Name: "etloffline", Provider: apc.AIS}
		bckTo   = cmn.Bck{Name: "etloffline-out-" + trand.String(5), Provider: apc.AIS}
		objCnt  = 10

		m = ioContext{
			t:         t,
			num:       objCnt,
			fileSize:  512,
			fixedSize: true,
			bck:       bckFrom,
		}
	)

	tools.CreateBucket(t, proxyURL, bckFrom, nil, true /*cleanup*/)
	m.init(true /*cleanup*/)

	m.puts()

	initMsg := tetl.InitSpec(t, baseParams, tetl.Echo, etl.Hpush, etl.ArgTypeDefault)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, initMsg.Name()) })

	msg := &apc.TCBMsg{
		Transform: apc.Transform{
			Name: initMsg.Name(),
		},
		CopyBckMsg: apc.CopyBckMsg{DryRun: true, Force: true},
	}
	xid, err := api.ETLBucket(baseParams, bckFrom, bckTo, msg)
	tassert.CheckFatal(t, err)

	args := xact.ArgsMsg{ID: xid, Timeout: time.Minute}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	exists, err := api.QueryBuckets(baseParams, cmn.QueryBcks(bckTo), apc.FltPresent)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, exists == false, "[dry-run] expected destination bucket not to be created")

	checkETLStats(t, xid, m.num, uint64(m.num*int(m.fileSize)), false)
}

func TestETLStopAndRestartETL(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		etlName    = tetl.Echo // TODO: currently, echo only - add more
	)

	msg := tetl.InitSpec(t, baseParams, etlName, etl.Hpush, etl.ArgTypeDefault)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, msg.Name()) })

	// 1. Check ETL is in running state
	tetl.ETLCheckStage(t, baseParams, msg.Name(), etl.Running)

	// 2. Stop ETL and verify it stopped successfully
	tlog.Logfln("stopping ETL[%s]", msg.Name())
	err := api.ETLStop(baseParams, msg.Name())
	tassert.CheckFatal(t, err)
	tetl.ETLCheckStage(t, baseParams, msg.Name(), etl.Aborted)

	// 3. Start ETL and verify it is in running state
	tlog.Logfln("restarting ETL[%s]", msg.Name())
	err = api.ETLStart(baseParams, msg.Name())
	tassert.CheckFatal(t, err)
	tetl.ETLCheckStage(t, baseParams, msg.Name(), etl.Running)
}

func TestETLCopyTransformSingleObj(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)

		bckFrom = cmn.Bck{
			Name:     "obj-cp-transform-from-" + trand.String(5),
			Provider: apc.AIS,
		}
		bckTo = cmn.Bck{
			Name:     "obj-cp-transform-to-" + trand.String(5),
			Provider: apc.AIS,
		}
		objFrom       = trand.String(10)
		objTo         = trand.String(10)
		transformer   = tetl.MD5
		transformFunc = tetl.MD5Transform
		comm          = etl.Hpush
		content       = []byte("This is a test object for ETL transformation. It will be transformed to MD5 checksum.")
		etl1, etl2    *api.ETL
	)
	msg := tetl.InitSpec(t, baseParams, transformer, comm, etl.ArgTypeDefault)
	etl1 = &api.ETL{ETLName: msg.Name()}
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, msg.Name()) })

	tools.CreateBucket(t, proxyURL, bckFrom, nil, true /*cleanup*/)
	tools.CreateBucket(t, proxyURL, bckTo, nil, true /*cleanup*/)

	tlog.Logln("PUT object")
	_, err := api.PutObject(&api.PutArgs{
		BaseParams: baseParams,
		Bck:        bckFrom,
		ObjName:    objFrom,
		Reader:     readers.NewBytes(content),
	})
	tassert.CheckFatal(t, err)

	// transform the object
	err = api.TransformObject(baseParams, &api.TransformArgs{
		CopyArgs: api.CopyArgs{
			FromBck:     bckFrom,
			FromObjName: objFrom,
			ToBck:       bckTo,
			ToObjName:   objTo,
		},
		ETL: *etl1,
	})
	tassert.CheckFatal(t, err)

	tlog.Logln("GET transformed object")
	r, _, err := api.GetObjectReader(baseParams, bckTo, objTo, &api.GetArgs{})
	tassert.CheckFatal(t, err)

	tassert.Fatal(t, tools.ReaderEqual(transformFunc(readers.NewBytes(content)), r), "expected transformed object to match the MD5 checksum")

	msg2 := tetl.InitSpec(t, baseParams, transformer, comm, etl.ArgTypeDefault)
	etl2 = &api.ETL{ETLName: msg2.Name()}
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, msg2.Name()) })

	err = api.TransformObject(baseParams, &api.TransformArgs{
		CopyArgs: api.CopyArgs{
			FromBck:     bckFrom,
			FromObjName: objFrom,
			ToBck:       bckTo,
			ToObjName:   objTo,
		},
		ETL: *etl1.Chain(etl2),
	})
	tassert.CheckFatal(t, err)

	tlog.Logln("GET transformed object")
	r, _, err = api.GetObjectReader(baseParams, bckTo, objTo, &api.GetArgs{})
	tassert.CheckFatal(t, err)

	tassert.Fatal(t, tools.ReaderEqual(transformFunc(transformFunc(readers.NewBytes(content))), r), "expected transformed object to match the MD5 of the MD5 checksum")
}

func TestETLMultipleTransformersAtATime(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s, Long: true})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	output, err := exec.Command("bash", "-c", "kubectl get nodes | grep Ready | wc -l").CombinedOutput()
	tassert.CheckFatal(t, err)
	if strings.Trim(string(output), "\n") != "1" {
		t.Skip("Requires a single node kubernetes cluster")
	}

	if tools.GetClusterMap(t, proxyURL).CountTargets() > 1 {
		t.Skip("Requires a single-node single-target deployment")
	}

	msg1 := tetl.InitSpec(t, baseParams, tetl.Echo, etl.Hpush, etl.ArgTypeDefault)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, msg1.Name()) })

	msg2 := tetl.InitSpec(t, baseParams, tetl.MD5, etl.Hpush, etl.ArgTypeDefault)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, msg2.Name()) })
}

const getMetricsTimeout = 15 * time.Second

func TestETLHealth(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		etlName    = tetl.Echo // TODO: currently, only echo - add more
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s, Long: true})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	msg := tetl.InitSpec(t, baseParams, etlName, etl.Hpull, etl.ArgTypeDefault)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, msg.Name()) })

	var (
		start    = time.Now()
		deadline = start.Add(getMetricsTimeout) // might take a while for metrics to become available
		healths  etl.HealthByTarget
		err      error
	)
	for {
		now := time.Now()
		if now.After(deadline) {
			t.Fatal("Timeout waiting for successful health response")
		}

		healths, err = api.ETLHealth(baseParams, etlName)
		if err == nil {
			if len(healths) > 0 {
				tlog.Logfln("Successfully received health data after %s", now.Sub(start))
				break
			}
			tlog.Logln("Unexpected empty health messages without error, retrying...")
			continue
		}

		herr, ok := err.(*cmn.ErrHTTP)
		tassert.Errorf(t, ok && herr.Status == http.StatusNotFound, "Unexpected error %v, expected 404", err)
		tlog.Logfln("ETL[%s] not found in metrics, retrying...", etlName)
		time.Sleep(10 * time.Second)
	}

	// TODO -- FIXME: see health handlers returning "OK" - revisit
	for _, msg := range healths {
		tassert.Errorf(t, msg.Status == "Running", "Expected pod at %s to be running, got %q",
			meta.Tname(msg.TargetID), msg.Status)
	}
}

func TestETLMetrics(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		etlName    = tetl.Echo // TODO: currently, only echo - add more
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	msg := tetl.InitSpec(t, baseParams, etlName, etl.Hpull, etl.ArgTypeDefault)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, msg.Name()) })

	var (
		start    = time.Now()
		deadline = start.Add(getMetricsTimeout) // might take a while for metrics to become available
		metrics  etl.CPUMemByTarget
		err      error
	)
	for {
		now := time.Now()
		if now.After(deadline) {
			t.Skip("Warning: timeout waiting for successful metrics response, metrics server might be unavailable")
		}

		metrics, err = api.ETLMetrics(baseParams, etlName)
		if err == nil {
			if len(metrics) > 0 {
				tlog.Logfln("Successfully received metrics after %s", now.Sub(start))
				break
			}
			tlog.Logln("Unexpected empty metrics messages without error, retrying...")
			continue
		}

		herr, ok := err.(*cmn.ErrHTTP)
		tassert.Errorf(t, ok && herr.Status == http.StatusNotFound, "Unexpected error %v, expected 404", err)
		tlog.Logfln("ETL[%s] not found in metrics, retrying...", etlName)
		time.Sleep(10 * time.Second)
	}

	for _, metric := range metrics {
		tassert.Errorf(t, metric.CPU > 0.0 || metric.Mem > 0, "[%s] expected non empty metrics info, got %v",
			metric.TargetID, metric)
	}
}

func TestETLList(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		etlName    = tetl.Echo // TODO: currently, only echo - add more
	)
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})

	msg := tetl.InitSpec(t, baseParams, etlName, etl.Hpush, etl.ArgTypeDefault)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, msg.Name()) })

	list, err := api.ETLList(baseParams)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(list) == 1, "expected exactly one ETL to be listed, got %d (%+v)", len(list), list)
	tassert.Fatalf(t, list[0].Name == msg.Name(), "expected ETL[%s], got %q", msg.Name(), list[0].Name)
}

func TestETLPodWithResourcesConstraint(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	smap := tools.GetClusterMap(t, proxyURL)
	si, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)

	msg := tetl.InitSpec(t, baseParams, tetl.PodWithResourcesConstraint, etl.Hpush, etl.ArgTypeDefault, "256Mi", "1", "512Mi", "1")
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, msg.Name()) })
	pod := tetl.InspectPod(t, msg.PodName(si.ID()))
	tassert.Fatalf(t, pod.Spec.Containers[0].Resources.Requests.Memory().Value() == 256*cos.MiB, "expected memory request to be 256Mi, got %d", pod.Spec.Containers[0].Resources.Requests.Memory().Value())
	tassert.Fatalf(t, pod.Spec.Containers[0].Resources.Requests.Cpu().Value() == 1, "expected CPU request to be 1, got %d", pod.Spec.Containers[0].Resources.Requests.Cpu().Value())
	tassert.Fatalf(t, pod.Spec.Containers[0].Resources.Limits.Memory().Value() == 512*cos.MiB, "expected memory limit to be 512Mi, got %d", pod.Spec.Containers[0].Resources.Limits.Memory().Value())
	tassert.Fatalf(t, pod.Spec.Containers[0].Resources.Limits.Cpu().Value() == 1, "expected CPU limit to be 1, got %d", pod.Spec.Containers[0].Resources.Limits.Cpu().Value())
}
func TestETLPodInitSpecFailure(t *testing.T) {
	var (
		proxyURL           = tools.RandomProxyURL(t)
		baseParams         = tools.BaseAPIParams(proxyURL)
		failureTestTimeout = cos.Duration(time.Second * 30) // Should fail quickly, no need to wait too long
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	tests := []struct {
		etlName      string
		commType     string
		cleanup      bool
		expectedErrs []string
	}{
		{etlName: tetl.InvalidYaml, commType: etl.Hpull, cleanup: false, expectedErrs: []string{"could not find expected ':'"}},
		{etlName: tetl.NonExistImage, commType: etl.Hpull, cleanup: true, expectedErrs: []string{"ErrImagePull", "ImagePullBackOff"}},
	}

	for _, test := range tests {
		t.Run(test.etlName, func(t *testing.T) {
			spec, err := tetl.GetTransformYaml(test.etlName)
			tassert.CheckFatal(t, err)

			msg := &etl.InitSpecMsg{
				InitMsgBase: etl.InitMsgBase{
					EtlName:     test.etlName,
					CommTypeX:   test.commType,
					InitTimeout: failureTestTimeout,
				},
				Spec: spec,
			}
			tassert.Fatalf(t, msg.Name() == test.etlName, "%q vs %q", msg.Name(), test.etlName)

			_, err = api.ETLInit(baseParams, msg)
			if test.cleanup {
				t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, test.etlName) })
			}

			testETLAnyErrors(t, err, test.expectedErrs...)
		})
	}
}

func TestETLPodInitClassFailure(t *testing.T) {
	var (
		proxyURL           = tools.RandomProxyURL(t)
		baseParams         = tools.BaseAPIParams(proxyURL)
		failureTestTimeout = cos.Duration(time.Second * 30) // Should fail quickly, no need to wait too long
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	if !testing.Short() {
		failureTestTimeout = cos.Duration(time.Minute * 3)
	}

	const (
		classPayload        = "ETL_CLASS_PAYLOAD"
		packages            = "PACKAGES"
		py310               = "aistorage/runtime_python:3.10"
		py311               = "aistorage/runtime_python:3.11"
		invalidClassPayload = `
	invalid_code_function_body...
	`
	)

	tests := []struct {
		etlName      string
		deps         string
		code         string
		runtime      string
		commType     string
		onlyLong     bool
		expectedErrs []string
	}{
		{etlName: "invalid-class-payload", deps: "", code: invalidClassPayload, runtime: py311, expectedErrs: []string{"Failed to deserialize ETL class"}},
		{etlName: "empty-class-payload", deps: "", code: "", runtime: py311, expectedErrs: []string{"ETL_CLASS_PAYLOAD is not set"}},
	}
	for _, test := range tests {
		t.Run(test.etlName, func(t *testing.T) {
			tools.CheckSkip(t, &tools.SkipTestArgs{Long: test.onlyLong})
			msg := etl.ETLSpecMsg{
				InitMsgBase: etl.InitMsgBase{
					EtlName:     test.etlName,
					CommTypeX:   test.commType,
					InitTimeout: failureTestTimeout,
				},
				Runtime: etl.RuntimeSpec{
					Image:   test.runtime,
					Command: []string{"python", "bootstrap.py"},
					Env: []corev1.EnvVar{
						{Name: packages, Value: test.deps},
						{Name: classPayload, Value: test.code},
					},
				},
			}

			_, err := api.ETLInit(baseParams, &msg)
			tlog.Logfln(err.Error())
			t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, test.etlName) })

			testETLAllErrors(t, err, test.expectedErrs...)
		})
	}
}
