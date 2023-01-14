// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/ext/etl/runtime"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tetl"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/go-tfdata/tfdata/core"
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

	testCloudObjConfig struct {
		cached   bool
		onlyLong bool
	}
)

func (tc testObjConfig) Name() string {
	return fmt.Sprintf("%s/%s", tc.transformer, strings.TrimSuffix(tc.comm, "://"))
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

	for i := 0; i < len(examples1); i++ {
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

func testETLObject(t *testing.T, etlName, inPath, outPath string, fTransform transformFunc, fEq filesEqualFunc) {
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
	_, err := rand.Read(buf)
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

	tools.CreateBucketWithCleanup(t, proxyURL, bck, nil)
	defer tools.DestroyBucket(t, proxyURL, bck)

	tlog.Logln("PUT object")
	reader, err := readers.NewFileReaderFromFile(inputFilePath, cos.ChecksumNone)
	tassert.CheckFatal(t, err)
	tools.PutObject(t, bck, objName, reader)

	fho, err := cos.CreateFile(outputFileName)
	tassert.CheckFatal(t, err)
	defer fho.Close()

	tlog.Logf("GET %s/%s via etl[%s]\n", bck, objName, etlName)
	err = api.ETLObject(baseParams, etlName, bck, objName, fho)
	tassert.CheckFatal(t, err)

	tlog.Logln("Compare output")
	same, err := fEq(outputFileName, expectedOutputFilePath)
	tassert.CheckError(t, err)
	tassert.Errorf(t, same, "file contents after transformation differ")
}

func testETLObjectCloud(t *testing.T, bck cmn.Bck, etlName string, onlyLong, cached bool) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	tools.CheckSkip(t, tools.SkipTestArgs{Long: onlyLong})

	// TODO: PUT and then transform many objects

	objName := fmt.Sprintf("%s-%s-object", etlName, trand.String(5))
	tlog.Logln("PUT object")
	reader, err := readers.NewRandReader(cos.KiB, cos.ChecksumNone)
	tassert.CheckFatal(t, err)

	err = api.PutObject(api.PutObjectArgs{
		BaseParams: baseParams,
		Bck:        bck,
		Object:     objName,
		Reader:     reader,
	})
	tassert.CheckFatal(t, err)

	if !cached {
		tlog.Logf("Evicting object %s\n", objName)
		err := api.EvictObject(baseParams, bck, objName)
		tassert.CheckFatal(t, err)
	}

	defer func() {
		// Could bucket is not destroyed, remove created object instead.
		err := api.DeleteObject(baseParams, bck, objName)
		tassert.CheckError(t, err)
	}()

	bf := bytes.NewBuffer(nil)
	tlog.Logf("Use etl[%s] to read transformed object\n", etlName)
	err = api.ETLObject(baseParams, etlName, bck, objName, bf)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, bf.Len() == cos.KiB, "Expected %d bytes, got %d", cos.KiB, bf.Len())
}

// Responsible for cleaning ETL xaction, ETL containers, destination bucket.
func testETLBucket(t *testing.T, uuid string, bckFrom cmn.Bck, objCnt int, fileSize uint64, timeout time.Duration) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)

		bckTo          = cmn.Bck{Name: "etloffline-out-" + trand.String(5), Provider: apc.AIS}
		requestTimeout = 30 * time.Second
	)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, uuid) })

	tlog.Logf("Start offline etl[%s]\n", uuid)
	xactID := tetl.ETLBucket(t, baseParams, bckFrom, bckTo, &apc.TCBMsg{
		ID:             uuid,
		RequestTimeout: cos.Duration(requestTimeout),
		CopyBckMsg:     apc.CopyBckMsg{Force: true},
	})

	err := tetl.WaitForFinished(baseParams, xactID, timeout)
	tassert.CheckFatal(t, err)

	list, err := api.ListObjects(baseParams, bckTo, nil, 0)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(list.Entries) == objCnt, "expected %d objects from offline ETL, got %d", objCnt, len(list.Entries))
	checkETLStats(t, xactID, objCnt, fileSize*uint64(objCnt))
}

// NOTE: BytesCount references number of bytes *before* the transformation.
func checkETLStats(t *testing.T, xactID string, expectedObjCnt int, expectedBytesCnt uint64) {
	snaps, err := api.QueryXactionSnaps(baseParams, api.XactReqArgs{ID: xactID})
	tassert.CheckFatal(t, err)

	objs, outObjs, inObjs := snaps.ObjCounts(xactID)

	tassert.Errorf(t, objs == int64(expectedObjCnt), "expected %d objects, got %d (where sent %d, received %d)",
		expectedObjCnt, objs, outObjs, inObjs)
	if outObjs != inObjs {
		tlog.Logf("Warning: (sent objects) %d != %d (received objects)\n", outObjs, inObjs)
	} else {
		tlog.Logf("Num sent/received objects: %d\n", outObjs)
	}

	if expectedBytesCnt == 0 {
		return // don't know the size
	}
	bytes, outBytes, inBytes := snaps.ByteCounts(xactID)

	// TODO -- FIXME: validate transformed bytes as well, make sure `expectedBytesCnt` is correct

	tlog.Logf("Byte counts: expected %d, got (original %d, sent %d, received %d)\n", expectedBytesCnt,
		bytes, outBytes, inBytes)
}

func TestETLObject(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	noopTransform := func(r io.Reader) io.Reader { return r }
	tests := []testObjConfig{
		{transformer: tetl.Echo, comm: etl.Hpull, transform: noopTransform, filesEqual: tools.FilesEqual, onlyLong: true},
		{transformer: tetl.Echo, comm: etl.Hrev, transform: noopTransform, filesEqual: tools.FilesEqual, onlyLong: true},
		{transformer: tetl.Echo, comm: etl.Hpush, transform: noopTransform, filesEqual: tools.FilesEqual, onlyLong: true},
		{tetl.Tar2TF, etl.Hpull, tar2tfIn, tar2tfOut, nil, tfDataEqual, true},
		{tetl.Tar2TF, etl.Hrev, tar2tfIn, tar2tfOut, nil, tfDataEqual, true},
		{tetl.Tar2TF, etl.Hpush, tar2tfIn, tar2tfOut, nil, tfDataEqual, true},
		{tetl.Tar2tfFilters, etl.Hpull, tar2tfFiltersIn, tar2tfFiltersOut, nil, tfDataEqual, false},
		{tetl.Tar2tfFilters, etl.Hrev, tar2tfFiltersIn, tar2tfFiltersOut, nil, tfDataEqual, false},
		{tetl.Tar2tfFilters, etl.Hpush, tar2tfFiltersIn, tar2tfFiltersOut, nil, tfDataEqual, false},
	}

	for _, test := range tests {
		t.Run(test.Name(), func(t *testing.T) {
			tools.CheckSkip(t, tools.SkipTestArgs{Long: test.onlyLong})

			uuid := tetl.InitSpec(t, baseParams, test.transformer, test.comm)
			t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, uuid) })

			testETLObject(t, uuid, test.inPath, test.outPath, test.transform, test.filesEqual)
		})
	}
}

func TestETLObjectCloud(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Bck: cliBck, RequiredDeployment: tools.ClusterTypeK8s, RemoteBck: true})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	tcs := map[string][]*testCloudObjConfig{
		etl.Hpull: {
			{cached: true, onlyLong: false},
			{cached: false, onlyLong: false},
		},
		etl.Hrev: {
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
			uuid := tetl.InitSpec(t, baseParams, tetl.Echo, comm)
			t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, uuid) })

			for _, conf := range configs {
				t.Run(fmt.Sprintf("cached=%t", conf.cached), func(t *testing.T) {
					testETLObjectCloud(t, cliBck, uuid, conf.onlyLong, conf.cached)
				})
			}
		})
	}
}

// TODO: initial impl - revise and add many more tests
func TestETLInline(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)

		bck = cmn.Bck{Provider: apc.AIS, Name: "etl-test"}

		tests = []testObjConfig{
			{transformer: tetl.MD5, comm: etl.Hpush},
		}
	)

	for _, test := range tests {
		t.Run(test.Name(), func(t *testing.T) {
			tools.CheckSkip(t, tools.SkipTestArgs{Long: test.onlyLong})

			uuid := tetl.InitSpec(t, baseParams, test.transformer, test.comm)
			t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, uuid) })

			tools.CreateBucketWithCleanup(t, proxyURL, bck, nil)

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
			_, err = api.GetObject(baseParams, bck, objName, api.GetObjectInput{
				Writer: outObject,
				Query:  map[string][]string{apc.QparamUUID: {uuid}},
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
	tools.CheckSkip(t, tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	uuid := tetl.InitSpec(t, baseParams, transformer, comm)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, uuid) })

	tools.CreateBucketWithCleanup(t, proxyURL, bck, nil)

	tlog.Logln("PUT object")
	objName := trand.String(10)
	reader, err := readers.NewRandReader(cos.MiB, cos.ChecksumMD5)
	tassert.CheckFatal(t, err)

	err = api.PutObject(api.PutObjectArgs{
		BaseParams: baseParams,
		Bck:        bck,
		Object:     objName,
		Reader:     reader,
	})
	tassert.CheckFatal(t, err)

	tlog.Logln("GET transformed object")
	outObject := memsys.PageMM().NewSGL(0)
	defer outObject.Free()
	_, err = api.GetObject(baseParams, bck, objName, api.GetObjectInput{
		Writer: outObject,
		Query:  map[string][]string{apc.QparamUUID: {uuid}},
	})
	tassert.CheckFatal(t, err)

	exp, got := reader.Cksum().Val(), string(outObject.Bytes())
	tassert.Errorf(t, exp == got, "expected transformed object to be md5 checksum %s, got %s", exp,
		got[:cos.Min(len(got), 16)])
}

func TestETLBucket(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)

		bck    = cmn.Bck{Name: "etloffline", Provider: apc.AIS}
		objCnt = 10

		m = ioContext{
			t:         t,
			num:       objCnt,
			fileSize:  512,
			fixedSize: true,
			bck:       bck,
		}

		tests = []testObjConfig{
			{transformer: tetl.Echo, comm: etl.Hpull, onlyLong: true},
			{transformer: tetl.MD5, comm: etl.Hrev},
			{transformer: tetl.MD5, comm: etl.Hpush, onlyLong: true},
		}
	)

	tools.CreateBucketWithCleanup(t, proxyURL, bck, nil)
	m.initWithCleanup()

	tlog.Logf("PUT %d objects", m.num)
	m.puts()

	for _, test := range tests {
		t.Run(test.Name(), func(t *testing.T) {
			tools.CheckSkip(t, tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s, Long: test.onlyLong})

			uuid := tetl.InitSpec(t, baseParams, test.transformer, test.comm)
			testETLBucket(t, uuid, bck, objCnt, m.fileSize, time.Minute)
		})
	}
}

func TestETLInitCode(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	const (
		md5 = `
import hashlib

def transform(input_bytes):
    md5 = hashlib.md5()
    md5.update(input_bytes)
    return md5.hexdigest().encode()
`
		echo = `
def transform(reader, w):
    for chunk in reader:
        w.write(chunk)
`

		md5IO = `
import hashlib
import sys

md5 = hashlib.md5()
chunk = sys.stdin.buffer.read()
md5.update(chunk)
sys.stdout.buffer.write(md5.hexdigest().encode())
`

		numpy = `
import numpy as np

def transform(input_bytes: bytes) -> bytes:
    x = np.array([[0, 1], [2, 3]], dtype='<u2')
    return x.tobytes()
`
		numpyDeps = `numpy==1.19.2`
	)

	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)

		m = ioContext{
			t:         t,
			num:       10,
			fileSize:  512,
			fixedSize: true,
			bck:       cmn.Bck{Name: "etl_build", Provider: apc.AIS},
		}

		tests = []struct {
			name      string
			code      string
			deps      string
			runtime   string
			commType  string
			chunkSize int64
			onlyLong  bool
		}{
			{name: "simple-py38", code: md5, deps: "", runtime: runtime.Py38, onlyLong: false},
			{name: "simple-py38-stream", code: echo, deps: "", runtime: runtime.Py38, onlyLong: false, chunkSize: 64},
			{name: "with-deps-py38", code: numpy, deps: numpyDeps, runtime: runtime.Py38, onlyLong: false},
			{name: "simple-py310-io", code: md5IO, deps: "", runtime: runtime.Py310, commType: etl.HpushStdin, onlyLong: false},
		}
	)

	tools.CreateBucketWithCleanup(t, proxyURL, m.bck, nil)

	m.initWithCleanup()

	tlog.Logf("PUT objects => source bucket %s\n", m.bck)
	m.puts()

	for _, testType := range []string{"etl_object", "etl_bucket"} {
		for _, test := range tests {
			t.Run(testType+"__"+test.name, func(t *testing.T) {
				tools.CheckSkip(t, tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s, Long: test.onlyLong})

				msg := etl.InitCodeMsg{
					InitMsgBase: etl.InitMsgBase{
						IDX:       test.name,
						CommTypeX: test.commType,
						Timeout:   cos.Duration(5 * time.Minute),
					},
					Code:      []byte(test.code),
					Deps:      []byte(test.deps),
					Runtime:   test.runtime,
					ChunkSize: test.chunkSize,
				}
				msg.Funcs.Transform = "transform"

				etlName := tetl.InitCode(t, baseParams, msg)

				switch testType {
				case "etl_object":
					t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, etlName) })

					testETLObject(t, etlName, "", "", func(r io.Reader) io.Reader {
						return r // TODO: Write function to transform input to md5.
					}, func(f1, f2 string) (bool, error) {
						return true, nil // TODO: Write function to compare output from md5.
					})
				case "etl_bucket":
					testETLBucket(t, etlName, m.bck, m.num, m.fileSize, time.Minute)
				default:
					panic(testType)
				}
			})
		}
	}
}

func TestETLBucketDryRun(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
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

	tools.CreateBucketWithCleanup(t, proxyURL, bckFrom, nil)
	m.initWithCleanup()

	tlog.Logf("PUT %d objects", m.num)
	m.puts()

	etlName := tetl.InitSpec(t, baseParams, tetl.Echo, etl.Hrev)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, etlName) })

	tlog.Logf("Start offline etl[%s]\n", etlName)
	xactID, err := api.ETLBucket(baseParams, bckFrom, bckTo,
		&apc.TCBMsg{ID: etlName, CopyBckMsg: apc.CopyBckMsg{DryRun: true, Force: true}})
	tassert.CheckFatal(t, err)

	args := api.XactReqArgs{ID: xactID, Timeout: time.Minute}
	_, err = api.WaitForXactionIC(baseParams, args)
	tassert.CheckFatal(t, err)

	exists, err := api.QueryBuckets(baseParams, cmn.QueryBcks(bckTo), apc.FltPresent)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, exists == false, "expected destination bucket to not be created")

	checkETLStats(t, xactID, m.num, uint64(m.num*int(m.fileSize)))
}

func TestETLStopAndRestartETL(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	etlName := tetl.InitSpec(t, baseParams, tetl.Echo, etl.Hrev)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, etlName) })

	// 1. Check ETL is in running state
	tetl.ETLShouldBeRunning(t, baseParams, etlName)

	// 2. Stop ETL and verify it stopped successfully
	tlog.Logf("stopping etl[%s]", etlName)
	err := api.ETLStop(baseParams, etlName)
	tassert.CheckFatal(t, err)
	tetl.ETLShouldNotBeRunning(t, baseParams, etlName)

	// 3. Start ETL and verify it is in running state
	tlog.Logf("restarting etl[%s]", etlName)
	err = api.ETLStart(baseParams, etlName)
	tassert.CheckFatal(t, err)
	tetl.ETLShouldBeRunning(t, baseParams, etlName)
}

func TestETLMultipleTransformersAtATime(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s, Long: true})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	output, err := exec.Command("bash", "-c", "kubectl get nodes | grep Ready | wc -l").CombinedOutput()
	tassert.CheckFatal(t, err)
	if strings.Trim(string(output), "\n") != "1" {
		t.Skip("Requires a single node kubernetes cluster")
	}

	if tools.GetClusterMap(t, proxyURL).CountTargets() > 1 {
		t.Skip("Requires a single-node single-target deployment")
	}

	etlName1 := tetl.InitSpec(t, baseParams, tetl.Echo, etl.Hrev)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, etlName1) })

	etlName2 := tetl.InitSpec(t, baseParams, tetl.MD5, etl.Hrev)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, etlName2) })
}

func TestETLHealth(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	tools.CheckSkip(t, tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s, Long: true})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	tlog.Logln("Starting ETL")
	etlName := tetl.InitSpec(t, baseParams, tetl.Echo, etl.Hpull)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, etlName) })

	var (
		start    = time.Now()
		deadline = start.Add(2 * time.Minute)
		healths  etl.PodsHealthMsg
		err      error
	)

	// It might take some time for metrics to be available.
	for {
		now := time.Now()
		if now.After(deadline) {
			t.Fatal("Timeout waiting for successful health response")
		}

		healths, err = api.ETLHealth(baseParams, etlName)
		if err == nil {
			if len(healths) > 0 {
				tlog.Logf("Successfully received metrics after %s\n", now.Sub(start))
				break
			}
			tlog.Logln("Unexpected empty health messages without error, retrying...")
			continue
		}

		herr, ok := err.(*cmn.ErrHTTP)
		tassert.Errorf(t, ok && herr.Status == http.StatusNotFound, "Unexpected error %v, expected 404", err)
		tlog.Logf("etl[%s] not found in metrics, retrying...\n", etlName)
		time.Sleep(10 * time.Second)
	}

	for _, health := range healths {
		tassert.Errorf(t, health.CPU > 0.0 || health.Mem > 0, "[%s] expected non empty health info, got %v", health.TargetID, health)
	}
}

func TestETLList(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)
	tools.CheckSkip(t, tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})

	name := tetl.InitSpec(t, baseParams, tetl.Echo, etl.Hrev)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, name) })

	list, err := api.ETLList(baseParams)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(list) == 1, "expected exactly one ETL to be listed, got %d", len(list))
	tassert.Fatalf(t, list[0].Name == name, "expected etl[%s], got %q", name, list[0].Name)
}
