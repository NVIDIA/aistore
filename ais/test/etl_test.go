// Package integration_test.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	cryptorand "crypto/rand"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/ext/etl/runtime"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tetl"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact"
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

func (tc *testObjConfig) Name() string {
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

	for i := range len(examples1) {
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
	tools.PutObject(t, bck, objName, reader)

	fho, err := cos.CreateFile(outputFileName)
	tassert.CheckFatal(t, err)
	defer fho.Close()

	tlog.Logf("GET %s via etl[%s]\n", bck.Cname(objName), etlName)
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

	tools.CheckSkip(t, &tools.SkipTestArgs{Long: onlyLong})

	// TODO: PUT and then transform many objects

	objName := fmt.Sprintf("%s-%s-object", etlName, trand.String(5))
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
		tlog.Logf("Evicting object %s\n", bck.Cname(objName))
		err := api.EvictObject(baseParams, bck, objName)
		tassert.CheckFatal(t, err)
	}

	defer func() {
		// Could bucket is not destroyed, remove created object instead.
		err := api.DeleteObject(baseParams, bck, objName)
		tassert.CheckError(t, err)
	}()

	bf := bytes.NewBuffer(nil)
	tlog.Logf("Use ETL[%s] to read transformed object\n", etlName)
	err = api.ETLObject(baseParams, etlName, bck, objName, bf)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, bf.Len() == cos.KiB, "Expected %d bytes, got %d", cos.KiB, bf.Len())
}

// NOTE: BytesCount references number of bytes *before* the transformation.
func checkETLStats(t *testing.T, xid string, expectedObjCnt int, expectedBytesCnt uint64, skipByteStats bool) {
	snaps, err := api.QueryXactionSnaps(baseParams, &xact.ArgsMsg{ID: xid})
	tassert.CheckFatal(t, err)

	objs, outObjs, inObjs := snaps.ObjCounts(xid)

	tassert.Errorf(t, objs == int64(expectedObjCnt), "expected %d objects, got %d (where sent %d, received %d)",
		expectedObjCnt, objs, outObjs, inObjs)
	if outObjs != inObjs {
		tlog.Logf("Warning: (sent objects) %d != %d (received objects)\n", outObjs, inObjs)
	} else {
		tlog.Logf("Num sent/received objects: %d\n", outObjs)
	}

	if skipByteStats {
		return // don't know the size
	}
	bytes, outBytes, inBytes := snaps.ByteCounts(xid)

	// TODO -- FIXME: validate transformed bytes as well, make sure `expectedBytesCnt` is correct

	tlog.Logf("Byte counts: expected %d, got (original %d, sent %d, received %d)\n", expectedBytesCnt,
		bytes, outBytes, inBytes)
}

func TestETLObject(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
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
			tools.CheckSkip(t, &tools.SkipTestArgs{Long: test.onlyLong})

			_ = tetl.InitSpec(t, baseParams, test.transformer, test.comm)
			t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, test.transformer) })

			testETLObject(t, test.transformer, test.inPath, test.outPath, test.transform, test.filesEqual)
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
			_ = tetl.InitSpec(t, baseParams, tetl.Echo, comm)
			t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, tetl.Echo) })

			for _, conf := range configs {
				t.Run(fmt.Sprintf("cached=%t", conf.cached), func(t *testing.T) {
					testETLObjectCloud(t, cliBck, tetl.Echo, conf.onlyLong, conf.cached)
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
		}
	)

	for _, test := range tests {
		t.Run(test.Name(), func(t *testing.T) {
			tools.CheckSkip(t, &tools.SkipTestArgs{Long: test.onlyLong})

			_ = tetl.InitSpec(t, baseParams, test.transformer, test.comm)
			t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, test.transformer) })

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
				Query:  url.Values{apc.QparamETLName: {test.transformer}},
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

	_ = tetl.InitSpec(t, baseParams, transformer, comm)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, transformer) })

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
		Query:  url.Values{apc.QparamETLName: {transformer}},
	})
	tassert.CheckFatal(t, err)

	exp, got := reader.Cksum().Val(), string(outObject.Bytes())
	tassert.Errorf(t, exp == got, "expected transformed object to be md5 checksum %s, got %s", exp,
		got[:min(len(got), 16)])
}

func TestETLAnyToAnyBucket(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		objCnt     = 100

		bcktests = []struct {
			srcRemote      bool
			evictRemoteSrc bool
			dstRemote      bool
		}{
			{false, false, false},
			{true, false, false},
			{true, true, false},
			{false, false, true},
		}
		tests = []testObjConfig{
			{transformer: tetl.Echo, comm: etl.Hpull, onlyLong: true},
			{transformer: tetl.MD5, comm: etl.Hrev},
			{transformer: tetl.MD5, comm: etl.Hpush, onlyLong: true},
		}
	)

	for _, bcktest := range bcktests {
		m := ioContext{
			t:         t,
			num:       objCnt,
			fileSize:  512,
			fixedSize: true, // see checkETLStats below
		}
		if bcktest.srcRemote {
			m.bck = cliBck
			m.deleteRemoteBckObjs = true
		} else {
			m.bck = cmn.Bck{Name: "etlsrc_" + cos.GenTie(), Provider: apc.AIS}
			tools.CreateBucket(t, proxyURL, m.bck, nil, true /*cleanup*/)
		}
		m.init(true /*cleanup*/)

		if bcktest.srcRemote {
			m.remotePuts(false) // (deleteRemoteBckObjs above)
			if bcktest.evictRemoteSrc {
				tlog.Logf("evicting %s\n", m.bck)
				//
				// evict all _cached_ data from the "local" cluster
				// keep the src bucket in the "local" BMD though
				//
				err := api.EvictRemoteBucket(baseParams, m.bck, true /*keep empty src bucket in the BMD*/)
				tassert.CheckFatal(t, err)
			}
		} else {
			m.puts()
		}

		for _, test := range tests {
			// NOTE: have to use one of the predefined etlName which, by coincidence,
			// corresponds to the test.transformer name and is further used to resolve
			// the corresponding init-spec yaml, e.g.:
			// https://raw.githubusercontent.com/NVIDIA/ais-etl/master/transformers/md5/pod.yaml"
			// See also: tetl.validateETLName
			etlName := test.transformer

			tname := fmt.Sprintf("%s-%s", test.transformer, strings.TrimSuffix(test.comm, "://"))
			if bcktest.srcRemote {
				if bcktest.evictRemoteSrc {
					tname += "/from-evicted-remote"
				} else {
					tname += "/from-remote"
				}
			} else {
				debug.Assert(!bcktest.evictRemoteSrc)
				tname += "/from-ais"
			}
			if bcktest.dstRemote {
				tname += "/to-remote"
			} else {
				tname += "/to-ais"
			}
			t.Run(tname, func(t *testing.T) {
				tools.CheckSkip(t, &tools.SkipTestArgs{Long: test.onlyLong})
				_ = tetl.InitSpec(t, baseParams, etlName, test.comm)

				var bckTo cmn.Bck
				if bcktest.dstRemote {
					bckTo = cliBck
					dstm := ioContext{t: t, bck: bckTo}
					dstm.del()
					t.Cleanup(func() { dstm.del() })
				} else {
					bckTo = cmn.Bck{Name: "etldst_" + cos.GenTie(), Provider: apc.AIS}
					// NOTE: ais will create dst bucket on the fly

					t.Cleanup(func() { tools.DestroyBucket(t, proxyURL, bckTo) })
				}
				testETLBucket(t, baseParams, etlName, &m, bckTo, time.Minute, false, bcktest.evictRemoteSrc)
			})
		}
	}
}

// also responsible for cleanup: ETL xaction, ETL containers, destination bucket.
func testETLBucket(t *testing.T, bp api.BaseParams, etlName string, m *ioContext, bckTo cmn.Bck, timeout time.Duration,
	skipByteStats, evictRemoteSrc bool) {
	var (
		xid, kind      string
		err            error
		bckFrom        = m.bck
		requestTimeout = 30 * time.Second

		msg = &apc.TCBMsg{
			Transform: apc.Transform{
				Name:    etlName,
				Timeout: cos.Duration(requestTimeout),
			},
			CopyBckMsg: apc.CopyBckMsg{Force: true},
		}
	)

	t.Cleanup(func() { tetl.StopAndDeleteETL(t, bp, etlName) })
	tlog.Logf("Start ETL[%s]: %s => %s ...\n", etlName, bckFrom.Cname(""), bckTo.Cname(""))

	if evictRemoteSrc {
		kind = apc.ActETLObjects // TODO -- FIXME: remove/simplify-out the reliance on x-kind
		xid, err = api.ETLBucket(bp, bckFrom, bckTo, msg, apc.FltExists)
	} else {
		kind = apc.ActETLBck
		xid, err = api.ETLBucket(bp, bckFrom, bckTo, msg)
	}
	tassert.CheckFatal(t, err)

	t.Cleanup(func() {
		if bckTo.IsRemote() {
			err = api.EvictRemoteBucket(bp, bckTo, false /*keep md*/)
			tassert.CheckFatal(t, err)
			tlog.Logf("[cleanup] %s evicted\n", bckTo)
		} else {
			tools.DestroyBucket(t, bp.URL, bckTo)
		}
	})

	tlog.Logf("ETL[%s]: running %s => %s x-etl[%s]\n", etlName, bckFrom.Cname(""), bckTo.Cname(""), xid)

	err = tetl.WaitForFinished(bp, xid, kind, timeout)
	tassert.CheckFatal(t, err)

	list, err := api.ListObjects(bp, bckTo, nil, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(list.Entries) == m.num, "expected %d objects, got %d", m.num, len(list.Entries))

	checkETLStats(t, xid, m.num, m.fileSize*uint64(m.num), skipByteStats)
}

func TestETLInitCode(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
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
			etlName   string
			code      string
			deps      string
			runtime   string
			commType  string
			chunkSize int64
			onlyLong  bool
		}{
			{etlName: "simple-py38", code: md5, deps: "", runtime: runtime.Py38, onlyLong: false},
			{etlName: "simple-py38-stream", code: echo, deps: "", runtime: runtime.Py38, onlyLong: false, chunkSize: 64},
			{etlName: "with-deps-py38", code: numpy, deps: numpyDeps, runtime: runtime.Py38, onlyLong: false},
			{etlName: "simple-py310-io", code: md5IO, deps: "", runtime: runtime.Py310, commType: etl.HpushStdin, onlyLong: false},
		}
	)

	tools.CreateBucket(t, proxyURL, m.bck, nil, true /*cleanup*/)

	m.init(true /*cleanup*/)

	m.puts()

	for _, testType := range []string{"etl_object", "etl_bucket"} {
		for _, test := range tests {
			t.Run(testType+"__"+test.etlName, func(t *testing.T) {
				tools.CheckSkip(t, &tools.SkipTestArgs{Long: test.onlyLong})

				msg := etl.InitCodeMsg{
					InitMsgBase: etl.InitMsgBase{
						IDX:       test.etlName,
						CommTypeX: test.commType,
						Timeout:   etlBucketTimeout,
					},
					Code:      []byte(test.code),
					Deps:      []byte(test.deps),
					Runtime:   test.runtime,
					ChunkSize: test.chunkSize,
				}
				msg.Funcs.Transform = "transform"

				_ = tetl.InitCode(t, baseParams, &msg)

				switch testType {
				case "etl_object":
					t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, test.etlName) })

					testETLObject(t, test.etlName, "", "", func(r io.Reader) io.Reader {
						return r // TODO: Write function to transform input to md5.
					}, func(_, _ string) (bool, error) {
						return true, nil // TODO: Write function to compare output from md5.
					})
				case "etl_bucket":
					bckTo := cmn.Bck{Name: "etldst_" + cos.GenTie(), Provider: apc.AIS}
					testETLBucket(t, baseParams, test.etlName, &m, bckTo, time.Minute,
						false /*skip checking byte counts*/, false /* remote src evicted */)
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

	_ = tetl.InitSpec(t, baseParams, tetl.Echo, etl.Hrev)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, tetl.Echo) })

	msg := &apc.TCBMsg{
		Transform: apc.Transform{
			Name: tetl.Echo,
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

	_ = tetl.InitSpec(t, baseParams, etlName, etl.Hrev)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, etlName) })

	// 1. Check ETL is in running state
	tetl.ETLShouldBeRunning(t, baseParams, etlName)

	// 2. Stop ETL and verify it stopped successfully
	tlog.Logf("stopping ETL[%s]\n", etlName)
	err := api.ETLStop(baseParams, etlName)
	tassert.CheckFatal(t, err)
	tetl.ETLShouldNotBeRunning(t, baseParams, etlName)

	// 3. Start ETL and verify it is in running state
	tlog.Logf("restarting ETL[%s]\n", etlName)
	err = api.ETLStart(baseParams, etlName)
	tassert.CheckFatal(t, err)
	tetl.ETLShouldBeRunning(t, baseParams, etlName)
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

	_ = tetl.InitSpec(t, baseParams, tetl.Echo, etl.Hrev)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, tetl.Echo) })

	_ = tetl.InitSpec(t, baseParams, tetl.MD5, etl.Hrev)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, tetl.MD5) })
}

const getMetricsTimeout = 90 * time.Second

func TestETLHealth(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
		etlName    = tetl.Echo // TODO: currently, only echo - add more
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s, Long: true})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	_ = tetl.InitSpec(t, baseParams, etlName, etl.Hpull)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, etlName) })

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
				tlog.Logf("Successfully received health data after %s\n", now.Sub(start))
				break
			}
			tlog.Logln("Unexpected empty health messages without error, retrying...")
			continue
		}

		herr, ok := err.(*cmn.ErrHTTP)
		tassert.Errorf(t, ok && herr.Status == http.StatusNotFound, "Unexpected error %v, expected 404", err)
		tlog.Logf("ETL[%s] not found in metrics, retrying...\n", etlName)
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

	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s, Long: true})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	_ = tetl.InitSpec(t, baseParams, etlName, etl.Hpull)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, etlName) })

	var (
		start    = time.Now()
		deadline = start.Add(getMetricsTimeout) // might take a while for metrics to become available
		metrics  etl.CPUMemByTarget
		err      error
	)
	for {
		now := time.Now()
		if now.After(deadline) {
			t.Fatal("Timeout waiting for successful metrics response")
		}

		metrics, err = api.ETLMetrics(baseParams, etlName)
		if err == nil {
			if len(metrics) > 0 {
				tlog.Logf("Successfully received metrics after %s\n", now.Sub(start))
				break
			}
			tlog.Logln("Unexpected empty metrics messages without error, retrying...")
			continue
		}

		herr, ok := err.(*cmn.ErrHTTP)
		tassert.Errorf(t, ok && herr.Status == http.StatusNotFound, "Unexpected error %v, expected 404", err)
		tlog.Logf("ETL[%s] not found in metrics, retrying...\n", etlName)
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

	_ = tetl.InitSpec(t, baseParams, etlName, etl.Hrev)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, etlName) })

	list, err := api.ETLList(baseParams)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(list) == 1, "expected exactly one ETL to be listed, got %d (%+v)", len(list), list)
	tassert.Fatalf(t, list[0].Name == etlName, "expected ETL[%s], got %q", etlName, list[0].Name)
}
