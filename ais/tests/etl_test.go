// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */

package integration

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/devtools/tutils/readers"
	"github.com/NVIDIA/aistore/devtools/tutils/tassert"
	"github.com/NVIDIA/aistore/devtools/tutils/tetl"
	"github.com/NVIDIA/aistore/etl"
	"github.com/NVIDIA/aistore/etl/runtime"
	"github.com/NVIDIA/go-tfdata/tfdata/core"
	jsoniter "github.com/json-iterator/go"
)

const (
	tar2tfIn  = "data/small-mnist-3.tar"
	tar2tfOut = "data/small-mnist-3.record"

	tar2tfFiltersIn  = "data/single-png-cls.tar"
	tar2tfFiltersOut = "data/single-png-cls-transformed.tfrecord"
)

type (
	filesEqualFunc func(f1, f2 string) (bool, error)

	testConfig struct {
		transformer string
		comm        string
		inPath      string         // optional
		outPath     string         // optional
		filesEqual  filesEqualFunc // optional
		onlyLong    bool           // run only with long tests
		cloud       bool           // run on a cloud bucket
	}
)

func (tc testConfig) Name() string {
	provider := "local"
	if tc.cloud {
		provider = "cloud"
	}
	return fmt.Sprintf("%s/%s/%s", tc.transformer, provider, strings.TrimSuffix(tc.comm, "://"))
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

func etlInit(name, comm string) (string, error) {
	tutils.Logln("Reading template")
	spec, err := tetl.GetTransformYaml(name)
	if err != nil {
		return "", err
	}
	pod, err := etl.ParsePodSpec(nil, spec)
	if err != nil {
		return "", err
	}
	if comm != "" {
		pod.Annotations["communication_type"] = comm
	}
	spec, _ = jsoniter.Marshal(pod)
	tutils.Logln("Init ETL")
	return api.ETLInit(baseParams, spec)
}

func testETLObject(t *testing.T, onlyLong, cloud bool, comm, transformer, inPath, outPath string, fEq filesEqualFunc) {
	bck := cmn.Bck{Name: "etl-test", Provider: cmn.ProviderAIS}
	if cloud {
		bck = cliBck
	}
	tutils.CheckSkip(t, tutils.SkipTestArgs{Bck: bck, Long: onlyLong, Cloud: cloud})

	var (
		testObjDir             = filepath.Join("data", "transformer", transformer)
		inputFileName          = filepath.Join(testObjDir, "object.in")
		expectedOutputFileName = filepath.Join(testObjDir, "object.out")

		objName        = fmt.Sprintf("%s-%s-object", transformer, cmn.RandString(5))
		outputFileName = filepath.Join(t.TempDir(), objName+".out")

		uuid string
	)

	if fEq == nil {
		fEq = tutils.FilesEqual
	}
	if inPath != "" {
		inputFileName = inPath
	}
	if outPath != "" {
		expectedOutputFileName = outPath
	}

	if !cloud {
		tutils.Logln("Creating bucket")
		tutils.CreateFreshBucket(t, proxyURL, bck)
	}

	tutils.Logln("Putting object")
	reader, err := readers.NewFileReaderFromFile(inputFileName, cmn.ChecksumNone)
	tassert.CheckFatal(t, err)

	if !cloud {
		tutils.PutObject(t, bck, objName, reader)
	} else {
		tutils.PutObjectInCloudBucketWithoutCachingLocally(t, bck, objName, reader)
		defer func() {
			// Could bucket is not destroyed, remove created object instead.
			err := api.DeleteObject(baseParams, bck, objName)
			tassert.CheckError(t, err)
		}()
	}

	uuid, err = etlInit(transformer, comm)
	tassert.CheckFatal(t, err)
	defer func() {
		tutils.Logf("Stop %q\n", uuid)
		tassert.CheckError(t, api.ETLStop(baseParams, uuid))
	}()

	fho, err := cmn.CreateFile(outputFileName)
	tassert.CheckFatal(t, err)
	defer fho.Close()

	tutils.Logf("Read %q\n", uuid)
	err = api.ETLObject(baseParams, uuid, bck, objName, fho)
	tassert.CheckFatal(t, err)

	tutils.Logln("Compare output")
	same, err := fEq(outputFileName, expectedOutputFileName)
	tassert.CheckError(t, err)
	tassert.Errorf(t, same, "file contents after transformation differ")
}

func testETLBucket(t *testing.T, uuid string, bckFrom cmn.Bck, objCnt int) {
	bckTo := cmn.Bck{Name: "etloffline-out-" + cmn.RandString(5), Provider: cmn.ProviderAIS}

	defer func() {
		tutils.Logf("Stop %q\n", uuid)
		tassert.CheckFatal(t, api.ETLStop(baseParams, uuid))
		tutils.DestroyBucket(t, proxyURL, bckTo)
	}()

	tutils.Logf("Start offline ETL %q\n", uuid)
	xactID, err := api.ETLBucket(baseParams, bckFrom, bckTo, &cmn.Bck2BckMsg{ID: uuid})
	tassert.CheckFatal(t, err)

	args := api.XactReqArgs{ID: xactID, Kind: cmn.ActETLBck, Timeout: time.Minute}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)

	list, err := api.ListObjects(baseParams, bckTo, nil, 0)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(list.Entries) == objCnt, "expected %d objects from offline ETL, got %d", objCnt, len(list.Entries))
}

func TestETLObject(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{K8s: true})
	tests := []testConfig{
		{transformer: tetl.Echo, comm: etl.RedirectCommType, onlyLong: true},
		{transformer: tetl.Echo, comm: etl.RevProxyCommType, onlyLong: true},
		{transformer: tetl.Echo, comm: etl.PushCommType, onlyLong: true},
		{tetl.Tar2TF, etl.RedirectCommType, tar2tfIn, tar2tfOut, tfDataEqual, true, false},
		{tetl.Tar2TF, etl.RevProxyCommType, tar2tfIn, tar2tfOut, tfDataEqual, true, false},
		{tetl.Tar2TF, etl.PushCommType, tar2tfIn, tar2tfOut, tfDataEqual, true, false},
		{tetl.Tar2tfFilters, etl.RedirectCommType, tar2tfFiltersIn, tar2tfFiltersOut, tfDataEqual, false, false},
		{tetl.Tar2tfFilters, etl.RevProxyCommType, tar2tfFiltersIn, tar2tfFiltersOut, tfDataEqual, false, false},
		{tetl.Tar2tfFilters, etl.PushCommType, tar2tfFiltersIn, tar2tfFiltersOut, tfDataEqual, false, false},

		// Cloud: only smoke tests to check if LOMs are loaded correctly.
		{transformer: tetl.Echo, comm: etl.RedirectCommType, cloud: true},
		{transformer: tetl.Echo, comm: etl.RevProxyCommType, cloud: true},
		{transformer: tetl.Echo, comm: etl.PushCommType, cloud: true},
	}

	for _, test := range tests {
		t.Run(test.Name(), func(t *testing.T) {
			testETLObject(t, test.onlyLong, test.cloud, test.comm, test.transformer, test.inPath, test.outPath, test.filesEqual)
		})
	}
}

func TestETLBucket(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{K8s: true})

	var (
		bck    = cmn.Bck{Name: "etloffline", Provider: cmn.ProviderAIS}
		objCnt = 10

		m = ioContext{
			t:         t,
			num:       objCnt,
			fileSize:  512,
			fixedSize: true,
			bck:       bck,
		}

		tests = []testConfig{
			{transformer: tetl.Echo, comm: etl.RedirectCommType, onlyLong: true},
			{transformer: tetl.Md5, comm: etl.RevProxyCommType},
			{transformer: tetl.Md5, comm: etl.PushCommType, onlyLong: true},
		}
	)

	tutils.Logln("Preparing source bucket")
	tutils.CreateFreshBucket(t, proxyURL, bck)
	m.init()

	tutils.Logln("Putting objects to source bucket")
	m.puts()

	for _, test := range tests {
		t.Run(test.Name(), func(t *testing.T) {
			tutils.CheckSkip(t, tutils.SkipTestArgs{K8s: true, Long: test.onlyLong})

			uuid, err := etlInit(test.transformer, test.comm)
			tassert.CheckFatal(t, err)

			testETLBucket(t, uuid, bck, objCnt)
		})
	}
}

func TestETLBuild(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{K8s: true})

	const (
		md5 = `
import hashlib

def transform(input_bytes):
    md5 = hashlib.md5()
    md5.update(input_bytes)
    return md5.hexdigest().encode()
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
		m = ioContext{
			t:         t,
			num:       10,
			fileSize:  512,
			fixedSize: true,
			bck:       cmn.Bck{Name: "etl_build", Provider: cmn.ProviderAIS},
		}

		tests = []struct {
			name     string
			code     string
			deps     string
			onlyLong bool
		}{
			{name: "simple", code: md5, deps: "", onlyLong: false},
			{name: "with_deps", code: numpy, deps: numpyDeps, onlyLong: true},
		}
	)

	tutils.Logln("Preparing source bucket")
	tutils.CreateFreshBucket(t, proxyURL, m.bck)

	m.init()

	tutils.Logln("Putting objects to source bucket")
	m.puts()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tutils.CheckSkip(t, tutils.SkipTestArgs{K8s: true, Long: test.onlyLong})

			uuid, err := api.ETLBuild(baseParams, etl.BuildMsg{
				Code:        []byte(test.code),
				Deps:        []byte(test.deps),
				Runtime:     runtime.Python3,
				WaitTimeout: cmn.DurationJSON(5 * time.Minute),
			})
			tassert.CheckFatal(t, err)

			testETLBucket(t, uuid, m.bck, m.num)
		})
	}
}

func TestETLBucketDryRun(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{K8s: true})
	var (
		bckFrom = cmn.Bck{Name: "etloffline", Provider: cmn.ProviderAIS}
		bckTo   = cmn.Bck{Name: "etloffline-out-" + cmn.RandString(5), Provider: cmn.ProviderAIS}
		objCnt  = 10

		m = ioContext{
			t:         t,
			num:       objCnt,
			fileSize:  512,
			fixedSize: true,
			bck:       bckFrom,
		}
	)

	tutils.Logln("Preparing source bucket")
	tutils.CreateFreshBucket(t, proxyURL, bckFrom)
	m.init()

	tutils.Logln("Putting objects to source bucket")
	m.puts()

	uuid, err := etlInit(tetl.Echo, etl.RevProxyCommType)
	tassert.CheckFatal(t, err)

	defer func() {
		tutils.Logf("Stop %q\n", uuid)
		tassert.CheckFatal(t, api.ETLStop(baseParams, uuid))
	}()

	tutils.Logf("Start offline ETL %q\n", uuid)
	xactID, err := api.ETLBucket(baseParams, bckFrom, bckTo,
		&cmn.Bck2BckMsg{ID: uuid, CopyBckMsg: cmn.CopyBckMsg{DryRun: true}})
	tassert.CheckFatal(t, err)

	args := api.XactReqArgs{ID: xactID, Timeout: time.Minute}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)

	exists, err := api.DoesBucketExist(baseParams, cmn.QueryBcks(bckTo))
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, exists == false, "expected destination bucket to not be created")

	stats, err := api.GetXactionStatsByID(baseParams, xactID)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, stats.ObjCount() == int64(objCnt), "dry run stats expected to return %d objects, got %d",
		objCnt, stats.ObjCount())
	expectedBytesCnt := int64(m.fileSize * uint64(objCnt))
	tassert.Errorf(t, stats.BytesCount() == expectedBytesCnt, "dry run stats expected to return %d bytes, got %d", expectedBytesCnt, stats.BytesCount())
}

func TestETLSingleTransformerAtATime(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{K8s: true, Long: true})
	output, err := exec.Command("bash", "-c", "kubectl get nodes | grep Ready | wc -l").CombinedOutput()
	tassert.CheckFatal(t, err)
	if strings.Trim(string(output), "\n") != "1" {
		t.Skip("Requires a single node kubernetes cluster")
	}

	if tutils.GetClusterMap(t, proxyURL).CountTargets() > 1 {
		t.Skip("Requires a single-node single-target deployment")
	}

	uuid1, err := etlInit("echo", "hrev://")
	tassert.CheckFatal(t, err)
	if uuid1 != "" {
		defer func() {
			tutils.Logf("Stop %q\n", uuid1)
			tassert.CheckFatal(t, api.ETLStop(baseParams, uuid1))
		}()
	}

	uuid2, err := etlInit("md5", "hrev://")
	tassert.Errorf(t, err != nil, "expected err to occur")
	if uuid2 != "" {
		tutils.Logf("Stop %q\n", uuid2)
		tassert.CheckFatal(t, api.ETLStop(baseParams, uuid2))
	}
}
