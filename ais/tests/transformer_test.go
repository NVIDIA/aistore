// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */

package integration

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/transform"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/readers"
	"github.com/NVIDIA/aistore/tutils/tassert"
	"github.com/NVIDIA/go-tfdata/tfdata/core"
	jsoniter "github.com/json-iterator/go"
)

const (
	tar2tf    = "tar2tf"
	tar2tfIn  = "data/small-mnist-3.tar"
	tar2tfOut = "data/small-mnist-3.record"

	tar2tfFilters    = "tar2tf-filters"
	tar2tfFiltersIn  = "data/single-png-cls.tar"
	tar2tfFiltersOut = "data/single-png-cls-transformed.tfrecord"
)

var (
	defaultAPIParams = api.BaseParams{Client: http.DefaultClient, URL: proxyURL}
)

func TestKubeTransformer(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Kubernetes: true})
	tests := []testConfig{
		{transformer: "echo", comm: transform.RedirectCommType},
		{transformer: "echo", comm: transform.RevProxyCommType},
		{transformer: "echo", comm: transform.PushCommType},
		{tar2tf, transform.RedirectCommType, tar2tfIn, tar2tfOut, tfDataEqual},
		{tar2tf, transform.RevProxyCommType, tar2tfIn, tar2tfOut, tfDataEqual},
		{tar2tf, transform.PushCommType, tar2tfIn, tar2tfOut, tfDataEqual},
		{tar2tfFilters, transform.RedirectCommType, tar2tfFiltersIn, tar2tfFiltersOut, tfDataEqual},
		{tar2tfFilters, transform.RevProxyCommType, tar2tfFiltersIn, tar2tfFiltersOut, tfDataEqual},
		{tar2tfFilters, transform.PushCommType, tar2tfFiltersIn, tar2tfFiltersOut, tfDataEqual},
	}

	for _, test := range tests {
		t.Run(test.Name(), func(t *testing.T) {
			testTransformer(t, test.comm, test.transformer, test.inPath, test.outPath, test.filesEqual)
		})
	}
}

func transformInit(t *testing.T, name, comm string) (string, error) {
	t.Log("Reading template")
	transformerTemplate := filepath.Join("templates", "transformer", name, "pod.yaml")
	spec, err := ioutil.ReadFile(transformerTemplate)
	if err != nil {
		return "", err
	}
	pod, err := transform.ParsePodSpec(spec)
	if err != nil {
		return "", err
	}
	if comm != "" {
		pod.Annotations["communication_type"] = comm
	}
	spec, _ = jsoniter.Marshal(pod)
	t.Log("Init transform")
	return api.TransformInit(defaultAPIParams, spec)
}

func testTransformer(t *testing.T, comm, transformer, inPath, outPath string, fEq filesEqualFunc) {
	var (
		bck        = cmn.Bck{Name: "transfomer-test", Provider: cmn.ProviderAIS}
		testObjDir = filepath.Join("data", "transformer", transformer)
		objName    = fmt.Sprintf("%s-object", transformer)

		inputFileName          = filepath.Join(testObjDir, "object.in")
		expectedOutputFileName = filepath.Join(testObjDir, "object.out")
		outputFileName         = filepath.Join(os.TempDir(), objName+".out")

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

	t.Log("Creating bucket")
	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	t.Log("Putting object")
	reader, err := readers.NewFileReaderFromFile(inputFileName, cmn.ChecksumNone)
	tassert.CheckFatal(t, err)
	errCh := make(chan error, 1)
	tutils.Put(proxyURL, bck, objName, reader, errCh)
	tassert.SelectErr(t, errCh, "put", false)
	uuid, err = transformInit(t, transformer, comm)
	tassert.CheckFatal(t, err)
	defer func() {
		t.Logf("Stop transform %q", uuid)
		tassert.CheckFatal(t, api.TransformStop(defaultAPIParams, uuid))
	}()

	fho, err := cmn.CreateFile(outputFileName)
	tassert.CheckFatal(t, err)
	defer func() {
		fho.Close()
		cmn.RemoveFile(outputFileName)
	}()

	t.Logf("Read transform %q", uuid)
	err = api.TransformObject(defaultAPIParams, uuid, bck, objName, fho)
	tassert.CheckFatal(t, err)

	t.Log("Compare transform output")
	same, err := fEq(outputFileName, expectedOutputFileName)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, same, "file contents after transformation differ")
}

type (
	filesEqualFunc func(f1, f2 string) (bool, error)

	testConfig struct {
		transformer string
		comm        string
		inPath      string         // optional
		outPath     string         // optional
		filesEqual  filesEqualFunc // optional
	}
)

func (tc testConfig) Name() string {
	return fmt.Sprintf("%s/%s", tc.transformer, strings.TrimSuffix(tc.comm, "://"))
}

// TODO: this should be a part of go-tfdata
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

func TestKubeSingleTransformerAtATime(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Kubernetes: true})
	output, err := exec.Command("bash", "-c", "kubectl get nodes | grep Ready | wc -l").CombinedOutput()
	tassert.CheckFatal(t, err)
	if strings.Trim(string(output), "\n") != "1" {
		t.Skip("requires a single node kubernetes cluster")
	}
	uuid1, err := transformInit(t, "echo", "hrev://")
	tassert.CheckFatal(t, err)
	if uuid1 != "" {
		defer func() {
			t.Logf("Stop transform %q", uuid1)
			tassert.CheckFatal(t, api.TransformStop(defaultAPIParams, uuid1))
		}()
	}
	uuid2, err := transformInit(t, "md5", "hrev://")
	tassert.Fatalf(t, err != nil, "Expected err!=nil, got %v", err)
	if uuid2 != "" {
		t.Logf("Stop transform %q", uuid2)
		tassert.CheckFatal(t, api.TransformStop(defaultAPIParams, uuid2))
	}
}
