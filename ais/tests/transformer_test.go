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

func TestKubeTransformer(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Kubernetes: true})
	tests := []testConfig{
		{"echo", "hpull://", "", "", nil, false},
		{"echo", "hrev://", "", "", nil, false},
		{"echo", "hpush://", "", "", nil, false},
		{cmn.Tar2Tf, "", "data/small-mnist-3.tar", "data/small-mnist-3.record", tfRecordsEqual, true},
	}
	for _, test := range tests {
		t.Run(test.Name(), func(t *testing.T) {
			testTransformer(t, test.comm, test.transformer, test.inPath, test.outPath, test.filesEqual, test.isStatic)
		})
	}
}

func testTransformer(t *testing.T, comm, transformer, inPath, outPath string, fEq filesEqualFunc, isStatic bool) {
	if !isStatic {
		// Static transformers don't need init, so no new pods are started and tests are fast.
		tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	}

	var (
		proxyURL         = tutils.RandomProxyURL()
		defaultAPIParams = api.BaseParams{Client: http.DefaultClient, URL: proxyURL}
		bck              = cmn.Bck{Name: "transfomer-test", Provider: cmn.ProviderAIS}
		testObjDir       = filepath.Join("data", "transformer", transformer)
		objName          = fmt.Sprintf("%s-object", transformer)

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

	if isStatic {
		uuid = transformer
	} else {
		// Static transformer doesn't need Init()
		t.Log("Reading template")
		transformerTemplate := filepath.Join("templates", "transformer", transformer, "pod.yaml")
		spec, err := ioutil.ReadFile(transformerTemplate)
		tassert.CheckError(t, err)

		pod, err := transform.ParsePodSpec(spec)
		tassert.CheckError(t, err)

		if comm != "" {
			pod.Annotations["communication_type"] = comm
		}
		spec, _ = jsoniter.Marshal(pod)

		t.Log("Init transform")
		uuid, err = api.TransformInit(defaultAPIParams, spec)
		tassert.CheckFatal(t, err)
		defer api.TransformStop(defaultAPIParams, uuid)
	}
	fho, err := cmn.CreateFile(outputFileName)
	tassert.CheckFatal(t, err)
	defer func() {
		fho.Close()
		cmn.RemoveFile(outputFileName)
	}()

	t.Log("Read transform")
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
		isStatic    bool
	}
)

func (tc testConfig) Name() string {
	return fmt.Sprintf("%s/%s", tc.transformer, strings.TrimSuffix(tc.comm, "://"))
}

// TODO: this should be a part of go-tfdata
// This function is necessary, as the same TFRecords can be different byte-wise.
// This is caused by the fact that order of TFExamples is can de different,
// as well as ordering of elements of a single TFExample can be different.
func tfRecordsEqual(n1, n2 string) (bool, error) {
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
