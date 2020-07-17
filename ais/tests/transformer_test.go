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
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/transform"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/readers"
	"github.com/NVIDIA/aistore/tutils/tassert"
	jsoniter "github.com/json-iterator/go"
)

func TestTransformer(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true, Kubernetes: true})
	tests := []testConfig{
		{"echo", "hpull://"},
		{"echo", "hrev://"},
		{"echo", "hpush://"},
	}
	for _, test := range tests {
		t.Run(test.Name(), func(t *testing.T) {
			testTransformer(t, test.comm, test.transformer)
		})
	}
}

func testTransformer(t *testing.T, comm, transformer string) {
	proxyURL := tutils.RandomProxyURL()
	defaultAPIParams := api.BaseParams{
		Client: http.DefaultClient,
		URL:    proxyURL,
	}
	t.Log("Creating bucket")
	bck := cmn.Bck{Name: "transfomer-test", Provider: cmn.ProviderAIS}
	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)
	testObjDir := filepath.Join("data", "transformer", transformer)

	t.Log("Putting object")
	objName := fmt.Sprintf("%s-object", transformer)
	reader, err := readers.NewFileReaderFromFile(filepath.Join(testObjDir, "object.in"), cmn.ChecksumNone)
	tassert.CheckFatal(t, err)
	errCh := make(chan error, 1)
	tutils.Put(proxyURL, bck, objName, reader, errCh)

	tassert.SelectErr(t, errCh, "put", false)

	t.Log("Reading template")
	transformerTemplate := filepath.Join("templates", "transformer", transformer, "pod.yaml")
	spec, err := ioutil.ReadFile(transformerTemplate)
	tassert.CheckError(t, err)

	pod, err := transform.ParsePodSpec(spec)
	tassert.CheckError(t, err)

	pod.Annotations["communication_type"] = comm
	spec, _ = jsoniter.Marshal(pod)

	t.Log("Init transform")
	uuid, err := api.TransformInit(defaultAPIParams, spec)
	tassert.CheckFatal(t, err)

	actualOutput := filepath.Join(os.TempDir(), objName+".out")
	fho, err := cmn.CreateFile(actualOutput)
	tassert.CheckFatal(t, err)
	defer cmn.RemoveFile(actualOutput)
	defer fho.Close()

	t.Log("Read transform")
	err = api.TransformObject(defaultAPIParams, uuid, bck, objName, fho)
	tassert.CheckFatal(t, err)

	t.Log("Compare transform output")
	expectedOutput := filepath.Join(testObjDir, "object.out")
	same, _ := tutils.FilesEqual(actualOutput, expectedOutput)
	if !same {
		t.Errorf("file contents after transformation differ")
	}
}

type testConfig struct {
	transformer string
	comm        string
}

func (tc testConfig) Name() string {
	return fmt.Sprintf("%s/%s", tc.transformer, strings.TrimSuffix(tc.comm, "://"))
}
