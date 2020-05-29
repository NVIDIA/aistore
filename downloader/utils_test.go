// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"io"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func TestNormalizeObjName(t *testing.T) {
	var normalizeObjTests = []struct {
		objName  string
		expected string
	}{
		{"?objname", "?objname"},
		{"filename", "filename"},
		{"dir/file", "dir/file"},
		{"dir%2Ffile", "dir/file"},
		{"path1/path2/path3%2Fpath4%2Ffile", "path1/path2/path3/path4/file"},
		{"file?arg1=a&arg2=b", "file"},
		{"imagenet%2Fimagenet_train-000000.tgz?alt=media", "imagenet/imagenet_train-000000.tgz"},
	}

	for _, test := range normalizeObjTests {
		actual, err := normalizeObjName(test.objName)

		if err != nil {
			t.Errorf("Unexpected error while normalizing %s: %v", test.objName, err)
		}

		if actual != test.expected {
			t.Errorf("normalizeObjName(%s) expected: %s, got: %s", test.objName, test.expected, actual)
		}
	}
}

func TestCompareObject(t *testing.T) {
	var (
		err error
		obj = dlObj{
			link: "https://storage.googleapis.com/minikube/iso/minikube-v0.23.2.iso.sha256",
		}
		lom = &cluster.LOM{
			T: cluster.NewTargetMock(nil),
		}
	)

	lom.FQN, err = downloadObject(obj.link)
	tassert.CheckFatal(t, err)

	// Modify local object to contain invalid (meta)data.
	customMD := cmn.SimpleKVs{
		cluster.SourceObjMD:        cluster.SourceGoogleObjMD,
		cluster.GoogleCRC32CObjMD:  "bad",
		cluster.GoogleVersionObjMD: "version",
	}
	lom.SetSize(10)
	lom.SetCustomMD(customMD)

	equal, err := compareObjects(nil, obj, lom)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, !equal, "expected the objects not to be equal")

	// Check that objects are still not equal after size update.
	lom.SetSize(65)
	equal, err = compareObjects(nil, obj, lom)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, !equal, "expected the objects not to be equal")

	// Check that objects are still not equal after version update.
	customMD[cluster.GoogleVersionObjMD] = "1503349750687573"
	equal, err = compareObjects(nil, obj, lom)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, !equal, "expected the objects not to be equal")

	// Finally, check if the objects are equal once we set all the metadata correctly.
	customMD[cluster.GoogleCRC32CObjMD] = "30a991bd"
	equal, err = compareObjects(nil, obj, lom)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, equal, "expected the objects to be equal")
}

func downloadObject(link string) (string, error) {
	resp, err := http.Get(link)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	f, err := ioutil.TempFile("", "")
	if err != nil {
		return "", err
	}
	defer f.Close()

	_, err = io.Copy(f, resp.Body)
	return f.Name(), err
}
