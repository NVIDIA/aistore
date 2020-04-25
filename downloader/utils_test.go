// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"testing"

	"github.com/NVIDIA/aistore/cluster"
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

func TestCompareObjectNotEqualSizes(t *testing.T) {
	var (
		obj = dlObj{
			link: "https://storage.googleapis.com/lpr-vision/imagenet256/imagenet256_train-000105.tgz",
		}
		lom = &cluster.LOM{}
	)

	lom.SetSize(10)

	equal, err := compareObjects(obj, lom)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, !equal, "expected the objects to be equal")
}
