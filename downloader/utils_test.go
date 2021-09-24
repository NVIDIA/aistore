// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package downloader_test

import (
	"testing"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/fs"
)

func TestNormalizeObjName(t *testing.T) {
	normalizeObjTests := []struct {
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
		actual, err := downloader.NormalizeObjName(test.objName)
		if err != nil {
			t.Errorf("Unexpected error while normalizing %s: %v", test.objName, err)
		}

		if actual != test.expected {
			t.Errorf("NormalizeObjName(%s) expected: %s, got: %s", test.objName, test.expected, actual)
		}
	}
}

func TestCompareObject(t *testing.T) {
	var (
		src = prepareObject(t)
		dst = &downloader.DstElement{
			Link: "https://storage.googleapis.com/minikube/iso/minikube-v0.23.2.iso.sha256",
		}
	)

	// Modify local object to contain invalid (meta)data.
	customMD := cos.SimpleKVs{
		cmn.SourceObjMD:  cmn.ProviderAmazon,
		cmn.VersionObjMD: "none",
		cmn.CRC32CObjMD:  "bad",
		cmn.MD5ObjMD:     "worse",
	}
	src.SetSize(10)
	src.SetCustomMD(customMD)
	equal, err := downloader.CompareObjects(src, dst)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, !equal, "expected the objects not to be equal")

	// Check that objects are still not equal after size update.
	src.SetSize(65)
	equal, err = downloader.CompareObjects(src, dst)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, !equal, "expected the objects not to be equal")

	// Check that correct CRC doesn't make them equal
	customMD[cmn.CRC32CObjMD] = "30a991bd"
	equal, err = downloader.CompareObjects(src, dst)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, !equal, "expected the objects not to be equal")

	// Check that the same provider still doesn't make them equal
	customMD[cmn.SourceObjMD] = cmn.ProviderGoogle
	equal, err = downloader.CompareObjects(src, dst)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, !equal, "expected the objects not to be equal")

	// Finally, check that objects are equal when they have the same (provider, version)
	src.SetCustomKey(cmn.VersionObjMD, "1503349750687573")
	src.SetCustomKey(cmn.MD5ObjMD, "7b01d3eacc5869db6eb9137f15335d27")
	customMD[cmn.VersionObjMD] = "1503349750687573"
	equal, err = downloader.CompareObjects(src, dst)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, equal, "expected the objects to be equal")
}

func prepareObject(t *testing.T) *cluster.LOM {
	out := tutils.PrepareObjects(t, tutils.ObjectsDesc{
		CTs: []tutils.ContentTypeDesc{{
			Type:       fs.ObjectType,
			ContentCnt: 1,
		}},
		MountpathsCnt: 1,
		ObjectSize:    1024,
	})
	lom := &cluster.LOM{FQN: out.FQNs[fs.ObjectType][0]}
	err := lom.Init(out.Bck)
	tassert.CheckFatal(t, err)
	err = lom.Load(false, false)
	tassert.CheckFatal(t, err)
	return lom
}
