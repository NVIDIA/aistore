/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

package dfc_test

import (
	"net/http"
	"testing"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/tutils"
)

func TestPutObjectNoDaemonID(t *testing.T) {
	const (
		bucket  = TestLocalBucketName
		objName = "someObject"
	)
	var (
		sid          string
		proxyURL     = getPrimaryURL(t, proxyURLRO)
		objDummyData = []byte("testing is so much fun")
	)

	smap, err := api.GetClusterMap(tutils.HTTPClient, proxyURL)
	tutils.CheckFatal(err, t)

	for sid = range smap.Tmap {
		break
	}

	url := smap.Tmap[sid].URL(cmn.NetworkPublic)
	reader := tutils.NewBytesReader(objDummyData)
	if err := api.PutObject(tutils.HTTPClient, url, bucket, objName, reader.XXHash(), reader); err == nil {
		t.Errorf("Error is nil, expected Bad Request error on a PUT to target with no daemon ID query string")
	}
}

func TestDeleteInvalidDaemonID(t *testing.T) {
	var (
		sid      = "abcde:abcde"
		proxyURL = getPrimaryURL(t, proxyURLRO)
	)

	url := proxyURL + cmn.URLPath(cmn.Version, cmn.Cluster, cmn.Daemon, sid)
	if err := tutils.HTTPRequest(http.MethodDelete, url, nil); err == nil {
		t.Errorf("Error is nil, expected NotFound error on a delete of a non-existing target")
	}
}
