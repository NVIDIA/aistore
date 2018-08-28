/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

package dfc_test

import (
	"net/http"
	"testing"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/pkg/client"
)

func TestPutObjectNoDaemonID(t *testing.T) {
	const (
		bucket = TestLocalBucketName
		object = "someObject"
	)
	var sid string

	smap, err := client.GetClusterMap(proxyurl)
	checkFatal(err, t)

	for sid = range smap.Tmap {
		break
	}

	url := smap.Tmap[sid].PublicNet.DirectURL + api.URLPath(api.Version, api.Objects, bucket, object)
	if err = client.HTTPRequest(http.MethodPut, url, nil); err == nil {
		t.Errorf("Error is nil, expected Bad Request error on a PUT to target with no daemon ID query string")
	}
}
