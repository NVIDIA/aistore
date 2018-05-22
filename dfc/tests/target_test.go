/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

package dfc_test

import (
	"net/http"
	"testing"

	"github.com/NVIDIA/dfcpub/dfc"
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

	url := smap.Tmap[sid].DirectURL + "/" + dfc.Rversion + "/" + dfc.Robjects + "/" + bucket + "/" + object
	err = client.HTTPRequest(http.MethodPut, url, nil)
	if err == nil {
		t.Errorf("Error is nil, expected Bad Request error on a PUT to target with no daemon ID query string")
	}
}
