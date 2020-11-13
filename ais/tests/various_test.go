// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"net/http"
	"testing"

	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/devtools/tutils/tassert"
)

func TestInvalidHTTPMethod(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)

	req, err := http.NewRequest("TEST", proxyURL, nil)
	tassert.CheckFatal(t, err)
	resp, err := tutils.HTTPClient.Do(req)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, resp.StatusCode == http.StatusBadRequest, "expected %d status code", http.StatusBadRequest)
	resp.Body.Close()
}
