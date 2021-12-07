// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"net/http"
	"testing"

	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tutils"
)

func TestInvalidHTTPMethod(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)

	req, err := http.NewRequest("TEST", proxyURL, http.NoBody)
	tassert.CheckFatal(t, err)
	tassert.DoAndCheckResp(t, tutils.HTTPClient, req, http.StatusBadRequest)
}
