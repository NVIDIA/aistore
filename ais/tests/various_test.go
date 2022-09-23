// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"net/http"
	"testing"

	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestInvalidHTTPMethod(t *testing.T) {
	proxyURL := tools.RandomProxyURL(t)

	req, err := http.NewRequest("TEST", proxyURL, http.NoBody)
	tassert.CheckFatal(t, err)
	tassert.DoAndCheckResp(t, tools.HTTPClient, req, http.StatusBadRequest)
}
