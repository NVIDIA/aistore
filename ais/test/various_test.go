// Package integration_test.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

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
