// Package integration contains AIStore integration tests.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"os"
	"strings"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
)

func TestX509Integration(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiresTLS: true})

	serverCrt := os.Getenv("AIS_SERVER_CRT")
	serverKey := os.Getenv("AIS_SERVER_KEY")
	tassert.Fatalf(t, serverCrt != "", "AIS_SERVER_CRT environment variable should be set")
	tassert.Fatalf(t, serverKey != "", "AIS_SERVER_KEY environment variable should be set")

	baseParams := tools.BaseAPIParams(tools.RandomProxyURL(t))
	err := api.LoadX509Cert(baseParams)
	tassert.CheckFatal(t, err)

	apiInfo, err := api.GetX509Info(baseParams)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(apiInfo) > 0, "Expected certificate info from API")

	validateCertificateResults(t, apiInfo)
}

func validateCertificateResults(t *testing.T, apiInfo cos.StrKVs) {
	expectedFields := []string{"issued-by (CN)", "serial-number", "valid"}
	tassert.Fatalf(t, apiInfo.ContainsAnyMatch(expectedFields) != "", "Expected API to return certificate fields")

	if cnFromAPI, exists := apiInfo["issued-by (CN)"]; exists {
		tassert.Fatalf(t, strings.Contains(cnFromAPI, "localhost"),
			"Certificate CN should contain 'localhost', got: %s", cnFromAPI)
		tlog.Logf("Certificate CN validated: %s", cnFromAPI)
	}
}
