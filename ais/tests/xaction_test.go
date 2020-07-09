// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"net/http"
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func TestXactionStartAbort(t *testing.T) {
	var (
		bck = cmn.Bck{
			Name:     "TESTQUERYWORKERBUCKET",
			Provider: cmn.ProviderAIS,
		}

		proxyURL = tutils.RandomProxyURL()

		startableKinds = []string{cmn.ActLRU}
	)

	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	for _, kind := range startableKinds {
		id := tutils.StartXaction(t, kind, bck)
		tutils.AbortXaction(t, id, kind, bck)
	}
}

func TestXactionNotFound(t *testing.T) {
	var (
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)

		missingID = "incorrect"

		isNotFoundErr = func(err error) bool {
			if err == nil {
				return false
			}
			if httpEr, ok := err.(*cmn.HTTPError); ok {
				return httpEr.Status == http.StatusNotFound
			}
			return false
		}
	)

	_, err := api.GetXactionStatsByID(baseParams, missingID)
	tassert.Fatalf(t, isNotFoundErr(err), "Should Raise: XactionNotFoundError")
}
