// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
)

func TestXactionStartAbort(t *testing.T) {
	var (
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     cmn.RandString(10),
			Provider: cmn.ProviderAIS,
		}

		startableKinds = []string{cmn.ActLRU}
	)

	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	for _, kind := range startableKinds {
		// Abort if there is any xaction running with this kind.
		api.AbortXaction(baseParams, api.XactReqArgs{Kind: kind})
		time.Sleep(time.Second)

		id := tutils.StartXaction(t, kind, bck)
		tutils.AbortXaction(t, id, kind, bck)
	}
}

func TestXactionNotFound(t *testing.T) {
	var (
		proxyURL   = tutils.RandomProxyURL()
		baseParams = tutils.BaseAPIParams(proxyURL)

		missingID = "incorrect"
	)

	_, err := api.GetXactionStatsByID(baseParams, missingID)
	tutils.CheckErrIsNotFound(t, err)
}
