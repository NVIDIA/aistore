// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func TestMaintenanceOnOff(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)

	// Invalid target case
	err := api.Maintenance(baseParams, "fakeID", cmn.ActSuspend)
	tassert.Fatalf(t, err != nil, "Maintenance for invalid daemon ID succeeded")

	mntTarget := tutils.ExtractTargetNodes(smap)[0]
	baseParams := tutils.BaseAPIParams(proxyURL)
	err = api.Maintenance(baseParams, mntTarget.ID(), cmn.ActSuspend)
	tassert.CheckError(t, err)
	err = api.Maintenance(baseParams, mntTarget.ID(), cmn.ActUnsuspend)
	tassert.CheckError(t, err)
	err = api.Maintenance(baseParams, mntTarget.ID(), cmn.ActUnsuspend)
	tassert.Fatalf(t, err != nil, "Canceling maintenance must fail for 'normal' daemon")
}
