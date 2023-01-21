// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/xact"
)

func TestXactionNotFound(t *testing.T) {
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)
	_, err := api.QueryXactionSnaps(baseParams, api.XactReqArgs{ID: "dummy-" + cos.GenUUID()})
	tools.CheckErrIsNotFound(t, err)
}

func TestXactionAllStatus(t *testing.T) {
	tests := []struct {
		running bool
		force   bool
	}{
		{running: false, force: false},
		{running: true, force: false},
		{running: false, force: true},
		{running: true, force: true},
	}
	for _, test := range tests {
		for kind := range xact.Table {
			xactArgs := api.XactReqArgs{Kind: kind, OnlyRunning: test.running}
			if mono.NanoTime()&0x1 == 0x1 {
				_, xname := xact.GetKindName(kind)
				xactArgs.Kind = xname
			}
			vec, err := api.GetAllXactionStatus(baseParams, xactArgs, test.force)
			tassert.CheckFatal(t, err)
			if len(vec) == 0 {
				continue
			}
			tlog.Logln(vec.String())
			for _, ns := range vec {
				tassert.Errorf(t, ns.Kind == kind, "kind %q vs %q", ns.Kind, kind)
			}
			if !test.running {
				continue
			}
			for _, ns := range vec {
				tassert.Errorf(t, ns.FinTime == 0, "%q expected to be running, got fintime=%v",
					ns.String(), time.Unix(0, ns.FinTime))
			}
		}
	}
}
