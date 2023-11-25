// Package integration_test.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/nl"
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
	_, err := api.QueryXactionSnaps(baseParams, &xact.ArgsMsg{ID: "dummy-" + cos.GenUUID()})
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
			xargs := xact.ArgsMsg{Kind: kind, OnlyRunning: test.running}
			if mono.NanoTime()&0x1 == 0x1 {
				_, xname := xact.GetKindName(kind)
				xargs.Kind = xname
			}
			vec, err := api.GetAllXactionStatus(baseParams, &xargs, test.force)
			tassert.CheckFatal(t, err)
			if len(vec) == 0 {
				continue
			}
			if kind != apc.ActList {
				tlog.Logln(vec.String())
			}
			for _, ns := range vec {
				tassert.Errorf(t, ns.Kind == kind, "kind %q vs %q", ns.Kind, kind)
			}
			if !test.running {
				continue
			}

			// check fin time for all running
			var aborted nl.StatusVec
			for _, ns := range vec {
				if ns.AbortedX {
					tlog.Logf("%q is aborted but hasn't finished yet\n", ns.String())
					aborted = append(aborted, ns)
				} else {
					// doesn't appear to be aborted and is, therefore, expected to be running
					tassert.Errorf(t, ns.EndTimeX == 0, "%q: non-sero fin time=%v",
						ns.String(), time.Unix(0, ns.EndTimeX))
				}
			}
			if len(aborted) == 0 {
				continue
			}

			// re-check after a while
			time.Sleep(2 * time.Second)

			xargs = xact.ArgsMsg{Kind: kind, OnlyRunning: false}
			vec, err = api.GetAllXactionStatus(baseParams, &xargs, test.force)
			tassert.CheckFatal(t, err)
			for _, a := range aborted {
				found := false
				for _, ns := range vec {
					if a.UUID == ns.UUID {
						found = true
						tassert.Errorf(t, ns.EndTimeX != 0, "%q: was aborted, is expected to be finished",
							ns.String())
						break
					}
				}
				tassert.Errorf(t, found,
					"%q: was aborted, is missing in the all-xaction-status results", a.String())
			}
		}
	}
}
