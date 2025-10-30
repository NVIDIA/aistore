// Package integration_test.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"strings"
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

func _string(vec nl.StatusVec) string {
	var sb strings.Builder
	for _, ns := range vec {
		sb.WriteString(ns.String())
		sb.WriteString(", ")
	}
	s := sb.String()
	return s[:max(0, len(s)-2)]
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
			xargs := xact.ArgsMsg{Kind: kind, OnlyRunning: test.running, Force: test.force}
			if mono.NanoTime()&0x1 == 0x1 {
				_, xname := xact.GetKindName(kind)
				xargs.Kind = xname
			}
			vec, err := api.GetAllXactionStatus(baseParams, &xargs)
			tassert.CheckFatal(t, err)
			if len(vec) == 0 {
				continue
			}
			if kind != apc.ActList {
				tlog.Logln(_string(vec))
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
					tlog.Logfln("%q is aborted but hasn't finished yet", ns.String())
					aborted = append(aborted, ns)
				} else if ns.EndTimeX != 0 {
					tlog.Logfln("Warning: must've %q already finished (non-zero fin time=%v)",
						ns.String(), time.Unix(0, ns.EndTimeX))

					// un-race
					xargs.OnlyRunning = true
					vec2, err2 := api.GetAllXactionStatus(baseParams, &xargs)
					tassert.CheckFatal(t, err2)
					for _, ns2 := range vec2 {
						if ns2.UUID == ns.UUID {
							tassert.Errorf(t, ns.EndTimeX == 0, "%q (1) already finished (non-zero fin time=%v)",
								ns.String(), time.Unix(0, ns.EndTimeX))
							tassert.Errorf(t, ns2.EndTimeX == 0, "%q (2) already finished (non-zero fin time=%v)",
								ns2.String(), time.Unix(0, ns2.EndTimeX))
						}
					}
				}
			}
			if len(aborted) == 0 {
				continue
			}

			// re-check after a while
			time.Sleep(2 * time.Second)

			xargs = xact.ArgsMsg{Kind: kind, OnlyRunning: false, Force: test.force}
			vec, err = api.GetAllXactionStatus(baseParams, &xargs)
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
