// Package ais: internal unit tests
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools/tassert"
)

// newHTRunWithSmap creates a minimal htrun with a valid smap containing one proxy node.
func newHTRunWithSmap() *htrun {
	h := &htrun{}
	primaryID := "p-primary"
	h.si = newSnode(primaryID, apc.Proxy)

	config := cmn.GCO.BeginUpdate()
	cmn.GCO.CommitUpdate(config)

	h.owner.smap = newSmapOwner(config, false /*isTarget*/)
	smap := newSmap()
	smap.addProxy(h.si)
	smap.Primary = h.si
	h.owner.smap.put(smap)
	h.svs.init()
	return h
}

func newSnode(id, daeType string) (snode *meta.Snode) {
	snode = &meta.Snode{}
	snode.Init(id, daeType, nil /*verifying key*/)
	return
}

func TestSetIntraHdrs_ClearsRelayedSenderHdrs(t *testing.T) {
	spoofed := func() http.Header {
		hdr := http.Header{}
		hdr.Set(apc.HdrSenderID, "p-spoofed")
		hdr.Set(apc.HdrSenderName, "p[spoofed]")
		hdr.Set(apc.HdrSenderIsPrimary, "true")
		hdr.Set(apc.HdrSenderSmapVer, "99999")
		hdr.Set(apc.HdrSenderNonce, "42")
		hdr.Set(apc.HdrSenderSig, "not-a-signature")
		return hdr
	}

	// self is NOT primary; signing disabled and grace expired (svState.init),
	// so signIntra is skipped - exactly the window where a relayed signature would survive
	h := newHTRunWithSmap()
	tassert.Fatalf(t, !h.svs.sign(), "expecting sign() == false")

	smap := h.owner.smap.get().clone()
	other := newSnode("p-other", apc.Proxy)
	smap.addProxy(other)
	smap.Primary = other
	smap.Version++
	h.owner.smap.put(smap)
	smap = h.owner.smap.get()

	req := &http.Request{Header: spoofed()}
	h.setIntraHdrs(req, smap, true /*peer present*/)

	for _, tc := range []struct{ hdr, expected string }{
		{apc.HdrSenderID, h.SID()},
		{apc.HdrSenderName, h.si.Name()},
		{apc.HdrSenderIsPrimary, ""},
		{apc.HdrSenderSmapVer, smap.vstr},
		{apc.HdrSenderNonce, ""},
		{apc.HdrSenderSig, ""},
	} {
		got := req.Header.Get(tc.hdr)
		tassert.Errorf(t, got == tc.expected, "%s: got %q, expected %q", tc.hdr, got, tc.expected)
	}

	// and the legitimate primary case is not broken by the above
	hp := newHTRunWithSmap()
	reqp := &http.Request{Header: spoofed()}
	hp.setIntraHdrs(reqp, hp.owner.smap.get(), true /*peer present*/)
	got := reqp.Header.Get(apc.HdrSenderIsPrimary)
	tassert.Errorf(t, got == "true", "%s: got %q, expected %q", apc.HdrSenderIsPrimary, got, "true")
}

func TestRetryJoin(t *testing.T) {
	var (
		req  = httptest.NewRequest(http.MethodPost, "/", http.NoBody)
		errs = map[string]error{
			"unres": newErrJoinUnresolved(req, errors.New("unresolved")),
			"rej":   newErrJoinRejected(req, errors.New("rejected")),
			"fail":  errors.New("unreachable"),
		}
		unres = []string{"unres", "unres", "unres", "unres"} // joinRounds
	)
	// attempts are instant: only the cases with a non-zero age ever reach the timeout
	const tout = time.Second
	for _, tc := range []struct {
		name   string
		urls   []string
		script map[string][]string // per candidate; the last step repeats; "ok" succeeds
		age    time.Duration       // time already spent before the first attempt
		calls  int
		want   string // kind of the returned result; "" for none
	}{
		{"succeeds at once", []string{"a"}, map[string][]string{"a": {"ok"}}, 0, 1, "ok"},
		{"rejection stops at once", []string{"a"}, map[string][]string{"a": {"fail", "rej"}}, 0, 2, "rej"},
		{"other failures stop after fixed rounds", []string{"a"}, map[string][]string{"a": {"fail"}}, 0, joinRounds, "fail"},
		{"fixed rounds try every candidate", []string{"a", "b"}, map[string][]string{"a": {"fail"}, "b": {"fail"}}, 0, 2 * joinRounds, "fail"},
		{"no candidates", nil, nil, 0, 0, ""},
		{
			"unresolved retries until success", []string{"a"},
			map[string][]string{"a": append(slices.Clone(unres), "unres", "unres", "ok")}, 0, joinRounds + 3, "ok",
		},
		{"unresolved after timeout gets fixed rounds only", []string{"a"}, map[string][]string{"a": {"unres"}}, time.Hour, joinRounds, "unres"},
		{
			"retries stop once failures change", []string{"a"},
			map[string][]string{"a": append(slices.Clone(unres), "fail")}, 0, joinRounds + 1, "fail",
		},
		{
			"unresolved from any candidate extends the round", []string{"a", "b"},
			map[string][]string{"a": {"fail"}, "b": append(slices.Clone(unres), "ok")}, 0, 2 * (joinRounds + 1), "ok",
		},
	} {
		var (
			calls   = map[string]int{}
			total   int
			started = mono.NanoTime() - int64(tc.age)
		)
		try := func(url string) *callResult {
			steps := tc.script[url]
			kind := steps[min(calls[url], len(steps)-1)]
			calls[url]++
			total++
			res := allocCR()
			res.err = errs[kind] // nil for "ok"
			return res
		}
		res := retryJoin(tc.urls, started, tout, 0 /*sleep*/, try)

		got := ""
		if res != nil {
			got = "ok"
			for kind, err := range errs {
				if res.err == err {
					got = kind
				}
			}
			freeCR(res)
		}
		tassert.Errorf(t, got == tc.want, "%s: got result %q, expected %q", tc.name, got, tc.want)
		tassert.Errorf(t, total == tc.calls, "%s: got %d calls, expected %d", tc.name, total, tc.calls)
	}
}
