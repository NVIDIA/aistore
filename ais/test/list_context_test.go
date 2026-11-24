// Package integration_test.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/xact"

	"github.com/tinylib/msgp/msgp"
)

//
// mock remote AIS server shared by inventory, prefetch, summary, copy, and CLI tests
// exercises remote LIST cancellation on abort and client timeout
//

const (
	lsoCtxStress    = 3
	lsoCtxRetryWait = 4 * time.Second
)

// fake remote AIS behavior, per bucket
const (
	lsoCtxBlock         = iota // respond 200, then block until canceled
	lsoCtxRetry                // 429
	lsoCtxEmpty                // empty pages, no last page
	lsoCtxSparse               // empty pages, then a single-entry last page
	lsoCtxEndless              // full sorted pages, no last page
	lsoCtxPageError            // fail the second call once
	lsoCtxPageTimeout          // return the first page, then block
	lsoCtxLastPage             // empty final page
	lsoCtxFinalPages           // empty pages, then an empty final page
	lsoCtxListFailure          // empty first page, then fail
	lsoCtxSlowPrefetch         // repeated pages, object GETs block until canceled
	lsoCtxPrefetchPages        // one object per page
	lsoCtxRetryOnce            // first LIST returns 429
)

type (
	lsoCtxBck struct {
		bck             cmn.Bck
		mode            int
		calls, canceled atomic.Int64
		gets            atomic.Int64
		ready           chan struct{}
		entries         cmn.LsoEntries
	}
	lsoCtxRemote struct {
		srv        *httptest.Server
		bcks       map[string]*lsoCtxBck // by bucket name
		smap       meta.Smap
		mu         sync.Mutex
		handshakes atomic.Int64
	}
)

func TestLsoRemotePageFailure(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal})
	bp := tools.BaseAPIParams()
	nat := tools.GetClusterMap(t, tools.GetPrimaryURL()).CountActiveTs()
	remote := attachLsoCtxRemote(t, bp, nat)

	for _, name := range []string{"remote-only", "single-target"} {
		t.Run(name, func(t *testing.T) {
			msg := &apc.LsoMsg{PageSize: 1}
			if name == "single-target" {
				tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 1, MaxTargets: 1})
				msg.AddProps(apc.GetPropsDefaultAIS...)
			} else {
				msg.SetFlag(apc.LsNameOnly)
			}
			b := remote.addBck(t, bp, "fail-page", lsoCtxPageError)

			page, err := api.ListObjectsPage(bp, b.bck, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, len(page.Entries) == 1 && page.Entries[0].Name == "first" && msg.ContinuationToken != "",
				"unexpected first page: %+v", page)
			_, err = api.ListObjectsPage(bp, b.bck, msg, api.ListArgs{})
			tassert.Fatalf(t, err != nil && strings.Contains(err.Error(), "injected page error"), "expected page error, got %v", err)

			page, err = api.ListObjectsPage(bp, b.bck, msg, api.ListArgs{})
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, len(page.Entries) == 1 && page.Entries[0].Name == "second" && msg.ContinuationToken == "",
				"retry did not return the last page: %+v", page)
			tassert.Fatalf(t, b.calls.Load() == 3, "expected three backend LISTs, got %d", b.calls.Load())
		})
	}
}

func TestLsoRemotePageTimeoutAbortsDesignated(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal, MinTargets: 2})
	bp := tools.BaseAPIParams()
	smap := tools.GetClusterMap(t, tools.GetPrimaryURL())
	remote := attachLsoCtxRemote(t, bp, smap.CountActiveTs())
	b := remote.addBck(t, bp, "block-lso", lsoCtxPageTimeout)

	config := tools.GetClusterConfig(t)
	t.Cleanup(func() {
		tools.SetClusterConfig(t, cos.StrKVs{"client.list_timeout": config.Client.ListObjTimeout.String()})
	})
	tools.SetClusterConfig(t, cos.StrKVs{"client.list_timeout": "2s"})

	msg := &apc.LsoMsg{PageSize: 1}
	msg.AddProps(apc.GetPropsDefaultAIS...)
	_, err := api.ListObjectsPage(bp, b.bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, msg.ContinuationToken != "", "expected a second page")
	dt, err := smap.HrwTargetTask(msg.UUID)
	tassert.CheckFatal(t, err)

	_, err = api.ListObjectsPage(bp, b.bck, msg, api.ListArgs{})
	tassert.Fatalf(t, err != nil && (strings.Contains(err.Error(), "deadline exceeded") ||
		strings.Contains(err.Error(), "timed out waiting")), "expected page timeout, got %v", err)
	waitLsoCtxCount(t, &b.canceled, 1)
	tassert.Fatalf(t, b.calls.Load() == 2, "expected two backend LISTs, got %d", b.calls.Load())

	args := &xact.ArgsMsg{ID: msg.UUID, Kind: apc.ActList, DaemonID: dt.ID()}
	snaps, err := api.QueryXactionSnaps(bp, args)
	tassert.CheckFatal(t, err)
	all := snaps[dt.ID()]
	tassert.Fatalf(t, len(all) == 1 && all[0].IsAborted() && strings.Contains(all[0].AbortErr, "2pc abort"),
		"expected designated target %s to receive the proxy abort: %+v", dt, all)
}

func TestLsoContext(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal, MinTargets: 2})
	var (
		bp     = tools.BaseAPIParams()
		nat    = tools.GetClusterMap(t, tools.GetPrimaryURL()).CountActiveTs()
		remote = attachLsoCtxRemote(t, bp, nat)
	)

	t.Run("backend-retry-wait", func(t *testing.T) {
		b := remote.addBck(t, bp, "retry", lsoCtxRetry)
		_, err := api.SetBucketProps(bp, b.bck, &cmn.BpropsToSet{RateLimit: &cmn.RateLimitConfToSet{
			Backend: &cmn.AdaptiveToSet{RateLimitBaseToSet: cmn.RateLimitBaseToSet{
				Enabled: apc.Ptr(false), MaxTokens: apc.Ptr(2 * nat), Interval: apc.Ptr(cos.Duration(lsoCtxRetryWait)),
			}, OnDemand: apc.Ptr(true), NumRetries: apc.Ptr(1)},
		}})
		tassert.CheckFatal(t, err)
		xid, err := api.Prefetch(bp, b.bck, &apc.PrefetchMsg{})
		tassert.CheckFatal(t, err)
		waitLsoCtxCount(t, &b.calls, 1)
		tassert.CheckFatal(t, api.AbortXaction(bp, &xact.ArgsMsg{ID: xid}))
		assertLsoCtxStable(t, &b.calls, 1, lsoCtxRetryWait)
		waitLsoCtxAborted(t, bp, xid, apc.ActPrefetchObjects, nat)
	})

	t.Run("bucket-summary-inflight", func(t *testing.T) {
		b := remote.addBck(t, bp, "block-summary", lsoCtxBlock)
		xids := make([]string, 0, lsoCtxStress)
		for range lsoCtxStress {
			msg := &apc.BsummCtrlMsg{ObjCached: false, BckPresent: true}
			xid, _, err := api.GetBucketSummary(bp, cmn.QueryBcks(b.bck), msg, api.BsummArgs{DontWait: true})
			tassert.CheckFatal(t, err)
			xids = append(xids, xid)
		}
		waitLsoCtxCount(t, &b.calls, lsoCtxStress)
		for _, xid := range xids {
			tassert.CheckFatal(t, api.AbortXaction(bp, &xact.ArgsMsg{ID: xid}))
		}
		assertLsoCtxStable(t, &b.calls, lsoCtxStress, 200*time.Millisecond)
		waitLsoCtxCount(t, &b.canceled, lsoCtxStress)
		for _, xid := range xids {
			waitLsoCtxAborted(t, bp, xid, apc.ActSummaryBck, 1)
		}
	})

	t.Run("lstco-empty-pages", func(t *testing.T) {
		var (
			b     = remote.addBck(t, bp, "empty", lsoCtxEmpty)
			bck   = b.bck
			bckTo = cmn.Bck{Name: "lsoctx-to-" + strings.ToLower(cos.GenTie()), Provider: apc.AIS}
		)
		t.Cleanup(func() { _ = api.DestroyBucket(bp, bckTo) }) // (if created)

		// not using api.CopyBucket: api retries client timeouts (see cos.IsErrRetriableConn)
		q := url.Values{apc.QparamFltPresence: []string{strconv.Itoa(apc.FltExists)}}
		bck.SetQuery(q)
		_ = bckTo.AddUnameToQuery(q, apc.QparamBckTo, "" /*objName*/)
		body := cos.MustMarshal(apc.ActMsg{Action: apc.ActCopyBck, Value: &apc.TCBMsg{}})
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
			bp.URL+apc.URLPathBuckets.Join(bck.Name)+"?"+q.Encode(), bytes.NewReader(body))
		tassert.CheckFatal(t, err)
		req.Header.Set(cos.HdrContentType, cos.ContentJSON)
		api.SetAuxHeaders(req, &bp)

		client := *bp.Client
		client.Timeout = 3 * time.Second
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		tassert.Fatalf(t, cos.IsErrClientTimeout(err), "expected client timeout, got %v", err)
		waitLsoCtxCount(t, &b.calls, lsoCtxStress)
		waitLsoCtxQuiet(t, &b.calls)
	})

	t.Run("cli-empty-pages", func(t *testing.T) {
		bin, err := exec.LookPath("ais")
		if err != nil {
			t.Skip("'ais' binary not found")
		}
		t.Setenv(env.AisEndpoint, bp.URL)
		if bp.Token != "" {
			t.Setenv(env.AisAuthToken, bp.Token)
			t.Setenv(env.AisAuthTokenFile, "")
		}
		b := remote.addBck(t, bp, "sparse", lsoCtxSparse)

		out, err := exec.Command(bin, "advanced", "check-lock", b.bck.Cname("")).CombinedOutput()
		tassert.Fatalf(t, err == nil && strings.Contains(string(out), "/obj"), "ais advanced check-lock: %v\n%s", err, out)
	})
}

const injectedListErr = "injected LIST error"

func TestPrefetchDT(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal, MinTargets: 1})
	bp := tools.BaseAPIParams()
	nat := tools.GetClusterMap(t, tools.GetPrimaryURL()).CountActiveTs()
	remote := attachLsoCtxRemote(t, bp, nat)
	for _, tc := range []struct {
		name  string
		mode  int
		calls int64
	}{
		{"last-page", lsoCtxLastPage, 1},
		{"empty-pages", lsoCtxFinalPages, lsoCtxStress + 1},
		{"objects", lsoCtxPrefetchPages, lsoCtxStress},
		{"generic-start", lsoCtxLastPage, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := remote.addBck(t, bp, tc.name, tc.mode)
			var (
				xid string
				err error
			)
			if tc.name == "generic-start" {
				xid, err = api.StartXaction(bp, &xact.ArgsMsg{Kind: apc.ActPrefetchObjects, Bck: b.bck}, "")
			} else {
				xid, err = api.Prefetch(bp, b.bck, &apc.PrefetchMsg{})
			}
			tassert.CheckFatal(t, err)
			args := &xact.ArgsMsg{ID: xid, Kind: apc.ActPrefetchObjects, Timeout: tools.EvictPrefetchTimeout}
			status, err := api.WaitForXactionIC(bp, args)
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, status.IsFinished() && !status.IsAborted() && status.ErrMsg == "", "prefetch did not finish cleanly: %+v", status)
			snaps, err := api.QueryXactionSnaps(bp, args)
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, len(snaps) == nat, "expected %d targets, got %v", nat, snaps)
			for tid, all := range snaps {
				tassert.Fatalf(t, len(all) == 1 && all[0].IsFinished() && !all[0].IsAborted() && all[0].Err == "", "target %s: %+v", tid, all)
			}
			tassert.Errorf(t, b.calls.Load() == tc.calls, "expected %d backend LISTs, got %d", tc.calls, b.calls.Load())
			if tc.mode == lsoCtxPrefetchPages {
				count, _, _ := snaps.ObjCounts(xid)
				tassert.Errorf(t, count == lsoCtxStress, "expected %d prefetched objects, got %d", lsoCtxStress, count)
				msg := &apc.LsoMsg{Flags: apc.LsCached | apc.LsNameOnly}
				page, err := api.ListObjectsPage(bp, b.bck, msg, api.ListArgs{})
				tassert.CheckFatal(t, err)
				tassert.Errorf(t, len(page.Entries) == lsoCtxStress, "expected %d cached objects, got %d", lsoCtxStress, len(page.Entries))
				for i := range lsoCtxStress {
					var got bytes.Buffer
					_, err := api.GetObject(bp, b.bck, fmt.Sprintf("obj-%d", i+1), &api.GetArgs{Writer: &got})
					tassert.CheckFatal(t, err)
					tassert.Errorf(t, got.String() == "x", "unexpected object data: %q", got.String())
				}
				tassert.Errorf(t, b.gets.Load() == lsoCtxStress, "expected %d backend GETs, got %d", lsoCtxStress, b.gets.Load())
			}
		})
	}
	t.Run("abort-inflight", func(t *testing.T) {
		b := remote.addBck(t, bp, "prefetch-block", lsoCtxBlock)
		xid, err := api.Prefetch(bp, b.bck, &apc.PrefetchMsg{})
		tassert.CheckFatal(t, err)
		waitLsoCtxCount(t, &b.calls, 1)
		tassert.CheckFatal(t, api.AbortXaction(bp, &xact.ArgsMsg{ID: xid}))
		waitLsoCtxCount(t, &b.canceled, 1)
		waitLsoCtxAborted(t, bp, xid, apc.ActPrefetchObjects, nat)
		tassert.Errorf(t, b.calls.Load() == 1, "expected one backend LIST, got %d", b.calls.Load())
	})
	t.Run("backend-error", func(t *testing.T) {
		tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 2})
		b := remote.addBck(t, bp, "prefetch-error", lsoCtxListFailure)
		xid, err := api.Prefetch(bp, b.bck, &apc.PrefetchMsg{})
		tassert.CheckFatal(t, err)
		status, err := api.WaitForXactionIC(bp, &xact.ArgsMsg{ID: xid, Kind: apc.ActPrefetchObjects, Timeout: tools.EvictPrefetchTimeout})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, status.IsAborted(), "expected abort, got %s (err=%q)", status, status.ErrMsg)

		var cause string
		err = tools.WaitForCondition(func() bool {
			snaps, err := api.QueryXactionSnaps(bp, &xact.ArgsMsg{ID: xid})
			if err != nil || len(snaps) != nat {
				return false
			}
			cause = ""
			for _, all := range snaps {
				if len(all) != 1 || !all[0].IsFinished() || !all[0].IsAborted() {
					return false
				}
				if strings.Contains(all[0].AbortErr, injectedListErr) {
					cause = all[0].AbortErr // DT's abort error
				}
			}
			return true
		}, tools.WaitRetryOpts{MaxRetries: 300, Interval: 50 * time.Millisecond})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, cause != "", "expected backend error in at least one target's abort error (IC: %q)", status.ErrMsg)
		tassert.Errorf(t, b.calls.Load() == 2, "expected two backend LISTs, got %d", b.calls.Load())
	})
	for _, workers := range []int{1, xact.NwpNone} {
		t.Run(fmt.Sprintf("backpressure-abort/%d", workers), func(t *testing.T) {
			tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 2})
			b := remote.addBck(t, bp, "prefetch-slow", lsoCtxSlowPrefetch)
			xid, err := api.Prefetch(bp, b.bck, &apc.PrefetchMsg{NumWorkers: workers})
			tassert.CheckFatal(t, err)
			smap := tools.GetClusterMap(t, bp.URL)
			dt, err := smap.HrwTargetTask(xid)
			tassert.CheckFatal(t, err)
			var receiver *meta.Snode
			for _, tsi := range smap.Tmap.ActiveNodes() {
				if tsi.ID() != dt.ID() {
					receiver = tsi
					break
				}
			}
			bck := meta.CloneBck(&b.bck)
			bck.Props, err = api.HeadBucket(bp, b.bck, true /*dontAddRemote*/)
			tassert.CheckFatal(t, err)
			for i := range 1024 {
				name := tools.GenerateObjectNameOnTarget(fmt.Sprintf("slow-%04d-", i), *bck.Bucket(), smap, receiver)
				b.entries = append(b.entries, &cmn.LsoEnt{Name: name})
			}
			close(b.ready)
			waitLsoCtxCount(t, &b.gets, 1)
			var previous int64
			steady := 0
			err = tools.WaitForCondition(func() bool {
				current := b.calls.Load()
				if current == previous {
					steady++
				} else {
					previous, steady = current, 0
				}
				return steady == 6
			}, tools.WaitRetryOpts{MaxRetries: 300, Interval: 50 * time.Millisecond})
			tassert.CheckFatal(t, err)
			tassert.CheckFatal(t, api.AbortXaction(bp, &xact.ArgsMsg{ID: xid}))
			waitLsoCtxCount(t, &b.canceled, 1)
			waitLsoCtxAborted(t, bp, xid, apc.ActPrefetchObjects, nat)
			assertLsoCtxStable(t, &b.calls, int(previous), 200*time.Millisecond)
		})
	}
	t.Run("membership-change", func(t *testing.T) {
		tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 2})
		b := remote.addBck(t, bp, "prefetch-membership", lsoCtxBlock)
		xid, err := api.Prefetch(bp, b.bck, &apc.PrefetchMsg{})
		tassert.CheckFatal(t, err)
		waitLsoCtxCount(t, &b.calls, 1)
		smap := tools.GetClusterMap(t, bp.URL)
		dt, err := smap.HrwTargetTask(xid)
		tassert.CheckFatal(t, err)
		args := &apc.ActValRmNode{SkipRebalance: true}
		for _, tsi := range smap.Tmap.ActiveNodes() {
			if tsi.ID() != dt.ID() {
				args.DaemonID = tsi.ID()
				break
			}
		}
		_, err = startMaintenanceRetry(t, bp, args)
		tassert.CheckFatal(t, err)
		t.Cleanup(func() {
			_, err := stopMaintenanceRetry(t, bp, args)
			tassert.CheckError(t, err)
			_, err = tools.WaitForClusterState(bp.URL, "restore prefetch target", smap.Version, smap.CountActivePs(), nat)
			tassert.CheckError(t, err)
		})
		waitLsoCtxCount(t, &b.canceled, 1)
		status, err := api.WaitForXactionIC(bp, &xact.ArgsMsg{ID: xid, Kind: apc.ActPrefetchObjects, Timeout: tools.EvictPrefetchTimeout})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, status.IsAborted(), "expected membership change to abort prefetch, got %+v", status)
		tassert.Errorf(t, b.calls.Load() == 1, "expected one backend LIST, got %d", b.calls.Load())
	})
}

func TestPrefetchListRetryPolicy(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal, MinTargets: 2})
	bp := tools.BaseAPIParams()
	nat := tools.GetClusterMap(t, bp.URL).CountActiveTs()
	remote := attachLsoCtxRemote(t, bp, nat)
	config := tools.GetClusterConfig(t)
	t.Cleanup(func() {
		tools.SetClusterConfig(t, cos.StrKVs{"client.list_timeout": config.Client.ListObjTimeout.String()})
	})
	tools.SetClusterConfig(t, cos.StrKVs{"client.list_timeout": "2s"})

	for _, tc := range []struct {
		name      string
		mode      int
		enabled   bool
		onDemand  bool
		retries   int
		calls     int64
		wantAbort bool
	}{
		{name: "off-success", mode: lsoCtxLastPage, calls: 1},
		{name: "off-429-no-retry", mode: lsoCtxRetryOnce, calls: 1, wantAbort: true},
		{name: "enabled-retry-recovers", mode: lsoCtxRetryOnce, enabled: true, retries: 1, calls: 2},
		{name: "on-demand-retry-recovers", mode: lsoCtxRetryOnce, onDemand: true, retries: 1, calls: 2},
		{name: "enabled-retries-exhausted", mode: lsoCtxRetry, enabled: true, retries: 1, calls: 2, wantAbort: true},
		{name: "on-demand-retries-exhausted", mode: lsoCtxRetry, onDemand: true, retries: 1, calls: 2, wantAbort: true},
		{name: "on-demand-zero-retries-uses-default", mode: lsoCtxRetryOnce, onDemand: true, calls: 2},
		{name: "on-demand-non-rate-limit-error", mode: lsoCtxListFailure, onDemand: true, retries: 1, calls: 2, wantAbort: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := remote.addBck(t, bp, "prefetch-list-policy", tc.mode)
			_, err := api.SetBucketProps(bp, b.bck, &cmn.BpropsToSet{RateLimit: &cmn.RateLimitConfToSet{
				Backend: &cmn.AdaptiveToSet{RateLimitBaseToSet: cmn.RateLimitBaseToSet{
					Enabled: apc.Ptr(tc.enabled), MaxTokens: apc.Ptr(2 * nat), Interval: apc.Ptr(cos.Duration(3 * time.Second)),
				}, OnDemand: apc.Ptr(tc.onDemand), NumRetries: apc.Ptr(tc.retries)},
			}})
			tassert.CheckFatal(t, err)
			xid, err := api.Prefetch(bp, b.bck, &apc.PrefetchMsg{})
			tassert.CheckFatal(t, err)
			args := &xact.ArgsMsg{ID: xid, Kind: apc.ActPrefetchObjects, Timeout: tools.EvictPrefetchTimeout}
			status, err := api.WaitForXactionIC(bp, args)
			tassert.CheckFatal(t, err)
			tassert.Errorf(t, status.IsAborted() == tc.wantAbort, "expected aborted=%t, got %+v", tc.wantAbort, status)
			tassert.Errorf(t, b.calls.Load() == tc.calls, "expected %d backend LISTs, got %d", tc.calls, b.calls.Load())
			if !status.IsAborted() {
				tassert.Fatalf(t, status.IsFinished() && status.ErrMsg == "", "prefetch did not finish cleanly: %+v", status)
			}
			snaps, err := api.WaitForSnaps(bp, args, func(snaps xact.MultiSnap) (bool, bool, error) {
				if len(snaps) != nat {
					return false, false, nil
				}
				for _, all := range snaps {
					if len(all) != 1 || !all[0].IsFinished() {
						return false, false, nil
					}
				}
				return true, false, nil
			})
			tassert.CheckFatal(t, err)
			for tid, all := range snaps {
				tassert.Errorf(t, len(all) == 1 && all[0].IsFinished() && all[0].IsAborted() == tc.wantAbort, "target %s: %+v", tid, all)
			}
		})
	}
}

func attachLsoCtxRemote(t *testing.T, bp api.BaseParams, nat int) *lsoCtxRemote {
	t.Helper()
	remote := newLsoCtxRemote()
	alias := tools.MockRemoteAISAliasPrefix + strings.ToLower(cos.GenTie())
	t.Cleanup(func() {
		for _, b := range remote.bcks {
			_ = api.EvictRemoteBucket(bp, b.bck, false /*keepMD*/)
		}
		_ = api.DetachRemoteAIS(bp, alias)
		remote.srv.CloseClientConnections()
		remote.srv.Close()
	})
	tassert.CheckFatal(t, api.AttachRemoteAIS(bp, alias, remote.srv.URL))
	waitLsoCtxCount(t, &remote.handshakes, nat)
	return remote
}

func newLsoCtxRemote() *lsoCtxRemote {
	remote := &lsoCtxRemote{
		bcks: make(map[string]*lsoCtxBck, 8),
		smap: meta.Smap{Pmap: make(meta.NodeMap), Tmap: make(meta.NodeMap), UUID: cos.GenUUID(), Version: 1},
	}
	remote.srv = httptest.NewServer(http.HandlerFunc(remote.serveHTTP))
	return remote
}

// register the bucket (and its behavior) before the cluster first sees it
func (remote *lsoCtxRemote) addBck(t *testing.T, bp api.BaseParams, name string, mode int) *lsoCtxBck {
	b := &lsoCtxBck{
		bck:  cmn.Bck{Name: name + "-" + strings.ToLower(cos.GenTie()), Provider: apc.AIS, Ns: cmn.Ns{UUID: remote.smap.UUID}},
		mode: mode,
	}
	if mode == lsoCtxSlowPrefetch {
		b.ready = make(chan struct{})
	}
	remote.mu.Lock()
	remote.bcks[b.bck.Name] = b
	remote.mu.Unlock()

	_, err := api.HeadBucket(bp, b.bck, false /*dontAddRemote*/)
	tassert.CheckFatal(t, err)
	return b
}

func (remote *lsoCtxRemote) lookup(r *http.Request) *lsoCtxBck {
	name, ok := strings.CutPrefix(r.URL.Path, apc.URLPathBuckets.S+"/")
	if !ok {
		name, ok = strings.CutPrefix(r.URL.Path, apc.URLPathObjects.S+"/")
		if !ok {
			return nil
		}
	}
	name, _, _ = strings.Cut(name, "/")
	remote.mu.Lock()
	b := remote.bcks[name]
	remote.mu.Unlock()
	return b
}

func (remote *lsoCtxRemote) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == apc.URLPathDae.S {
		remote.handshakes.Add(1)
		w.Header().Set(cos.HdrContentType, cos.ContentJSON)
		_ = json.NewEncoder(w).Encode(&remote.smap)
		return
	}
	if r.Method == http.MethodHead {
		w.Header().Set(apc.HdrBucketProps, "{}")
		return
	}
	b := remote.lookup(r)
	if b == nil {
		http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusBadRequest)
		return
	}
	if b.mode == lsoCtxSlowPrefetch && strings.HasPrefix(r.URL.Path, apc.URLPathObjects.S+"/") {
		b.gets.Add(1)
		w.Header().Set(cos.HdrContentLength, "1")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		b.canceled.Add(1)
		return
	}
	if b.mode == lsoCtxPrefetchPages && strings.HasPrefix(r.URL.Path, apc.URLPathObjects.S+"/") {
		b.gets.Add(1)
		_, _ = w.Write([]byte("x"))
		return
	}
	n := b.calls.Add(1)
	switch b.mode {
	case lsoCtxLastPage, lsoCtxFinalPages:
		w.Header().Set(cos.HdrContentType, cos.ContentMsgPack)
		page := &cmn.LsoRes{}
		if b.mode == lsoCtxFinalPages && n <= lsoCtxStress {
			page.ContinuationToken = strconv.FormatInt(n, 10)
		}
		_ = msgp.Encode(w, page)
	case lsoCtxListFailure:
		if n == 1 {
			w.Header().Set(cos.HdrContentType, cos.ContentMsgPack)
			_ = msgp.Encode(w, &cmn.LsoRes{ContinuationToken: "next"})
		} else {
			http.Error(w, injectedListErr, http.StatusInternalServerError)
		}
	case lsoCtxPrefetchPages:
		token := ""
		if n < lsoCtxStress {
			token = strconv.FormatInt(n, 10)
		}
		servePage(w, fmt.Sprintf("obj-%d", n), token)
	case lsoCtxSlowPrefetch:
		select {
		case <-b.ready:
			w.Header().Set(cos.HdrContentType, cos.ContentMsgPack)
			_ = msgp.Encode(w, &cmn.LsoRes{Entries: b.entries, ContinuationToken: strconv.FormatInt(n, 10)})
		case <-r.Context().Done():
		}
	case lsoCtxRetryOnce:
		if n == 1 {
			http.Error(w, "retry LIST", http.StatusTooManyRequests)
		} else {
			w.Header().Set(cos.HdrContentType, cos.ContentMsgPack)
			_ = msgp.Encode(w, &cmn.LsoRes{})
		}
	case lsoCtxRetry:
		http.Error(w, "throttled", http.StatusTooManyRequests)
	case lsoCtxSparse:
		if n > lsoCtxStress {
			servePage(w, "obj", "")
			return
		}
		fallthrough
	case lsoCtxEmpty:
		time.Sleep(10 * time.Millisecond) // pace the empty pages
		w.Header().Set(cos.HdrContentType, cos.ContentMsgPack)
		_ = msgp.Encode(w, &cmn.LsoRes{ContinuationToken: strconv.FormatInt(n, 10)})
	case lsoCtxEndless:
		time.Sleep(10 * time.Millisecond) // pace the inventory cleanup stress test
		serveEndlessPage(w, r)
	case lsoCtxPageError:
		switch n {
		case 1:
			servePage(w, "first", "next")
		case 2:
			http.Error(w, "injected page error", http.StatusInternalServerError)
		default:
			servePage(w, "second", "")
		}
	case lsoCtxPageTimeout:
		if n == 1 {
			servePage(w, "first", "next")
			return
		}
		fallthrough
	case lsoCtxBlock:
		w.Header().Set(cos.HdrContentType, cos.ContentMsgPack)
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		b.canceled.Add(1)
	default:
		http.Error(w, "invalid mode "+strconv.Itoa(b.mode), http.StatusInternalServerError)
	}
}

// single-entry page; empty token: last page
func servePage(w http.ResponseWriter, name, token string) {
	w.Header().Set(cos.HdrContentType, cos.ContentMsgPack)
	_ = msgp.Encode(w, &cmn.LsoRes{Entries: cmn.LsoEntries{{Name: name}}, ContinuationToken: token})
}

// generate a sorted page of names (the bucket has no last page)
func serveEndlessPage(w http.ResponseWriter, r *http.Request) {
	var (
		actMsg apc.ActMsg
		lsmsg  apc.LsoMsg
		pnum   int64
	)
	if err := json.NewDecoder(r.Body).Decode(&actMsg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := cos.MorphMarshal(actMsg.Value, &lsmsg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if lsmsg.ContinuationToken != "" {
		pnum, _ = strconv.ParseInt(lsmsg.ContinuationToken, 10, 64)
	}
	size := lsmsg.PageSize // remote backend always sets it (see calcPageSize)
	lst := &cmn.LsoRes{Entries: make(cmn.LsoEntries, size), ContinuationToken: strconv.FormatInt(pnum+1, 10)}
	for i := range lst.Entries {
		lst.Entries[i] = &cmn.LsoEnt{Name: fmt.Sprintf("obj-%012d", pnum*size+int64(i))}
	}
	w.Header().Set(cos.HdrContentType, cos.ContentMsgPack)
	_ = msgp.Encode(w, lst)
}

func waitLsoCtxCount(t *testing.T, counter *atomic.Int64, expected int) {
	err := tools.WaitForCondition(func() bool { return counter.Load() >= int64(expected) },
		tools.WaitRetryOpts{MaxRetries: 300, Interval: 50 * time.Millisecond})
	tassert.Fatalf(t, err == nil, "waiting for %d calls (got %d): %v", expected, counter.Load(), err)
}

func waitLsoCtxQuiet(t *testing.T, counter *atomic.Int64) {
	prev := counter.Load()
	for range 30 {
		time.Sleep(time.Second)
		curr := counter.Load()
		if curr == prev {
			return
		}
		prev = curr
	}
	tassert.Fatalf(t, false, "LIST continued after the client timed out: %d calls", prev)
}

func waitLsoCtxAborted(t *testing.T, bp api.BaseParams, xid, kind string, minAborted int) {
	err := tools.WaitForCondition(func() bool {
		snaps, err := api.QueryXactionSnaps(bp, &xact.ArgsMsg{ID: xid, Kind: kind})
		if err != nil {
			return false
		}
		aborted := 0
		for _, all := range snaps {
			for _, snap := range all {
				if snap.ID != xid || !snap.IsFinished() {
					continue
				}
				// user abort may race with IC abort: the first target to report aborted
				// triggers IC(notifications) => (abort the rest)
				// (see prxnotif done())
				if snap.IsAborted() &&
					(snap.AbortErr == cmn.ErrXactUserAbort.Error() || snap.AbortErr == cmn.ErrXactICNotifAbort.Error()) {
					aborted++
				}
			}
		}
		return aborted >= minAborted
	}, tools.WaitRetryOpts{MaxRetries: 300, Interval: 50 * time.Millisecond})
	tassert.CheckFatal(t, err)
}

func assertLsoCtxStable(t *testing.T, counter *atomic.Int64, expected int, wait time.Duration) {
	count := counter.Load()
	tassert.Fatalf(t, count >= int64(expected), "expected at least %d LIST calls, got %d", expected, count)
	time.Sleep(wait)
	after := counter.Load()
	tassert.Fatalf(t, after == count, "LIST continued after abort: %d => %d calls", count, after)
}
