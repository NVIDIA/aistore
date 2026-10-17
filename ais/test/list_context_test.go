// Package integration_test.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
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

const listCtxStress = 3

type listCtxRemote struct {
	srv                         *httptest.Server
	smap                        meta.Smap
	bcks                        []cmn.Bck
	handshakes, calls, canceled atomic.Int64
}

func TestListContextCancellation(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal, MinTargets: 2})
	var (
		bp     = tools.BaseAPIParams()
		nat    = tools.GetClusterMap(t, tools.GetPrimaryURL()).CountActiveTs()
		remote = newListCtxRemote()
		alias  = "listctx-" + strings.ToLower(cos.GenTie())
	)
	t.Cleanup(func() {
		for _, bck := range remote.bcks {
			_ = api.EvictRemoteBucket(bp, bck, false /*keepMD*/)
		}
		_ = api.DetachRemoteAIS(bp, alias)
		remote.srv.CloseClientConnections()
		remote.srv.Close()
	})
	tassert.CheckFatal(t, api.AttachRemoteAIS(bp, alias, remote.srv.URL))
	waitListCtxCount(t, &remote.handshakes, nat)

	t.Run("lrit-retry-wait", func(t *testing.T) {
		bck := remote.addBck(t, bp, "retry")
		remote.reset()
		xid, err := api.Prefetch(bp, bck, &apc.PrefetchMsg{})
		tassert.CheckFatal(t, err)
		waitListCtxCount(t, &remote.calls, nat)
		tassert.CheckFatal(t, api.AbortXaction(bp, &xact.ArgsMsg{ID: xid}))
		assertListCtxStable(t, &remote.calls, nat, 3*time.Second)
		waitListCtxAborted(t, bp, xid, apc.ActPrefetchObjects, nat, true /*finished*/)
	})

	t.Run("nbi-inflight", func(t *testing.T) {
		bck := remote.addBck(t, bp, "block-nbi")
		remote.reset()
		xid, err := api.CreateNBI(bp, bck, &apc.CreateNBIMsg{})
		tassert.CheckFatal(t, err)
		waitListCtxCount(t, &remote.calls, nat)
		tassert.CheckFatal(t, api.AbortXaction(bp, &xact.ArgsMsg{ID: xid}))
		assertListCtxStable(t, &remote.calls, nat, 200*time.Millisecond)
		waitListCtxCount(t, &remote.canceled, nat)
		waitListCtxAborted(t, bp, xid, apc.ActCreateNBI, nat, false /*finished*/)
	})

	t.Run("bucket-summary-inflight", func(t *testing.T) {
		bck := remote.addBck(t, bp, "block-summary")
		remote.reset()
		xids := make([]string, 0, listCtxStress)
		for range listCtxStress {
			msg := &apc.BsummCtrlMsg{ObjCached: false, BckPresent: true}
			xid, _, err := api.GetBucketSummary(bp, cmn.QueryBcks(bck), msg, api.BsummArgs{DontWait: true})
			tassert.CheckFatal(t, err)
			xids = append(xids, xid)
		}
		waitListCtxCount(t, &remote.calls, listCtxStress)
		for _, xid := range xids {
			tassert.CheckFatal(t, api.AbortXaction(bp, &xact.ArgsMsg{ID: xid}))
		}
		assertListCtxStable(t, &remote.calls, listCtxStress, 200*time.Millisecond)
		waitListCtxCount(t, &remote.canceled, listCtxStress)
		for _, xid := range xids {
			waitListCtxAborted(t, bp, xid, apc.ActSummaryBck, 1, true /*finished*/)
		}
	})

	t.Run("lstco-empty-pages", func(t *testing.T) {
		bck := remote.addBck(t, bp, "empty")
		bckTo := cmn.Bck{Name: "listctx-to", Provider: apc.AIS}
		remote.reset()

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
		waitListCtxCount(t, &remote.calls, listCtxStress)
		waitListCtxQuiet(t, &remote.calls)
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
		bck := remote.addBck(t, bp, "sparse")
		remote.reset()

		out, err := exec.Command(bin, "advanced", "check-lock", bck.Cname("")).CombinedOutput()
		tassert.Fatalf(t, err == nil && strings.Contains(string(out), "/obj"), "ais advanced check-lock: %v\n%s", err, out)
	})
}

func newListCtxRemote() *listCtxRemote {
	remote := &listCtxRemote{smap: meta.Smap{
		Pmap: make(meta.NodeMap), Tmap: make(meta.NodeMap), UUID: cos.GenUUID(), Version: 1,
	}}
	remote.srv = httptest.NewServer(http.HandlerFunc(remote.serveHTTP))
	return remote
}

func (remote *listCtxRemote) addBck(t *testing.T, bp api.BaseParams, name string) cmn.Bck {
	bck := cmn.Bck{Name: name + "-" + strings.ToLower(cos.GenTie()), Provider: apc.AIS,
		Ns: cmn.Ns{UUID: remote.smap.UUID}}
	_, err := api.HeadBucket(bp, bck, false /*dontAddRemote*/)
	tassert.CheckFatal(t, err)
	remote.bcks = append(remote.bcks, bck)
	return bck
}

func (remote *listCtxRemote) serveHTTP(w http.ResponseWriter, r *http.Request) {
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
	n := remote.calls.Add(1)
	if strings.Contains(r.URL.Path, "/sparse-") && n > listCtxStress {
		w.Header().Set(cos.HdrContentType, cos.ContentMsgPack)
		_ = msgp.Encode(w, &cmn.LsoRes{Entries: cmn.LsoEntries{{Name: "obj"}}})
		return
	}
	if strings.Contains(r.URL.Path, "/empty-") || strings.Contains(r.URL.Path, "/sparse-") {
		time.Sleep(10 * time.Millisecond) // pace the empty pages
		w.Header().Set(cos.HdrContentType, cos.ContentMsgPack)
		_ = msgp.Encode(w, &cmn.LsoRes{ContinuationToken: strconv.FormatInt(n, 10)})
		return
	}
	if strings.Contains(r.URL.Path, "/retry-") {
		http.Error(w, "throttled", http.StatusTooManyRequests)
		return
	}
	w.Header().Set(cos.HdrContentType, cos.ContentMsgPack)
	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()
	<-r.Context().Done()
	remote.canceled.Add(1)
}

func (remote *listCtxRemote) reset() {
	remote.calls.Store(0)
	remote.canceled.Store(0)
}

func waitListCtxCount(t *testing.T, counter *atomic.Int64, expected int) {
	err := tools.WaitForCondition(func() bool { return counter.Load() >= int64(expected) },
		tools.WaitRetryOpts{MaxRetries: 300, Interval: 50 * time.Millisecond})
	tassert.Fatalf(t, err == nil, "waiting for %d calls (got %d): %v", expected, counter.Load(), err)
}

func waitListCtxQuiet(t *testing.T, counter *atomic.Int64) {
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

func waitListCtxAborted(t *testing.T, bp api.BaseParams, xid, kind string, minAborted int, finished bool) {
	err := tools.WaitForCondition(func() bool {
		snaps, err := api.QueryXactionSnaps(bp, &xact.ArgsMsg{ID: xid, Kind: kind})
		if err != nil {
			return false
		}
		aborted := 0
		for _, all := range snaps {
			for _, snap := range all {
				if snap.ID != xid || (finished && !snap.IsFinished()) {
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

func assertListCtxStable(t *testing.T, counter *atomic.Int64, expected int, wait time.Duration) {
	count := counter.Load()
	tassert.Fatalf(t, count >= int64(expected), "expected at least %d LIST calls, got %d", expected, count)
	time.Sleep(wait)
	after := counter.Load()
	tassert.Fatalf(t, after == count, "LIST continued after abort: %d => %d calls", count, after)
}
