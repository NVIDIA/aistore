// Package integration_test.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/docker"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact"
)

//
// unified stress test ------------------------------------------------------------------------------------
//

const (
	resilLongTimeout  = time.Minute
	resilShortTimeout = 3 * time.Second
	resilShortPoll    = 100 * time.Millisecond
	resilLongPoll     = time.Second
)

type resilverStressCtx struct {
	target     *meta.Snode
	origMpaths []string

	reg *ioContext
	chk *ioContext
	mir *ioContext
	ec  *ioContext
}

func TestResilverStress(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{
		Long:               true,
		MinMountpaths:      4, // copies
		MinTargets:         5, // EC (2, 2)
		RequiredDeployment: tools.ClusterTypeLocal,
	})

	c := newResilverStressCtx(t)
	createAndPopulateResilverBuckets(t, c)

	c.validateAllData(t, "baseline")

	run := func(name string, fn func(t *testing.T, c *resilverStressCtx)) {
		t.Run(name, func(t *testing.T) {
			purl := tools.RandomProxyURL(t)
			bp := tools.BaseAPIParams(purl)
			t.Cleanup(func() { c.restoreMountpaths(t, bp) })
			fn(t, c)
		})
	}

	run("A_disable_disable_preempt", testAdisableDisablePreempt)
	run("B_disable_enable_toggle", testBdisableEnableToggle)
	run("C_detach_attach_cycle", testCdetachAttachCycle)
	run("D_rapid_fire", testDrapidFire)
	run("E_no_resilver_guard", testEnoResilverGuard)

	c.validateAllData(t, "final")
	tlog.Logln("Test completed successfully")
}

func newResilverStressCtx(t *testing.T) *resilverStressCtx {
	purl := tools.RandomProxyURL(t)
	bp := tools.BaseAPIParams(purl)
	smap := tools.GetClusterMap(t, purl)

	target, _ := smap.GetRandTarget()
	tlog.Logfln("Using target %s", target.StringEx())

	mpList, err := api.GetMountpaths(bp, target)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(mpList.Available) >= 4, "need at least 4 mountpaths, have %d", len(mpList.Available))
	ensureNoDisabledMountpaths(t, target, mpList)

	origMpaths := mpList.Available
	tlog.Logfln("Target %s has %d mountpaths", target.StringEx(), len(origMpaths))

	return &resilverStressCtx{
		target:     target,
		origMpaths: origMpaths,
	}
}

// (best-effort)
func (c *resilverStressCtx) restoreMountpaths(t *testing.T, bp api.BaseParams) {
	t.Helper()

	deadline := time.Now().Add(resilLongTimeout)
	var lastErr error

	for time.Now().Before(deadline) {
		mpl, err := api.GetMountpaths(bp, c.target)
		if err != nil {
			lastErr = err
			time.Sleep(resilShortPoll)
			continue
		}

		// 1) attach back anything baseline that is neither available nor disabled nor waiting-dd
		for _, mp := range c.origMpaths {
			if slices.Contains(mpl.Available, mp) || slices.Contains(mpl.Disabled, mp) || slices.Contains(mpl.WaitingDD, mp) {
				continue
			}
			if err := api.AttachMountpath(bp, c.target, mp, ""); err != nil {
				lastErr = err
			}
		}

		// 2) enable baseline mountpaths that are disabled
		for _, mp := range c.origMpaths {
			if !slices.Contains(mpl.Disabled, mp) {
				continue
			}
			if err := api.EnableMountpath(bp, c.target, mp); err != nil {
				lastErr = err
			}
		}

		// 3) wait for transitions to settle (best-effort)
		mpl = c.waitPostDDNoFatal(t, bp)
		if mpl == nil {
			// couldn't obtain a stable view; retry
			time.Sleep(resilShortPoll)
			continue
		}

		// success: all baseline mountpaths are available and none are disabled
		ok := true
		for _, mp := range c.origMpaths {
			if !slices.Contains(mpl.Available, mp) || slices.Contains(mpl.Disabled, mp) {
				ok = false
				break
			}
		}
		if ok {
			return
		}

		time.Sleep(resilShortPoll)
	}

	_, err := api.GetMountpaths(bp, c.target)
	if err != nil {
		tlog.Logfln("cleanup restoreMountpaths: target=%s: failed to query mountpaths at end: %v (lastErr=%v)",
			c.target.ID(), err, lastErr)
	}
}

// cleanup-safe variant of waitPostDD
func (c *resilverStressCtx) waitPostDDNoFatal(t *testing.T, bp api.BaseParams) *apc.MountpathList {
	t.Helper()

	deadline := time.Now().Add(resilLongTimeout)
	for time.Now().Before(deadline) {
		mp, err := api.GetMountpaths(bp, c.target)
		if err != nil {
			time.Sleep(resilLongPoll)
			continue
		}
		if len(mp.WaitingDD) == 0 {
			return mp
		}
		time.Sleep(resilLongPoll)
	}
	return nil
}

// query resilver snaps (optionally only running) for the selected target
func (c *resilverStressCtx) queryResilver(t *testing.T, bp api.BaseParams, onlyRunning bool) xact.MultiSnap {
	t.Helper()
	xargs := xact.ArgsMsg{Kind: apc.ActResilver, DaemonID: c.target.ID(), OnlyRunning: onlyRunning}
	xs, err := api.QueryXactionSnaps(bp, &xargs)
	tassert.CheckFatal(t, err)
	return xs
}

func (c *resilverStressCtx) getSnap(t *testing.T, xs xact.MultiSnap, xid string) (*core.Snap, bool) {
	t.Helper()
	snaps, ok := xs[c.target.ID()]
	if !ok {
		return nil, false
	}
	for _, s := range snaps {
		if s.ID == xid {
			return s, true
		}
	}
	return nil, false
}

// wait until some resilver is running on this target; return its UUID
func (c *resilverStressCtx) waitResilverStartOnTarget(t *testing.T, bp api.BaseParams) string {
	t.Helper()
	xargs := xact.ArgsMsg{
		Kind:        apc.ActResilver,
		DaemonID:    c.target.ID(),
		OnlyRunning: true,
		Timeout:     resilShortTimeout,
	}
	snaps, err := api.WaitForSnaps(bp, &xargs, xargs.Started())
	tassert.CheckFatal(t, err)

	_, snap, err := snaps.RunningTarget("")
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, snap != nil, "resilver did not start within %v", resilShortTimeout)
	return snap.ID
}

func (c *resilverStressCtx) waitResilverFinishAny(t *testing.T, bp api.BaseParams) {
	t.Helper()
	xargs := xact.ArgsMsg{
		Kind:        apc.ActResilver,
		DaemonID:    c.target.ID(),
		OnlyRunning: true,
		Timeout:     resilLongTimeout,
	}
	// resilver does NOT notify IC; must use snaps-based wait
	_, err := api.WaitForSnaps(bp, &xargs, nil)
	tassert.CheckFatal(t, err)
}

func (c *resilverStressCtx) waitMpathNotAvailable(t *testing.T, bp api.BaseParams, mp string) {
	t.Helper()
	var elapsed time.Duration
	for elapsed < resilLongTimeout {
		mpl, err := api.GetMountpaths(bp, c.target)
		tassert.CheckFatal(t, err)
		if !slices.Contains(mpl.Available, mp) && !slices.Contains(mpl.WaitingDD, mp) {
			return
		}
		time.Sleep(resilShortPoll)
		elapsed += resilShortPoll
	}
	tassert.Fatalf(t, false, "mountpath %q still available (or waiting-dd) after %v", mp, resilLongTimeout)
}

func (c *resilverStressCtx) waitMpathAvailable(t *testing.T, bp api.BaseParams, mp string) {
	t.Helper()
	var elapsed time.Duration
	for elapsed < resilLongTimeout {
		mpl, err := api.GetMountpaths(bp, c.target)
		tassert.CheckFatal(t, err)
		if slices.Contains(mpl.Available, mp) && !slices.Contains(mpl.WaitingDD, mp) {
			return
		}
		time.Sleep(resilShortPoll)
		elapsed += resilShortPoll
	}
	tassert.Fatalf(t, false, "mountpath %q did not become available after %v", mp, resilLongTimeout)
}

// start-window: if resilver doesn't start soon, don't hang for a minute
// NOTE motivation: faster timeout
func (c *resilverStressCtx) waitResilverFinishIfStarted(t *testing.T, bp api.BaseParams, why string) {
	t.Helper()
	xargs := xact.ArgsMsg{
		Kind:        apc.ActResilver,
		DaemonID:    c.target.ID(),
		OnlyRunning: true,
		Timeout:     resilShortTimeout,
	}
	_, err := api.WaitForSnaps(bp, &xargs, xargs.Started())
	if err != nil {
		tlog.Logfln("No resilver started (%s) within %v; continuing", why, resilShortTimeout)
		return
	}
	c.waitNoRunningResilver(t, bp)
}

// wait until there's no running resilver on the target
func (c *resilverStressCtx) waitNoRunningResilver(t *testing.T, bp api.BaseParams) {
	t.Helper()
	xargs := xact.ArgsMsg{
		Kind:     apc.ActResilver,
		DaemonID: c.target.ID(),
		Timeout:  resilLongTimeout,
	}
	_, err := api.WaitForSnaps(bp, &xargs, xargs.NotRunning())
	tassert.CheckFatal(t, err)
}

// barrier:
// 1) no running resilver
// 2) mountpaths stable (no waiting-dd)
func (c *resilverStressCtx) barrier(t *testing.T, bp api.BaseParams, _ int /*expectedDisabled*/) {
	t.Helper()
	c.waitNoRunningResilver(t, bp)
	c.waitPostDD(t, bp) // ignore disabled count here (by design)
}

// wait until the target - all mountpaths - is not in the middle of its DD transition
func (c *resilverStressCtx) waitPostDD(t *testing.T, bp api.BaseParams) *apc.MountpathList {
	t.Helper()

	var elapsed time.Duration
	for elapsed < resilLongTimeout {
		mp, err := api.GetMountpaths(bp, c.target)
		tassert.CheckFatal(t, err)

		if len(mp.WaitingDD) == 0 {
			return mp
		}
		time.Sleep(resilLongPoll)
		elapsed += resilLongPoll
	}

	tassert.Fatalf(t, false, "mountpaths did not stabilize (waiting-dd non-empty) within %v", resilLongTimeout)
	return nil
}

// wait until the specified mountpath is not in the middle of its DD transition
// (i.e., is not listed under WaitingDD).
func (c *resilverStressCtx) waitMpathPostDD(t *testing.T, bp api.BaseParams, mp string) {
	t.Helper()

	var elapsed time.Duration
	for elapsed < resilLongTimeout {
		mpl, err := api.GetMountpaths(bp, c.target)
		tassert.CheckFatal(t, err)

		if !slices.Contains(mpl.WaitingDD, mp) {
			return
		}

		time.Sleep(resilShortPoll)
		elapsed += resilShortPoll
	}

	tassert.Fatalf(t, false, "mountpath %q did not stabilize (still in waiting-dd) within %v", mp, resilLongTimeout)
}

// conventional validation using API
func (c *resilverStressCtx) validateAPIOnly(t *testing.T) {
	t.Helper()

	c.reg.t, c.chk.t, c.mir.t, c.ec.t = t, t, t, t

	tlog.Logln("Validating data via API only...")

	// regular
	c.reg.gets(nil, true /*withValidation*/)
	c.reg.ensureNoGetErrors()

	// chunked: validate via GET only
	c.chk.gets(nil, true /*withValidation*/)
	c.chk.ensureNoGetErrors()

	// mirrored
	c.mir.gets(nil, true /*withValidation*/)
	c.mir.ensureNoGetErrors()
	// copies can legitimately be >2 during transitions; keep it lenient here
	c.mir.ensureNumCopies(tools.BaseAPIParams() /*bp*/, 2, true /*greaterOk*/)

	// EC
	c.ec.gets(nil, true /*withValidation*/)
	c.ec.ensureNoGetErrors()
}

func (c *resilverStressCtx) validateAllData(t *testing.T, stage string) {
	t.Helper()

	// ioContext captures `t` - refresh per subtest
	c.reg.t, c.chk.t, c.mir.t, c.ec.t = t, t, t, t

	tlog.Logfln("[%s] Validating all data...", stage)

	// regular
	c.reg.gets(nil, true /*withValidation*/)
	c.reg.ensureNoGetErrors()

	// chunked
	c.chk.gets(nil, true /*withValidation*/)
	c.chk.ensureNoGetErrors()

	// NOTE:
	// validateChunksOnDisk() below uses fs.WalkBck() which requires test-side fs mountpath map.
	// This is only meaningful for local deployments; initMountpaths() will skip otherwise.
	purl := tools.RandomProxyURL(t)
	initMountpaths(t, purl)

	for i, objName := range c.chk.objNames {
		c.chk.validateChunksOnDisk(c.chk.bck, objName, c.chk.chunksConf.numChunks)
		if i > 0 && i%200 == 0 {
			tlog.Logfln("  validated %d/%d chunked objects", i, len(c.chk.objNames))
		}
	}

	// mirrored
	c.mir.gets(nil, true /*withValidation*/)
	c.mir.ensureNoGetErrors()
	c.mir.ensureNumCopies(tools.BaseAPIParams() /*bp*/, 2, true /*greaterOk*/)

	// EC
	c.ec.gets(nil, true /*withValidation*/)
	c.ec.ensureNoGetErrors()

	tlog.Logfln("[%s] All data validated successfully", stage)
}

func createAndPopulateResilverBuckets(t *testing.T, c *resilverStressCtx) {
	// object counts - total ~10K+
	const (
		numRegular  = 4000
		numChunked  = 1000
		numMirrored = 3000
		numEC       = 2000
		numChunks   = 4
	)

	var (
		purl = tools.RandomProxyURL(t)

		// bucket names
		bckRegular  = cmn.Bck{Name: "resilver-regular-" + trand.String(6), Provider: apc.AIS}
		bckChunked  = cmn.Bck{Name: "resilver-chunked-" + trand.String(6), Provider: apc.AIS}
		bckMirrored = cmn.Bck{Name: "resilver-mirrored-" + trand.String(6), Provider: apc.AIS}
		bckEC       = cmn.Bck{Name: "resilver-ec-" + trand.String(6), Provider: apc.AIS}
	)

	tlog.Logln("Creating buckets...")

	tools.CreateBucket(t, purl, bckRegular, nil, true /*cleanup*/)
	tools.CreateBucket(t, purl, bckChunked, nil, true /*cleanup*/)

	// mirrored bucket (2 copies)
	mirrorProps := &cmn.BpropsToSet{
		Mirror: &cmn.MirrorConfToSet{
			Enabled: apc.Ptr(true),
			Copies:  apc.Ptr(int64(2)),
		},
	}
	tools.CreateBucket(t, purl, bckMirrored, mirrorProps, true /*cleanup*/)

	// EC bucket (2 data + 2 parity)
	ecProps := &cmn.BpropsToSet{
		EC: &cmn.ECConfToSet{
			Enabled:      apc.Ptr(true),
			DataSlices:   apc.Ptr(2),
			ParitySlices: apc.Ptr(2),
			ObjSizeLimit: apc.Ptr(int64(0)), // all objects
		},
	}
	tools.CreateBucket(t, purl, bckEC, ecProps, true /*cleanup*/)

	tlog.Logln("Populating buckets with objects...")

	// regular
	c.reg = &ioContext{
		t:             t,
		num:           numRegular,
		bck:           bckRegular,
		proxyURL:      purl,
		fileSizeRange: [2]uint64{cos.KiB, 64 * cos.KiB},
		prefix:        "reg-",
		silent:        false,
	}
	c.reg.init(false)
	c.reg.puts()

	// chunked (multipart)
	c.chk = &ioContext{
		t:        t,
		num:      numChunked,
		bck:      bckChunked,
		proxyURL: purl,
		chunksConf: &ioCtxChunksConf{
			numChunks: numChunks,
			multipart: true,
		},
		fileSizeRange: [2]uint64{4 * cos.MiB, 8 * cos.MiB},
		prefix:        "chunk-",
		silent:        false,
	}
	c.chk.init(false)
	c.chk.puts()

	// mirrored
	c.mir = &ioContext{
		t:             t,
		num:           numMirrored,
		bck:           bckMirrored,
		proxyURL:      purl,
		fileSizeRange: [2]uint64{cos.KiB, 32 * cos.KiB},
		prefix:        "mirror-",
		silent:        false,
	}
	c.mir.init(false)
	c.mir.puts()

	// wait for mirroring
	xargs := xact.ArgsMsg{Kind: apc.ActPutCopies, Bck: bckMirrored, Timeout: resilLongTimeout}
	api.WaitForSnapsIdle(tools.BaseAPIParams(purl), &xargs)

	// EC
	c.ec = &ioContext{
		t:             t,
		num:           numEC,
		bck:           bckEC,
		proxyURL:      purl,
		fileSizeRange: [2]uint64{64 * cos.KiB, 256 * cos.KiB},
		prefix:        "ec-",
		silent:        false,
	}
	c.ec.init(false)
	c.ec.puts()

	// wait for EC encoding
	xargs = xact.ArgsMsg{Kind: apc.ActECPut, Bck: bckEC, Timeout: resilLongTimeout}
	api.WaitForSnapsIdle(tools.BaseAPIParams(purl), &xargs)

	totalObjects := numRegular + numChunked + numMirrored + numEC
	tlog.Logfln("All %d objects created across 4 buckets", totalObjects)
}

// ---------------- stress test cases: scenarios A through E -------------------------

func testAdisableDisablePreempt(t *testing.T, c *resilverStressCtx) {
	purl := tools.RandomProxyURL(t)
	bp := tools.BaseAPIParams(purl)

	tlog.Logln("=== disable -> disable (preemption) ===")

	mp1, mp2 := c.origMpaths[0], c.origMpaths[1]

	// precondition: stable + all enabled
	c.barrier(t, bp, 0)

	tlog.Logfln("Disabling %s...", mp1)
	err := api.DisableMountpath(bp, c.target, mp1, false /*dontResilver*/)
	tassert.CheckFatal(t, err)

	// wait for DD transition before stacking another disable
	c.waitMpathPostDD(t, bp, mp1)

	tlog.Logfln("Disabling %s (should preempt R1)...", mp2)
	err = api.DisableMountpath(bp, c.target, mp2, false /*dontResilver*/)
	tassert.CheckFatal(t, err)

	// while mountpaths are disabled, some objects/chunks may be inaccessible
	c.barrier(t, bp, -1)
	tlog.Logln("Mountpaths disabled; skipping validation until re-enable")

	tlog.Logfln("Re-enabling %s and %s...", mp1, mp2)
	err = api.EnableMountpath(bp, c.target, mp1)
	tassert.CheckFatal(t, err)
	err = api.EnableMountpath(bp, c.target, mp2)
	tassert.CheckFatal(t, err)

	c.waitResilverFinishAny(t, bp)

	ensureNumMountpaths(t, c.target, &apc.MountpathList{Available: c.origMpaths})

	c.validateAllData(t, "after-A-reenable")
	c.validateAPIOnly(t)
}

func testBdisableEnableToggle(t *testing.T, c *resilverStressCtx) {
	t.Skipf("skipping %s - not ready yet", t.Name()) // TODO: enable

	purl := tools.RandomProxyURL(t)
	bp := tools.BaseAPIParams(purl)

	tlog.Logln("=== disable -> enable (quick toggle) ===")

	mp1 := c.origMpaths[0]

	tlog.Logfln("Disabling %s...", mp1)
	err := api.DisableMountpath(bp, c.target, mp1, false /*dontResilver*/)
	tassert.CheckFatal(t, err)

	r1id := c.waitResilverStartOnTarget(t, bp)
	tlog.Logfln("R1 started: %s", r1id)

	time.Sleep(500 * time.Millisecond)

	tlog.Logfln("Re-enabling %s (should preempt R1)...", mp1)
	err = api.EnableMountpath(bp, c.target, mp1)
	tassert.CheckFatal(t, err)

	c.waitResilverFinishAny(t, bp)

	// if still queryable, check for 'aborted'
	xs := c.queryResilver(t, bp, false /*onlyRunning*/)
	if snap, ok := c.getSnap(t, xs, r1id); ok {
		switch {
		case snap.IsRunning():
			tlog.Logfln("R1 %s is still running (unexpected)", r1id)
		case snap.IsAborted():
			tlog.Logfln("R1 %s was aborted as expected (abort-err=%q err=%q)", r1id, snap.AbortErr, snap.Err)
		default:
			tlog.Logfln("R1 %s finished naturally (was fast)", r1id)
		}
	} else {
		tlog.Logfln("R1 %s not found in snaps (likely aged out / finished fast)", r1id)
	}

	ensureNumMountpaths(t, c.target, &apc.MountpathList{Available: c.origMpaths})
	c.validateAllData(t, "after-B")

	c.validateAPIOnly(t)
}

func testCdetachAttachCycle(t *testing.T, c *resilverStressCtx) {
	t.Skipf("skipping %s - not ready yet", t.Name()) // TODO: enable
	purl := tools.RandomProxyURL(t)
	bp := tools.BaseAPIParams(purl)

	tlog.Logln("=== detach -> attach (full cycle) ===")

	mp1 := c.origMpaths[0]

	tlog.Logfln("Detaching %s...", mp1)
	err := api.DetachMountpath(bp, c.target, mp1, false /*dontResilver*/)
	tassert.CheckFatal(t, err)

	c.waitMpathNotAvailable(t, bp, mp1)
	c.waitResilverFinishIfStarted(t, bp, "after detach")

	c.validateAllData(t, "after-C-detach")

	tlog.Logfln("Attaching %s...", mp1)
	err = api.AttachMountpath(bp, c.target, mp1, "")
	tassert.CheckFatal(t, err)

	c.waitMpathAvailable(t, bp, mp1)
	c.waitResilverFinishIfStarted(t, bp, "after attach")
	ensureNumMountpaths(t, c.target, &apc.MountpathList{Available: c.origMpaths})
	c.validateAllData(t, "after-C")

	c.validateAPIOnly(t)
}

func testDrapidFire(t *testing.T, c *resilverStressCtx) {
	t.Skipf("skipping %s - not ready yet", t.Name()) // TODO: enable

	purl := tools.RandomProxyURL(t)
	bp := tools.BaseAPIParams(purl)

	tlog.Logln("=== rapid fire (stress) ===")

	mp1, mp2, mp3 := c.origMpaths[0], c.origMpaths[1], c.origMpaths[2]

	tlog.Logfln("Rapid fire: disable %s, disable %s, disable %s...", mp1, mp2, mp3)

	err := api.DisableMountpath(bp, c.target, mp1, false)
	tassert.CheckFatal(t, err)
	time.Sleep(200 * time.Millisecond)

	err = api.DisableMountpath(bp, c.target, mp2, false)
	tassert.CheckFatal(t, err)
	time.Sleep(200 * time.Millisecond)

	err = api.DisableMountpath(bp, c.target, mp3, false)
	tassert.CheckFatal(t, err)

	c.waitResilverFinishAny(t, bp)
	c.validateAllData(t, "after-D-disable")

	tlog.Logfln("Rapid fire: enable %s, enable %s, enable %s...", mp1, mp2, mp3)

	err = api.EnableMountpath(bp, c.target, mp1)
	tassert.CheckFatal(t, err)
	time.Sleep(200 * time.Millisecond)

	err = api.EnableMountpath(bp, c.target, mp2)
	tassert.CheckFatal(t, err)
	time.Sleep(200 * time.Millisecond)

	err = api.EnableMountpath(bp, c.target, mp3)
	tassert.CheckFatal(t, err)

	c.waitResilverFinishAny(t, bp)
	ensureNumMountpaths(t, c.target, &apc.MountpathList{Available: c.origMpaths})
	c.validateAllData(t, "after-D")

	c.validateAPIOnly(t)
}

func testEnoResilverGuard(t *testing.T, c *resilverStressCtx) {
	t.Skipf("skipping %s - not ready yet", t.Name()) // TODO: enable

	purl := tools.RandomProxyURL(t)
	bp := tools.BaseAPIParams(purl)

	tlog.Logln("=== --no-resilver with active resilver (should fail) ===")

	mp1, mp2 := c.origMpaths[0], c.origMpaths[1]

	tlog.Logfln("Disabling %s...", mp1)
	err := api.DisableMountpath(bp, c.target, mp1, false /*dontResilver*/)
	tassert.CheckFatal(t, err)

	_ = c.waitResilverStartOnTarget(t, bp)

	tlog.Logfln("Attempting disable %s with --no-resilver (should fail)...", mp2)
	err = api.DisableMountpath(bp, c.target, mp2, true /*dontResilver*/)
	tassert.Errorf(t, err != nil, "expected error when using --no-resilver with active resilver")
	if err != nil {
		tlog.Logfln("Got expected error: %v", err)
	}

	c.waitResilverFinishAny(t, bp)

	tlog.Logfln("Disabling %s with --no-resilver (should succeed now)...", mp2)
	err = api.DisableMountpath(bp, c.target, mp2, true /*dontResilver*/)
	tassert.CheckFatal(t, err)

	err = api.EnableMountpath(bp, c.target, mp1)
	tassert.CheckFatal(t, err)
	err = api.EnableMountpath(bp, c.target, mp2)
	tassert.CheckFatal(t, err)

	c.waitResilverFinishAny(t, bp)
	ensureNumMountpaths(t, c.target, &apc.MountpathList{Available: c.origMpaths})
	c.validateAllData(t, "after-E")

	c.validateAPIOnly(t)
}

//
// older (legacy) tests ------------------------------------------------------------------------------------
//

func TestSingleResilver(t *testing.T) {
	m := ioContext{t: t}
	m.initAndSaveState(true /*cleanup*/)
	baseParams := tools.BaseAPIParams(m.proxyURL)

	// Select a random target
	target, _ := m.smap.GetRandTarget()

	// Start resilvering just on the target
	args := xact.ArgsMsg{Kind: apc.ActResilver, DaemonID: target.ID()}
	id, err := api.StartXaction(baseParams, &args, "")
	tassert.CheckFatal(t, err)

	// Wait for specific resilvering x[id]
	args = xact.ArgsMsg{ID: id, Kind: apc.ActResilver, Timeout: tools.RebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, &args)
	tassert.CheckFatal(t, err)

	// Make sure other nodes were not resilvered
	args = xact.ArgsMsg{ID: id}
	snaps, err := api.QueryXactionSnaps(baseParams, &args)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(snaps) == 1, "expected only 1 resilver")
}

func TestGetDuringResilver(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	var (
		m = ioContext{
			t:   t,
			num: 20000,
		}
		baseParams = tools.BaseAPIParams()
	)

	m.initAndSaveState(true /*cleanup*/)
	m.expectTargets(1)

	tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

	target, _ := m.smap.GetRandTarget()
	mpList, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)
	ensureNoDisabledMountpaths(t, target, mpList)

	if len(mpList.Available) < 2 {
		t.Fatal("Must have at least 2 mountpaths")
	}

	// select up to 2 mountpath
	mpaths := []string{mpList.Available[0]}
	if len(mpList.Available) > 2 {
		mpaths = append(mpaths, mpList.Available[1])
	}

	// Disable mountpaths temporarily
	for _, mp := range mpaths {
		err = api.DisableMountpath(baseParams, target, mp, false /*dont-resil*/)
		tassert.CheckFatal(t, err)
	}

	m.puts()

	// Start getting objects and enable mountpaths in parallel
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.getsUntilStop()
	}()

	for _, mp := range mpaths {
		time.Sleep(time.Second)
		err = api.EnableMountpath(baseParams, target, mp)
		tassert.CheckFatal(t, err)
	}
	m.stopGets()

	wg.Wait()
	time.Sleep(2 * time.Second)

	tlog.Logfln("Wait for rebalance (when target %s that has previously lost all mountpaths joins back)", target.StringEx())
	args := xact.ArgsMsg{Kind: apc.ActRebalance, Timeout: tools.RebalanceTimeout}
	_, _ = api.WaitForXactionIC(baseParams, &args)

	tools.WaitForResilvering(t, baseParams, nil)

	m.ensureNoGetErrors()
	m.ensureNumMountpaths(target, mpList)
}

func TestResilverAfterAddingMountpath(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
	var (
		m = ioContext{
			t:               t,
			num:             5000,
			numGetsEachFile: 2,
		}
		baseParams = tools.BaseAPIParams()
	)

	m.initAndSaveState(true /*cleanup*/)
	m.expectTargets(1)
	target, _ := m.smap.GetRandTarget()
	mpList, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)
	ensureNoDisabledMountpaths(t, target, mpList)

	tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)

	if docker.IsRunning() {
		err := docker.CreateMpathDir(0, testMpath)
		tassert.CheckFatal(t, err)
	} else {
		err := cos.CreateDir(testMpath)
		tassert.CheckFatal(t, err)
	}

	defer func() {
		if !docker.IsRunning() {
			os.RemoveAll(testMpath)
		}
	}()

	m.puts()

	// Add new mountpath to target
	tlog.Logfln("attach new %q at target %s", testMpath, target.StringEx())
	err = api.AttachMountpath(baseParams, target, testMpath)
	tassert.CheckFatal(t, err)

	tools.WaitForResilvering(t, baseParams, target)

	m.gets(nil, false)

	// Remove new mountpath from target
	tlog.Logfln("detach %q from target %s", testMpath, target.StringEx())
	if docker.IsRunning() {
		if err := api.DetachMountpath(baseParams, target, testMpath, false /*dont-resil*/); err != nil {
			t.Error(err.Error())
		}
	} else {
		err = api.DetachMountpath(baseParams, target, testMpath, false /*dont-resil*/)
		tassert.CheckFatal(t, err)
	}

	m.ensureNoGetErrors()

	tools.WaitForResilvering(t, baseParams, target)
	m.ensureNumMountpaths(target, mpList)
}

// Simple resilver for EC bucket
//  1. Create a bucket
//  2. Remove mpath from one target
//  3. Creates enough objects to have at least one per mpath
//     So, minimal is <target count>*<mpath count>*2.
//     For tests 100 looks good
//  4. Attach removed mpath
//  5. Wait for rebalance to finish
//  6. Check that all objects returns the non-zero number of Data and Parity
//     slices in HEAD response
//  7. Extra check: the number of objects after rebalance equals initial number
func TestECResilver(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     testBucketName + "-ec-resilver",
			Provider: apc.AIS,
		}
		proxyURL = tools.RandomProxyURL()
	)
	o := &ecOptions{
		objCount:     100,
		concurrency:  8,
		pattern:      "obj-reb-loc-%04d",
		silent:       true,
		objSizeLimit: ecObjLimit,
	}
	o.init(t, proxyURL)
	initMountpaths(t, proxyURL)

	for _, test := range ecTests {
		t.Run(test.name, func(t *testing.T) {
			if o.smap.CountActiveTs() <= test.parity+test.data {
				t.Skip(cmn.ErrNotEnoughTargets)
			}
			o.parityCnt = test.parity
			o.dataCnt = test.data
			o.objSizeLimit = test.objSizeLimit
			ecResilver(t, o, proxyURL, bck)
		})
	}
}

func ecResilver(t *testing.T, o *ecOptions, proxyURL string, bck cmn.Bck) {
	baseParams := tools.BaseAPIParams(proxyURL)

	newLocalBckWithProps(t, baseParams, bck, defaultECBckProps(o), o)

	tgtList := o.smap.Tmap.ActiveNodes()
	tgtLost := tgtList[0]
	lostFSList, err := api.GetMountpaths(baseParams, tgtLost)
	tassert.CheckFatal(t, err)
	if len(lostFSList.Available) < 2 {
		t.Fatalf("%s has only %d mountpaths, required 2 or more", tgtLost.ID(), len(lostFSList.Available))
	}
	lostPath := lostFSList.Available[0]
	err = api.DetachMountpath(baseParams, tgtLost, lostPath, false /*dont-resil*/)
	tassert.CheckFatal(t, err)
	time.Sleep(time.Second)

	wg := sync.WaitGroup{}

	wg.Add(o.objCount)
	for i := range o.objCount {
		go func(i int) {
			defer wg.Done()
			objName := fmt.Sprintf(o.pattern, i)
			createECObject(t, baseParams, bck, objName, i, o)
		}(i)
	}
	wg.Wait()
	tlog.Logfln("Created %d objects", o.objCount)

	err = api.AttachMountpath(baseParams, tgtLost, lostPath)
	tassert.CheckFatal(t, err)
	// loop above may fail (even if AddMountpath works) and mark a test failed
	if t.Failed() {
		t.FailNow()
	}

	tools.WaitForResilvering(t, baseParams, nil)

	msg := &apc.LsoMsg{Props: apc.GetPropsSize}
	resEC, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tlog.Logfln("%d objects in %s after rebalance", len(resEC.Entries), bck.String())
	if len(resEC.Entries) != o.objCount {
		t.Errorf("Expected %d objects after rebalance, found %d", o.objCount, len(resEC.Entries))
	}

	for i := range o.objCount {
		objName := ecTestDir + fmt.Sprintf(o.pattern, i)
		hargs := api.HeadArgs{FltPresence: apc.FltPresent}
		props, err := api.HeadObject(baseParams, bck, objName, hargs)
		if err != nil {
			t.Errorf("HEAD for %s failed: %v", objName, err)
		} else if props.EC.DataSlices == 0 || props.EC.ParitySlices == 0 {
			t.Errorf("%s has not EC info", objName)
		}
	}
}
