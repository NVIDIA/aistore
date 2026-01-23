// Package integration_test.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/xact"

	jsoniter "github.com/json-iterator/go"
)

const (
	retriesPollMpaths = 8
)

func (m *ioContext) ensureNoGetErrors() {
	m.t.Helper()
	if m.numGetErrs.Load() > 0 {
		m.t.Fatalf("Number of get errors: %d\n", m.numGetErrs.Load())
	}
}

func (m *ioContext) ensureNumCopies(bp api.BaseParams, expectedCopies int, greaterOk bool) {
	m.t.Helper()
	time.Sleep(time.Second)
	xargs := xact.ArgsMsg{Kind: apc.ActMakeNCopies, Bck: m.bck, Timeout: tools.RebalanceTimeout}
	_, err := api.WaitForXactionIC(bp, &xargs)

	if err != nil && isErrNotFound(err) {
		tlog.Logfln("Warning: (kind %s, bucket %s), err: %v", apc.ActMakeNCopies, m.bck.Cname(""), err)
		err = nil
	}
	tassert.CheckFatal(m.t, err)

	// List Bucket - primarily for the copies
	msg := &apc.LsoMsg{Flags: apc.LsCached, Prefix: m.prefix}
	msg.AddProps(apc.GetPropsCopies, apc.GetPropsAtime, apc.GetPropsStatus)
	objectList, err := api.ListObjects(bp, m.bck, msg, api.ListArgs{})
	tassert.CheckFatal(m.t, err)

	total := 0
	copiesToNumObjects := make(map[int]int)
	for _, entry := range objectList.Entries {
		if entry.Atime == "" {
			m.t.Errorf("%s: access time is empty", m.bck.Cname(entry.Name))
		}
		total++
		if greaterOk && int(entry.Copies) > expectedCopies {
			copiesToNumObjects[expectedCopies]++
		} else {
			copiesToNumObjects[int(entry.Copies)]++
		}
	}
	tlog.Logfln("objects (total, copies) = (%d, %v)", total, copiesToNumObjects)
	if total != m.num {
		m.t.Errorf("list_objects: expecting %d objects, got %d", m.num, total)
	}

	if len(copiesToNumObjects) != 1 {
		s, _ := jsoniter.MarshalIndent(copiesToNumObjects, "", " ")
		m.t.Errorf("some objects do not have expected number of copies: %s", s)
	}

	for copies := range copiesToNumObjects {
		if copies != expectedCopies {
			m.t.Errorf("Expecting %d objects all to have %d replicas, got: %d", total, expectedCopies, copies)
		}
	}
}

func (m *ioContext) ensureNumMountpaths(target *meta.Snode, mpList *apc.MountpathList) {
	ensureNumMountpaths(m.t, target, mpList)
}

const (
	tagRecoverMpaths = "post test-fail recovery"
)

func getCompareMpaths(t *testing.T, bp api.BaseParams, target *meta.Snode, mpList *apc.MountpathList) (mpl *apc.MountpathList, ok bool) {
	t.Helper()

	var err error
	mpl, err = api.GetMountpaths(bp, target)
	tassert.CheckFatal(t, err)

	ok = len(mpl.Available) == len(mpList.Available) &&
		len(mpl.Disabled) == len(mpList.Disabled) &&
		len(mpl.WaitingDD) == len(mpList.WaitingDD)
	return mpl, ok
}

func ensureNumMountpaths(t *testing.T, target *meta.Snode, mpList *apc.MountpathList) {
	t.Helper()
	var (
		errRec   error
		mpl      *apc.MountpathList
		tname    = target.StringEx()
		bp       = tools.BaseAPIParams()
		ok       bool
		recovery bool
	)
	for i := 0; i < retriesPollMpaths && !ok; i++ {
		mpl, ok = getCompareMpaths(t, bp, target, mpList)
		if !ok {
			time.Sleep(time.Second)
		}
	}

	if len(mpl.Available) != len(mpList.Available) {
		endedUp := fmt.Sprintf("%s ended up with %d mountpaths (dd=%v, disabled=%v), expecting: %d",
			tname, len(mpl.Available), mpl.WaitingDD, mpl.Disabled, len(mpList.Available))
		if !bestEffortReenableDisabledMPs(t, bp, target, mpList, mpl) {
			t.Error(endedUp)
			return
		}
		tlog.Logfln("Warning: %s", endedUp)

		recovery = true
		tlog.Logfln("%s: wait resilver started on %s", tagRecoverMpaths, tname)
		xargs := xact.ArgsMsg{
			Kind:        apc.ActResilver,
			DaemonID:    target.ID(),
			OnlyRunning: false,
			Timeout:     resilShortTimeout,
		}
		if _, err := api.WaitForSnaps(bp, &xargs, xargs.Started()); err != nil {
			tlog.Logfln("Warning: %v", err)
		}

		tlog.Logfln("%s: wait resilver finished on %s", tagRecoverMpaths, tname)
		xargs.Timeout = resilLongTimeout
		xargs.OnlyRunning = true // redundant - xargs.Finished() always sets it

		if _, errRec = api.WaitForSnaps(bp, &xargs, xargs.Finished()); errRec != nil {
			tlog.Logfln("Warning: %v", errRec)
		}
	} else if len(mpl.Disabled) != len(mpList.Disabled) || len(mpl.WaitingDD) != len(mpList.WaitingDD) {
		t.Errorf("%s ended up with (dd=%v, disabled=%v) mountpaths, expecting (%v and %v), respectively",
			tname, mpl.WaitingDD, mpl.Disabled, mpList.WaitingDD, mpList.Disabled)
	}

	// ex post facto
	if recovery {
		if _, ok := getCompareMpaths(t, bp, target, mpList); ok {
			tlog.Logfln("%s: success", tagRecoverMpaths)
		} else {
			tassert.CheckError(t, fmt.Errorf("%s failure: %v", tagRecoverMpaths, errRec))
		}
	}
}

func bestEffortReenableDisabledMPs(t *testing.T, bp api.BaseParams, target *meta.Snode,
	expected, actual *apc.MountpathList) (enabled bool) {
	t.Helper()

	tname := target.StringEx()
	for _, mp := range expected.Available {
		if slices.Contains(actual.Available, mp) {
			continue
		}
		if !slices.Contains(actual.Disabled, mp) {
			continue // NOTE: won't recover detached mountpaths
		}
		if err := api.EnableMountpath(bp, target, mp); err != nil {
			tlog.Logfln("%s: failed to enable %q on %s: %v", tagRecoverMpaths, mp, tname, err)
		} else {
			tlog.Logfln("%s: enabled %q on %s", tagRecoverMpaths, mp, tname)
			enabled = true
		}
	}
	return enabled
}

func ensureNoDisabledMountpaths(t *testing.T, target *meta.Snode, mpList *apc.MountpathList) {
	t.Helper()
	for range retriesPollMpaths {
		if len(mpList.WaitingDD) == 0 && len(mpList.Disabled) == 0 {
			break
		}
		time.Sleep(time.Second)
	}
	if len(mpList.WaitingDD) != 0 || len(mpList.Disabled) != 0 {
		t.Fatalf("%s: disabled mountpaths at the start of the %q (avail=%d, dd=%v, disabled=%v)\n",
			target.StringEx(), t.Name(), len(mpList.Available), mpList.WaitingDD, mpList.Disabled)
	}
}

// background: shuffle=on increases the chance to have still-running rebalance
// at the beginning of a new rename, rebalance, copy-bucket and similar
func ensurePrevRebalanceIsFinished(bp api.BaseParams, err error) bool {
	herr, ok := err.(*cmn.ErrHTTP)
	if !ok {
		return false
	}
	if herr.TypeCode != "ErrLimitedCoexistence" {
		return false
	}

	tools.PromptWaitOnHerr(herr)

	args := xact.ArgsMsg{Kind: apc.ActRebalance, Timeout: tools.RebalanceTimeout}
	_, _ = api.WaitForXactionIC(bp, &args)
	time.Sleep(5 * time.Second)
	return true
}

// TODO: consider
// - tassert/fail instead of warnings
// - usage via t.Cleanup() in rebalancing tests
func ensureICvsSnapsConsistency(bp api.BaseParams, action string) {
	xargs := xact.ArgsMsg{Kind: action, OnlyRunning: true, Timeout: tools.RebalanceTimeout}
	snaps, err := api.QueryXactionSnaps(bp, &xargs)
	if err != nil {
		return
	}
	uuids := snaps.GetUUIDs()

	ns, err := api.GetOneXactionStatus(bp, &xargs)
	if err == nil {
		if !slices.Contains(uuids, ns.UUID) {
			tlog.Logfln("Warning: Snaps/IC APIs disagree: %v vs %s", uuids, ns)
		}
		return
	}

	if isErrNotFound(err) && len(uuids) == 0 {
		return
	}

	tlog.Logfln("Warning: Snaps/IC API disagree: %v vs %v", uuids, err)
}
