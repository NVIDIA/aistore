// Package integration_test.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
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

func (m *ioContext) ensureNoGetErrors() {
	m.t.Helper()
	if m.numGetErrs.Load() > 0 {
		m.t.Fatalf("Number of get errors is non-zero: %d\n", m.numGetErrs.Load())
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

func ensureNumMountpaths(t *testing.T, target *meta.Snode, mpList *apc.MountpathList) {
	t.Helper()
	tname := target.StringEx()
	baseParams := tools.BaseAPIParams()
	mpl, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)
	for range 6 {
		if len(mpl.Available) == len(mpList.Available) &&
			len(mpl.Disabled) == len(mpList.Disabled) &&
			len(mpl.WaitingDD) == len(mpList.WaitingDD) {
			break
		}
		time.Sleep(time.Second)
	}
	if len(mpl.Available) != len(mpList.Available) {
		t.Errorf("%s ended up with %d mountpaths (dd=%v, disabled=%v), expecting: %d",
			tname, len(mpl.Available), mpl.WaitingDD, mpl.Disabled, len(mpList.Available))
	} else if len(mpl.Disabled) != len(mpList.Disabled) || len(mpl.WaitingDD) != len(mpList.WaitingDD) {
		t.Errorf("%s ended up with (dd=%v, disabled=%v) mountpaths, expecting (%v and %v), respectively",
			tname, mpl.WaitingDD, mpl.Disabled, mpList.WaitingDD, mpList.Disabled)
	}
}

func ensureNoDisabledMountpaths(t *testing.T, target *meta.Snode, mpList *apc.MountpathList) {
	t.Helper()
	for range 6 {
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
