// Package integration_test.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/xact"
)

const (
	lcNumObjs  = 10000
	lcObjSize  = 1024
	lcProbeTag = "membership-probe:"
)

// TestMembershipBusy verifies the handoff between the short primary-side
// admission transaction and its causal rebalance listener. Once the listener
// is armed, every administrative membership operation, including an exact
// inverse, must be rejected until the rebalance finishes.
func TestMembershipBusy(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true, MinTargets: 4})

	var (
		bck = cmn.Bck{Name: "lc-busy", Provider: apc.AIS}
		m   = &ioContext{
			t:         t,
			num:       lcNumObjs,
			fileSize:  lcObjSize,
			fixedSize: true,
			bck:       bck,
			silent:    true,
		}
		bp = tools.BaseAPIParams(proxyURL)
	)
	m.initAndSaveState(true /*cleanup*/)
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	m.puts()

	nodes := lcSelectTargets(t, 3)
	a, b, c := nodes[0], nodes[1], nodes[2]
	armed := lcIDs(nodes[:2]) // {A, B}

	var (
		rebID    string
		restored bool
	)
	defer func() {
		if !restored {
			// A stop-maintenance issued while the original rebalance is still
			// running is deliberately rejected, so drain it before cleanup.
			if rebID != "" {
				tools.WaitForRebalanceByID(t, bp, rebID)
			}
			lcRestore(t, bp, armed)
		}
		ensureMembershipAdmits(t, bp)
	}()

	rebID = armStartMaint(t, bp, armed...)
	if !stillArmed(t, bp, rebID) {
		tlog.Logfln("Skip: rebalance[%s] already finished - nothing to be refused by", rebID)
		return
	}

	reversed := []string{b.ID(), a.ID()}
	rejects := []struct {
		name string
		call func() (string, error)
	}{
		{
			name: "stop-maintenance exact inverse {B,A}",
			call: func() (string, error) { return lcStopMaint(bp, reversed...) },
		},
		{
			name: "stop-maintenance subset {A}",
			call: func() (string, error) { return lcStopMaint(bp, a.ID()) },
		},
		{
			name: "stop-maintenance superset {A,B,C}",
			call: func() (string, error) { return lcStopMaint(bp, a.ID(), b.ID(), c.ID()) },
		},
		{
			name: "stop-maintenance disjoint {C}",
			call: func() (string, error) { return lcStopMaint(bp, c.ID()) },
		},
		{
			name: "start-maintenance {C}",
			call: func() (string, error) { return lcStartMaint(bp, c.ID()) },
		},
		{
			name: "decommission {C}",
			call: func() (string, error) { return lcDecommission(bp, c.ID()) },
		},
		{
			name: "stop-maintenance exact inverse --skip-rebalance",
			call: func() (string, error) { return lcStopMaintNoReb(bp, armed...) },
		},
		{
			name: "explicit rebalance",
			call: func() (string, error) { return lcStartRebalance(bp) },
		},
		{
			name: "explicit rebalance --cleanup",
			call: func() (string, error) {
				args := &xact.ArgsMsg{
					Kind:  apc.ActRebalance,
					Flags: xact.FlagRemoveMisplaced,
				}
				return api.StartXaction(bp, args, "")
			},
		},
	}

	for _, tc := range rejects {
		if !stillArmed(t, bp, rebID) {
			tlog.Logfln("Skip %q: rebalance[%s] finished mid-matrix", tc.name, rebID)
			break
		}

		_, err := tc.call()
		busy := isMembershipBusy(err)
		tassert.Errorf(t, busy,
			"%s: expected ErrBusy while rebalance[%s] is armed, got %v", tc.name, rebID, err)
		if busy {
			tlog.Logfln("Rejected (as expected) %s: %v", tc.name, err)
		}
	}

	// The inverse becomes an ordinary admissible transaction only after the
	// original causal rebalance has reached a terminal state.
	tools.WaitForRebalanceByID(t, bp, rebID)
	restored = lcRestore(t, bp, armed)
	if !restored {
		return
	}

	ensureNoRunningReb(t, bp)
}

func isMembershipBusy(err error) bool {
	herr := cmn.AsErrHTTP(err)
	return herr != nil && herr.TypeCode == "ErrBusy"
}

// lcRestore brings nodes back and drains the resulting rebalance.
func lcRestore(t *testing.T, bp api.BaseParams, sids []string) bool {
	t.Helper()
	rebID, err := lcStopMaint(bp, sids...)
	if err != nil {
		tassert.CheckError(t, err)
		return false
	}
	if rebID != "" {
		tools.WaitForRebalanceByID(t, bp, rebID)
	}
	return true
}

// lcSelectTargets returns num active targets.
func lcSelectTargets(t *testing.T, num int) []*meta.Snode {
	t.Helper()
	smap := tools.GetClusterMap(t, proxyURL)
	nodes := make([]*meta.Snode, 0, num)
	for _, si := range smap.Tmap.ActiveNodes() {
		nodes = append(nodes, si)
		if len(nodes) == num {
			break
		}
	}
	tassert.Fatalf(t, len(nodes) == num, "expecting %d active targets, got %d", num, len(nodes))
	return nodes
}

func lcIDs(nodes []*meta.Snode) []string {
	sids := make([]string, 0, len(nodes))
	for _, si := range nodes {
		sids = append(sids, si.ID())
	}
	return sids
}

func lcStartMaint(bp api.BaseParams, sids ...string) (string, error) {
	args := &apc.ActValRmNode{}
	args.SetIDs(sids...)
	return api.StartMaintenance(bp, args)
}

func lcStopMaint(bp api.BaseParams, sids ...string) (string, error) {
	args := &apc.ActValRmNode{}
	args.SetIDs(sids...)
	return api.StopMaintenance(bp, args)
}

func lcStopMaintNoReb(bp api.BaseParams, sids ...string) (string, error) {
	args := &apc.ActValRmNode{SkipRebalance: true}
	args.SetIDs(sids...)
	return api.StopMaintenance(bp, args)
}

func lcDecommission(bp api.BaseParams, sids ...string) (string, error) {
	args := &apc.ActValRmNode{}
	args.SetIDs(sids...)
	return api.DecommissionNode(bp, args)
}

func lcStartRebalance(bp api.BaseParams) (string, error) {
	return api.StartXaction(bp, &xact.ArgsMsg{Kind: apc.ActRebalance}, "")
}

// put nodes in maintenance and returns the causal rebalance ID.
func armStartMaint(t *testing.T, bp api.BaseParams, sids ...string) string {
	t.Helper()
	rebID, err := lcStartMaint(bp, sids...)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, rebID != "", "expecting start-maintenance %v to trigger rebalance", sids)
	tlog.Logfln("Armed: start-maintenance %v => rebalance[%s]", sids, rebID)
	return rebID
}

// check whether the rebalance listener remains running
func stillArmed(t *testing.T, bp api.BaseParams, rebID string) bool {
	t.Helper()
	if rebID == "" {
		return false
	}
	xargs := xact.ArgsMsg{ID: rebID, Kind: apc.ActRebalance, OnlyRunning: true}
	status, err := api.GetOneXactionStatus(bp, &xargs)
	if err != nil {
		tassert.Fatalf(t, cmn.IsStatusNotFound(err), "rebalance[%s] status: %v", rebID, err)
		return false // reaped
	}
	return !status.IsFinished()
}

// use a no-op stop-maintenance request to verify that
// neither the short admission bit nor a running rebalance remains
func ensureMembershipAdmits(t *testing.T, bp api.BaseParams) {
	t.Helper()
	smap := tools.GetClusterMap(t, proxyURL)
	var tsi *meta.Snode
	for _, si := range smap.Tmap {
		if !si.InMaintOrDecomm() {
			tsi = si
			break
		}
	}
	tassert.Fatalf(t, tsi != nil, "%s no active target in %s", lcProbeTag, smap.StringEx())

	rebID, err := lcStopMaint(bp, tsi.ID())
	tassert.Errorf(t, err == nil, "%s lifecycle admission is stranded: %v", lcProbeTag, err)
	tassert.Errorf(t, rebID == "", "%s expecting a no-op, got rebalance[%s]", lcProbeTag, rebID)
}

func ensureNoRunningReb(t *testing.T, bp api.BaseParams) {
	t.Helper()
	running, err := api.GetAllRunningXactions(bp, apc.ActRebalance)
	tassert.CheckError(t, err)
	tassert.Errorf(t, len(running) == 0, "expecting no running rebalance, got %v", running)
}
