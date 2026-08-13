// Package integration_test.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
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
)

const (
	lcNumObjs  = 10000
	lcObjSize  = 1024
	lcProbeTag = "membership-probe:"
)

func TestMaintenanceRepeatNoPostReb(t *testing.T) {
	m := ioContext{t: t}
	m.initAndSaveState(true /*cleanup*/)
	m.expectTargets(1)
	target := m.startMaintenanceNoRebalance()

	// Repeat on a target whose maintenance transition has no post-rebalance step
	// (see "Incomplete Transitions" in docs/lifecycle_node.md).
	smap1 := tools.GetClusterMap(t, m.proxyURL)
	lcAssertMaint(t, smap1, target.ID(), false /*postReb*/)

	tlog.Logfln("Trying to put this same %s in maintenance (expecting a no-op)", target.StringEx())
	args := &apc.ActValRmNode{DaemonID: target.ID(), SkipRebalance: true}
	rebID, err := api.StartMaintenance(tools.BaseAPIParams(m.proxyURL), args)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, rebID == "", "expected a no-op, got rebalance[%s]", rebID)

	smap2 := tools.GetClusterMap(t, m.proxyURL)
	tassert.Errorf(t, smap1.Version == smap2.Version,
		"expected Smap v%d unchanged, got v%d", smap1.Version, smap2.Version)
	tassert.Errorf(t, smap2.CountActiveTs() == m.originalTargetCount-1,
		"expected %d active targets, got %d", m.originalTargetCount-1, smap2.CountActiveTs())

	// Bring the cluster back to its original state.
	rebID = m.stopMaintenance(target)
	m.waitAndCheckCluState()
	tools.WaitForRebalanceByID(t, tools.BaseAPIParams(m.proxyURL), rebID)
}

// TestMaintenanceMixedBatch exercises {no-post-rebalance A, active B}. The
// active target takes the entire batch through the regular rebalance path, and
// its post-rebalance step must complete both maintenance transitions.
func TestMaintenanceMixedBatch(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true, MinTargets: 3})

	var (
		bp   = tools.BaseAPIParams(proxyURL)
		smap = tools.GetClusterMap(t, proxyURL)
		pCnt = smap.CountActivePs()
		tCnt = smap.CountActiveTs()
	)
	nodes := lcSelectTargets(t, 2)
	a, b := nodes[0], nodes[1]
	sids := lcIDs(nodes)
	restored := false
	defer func() {
		if !restored {
			tools.WaitForRebalAndResil(t, bp)
			lcRestore(t, bp, sids)
		}
		ensureMembershipAdmits(t, bp)
	}()

	rebID, err := lcStartMaintNoReb(bp, a.ID())
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, rebID == "", "start-maintenance --no-rebalance returned %q", rebID)

	smap, err = tools.WaitForClusterState(proxyURL, "first target in maintenance",
		smap.Version, pCnt, tCnt-1)
	tassert.CheckFatal(t, err)
	lcAssertMaint(t, smap, a.ID(), false /*postReb*/)

	rebID = armStartMaint(t, bp, a.ID(), b.ID())
	tools.WaitForRebalanceByID(t, bp, rebID)
	smap = lcWaitMaintPostReb(t, bp, sids...)
	tassert.Errorf(t, smap.CountActiveTs() == tCnt-2,
		"expected %d active targets after batch maintenance, got %d", tCnt-2, smap.CountActiveTs())

	restored = lcRestore(t, bp, sids)
	if !restored {
		return
	}
	smap, err = tools.WaitForClusterState(proxyURL, "mixed batch restored",
		smap.Version, pCnt, tCnt)
	tassert.CheckFatal(t, err)
	for _, sid := range sids {
		tassert.Errorf(t, smap.GetActiveNode(sid) != nil,
			"%s is not active in %s", meta.Tname(sid), smap.StringEx())
	}
}

// TestLifecycleBatchShutdown verifies that a two-target shutdown is finalized
// for the entire batch after one rebalance and that both targets can be
// restarted and reactivated together.
func TestLifecycleBatchShutdown(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{
		Long: true, MinTargets: 4, RequiredDeployment: tools.ClusterTypeLocal,
	})

	var (
		bck = cmn.Bck{Name: "lc-batch-shutdown", Provider: apc.AIS}
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

	nodes := lcSelectTargets(t, 2)
	sids := lcIDs(nodes)
	cmds := lcRestoreCmds(nodes)
	issued, restarted, restored := false, false, false
	defer func() {
		if issued && !restarted {
			tools.WaitForRebalAndResil(t, bp)
			lcWaitBatchStopped(t, nodes)
			lcRestartBatch(t, cmds)
			restarted = true
			lcWaitBatchReady(t, cmds)
		}
		if issued && !restored {
			lcRestore(t, bp, sids)
		}
		ensureMembershipAdmits(t, bp)
	}()

	args := &apc.ActValRmNode{}
	args.SetIDs(sids...)
	rebID, err := api.ShutdownNode(bp, args)
	tassert.CheckFatal(t, err)
	issued = true
	tassert.Fatalf(t, rebID != "", "expecting batch shutdown %v to trigger rebalance", sids)
	tools.WaitForRebalanceByID(t, bp, rebID)
	smap := lcWaitMaintPostReb(t, bp, sids...)
	lcWaitBatchStopped(t, nodes)
	tassert.Errorf(t, smap.CountTargets() == m.originalTargetCount,
		"shutdown removed target(s): expected %d, got %d", m.originalTargetCount, smap.CountTargets())
	m.gets(nil, false)
	m.ensureNoGetErrors()

	lcRestartBatch(t, cmds)
	restarted = true
	lcWaitBatchReady(t, cmds)
	restored = lcRestore(t, bp, sids)
	if !restored {
		return
	}
	_, err = tools.WaitForClusterState(proxyURL, "shutdown batch restored",
		smap.Version, m.originalProxyCount, m.originalTargetCount)
	tassert.CheckFatal(t, err)
}

// TestLifecycleBatchDecommission verifies one rebalance and one coordinated
// final removal for two targets, then restores the local processes so the test
// leaves the original cluster intact.
func TestLifecycleBatchDecommission(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{
		Long: true, MinTargets: 4, RequiredDeployment: tools.ClusterTypeLocal,
	})

	var (
		bck = cmn.Bck{Name: "lc-batch-decommission", Provider: apc.AIS}
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

	smap := tools.GetClusterMap(t, proxyURL)
	nodes := lcSelectTargets(t, 2)
	sids := lcIDs(nodes)
	cmds := lcRestoreCmds(nodes)
	issued, restarted, restored := false, false, false
	defer func() {
		if issued && !restarted {
			tools.WaitForRebalAndResil(t, bp)
			lcWaitBatchStopped(t, nodes)
			lcRestartBatch(t, cmds)
			restarted = true
			lcWaitBatchReady(t, cmds)
		}
		if issued && !restored {
			lcWaitBatchAdded(t, bp, sids)
		}
		ensureMembershipAdmits(t, bp)
	}()

	args := &apc.ActValRmNode{KeepInitialConfig: true}
	args.SetIDs(sids...)
	rebID, err := api.DecommissionNode(bp, args)
	tassert.CheckFatal(t, err)
	issued = true
	tassert.Fatalf(t, rebID != "", "expecting batch decommission %v to trigger rebalance", sids)
	tools.WaitForRebalanceByID(t, bp, rebID)
	smap, err = tools.WaitForClusterStateActual(proxyURL, "batch decommissioned",
		smap.Version, m.originalProxyCount, m.originalTargetCount-len(nodes), sids...)
	tassert.CheckFatal(t, err)
	for _, sid := range sids {
		tassert.Errorf(t, smap.GetNode(sid) == nil,
			"%s remains in %s", meta.Tname(sid), smap.StringEx())
	}
	m.gets(nil, false)
	m.ensureNoGetErrors()

	lcWaitBatchStopped(t, nodes)
	lcRestartBatch(t, cmds)
	restarted = true
	lcWaitBatchReady(t, cmds)
	_, err = tools.WaitForClusterState(proxyURL, "decommissioned batch restored",
		smap.Version, m.originalProxyCount, m.originalTargetCount)
	tassert.CheckFatal(t, err)
	tools.WaitForRebalAndResil(t, bp)
	restored = true
}

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
	tassert.Fatalf(t, len(nodes) == num,
		"expecting %d active targets, got %d", num, len(nodes))
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

func lcStartMaintNoReb(bp api.BaseParams, sids ...string) (string, error) {
	args := &apc.ActValRmNode{SkipRebalance: true}
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

func lcAssertMaint(t *testing.T, smap *meta.Smap, sid string, postReb bool) {
	t.Helper()
	tsi := smap.GetTarget(sid)
	tassert.Fatalf(t, tsi != nil,
		"%s is missing from %s", meta.Tname(sid), smap.StringEx())
	tassert.Errorf(t, tsi.InMaint(),
		"%s is not in maintenance in %s", tsi.StringEx(), smap.StringEx())
	tassert.Errorf(t, tsi.InMaintPostReb() == postReb,
		"%s: expected post-rebalance=%t, flags=%s",
		tsi.StringEx(), postReb, tsi.Fl2S())
}

func lcWaitMaintPostReb(t *testing.T, bp api.BaseParams, sids ...string) *meta.Smap {
	t.Helper()
	deadline := time.Now().Add(tools.RebalanceTimeout)
	for {
		smap, err := api.GetClusterMap(bp)
		tassert.CheckFatal(t, err)
		complete := true
		for _, sid := range sids {
			tsi := smap.GetTarget(sid)
			if tsi == nil || !tsi.InMaintPostReb() {
				complete = false
				break
			}
		}
		if complete {
			for _, sid := range sids {
				lcAssertMaint(t, smap, sid, true /*postReb*/)
			}
			return smap
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for post-rebalance maintenance on %v in %s",
				sids, smap.StringEx())
		}
		time.Sleep(time.Second)
	}
}

func lcRestoreCmds(nodes []*meta.Snode) []tools.RestoreCmd {
	cmds := make([]tools.RestoreCmd, 0, len(nodes))
	for _, node := range nodes {
		cmds = append(cmds, tools.GetRestoreCmd(node))
	}
	return cmds
}

func lcWaitBatchStopped(t *testing.T, nodes []*meta.Snode) {
	t.Helper()
	for _, node := range nodes {
		err := tools.WaitNodePubAddrNotInUse(node, time.Minute)
		tassert.CheckFatal(t, err)
	}
}

func lcRestartBatch(t *testing.T, cmds []tools.RestoreCmd) {
	t.Helper()
	for _, cmd := range cmds {
		err := tools.RestoreNode(cmd, false /*asPrimary*/, "batch "+cmd.Node.Type())
		tassert.CheckFatal(t, err)
	}
}

// A freshly restored process may return a connection error that WaitNodeReady
// does not classify as retriable. Retry the complete readiness check while the
// process binds its port.
func lcWaitBatchReady(t *testing.T, cmds []tools.RestoreCmd) {
	t.Helper()
	for _, cmd := range cmds {
		var err error
		deadline := time.Now().Add(time.Minute)
		for {
			err = tools.WaitNodeReady(cmd.Node.URL(cmn.NetPublic))
			if err == nil || time.Now().After(deadline) {
				break
			}
			time.Sleep(time.Second)
		}
		tassert.CheckFatal(t, err)
	}
}

func lcWaitBatchAdded(t *testing.T, bp api.BaseParams, sids []string) {
	t.Helper()
	for _, sid := range sids {
		_, err := tools.WaitNodeAdded(bp, sid)
		tassert.CheckFatal(t, err)
	}
	tools.WaitForRebalAndResil(t, bp)
}

// put nodes in maintenance and returns the causal rebalance ID.
func armStartMaint(t *testing.T, bp api.BaseParams, sids ...string) string {
	t.Helper()
	rebID, err := lcStartMaint(bp, sids...)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, rebID != "",
		"expecting start-maintenance %v to trigger rebalance", sids)
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
		tassert.Fatalf(t, cmn.IsStatusNotFound(err),
			"rebalance[%s] status: %v", rebID, err)
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
	tassert.Fatalf(t, tsi != nil,
		"%s no active target in %s", lcProbeTag, smap.StringEx())

	rebID, err := lcStopMaint(bp, tsi.ID())
	tassert.Errorf(t, err == nil,
		"%s lifecycle admission is stranded: %v", lcProbeTag, err)
	tassert.Errorf(t, rebID == "",
		"%s expecting a no-op, got rebalance[%s]", lcProbeTag, rebID)
}

func ensureNoRunningReb(t *testing.T, bp api.BaseParams) {
	t.Helper()
	running, err := api.GetAllRunningXactions(bp, apc.ActRebalance)
	tassert.CheckError(t, err)
	tassert.Errorf(t, len(running) == 0,
		"expecting no running rebalance, got %v", running)
}
