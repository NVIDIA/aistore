// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
)

func TestMaintenanceOnOff(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{MinTargets: 3})
	proxyURL := tools.RandomProxyURL(t)
	smap := tools.GetClusterMap(t, proxyURL)

	// Invalid target case
	msg := &apc.ActValRmNode{DaemonID: "fakeID", SkipRebalance: true}
	_, err := api.StartMaintenance(baseParams, msg)
	tassert.Fatalf(t, err != nil, "Maintenance for invalid daemon ID succeeded")

	mntTarget, _ := smap.GetRandTarget()
	msg.DaemonID = mntTarget.ID()
	baseParams := tools.BaseAPIParams(proxyURL)
	_, err = api.StartMaintenance(baseParams, msg)
	tassert.CheckFatal(t, err)
	smap, err = tools.WaitForClusterState(proxyURL, "target in maintenance",
		smap.Version, smap.CountActiveProxies(), smap.CountActiveTargets()-1)
	tassert.CheckFatal(t, err)
	_, err = api.StopMaintenance(baseParams, msg)
	tassert.CheckFatal(t, err)
	_, err = tools.WaitForClusterState(proxyURL, "target is back",
		smap.Version, smap.CountActiveProxies(), smap.CountTargets())
	tassert.CheckFatal(t, err)
	_, err = api.StopMaintenance(baseParams, msg)
	tassert.Fatalf(t, err != nil, "Canceling maintenance must fail for 'normal' daemon")
}

func TestMaintenanceListObjects(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true, MinTargets: 3})

	var (
		bck = cmn.Bck{Name: "maint-list", Provider: apc.AIS}
		m   = &ioContext{
			t:         t,
			num:       1500,
			fileSize:  cos.KiB,
			fixedSize: true,
			bck:       bck,
			proxyURL:  proxyURL,
		}
		proxyURL    = tools.RandomProxyURL(t)
		baseParams  = tools.BaseAPIParams(proxyURL)
		origEntries = make(map[string]*cmn.LsoEntry, 1500)
	)

	m.initWithCleanupAndSaveState()
	tools.CreateBucketWithCleanup(t, proxyURL, bck, nil)

	m.puts()
	// 1. Perform list-object and populate entries map
	msg := &apc.LsoMsg{}
	msg.AddProps(apc.GetPropsChecksum, apc.GetPropsVersion, apc.GetPropsCopies, apc.GetPropsSize)
	lst, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(lst.Entries) == m.num, "list-object should return %d objects - returned %d",
		m.num, len(lst.Entries))
	for _, entry := range lst.Entries {
		origEntries[entry.Name] = entry
	}

	// 2. Put a random target under maintenance
	tsi, _ := m.smap.GetRandTarget()
	tlog.Logf("Put target %s under maintenance\n", tsi.StringEx())
	actVal := &apc.ActValRmNode{DaemonID: tsi.ID(), SkipRebalance: false}
	rebID, err := api.StartMaintenance(baseParams, actVal)
	tassert.CheckFatal(t, err)

	defer func() {
		rebID, err = api.StopMaintenance(baseParams, actVal)
		tassert.CheckFatal(t, err)
		_, err = tools.WaitForClusterState(proxyURL, "target is back",
			m.smap.Version, m.smap.CountActiveProxies(), m.smap.CountTargets())
		args := api.XactReqArgs{ID: rebID, Timeout: rebalanceTimeout}
		_, err = api.WaitForXactionIC(baseParams, args)
		tassert.CheckFatal(t, err)
	}()

	m.smap, err = tools.WaitForClusterState(proxyURL, "target in maintenance",
		m.smap.Version, m.smap.CountActiveProxies(), m.smap.CountActiveTargets()-1)
	tassert.CheckFatal(t, err)

	// Wait for reb to complete
	args := api.XactReqArgs{ID: rebID, Timeout: rebalanceTimeout}
	_, err = api.WaitForXactionIC(baseParams, args)
	tassert.CheckFatal(t, err)

	// 3. Check if we can list all the objects
	lst, err = api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(lst.Entries) == m.num, "list-object should return %d objects - returned %d",
		m.num, len(lst.Entries))
	for _, entry := range lst.Entries {
		origEntry, ok := origEntries[entry.Name]
		tassert.Fatalf(t, ok, "object %s missing in original entries", entry.Name)
		if entry.Checksum != origEntry.Checksum ||
			entry.Version != origEntry.Version ||
			entry.Flags != origEntry.Flags ||
			entry.Copies != origEntry.Copies {
			t.Errorf("some fields of object %q, don't match: %#v v/s %#v ", entry.Name, entry, origEntry)
		}
	}
}

func TestMaintenanceMD(t *testing.T) {
	// NOTE: this test requires local deployment as it checks local filesystem for VMDs.
	tools.CheckSkip(t, tools.SkipTestArgs{MinTargets: 3, RequiredDeployment: tools.ClusterTypeLocal})

	var (
		proxyURL   = tools.RandomProxyURL(t)
		smap       = tools.GetClusterMap(t, proxyURL)
		baseParams = tools.BaseAPIParams(proxyURL)

		dcmTarget, _  = smap.GetRandTarget()
		allTgtsMpaths = tools.GetTargetsMountpaths(t, smap, baseParams)
	)

	t.Cleanup(func() {
		args := api.XactReqArgs{Kind: apc.ActRebalance, Timeout: rebalanceTimeout}
		api.WaitForXactionIC(baseParams, args)
	})

	cmd := tools.GetRestoreCmd(dcmTarget)
	msg := &apc.ActValRmNode{DaemonID: dcmTarget.ID(), SkipRebalance: true, KeepInitialConfig: true}
	_, err := api.DecommissionNode(baseParams, msg)
	tassert.CheckFatal(t, err)

	_, err = tools.WaitForClusterState(proxyURL, "target decommission", smap.Version, smap.CountActiveProxies(),
		smap.CountTargets()-1)
	tassert.CheckFatal(t, err)

	vmdTargets := countVMDTargets(allTgtsMpaths)
	tassert.Errorf(t, vmdTargets == smap.CountTargets()-1, "expected VMD to be found on %d targets, got %d.",
		smap.CountTargets()-1, vmdTargets)

	time.Sleep(time.Second)
	err = tools.RestoreNode(cmd, false, "target")
	tassert.CheckFatal(t, err)
	_, err = tools.WaitForClusterState(proxyURL, "target joined back", smap.Version, smap.CountActiveProxies(),
		smap.CountTargets())
	tassert.CheckFatal(t, err)

	smap = tools.GetClusterMap(t, proxyURL)
	vmdTargets = countVMDTargets(allTgtsMpaths)
	tassert.Errorf(t, vmdTargets == smap.CountTargets(),
		"expected VMD to be found on all %d targets after joining cluster, got %d",
		smap.CountTargets(), vmdTargets)
}

func TestMaintenanceDecommissionRebalance(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{MinTargets: 3, RequiredDeployment: tools.ClusterTypeLocal, Long: true})
	var (
		proxyURL   = tools.RandomProxyURL(t)
		smap       = tools.GetClusterMap(t, proxyURL)
		baseParams = tools.BaseAPIParams(proxyURL)
		objCount   = 100
		objPath    = "ic-decomm/"
		fileSize   = cos.KiB

		dcmTarget, _          = smap.GetRandTarget()
		origTargetCount       = smap.CountTargets()
		origActiveTargetCount = smap.CountActiveTargets()
		origActiveProxyCount  = smap.CountActiveProxies()
		bck                   = cmn.Bck{Name: t.Name(), Provider: apc.AIS}
	)
	tools.CreateBucketWithCleanup(t, proxyURL, bck, nil)
	for i := 0; i < objCount; i++ {
		objName := fmt.Sprintf("%sobj%04d", objPath, i)
		r, _ := readers.NewRandReader(int64(fileSize), cos.ChecksumXXHash)
		err := api.PutObject(api.PutObjectArgs{
			BaseParams: baseParams,
			Bck:        bck,
			Object:     objName,
			Reader:     r,
			Size:       uint64(fileSize),
		})
		tassert.CheckFatal(t, err)
	}

	cmd := tools.GetRestoreCmd(dcmTarget)
	msg := &apc.ActValRmNode{DaemonID: dcmTarget.ID(), RmUserData: true, KeepInitialConfig: true}
	rebID, err := api.DecommissionNode(baseParams, msg)
	tassert.CheckError(t, err)
	_, err = tools.WaitForClusterState(proxyURL, "target decommission",
		smap.Version, origActiveProxyCount, origTargetCount-1, dcmTarget.ID())
	tassert.CheckFatal(t, err)

	tools.WaitForRebalanceByID(t, origActiveTargetCount, baseParams, rebID, rebalanceTimeout)
	msgList := &apc.LsoMsg{Prefix: objPath}
	lst, err := api.ListObjects(baseParams, bck, msgList, 0)
	tassert.CheckError(t, err)
	if lst != nil && len(lst.Entries) != objCount {
		t.Errorf("Wrong number of objects: have %d, expected %d", len(lst.Entries), objCount)
	}

	// FIXME: must use WaitForNodeToTerminate instead of sleep
	time.Sleep(13 * time.Second)

	smap = tools.GetClusterMap(t, proxyURL)
	err = tools.RestoreNode(cmd, false, "target")
	tassert.CheckFatal(t, err)
	smap, err = tools.WaitForClusterState(proxyURL, "target restored", smap.Version, 0, 0)
	tassert.CheckFatal(t, err)

	// If any node is in maintenance cancel the state
	var dcm *cluster.Snode
	for _, node := range smap.Tmap {
		if smap.PresentInMaint(node) {
			dcm = node
			break
		}
	}
	if dcm != nil {
		tlog.Logf("Canceling maintenance for %s\n", dcm.ID())
		args := api.XactReqArgs{Kind: apc.ActRebalance}
		err = api.AbortXaction(baseParams, args)
		tassert.CheckError(t, err)
		val := &apc.ActValRmNode{DaemonID: dcm.ID()}
		rebID, err = api.StopMaintenance(baseParams, val)
		tassert.CheckError(t, err)
		tools.WaitForRebalanceByID(t, origActiveTargetCount, baseParams, rebID, rebalanceTimeout)
	} else {
		args := api.XactReqArgs{Kind: apc.ActRebalance, Timeout: rebalanceTimeout}
		_, err = api.WaitForXactionIC(baseParams, args)
		tassert.CheckError(t, err)
	}

	lst, err = api.ListObjects(baseParams, bck, msgList, 0)
	tassert.CheckError(t, err)
	if lst != nil && len(lst.Entries) != objCount {
		t.Errorf("Invalid number of objects: %d, expected %d", len(lst.Entries), objCount)
	}
}

func countVMDTargets(tsMpaths map[*cluster.Snode][]string) (total int) {
	for _, mpaths := range tsMpaths {
		for _, mpath := range mpaths {
			if err := cos.Stat(filepath.Join(mpath, fname.Vmd)); err == nil {
				total++
				break
			}
		}
	}
	return
}

func TestMaintenanceRebalance(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{MinTargets: 3, Long: true})
	var (
		bck = cmn.Bck{Name: "maint-reb", Provider: apc.AIS}
		m   = &ioContext{
			t:               t,
			num:             30,
			fileSize:        512,
			fixedSize:       true,
			bck:             bck,
			numGetsEachFile: 1,
			proxyURL:        proxyURL,
		}
		actVal     = &apc.ActValRmNode{}
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	m.initWithCleanupAndSaveState()
	tools.CreateBucketWithCleanup(t, proxyURL, bck, nil)
	origProxyCnt, origTargetCount := m.smap.CountActiveProxies(), m.smap.CountActiveTargets()

	m.puts()
	tsi, _ := m.smap.GetRandTarget()
	tlog.Logf("Removing target %s\n", tsi)
	restored := false
	actVal.DaemonID = tsi.ID()
	rebID, err := api.StartMaintenance(baseParams, actVal)
	tassert.CheckError(t, err)
	defer func() {
		if !restored {
			rebID, err := api.StopMaintenance(baseParams, actVal)
			tassert.CheckError(t, err)
			_, err = tools.WaitForClusterState(
				proxyURL,
				"target joined the cluster",
				m.smap.Version, origProxyCnt, origTargetCount,
			)
			tassert.CheckFatal(t, err)
			tools.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID, time.Minute)
		}
		tools.ClearMaintenance(baseParams, tsi)
	}()
	tlog.Logf("Wait for rebalance %s\n", rebID)
	tools.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID, time.Minute)

	smap, err := tools.WaitForClusterState(
		proxyURL,
		"target removed from the cluster",
		m.smap.Version, origProxyCnt, origTargetCount-1, tsi.ID(),
	)
	tassert.CheckFatal(t, err)
	m.smap = smap

	m.gets()
	m.ensureNoGetErrors()

	rebID, err = api.StopMaintenance(baseParams, actVal)
	tassert.CheckFatal(t, err)
	restored = true
	smap, err = tools.WaitForClusterState(
		proxyURL,
		"target joined the cluster",
		m.smap.Version, origProxyCnt, origTargetCount,
	)
	tassert.CheckFatal(t, err)
	m.smap = smap

	tools.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID, time.Minute)
}

func TestMaintenanceGetWhileRebalance(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{MinTargets: 3, Long: true})
	var (
		bck = cmn.Bck{Name: "maint-get-reb", Provider: apc.AIS}
		m   = &ioContext{
			t:               t,
			num:             5000,
			fileSize:        1024,
			fixedSize:       true,
			bck:             bck,
			numGetsEachFile: 1,
			proxyURL:        proxyURL,
		}
		actVal     = &apc.ActValRmNode{}
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	m.initWithCleanupAndSaveState()
	tools.CreateBucketWithCleanup(t, proxyURL, bck, nil)
	origProxyCnt, origTargetCount := m.smap.CountActiveProxies(), m.smap.CountActiveTargets()

	m.puts()
	go m.getsUntilStop()
	stopped := false

	tsi, _ := m.smap.GetRandTarget()
	tlog.Logf("Removing target %s\n", tsi)
	restored := false
	actVal.DaemonID = tsi.ID()
	rebID, err := api.StartMaintenance(baseParams, actVal)
	tassert.CheckFatal(t, err)
	defer func() {
		if !stopped {
			m.stopGets()
		}
		if !restored {
			rebID, err := api.StopMaintenance(baseParams, actVal)
			tassert.CheckFatal(t, err)
			_, err = tools.WaitForClusterState(
				proxyURL,
				"target joined the cluster",
				m.smap.Version, origProxyCnt, origTargetCount,
			)
			tassert.CheckFatal(t, err)
			tools.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID, time.Minute)
		}
		tools.ClearMaintenance(baseParams, tsi)
	}()
	tlog.Logf("Wait for rebalance %s\n", rebID)
	tools.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID, time.Minute)

	smap, err := tools.WaitForClusterState(
		proxyURL,
		"target removed from the cluster",
		m.smap.Version, origProxyCnt, origTargetCount-1, tsi.ID(),
	)
	tassert.CheckFatal(t, err)
	m.smap = smap

	m.stopGets()
	stopped = true
	m.ensureNoGetErrors()

	rebID, err = api.StopMaintenance(baseParams, actVal)
	tassert.CheckFatal(t, err)
	restored = true
	smap, err = tools.WaitForClusterState(
		proxyURL,
		"target joined the cluster",
		m.smap.Version, origProxyCnt, origTargetCount,
	)
	tassert.CheckFatal(t, err)
	m.smap = smap
	tools.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID, time.Minute)
}

func TestNodeShutdown(t *testing.T) {
	for _, ty := range []string{apc.Proxy, apc.Target} {
		t.Run(ty, func(t *testing.T) {
			testNodeShutdown(t, ty)
			time.Sleep(time.Second)
		})
	}
}

func testNodeShutdown(t *testing.T, nodeType string) {
	const nodeOffTimeout = 10 * time.Second
	var (
		proxyURL = tools.GetPrimaryURL()
		smap     = tools.GetClusterMap(t, proxyURL)
		node     *cluster.Snode
		err      error
		pdc, tdc int

		origProxyCnt    = smap.CountActiveProxies()
		origTargetCount = smap.CountActiveTargets()
	)
	if nodeType == apc.Proxy {
		if origProxyCnt == 1 {
			t.Skipf("%s requires at least %d proxies (have %d)", t.Name(), 2, origProxyCnt)
		}
		node, err = smap.GetRandProxy(true)
		pdc = 1
	} else {
		if origTargetCount == 0 {
			t.Skipf("%s requires at least %d targets (have %d)", t.Name(), 1, origTargetCount)
		}
		node, err = smap.GetRandTarget()
		tdc = 1
	}
	tassert.CheckFatal(t, err)

	// 1. Shutdown a random node.
	pid, cmd, err := tools.ShutdownNode(t, baseParams, node)
	tassert.CheckFatal(t, err)
	if nodeType == apc.Target {
		tools.WaitForRebalAndResil(t, baseParams)
	}

	// 2. Make sure the node has been shut down.
	err = tools.WaitForNodeToTerminate(pid, nodeOffTimeout)
	tassert.CheckError(t, err)
	_, err = tools.WaitForClusterState(proxyURL, "shutdown node",
		smap.Version, origProxyCnt-pdc, origTargetCount-tdc, node.ID())
	tassert.CheckError(t, err)

	// 3. Start node again.
	err = tools.RestoreNode(cmd, false, nodeType)
	tassert.CheckError(t, err)
	smap, err = tools.WaitForClusterState(proxyURL, "restart node",
		smap.Version, origProxyCnt-pdc, origTargetCount-tdc)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, smap.GetNode(node.ID()) != nil, "node %s does not exist in %s", node.ID(), smap)
	tassert.Errorf(t, smap.GetNode(node.ID()).Flags.IsSet(cluster.NodeFlagMaint),
		"node should be in maintenance after starting")

	// 4. Remove the node from maintenance.
	_, err = api.StopMaintenance(baseParams, &apc.ActValRmNode{DaemonID: node.ID()})
	tassert.CheckError(t, err)
	_, err = tools.WaitForClusterState(proxyURL, "remove node from maintenance",
		smap.Version, origProxyCnt, origTargetCount)
	tassert.CheckError(t, err)

	if nodeType == apc.Target {
		tools.WaitForRebalAndResil(t, baseParams)
	}
}

func TestShutdownListObjects(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	const nodeOffTimeout = 10 * time.Second
	var (
		bck = cmn.Bck{Name: "shutdown-list", Provider: apc.AIS}
		m   = &ioContext{
			t:         t,
			num:       1500,
			fileSize:  cos.KiB,
			fixedSize: true,
			bck:       bck,
			proxyURL:  proxyURL,
		}
		proxyURL    = tools.RandomProxyURL(t)
		baseParams  = tools.BaseAPIParams(proxyURL)
		origEntries = make(map[string]*cmn.LsoEntry, 1500)
	)

	m.initWithCleanupAndSaveState()
	origTargetCount := m.smap.CountActiveTargets()
	tools.CreateBucketWithCleanup(t, proxyURL, bck, nil)
	m.puts()

	// 1. Perform list-object and populate entries map.
	msg := &apc.LsoMsg{}
	msg.AddProps(apc.GetPropsChecksum, apc.GetPropsCopies, apc.GetPropsSize)
	lst, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(lst.Entries) == m.num, "list-object should return %d objects - returned %d",
		m.num, len(lst.Entries))
	for _, entry := range lst.Entries {
		origEntries[entry.Name] = entry
	}

	// 2. Shut down a random target.
	tsi, _ := m.smap.GetRandTarget()
	pid, cmd, err := tools.ShutdownNode(t, baseParams, tsi)
	tassert.CheckFatal(t, err)

	// Restore target after test is over.
	t.Cleanup(func() {
		err = tools.RestoreNode(cmd, false, apc.Target)
		tassert.CheckError(t, err)
		_, err = tools.WaitForClusterState(proxyURL, "target is back",
			m.smap.Version, 0, origTargetCount-1)
		tassert.CheckError(t, err)

		// Remove the node from maintenance.
		_, err = api.StopMaintenance(baseParams, &apc.ActValRmNode{DaemonID: tsi.ID()})
		tassert.CheckError(t, err)
		_, err = tools.WaitForClusterState(proxyURL, "remove node from maintenance",
			m.smap.Version, 0, origTargetCount)
		tassert.CheckError(t, err)

		tools.WaitForRebalAndResil(t, baseParams)
	})

	// Wait for reb, shutdown to complete.
	tools.WaitForRebalAndResil(t, baseParams)
	tassert.CheckError(t, err)
	err = tools.WaitForNodeToTerminate(pid, nodeOffTimeout)
	tassert.CheckError(t, err)
	m.smap, err = tools.WaitForClusterState(proxyURL, "target in maintenance",
		m.smap.Version, 0, origTargetCount-1, tsi.ID())
	tassert.CheckError(t, err)

	// 3. Check if we can list all the objects.
	if m.smap.CountActiveTargets() == 0 {
		tlog.Logln("Shutdown single target - nothing to do")
		return
	}
	tlog.Logln("Listing objects")
	lst, err = api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(lst.Entries) == m.num, "list-object should return %d objects - returned %d",
		m.num, len(lst.Entries))
	for _, entry := range lst.Entries {
		origEntry, ok := origEntries[entry.Name]
		tassert.Errorf(t, ok, "object %s missing in original entries", entry.Name)
		if entry.Version != origEntry.Version ||
			entry.Flags != origEntry.Flags ||
			entry.Copies != origEntry.Copies {
			t.Errorf("some fields of object %q, don't match: %#v v/s %#v ", entry.Name, entry, origEntry)
		}
	}
}
