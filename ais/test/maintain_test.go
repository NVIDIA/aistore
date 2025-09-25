// Package integration_test.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/xact"
)

func TestMaintenanceOnOff(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 3})
	proxyURL := tools.RandomProxyURL(t)
	smap := tools.GetClusterMap(t, proxyURL)

	tlog.Logfln("targets: %d, proxies: %d", smap.CountActiveTs(), smap.CountActivePs())

	// Invalid target case
	msg := &apc.ActValRmNode{DaemonID: "fakeID", SkipRebalance: true}
	_, err := tools.StartMaintenance(baseParams, msg)
	tassert.Fatalf(t, err != nil, "Maintenance for invalid daemon ID succeeded")

	mntTarget, _ := smap.GetRandTarget()
	msg.DaemonID = mntTarget.ID()
	baseParams := tools.BaseAPIParams(proxyURL)
	_, err = tools.StartMaintenance(baseParams, msg)
	tassert.CheckFatal(t, err)
	smap, err = tools.WaitForClusterState(proxyURL, "target in maintenance",
		smap.Version, smap.CountActivePs(), smap.CountActiveTs()-1)
	tassert.CheckFatal(t, err)
	_, err = api.StopMaintenance(baseParams, msg)
	tassert.CheckFatal(t, err)
	_, err = tools.WaitForClusterState(proxyURL, "target is back",
		smap.Version, smap.CountActivePs(), smap.CountTargets())
	tassert.CheckFatal(t, err)
	_, err = api.StopMaintenance(baseParams, msg)
	tassert.Fatalf(t, err != nil, "Canceling maintenance must fail for 'normal' daemon")
}

func TestMaintenanceListObjects(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true, MinTargets: 3})

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
		origEntries = make(map[string]*cmn.LsoEnt, 1500)
	)

	m.initAndSaveState(true /*cleanup*/)
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	m.puts()
	// 1. Perform list-object and populate entries map
	msg := &apc.LsoMsg{}
	msg.AddProps(apc.GetPropsChecksum, apc.GetPropsVersion, apc.GetPropsCopies, apc.GetPropsSize)
	lst, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(lst.Entries) == m.num, "list-object should return %d objects - returned %d",
		m.num, len(lst.Entries))
	for _, en := range lst.Entries {
		origEntries[en.Name] = en
	}

	// 2. Put a random target in maintenance mode
	tsi, _ := m.smap.GetRandTarget()
	tlog.Logfln("Put target %s in maintenance mode", tsi.StringEx())
	actVal := &apc.ActValRmNode{DaemonID: tsi.ID(), SkipRebalance: false}
	rebID, err := tools.StartMaintenance(baseParams, actVal)
	tassert.CheckFatal(t, err)

	defer func() {
		rebID, err = api.StopMaintenance(baseParams, actVal)
		tassert.CheckFatal(t, err)
		_, err = tools.WaitForClusterState(proxyURL, "target is back",
			m.smap.Version, m.smap.CountActivePs(), m.smap.CountTargets())
		args := xact.ArgsMsg{ID: rebID, Timeout: tools.RebalanceTimeout}
		_, err = api.WaitForXactionIC(baseParams, &args)
		tassert.CheckFatal(t, err)
	}()

	m.smap, err = tools.WaitForClusterState(proxyURL, "target in maintenance",
		m.smap.Version, m.smap.CountActivePs(), m.smap.CountActiveTs()-1)
	tassert.CheckFatal(t, err)

	tools.WaitForRebalanceByID(t, baseParams, rebID)

	// 3. Check if we can list all the objects
	lst, err = api.ListObjects(baseParams, bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(lst.Entries) == m.num, "list-object should return %d objects - returned %d",
		m.num, len(lst.Entries))
	for _, en := range lst.Entries {
		origEntry, ok := origEntries[en.Name]
		tassert.Fatalf(t, ok, "object %s missing in original entries", en.Name)
		if en.Checksum != origEntry.Checksum ||
			en.Version != origEntry.Version ||
			en.Flags != origEntry.Flags ||
			en.Copies != origEntry.Copies {
			t.Errorf("some fields of object %q, don't match: %#v v/s %#v ", en.Name, en, origEntry)
		}
	}
}

func TestMaintenanceMD(t *testing.T) {
	// NOTE: this test requires local deployment as it checks local filesystem for VMDs.
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 3, RequiredDeployment: tools.ClusterTypeLocal})

	var (
		proxyURL   = tools.RandomProxyURL(t)
		smap       = tools.GetClusterMap(t, proxyURL)
		baseParams = tools.BaseAPIParams(proxyURL)

		dcmTarget, _  = smap.GetRandTarget()
		allTgtsMpaths = tools.GetTargetsMountpaths(t, smap, baseParams)
	)

	tlog.Logfln("targets: %d, proxies: %d", smap.CountActiveTs(), smap.CountActivePs())

	t.Cleanup(func() {
		args := xact.ArgsMsg{Kind: apc.ActRebalance, Timeout: tools.RebalanceTimeout}
		api.WaitForXactionIC(baseParams, &args)
	})

	tlog.Logfln("Decommission %s", dcmTarget.StringEx())
	cmd := tools.GetRestoreCmd(dcmTarget)
	msg := &apc.ActValRmNode{DaemonID: dcmTarget.ID(), SkipRebalance: true, KeepInitialConfig: true}
	_, err := tools.DecommissionNode(baseParams, msg)
	tassert.CheckFatal(t, err)

	_, err = tools.WaitForClusterState(proxyURL, "target decommissioned", smap.Version, smap.CountActivePs(),
		smap.CountTargets()-1)
	if err == tools.ErrTimedOutStabilize {
		tlog.Logfln("Retrying - checking with primary %s ...", smap.Primary.StringEx())
		proxyURL = smap.Primary.URL(cmn.NetPublic)
		_, err = tools.WaitForClusterState(proxyURL, "target decommissioned", smap.Version, smap.CountActivePs(),
			smap.CountTargets()-1)
	}
	if err != nil {
		// fail the test but first, try to recover cluster membership
		_ = tools.RestoreNode(cmd, false, "target")
		time.Sleep(10 * time.Second)
		tassert.CheckFatal(t, err)
	}

	vmdTargets := countVMDTargets(allTgtsMpaths)
	tassert.Errorf(t, vmdTargets == smap.CountTargets()-1, "expected VMD to be found on %d targets, got %d.",
		smap.CountTargets()-1, vmdTargets)

	// restarting before the daemon fully terminates may result in "bind: address already in use"
	err = tools.WaitNodePubAddrNotInUse(dcmTarget, time.Minute)
	tassert.CheckFatal(t, err)

	err = tools.RestoreNode(cmd, false, "target")
	tassert.CheckFatal(t, err)
	_, err = tools.WaitForClusterState(proxyURL, "target joined back", smap.Version, smap.CountActivePs(),
		smap.CountTargets())
	tassert.CheckFatal(t, err)

	smap = tools.GetClusterMap(t, proxyURL)
	vmdTargets = countVMDTargets(allTgtsMpaths)
	tassert.Errorf(t, vmdTargets == smap.CountTargets(),
		"expected VMD to be found on all %d targets after joining cluster, got %d",
		smap.CountTargets(), vmdTargets)
}

func TestMaintenanceDecommissionRebalance(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 3, RequiredDeployment: tools.ClusterTypeLocal, Long: true})
	var (
		proxyURL   = tools.RandomProxyURL(t)
		smap       = tools.GetClusterMap(t, proxyURL)
		baseParams = tools.BaseAPIParams(proxyURL)
		objCount   = 100
		objPath    = "ic-decomm/"
		fileSize   = cos.KiB

		dcmTarget, _         = smap.GetRandTarget()
		origTargetCount      = smap.CountTargets()
		origActiveProxyCount = smap.CountActivePs()
		bck                  = cmn.Bck{Name: t.Name(), Provider: apc.AIS}
	)
	tlog.Logfln("targets: %d, proxies: %d", smap.CountActiveTs(), smap.CountActivePs())

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	for i := range objCount {
		objName := fmt.Sprintf("%sobj%04d", objPath, i)
		r, _ := readers.NewRand(int64(fileSize), cos.ChecksumCesXxh)
		_, err := api.PutObject(&api.PutArgs{
			BaseParams: baseParams,
			Bck:        bck,
			ObjName:    objName,
			Reader:     r,
			Size:       uint64(fileSize),
		})
		tassert.CheckFatal(t, err)
	}

	tlog.Logfln("Decommission %s", dcmTarget.StringEx())
	cmd := tools.GetRestoreCmd(dcmTarget)
	msg := &apc.ActValRmNode{DaemonID: dcmTarget.ID(), RmUserData: true, KeepInitialConfig: true}
	rebID, err := tools.DecommissionNode(baseParams, msg)
	tassert.CheckError(t, err)
	_, err = tools.WaitForClusterState(proxyURL, "target decommissioned",
		smap.Version, origActiveProxyCount, origTargetCount-1, dcmTarget.ID())

	if err == tools.ErrTimedOutStabilize {
		tlog.Logfln("Retrying - checking with primary %s ...", smap.Primary.StringEx())
		proxyURL = smap.Primary.URL(cmn.NetPublic)
		_, err = tools.WaitForClusterState(proxyURL, "target decommissioned",
			smap.Version, origActiveProxyCount, origTargetCount-1, dcmTarget.ID())
	}
	if err != nil {
		// fail the test but first, try to recover cluster membership
		_ = tools.RestoreNode(cmd, false, "target")
		time.Sleep(10 * time.Second)
		tassert.CheckFatal(t, err)
	}

	tools.WaitForRebalanceByID(t, baseParams, rebID)

	msgList := &apc.LsoMsg{Prefix: objPath}
	lst, err := api.ListObjects(baseParams, bck, msgList, api.ListArgs{})
	tassert.CheckError(t, err)
	if lst != nil && len(lst.Entries) != objCount {
		t.Errorf("Wrong number of objects: have %d, expected %d", len(lst.Entries), objCount)
	}

	// restarting before the daemon fully terminates may result in "bind: address already in use"
	err = tools.WaitNodePubAddrNotInUse(dcmTarget, time.Minute)
	tassert.CheckFatal(t, err)

	smap = tools.GetClusterMap(t, proxyURL)
	err = tools.RestoreNode(cmd, false, "target")
	tassert.CheckFatal(t, err)
	smap, err = tools.WaitForClusterState(proxyURL, "target restored", smap.Version, 0, 0)
	tassert.CheckFatal(t, err)

	// If any node is in maintenance cancel the state
	var dcm *meta.Snode
	for tid, node := range smap.Tmap {
		if smap.InMaintOrDecomm(tid) {
			dcm = node
			break
		}
	}
	if dcm != nil {
		tlog.Logfln("Canceling maintenance for %s", dcm.ID())
		args := xact.ArgsMsg{Kind: apc.ActRebalance}
		err = api.AbortXaction(baseParams, &args)
		tassert.CheckError(t, err)
		val := &apc.ActValRmNode{DaemonID: dcm.ID()}
		rebID, err = api.StopMaintenance(baseParams, val)
		tassert.CheckError(t, err)
		tools.WaitForRebalanceByID(t, baseParams, rebID)
	} else {
		args := xact.ArgsMsg{Kind: apc.ActRebalance, Timeout: tools.RebalanceTimeout}
		_, err = api.WaitForXactionIC(baseParams, &args)
		tassert.CheckError(t, err)
	}

	lst, err = api.ListObjects(baseParams, bck, msgList, api.ListArgs{})
	tassert.CheckError(t, err)
	if lst != nil && len(lst.Entries) != objCount {
		t.Errorf("Invalid number of objects: %d, expected %d", len(lst.Entries), objCount)
	}
}

func countVMDTargets(tsMpaths map[*meta.Snode][]string) (total int) {
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
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 3, Long: true})
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

	m.initAndSaveState(true /*cleanup*/)
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	origProxyCnt, origTargetCount := m.smap.CountActivePs(), m.smap.CountActiveTs()

	m.puts()
	tsi, _ := m.smap.GetRandTarget()
	tlog.Logfln("Removing %s", tsi.StringEx())
	restored := false
	actVal.DaemonID = tsi.ID()
	rebID, err := tools.StartMaintenance(baseParams, actVal)
	tassert.CheckError(t, err)
	defer func() {
		if !restored {
			rebID, err := api.StopMaintenance(baseParams, actVal)
			tassert.CheckError(t, err)
			_, err = tools.WaitForClusterState(
				proxyURL,
				"target joined (2nd attempt)",
				m.smap.Version, origProxyCnt, origTargetCount,
			)
			tassert.CheckFatal(t, err)
			tools.WaitForRebalanceByID(t, baseParams, rebID)
		}
		tools.ClearMaintenance(baseParams, tsi)
	}()
	tools.WaitForRebalanceByID(t, baseParams, rebID)

	smap, err := tools.WaitForClusterState(
		proxyURL,
		"target removed from the cluster",
		m.smap.Version, origProxyCnt, origTargetCount-1, tsi.ID(),
	)
	tassert.CheckFatal(t, err)
	m.smap = smap

	m.gets(nil, false)
	m.ensureNoGetErrors()

	rebID, err = api.StopMaintenance(baseParams, actVal)
	tassert.CheckFatal(t, err)
	smap, err = tools.WaitForClusterState(
		proxyURL,
		"target joined",
		m.smap.Version, origProxyCnt, origTargetCount,
	)
	tassert.CheckFatal(t, err)
	restored = true
	m.smap = smap

	tools.WaitForRebalanceByID(t, baseParams, rebID)
}

func TestMaintenanceRebalanceWithChunkedObjects(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 3})
	var (
		bck = cmn.Bck{Name: "maint-reb", Provider: apc.AIS}
		m   = &ioContext{
			t:               t,
			num:             1000,
			fileSizeRange:   [2]uint64{4 * cos.KiB, 4 * cos.MiB},
			bck:             bck,
			numGetsEachFile: 1,
			proxyURL:        proxyURL,
			chunksConf: &ioCtxChunksConf{
				numChunks: 10,
				multipart: true,
			},
		}
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	if testing.Short() {
		m.num /= 10
		m.fileSizeRange[1] /= 8
	}

	m.initAndSaveState(true /*cleanup*/)
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	origProxyCnt, origTargetCount := m.smap.CountActivePs(), m.smap.CountActiveTs()

	m.puts()
	tsi, _ := m.smap.GetRandTarget()
	tlog.Logfln("Removing %s", tsi.StringEx())
	rebID, err := tools.StartMaintenance(baseParams, &apc.ActValRmNode{DaemonID: tsi.ID()})
	tassert.CheckError(t, err)

	maintenanceStopped := false
	t.Cleanup(func() {
		if !maintenanceStopped {
			stopMaintenance(t, baseParams, &apc.ActValRmNode{DaemonID: tsi.ID()}, proxyURL, m.smap.Version, origProxyCnt, origTargetCount)
		}
		tools.ClearMaintenance(baseParams, tsi)
	})

	tools.WaitForRebalanceByID(t, baseParams, rebID)

	smap, err := tools.WaitForClusterState(proxyURL, "target removed from the cluster", m.smap.Version, origProxyCnt, origTargetCount-1, tsi.ID())
	tassert.CheckFatal(t, err)
	m.smap = smap

	m.gets(nil, true)
	m.ensureNoGetErrors()

	rebID, err = stopMaintenance(t, baseParams, &apc.ActValRmNode{DaemonID: tsi.ID()}, proxyURL, m.smap.Version, origProxyCnt, origTargetCount)
	tassert.CheckFatal(t, err)
	maintenanceStopped = true

	smap, err = tools.WaitForClusterState(proxyURL, "target joined", m.smap.Version, origProxyCnt, origTargetCount)
	tassert.CheckFatal(t, err)
	m.smap = smap

	tools.WaitForRebalanceByID(t, baseParams, rebID)
}

func stopMaintenance(t *testing.T, baseParams api.BaseParams, actVal *apc.ActValRmNode, proxyURL string, smapVersion int64, origProxyCnt, origTargetCount int) (string, error) {
	rebID, err := api.StopMaintenance(baseParams, actVal)
	if err != nil {
		return "", err
	}
	_, err = tools.WaitForClusterState(proxyURL, "target joined (cleanup)", smapVersion, origProxyCnt, origTargetCount)
	if err == nil {
		tools.WaitForRebalanceByID(t, baseParams, rebID)
	}
	return rebID, nil
}

func TestMaintenanceGetWhileRebalance(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{MinTargets: 3, Long: true})
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

	m.initAndSaveState(true /*cleanup*/)
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	origProxyCnt, origTargetCount := m.smap.CountActivePs(), m.smap.CountActiveTs()

	m.puts()
	go m.getsUntilStop()
	stopped := false

	tsi, _ := m.smap.GetRandTarget()
	tlog.Logfln("Removing %s", tsi.StringEx())
	restored := false
	actVal.DaemonID = tsi.ID()
	rebID, err := tools.StartMaintenance(baseParams, actVal)
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
				"target joined",
				m.smap.Version, origProxyCnt, origTargetCount,
			)
			tassert.CheckFatal(t, err)
			tools.WaitForRebalanceByID(t, baseParams, rebID)
		}
		tools.ClearMaintenance(baseParams, tsi)
	}()
	tools.WaitForRebalanceByID(t, baseParams, rebID)

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
		"target joined",
		m.smap.Version, origProxyCnt, origTargetCount,
	)
	tassert.CheckFatal(t, err)
	m.smap = smap
	tools.WaitForRebalanceByID(t, baseParams, rebID)
}

func TestNodeShutdown(t *testing.T) {
	for _, ty := range []string{apc.Proxy, apc.Target} {
		t.Run(ty, func(t *testing.T) {
			testNodeShutdown(t, ty)
			time.Sleep(time.Second)
		})
	}
}

// TODO -- FIXME: pass with a single target
func testNodeShutdown(t *testing.T, nodeType string) {
	const minNumNodes = 2
	var (
		proxyURL = tools.GetPrimaryURL()
		smap     = tools.GetClusterMap(t, proxyURL)
		node     *meta.Snode
		err      error
		pdc, tdc int

		origProxyCnt    = smap.CountActivePs()
		origTargetCount = smap.CountActiveTs()
	)
	if nodeType == apc.Proxy {
		if origProxyCnt < minNumNodes {
			t.Skipf("%s requires at least %d gateway%s (have %d)",
				t.Name(), minNumNodes, cos.Plural(minNumNodes), origProxyCnt)
		}
		node, err = smap.GetRandProxy(true)
		pdc = 1
	} else {
		if origTargetCount < minNumNodes {
			t.Skipf("%s requires at least %d target%s (have %d)",
				t.Name(), minNumNodes, cos.Plural(minNumNodes), origTargetCount)
		}
		bck := cmn.Bck{Name: "shutdown-node" + cos.GenTie(), Provider: apc.AIS}
		tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

		node, err = smap.GetRandTarget()
		tdc = 1
	}
	tassert.CheckFatal(t, err)

	// 1. Shutdown a random node.
	_, cmd, rebID, err := tools.ShutdownNode(t, baseParams, node)
	tassert.CheckFatal(t, err)
	if nodeType == apc.Target && origTargetCount > 1 {
		time.Sleep(time.Second)
		xargs := xact.ArgsMsg{ID: rebID, Kind: apc.ActRebalance, Timeout: tools.RebalanceTimeout}
		for range 3 {
			status, err := api.WaitForXactionIC(baseParams, &xargs)
			if err == nil {
				tlog.Logfln("%v", status)
				break
			}
			if herr := cmn.UnwrapErrHTTP(err); herr != nil {
				tassert.Errorf(t, herr.Status == http.StatusNotFound, "expecting not found, got %+v", herr)
			}
			time.Sleep(time.Second)
		}
	}

	smap, err = tools.WaitForClusterState(proxyURL, "shutdown node",
		smap.Version, origProxyCnt-pdc, origTargetCount-tdc, node.ID())
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, smap.GetNode(node.ID()) != nil, "node %s does not exist in %s after shutdown", node.ID(), smap)
	tassert.Errorf(t, smap.GetNode(node.ID()).Flags.IsSet(meta.SnodeMaint),
		"node should be in maintenance mode after shutdown")

	// restarting before the daemon fully terminates may result in "bind: address already in use"
	err = tools.WaitNodePubAddrNotInUse(node, time.Minute)
	tassert.CheckFatal(t, err)

	// 3. Start node again.
	err = tools.RestoreNode(cmd, false, nodeType)
	tassert.CheckError(t, err)
	time.Sleep(5 * time.Second) // FIXME: wait-for(node started)
	smap = tools.GetClusterMap(t, proxyURL)
	tassert.Fatalf(t, smap.GetNode(node.ID()) != nil, "node %s does not exist in %s after restart", node.ID(), smap)
	tassert.Errorf(t, smap.GetNode(node.ID()).Flags.IsSet(meta.SnodeMaint),
		"node should be in maintenance mode after restart")

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

// TODO -- FIXME: pass with chunked objects
// Currently, rebalance transfers objects to another target node as a whole, even if objects were originally chunked.
// As a result, after rebalance, objects lose their chunked state and the `chunked` flag no longer matches.
func TestShutdownListObjects(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
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
		origEntries = make(map[string]*cmn.LsoEnt, m.num)
	)

	m.initAndSaveState(true /*cleanup*/)
	origTargetCount := m.smap.CountActiveTs()
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	m.puts()

	// 1. Perform list-object and populate entries map.
	msg := &apc.LsoMsg{}
	msg.AddProps(apc.GetPropsChecksum, apc.GetPropsCopies, apc.GetPropsSize)
	lst, err := api.ListObjects(baseParams, bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(lst.Entries) == m.num, "list-object should return %d objects - returned %d",
		m.num, len(lst.Entries))
	for _, en := range lst.Entries {
		origEntries[en.Name] = en
	}

	// 2. Shut down a random target.
	tsi, _ := m.smap.GetRandTarget()
	_, cmd, rebID, err := tools.ShutdownNode(t, baseParams, tsi)
	tassert.CheckFatal(t, err)

	// Restore target after test is over.
	t.Cleanup(func() {
		// restarting before the daemon fully terminates may result in "bind: address already in use"
		err = tools.WaitNodePubAddrNotInUse(tsi, time.Minute)
		tassert.CheckFatal(t, err)

		err = tools.RestoreNode(cmd, false, apc.Target)
		tassert.CheckError(t, err)

		// first, activate target, second, wait-for-cluster-state
		time.Sleep(time.Second)

		_, err = api.StopMaintenance(baseParams, &apc.ActValRmNode{DaemonID: tsi.ID()})
		if err != nil {
			time.Sleep(3 * time.Second)
			_, err = api.StopMaintenance(baseParams, &apc.ActValRmNode{DaemonID: tsi.ID()})
		}
		tassert.CheckError(t, err)
		_, err = tools.WaitForClusterState(proxyURL, "remove node from maintenance", m.smap.Version, 0, origTargetCount)
		tassert.CheckError(t, err)

		tools.WaitForRebalAndResil(t, baseParams)
	})

	if origTargetCount > 1 {
		time.Sleep(time.Second)
		xargs := xact.ArgsMsg{ID: rebID, Kind: apc.ActRebalance, Timeout: tools.RebalanceTimeout}
		for range 3 {
			status, err := api.WaitForXactionIC(baseParams, &xargs)
			if err == nil {
				tlog.Logfln("%v", status)
				break
			}
			herr := cmn.UnwrapErrHTTP(err)
			tassert.Errorf(t, herr.Status == http.StatusNotFound, "expecting not found, got %+v", herr)
			time.Sleep(time.Second)
		}
	}

	m.smap, err = tools.WaitForClusterState(proxyURL, "target shutdown", m.smap.Version, 0, origTargetCount-1, tsi.ID())
	tassert.CheckFatal(t, err)

	// 3. Check if we can list all the objects.
	if m.smap.CountActiveTs() == 0 {
		tlog.Logln("Shutdown single target - nothing to do")
		return
	}
	tlog.Logln("Listing objects")
	lst, err = api.ListObjects(baseParams, bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(lst.Entries) == m.num, "list-object should return %d objects - returned %d",
		m.num, len(lst.Entries))
	for _, en := range lst.Entries {
		origEntry, ok := origEntries[en.Name]
		tassert.Errorf(t, ok, "object %s missing in original entries", en.Name)
		if en.Version != origEntry.Version ||
			en.Flags != origEntry.Flags ||
			en.Copies != origEntry.Copies {
			t.Errorf("some fields of object %q, don't match: %#v v/s %#v ", en.Name, en, origEntry)
		}
	}
}
