// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/containers"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/devtools/tutils/tassert"
	"github.com/NVIDIA/aistore/fs"
)

func TestMaintenanceOnOff(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)

	// Invalid target case
	msg := &cmn.ActValDecommision{DaemonID: "fakeID", SkipRebalance: true}
	_, err := api.StartMaintenance(baseParams, msg)
	tassert.Fatalf(t, err != nil, "Maintenance for invalid daemon ID succeeded")

	mntTarget, _ := smap.GetRandTarget()
	msg.DaemonID = mntTarget.ID()
	baseParams := tutils.BaseAPIParams(proxyURL)
	_, err = api.StartMaintenance(baseParams, msg)
	tassert.CheckFatal(t, err)
	smap, err = tutils.WaitForClusterState(proxyURL, "target in maintenance",
		smap.Version, smap.CountActiveProxies(), smap.CountActiveTargets()-1)
	tassert.CheckFatal(t, err)
	_, err = api.StopMaintenance(baseParams, msg)
	tassert.CheckFatal(t, err)
	_, err = tutils.WaitForClusterState(proxyURL, "target is back",
		smap.Version, smap.CountActiveProxies(), smap.CountTargets())
	tassert.CheckFatal(t, err)
	_, err = api.StopMaintenance(baseParams, msg)
	tassert.Fatalf(t, err != nil, "Canceling maintenance must fail for 'normal' daemon")
}

func TestMaintenanceListObjects(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{Name: "maint-list", Provider: cmn.ProviderAIS}
		m   = &ioContext{
			t:         t,
			num:       1500,
			fileSize:  cmn.KiB,
			fixedSize: true,
			bck:       bck,
			proxyURL:  proxyURL,
		}
		proxyURL    = tutils.RandomProxyURL(t)
		baseParams  = tutils.BaseAPIParams(proxyURL)
		origEntries = make(map[string]*cmn.BucketEntry, 1500)
	)

	m.saveClusterState()
	tutils.CreateFreshBucket(t, proxyURL, bck, nil)

	m.puts()
	// 1. Perform list-object and populate entries map
	msg := &cmn.SelectMsg{}
	msg.AddProps(cmn.GetPropsChecksum, cmn.GetPropsVersion, cmn.GetPropsCopies, cmn.GetPropsSize)
	bckList, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(bckList.Entries) == m.num, "list-object should return %d objects - returned %d", m.num, len(bckList.Entries))
	for _, entry := range bckList.Entries {
		origEntries[entry.Name] = entry
	}

	// 2. Put a random target under maintenanace
	tsi, _ := m.smap.GetRandTarget()
	tutils.Logf("Put target maintenanace %s\n", tsi)
	actVal := &cmn.ActValDecommision{DaemonID: tsi.ID(), SkipRebalance: false}
	rebID, err := api.StartMaintenance(baseParams, actVal)
	tassert.CheckFatal(t, err)

	defer func() {
		rebID, err = api.StopMaintenance(baseParams, actVal)
		tassert.CheckFatal(t, err)
		_, err = tutils.WaitForClusterState(proxyURL, "target is back",
			m.smap.Version, m.smap.CountActiveProxies(), m.smap.CountTargets())
		args := api.XactReqArgs{ID: rebID, Timeout: rebalanceTimeout}
		_, err = api.WaitForXaction(baseParams, args)
		tassert.CheckFatal(t, err)
	}()

	m.smap, err = tutils.WaitForClusterState(proxyURL, "target in maintenance",
		m.smap.Version, m.smap.CountActiveProxies(), m.smap.CountActiveTargets()-1)
	tassert.CheckFatal(t, err)

	// Wait for reb to complete
	args := api.XactReqArgs{ID: rebID, Timeout: rebalanceTimeout}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)

	// 3. Check if we can list all the objects
	bckList, err = api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(bckList.Entries) == m.num, "list-object should return %d objects - returned %d", m.num, len(bckList.Entries))
	for _, entry := range bckList.Entries {
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

// TODO: Run only with long tests when the test is stable.
func TestMaintenanceMD(t *testing.T) {
	// NOTE: This function requires local deployment as it checks local file system for VMDs.
	tutils.CheckSkip(t, tutils.SkipTestArgs{K8s: true})
	if containers.DockerRunning() {
		t.Skip("skipping in docker")
	}

	var (
		proxyURL   = tutils.RandomProxyURL(t)
		smap       = tutils.GetClusterMap(t, proxyURL)
		baseParams = tutils.BaseAPIParams(proxyURL)

		dcmTarget, _  = smap.GetRandTarget()
		allTgtsMpaths = tutils.GetTargetsMountpaths(t, smap, baseParams)
	)

	msg := &cmn.ActValDecommision{DaemonID: dcmTarget.ID(), SkipRebalance: true}
	_, err := api.Decommission(baseParams, msg)
	tassert.CheckError(t, err)
	_, err = tutils.WaitForClusterState(proxyURL, "target decomission", smap.Version, smap.CountActiveProxies(),
		smap.CountTargets()-1)
	tassert.CheckFatal(t, err)

	vmdTargets := countVMDTargets(allTgtsMpaths)
	tassert.Errorf(t, vmdTargets == smap.CountTargets()-1, "expected VMD to be found on %d targets, got %d.",
		smap.CountTargets()-1, vmdTargets)

	rebID, err := tutils.JoinCluster(proxyURL, dcmTarget)
	tassert.CheckFatal(t, err)
	args := api.XactReqArgs{ID: rebID, Kind: cmn.ActRebalance, Timeout: rebalanceTimeout}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckError(t, err)

	// NOTE: Target will have the same DaemonID as it keeps it in the memory.
	smap = tutils.GetClusterMap(t, proxyURL)
	tassert.Errorf(t, smap.GetTarget(dcmTarget.DaemonID) != nil, "decommissioned target should have the same daemonID")
	vmdTargets = countVMDTargets(allTgtsMpaths)
	tassert.Errorf(t, vmdTargets == smap.CountTargets(),
		"expected VMD to be found on all %d targets after joining cluster, got %d",
		smap.CountTargets(), vmdTargets)
}

func countVMDTargets(tsMpaths map[string][]string) (total int) {
	for _, mpaths := range tsMpaths {
		for _, mpath := range mpaths {
			if _, err := os.Stat(filepath.Join(mpath, fs.VmdPersistedFileName)); err == nil {
				total++
				break
			}
		}
	}
	return
}

func TestMaintenanceRebalance(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	var (
		bck = cmn.Bck{Name: "maint-reb", Provider: cmn.ProviderAIS}
		m   = &ioContext{
			t:               t,
			num:             30,
			fileSize:        512,
			fixedSize:       true,
			bck:             bck,
			numGetsEachFile: 1,
			proxyURL:        proxyURL,
		}
		actVal     = &cmn.ActValDecommision{}
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	m.saveClusterState()
	tutils.CreateFreshBucket(t, proxyURL, bck, nil)

	m.puts()
	tsi, _ := m.smap.GetRandTarget()
	tutils.Logf("Removing target %s\n", tsi)
	restored := false
	actVal.DaemonID = tsi.ID()
	rebID, err := api.Decommission(baseParams, actVal)
	tassert.CheckError(t, err)
	defer func() {
		if !restored {
			rebID, _ = tutils.RestoreTarget(t, proxyURL, tsi)
			tutils.WaitForRebalanceByID(t, baseParams, rebID, time.Minute)
		}
		tutils.ClearMaintenance(baseParams, tsi)
	}()
	tutils.Logf("Wait for rebalance %s\n", rebID)
	args := api.XactReqArgs{ID: rebID, Kind: cmn.ActRebalance, Timeout: time.Minute}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)

	smap, err := tutils.WaitForClusterState(
		proxyURL,
		"to target removed from the cluster",
		m.smap.Version,
		m.smap.CountActiveProxies(),
		m.smap.CountActiveTargets()-1,
	)
	tassert.CheckFatal(t, err)
	m.smap = smap

	m.gets()
	m.ensureNoErrors()

	rebID, _ = tutils.RestoreTarget(t, proxyURL, tsi)
	restored = true
	tutils.WaitForRebalanceByID(t, baseParams, rebID, time.Minute)
}

func TestMaintenanceGetWhileRebalance(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	var (
		bck = cmn.Bck{Name: "maint-get-reb", Provider: cmn.ProviderAIS}
		m   = &ioContext{
			t:               t,
			num:             5000,
			fileSize:        1024,
			fixedSize:       true,
			bck:             bck,
			numGetsEachFile: 1,
			proxyURL:        proxyURL,
		}
		actVal     = &cmn.ActValDecommision{}
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	m.saveClusterState()
	tutils.CreateFreshBucket(t, proxyURL, bck, nil)

	m.puts()
	go m.getsUntilStop()
	stopped := false

	tsi, _ := m.smap.GetRandTarget()
	tutils.Logf("Removing target %s\n", tsi)
	restored := false
	actVal.DaemonID = tsi.ID()
	rebID, err := api.Decommission(baseParams, actVal)
	tassert.CheckFatal(t, err)
	defer func() {
		if !stopped {
			m.stopGets()
		}
		if !restored {
			rebID, _ = tutils.RestoreTarget(t, proxyURL, tsi)
			tutils.WaitForRebalanceByID(t, baseParams, rebID, time.Minute)
		}
		tutils.ClearMaintenance(baseParams, tsi)
	}()
	tutils.Logf("Wait for rebalance %s\n", rebID)
	args := api.XactReqArgs{ID: rebID, Kind: cmn.ActRebalance, Timeout: time.Minute}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)

	smap, err := tutils.WaitForClusterState(
		proxyURL,
		"target removed from the cluster",
		m.smap.Version,
		m.smap.CountProxies(),
		m.smap.CountTargets()-1,
	)
	tassert.CheckFatal(t, err)
	m.smap = smap

	m.stopGets()
	stopped = true
	m.ensureNoErrors()

	rebID, _ = tutils.RestoreTarget(t, proxyURL, tsi)
	restored = true
	tutils.WaitForRebalanceByID(t, baseParams, rebID, time.Minute)
}
