// Package integration_test.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/docker"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact"
)

type Test struct {
	name   string
	method func(*testing.T)
}

type regressionTestData struct {
	bck        cmn.Bck
	renamedBck cmn.Bck
	numBuckets int
	rename     bool
	wait       bool
}

const (
	testBucketName = "TESTAISBUCKET"
)

// 1. PUT file
// 2. Corrupt the file
// 3. GET file
func TestGetCorruptFileAfterPut(t *testing.T) {
	var (
		m = ioContext{
			t:        t,
			num:      1,
			fileSize: cos.KiB,
		}

		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)
	)

	if docker.IsRunning() {
		t.Skipf("%q requires setting xattrs, doesn't work with docker", t.Name())
	}

	m.init(true /*cleanup*/)
	initMountpaths(t, proxyURL)

	tools.CreateBucket(t, proxyURL, m.bck, nil, true /*cleanup*/)

	m.puts()

	// Test corrupting the file contents.
	objName := m.objNames[0]
	var fqn string
	if m.chunksConf != nil && m.chunksConf.multipart {
		fqns := m.findObjChunksOnDisk(m.bck, objName)
		tassert.Fatalf(t, len(fqns) > 0, "object should have chunks: %s", objName)
		fqn = fqns[0]
	} else {
		fqn = m.findObjOnDisk(m.bck, objName)
	}
	tlog.Logfln("Corrupting object data %q: %s", objName, fqn)
	err := os.WriteFile(fqn, []byte("this file has been corrupted"), cos.PermRWR)
	tassert.CheckFatal(t, err)

	_, err = api.GetObjectWithValidation(bp, m.bck, objName, nil)
	tassert.Errorf(t, err != nil, "error is nil, expected error getting corrupted object")
}

func TestRegressionBuckets(t *testing.T) {
	var (
		bck = cmn.Bck{
			Name:     testBucketName,
			Provider: apc.AIS,
		}
		proxyURL = tools.RandomProxyURL(t)
	)
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	doBucketRegressionTest(t, proxyURL, regressionTestData{bck: bck})
}

func TestRenameBucket(t *testing.T) {
	var (
		bck = cmn.Bck{
			Name:     testBucketName,
			Provider: apc.AIS,
		}
		proxyURL   = tools.RandomProxyURL(t)
		bp         = tools.BaseAPIParams(proxyURL)
		renamedBck = cmn.Bck{
			Name:     bck.Name + "_" + cos.GenTie(),
			Provider: apc.AIS,
		}
	)
	for _, wait := range []bool{true, false} {
		t.Run(fmt.Sprintf("wait=%v", wait), func(t *testing.T) {
			tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
			t.Cleanup(func() {
				tools.DestroyBucket(t, proxyURL, renamedBck)
			})

			bcks, err := api.ListBuckets(bp, cmn.QueryBcks{Provider: bck.Provider}, apc.FltPresent)
			tassert.CheckFatal(t, err)

			regData := regressionTestData{
				bck: bck, renamedBck: renamedBck,
				numBuckets: len(bcks), rename: true, wait: wait,
			}
			doBucketRegressionTest(t, proxyURL, regData)
		})
	}
}

//
// doBucketRe*
//

//nolint:gocritic // ignoring (regressionTestData) hugeParam
func doBucketRegressionTest(t *testing.T, proxyURL string, rtd regressionTestData) {
	var (
		m = ioContext{
			t:        t,
			bck:      rtd.bck,
			num:      2036,
			fileSize: cos.KiB,
		}
		bp = tools.BaseAPIParams(proxyURL)

		xid string
		err error
	)

	m.init(true /*cleanup*/)
	m.puts()

	if rtd.rename {
		xid, err = api.RenameBucket(bp, rtd.bck, rtd.renamedBck)
		if err != nil && ensurePrevRebalanceIsFinished(bp, err) {
			// can retry
			xid, err = api.RenameBucket(bp, rtd.bck, rtd.renamedBck)
		}
		tassert.CheckFatal(t, err)

		tlog.Logfln("Renamed %s => %s", rtd.bck.String(), rtd.renamedBck.String())
		if rtd.wait {
			postRenameWaitAndCheck(t, bp, rtd, m.num, m.objNames, xid)
		}
		m.bck = rtd.renamedBck
	}

	var getArgs *api.GetArgs
	if !rtd.wait {
		tlog.Logln("Warning: proceeding to GET while rebalance is running ('silence = true')")
		getArgs = &api.GetArgs{Query: url.Values{apc.QparamSilent: []string{"true"}}}
	}
	m.gets(getArgs, false /*with validation*/)

	if !rtd.rename || rtd.wait {
		m.del()
	} else {
		postRenameWaitAndCheck(t, bp, rtd, m.num, m.objNames, xid)
		m.del()
	}
}

//nolint:gocritic // ignoring (regressionTestData) hugeParam
func postRenameWaitAndCheck(t *testing.T, bp api.BaseParams, rtd regressionTestData, numPuts int, objNames []string, xid string) {
	xargs := xact.ArgsMsg{ID: xid, Kind: apc.ActMoveBck, Bck: rtd.renamedBck, Timeout: tools.RebalanceTimeout}
	err := api.WaitForXaction(bp, &xargs)
	if err != nil {
		if herr, ok := err.(*cmn.ErrHTTP); ok && herr.Status == http.StatusNotFound {
			smap := tools.GetClusterMap(t, proxyURL)
			if smap.CountActiveTs() == 1 {
				err = nil
			}
		}
		tassert.CheckFatal(t, err)
	} else {
		tlog.Logfln("rename-bucket[%s] %s => %s done", xid, rtd.bck.String(), rtd.renamedBck.String())
	}
	bcks, err := api.ListBuckets(bp, cmn.QueryBcks{Provider: rtd.bck.Provider}, apc.FltPresent)
	tassert.CheckFatal(t, err)

	if len(bcks) != rtd.numBuckets {
		t.Fatalf("wrong number of ais buckets (names) before and after rename (before: %d. after: %+v)",
			rtd.numBuckets, bcks)
	}

	renamedBucketExists := false
	for _, bck := range bcks {
		switch bck.Name {
		case rtd.renamedBck.Name:
			renamedBucketExists = true
		case rtd.bck.Name:
			t.Fatalf("original ais bucket %s still exists after rename", rtd.bck.String())
		}
	}

	if !renamedBucketExists {
		t.Fatalf("renamed ais bucket %s does not exist after rename", rtd.renamedBck.String())
	}

	lst, err := api.ListObjects(bp, rtd.renamedBck, nil, api.ListArgs{})
	tassert.CheckFatal(t, err)
	unique := make(map[string]bool)
	for _, e := range lst.Entries {
		base := filepath.Base(e.Name)
		unique[base] = true
	}
	if len(unique) != numPuts {
		const maxcnt = 4
		var cnt int
		for _, name := range objNames {
			if _, ok := unique[name]; !ok {
				cnt++
				if cnt > maxcnt && numPuts-len(unique)-cnt > 1 {
					tlog.Logfln("not found: %s (and %d more objects)", name, numPuts-len(unique)-cnt)
					break
				}
				tlog.Logfln("not found: %s", name)
			}
		}
		err := fmt.Errorf("wrong number of objects in the bucket %s renamed as %s (before: %d. after: %d)",
			rtd.bck.String(), rtd.renamedBck.String(), numPuts, len(unique))
		if rtd.wait {
			t.Fatal(err)
		} else {
			tlog.Logln("Warning: " + err.Error())
		}
	}
}

func TestRenameObjects(t *testing.T) {
	var (
		renameStr = "rename"
		proxyURL  = tools.RandomProxyURL(t)
		bp        = tools.BaseAPIParams(proxyURL)
		bck       = cmn.Bck{
			Name:     t.Name(),
			Provider: apc.AIS,
		}
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	objNames, _, err := tools.PutRandObjs(tools.PutObjectsArgs{
		Context:   t.Context(),
		ProxyURL:  proxyURL,
		Bck:       bck,
		ObjCnt:    100,
		CksumType: bck.DefaultProps(initialClusterConfig).Cksum.Type,
	})
	tassert.CheckFatal(t, err)

	newObjNames := make([]string, 0, len(objNames))
	for i, objName := range objNames {
		newObjName := path.Join(renameStr, objName) + ".renamed" // objName fqn
		newObjNames = append(newObjNames, newObjName)

		err := api.RenameObject(bp, bck, objName, newObjName)
		tassert.CheckFatal(t, err)

		i++
		if i%50 == 0 {
			tlog.Logfln("Renamed %s => %s", objName, newObjName)
		}
	}

	// Check that renamed objects exist.
	for _, newObjName := range newObjNames {
		_, err := api.GetObject(bp, bck, newObjName, nil)
		tassert.CheckError(t, err)
	}
}

func TestObjectPrefix(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		var (
			proxyURL  = tools.RandomProxyURL(t)
			b         = bck.Clone()
			fileNames = prefixCreateFiles(t, proxyURL, b, bck.Props.Cksum.Type)
		)
		prefixLookup(t, proxyURL, b, fileNames)
		prefixCleanup(t, proxyURL, b, fileNames)
	})
}

func TestReregisterMultipleTargets(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	var (
		filesSentOrig = make(map[string]int64)
		filesRecvOrig = make(map[string]int64)
		bytesSentOrig = make(map[string]int64)
		bytesRecvOrig = make(map[string]int64)
		filesSent     int64
		filesRecv     int64
		bytesSent     int64
		bytesRecv     int64

		m = ioContext{
			t:             t,
			num:           10000,
			getErrIsFatal: true, // report the actual GET errors
		}
	)

	m.initAndSaveState(true /*cleanup*/)
	m.expectTargets(2)
	targetsToUnregister := m.originalTargetCount - 1

	// Step 0: Collect rebalance stats
	clusterStats := tools.GetClusterStats(t, m.proxyURL)
	for targetID, targetStats := range clusterStats.Target {
		filesSentOrig[targetID] = tools.GetNamedStatsVal(targetStats, cos.StreamsOutObjCount)
		filesRecvOrig[targetID] = tools.GetNamedStatsVal(targetStats, cos.StreamsInObjCount)
		bytesSentOrig[targetID] = tools.GetNamedStatsVal(targetStats, cos.StreamsOutObjSize)
		bytesRecvOrig[targetID] = tools.GetNamedStatsVal(targetStats, cos.StreamsInObjSize)
	}

	proxyURL := tools.RandomProxyURL(t)
	bp := tools.BaseAPIParams(proxyURL)

	var (
		selected      = m.smap.Tmap.ActiveNodes()[:targetsToUnregister]
		sids          = make([]string, 0, targetsToUnregister)
		snames        = make([]string, 0, targetsToUnregister)
		rebID         string
		inMaintenance bool
		getsRunning   bool
	)
	for _, tsi := range selected {
		sids = append(sids, tsi.ID())
		snames = append(snames, tsi.StringEx())
	}

	// Restore the cluster and stop background GETs on every failure path.
	defer func() {
		if getsRunning {
			m.stopGets()
		}
		if inMaintenance {
			tlog.Logfln("Cleanup: take %v out of maintenance", snames)
			args := &apc.ActValRmNode{}
			args.SetIDs(sids...)

			id, err := api.StopMaintenance(bp, args)
			tassert.CheckError(t, err)
			rebID = id
		}

		// never leave a batch rebalance in flight - the next test's
		// membership change would be rejected by the coexistence check
		if rebID != "" {
			tools.WaitForRebalanceByID(t, bp, rebID)
		}
	}()

	// Step 1: Batch start-maintenance (no rebalance)
	tlog.Logfln("Put %v in maintenance (no rebalance)", snames)
	args := &apc.ActValRmNode{SkipRebalance: true}
	args.SetIDs(sids...)

	_, err := api.StartMaintenance(bp, args)
	tassert.CheckFatal(t, err)
	inMaintenance = true

	smap, err := tools.WaitForClusterState(
		proxyURL,
		"remove targets",
		m.smap.Version,
		m.originalProxyCount,
		m.originalTargetCount-targetsToUnregister,
	)
	tassert.CheckFatal(t, err)
	tlog.Logfln("The cluster now has %d target(s)", smap.CountActiveTs())

	// Step 2: PUT objects into a newly created bucket
	tools.CreateBucket(t, m.proxyURL, m.bck, nil, true /*cleanup*/)
	m.puts()

	// Step 3: Start performing GET requests
	getsRunning = true
	go m.getsUntilStop()

	// Step 4: Batch stop-maintenance (one rebalance)
	tlog.Logfln("Bring %v out of maintenance (with rebalance)", snames)
	args = &apc.ActValRmNode{}
	args.SetIDs(sids...)

	rebID, err = api.StopMaintenance(bp, args)
	tassert.CheckFatal(t, err)

	// The request was accepted and committed. Do not issue the same transition
	// again from deferred cleanup if a later assertion fails - but `rebID` stays
	// set so that cleanup still waits for the rebalance it started.
	inMaintenance = false

	smap, err = tools.WaitForClusterState(
		proxyURL,
		"stopping maintenance",
		smap.Version,
		m.originalProxyCount,
		m.originalTargetCount,
	)
	tassert.CheckFatal(t, err)
	tlog.Logfln("The cluster now has %d target(s)", smap.CountActiveTs())

	tlog.Logfln("Stopping GETs...")
	m.stopGets()
	getsRunning = false

	tools.WaitForRebalanceByID(t, bp, rebID)
	rebID = "" // drained

	clusterStats = tools.GetClusterStats(t, m.proxyURL)
	for targetID, targetStats := range clusterStats.Target {
		filesSent += tools.GetNamedStatsVal(targetStats, cos.StreamsOutObjCount) - filesSentOrig[targetID]
		filesRecv += tools.GetNamedStatsVal(targetStats, cos.StreamsInObjCount) - filesRecvOrig[targetID]
		bytesSent += tools.GetNamedStatsVal(targetStats, cos.StreamsOutObjSize) - bytesSentOrig[targetID]
		bytesRecv += tools.GetNamedStatsVal(targetStats, cos.StreamsInObjSize) - bytesRecvOrig[targetID]
	}

	// Step 5: Log rebalance stats
	tlog.Logfln("Rebalance sent     %s in %d files", cos.IEC(bytesSent, 2), filesSent)
	tlog.Logfln("Rebalance received %s in %d files", cos.IEC(bytesRecv, 2), filesRecv)

	m.ensureNoGetErrors()
	m.waitAndCheckCluState()
}

func TestGetNodeStats(t *testing.T) {
	proxyURL := tools.RandomProxyURL(t)
	bp := tools.BaseAPIParams(proxyURL)
	smap := tools.GetClusterMap(t, proxyURL)

	proxy, err := smap.GetRandProxy(false)
	tassert.CheckFatal(t, err)
	tlog.Logfln("%s:", proxy.StringEx())
	stats, err := api.GetDaemonStats(bp, proxy)
	tassert.CheckFatal(t, err)
	tlog.Logfln("%+v", stats)

	target, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)
	tlog.Logfln("%s:", target.StringEx())
	stats, err = api.GetDaemonStats(bp, target)
	tassert.CheckFatal(t, err)
	tlog.Logfln("%+v", stats)
}

func TestGetSoftwareVersion(t *testing.T) {
	proxyURL := tools.RandomProxyURL(t)
	bp := tools.BaseAPIParams(proxyURL)
	smap := tools.GetClusterMap(t, proxyURL)
	target, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)
	expected, ok := cos.ParseVersion(cmn.VersionAIStore)
	tassert.Fatalf(t, ok, "failed to parse expected version %q", cmn.VersionAIStore)

	for _, node := range []*meta.Snode{smap.Primary, target} {
		version, err := api.GetSoftwareVersion(bp, node)
		tassert.CheckFatal(t, err)
		parsed, ok := cos.ParseVersion(version)
		tassert.Fatalf(t, ok, "%s: failed to parse version %q", node, version)
		// aisnode appends its build ID to cmn.VersionAIStore (for example, "5.0.rc2.<git-sha>").
		tassert.Errorf(t, strings.HasPrefix(parsed.String(), expected.String()+"."),
			"%s: expected version %q, got %q", node, expected, parsed)
		tassert.Errorf(t, parsed.String() == version,
			"%s: version round-trip: expected %q, got %q", node, version, parsed.String())
	}
}

func TestGetClusterStats(t *testing.T) {
	proxyURL := tools.RandomProxyURL(t)
	smap := tools.GetClusterMap(t, proxyURL)
	cluStats := tools.GetClusterStats(t, proxyURL)
	bp := tools.BaseAPIParams(proxyURL)

	for tid, vStats := range cluStats.Target {
		tsi := smap.GetNode(tid)
		tname := tsi.StringEx()
		tassert.Fatalf(t, tsi != nil, "%s is nil", tid)
		tStats, err := api.GetDaemonStats(bp, tsi)
		tassert.CheckFatal(t, err)

		vCDF := vStats.Tcdf
		tCDF := tStats.Tcdf
		if vCDF.PctMax != tCDF.PctMax || vCDF.PctAvg != tCDF.PctAvg {
			t.Errorf("%s: stats are different: [%+v] vs [%+v]\n", tname, vCDF, tCDF)
		}
		if len(vCDF.Mountpaths) != len(tCDF.Mountpaths) {
			t.Errorf("%s: num mountpaths is different: [%+v] vs [%+v]\n", tname, vCDF, tCDF)
		}
		for mpath := range vCDF.Mountpaths {
			tcdf := tCDF.Mountpaths[mpath]
			s := tname + mpath
			if tcdf.Capacity.Used != 0 {
				tlog.Logfln("%-30s %+v %+v", s, tcdf.Disks, tcdf.Capacity)
			}
		}
	}
}

func TestLRU(t *testing.T) {
	var (
		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)

		m = &ioContext{
			t:      t,
			bck:    cliBck,
			num:    100,
			prefix: t.Name() + "_" + cos.GenTie(),
		}
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: m.bck, RequiredDeployment: tools.ClusterTypeLocal})

	m.init(true /*cleanup*/)
	m.remotePuts(false /*evict*/)

	// NOTE: cannot "fight" atimes here, need to unload all cached LOMs
	err := api.ClearLcache(bp, "" /*all targets*/)
	tassert.CheckFatal(t, err)

	m.backdateLocalObjs(time.Hour * 3)

	// Remember targets' watermarks
	var (
		usedPct      = int32(100)
		cluStats     = tools.GetClusterStats(t, proxyURL)
		filesEvicted = make(map[string]int64)
		bytesEvicted = make(map[string]int64)
	)

	// Find out min usage % across all targets
	for tid, v := range cluStats.Target {
		filesEvicted[tid] = tools.GetNamedStatsVal(v, "lru.evict.n")
		bytesEvicted[tid] = tools.GetNamedStatsVal(v, "lru.evict.size")
		for _, c := range v.Tcdf.Mountpaths {
			usedPct = min(usedPct, c.PctUsed)
		}
	}

	var (
		lowWM     = usedPct - 5
		cleanupWM = lowWM - 1
		highWM    = usedPct - 2
	)
	if int(lowWM) < 2 {
		t.Skipf("The current space usage is too low (%d) for the LRU to be tested", lowWM)
		return
	}

	tlog.Logfln("LRU: current min space usage in the cluster: %d%%", usedPct)
	tlog.Logfln("setting 'space.lowwm=%d' and 'space.highwm=%d'", lowWM, highWM)

	// All targets: set new watermarks; restore upon exit
	oconfig := tools.GetClusterConfig(t)
	t.Cleanup(func() {
		var (
			cleanupWMStr, _ = cos.ConvertToString(oconfig.Space.CleanupWM)
			lowWMStr, _     = cos.ConvertToString(oconfig.Space.LowWM)
			highWMStr, _    = cos.ConvertToString(oconfig.Space.HighWM)
		)
		tools.SetClusterConfig(t, cos.StrKVs{
			"space.cleanupwm":       cleanupWMStr,
			"space.lowwm":           lowWMStr,
			"space.highwm":          highWMStr,
			"lru.dont_evict_time":   oconfig.LRU.DontEvictTime.String(),
			"lru.capacity_upd_time": oconfig.LRU.CapacityUpdTime.String(),
		})
	})

	// Cluster-wide reduce dont-evict-time
	cleanupWMStr, _ := cos.ConvertToString(cleanupWM)
	lowWMStr, _ := cos.ConvertToString(lowWM)
	highWMStr, _ := cos.ConvertToString(highWM)
	tools.SetClusterConfig(t, cos.StrKVs{
		"space.cleanupwm":       cleanupWMStr,
		"space.lowwm":           lowWMStr,
		"space.highwm":          highWMStr,
		"lru.dont_evict_time":   time.Hour.String(),
		"lru.capacity_upd_time": "10s",
	})

	tlog.Logln("starting LRU...")
	xid, err := api.StartXaction(bp, &xact.ArgsMsg{
		Kind:    apc.ActLRU,
		Buckets: []cmn.Bck{m.bck},
		Force:   true, // remote-bucket LRU may be disabled by default
	}, "")
	tassert.CheckFatal(t, err)

	args := xact.ArgsMsg{ID: xid, Kind: apc.ActLRU, Timeout: tools.RebalanceTimeout}
	err = api.WaitForXaction(bp, &args)
	tassert.CheckFatal(t, err)

	// Check results
	tlog.Logln("checking the results...")
	cluStats = tools.GetClusterStats(t, proxyURL)
	for k, v := range cluStats.Target {
		diffFilesEvicted := tools.GetNamedStatsVal(v, "lru.evict.n") - filesEvicted[k]
		diffBytesEvicted := tools.GetNamedStatsVal(v, "lru.evict.size") - bytesEvicted[k]
		tlog.Logf(
			"Target %s: evicted %d objects - %s (%dB) total\n",
			k, diffFilesEvicted, cos.IEC(diffBytesEvicted, 2), diffBytesEvicted,
		)

		if diffFilesEvicted == 0 {
			t.Errorf("Target %s: LRU failed to evict any objects", k)
		}
	}
}

func TestPrefetchList(t *testing.T) {
	var (
		m = ioContext{
			t:        t,
			bck:      cliBck,
			num:      100,
			fileSize: cos.KiB,
			chunksConf: &ioCtxChunksConf{
				multipart: true,
				numChunks: 4, // will create 4 chunks
			},
		}
		bck      = cliBck
		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true, RemoteBck: true, Bck: bck})

	m.initAndSaveState(true /*cleanup*/)
	m.expectTargets(2)
	m.puts()

	// 2. Evict those objects from the cache and prefetch them
	tlog.Logfln("Evicting and prefetching %d objects", len(m.objNames))
	evdMsg := &apc.EvdMsg{ListRange: apc.ListRange{ObjNames: m.objNames}}
	xid, err := api.EvictMultiObj(bp, bck, evdMsg)
	if err != nil {
		t.Error(err)
	}

	args := xact.ArgsMsg{ID: xid, Kind: apc.ActEvictObjects, Timeout: tools.RebalanceTimeout}
	err = api.WaitForXaction(bp, &args)
	tassert.CheckFatal(t, err)

	// 3. Prefetch evicted objects
	{
		var msg apc.PrefetchMsg
		msg.ObjNames = m.objNames
		xid, err = api.Prefetch(bp, bck, &msg)
		if err != nil {
			t.Error(err)
		}
	}

	args = xact.ArgsMsg{ID: xid, Kind: apc.ActPrefetchObjects, Timeout: tools.RebalanceTimeout}
	err = api.WaitForXaction(bp, &args)
	tassert.CheckFatal(t, err)

	// 4. Ensure that all the prefetches occurred.
	xargs := xact.ArgsMsg{ID: xid, Timeout: tools.RebalanceTimeout}
	snaps, err := api.QueryXactionSnaps(bp, &xargs)
	tassert.CheckFatal(t, err)
	locObjs, _, _ := snaps.ObjCounts(xid)
	if locObjs != int64(m.num) {
		t.Errorf("did not prefetch all files: missing %d of %d", int64(m.num)-locObjs, m.num)
	}

	msg := &apc.LsoMsg{}
	msg.SetFlag(apc.LsCached)
	lst, err := api.ListObjects(bp, bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	if len(lst.Entries) != m.num {
		t.Errorf("list-objects %s: expected %d, got %d", bck.String(), m.num, len(lst.Entries))
	} else {
		tlog.Logfln("list-objects %s: %d is correct", bck.String(), len(m.objNames))
	}
}

func TestPrefetchSpecificTargets(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RemoteBck: true, Bck: cliBck, MinTargets: 3})

	// 1. Set up a unique prefix and register cleanup for the test objects.
	m := ioContext{t: t, bck: cliBck, prefix: "prefetch-target-" + cos.GenUUID() + "/", skipRemoteEvict: true}
	m.initAndSaveState(false /*cleanup*/)

	t.Cleanup(func() { m.del() })
	bp := tools.BaseAPIParams(m.proxyURL)

	// 2. Use the server's bucket properties and remote identity for placement.
	props, err := api.HeadBucket(bp, m.bck, false /*dontAddRemote*/)
	tassert.CheckFatal(t, err)
	placementBck := meta.CloneBck(&m.bck)
	placementBck.Props = props
	if m.bck.IsRemoteAIS() {
		remotes, err := api.GetRemoteAIS(bp)
		tassert.CheckFatal(t, err)
		for _, remote := range remotes.A {
			if placementBck.Ns.UUID == remote.Alias {
				placementBck.Ns.UUID = remote.UUID
				break
			}
		}
	}

	// 3. Create one object per target; numeric names allow a range over two owners.
	targets := m.smap.Tmap.ActiveNodes()[:3]
	for _, target := range targets {
		name := tools.GenerateObjectNameOnTarget(m.prefix+"1", *placementBck.Bucket(), m.smap, target)
		reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: cos.KiB, CksumType: props.Cksum.Type})
		tassert.CheckFatal(t, err)
		_, err = api.PutObject(&api.PutArgs{BaseParams: bp, Bck: m.bck, ObjName: name, Reader: reader})
		tassert.CheckFatal(t, err)
		m.objNames = append(m.objNames, name)
		m.num++
	}
	owners := cos.NewStrKVs(m.num)
	for _, name := range m.objNames {
		owner, err := m.smap.HrwObjName2T(placementBck, name)
		tassert.CheckFatal(t, err)
		owners[name] = owner.ID()
	}

	// The range contains exactly one selected-target object and one excluded-target object.
	rangeNames := []string{m.objNames[0], m.objNames[2]}
	first, err := strconv.Atoi(strings.TrimPrefix(rangeNames[0], m.prefix))
	tassert.CheckFatal(t, err)
	last, err := strconv.Atoi(strings.TrimPrefix(rangeNames[1], m.prefix))
	tassert.CheckFatal(t, err)
	first, last = min(first, last), max(first, last)
	rangeTmpl := fmt.Sprintf("%s{%d..%d..%d}", m.prefix, first, last, last-first)
	tassert.Fatalf(t, owners[rangeNames[0]] == targets[0].ID() && owners[rangeNames[1]] == targets[2].ID(),
		"range must cover selected and excluded owners")

	// 4. Test one target, a subset, the default set, and a selection owned by an excluded target.
	pair := []string{targets[0].ID(), targets[1].ID()}
	excluded := m.objNames[len(m.objNames)-1:]
	t.Run("subset-prefix-list", func(t *testing.T) {
		tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal})
		remote := attachLsoCtxRemote(t, bp, m.smap.CountActiveTs())
		b := remote.addBck(t, bp, "prefetch-targets", lsoCtxLastPage)
		msg := &apc.PrefetchMsg{ListRange: apc.ListRange{Template: "prefix/"}}
		xid, err := api.Prefetch(bp, b.bck, msg, pair...)
		tassert.CheckFatal(t, err)
		args := &xact.ArgsMsg{ID: xid, Kind: apc.ActPrefetchObjects, Timeout: tools.RebalanceTimeout}
		status, err := api.WaitForXactionIC(bp, args)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, status.IsFinished() && !status.AbortedX && status.ErrMsg == "", "prefetch did not finish cleanly: %+v", status)
		snaps, err := api.QueryXactionSnaps(bp, args)
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, len(snaps) == len(pair), "expected targets %v, got %v", pair, snaps)
		for _, tid := range pair {
			tassert.Fatalf(t, len(snaps[tid]) == 1, "expected one snapshot from target %s, got %v", tid, snaps)
			snap := snaps[tid][0]
			tassert.Errorf(t, snap.IsFinished() && !snap.IsAborted() && snap.Err == "", "target %s: prefetch failed: %+v", tid, snap)
		}
		// one final page per target; no retries or continuation requests
		tassert.Errorf(t, b.calls.Load() == int64(len(pair)), "expected %d LIST calls, got %d", len(pair), b.calls.Load())
	})
	for _, tc := range []struct {
		name      string
		selection apc.ListRange
		ids       []string
		names     []string
	}{
		{"single-list", apc.ListRange{ObjNames: m.objNames}, pair[:1], m.objNames},
		{"subset-prefix", apc.ListRange{Template: m.prefix}, pair, m.objNames},
		{"subset-range", apc.ListRange{Template: rangeTmpl}, pair, rangeNames},
		{"all-prefix", apc.ListRange{Template: m.prefix}, nil, m.objNames},
		{"all-list", apc.ListRange{ObjNames: m.objNames}, nil, m.objNames},
		{"excluded-object", apc.ListRange{ObjNames: excluded}, pair, excluded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := &apc.PrefetchMsg{ListRange: tc.selection}
			testPrefetchTargetSelection(t, &m, msg, tc.ids, tc.names, owners)
		})
	}

	// 5. Reject empty, duplicate, missing, and proxy IDs, including mixed valid/invalid lists;
	// each value is one ID: no separators, no trimming.
	for _, ids := range [][]string{
		{""}, {pair[0], ""}, {pair[0], pair[0]}, {pair[0], "missing-" + cos.GenUUID()}, {pair[0], m.smap.Primary.ID()},
		{pair[0], " " + pair[1]}, {pair[0] + "," + pair[1]},
	} {
		_, err := api.Prefetch(bp, m.bck, &apc.PrefetchMsg{ListRange: apc.ListRange{ObjNames: m.objNames}}, ids...)
		tassert.Errorf(t, api.HTTPStatus(err) == http.StatusBadRequest, "targets %v: expected HTTP 400, got %v", ids, err)
	}

	// Reject a malformed query: it must not fall back to all targets.
	q := url.Values{}
	m.bck.SetQuery(q)
	body := cos.MustMarshal(apc.ActMsg{Action: apc.ActPrefetchObjects, Value: &apc.PrefetchMsg{ListRange: apc.ListRange{ObjNames: m.objNames}}})
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		bp.URL+apc.URLPathBuckets.Join(m.bck.Name)+"?"+q.Encode()+"&"+apc.QparamTID+"=%ZZ", bytes.NewReader(body))
	tassert.CheckFatal(t, err)
	req.Header.Set(cos.HdrContentType, cos.ContentJSON)
	api.SetAuxHeaders(req, &bp)
	resp, err := bp.Client.Do(req)
	tassert.CheckFatal(t, err)
	resp.Body.Close()
	tassert.Errorf(t, resp.StatusCode == http.StatusBadRequest, "malformed query: expected HTTP 400, got %d", resp.StatusCode)

	// 6. Reject a target in maintenance and restore it before removing the test objects.
	args := &apc.ActValRmNode{DaemonID: pair[0], SkipRebalance: true}
	_, err = startMaintenanceRetry(t, bp, args)
	tassert.CheckFatal(t, err)
	t.Cleanup(func() {
		_, err := stopMaintenanceRetry(t, bp, args)
		tassert.CheckError(t, err)
		m.waitAndCheckCluState()
	})
	_, err = tools.WaitForClusterState(m.proxyURL, "prefetch target in maintenance", m.smap.Version,
		m.smap.CountActivePs(), m.smap.CountActiveTs()-1)
	tassert.CheckFatal(t, err)
	_, err = api.Prefetch(bp, m.bck, &apc.PrefetchMsg{ListRange: apc.ListRange{ObjNames: m.objNames}}, pair...)
	herr := cmn.AsErrHTTP(err)
	tassert.Errorf(t, herr != nil && herr.Status == http.StatusBadRequest && herr.TypeCode == "ErrXactTgtInMaint",
		"expected maintenance rejection, got %v", err)
}

func testPrefetchTargetSelection(t *testing.T, m *ioContext, msg *apc.PrefetchMsg, ids, names []string, owners cos.StrKVs) {
	t.Helper()
	bp := tools.BaseAPIParams(m.proxyURL)
	tids := ids
	if len(ids) == 0 {
		ids = m.smap.Tmap.ActiveMap().IDs()
	}
	wanted := cos.NewStrSet()
	for _, name := range names {
		if slices.Contains(ids, owners[name]) {
			wanted.Set(name)
		}
	}

	// 1. Evict the test objects so the first pass is cold and the second pass is warm.
	xid, err := api.EvictMultiObj(bp, m.bck, &apc.EvdMsg{ListRange: apc.ListRange{ObjNames: m.objNames}})
	tassert.CheckFatal(t, err)
	tassert.CheckFatal(t, api.WaitForXaction(bp, &xact.ArgsMsg{ID: xid, Kind: apc.ActEvictObjects, Timeout: tools.RebalanceTimeout}))
	for pass := range 2 {
		xid, err = api.Prefetch(bp, m.bck, msg, tids...)
		tassert.CheckFatal(t, err)
		status, err := api.WaitForXactionIC(bp, &xact.ArgsMsg{ID: xid, Kind: apc.ActPrefetchObjects, Timeout: tools.RebalanceTimeout})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, status.IsFinished() && !status.AbortedX && status.ErrMsg == "", "prefetch did not finish cleanly: %+v", status)
		snaps, err := api.QueryXactionSnaps(bp, &xact.ArgsMsg{ID: xid, Kind: apc.ActPrefetchObjects})
		tassert.CheckFatal(t, err)
		tassert.Fatalf(t, len(snaps) == len(ids), "expected targets %v, got %v", ids, snaps)
		for _, id := range ids {
			tassert.Fatalf(t, len(snaps[id]) == 1, "expected one snapshot from target %s, got %v", id, snaps)
			snap := snaps[id][0]
			tassert.Errorf(t, snap.Err == "" && !snap.AbortedX, "target %s: prefetch failed: %+v", id, snap)
		}
		count, _, _ := snaps.ObjCounts(xid)
		want := len(wanted)
		if pass == 1 {
			want = 0 // already cached
		}
		tassert.Errorf(t, count == int64(want), "downloaded %d, expected %d", count, want)
	}

	// 2. Verify the cached object count and exclude objects assigned to other targets.
	lsmsg := &apc.LsoMsg{Prefix: m.prefix}
	lsmsg.SetFlag(apc.LsCached)
	lst, err := api.ListObjects(bp, m.bck, lsmsg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(lst.Entries) == len(wanted), "cached %d, expected %d", len(lst.Entries), len(wanted))
	for _, entry := range lst.Entries {
		tassert.Errorf(t, wanted.Contains(entry.Name), "cached unexpected object: %s", entry.Name)
	}
}

func TestDeleteList(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		var (
			err      error
			prefix   = "__listrange/tstf-"
			wg       = &sync.WaitGroup{}
			objCnt   = 100
			errCh    = make(chan error, objCnt)
			files    = make([]string, 0, objCnt)
			proxyURL = tools.RandomProxyURL(t)
			bp       = tools.BaseAPIParams(proxyURL)
			b        = bck.Clone()
		)

		// 1. Put files to delete
		for i := range objCnt {
			r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: fileSize, CksumType: bck.Props.Cksum.Type})
			tassert.CheckFatal(t, err)

			keyname := fmt.Sprintf("%s%d", prefix, i)

			wg.Go(func() {
				tools.Put(proxyURL, b, keyname, r, fileSize /*size*/, 0 /*numChunks*/, errCh)
			})
			files = append(files, keyname)
		}
		wg.Wait()
		tassert.SelectErr(t, errCh, "put", true)
		tlog.Logfln("PUT done.")

		// 2. Delete the objects
		evdMsg := &apc.EvdMsg{ListRange: apc.ListRange{ObjNames: files}}
		xid, err := api.DeleteMultiObj(bp, b, evdMsg)
		tassert.CheckError(t, err)

		args := xact.ArgsMsg{ID: xid, Kind: apc.ActDeleteObjects, Timeout: tools.RebalanceTimeout}
		err = api.WaitForXaction(bp, &args)
		tassert.CheckFatal(t, err)

		// 3. Check to see that all the files have been deleted
		msg := &apc.LsoMsg{Prefix: prefix}
		bktlst, err := api.ListObjects(bp, b, msg, api.ListArgs{})
		tassert.CheckFatal(t, err)
		if len(bktlst.Entries) != 0 {
			t.Errorf("Incorrect number of remaining files: %d, should be 0", len(bktlst.Entries))
		}
	})
}

func TestRemoteAISRateLimitError(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiresRemoteCluster: true})
	config := tools.GetClusterConfig(t)
	bp := tools.BaseAPIParams(tools.RandomProxyURL(t))
	remoteBP := tools.BaseAPIParams(tools.RemoteCluster.URL)
	remoteSmap, err := api.GetClusterMap(remoteBP)
	tassert.CheckFatal(t, err)
	for _, tc := range []struct {
		name  string
		retry bool
	}{
		{name: "forward-429"},
		{name: "backend-retries-429", retry: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remoteBck := cmn.Bck{Name: "remais-rate-limit-" + cos.GenTie(), Provider: apc.AIS}
			tools.CreateBucket(t, remoteBP.URL, remoteBck, nil, true)
			names, _, err := tools.PutRandObjs(tools.PutObjectsArgs{
				Context: t.Context(), ProxyURL: remoteBP.URL, Bck: remoteBck,
				ObjCnt: 1, ObjSize: cos.KiB, FixedSize: true,
			})
			tassert.CheckFatal(t, err)
			bck := remoteBck
			bck.Ns.UUID = tools.RemoteCluster.UUID
			_, err = api.HeadBucket(bp, bck, false)
			tassert.CheckFatal(t, err)
			t.Cleanup(func() { tassert.CheckError(t, api.EvictRemoteBucket(bp, bck, false)) })
			_, err = api.SetBucketProps(bp, bck, &cmn.BpropsToSet{RateLimit: &cmn.RateLimitConfToSet{
				Backend: &cmn.AdaptiveToSet{RateLimitBaseToSet: cmn.RateLimitBaseToSet{
					Enabled: apc.Ptr(tc.retry), MaxTokens: apc.Ptr(100), Interval: apc.Ptr(cos.Duration(time.Second)),
				}, NumRetries: apc.Ptr(1)},
			}})
			tassert.CheckFatal(t, err)
			_, err = api.SetBucketProps(remoteBP, remoteBck, &cmn.BpropsToSet{RateLimit: &cmn.RateLimitConfToSet{
				Frontend: &cmn.BurstyToSet{RateLimitBaseToSet: cmn.RateLimitBaseToSet{
					Enabled: apc.Ptr(true), MaxTokens: apc.Ptr(4 * remoteSmap.CountActivePs()),
					Interval: apc.Ptr(cos.Duration(time.Hour)),
				}, Size: apc.Ptr(1)},
			}})
			tassert.CheckFatal(t, err)
			tassert.CheckFatal(t, tools.WaitForCondition(func() bool {
				_, err := api.GetObject(remoteBP, remoteBck, names[0], nil)
				if err == nil {
					return false
				}
				herr := cmn.AsErrHTTP(err)
				tassert.Fatalf(t, herr != nil && herr.Status == http.StatusTooManyRequests, "expected remote 429, got %v", err)
				return true
			}, tools.WaitRetryOpts{MaxRetries: 32, Interval: time.Millisecond}))
			_, err = api.GetObject(bp, bck, names[0], nil)
			if !tc.retry {
				// Wait out core.RR.Throttle's target-wide backoff before subsequent blob tests.
				resumeAt := time.Now().Add(config.Timeout.ColdRangeBackoff())
				t.Cleanup(func() { time.Sleep(time.Until(resumeAt)) })
			}
			herr := cmn.AsErrHTTP(err)
			tassert.Fatalf(t, herr != nil, "expected HTTP error, got %v", err)
			if tc.retry {
				tassert.Fatalf(t, herr.TypeCode == "ErrBackendRetry", "expected exhausted backend retry, got %v", err)
			} else {
				tassert.Fatalf(t, herr.Status == http.StatusTooManyRequests, "expected forwarded 429, got %v", err)
			}
		})
	}
}

func TestInvalidNumWorkers(t *testing.T) {
	const errInvalid = "invalid num-workers"
	var (
		proxyURL = tools.RandomProxyURL(t)
		bp       = tools.BaseAPIParams(proxyURL)
		bck      = cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
		objName  = t.Name() + "/" + trand.String(8)
	)
	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)
	tassert.CheckFatal(t, tools.PutObjRR(bp, bck, objName, cos.KiB, cos.ChecksumNone))

	checkErr := func(t *testing.T, err error) {
		tassert.Errorf(t, err != nil && strings.Contains(err.Error(), errInvalid), "expected %q, got %v", errInvalid, err)
	}

	t.Run("delete", func(t *testing.T) {
		evdMsg := &apc.EvdMsg{ListRange: apc.ListRange{ObjNames: []string{objName}}, NumWorkers: -10}
		_, err := api.DeleteMultiObj(bp, bck, evdMsg)
		checkErr(t, err)
		checkLsoCount(t, bp, bck, 1)
	})

	t.Run("tco", func(t *testing.T) {
		bckTo := cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
		tools.CreateBucket(t, proxyURL, bckTo, nil, true /*cleanup*/)

		msg := cmn.TCOMsg{ToBck: bckTo}
		msg.ObjNames = []string{objName}
		msg.NumWorkers = -2
		_, err := api.CopyMultiObj(bp, bck, &msg)
		checkErr(t, err)
		checkLsoCount(t, bp, bck, 1)
	})

	t.Run("tcb", func(t *testing.T) {
		bckTo := cmn.Bck{Name: trand.String(10), Provider: apc.AIS}
		tools.CreateBucket(t, proxyURL, bckTo, nil, true /*cleanup*/)

		_, err := api.CopyBucket(bp, bck, bckTo, &apc.TCBMsg{NumWorkers: -2})
		checkErr(t, err)
		checkLsoCount(t, bp, bck, 1)
	})

	t.Run("shard-index", func(t *testing.T) {
		_, err := api.IndexBucketShards(bp, bck, &apc.IndexShardMsg{NumWorkers: -2})
		checkErr(t, err)
		checkLsoCount(t, bp, bck, 1)
	})

	// -1 (no workers) is valid
	t.Run("delete-none", func(t *testing.T) {
		evdMsg := &apc.EvdMsg{ListRange: apc.ListRange{ObjNames: []string{objName}}, NumWorkers: xact.NwpNone}
		xid, err := api.DeleteMultiObj(bp, bck, evdMsg)
		tassert.CheckFatal(t, err)
		args := xact.ArgsMsg{ID: xid, Kind: apc.ActDeleteObjects, Timeout: tools.RebalanceTimeout}
		tassert.CheckFatal(t, api.WaitForXaction(bp, &args))
		checkLsoCount(t, bp, bck, 0)
	})
}

func checkLsoCount(t *testing.T, bp api.BaseParams, bck cmn.Bck, expected int) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	lst, err := api.ListObjects(bp, bck, nil, api.ListArgs{Context: ctx})
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(lst.Entries) == expected, "expected %d object(s), got %d", expected, len(lst.Entries))
}

func TestBackendRateLimitBackoffRemoteAIS(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiresRemoteCluster: true, Long: true})
	bp := tools.BaseAPIParams(tools.RandomProxyURL(t))
	remoteBP := tools.BaseAPIParams(tools.RemoteCluster.URL)
	remoteSmap, err := api.GetClusterMap(remoteBP)
	tassert.CheckFatal(t, err)
	for _, tc := range []struct {
		name                string
		backendRateLimit    bool
		backendMaxTokens    int
		backendInterval     time.Duration
		rangeRead           bool
		remoteTokenInterval time.Duration
		missingObject       bool
		wantHTTPError       int
		wantErrorType       string
		numRetries          int
		wantRetrySleep      time.Duration
	}{
		{
			name: "cold-get-succeeds-without-429", backendRateLimit: true,
		},
		{
			name: "missing-object-returns-404", backendRateLimit: true, missingObject: true,
			wantHTTPError: http.StatusNotFound,
		},
		{
			name: "disabled-backend-limiter-forwards-429", backendRateLimit: false,
			remoteTokenInterval: 5 * time.Minute, wantHTTPError: http.StatusTooManyRequests,
		},
		{
			name: "cold-get-recovers-from-429-and-clears-retry-delay", backendRateLimit: true,
			remoteTokenInterval: time.Minute,
		},
		{
			name: "range-read-recovers-from-429-and-clears-retry-delay", backendRateLimit: true, rangeRead: true,
			remoteTokenInterval: time.Minute,
		},
		{
			name: "one-retry-sleeps-20ms", backendRateLimit: true, numRetries: 1,
			remoteTokenInterval: time.Hour, wantErrorType: "ErrBackendRetry", wantRetrySleep: 20 * time.Millisecond,
		},
		{
			name: "two-retries-sleep-20ms-plus-40ms", backendRateLimit: true, numRetries: 2,
			remoteTokenInterval: time.Hour, wantErrorType: "ErrBackendRetry", wantRetrySleep: 60 * time.Millisecond,
		},
		{
			name: "three-retries-sleep-20ms-plus-40ms-plus-80ms", backendRateLimit: true, numRetries: 3,
			remoteTokenInterval: time.Hour, wantErrorType: "ErrBackendRetry", wantRetrySleep: 140 * time.Millisecond,
		},
		{
			name: "slow-backend-limit-keeps-10s-retry-interval", backendRateLimit: true, numRetries: 1,
			backendMaxTokens: 1, backendInterval: 20 * time.Second,
			remoteTokenInterval: time.Hour, wantErrorType: "ErrBackendRetry", wantRetrySleep: 10 * time.Second,
		},
		{
			name: "persistent-429-exhausts-retry-budget", backendRateLimit: true,
			remoteTokenInterval: time.Hour, wantErrorType: "ErrBackendRetry",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remoteBck := cmn.Bck{Name: "backend-backoff-" + cos.GenTie(), Provider: apc.AIS}
			tools.CreateBucket(t, remoteBP.URL, remoteBck, nil, true)
			names, _, err := tools.PutRandObjs(tools.PutObjectsArgs{
				Context: t.Context(), ProxyURL: remoteBP.URL, Bck: remoteBck,
				ObjCnt: 1, ObjSize: cos.KiB, FixedSize: true, CksumType: cos.ChecksumOneXxh,
			})
			tassert.CheckFatal(t, err)
			bck := remoteBck
			bck.Ns.UUID = tools.RemoteCluster.UUID
			_, err = api.HeadBucket(bp, bck, false)
			tassert.CheckFatal(t, err)
			t.Cleanup(func() { tassert.CheckError(t, api.EvictRemoteBucket(bp, bck, false)) })
			numRetries := cos.DfltRateMaxRetries
			if tc.numRetries != 0 {
				numRetries = tc.numRetries
			}
			maxTokens, interval := tc.backendMaxTokens, tc.backendInterval
			if maxTokens == 0 {
				maxTokens, interval = 10_000, time.Second
			}
			_, err = api.SetBucketProps(bp, bck, &cmn.BpropsToSet{RateLimit: &cmn.RateLimitConfToSet{
				Backend: &cmn.AdaptiveToSet{RateLimitBaseToSet: cmn.RateLimitBaseToSet{
					Enabled: apc.Ptr(tc.backendRateLimit), MaxTokens: apc.Ptr(maxTokens), Interval: apc.Ptr(cos.Duration(interval)),
				}, NumRetries: apc.Ptr(numRetries)},
			}})
			tassert.CheckFatal(t, err)
			if tc.remoteTokenInterval != 0 {
				remoteMaxTokens := 8 * remoteSmap.CountActivePs()
				_, err = api.SetBucketProps(remoteBP, remoteBck, &cmn.BpropsToSet{RateLimit: &cmn.RateLimitConfToSet{
					Frontend: &cmn.BurstyToSet{RateLimitBaseToSet: cmn.RateLimitBaseToSet{
						Enabled: apc.Ptr(true), MaxTokens: apc.Ptr(remoteMaxTokens),
						Interval: apc.Ptr(cos.Duration(tc.remoteTokenInterval)),
					}, Size: apc.Ptr(1)},
				}})
				tassert.CheckFatal(t, err)
				tassert.CheckFatal(t, tools.WaitForCondition(func() bool {
					_, getErr := api.GetObject(remoteBP, remoteBck, names[0], nil)
					if getErr == nil {
						return false
					}
					herr := cmn.AsErrHTTP(getErr)
					tassert.Fatalf(t, herr != nil && herr.Status == http.StatusTooManyRequests, "expected remote 429, got %v", getErr)
					return true
				}, tools.WaitRetryOpts{MaxRetries: 2 * remoteMaxTokens, Interval: time.Millisecond}))
			}
			args := &api.GetArgs{Context: t.Context()}
			wantSize := int64(cos.KiB)
			if tc.rangeRead {
				wantSize = 1
				args.Header = http.Header{cos.HdrRange: []string{"bytes=0-0"}}
			}
			var (
				owner  *meta.Snode
				before *stats.Node
			)
			if tc.wantRetrySleep != 0 {
				smap := tools.GetClusterMap(t, bp.URL)
				smap.InitDigests()
				owner, err = smap.HrwObjName2T(meta.CloneBck(&bck), names[0])
				tassert.CheckFatal(t, err)
				before, err = api.GetDaemonStats(bp, owner)
				tassert.CheckFatal(t, err)
			}
			started := time.Now()
			name := names[0]
			if tc.missingObject {
				name += "-missing"
			}
			oa, err := api.GetObject(bp, bck, name, args)
			elapsed := time.Since(started)
			tlog.Logfln("GET completed in %v: %v", elapsed, err)
			if tc.wantRetrySleep != 0 {
				after, statsErr := api.GetDaemonStats(bp, owner)
				tassert.CheckFatal(t, statsErr)
				retried := tools.GetNamedStatsVal(after, stats.RatelimGetRetryCount) - tools.GetNamedStatsVal(before, stats.RatelimGetRetryCount)
				sleep := time.Duration(tools.GetNamedStatsVal(after, stats.RatelimGetRetryLatencyTotal) - tools.GetNamedStatsVal(before, stats.RatelimGetRetryLatencyTotal))
				tolerance := tc.wantRetrySleep * time.Duration(tc.numRetries) / 32
				lower := tc.wantRetrySleep - tolerance
				if tc.backendMaxTokens != 0 {
					lower = tc.wantRetrySleep
				}
				tassert.Fatalf(t, retried == 1 && sleep >= lower && sleep <= tc.wantRetrySleep+tolerance,
					"expected one retried GET with %v +/- %v total sleep, got %d with %v", tc.wantRetrySleep, tolerance, retried, sleep)
			}
			if tc.wantHTTPError == 0 && tc.wantErrorType == "" {
				tassert.Fatalf(t, err == nil, "GET failed: %v", err)
				tassert.Fatalf(t, oa.Size() == wantSize, "expected %d bytes, got %d", wantSize, oa.Size())
				if tc.remoteTokenInterval != 0 {
					_, err = api.SetBucketProps(remoteBP, remoteBck, &cmn.BpropsToSet{RateLimit: &cmn.RateLimitConfToSet{
						Frontend: &cmn.BurstyToSet{RateLimitBaseToSet: cmn.RateLimitBaseToSet{Enabled: apc.Ptr(false)}},
					}})
					tassert.CheckFatal(t, err)
					tassert.CheckFatal(t, api.EvictObject(bp, bck, name))
					started = time.Now()
					oa, err = api.GetObject(bp, bck, name, args)
					elapsed = time.Since(started)
					tassert.CheckFatal(t, err)
					tassert.Fatalf(t, oa.Size() == wantSize, "expected %d bytes after recovery, got %d", wantSize, oa.Size())
					tassert.Fatalf(t, elapsed < time.Second, "successful GET retained retry delay: %v", elapsed)
					tlog.Logfln("GET after recovery completed in %v", elapsed)
				}
				return
			}
			herr := cmn.AsErrHTTP(err)
			tassert.Fatalf(t, herr != nil, "expected HTTP error, got %v", err)
			if tc.wantErrorType != "" {
				tassert.Fatalf(t, herr.TypeCode == tc.wantErrorType, "expected %s, got %v", tc.wantErrorType, err)
				tassert.Fatalf(t, elapsed < 2*cos.DfltRateMaxWait,
					"persistent 429 exceeded the retry wait budget: %v", elapsed)
			} else {
				tassert.Fatalf(t, herr.Status == tc.wantHTTPError, "expected HTTP %d, got %v", tc.wantHTTPError, err)
			}
		})
	}
}

func TestBackendRateLimitJitterRemoteAIS(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiresRemoteCluster: true, Long: true})
	bp := tools.BaseAPIParams(tools.RandomProxyURL(t))
	remoteBP := tools.BaseAPIParams(tools.RemoteCluster.URL)
	smap := tools.GetClusterMap(t, bp.URL)
	smap.InitDigests()
	remoteSmap, err := api.GetClusterMap(remoteBP)
	tassert.CheckFatal(t, err)
	for _, tc := range []struct {
		name    string
		minimum time.Duration
		base    time.Duration
	}{
		{name: "jitter-around-6s-base-cap", minimum: 3 * time.Second, base: 6 * time.Second},
		{name: "jitter-preserves-5.95s-minimum", minimum: 5950 * time.Millisecond, base: 6 * time.Second},
		{name: "jitter-preserves-10s-minimum", minimum: 10 * time.Second, base: 10 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			remoteBck := cmn.Bck{Name: "backend-jitter-" + cos.GenTie(), Provider: apc.AIS}
			tools.CreateBucket(t, remoteBP.URL, remoteBck, nil, true)
			names, _, err := tools.PutRandObjs(tools.PutObjectsArgs{
				Context: t.Context(), ProxyURL: remoteBP.URL, Bck: remoteBck,
				ObjCnt: 1, ObjSize: cos.KiB, FixedSize: true, CksumType: cos.ChecksumOneXxh,
			})
			tassert.CheckFatal(t, err)
			bck := remoteBck
			bck.Ns.UUID = tools.RemoteCluster.UUID
			_, err = api.HeadBucket(bp, bck, false)
			tassert.CheckFatal(t, err)
			t.Cleanup(func() { tassert.CheckError(t, api.EvictRemoteBucket(bp, bck, false)) })
			tokensPerTarget := 2
			interval := time.Duration(tokensPerTarget+tokensPerTarget>>2) * tc.minimum
			_, err = api.SetBucketProps(bp, bck, &cmn.BpropsToSet{RateLimit: &cmn.RateLimitConfToSet{
				Backend: &cmn.AdaptiveToSet{RateLimitBaseToSet: cmn.RateLimitBaseToSet{
					Enabled: apc.Ptr(true), MaxTokens: apc.Ptr(tokensPerTarget * smap.CountActiveTs()), Interval: apc.Ptr(cos.Duration(interval)),
				}, NumRetries: apc.Ptr(1)},
			}})
			tassert.CheckFatal(t, err)
			remoteTokens := 8 * remoteSmap.CountActivePs()
			_, err = api.SetBucketProps(remoteBP, remoteBck, &cmn.BpropsToSet{RateLimit: &cmn.RateLimitConfToSet{
				Frontend: &cmn.BurstyToSet{RateLimitBaseToSet: cmn.RateLimitBaseToSet{
					Enabled: apc.Ptr(true), MaxTokens: apc.Ptr(remoteTokens), Interval: apc.Ptr(cos.Duration(time.Hour)),
				}, Size: apc.Ptr(1)},
			}})
			tassert.CheckFatal(t, err)
			tassert.CheckFatal(t, tools.WaitForCondition(func() bool {
				_, getErr := api.GetObject(remoteBP, remoteBck, names[0], nil)
				if getErr == nil {
					return false
				}
				tassert.Fatalf(t, api.HTTPStatus(getErr) == http.StatusTooManyRequests, "expected remote 429, got %v", getErr)
				return true
			}, tools.WaitRetryOpts{MaxRetries: remoteTokens + 1, Interval: time.Millisecond}))
			owner, err := smap.HrwObjName2T(meta.CloneBck(&bck), names[0])
			tassert.CheckFatal(t, err)
			before, err := api.GetDaemonStats(bp, owner)
			tassert.CheckFatal(t, err)
			_, err = api.GetObject(bp, bck, names[0], &api.GetArgs{Context: t.Context()})
			herr := cmn.AsErrHTTP(err)
			tassert.Fatalf(t, herr != nil && herr.TypeCode == "ErrBackendRetry", "expected exhausted 429 retry, got %v", err)
			after, err := api.GetDaemonStats(bp, owner)
			tassert.CheckFatal(t, err)
			retried := tools.GetNamedStatsVal(after, stats.RatelimGetRetryCount) - tools.GetNamedStatsVal(before, stats.RatelimGetRetryCount)
			sleep := time.Duration(tools.GetNamedStatsVal(after, stats.RatelimGetRetryLatencyTotal) - tools.GetNamedStatsVal(before, stats.RatelimGetRetryLatencyTotal))
			valid := false
			for slot := range 8 {
				valid = valid || sleep == max(tc.base+time.Duration(slot-4)*(tc.base>>7), tc.minimum)
			}
			tassert.Fatalf(t, retried == 1 && valid, "expected one jittered retry around %v with minimum %v, got %d with %v sleep",
				tc.base, tc.minimum, retried, sleep)
			tlog.Logfln("retry sleep %v", sleep)
		})
	}
}

func TestPrefetchRetryRemoteAIS(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiresRemoteCluster: true})
	bp := tools.BaseAPIParams(tools.RandomProxyURL(t))
	remoteBP := tools.BaseAPIParams(tools.RemoteCluster.URL)
	remoteSmap, err := api.GetClusterMap(remoteBP)
	tassert.CheckFatal(t, err)
	numTargets := tools.GetClusterMap(t, bp.URL).CountActiveTs()

	for _, tc := range []struct {
		name        string
		prefix      string
		numRetries  int
		enabled     bool
		onDemand    bool
		blob        bool
		unlimited   bool
		wantFailure bool
	}{
		{name: "unthrottled-with-policy-disabled", unlimited: true},
		{name: "unthrottled-on-demand-runs-without-pacing", enabled: true, onDemand: true, unlimited: true},
		{name: "cold-get-with-policy-disabled", wantFailure: true},
		{name: "on-demand-with-policy-disabled-does-not-retry", onDemand: true, wantFailure: true},
		{name: "cold-get-with-proactive-policy", enabled: true},
		{name: "cold-get-with-on-demand-policy", enabled: true, onDemand: true},
		{name: "on-demand-honors-one-retry-budget", enabled: true, onDemand: true, numRetries: 1, wantFailure: true},
		{name: "prefix-cold-get-with-on-demand-policy", prefix: "objects/", enabled: true, onDemand: true},
		{name: "blob-with-policy-disabled", blob: true, wantFailure: true},
		{name: "blob-with-on-demand-policy", enabled: true, onDemand: true, blob: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tools.CheckSkip(t, &tools.SkipTestArgs{Long: tc.blob || tc.unlimited})
			if tc.unlimited {
				tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal})
			}
			numObjects := 16
			if tc.unlimited {
				numObjects = 100 * numTargets
			}
			objSize := uint64(16 * cos.KiB)
			if tc.blob {
				objSize = 2 * cos.MiB
			}
			remoteBck := cmn.Bck{Name: "prefetch-retry-" + cos.GenTie(), Provider: apc.AIS}
			tools.CreateBucket(t, remoteBP.URL, remoteBck, nil, true)
			names, _, err := tools.PutRandObjs(tools.PutObjectsArgs{
				Context: t.Context(), ProxyURL: remoteBP.URL, Bck: remoteBck, ObjPath: tc.prefix,
				ObjCnt: numObjects, ObjSize: objSize, FixedSize: true, CksumType: cos.ChecksumOneXxh,
			})
			tassert.CheckFatal(t, err)
			bck := remoteBck
			bck.Ns.UUID = tools.RemoteCluster.UUID
			props, err := api.HeadBucket(bp, bck, false)
			tassert.CheckFatal(t, err)
			t.Cleanup(func() { tassert.CheckError(t, api.EvictRemoteBucket(bp, bck, false)) })
			tassert.Fatalf(t, !props.RateLimit.Frontend.Enabled && !props.RateLimit.Backend.Enabled,
				"local rate limiting must be disabled: %+v", props.RateLimit)
			_, err = api.SetBucketProps(bp, bck, &cmn.BpropsToSet{RateLimit: &cmn.RateLimitConfToSet{
				Backend: &cmn.AdaptiveToSet{RateLimitBaseToSet: cmn.RateLimitBaseToSet{
					Enabled: apc.Ptr(tc.enabled), MaxTokens: apc.Ptr(10_000),
					Interval: apc.Ptr(cos.Duration(time.Second)),
				}, OnDemand: apc.Ptr(tc.onDemand), NumRetries: apc.Ptr(cos.NonZero(tc.numRetries, cos.DfltRateMaxRetries))},
			}})
			tassert.CheckFatal(t, err)

			if !tc.unlimited {
				remoteMaxTokens := 4 * remoteSmap.CountActivePs()
				_, err = api.SetBucketProps(remoteBP, remoteBck, &cmn.BpropsToSet{RateLimit: &cmn.RateLimitConfToSet{
					Frontend: &cmn.BurstyToSet{RateLimitBaseToSet: cmn.RateLimitBaseToSet{
						Enabled: apc.Ptr(true), MaxTokens: apc.Ptr(remoteMaxTokens),
						Interval: apc.Ptr(cos.Duration(time.Second)),
					}, Size: apc.Ptr(1)},
				}})
				tassert.CheckFatal(t, err)
				tassert.CheckFatal(t, tools.WaitForCondition(func() bool {
					_, getErr := api.GetObject(remoteBP, remoteBck, names[0], nil)
					if getErr == nil {
						return false
					}
					herr := cmn.AsErrHTTP(getErr)
					tassert.Fatalf(t, herr != nil && herr.Status == http.StatusTooManyRequests,
						"expected remote frontend 429, got %v", getErr)
					return true
				}, tools.WaitRetryOpts{MaxRetries: 2 * remoteMaxTokens, Interval: time.Millisecond}))
			}

			msg := apc.PrefetchMsg{ListRange: apc.ListRange{ObjNames: names}, NumWorkers: 4}
			if tc.prefix != "" {
				msg.ListRange = apc.ListRange{Template: tc.prefix}
			}
			if tc.blob {
				msg.BlobThreshold, msg.BlobChunkSize, msg.BlobNumWorkers = cos.MiB, cos.MiB, 2
			}
			xid, err := api.Prefetch(bp, bck, &msg)
			tassert.CheckFatal(t, err)
			args := &xact.ArgsMsg{ID: xid, Kind: apc.ActPrefetchObjects, Timeout: 2 * time.Minute}
			t.Cleanup(func() { _ = api.AbortXaction(bp, args) })
			snaps, err := api.WaitForSnaps(bp, args, args.Finished())
			tassert.CheckFatal(t, err)
			var fetched, size int64
			var failedTargets int
			var elapsed time.Duration
			for _, targetSnaps := range snaps {
				for _, snap := range targetSnaps {
					tassert.Fatalf(t, !snap.IsAborted(), "unexpected prefetch abort: %s", snap.AbortErr)
					fetched += snap.Stats.Objs
					size += snap.Stats.Bytes
					elapsed = max(elapsed, snap.EndTime.Sub(snap.StartTime))
					if snap.Err != "" {
						tassert.Fatalf(t, tc.wantFailure,
							"unexpected prefetch error: %s", snap.Err)
						failedTargets++
					}
				}
			}
			cached, err := api.ListObjects(bp, bck, &apc.LsoMsg{Flags: apc.LsCached, Props: apc.GetPropsName}, api.ListArgs{})
			tassert.CheckFatal(t, err)
			tlog.Logfln("enabled=%t, on-demand=%t, blob=%t: prefetched %d/%d objects, cached=%d, targets reporting errors=%d",
				tc.enabled, tc.onDemand, tc.blob, fetched, numObjects, len(cached.Entries), failedTargets)
			tassert.Fatalf(t, int64(len(cached.Entries)) == fetched, "cached objects disagree with prefetch statistics")
			if tc.wantFailure {
				tassert.Fatalf(t, failedTargets > 0 && fetched < int64(numObjects), "expected failed, incomplete prefetch")
				return
			}
			tassert.Fatalf(t, fetched == int64(numObjects) && size == int64(numObjects)*int64(objSize),
				"incomplete prefetch: %d objects, %d bytes", fetched, size)
			if tc.unlimited {
				minimumPacedDuration := time.Duration(numObjects/numTargets-1) * (cos.DfltRateMinBtwn >> 1)
				tlog.Logfln("unthrottled prefetch duration=%v, minimum when paced=%v", elapsed, minimumPacedDuration)
				tassert.Fatalf(t, elapsed < minimumPacedDuration, "prefetch paced before any 429: %v >= %v", elapsed, minimumPacedDuration)
			}
			for _, name := range names {
				_, err := api.GetObjectWithValidation(bp, bck, name, nil)
				tassert.CheckFatal(t, err)
			}
		})
	}
}

func TestPrefetchRange(t *testing.T) {
	var (
		m = ioContext{
			t:        t,
			bck:      cliBck,
			num:      200,
			fileSize: cos.KiB,
			prefix:   "regressionList/obj-",
			ordered:  true,
		}
		proxyURL      = tools.RandomProxyURL(t)
		bp            = tools.BaseAPIParams(proxyURL)
		prefetchRange = "{1..150}"
		bck           = cliBck
	)

	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true, RemoteBck: true, Bck: bck})

	m.initAndSaveState(true /*cleanup*/)

	m.expectTargets(2)
	m.puts()
	// 1. Parse arguments
	pt, err := cos.ParseBashTemplate(prefetchRange)
	tassert.CheckFatal(t, err)
	rangeMin, rangeMax := pt.Ranges[0].Start, pt.Ranges[0].End

	// 2. Discover the number of items we expect to be prefetched
	files := make([]string, 0)
	for _, objName := range m.objNames {
		oName := strings.TrimPrefix(objName, m.prefix)
		if i, err := strconv.ParseInt(oName, 10, 64); err != nil {
			continue
		} else if (rangeMin == 0 && rangeMax == 0) || (i >= rangeMin && i <= rangeMax) {
			files = append(files, objName)
		}
	}

	// 3. Evict those objects from the cache, and then prefetch them
	rng := fmt.Sprintf("%s%s", m.prefix, prefetchRange)
	tlog.Logfln("Evicting and prefetching %d objects (range: %s)", len(files), rng)
	evdMsg := &apc.EvdMsg{ListRange: apc.ListRange{ObjNames: nil, Template: rng}}
	xid, err := api.EvictMultiObj(bp, bck, evdMsg)
	tassert.CheckError(t, err)
	args := xact.ArgsMsg{ID: xid, Kind: apc.ActEvictObjects, Timeout: tools.RebalanceTimeout}
	err = api.WaitForXaction(bp, &args)
	tassert.CheckFatal(t, err)

	{
		var msg apc.PrefetchMsg
		msg.Template = rng
		xid, err = api.Prefetch(bp, bck, &msg)
		tassert.CheckError(t, err)
		args = xact.ArgsMsg{ID: xid, Kind: apc.ActPrefetchObjects, Timeout: tools.RebalanceTimeout}
		err = api.WaitForXaction(bp, &args)
		tassert.CheckFatal(t, err)
	}

	// 4. Ensure all done
	xargs := xact.ArgsMsg{ID: xid, Timeout: tools.RebalanceTimeout}
	snaps, err := api.QueryXactionSnaps(bp, &xargs)
	tassert.CheckFatal(t, err)
	locObjs, _, _ := snaps.ObjCounts(xid)
	if locObjs != int64(len(files)) {
		t.Errorf("did not prefetch all files: missing %d of %d", int64(len(files))-locObjs, len(files))
	}

	msg := &apc.LsoMsg{Prefix: m.prefix}
	msg.SetFlag(apc.LsCached)
	lst, err := api.ListObjects(bp, bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	if len(lst.Entries) < len(files) {
		t.Errorf("list-objects %s/%s: expected %d, got %d", bck.String(), m.prefix, len(files), len(lst.Entries))
	} else {
		var count int
		for _, e := range lst.Entries {
			s := e.Name[len(m.prefix):] // "obj-"
			idx, err := strconv.Atoi(s)
			if err == nil && idx >= 1 && idx <= 150 {
				count++
			}
		}
		if count != len(files) {
			t.Errorf("list-objects %s/%s: expected %d, got %d", bck.String(), m.prefix, len(files), count)
		} else {
			tlog.Logfln("list-objects %s/%s: %d is correct", bck.String(), m.prefix, len(files))
		}
	}
}

func TestDeleteRange(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *meta.Bck) {
		var (
			err            error
			objCnt         = 100
			quarter        = objCnt / 4
			third          = objCnt / 3
			smallrangesize = third - quarter + 1
			prefix         = "__listrange/tstf-"
			smallrange     = fmt.Sprintf("%s{%04d..%04d}", prefix, quarter, third)
			bigrange       = fmt.Sprintf("%s{0000..%04d}", prefix, objCnt)
			wg             = &sync.WaitGroup{}
			errCh          = make(chan error, objCnt)
			proxyURL       = tools.RandomProxyURL(t)
			bp             = tools.BaseAPIParams(proxyURL)
			b              = bck.Clone()
		)

		// 1. Put files to delete
		for i := range objCnt {
			r, err := readers.New(&readers.Arg{Type: readers.Rand, Size: fileSize, CksumType: bck.Props.Cksum.Type})
			tassert.CheckFatal(t, err)

			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				tools.Put(proxyURL, b, fmt.Sprintf("%s%04d", prefix, i), r, fileSize, 0 /*numChunks*/, errCh)
			}(i)
		}
		wg.Wait()
		tassert.SelectErr(t, errCh, "put", true)
		tlog.Logfln("PUT done.")

		// 2. Delete the small range of objects
		tlog.Logfln("Delete in range %s", smallrange)
		evdSmallMsg := &apc.EvdMsg{ListRange: apc.ListRange{ObjNames: nil, Template: smallrange}}
		xid, err := api.DeleteMultiObj(bp, b, evdSmallMsg)
		tassert.CheckError(t, err)
		args := xact.ArgsMsg{ID: xid, Kind: apc.ActDeleteObjects, Timeout: tools.RebalanceTimeout}
		err = api.WaitForXaction(bp, &args)
		tassert.CheckFatal(t, err)

		// 3. Check to see that the correct files have been deleted
		msg := &apc.LsoMsg{Prefix: prefix}
		bktlst, err := api.ListObjects(bp, b, msg, api.ListArgs{})
		tassert.CheckFatal(t, err)
		if len(bktlst.Entries) != objCnt-smallrangesize {
			t.Errorf("Incorrect number of remaining files: %d, should be %d", len(bktlst.Entries), objCnt-smallrangesize)
		}
		filemap := make(map[string]*cmn.LsoEnt)
		for _, en := range bktlst.Entries {
			filemap[en.Name] = en
		}
		for i := range objCnt {
			keyname := fmt.Sprintf("%s%04d", prefix, i)
			_, ok := filemap[keyname]
			if ok && i >= quarter && i <= third {
				t.Errorf("File exists that should have been deleted: %s", keyname)
			} else if !ok && (i < quarter || i > third) {
				t.Errorf("File does not exist that should not have been deleted: %s", keyname)
			}
		}

		tlog.Logfln("Delete in range %s", bigrange)
		// 4. Delete the big range of objects
		evdBigMsg := &apc.EvdMsg{ListRange: apc.ListRange{ObjNames: nil, Template: bigrange}}
		xid, err = api.DeleteMultiObj(bp, b, evdBigMsg)
		tassert.CheckError(t, err)
		args = xact.ArgsMsg{ID: xid, Kind: apc.ActDeleteObjects, Timeout: tools.RebalanceTimeout}
		err = api.WaitForXaction(bp, &args)
		tassert.CheckFatal(t, err)

		// 5. Check to see that all the files have been deleted
		bktlst, err = api.ListObjects(bp, b, msg, api.ListArgs{})
		tassert.CheckFatal(t, err)
		if len(bktlst.Entries) != 0 {
			t.Errorf("Incorrect number of remaining files: %d, should be 0", len(bktlst.Entries))
		}
	})
}

// Testing only ais bucket objects since generally not concerned with cloud bucket object deletion
func TestStressDeleteRange(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})

	const (
		numFiles   = 20000 // FIXME: must divide by 10 and by the numReaders
		numReaders = 200
	)

	var (
		err           error
		wg            = &sync.WaitGroup{}
		errCh         = make(chan error, numFiles)
		proxyURL      = tools.RandomProxyURL(t)
		tenth         = numFiles / 10
		objNamePrefix = "__listrange/tstf-"
		partialRange  = fmt.Sprintf("%s{%d..%d}", objNamePrefix, 0, numFiles-tenth-1) // TODO: partial range with non-zero left boundary
		fullRange     = fmt.Sprintf("%s{0..%d}", objNamePrefix, numFiles)
		bp            = tools.BaseAPIParams(proxyURL)
		bck           = cmn.Bck{
			Name:     testBucketName,
			Provider: apc.AIS,
		}
		cksumType = bck.DefaultProps(initialClusterConfig).Cksum.Type
	)

	tools.CreateBucket(t, proxyURL, bck, nil, true /*cleanup*/)

	// 1. PUT
	tlog.Logln("putting objects...")
	for i := range numReaders {
		size := rand.Int64N(cos.KiB*128) + cos.KiB/3
		tassert.CheckFatal(t, err)
		reader, err := readers.New(&readers.Arg{Type: readers.Rand, Size: size, CksumType: cksumType})
		tassert.CheckFatal(t, err)

		wg.Add(1)
		go func(i int, reader readers.Reader) {
			defer wg.Done()

			for j := range numFiles / numReaders {
				objName := fmt.Sprintf("%s%d", objNamePrefix, i*numFiles/numReaders+j)
				putArgs := api.PutArgs{
					BaseParams: bp,
					Bck:        bck,
					ObjName:    objName,
					Cksum:      reader.Cksum(),
					Reader:     reader,
				}
				_, err = api.PutObject(&putArgs)
				if err != nil {
					errCh <- err
				}
				reader.Reset()
			}
		}(i, reader)
	}
	wg.Wait()
	tassert.SelectErr(t, errCh, "put", true)

	// 2. Delete a range of objects
	tlog.Logfln("Deleting objects in range: %s", partialRange)
	evdPartialMsg := &apc.EvdMsg{ListRange: apc.ListRange{ObjNames: nil, Template: partialRange}}
	xid, err := api.DeleteMultiObj(bp, bck, evdPartialMsg)
	tassert.CheckError(t, err)
	args := xact.ArgsMsg{ID: xid, Kind: apc.ActDeleteObjects, Timeout: tools.RebalanceTimeout}
	err = api.WaitForXaction(bp, &args)
	tassert.CheckFatal(t, err)

	// 3. Check to see that correct objects have been deleted
	expectedRemaining := tenth
	msg := &apc.LsoMsg{Prefix: objNamePrefix}
	lst, err := api.ListObjects(bp, bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	if len(lst.Entries) != expectedRemaining {
		t.Errorf("Incorrect number of remaining objects: %d, expected: %d",
			len(lst.Entries), expectedRemaining)
	}

	objNames := make(map[string]*cmn.LsoEnt)
	for _, en := range lst.Entries {
		objNames[en.Name] = en
	}
	for i := range numFiles {
		objName := fmt.Sprintf("%s%d", objNamePrefix, i)
		_, ok := objNames[objName]
		if ok && i < numFiles-tenth {
			t.Errorf("%s exists (expected to be deleted)", objName)
		} else if !ok && i >= numFiles-tenth {
			t.Errorf("%s does not exist", objName)
		}
	}

	// 4. Delete the entire range of objects
	tlog.Logfln("Deleting objects in range: %s", fullRange)
	evdFullMsg := &apc.EvdMsg{ListRange: apc.ListRange{ObjNames: nil, Template: fullRange}}
	xid, err = api.DeleteMultiObj(bp, bck, evdFullMsg)
	tassert.CheckError(t, err)
	args = xact.ArgsMsg{ID: xid, Kind: apc.ActDeleteObjects, Timeout: tools.RebalanceTimeout}
	err = api.WaitForXaction(bp, &args)
	tassert.CheckFatal(t, err)

	// 5. Check to see that all files have been deleted
	msg = &apc.LsoMsg{Prefix: objNamePrefix}
	lst, err = api.ListObjects(bp, bck, msg, api.ListArgs{})
	tassert.CheckFatal(t, err)
	if len(lst.Entries) != 0 {
		t.Errorf("Incorrect number of remaining files: %d, should be 0", len(lst.Entries))
	}
}
