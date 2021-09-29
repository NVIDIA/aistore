// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/containers"
	"github.com/NVIDIA/aistore/devtools/readers"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tlog"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/stats"
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
	rootDir        = "/tmp/ais"
	testBucketName = "TESTAISBUCKET"
)

var (
	HighWaterMark    = int32(80)
	LowWaterMark     = int32(60)
	UpdTime          = time.Second * 20
	configRegression = map[string]string{
		"periodic.stats_time":   fmt.Sprintf("%v", UpdTime),
		"lru.enabled":           "true",
		"lru.lowwm":             fmt.Sprintf("%d", LowWaterMark),
		"lru.highwm":            fmt.Sprintf("%d", HighWaterMark),
		"lru.capacity_upd_time": fmt.Sprintf("%v", UpdTime),
		"lru.dont_evict_time":   fmt.Sprintf("%v", UpdTime),
	}
)

func TestLocalListObjectsGetTargetURL(t *testing.T) {
	var (
		m = ioContext{
			t:         t,
			num:       1000,
			fileSize:  cos.KiB,
			fixedSize: true,
		}

		targets    = make(map[string]struct{})
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)
		smap       = tutils.GetClusterMap(t, proxyURL)
	)

	m.initAndSaveCluState()
	m.expectTargets(1)

	tutils.CreateFreshBucket(t, proxyURL, m.bck, nil)

	m.puts()

	msg := &cmn.SelectMsg{Props: cmn.GetTargetURL}
	bl, err := api.ListObjects(baseParams, m.bck, msg, uint(m.num))
	tassert.CheckFatal(t, err)

	tutils.SetClusterConfig(t, cos.SimpleKVs{"client.features": strconv.FormatUint(cmn.FeatureDirectAccess, 10)})
	defer tutils.SetClusterConfig(t, cos.SimpleKVs{"client.features": "0"})

	if len(bl.Entries) != m.num {
		t.Errorf("Expected %d bucket list entries, found %d\n", m.num, len(bl.Entries))
	}

	for _, e := range bl.Entries {
		if e.TargetURL == "" {
			t.Error("Target URL in response is empty")
		}
		if _, ok := targets[e.TargetURL]; !ok {
			targets[e.TargetURL] = struct{}{}
		}
		baseParams := tutils.BaseAPIParams(e.TargetURL)
		l, err := api.GetObject(baseParams, m.bck, e.Name)
		tassert.CheckFatal(t, err)
		if uint64(l) != m.fileSize {
			t.Errorf("Expected filesize: %d, actual filesize: %d\n", m.fileSize, l)
		}
	}

	if smap.CountActiveTargets() != len(targets) { // The objects should have been distributed to all targets
		t.Errorf("Expected %d different target URLs, actual: %d different target URLs", smap.CountActiveTargets(), len(targets))
	}

	// Ensure no target URLs are returned when the property is not requested
	msg.Props = ""
	bl, err = api.ListObjects(baseParams, m.bck, msg, uint(m.num))
	tassert.CheckFatal(t, err)

	if len(bl.Entries) != m.num {
		t.Errorf("Expected %d bucket list entries, found %d\n", m.num, len(bl.Entries))
	}

	for _, e := range bl.Entries {
		if e.TargetURL != "" {
			t.Fatalf("Target URL: %s returned when empty target URL expected\n", e.TargetURL)
		}
	}
}

func TestCloudListObjectsGetTargetURL(t *testing.T) {
	var (
		m = ioContext{
			t:        t,
			bck:      cliBck,
			num:      100,
			fileSize: cos.KiB,
		}
		targets    = make(map[string]struct{})
		bck        = cliBck
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{RemoteBck: true, Bck: bck})

	m.initAndSaveCluState()
	m.expectTargets(2)

	m.puts()

	t.Cleanup(func() {
		m.del()
	})

	listObjectsMsg := &cmn.SelectMsg{Props: cmn.GetTargetURL}
	bucketList, err := api.ListObjects(baseParams, bck, listObjectsMsg, 0)
	tassert.CheckFatal(t, err)

	tutils.SetClusterConfig(t, cos.SimpleKVs{"client.features": strconv.FormatUint(cmn.FeatureDirectAccess, 10)})
	defer tutils.SetClusterConfig(t, cos.SimpleKVs{"client.features": "0"})

	if len(bucketList.Entries) != m.num {
		t.Errorf("Number of entries in bucket list [%d] must be equal to [%d]", len(bucketList.Entries), m.num)
	}

	for _, object := range bucketList.Entries {
		if object.TargetURL == "" {
			t.Errorf("Target URL in response is empty for object [%s]", object.Name)
		}
		if _, ok := targets[object.TargetURL]; !ok {
			targets[object.TargetURL] = struct{}{}
		}
		baseParams := tutils.BaseAPIParams(object.TargetURL)
		objectSize, err := api.GetObject(baseParams, bck, object.Name)
		tassert.CheckFatal(t, err)
		if uint64(objectSize) != m.fileSize {
			t.Errorf("Expected fileSize: %d, actual fileSize: %d\n", m.fileSize, objectSize)
		}
	}

	// The objects should have been distributed to all targets
	if m.originalTargetCount != len(targets) {
		t.Errorf("Expected %d different target URLs, actual: %d different target URLs", m.originalTargetCount, len(targets))
	}

	// Ensure no target URLs are returned when the property is not requested
	listObjectsMsg.Props = ""
	bucketList, err = api.ListObjects(baseParams, bck, listObjectsMsg, 0)
	tassert.CheckFatal(t, err)

	if len(bucketList.Entries) != m.num {
		t.Errorf("Expected %d bucket list entries, found %d\n", m.num, len(bucketList.Entries))
	}

	for _, object := range bucketList.Entries {
		if object.TargetURL != "" {
			t.Fatalf("Target URL: %s returned when empty target URL expected\n", object.TargetURL)
		}
	}
}

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

		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	if containers.DockerRunning() {
		t.Skip(fmt.Sprintf("%q requires setting Xattrs, doesn't work with docker", t.Name()))
	}

	m.init()
	initMountpaths(t, proxyURL)

	tutils.CreateFreshBucket(t, proxyURL, m.bck, nil)

	m.puts()

	// Test corrupting the file contents.
	objName := m.objNames[0]
	fqn := findObjOnDisk(m.bck, objName)
	tlog.Logf("Corrupting object data %q: %s\n", objName, fqn)
	err := os.WriteFile(fqn, []byte("this file has been corrupted"), cos.PermRWR)
	tassert.CheckFatal(t, err)

	_, err = api.GetObjectWithValidation(baseParams, m.bck, objName)
	tassert.Errorf(t, err != nil, "error is nil, expected non-nil error on a a GET for an object with corrupted contents")
}

func TestRegressionBuckets(t *testing.T) {
	var (
		bck = cmn.Bck{
			Name:     testBucketName,
			Provider: cmn.ProviderAIS,
		}
		proxyURL = tutils.RandomProxyURL(t)
	)
	tutils.CreateFreshBucket(t, proxyURL, bck, nil)
	doBucketRegressionTest(t, proxyURL, regressionTestData{bck: bck})
}

func TestRenameBucket(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	var (
		bck = cmn.Bck{
			Name:     testBucketName,
			Provider: cmn.ProviderAIS,
		}
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)
		renamedBck = cmn.Bck{
			Name:     bck.Name + "_" + cos.GenTie(),
			Provider: cmn.ProviderAIS,
		}
	)
	for _, wait := range []bool{true, false} {
		t.Run(fmt.Sprintf("wait=%v", wait), func(t *testing.T) {
			tutils.CreateFreshBucket(t, proxyURL, bck, nil)
			tutils.DestroyBucket(t, proxyURL, renamedBck) // cleanup post Ctrl-C etc.
			defer tutils.DestroyBucket(t, proxyURL, renamedBck)

			bcks, err := api.ListBuckets(baseParams, cmn.QueryBcks(bck))
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

func doBucketRegressionTest(t *testing.T, proxyURL string, rtd regressionTestData) {
	var (
		m = ioContext{
			t:        t,
			bck:      rtd.bck,
			num:      2036,
			fileSize: cos.KiB,
		}
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	m.init()
	m.puts()

	if rtd.rename {
		// Rename bucket fails when rebalance or resilver is running.
		// Ensure rebalance or resilver isn't running before performing a rename.
		tutils.WaitForRebalanceToComplete(t, baseParams, rebalanceTimeout)

		_, err := api.RenameBucket(baseParams, rtd.bck, rtd.renamedBck)
		tassert.CheckFatal(t, err)

		tlog.Logf("Renamed %s (obj_cnt: %d) => %s\n", rtd.bck, m.num, rtd.renamedBck)
		if rtd.wait {
			postRenameWaitAndCheck(t, baseParams, rtd, m.num, m.objNames)
		}
		m.bck = rtd.renamedBck
	}

	m.gets()

	if !rtd.rename || rtd.wait {
		m.del()
	} else {
		postRenameWaitAndCheck(t, baseParams, rtd, m.num, m.objNames)
		m.del()
	}
}

func postRenameWaitAndCheck(t *testing.T, baseParams api.BaseParams, rtd regressionTestData, numPuts int, objNames []string) {
	xactArgs := api.XactReqArgs{Kind: cmn.ActMoveBck, Bck: rtd.renamedBck, Timeout: rebalanceTimeout}
	_, err := api.WaitForXaction(baseParams, xactArgs)
	if err != nil {
		if httpErr, ok := err.(*cmn.ErrHTTP); ok && httpErr.Status == http.StatusNotFound {
			smap := tutils.GetClusterMap(t, proxyURL)
			if smap.CountActiveTargets() == 1 {
				err = nil
			}
		}
		tassert.CheckFatal(t, err)
	} else {
		tlog.Logf("xaction (rename %s=>%s) done\n", rtd.bck, rtd.renamedBck)
	}
	bcks, err := api.ListBuckets(baseParams, cmn.QueryBcks(rtd.bck))
	tassert.CheckFatal(t, err)

	if len(bcks) != rtd.numBuckets {
		t.Fatalf("wrong number of ais buckets (names) before and after rename (before: %d. after: %+v)",
			rtd.numBuckets, bcks)
	}

	renamedBucketExists := false
	for _, bck := range bcks {
		if bck.Name == rtd.renamedBck.Name {
			renamedBucketExists = true
		} else if bck.Name == rtd.bck.Name {
			t.Fatalf("original ais bucket %s still exists after rename", rtd.bck)
		}
	}

	if !renamedBucketExists {
		t.Fatalf("renamed ais bucket %s does not exist after rename", rtd.renamedBck)
	}

	bckList, err := api.ListObjects(baseParams, rtd.renamedBck, &cmn.SelectMsg{}, 0)
	tassert.CheckFatal(t, err)
	unique := make(map[string]bool)
	for _, e := range bckList.Entries {
		base := filepath.Base(e.Name)
		unique[base] = true
	}
	if len(unique) != numPuts {
		for _, name := range objNames {
			if _, ok := unique[name]; !ok {
				tlog.Logf("not found: %s\n", name)
			}
		}
		t.Fatalf("wrong number of objects in the bucket %s renamed as %s (before: %d. after: %d)",
			rtd.bck, rtd.renamedBck, numPuts, len(unique))
	}
}

func TestRenameObjects(t *testing.T) {
	var (
		renameStr  = "rename"
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)
		bck        = cmn.Bck{
			Name:     t.Name(),
			Provider: cmn.ProviderAIS,
		}
	)

	tutils.CreateFreshBucket(t, proxyURL, bck, nil)

	objNames, _, err := tutils.PutRandObjs(tutils.PutObjectsArgs{
		ProxyURL:  proxyURL,
		Bck:       bck,
		ObjCnt:    100,
		CksumType: cmn.DefaultBckProps(bck).Cksum.Type,
	})
	tassert.CheckFatal(t, err)

	newObjNames := make([]string, 0, len(objNames))
	for i, objName := range objNames {
		newObjName := path.Join(renameStr, objName) + ".renamed" // objName fqn
		newObjNames = append(newObjNames, newObjName)

		err := api.RenameObject(baseParams, bck, objName, newObjName)
		tassert.CheckFatal(t, err)

		i++
		if i%50 == 0 {
			tlog.Logf("Renamed %s => %s\n", objName, newObjName)
		}
	}

	// Check that renamed objects exist.
	for _, newObjName := range newObjNames {
		_, err := api.GetObject(baseParams, bck, newObjName)
		tassert.CheckError(t, err)
	}
}

func TestObjectPrefix(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *cluster.Bck) {
		var (
			proxyURL  = tutils.RandomProxyURL(t)
			fileNames = prefixCreateFiles(t, proxyURL, bck.Bck, bck.Props.Cksum.Type)
		)
		prefixLookup(t, proxyURL, bck.Bck, fileNames)
		prefixCleanup(t, proxyURL, bck.Bck, fileNames)
	})
}

func TestReregisterMultipleTargets(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

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
			t:   t,
			num: 10000,
		}
	)

	m.initAndSaveCluState()
	m.expectTargets(2)
	targetsToUnregister := m.originalTargetCount - 1

	// Step 0: Collect rebalance stats
	clusterStats := tutils.GetClusterStats(t, m.proxyURL)
	for targetID, targetStats := range clusterStats.Target {
		filesSentOrig[targetID] = tutils.GetNamedTargetStats(targetStats, stats.RebTxCount)
		filesRecvOrig[targetID] = tutils.GetNamedTargetStats(targetStats, stats.RebRxCount)
		bytesSentOrig[targetID] = tutils.GetNamedTargetStats(targetStats, stats.RebTxSize)
		bytesRecvOrig[targetID] = tutils.GetNamedTargetStats(targetStats, stats.RebRxSize)
	}

	// Step 1: Unregister multiple targets
	removed := make(map[string]*cluster.Snode, m.smap.CountActiveTargets()-1)
	defer func() {
		var rebID string
		for _, tgt := range removed {
			rebID = m.stopMaintenance(tgt)
		}
		if len(removed) != 0 && rebID != "" {
			tutils.WaitForRebalanceByID(t, m.originalTargetCount, baseParams, rebID)
		}
	}()

	targets := m.smap.Tmap.ActiveNodes()
	for i := 0; i < targetsToUnregister; i++ {
		tlog.Logf("Put %s in maintenance (no rebalance)\n", targets[i].StringEx())
		args := &cmn.ActValRmNode{DaemonID: targets[i].ID(), SkipRebalance: true}
		_, err := api.StartMaintenance(baseParams, args)
		tassert.CheckFatal(t, err)
		removed[targets[i].ID()] = targets[i]
	}

	smap, err := tutils.WaitForClusterState(proxyURL, "remove targets",
		m.smap.Version, m.originalProxyCount, m.originalTargetCount-targetsToUnregister)
	tassert.CheckFatal(t, err)
	tlog.Logf("The cluster now has %d target(s)\n", smap.CountActiveTargets())

	// Step 2: PUT objects into a newly created bucket
	tutils.CreateFreshBucket(t, m.proxyURL, m.bck, nil)
	m.puts()

	// Step 3: Start performing GET requests
	go m.getsUntilStop()

	// Step 4: Simultaneously reregister each
	wg := &sync.WaitGroup{}
	for i := 0; i < targetsToUnregister; i++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			m.stopMaintenance(targets[r])
			delete(removed, targets[r].ID())
		}(i)
		time.Sleep(5 * time.Second) // wait some time before reregistering next target
	}
	wg.Wait()
	tlog.Logf("Stopping GETs...\n")
	m.stopGets()

	baseParams := tutils.BaseAPIParams(m.proxyURL)
	tutils.WaitForRebalanceToComplete(t, baseParams, rebalanceTimeout)

	clusterStats = tutils.GetClusterStats(t, m.proxyURL)
	for targetID, targetStats := range clusterStats.Target {
		filesSent += tutils.GetNamedTargetStats(targetStats, stats.RebTxCount) - filesSentOrig[targetID]
		filesRecv += tutils.GetNamedTargetStats(targetStats, stats.RebRxCount) - filesRecvOrig[targetID]
		bytesSent += tutils.GetNamedTargetStats(targetStats, stats.RebTxSize) - bytesSentOrig[targetID]
		bytesRecv += tutils.GetNamedTargetStats(targetStats, stats.RebRxSize) - bytesRecvOrig[targetID]
	}

	// Step 5: Log rebalance stats
	tlog.Logf("Rebalance sent     %s in %d files\n", cos.B2S(bytesSent, 2), filesSent)
	tlog.Logf("Rebalance received %s in %d files\n", cos.B2S(bytesRecv, 2), filesRecv)

	m.ensureNoErrors()
	m.waitAndCheckCluState()
}

func TestGetClusterStats(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)
	stats := tutils.GetClusterStats(t, proxyURL)

	for k, v := range stats.Target {
		tdstats := tutils.GetDaemonStats(t, smap.Tmap[k].PublicNet.DirectURL)
		tdcapstats := tdstats["capacity"].(map[string]interface{})
		dcapstats := v.MPCap
		for fspath, fstats := range dcapstats {
			tfstats := tdcapstats[fspath].(map[string]interface{})
			used, err := strconv.ParseInt(tfstats["used"].(string), 10, 64)
			if err != nil {
				t.Fatalf("Could not decode Target Stats: fstats.Used")
			}
			avail, err := strconv.ParseInt(tfstats["avail"].(string), 10, 64)
			if err != nil {
				t.Fatalf("Could not decode Target Stats: fstats.Avail")
			}
			pct := int64(tfstats["pct_used"].(float64))
			if used != int64(fstats.Used) || avail != int64(fstats.Avail) || pct != int64(fstats.PctUsed) {
				t.Errorf("Stats are different when queried from Target and Proxy: "+
					"Used: %v, %v | Available:  %v, %v | Percentage: %v, %v",
					tfstats["used"], fstats.Used, tfstats["avail"],
					fstats.Avail, tfstats["pct_used"], fstats.PctUsed)
			}
			if fstats.PctUsed > HighWaterMark {
				t.Errorf("Used Percentage (%d%%) is above High Watermark (%d%%)",
					fstats.PctUsed, HighWaterMark)
			}
		}
	}
}

func TestLRU(t *testing.T) {
	var (
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)

		m = &ioContext{
			t:      t,
			bck:    cliBck,
			num:    100,
			prefix: t.Name(),
		}
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{RemoteBck: true, Bck: m.bck})

	m.init()
	m.remotePuts(false /*evict*/)

	// Remember targets' watermarks
	var (
		usedPct      = int32(100)
		cluStats     = tutils.GetClusterStats(t, proxyURL)
		filesEvicted = make(map[string]int64)
		bytesEvicted = make(map[string]int64)
	)

	// Find out min usage % across all targets
	for k, v := range cluStats.Target {
		filesEvicted[k] = tutils.GetNamedTargetStats(v, "lru.evict.n")
		bytesEvicted[k] = tutils.GetNamedTargetStats(v, "lru.evict.size")
		for _, c := range v.MPCap {
			usedPct = cos.MinI32(usedPct, c.PctUsed)
		}
	}

	var (
		lowWM  = usedPct - 5
		highWM = usedPct - 2
	)
	if int(lowWM) < 2 {
		t.Skipf("The current space usage is too low (%d) for the LRU to be tested", lowWM)
		return
	}

	tlog.Logf("LRU: current min space usage in the cluster: %d%%\n", usedPct)
	tlog.Logf("setting 'lru.lowm=%d' and 'lru.highwm=%d'\n", lowWM, highWM)

	// All targets: set new watermarks; restore upon exit
	oconfig := tutils.GetClusterConfig(t)
	defer func() {
		lowWMStr, _ := cos.ConvertToString(oconfig.LRU.LowWM)
		highWMStr, _ := cos.ConvertToString(oconfig.LRU.HighWM)
		tutils.SetClusterConfig(t, cos.SimpleKVs{
			"lru.lowwm":             lowWMStr,
			"lru.highwm":            highWMStr,
			"lru.dont_evict_time":   fmt.Sprintf("%v", oconfig.LRU.DontEvictTime),
			"lru.capacity_upd_time": fmt.Sprintf("%v", oconfig.LRU.CapacityUpdTime),
		})
	}()

	// Cluster-wide reduce dont-evict-time
	lowWMStr, _ := cos.ConvertToString(lowWM)
	highWMStr, _ := cos.ConvertToString(highWM)
	tutils.SetClusterConfig(t, cos.SimpleKVs{
		"lru.lowwm":             lowWMStr,
		"lru.highwm":            highWMStr,
		"lru.dont_evict_time":   "0s",
		"lru.capacity_upd_time": "2s",
	})

	tlog.Logln("starting LRU...")
	xactID, err := api.StartXaction(baseParams, api.XactReqArgs{Kind: cmn.ActLRU})
	tassert.CheckFatal(t, err)

	args := api.XactReqArgs{ID: xactID, Kind: cmn.ActLRU, Timeout: rebalanceTimeout}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)

	// Check results
	tlog.Logln("checking the results...")
	cluStats = tutils.GetClusterStats(t, proxyURL)
	for k, v := range cluStats.Target {
		diffFilesEvicted := tutils.GetNamedTargetStats(v, "lru.evict.n") - filesEvicted[k]
		diffBytesEvicted := tutils.GetNamedTargetStats(v, "lru.evict.size") - bytesEvicted[k]
		tlog.Logf(
			"Target %s: evicted %d objects - %s (%dB) total\n",
			k, diffFilesEvicted, cos.B2S(diffBytesEvicted, 2), diffBytesEvicted,
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
		}
		bck        = cliBck
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true, RemoteBck: true, Bck: bck})

	m.initAndSaveCluState()
	m.expectTargets(2)
	m.puts()

	// 2. Evict those objects from the cache and prefetch them
	tlog.Logf("Evicting and Prefetching %d objects\n", len(m.objNames))
	xactID, err := api.EvictList(baseParams, bck, m.objNames)
	if err != nil {
		t.Error(err)
	}

	args := api.XactReqArgs{ID: xactID, Kind: cmn.ActEvictObjects, Timeout: rebalanceTimeout}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)

	// 3. Prefetch evicted objects
	xactID, err = api.PrefetchList(baseParams, bck, m.objNames)
	if err != nil {
		t.Error(err)
	}

	args = api.XactReqArgs{ID: xactID, Kind: cmn.ActPrefetchObjects, Timeout: rebalanceTimeout}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)

	// 4. Ensure that all the prefetches occurred.
	xactArgs := api.XactReqArgs{ID: xactID, Timeout: rebalanceTimeout}
	xactStats, err := api.QueryXactionStats(baseParams, xactArgs)
	tassert.CheckFatal(t, err)
	if xactStats.ObjCount() != int64(m.num) {
		t.Errorf(
			"did not prefetch all files: missing %d of %d",
			int64(m.num)-xactStats.ObjCount(), m.num,
		)
	}
}

func TestDeleteList(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *cluster.Bck) {
		var (
			err        error
			prefix     = "__listrange/tstf-"
			wg         = &sync.WaitGroup{}
			objCnt     = 100
			errCh      = make(chan error, objCnt)
			files      = make([]string, 0, objCnt)
			proxyURL   = tutils.RandomProxyURL(t)
			baseParams = tutils.BaseAPIParams(proxyURL)
		)

		// 1. Put files to delete
		for i := 0; i < objCnt; i++ {
			r, err := readers.NewRandReader(fileSize, bck.Props.Cksum.Type)
			tassert.CheckFatal(t, err)

			keyname := fmt.Sprintf("%s%d", prefix, i)

			wg.Add(1)
			go func() {
				defer wg.Done()
				tutils.Put(proxyURL, bck.Bck, keyname, r, errCh)
			}()
			files = append(files, keyname)
		}
		wg.Wait()
		tassert.SelectErr(t, errCh, "put", true)
		tlog.Logf("PUT done.\n")

		// 2. Delete the objects
		xactID, err := api.DeleteList(baseParams, bck.Bck, files)
		tassert.CheckError(t, err)

		args := api.XactReqArgs{ID: xactID, Kind: cmn.ActDeleteObjects, Timeout: rebalanceTimeout}
		_, err = api.WaitForXaction(baseParams, args)
		tassert.CheckFatal(t, err)

		// 3. Check to see that all the files have been deleted
		msg := &cmn.SelectMsg{Prefix: prefix}
		bktlst, err := api.ListObjects(baseParams, bck.Bck, msg, 0)
		tassert.CheckFatal(t, err)
		if len(bktlst.Entries) != 0 {
			t.Errorf("Incorrect number of remaining files: %d, should be 0", len(bktlst.Entries))
		}
	})
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
		proxyURL      = tutils.RandomProxyURL(t)
		baseParams    = tutils.BaseAPIParams(proxyURL)
		prefetchRange = "{1..150}"
		bck           = cliBck
	)

	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true, RemoteBck: true, Bck: bck})

	m.initAndSaveCluState()
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
	tlog.Logf("Evicting and Prefetching %d objects\n", len(files))
	rng := fmt.Sprintf("%s%s", m.prefix, prefetchRange)
	xactID, err := api.EvictRange(baseParams, bck, rng)
	tassert.CheckError(t, err)
	args := api.XactReqArgs{ID: xactID, Kind: cmn.ActEvictObjects, Timeout: rebalanceTimeout}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)

	xactID, err = api.PrefetchRange(baseParams, bck, rng)
	tassert.CheckError(t, err)
	args = api.XactReqArgs{ID: xactID, Kind: cmn.ActPrefetchObjects, Timeout: rebalanceTimeout}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)

	// 4. Ensure that all the prefetches occurred
	xactArgs := api.XactReqArgs{ID: xactID, Timeout: rebalanceTimeout}
	xactStats, err := api.QueryXactionStats(baseParams, xactArgs)
	tassert.CheckFatal(t, err)
	if xactStats.ObjCount() != int64(len(files)) {
		t.Errorf(
			"did not prefetch all files: missing %d of %d",
			int64(len(files))-xactStats.ObjCount(), len(files),
		)
	}
}

func TestDeleteRange(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *cluster.Bck) {
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
			proxyURL       = tutils.RandomProxyURL(t)
			baseParams     = tutils.BaseAPIParams(proxyURL)
		)

		// 1. Put files to delete
		for i := 0; i < objCnt; i++ {
			r, err := readers.NewRandReader(fileSize, bck.Props.Cksum.Type)
			tassert.CheckFatal(t, err)

			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				tutils.Put(proxyURL, bck.Bck, fmt.Sprintf("%s%04d", prefix, i), r, errCh)
			}(i)
		}
		wg.Wait()
		tassert.SelectErr(t, errCh, "put", true)
		tlog.Logf("PUT done.\n")

		// 2. Delete the small range of objects
		tlog.Logf("Delete in range %s\n", smallrange)
		xactID, err := api.DeleteRange(baseParams, bck.Bck, smallrange)
		tassert.CheckError(t, err)
		args := api.XactReqArgs{ID: xactID, Kind: cmn.ActDeleteObjects, Timeout: rebalanceTimeout}
		_, err = api.WaitForXaction(baseParams, args)
		tassert.CheckFatal(t, err)

		// 3. Check to see that the correct files have been deleted
		msg := &cmn.SelectMsg{Prefix: prefix}
		bktlst, err := api.ListObjects(baseParams, bck.Bck, msg, 0)
		tassert.CheckFatal(t, err)
		if len(bktlst.Entries) != objCnt-smallrangesize {
			t.Errorf("Incorrect number of remaining files: %d, should be %d", len(bktlst.Entries), objCnt-smallrangesize)
		}
		filemap := make(map[string]*cmn.BucketEntry)
		for _, entry := range bktlst.Entries {
			filemap[entry.Name] = entry
		}
		for i := 0; i < objCnt; i++ {
			keyname := fmt.Sprintf("%s%04d", prefix, i)
			_, ok := filemap[keyname]
			if ok && i >= quarter && i <= third {
				t.Errorf("File exists that should have been deleted: %s", keyname)
			} else if !ok && (i < quarter || i > third) {
				t.Errorf("File does not exist that should not have been deleted: %s", keyname)
			}
		}

		tlog.Logf("Delete in range %s\n", bigrange)
		// 4. Delete the big range of objects
		xactID, err = api.DeleteRange(baseParams, bck.Bck, bigrange)
		tassert.CheckError(t, err)
		args = api.XactReqArgs{ID: xactID, Kind: cmn.ActDeleteObjects, Timeout: rebalanceTimeout}
		_, err = api.WaitForXaction(baseParams, args)
		tassert.CheckFatal(t, err)

		// 5. Check to see that all the files have been deleted
		bktlst, err = api.ListObjects(baseParams, bck.Bck, msg, 0)
		tassert.CheckFatal(t, err)
		if len(bktlst.Entries) != 0 {
			t.Errorf("Incorrect number of remaining files: %d, should be 0", len(bktlst.Entries))
		}
	})
}

// Testing only ais bucket objects since generally not concerned with cloud bucket object deletion
func TestStressDeleteRange(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	const (
		numFiles   = 20000 // FIXME: must divide by 10 and by the numReaders
		numReaders = 200
	)

	var (
		err           error
		wg            = &sync.WaitGroup{}
		errCh         = make(chan error, numFiles)
		proxyURL      = tutils.RandomProxyURL(t)
		tenth         = numFiles / 10
		objNamePrefix = "__listrange/tstf-"
		partialRange  = fmt.Sprintf("%s{%d..%d}", objNamePrefix, 0, numFiles-tenth-1) // TODO: partial range with non-zero left boundary
		fullRange     = fmt.Sprintf("%s{0..%d}", objNamePrefix, numFiles)
		baseParams    = tutils.BaseAPIParams(proxyURL)
		bck           = cmn.Bck{
			Name:     testBucketName,
			Provider: cmn.ProviderAIS,
		}
		cksumType = cmn.DefaultBckProps(bck).Cksum.Type
	)

	tutils.CreateFreshBucket(t, proxyURL, bck, nil)

	// 1. PUT
	tlog.Logln("putting objects...")
	for i := 0; i < numReaders; i++ {
		size := rand.Int63n(cos.KiB*128) + cos.KiB/3
		tassert.CheckFatal(t, err)
		reader, err := readers.NewRandReader(size, cksumType)
		tassert.CheckFatal(t, err)

		wg.Add(1)
		go func(i int, reader readers.Reader) {
			defer wg.Done()

			for j := 0; j < numFiles/numReaders; j++ {
				objName := fmt.Sprintf("%s%d", objNamePrefix, i*numFiles/numReaders+j)
				putArgs := api.PutObjectArgs{
					BaseParams: baseParams,
					Bck:        bck,
					Object:     objName,
					Cksum:      reader.Cksum(),
					Reader:     reader,
				}
				err = api.PutObject(putArgs)
				if err != nil {
					errCh <- err
				}
				reader.Seek(0, io.SeekStart)
			}
		}(i, reader)
	}
	wg.Wait()
	tassert.SelectErr(t, errCh, "put", true)

	// 2. Delete a range of objects
	tlog.Logf("Deleting objects in range: %s\n", partialRange)
	xactID, err := api.DeleteRange(baseParams, bck, partialRange)
	tassert.CheckError(t, err)
	args := api.XactReqArgs{ID: xactID, Kind: cmn.ActDeleteObjects, Timeout: rebalanceTimeout}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)

	// 3. Check to see that correct objects have been deleted
	expectedRemaining := tenth
	msg := &cmn.SelectMsg{Prefix: objNamePrefix}
	bckList, err := api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	if len(bckList.Entries) != expectedRemaining {
		t.Errorf("Incorrect number of remaining objects: %d, expected: %d",
			len(bckList.Entries), expectedRemaining)
	}

	objNames := make(map[string]*cmn.BucketEntry)
	for _, entry := range bckList.Entries {
		objNames[entry.Name] = entry
	}
	for i := 0; i < numFiles; i++ {
		objName := fmt.Sprintf("%s%d", objNamePrefix, i)
		_, ok := objNames[objName]
		if ok && i < numFiles-tenth {
			t.Errorf("%s exists (expected to be deleted)", objName)
		} else if !ok && i >= numFiles-tenth {
			t.Errorf("%s does not exist", objName)
		}
	}

	// 4. Delete the entire range of objects
	tlog.Logf("Deleting objects in range: %s\n", fullRange)
	xactID, err = api.DeleteRange(baseParams, bck, fullRange)
	tassert.CheckError(t, err)
	args = api.XactReqArgs{ID: xactID, Kind: cmn.ActDeleteObjects, Timeout: rebalanceTimeout}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)

	// 5. Check to see that all files have been deleted
	msg = &cmn.SelectMsg{Prefix: objNamePrefix}
	bckList, err = api.ListObjects(baseParams, bck, msg, 0)
	tassert.CheckFatal(t, err)
	if len(bckList.Entries) != 0 {
		t.Errorf("Incorrect number of remaining files: %d, should be 0", len(bckList.Entries))
	}
}

func TestXactionNotFound(t *testing.T) {
	var (
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)

		missingID = "incorrect"
	)

	_, err := api.GetXactionStatsByID(baseParams, missingID)
	tutils.CheckErrIsNotFound(t, err)
}
