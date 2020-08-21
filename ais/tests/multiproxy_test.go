// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/ais"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/containers"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/readers"
	"github.com/NVIDIA/aistore/tutils/tassert"
	"github.com/OneOfOne/xxhash"
	jsoniter "github.com/json-iterator/go"
)

const (
	localBucketDir  = "multipleproxy"
	defaultChanSize = 10
)

var (
	voteTests = []Test{
		{"PrimaryCrash", primaryCrashElectRestart},
		{"SetPrimaryBackToOriginal", primarySetToOriginal},
		{"proxyCrash", proxyCrash},
		{"PrimaryAndTargetCrash", primaryAndTargetCrash},
		{"PrimaryAndProxyCrash", primaryAndProxyCrash},
		{"CrashAndFastRestore", crashAndFastRestore},
		{"TargetRejoin", targetRejoin},
		{"JoinWhileVoteInProgress", joinWhileVoteInProgress},
		{"MinorityTargetMapVersionMismatch", minorityTargetMapVersionMismatch},
		{"MajorityTargetMapVersionMismatch", majorityTargetMapVersionMismatch},
		{"ConcurrentPutGetDel", concurrentPutGetDel},
		{"ProxyStress", proxyStress},
		{"NetworkFailure", networkFailure},
		{"PrimaryAndNextCrash", primaryAndNextCrash},
	}

	icTests = []Test{
		{"ICMemberLeaveAndRejoin", icMemberLeaveAndRejoin},
		{"ICKillAndRestorePrimary", icKillAndRestorePrimary},
		{"ICSyncOwnTbl", icSyncOwnershipTable},
		{"ICSinglePrimaryRevamp", icSinglePrimaryRevamp},
	}
)

type restoreCmd struct {
	node *cluster.Snode
	cmd  string
	args []string
}

func TestMultiProxy(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})

	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)
	if smap.CountProxies() < 3 {
		t.Fatal("Not enough proxies to run proxy tests, must be more than 2")
	}

	if smap.CountTargets() < 1 {
		t.Fatal("Not enough targets to run proxy tests, must be at least 1")
	}

	for _, test := range voteTests {
		t.Run(test.name, test.method)
		if t.Failed() && abortonerr {
			t.FailNow()
		}
	}

	clusterHealthCheck(t, smap)
}

// clusterHealthCheck verifies the cluster has the same servers after tests
// note: add verify primary if primary is reset
func clusterHealthCheck(t *testing.T, smapBefore *cluster.Smap) {
	proxyURL := tutils.RandomProxyURL(t)
	smapAfter := tutils.GetClusterMap(t, proxyURL)
	if smapAfter.CountTargets() != smapBefore.CountTargets() {
		t.Fatalf("Number of targets mismatch, before = %d, after = %d",
			smapBefore.CountTargets(), smapAfter.CountTargets())
	}

	if smapAfter.CountProxies() != smapBefore.CountProxies() {
		t.Fatalf("Number of proxies mismatch, before = %d, after = %d",
			smapBefore.CountProxies(), smapAfter.CountProxies())
	}

	for _, b := range smapBefore.Tmap {
		a, ok := smapAfter.Tmap[b.ID()]
		if !ok {
			t.Fatalf("Failed to find target %s", b.ID())
		}

		if !a.Equals(b) {
			t.Fatalf("Target %s changed, before = %+v, after = %+v", b.ID(), b, a)
		}
	}

	for _, b := range smapBefore.Pmap {
		a, ok := smapAfter.Pmap[b.ID()]
		if !ok {
			t.Fatalf("Failed to find proxy %s", b.ID())
		}

		// note: can't compare Primary field unless primary is always reset to the original one
		if !a.Equals(b) {
			t.Fatalf("Proxy %s changed, before = %+v, after = %+v", b.ID(), b, a)
		}
	}

	if containers.DockerRunning() {
		pCnt, tCnt := containers.ContainerCount()
		if pCnt != smapAfter.CountProxies() {
			t.Fatalf("Some proxy containers crashed: expected %d, found %d containers",
				smapAfter.CountProxies(), pCnt)
		}
		if tCnt != smapAfter.CountTargets() {
			t.Fatalf("Some target containers crashed: expected %d, found %d containers",
				smapAfter.CountTargets(), tCnt)
		}
		return
	}

	// no proxy/target died (or not restored)
	for _, b := range smapBefore.Tmap {
		_, err := getPID(b.PublicNet.DaemonPort)
		tassert.CheckFatal(t, err)
	}

	for _, b := range smapBefore.Pmap {
		_, err := getPID(b.PublicNet.DaemonPort)
		tassert.CheckFatal(t, err)
	}
}

// primaryCrashElectRestart kills the current primary proxy, wait for the new primary proxy is up and verifies it,
// restores the original primary proxy as non primary
func primaryCrashElectRestart(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)
	tutils.Logf("targets: %d, proxies: %d\n", smap.CountTargets(), smap.CountProxies())

	newPrimaryID, newPrimaryURL, err := chooseNextProxy(smap)
	tassert.CheckFatal(t, err)
	checkPmapVersions(t, proxyURL)

	oldPrimaryURL := smap.Primary.PublicNet.DirectURL
	oldPrimaryID := smap.Primary.ID()
	tutils.Logf("New primary: %s --> %s\n", newPrimaryID, newPrimaryURL)
	tutils.Logf("Killing primary: %s --> %s\n", oldPrimaryURL, oldPrimaryID)

	killedURL := smap.Primary.PublicNet.DirectURL
	cmd, err := kill(smap.Primary)
	// cmd and args are the original command line of how the proxy is started
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForPrimaryProxy(newPrimaryURL, "to designate new primary",
		smap.Version, testing.Verbose())
	tassert.CheckFatal(t, err)
	tutils.Logf("New primary elected: %s\n", newPrimaryID)

	if smap.Primary.ID() != newPrimaryID {
		t.Fatalf("Wrong primary proxy: %s, expecting: %s", smap.Primary.ID(), newPrimaryID)
	}

	// re-construct the command line to start the original proxy but add the current primary proxy to the args
	err = restore(cmd, false, "proxy (prev primary)")
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForPrimaryProxy(newPrimaryURL, "to restore", smap.Version, testing.Verbose())
	tassert.CheckFatal(t, err)

	tutils.WaitStarted(t, killedURL)

	if _, ok := smap.Pmap[oldPrimaryID]; !ok {
		t.Fatalf("Previous primary proxy did not rejoin the cluster")
	}
	checkPmapVersions(t, newPrimaryURL)
}

// primaryAndTargetCrash kills the primary p[roxy and one random target, verifies the next in
// line proxy becomes the new primary, restore the target and proxy, restore original primary.
func primaryAndTargetCrash(t *testing.T) {
	if containers.DockerRunning() {
		t.Skip("Skipped because setting new primary URL in command line for docker is not supported")
	}

	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)
	tutils.Logf("targets: %d, proxies: %d\n", smap.CountTargets(), smap.CountProxies())

	newPrimaryID, newPrimaryURL, err := chooseNextProxy(smap)
	tassert.CheckFatal(t, err)

	oldPrimaryURL := smap.Primary.PublicNet.DirectURL
	tutils.Logf("Killing proxy %s - %s\n", oldPrimaryURL, smap.Primary.ID())
	killedURL := smap.Primary.PublicNet.DirectURL
	cmd, err := kill(smap.Primary)
	tassert.CheckFatal(t, err)

	// Select a random target
	var (
		targetURL       string
		targetID        string
		targetNode      *cluster.Snode
		origTargetCount = smap.CountTargets()
		origProxyCount  = smap.CountProxies()
	)

	for _, v := range smap.Tmap {
		targetURL = v.PublicNet.DirectURL
		targetID = v.ID()
		targetNode = v
		break
	}

	tutils.Logf("Killing target: %s - %s\n", targetURL, targetID)
	tcmd, err := kill(targetNode)
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForPrimaryProxy(newPrimaryURL, "to designate new primary",
		smap.Version, testing.Verbose(), origProxyCount-1, origTargetCount-1)
	tassert.CheckFatal(t, err)

	if smap.Primary.ID() != newPrimaryID {
		t.Fatalf("Wrong primary proxy: %s, expecting: %s", smap.Primary.ID(), newPrimaryID)
	}

	err = restore(tcmd, false, "target")
	tassert.CheckFatal(t, err)

	err = restore(cmd, false, "proxy (prev primary)")
	tassert.CheckFatal(t, err)

	_, err = tutils.WaitForPrimaryProxy(newPrimaryURL, "to restore", smap.Version,
		testing.Verbose(), origProxyCount, origTargetCount)
	tassert.CheckFatal(t, err)

	tutils.WaitStarted(t, killedURL)
}

// A very simple test to check if a primary proxy can detect non-primary one
// dies and then update and sync SMap
func proxyCrash(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)
	tutils.Logf("targets: %d, proxies: %d\n", smap.CountTargets(), smap.CountProxies())

	oldPrimaryURL, oldPrimaryID := smap.Primary.PublicNet.DirectURL, smap.Primary.ID()
	tutils.Logf("Primary proxy: %s\n", oldPrimaryURL)

	var (
		secondURL      string
		secondID       string
		secondNode     *cluster.Snode
		origProxyCount = smap.CountProxies()
	)

	// Select a random non-primary proxy
	for k, v := range smap.Pmap {
		if k != oldPrimaryID {
			secondURL = v.PublicNet.DirectURL
			secondID = v.ID()
			secondNode = v
			break
		}
	}

	tutils.Logf("Killing non-primary proxy: %s - %s\n", secondURL, secondID)
	secondCmd, err := kill(secondNode)
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForPrimaryProxy(oldPrimaryURL, "to propagate new Smap",
		smap.Version, testing.Verbose(), origProxyCount-1)
	tassert.CheckFatal(t, err)

	err = restore(secondCmd, false, "proxy")
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForPrimaryProxy(proxyURL, "to restore", smap.Version,
		testing.Verbose(), origProxyCount)
	tassert.CheckFatal(t, err)

	tutils.WaitStarted(t, secondURL)

	if _, ok := smap.Pmap[secondID]; !ok {
		t.Fatalf("Non-primary proxy did not rejoin the cluster.")
	}
}

// primaryAndProxyCrash kills primary proxy and one another proxy(not the next in line primary)
// and restore them afterwards
func primaryAndProxyCrash(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)
	tutils.Logf("targets: %d, proxies: %d\n", smap.CountTargets(), smap.CountProxies())

	newPrimaryID, newPrimaryURL, err := chooseNextProxy(smap)
	tassert.CheckFatal(t, err)

	oldPrimaryURL, oldPrimaryID := smap.Primary.PublicNet.DirectURL, smap.Primary.ID()
	tutils.Logf("Killing primary proxy: %s - %s\n", oldPrimaryURL, oldPrimaryID)
	cmd, err := kill(smap.Primary)
	tassert.CheckFatal(t, err)

	var (
		secondURL      string
		secondID       string
		secondNode     *cluster.Snode
		origProxyCount = smap.CountProxies()
	)

	// Select a third random proxy
	// Do not choose the next primary in line, or the current primary proxy
	// This is because the system currently cannot recover if the next proxy in line is
	// also killed.
	for k, v := range smap.Pmap {
		if k != newPrimaryID && k != oldPrimaryID {
			secondURL = v.PublicNet.DirectURL
			secondID = v.ID()
			secondNode = v
			break
		}
	}

	time.Sleep(10 * time.Second) // TODO -- FIXME: remove

	tutils.Logf("Killing non-primary proxy: %s - %s\n", secondURL, secondID)
	secondCmd, err := kill(secondNode)
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForPrimaryProxy(newPrimaryURL, "to designate new primary",
		smap.Version, testing.Verbose(), origProxyCount-2)
	tassert.CheckFatal(t, err)

	err = restore(cmd, true, "proxy (prev primary)")
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForPrimaryProxy(newPrimaryURL, "to designate new primary",
		smap.Version, testing.Verbose(), origProxyCount-1)
	tassert.CheckFatal(t, err)
	err = restore(secondCmd, false, "proxy")
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForPrimaryProxy(newPrimaryURL, "to restore",
		smap.Version, testing.Verbose(), origProxyCount)
	tassert.CheckFatal(t, err)

	if smap.Primary.ID() != newPrimaryID {
		t.Fatalf("Wrong primary proxy: %s, expecting: %s", smap.Primary.ID(), newPrimaryID)
	}

	tutils.WaitStarted(t, oldPrimaryURL)
	tutils.WaitStarted(t, secondURL)

	if _, ok := smap.Pmap[oldPrimaryID]; !ok {
		t.Fatalf("Previous primary proxy %s did not rejoin the cluster", oldPrimaryID)
	}

	if _, ok := smap.Pmap[secondID]; !ok {
		t.Fatalf("Second proxy %s did not rejoin the cluster", secondID)
	}
}

// targetRejoin kills a random selected target, wait for it to rejoin and verifies it
func targetRejoin(t *testing.T) {
	var (
		id       string
		node     *cluster.Snode
		proxyURL = tutils.RandomProxyURL(t)
	)

	smap := tutils.GetClusterMap(t, proxyURL)
	tutils.Logf("targets: %d, proxies: %d\n", smap.CountTargets(), smap.CountProxies())

	for _, v := range smap.Tmap {
		id = v.ID()
		node = v
		break
	}

	cmd, err := kill(node)
	tassert.CheckFatal(t, err)
	smap, err = tutils.WaitForPrimaryProxy(proxyURL, "to synchronize on 'target crashed'",
		smap.Version, testing.Verbose())
	tassert.CheckFatal(t, err)

	if _, ok := smap.Tmap[id]; ok {
		t.Fatalf("Killed target was not removed from the Smap: %v", id)
	}

	err = restore(cmd, false, "target")
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForPrimaryProxy(proxyURL, "to synchronize on 'target rejoined'",
		smap.Version, testing.Verbose())
	tassert.CheckFatal(t, err)

	if _, ok := smap.Tmap[id]; !ok {
		t.Fatalf("Restarted target %s did not rejoin the cluster", id)
	}
}

// crashAndFastRestore kills the primary and restores it before a new leader is elected
func crashAndFastRestore(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)
	tutils.Logf("targets: %d, proxies: %d\n", smap.CountTargets(), smap.CountProxies())

	id := smap.Primary.ID()
	oldPrimaryURL := smap.Primary.URL(cmn.NetworkPublic)
	tutils.Logf("The current primary %s, Smap version %d\n", id, smap.Version)

	cmd, err := kill(smap.Primary)
	tassert.CheckFatal(t, err)

	// quick crash and recover
	time.Sleep(2 * time.Second)
	err = restore(cmd, true, "proxy (primary)")
	tassert.CheckFatal(t, err)

	tutils.Logf("The %s is currently restarting\n", id)

	// Note: using (version - 1) because the primary will restart with its old version,
	//       there will be no version change for this restore, so force beginning version to 1 less
	//       than the original version in order to use WaitForPrimaryProxy
	smap, err = tutils.WaitForPrimaryProxy(proxyURL, "to restore",
		smap.Version-1, testing.Verbose())
	tassert.CheckFatal(t, err)

	tutils.WaitStarted(t, oldPrimaryURL)

	if smap.Primary.ID() != id {
		t.Fatalf("Wrong primary proxy: %s, expecting: %s", smap.Primary.ID(), id)
	}
}

func joinWhileVoteInProgress(t *testing.T) {
	if containers.DockerRunning() {
		t.Skip("Skipping because mocking is not supported for docker cluster")
	}

	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)
	tutils.Logf("targets: %d, proxies: %d\n", smap.CountTargets(), smap.CountProxies())

	newPrimaryID, newPrimaryURL, err := chooseNextProxy(smap)
	oldTargetCnt := smap.CountTargets()
	oldProxyCnt := smap.CountProxies()
	tassert.CheckFatal(t, err)

	stopch := make(chan struct{})
	errCh := make(chan error, 10)
	mocktgt := &voteRetryMockTarget{
		voteInProgress: true,
		errCh:          errCh,
	}

	go runMockTarget(t, proxyURL, mocktgt, stopch, smap)

	smap, err = tutils.WaitForPrimaryProxy(proxyURL, "to synchronize on 'new mock target'",
		smap.Version, testing.Verbose(), 0, oldTargetCnt+1)
	tassert.CheckFatal(t, err)

	oldPrimaryID := smap.Primary.ID()
	cmd, err := kill(smap.Primary)
	tassert.CheckFatal(t, err)

	_, err = tutils.WaitForPrimaryProxy(newPrimaryURL, "to designate new primary",
		smap.Version, testing.Verbose(), oldProxyCnt-1, oldTargetCnt+1)
	tassert.CheckFatal(t, err)

	err = restore(cmd, true, "proxy (prev primary)")
	tassert.CheckFatal(t, err)

	// check if the previous primary proxy has not yet rejoined the cluster
	// it should be waiting for the mock target to return voteInProgress=false
	time.Sleep(5 * time.Second)
	smap = tutils.GetClusterMap(t, newPrimaryURL)
	if smap.Primary.ID() != newPrimaryID {
		t.Fatalf("Wrong primary proxy: %s, expecting: %s", smap.Primary.ID(), newPrimaryID)
	}
	//
	// FIXME: election is in progress if and only when xaction(cmn.ActElection) is running -
	//        simulating the scenario via mocktgt.voteInProgress = true is incorrect
	//
	// if _, ok := smap.Pmap[oldPrimaryID]; ok {
	//	t.Fatalf("Previous primary proxy rejoined the cluster during a vote")
	// }
	mocktgt.voteInProgress = false
	// smap, err = tutils.WaitForPrimaryProxy(newPrimaryURL, "to synchronize new Smap",
	// smap.Version, testing.Verbose(), oldProxyCnt, oldTargetCnt+1)
	// tassert.CheckFatal(t, err)
	//
	// end of FIXME

	if smap.Primary.ID() != newPrimaryID {
		t.Fatalf("Wrong primary proxy: %s, expectinge: %s", smap.Primary.ID(), newPrimaryID)
	}

	if _, ok := smap.Pmap[oldPrimaryID]; !ok {
		t.Fatalf("Previous primary proxy did not rejoin the cluster")
	}

	// time to kill the mock target, job well done
	var v struct{}
	stopch <- v
	close(stopch)
	select {
	case err := <-errCh:
		t.Errorf("Mock Target Error: %v", err)

	default:
	}

	_, err = tutils.WaitForPrimaryProxy(newPrimaryURL, "to kill mock target",
		smap.Version, testing.Verbose())
	tassert.CheckFatal(t, err)
}

func minorityTargetMapVersionMismatch(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)
	targetMapVersionMismatch(
		func(i int) int {
			return i/4 + 1
		}, t, proxyURL)
}

func majorityTargetMapVersionMismatch(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)
	targetMapVersionMismatch(
		func(i int) int {
			return i/2 + 1
		}, t, proxyURL)
}

// targetMapVersionMismatch updates map version of a few targets, kill the primary proxy
// wait for the new leader to come online
func targetMapVersionMismatch(getNum func(int) int, t *testing.T, proxyURL string) {
	smap := tutils.GetClusterMap(t, proxyURL)
	tutils.Logf("targets: %d, proxies: %d\n", smap.CountTargets(), smap.CountProxies())

	oldVer := smap.Version
	oldProxyCnt := smap.CountProxies()

	smap.Version++
	jsonMap, err := jsoniter.Marshal(smap)
	tassert.CheckFatal(t, err)

	n := getNum(smap.CountTargets() + smap.CountProxies() - 1)
	for _, v := range smap.Tmap {
		if n == 0 {
			break
		}

		baseParams := tutils.BaseAPIParams(v.URL(cmn.NetworkPublic))
		baseParams.Method = http.MethodPut
		err = api.DoHTTPRequest(api.ReqParams{
			BaseParams: baseParams,
			Path:       cmn.URLPath(cmn.Version, cmn.Daemon, cmn.SyncSmap),
			Body:       jsonMap,
		})
		tassert.CheckFatal(t, err)
		n--
	}

	nextProxyID, nextProxyURL, err := chooseNextProxy(smap)
	tassert.CheckFatal(t, err)

	killedURL := smap.Primary.PublicNet.DirectURL
	cmd, err := kill(smap.Primary)
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForPrimaryProxy(nextProxyURL, "to designate new primary",
		oldVer, testing.Verbose(), oldProxyCnt-1)
	tassert.CheckFatal(t, err)

	if smap.Primary == nil {
		t.Fatalf("Nil Primary in retrieved Smap")
	}

	if smap.Primary.ID() != nextProxyID {
		t.Fatalf("Wrong primary proxy: %s, expecting: %s", smap.Primary.ID(), nextProxyID)
	}

	err = restore(cmd, false, "proxy (prev primary)")
	tassert.CheckFatal(t, err)

	_, err = tutils.WaitForPrimaryProxy(nextProxyURL, "to restore", smap.Version, testing.Verbose())
	tassert.CheckFatal(t, err)

	tutils.WaitStarted(t, killedURL)
}

// concurrentPutGetDel does put/get/del sequence against all proxies concurrently
func concurrentPutGetDel(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *cluster.Bck) {
		proxyURL := tutils.RandomProxyURL(t)
		smap := tutils.GetClusterMap(t, proxyURL)
		tutils.Logf("targets: %d, proxies: %d\n", smap.CountTargets(), smap.CountProxies())

		var (
			wg        = &sync.WaitGroup{}
			errCh     = make(chan error, smap.CountProxies())
			cksumType = bck.Props.Cksum.Type
		)

		// cid = a goroutine ID to make filenames unique
		// otherwise it is easy to run into a trouble when 2 goroutines do:
		//   1PUT 2PUT 1DEL 2DEL
		// And the second goroutine fails with error "object does not exist"
		for _, v := range smap.Pmap {
			wg.Add(1)
			go func(url string) {
				defer wg.Done()
				errCh <- proxyPutGetDelete(100, url, bck.Bck, cksumType)
			}(v.PublicNet.DirectURL)
		}

		wg.Wait()
		close(errCh)

		for err := range errCh {
			tassert.CheckFatal(t, err)
		}
	})
}

// proxyPutGetDelete repeats put/get/del N times, all requests go to the same proxy
func proxyPutGetDelete(count int, proxyURL string, bck cmn.Bck, cksumType string) error {
	baseParams := tutils.BaseAPIParams(proxyURL)
	for i := 0; i < count; i++ {
		reader, err := readers.NewRandReader(fileSize, cksumType)
		if err != nil {
			return fmt.Errorf("error creating reader: %v", err)
		}
		fname := tutils.GenRandomString(fnlen)
		keyname := fmt.Sprintf("%s/%s", localBucketDir, fname)
		putArgs := api.PutObjectArgs{
			BaseParams: baseParams,
			Bck:        bck,
			Object:     keyname,
			Cksum:      reader.Cksum(),
			Reader:     reader,
		}
		if err = api.PutObject(putArgs); err != nil {
			return fmt.Errorf("error executing put: %v", err)
		}
		if _, err = api.GetObject(baseParams, bck, keyname); err != nil {
			return fmt.Errorf("error executing get: %v", err)
		}
		if err = tutils.Del(proxyURL, bck, keyname, nil /* wg */, nil /* errCh */, true /* silent */); err != nil {
			return fmt.Errorf("error executing del: %v", err)
		}
	}

	return nil
}

// putGetDelWorker does put/get/del in sequence; if primary proxy change happens, it checks the failed delete
// channel and route the deletes to the new primary proxy
// stops when told to do so via the stop channel
func putGetDelWorker(proxyURL string, stopCh <-chan struct{}, proxyURLCh <-chan string, errCh chan error,
	wg *sync.WaitGroup) {
	defer wg.Done()

	missedDeleteCh := make(chan string, 100)
	baseParams := tutils.BaseAPIParams(proxyURL)

	bck := cmn.Bck{
		Name:     TestBucketName,
		Provider: cmn.ProviderAIS,
	}
	cksumType := cmn.DefaultBucketProps().Cksum.Type
loop:
	for {
		select {
		case <-stopCh:
			close(errCh)
			break loop

		case url := <-proxyURLCh:
			// send failed deletes to the new primary proxy
		deleteLoop:
			for {
				select {
				case objName := <-missedDeleteCh:
					err := tutils.Del(url, bck, objName, nil, errCh, true)
					if err != nil {
						missedDeleteCh <- objName
					}

				default:
					break deleteLoop
				}
			}

		default:
		}

		reader, err := readers.NewRandReader(fileSize, cksumType)
		if err != nil {
			errCh <- err
			continue
		}

		fname := tutils.GenRandomString(fnlen)
		objName := fmt.Sprintf("%s/%s", localBucketDir, fname)
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
			continue
		}
		_, err = api.GetObject(baseParams, bck, objName)
		if err != nil {
			errCh <- err
		}

		err = tutils.Del(proxyURL, bck, objName, nil, errCh, true)
		if err != nil {
			missedDeleteCh <- objName
		}
	}

	// process left over not deleted objects
	close(missedDeleteCh)
	for n := range missedDeleteCh {
		tutils.Del(proxyURL, bck, n, nil, nil, true)
	}
}

// primaryKiller kills primary proxy, notifies all workers, and restore it.
func primaryKiller(t *testing.T, proxyURL string, stopch <-chan struct{}, proxyurlchs []chan string,
	errCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()

loop:
	for {
		select {
		case <-stopch:
			close(errCh)
			for _, ch := range proxyurlchs {
				close(ch)
			}

			break loop

		default:
		}

		smap := tutils.GetClusterMap(t, proxyURL)
		_, nextProxyURL, err := chooseNextProxy(smap)
		currentPrimaryURL := smap.Primary.URL(cmn.NetworkPublic)
		tassert.CheckFatal(t, err)

		cmd, err := kill(smap.Primary)
		tassert.CheckFatal(t, err)

		// let the workers go to the dying primary for a little while longer to generate errored requests
		time.Sleep(time.Second)

		smap, err = tutils.WaitForPrimaryProxy(nextProxyURL, "to propagate 'primary crashed'",
			smap.Version, testing.Verbose())
		tassert.CheckFatal(t, err)

		for _, ch := range proxyurlchs {
			ch <- nextProxyURL
		}

		err = restore(cmd, false, "proxy (prev primary)")
		tassert.CheckFatal(t, err)

		_, err = tutils.WaitForPrimaryProxy(nextProxyURL, "to synchronize on 'primary restored'",
			smap.Version, testing.Verbose())
		tassert.CheckFatal(t, err)

		tutils.WaitStarted(t, currentPrimaryURL)
	}
}

// proxyStress starts a group of workers doing put/get/del in sequence against primary proxy,
// while the operations are on going, a separate go routine kills the primary proxy, notifies all
// workers about the proxy change, restart the killed proxy as a non-primary proxy.
// the process is repeated until a pre-defined time duration is reached.
func proxyStress(t *testing.T) {
	var (
		wg          sync.WaitGroup
		errChs      = make([]chan error, numworkers+1)
		stopChs     = make([]chan struct{}, numworkers+1)
		proxyURLChs = make([]chan string, numworkers)
		bck         = cmn.Bck{
			Name:     TestBucketName,
			Provider: cmn.ProviderAIS,
		}
		proxyURL = tutils.RandomProxyURL(t)
	)

	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	// start all workers
	for i := 0; i < numworkers; i++ {
		errChs[i] = make(chan error, defaultChanSize)
		stopChs[i] = make(chan struct{}, defaultChanSize)
		proxyURLChs[i] = make(chan string, defaultChanSize)

		wg.Add(1)
		go putGetDelWorker(proxyURL, stopChs[i], proxyURLChs[i], errChs[i], &wg)

		// stagger the workers so they don't always do the same operation at the same time
		n := cmn.NowRand().Intn(999)
		time.Sleep(time.Duration(n+1) * time.Millisecond)
	}

	errChs[numworkers] = make(chan error, defaultChanSize)
	stopChs[numworkers] = make(chan struct{}, defaultChanSize)
	wg.Add(1)
	go primaryKiller(t, proxyURL, stopChs[numworkers], proxyURLChs, errChs[numworkers], &wg)

	timer := time.After(multiProxyTestDuration)
loop:
	for {
		for _, ch := range errChs {
			select {
			case <-timer:
				break loop

			case <-ch:
				// read errors, throw away, this is needed to unblock the workers

			default:
			}
		}
	}

	// stop all workers
	for _, stopCh := range stopChs {
		stopCh <- struct{}{}
		close(stopCh)
	}

	wg.Wait()
}

// smap 	- current Smap
// directURL	- DirectURL of the proxy that we send the request to
//           	  (not necessarily the current primary)
// toID, toURL 	- DaemonID and DirectURL of the proxy that must become the new primary
func setPrimaryTo(t *testing.T, proxyURL string, smap *cluster.Smap, directURL, toID string) (newSmap *cluster.Smap) {
	if directURL == "" {
		directURL = smap.Primary.PublicNet.DirectURL
	}
	// http://host:8081/v1/cluster/proxy/15205:8080

	baseParams := tutils.BaseAPIParams(directURL)
	tutils.Logf("Setting primary from %s to %s\n", smap.Primary.ID(), toID)
	err := api.SetPrimaryProxy(baseParams, toID)
	tassert.CheckFatal(t, err)

	newSmap, err = tutils.WaitForPrimaryProxy(proxyURL, "to designate new primary ID="+toID,
		smap.Version, testing.Verbose())
	tassert.CheckFatal(t, err)
	if newSmap.Primary.ID() != toID {
		t.Fatalf("Expected primary=%s, got %s", toID, newSmap.Primary.ID())
	}
	checkPmapVersions(t, proxyURL)
	return
}

func chooseNextProxy(smap *cluster.Smap) (proxyid, proxyURL string, err error) {
	pid, err := hrwProxyTest(smap, smap.Primary.ID())
	pi := smap.Pmap[pid]
	if err != nil {
		return
	}

	return pi.ID(), pi.PublicNet.DirectURL, nil
}

func kill(node *cluster.Snode) (cmd restoreCmd, err error) {
	var (
		daemonID = node.ID()
		port     = node.PublicNet.DaemonPort
		pid      string
	)
	cmd.node = node
	if containers.DockerRunning() {
		tutils.Logf("Stopping container %s\n", daemonID)
		err := containers.StopContainer(daemonID)
		return cmd, err
	}

	pid, cmd.cmd, cmd.args, err = getProcess(port)
	if err != nil {
		return
	}
	_, err = exec.Command("kill", "-2", pid).CombinedOutput()
	if err != nil {
		return
	}
	// wait for the process to actually disappear
	to := time.Now().Add(time.Second * 30)
	for {
		_, _, _, errpid := getProcess(port)
		if errpid != nil {
			break
		}
		if time.Now().After(to) {
			err = fmt.Errorf("failed to kill -2 process pid=%s at port %s", pid, port)
			break
		}
		time.Sleep(time.Second)
	}

	exec.Command("kill", "-9", pid).CombinedOutput()
	time.Sleep(time.Second)

	if err != nil {
		_, _, _, errpid := getProcess(port)
		if errpid != nil {
			err = nil
		} else {
			err = fmt.Errorf("failed to kill -9 process pid=%s at port %s", pid, port)
		}
	}
	return
}

func restore(cmd restoreCmd, asPrimary bool, tag string) error {
	if containers.DockerRunning() {
		tutils.Logf("Restarting %s container %s\n", tag, cmd)
		return containers.RestartContainer(cmd.cmd)
	}
	if asPrimary && !cmn.StringInSlice("-skip_startup=true", cmd.args) {
		// 50-50 to apply flag or not (randomize to test different startup paths)
		if rand.Intn(2) == 0 {
			cmd.args = append(cmd.args, "-skip_startup=true")
		}
	}
	tutils.Logf("Restoring %s: %s %+v\n", tag, cmd, cmd.args)

	ncmd := exec.Command(cmd.cmd, cmd.args...)
	// When using Ctrl-C on test, children (restored daemons) should not be
	// killed as well.
	// (see: https://groups.google.com/forum/#!topic/golang-nuts/shST-SDqIp4)
	ncmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
	if asPrimary {
		// Sets the environment variable to start as primary proxy to true
		env := os.Environ()
		env = append(env, fmt.Sprintf("%s=true", cmn.EnvVars.IsPrimary))
		ncmd.Env = env
	}

	err := ncmd.Start()
	ncmd.Process.Release()
	return err
}

// getPID uses 'lsof' to find the pid of the ais process listening on a port
func getPID(port string) (string, error) {
	output, err := exec.Command("lsof", []string{"-sTCP:LISTEN", "-i", ":" + port}...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("error executing LSOF command: %v", err)
	}

	// Skip lines before first appearance of "COMMAND"
	lines := strings.Split(string(output), "\n")
	i := 0
	for ; ; i++ {
		if strings.HasPrefix(lines[i], "COMMAND") {
			break
		}
	}

	// second colume is the pid
	return strings.Fields(lines[i+1])[1], nil
}

// getProcess finds the ais process by 'lsof' using a port number, it finds the ais process's
// original command line by 'ps', returns the command line for later to restart(restore) the process.
func getProcess(port string) (string, string, []string, error) {
	pid, err := getPID(port)
	if err != nil {
		return "", "", nil, fmt.Errorf("error getting pid on port: %v", err)
	}

	output, err := exec.Command("ps", "-p", pid, "-o", "command").CombinedOutput()
	if err != nil {
		return "", "", nil, fmt.Errorf("error executing PS command: %v", err)
	}

	line := strings.Split(string(output), "\n")[1]
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return "", "", nil, fmt.Errorf("no returned fields")
	}

	return pid, fields[0], fields[1:], nil
}

// Read Pmap from all proxies and checks versions. If any proxy's smap version
// differs from primary's one then an error returned
func checkPmapVersions(t *testing.T, proxyURL string) {
	smapPrimary := tutils.GetClusterMap(t, proxyURL)
	for proxyID, proxyInfo := range smapPrimary.Pmap {
		if proxyURL == proxyInfo.PublicNet.DirectURL {
			continue
		}
		smap := tutils.GetClusterMap(t, proxyInfo.PublicNet.DirectURL)
		if smap.Version != smapPrimary.Version {
			err := fmt.Errorf("proxy %s has version %d, but primary proxy has version %d of Pmap",
				proxyID, smap.Version, smapPrimary.Version)
			t.Error(err)
		}
	}
}

// primarySetToOriginal reads original primary proxy from configuration and
// makes it a primary proxy again
// NOTE: This test cannot be run as separate test. It requires that original
// primary proxy was down and retuned back. So, the test should be executed
// after primaryCrashElectRestart test
func primarySetToOriginal(t *testing.T) {
	proxyURL := tutils.GetPrimaryURL()
	smap := tutils.GetClusterMap(t, proxyURL)
	var (
		currID, currURL       string
		byURL, byPort, origID string
	)
	currID = smap.Primary.ID()
	currURL = smap.Primary.PublicNet.DirectURL
	if currURL != proxyURL {
		t.Fatalf("Err in the test itself: expecting currURL %s == proxyurl %s", currURL, proxyURL)
	}
	tutils.Logf("Setting primary proxy %s back to the original, Smap version %d\n", currID, smap.Version)

	config := tutils.GetClusterConfig(t)
	proxyconf := config.Proxy
	origURL := proxyconf.OriginalURL

	if origURL == "" {
		t.Fatal("Original primary proxy is not defined in configuration")
	}
	urlparts := strings.Split(origURL, ":")
	proxyPort := urlparts[len(urlparts)-1]

	for key, val := range smap.Pmap {
		if val.PublicNet.DirectURL == origURL {
			byURL = key
			break
		}

		keyparts := strings.Split(val.PublicNet.DirectURL, ":")
		port := keyparts[len(keyparts)-1]
		if port == proxyPort {
			byPort = key
		}
	}
	if byPort == "" && byURL == "" {
		t.Fatalf("No original primary proxy: %v", proxyconf)
	}
	origID = byURL
	if origID == "" {
		origID = byPort
	}
	tutils.Logf("Found original primary ID: %s\n", origID)
	if currID == origID {
		tutils.Logf("Original %s == the current primary: nothing to do\n", origID)
		return
	}

	setPrimaryTo(t, proxyURL, smap, "", origID)
}

// This is duplicated in the tests because the `idDigest` of `daemonInfo` is not
// exported. As a result of this, ais.HrwProxy will not return the correct
// proxy since the `idDigest` will be initialized to 0. To avoid this, we
// compute the checksum directly in this method.
func hrwProxyTest(smap *cluster.Smap, idToSkip string) (pi string, err error) {
	if smap.CountProxies() == 0 {
		err = errors.New("AIStore cluster map is empty: no proxies")
		return
	}
	var (
		max     uint64
		skipped int
	)
	for id, snode := range smap.Pmap {
		if id == idToSkip {
			skipped++
			continue
		}
		if _, ok := smap.NonElects[id]; ok {
			skipped++
			continue
		}
		cs := xxhash.ChecksumString64S(snode.ID(), cmn.MLCG32)
		if cs > max {
			max = cs
			pi = id
		}
	}
	if pi == "" {
		err = fmt.Errorf("cannot HRW-select proxy: current count=%d, skipped=%d", smap.CountProxies(), skipped)
	}
	return
}

func networkFailureTarget(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)
	if smap.CountTargets() == 0 {
		t.Fatal("At least 1 target required")
	}
	proxyCount, targetCount := smap.CountProxies(), smap.CountTargets()

	targetID := ""
	for id := range smap.Tmap {
		targetID = id
		break
	}

	tutils.Logf("Disconnecting target: %s\n", targetID)
	oldNetworks, err := containers.DisconnectContainer(targetID)
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForPrimaryProxy(
		proxyURL,
		"target is down",
		smap.Version, testing.Verbose(),
		proxyCount,
		targetCount-1,
	)
	tassert.CheckFatal(t, err)

	tutils.Logf("Connecting target %s to networks again\n", targetID)
	err = containers.ConnectContainer(targetID, oldNetworks)
	tassert.CheckFatal(t, err)

	_, err = tutils.WaitForPrimaryProxy(
		proxyURL,
		"to check cluster state",
		smap.Version, testing.Verbose(),
		proxyCount,
		targetCount,
	)
	tassert.CheckFatal(t, err)
}

func networkFailureProxy(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)
	if smap.CountProxies() < 2 {
		t.Fatal("At least 2 proxy required")
	}
	proxyCount, targetCount := smap.CountProxies(), smap.CountTargets()

	oldPrimaryID := smap.Primary.ID()
	proxyID, _, err := chooseNextProxy(smap)
	tassert.CheckFatal(t, err)

	tutils.Logf("Disconnecting proxy: %s\n", proxyID)
	oldNetworks, err := containers.DisconnectContainer(proxyID)
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForPrimaryProxy(
		proxyURL,
		"proxy is down",
		smap.Version, testing.Verbose(),
		proxyCount-1,
		targetCount,
	)
	tassert.CheckFatal(t, err)

	tutils.Logf("Connecting proxy %s to networks again\n", proxyID)
	err = containers.ConnectContainer(proxyID, oldNetworks)
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForPrimaryProxy(
		proxyURL,
		"to check cluster state",
		smap.Version, testing.Verbose(),
		proxyCount,
		targetCount,
	)
	tassert.CheckFatal(t, err)

	if oldPrimaryID != smap.Primary.ID() {
		t.Fatalf("Primary proxy changed from %s to %s",
			oldPrimaryID, smap.Primary.ID())
	}
}

func networkFailurePrimary(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)
	if smap.CountProxies() < 2 {
		t.Fatal("At least 2 proxy required")
	}

	proxyCount, targetCount := smap.CountProxies(), smap.CountTargets()
	oldPrimaryID, oldPrimaryURL := smap.Primary.ID(), smap.Primary.PublicNet.DirectURL
	newPrimaryID, newPrimaryURL, err := chooseNextProxy(smap)
	tassert.CheckFatal(t, err)

	// Disconnect primary
	tutils.Logf("Disconnecting primary %s from all networks\n", oldPrimaryID)
	oldNetworks, err := containers.DisconnectContainer(oldPrimaryID)
	tassert.CheckFatal(t, err)

	// Check smap
	smap, err = tutils.WaitForPrimaryProxy(
		newPrimaryURL,
		"original primary is gone",
		smap.Version, testing.Verbose(),
		proxyCount-1,
		targetCount,
	)
	tassert.CheckFatal(t, err)

	if smap.Primary.ID() != newPrimaryID {
		t.Fatalf("wrong primary proxy: %s, expecting: %s after disconnecting",
			smap.Primary.ID(), newPrimaryID)
	}

	// Connect again
	tutils.Logf("Connecting primary %s to networks again\n", oldPrimaryID)
	err = containers.ConnectContainer(oldPrimaryID, oldNetworks)
	tassert.CheckFatal(t, err)

	// give a little time to original primary, so it picks up the network
	// connections and starts talking to neighbors
	_, err = tutils.WaitForPrimaryProxy(
		oldPrimaryID,
		"original primary is restored",
		smap.Version, testing.Verbose(),
		proxyCount,
		targetCount,
	)
	tassert.CheckFatal(t, err)

	oldSmap := tutils.GetClusterMap(t, oldPrimaryURL)
	// the original primary still thinks that it is the primary, so its smap
	// should not change after the network is back
	if oldSmap.Primary.ID() != oldPrimaryID {
		tutils.Logf("Old primary changed its smap. Its current primary: %s (expected %s - self)\n",
			oldSmap.Primary.ID(), oldPrimaryID)
	}

	// Forcefully set new primary for the original one
	baseParams := tutils.BaseAPIParams(oldPrimaryURL)
	baseParams.Method = http.MethodPut
	err = api.DoHTTPRequest(api.ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Proxy, newPrimaryID),
		Query: url.Values{
			cmn.URLParamForce:            {"true"},
			cmn.URLParamPrimaryCandidate: {newPrimaryURL},
		},
	})
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForPrimaryProxy(
		newPrimaryURL,
		"original primary joined the new primary",
		smap.Version, testing.Verbose(),
		proxyCount,
		targetCount,
	)
	tassert.CheckFatal(t, err)

	if smap.Primary.ID() != newPrimaryID {
		t.Fatalf("expected primary=%s, got %s after connecting again", newPrimaryID, smap.Primary.ID())
	}
}

func networkFailure(t *testing.T) {
	if !containers.DockerRunning() {
		t.Skip("Network failure test requires Docker cluster")
	}

	t.Run("Target network disconnect", networkFailureTarget)
	t.Run("Secondary proxy network disconnect", networkFailureProxy)
	t.Run("Primary proxy network disconnect", networkFailurePrimary)
}

// primaryAndNextCrash kills the primary proxy and a proxy that should be selected
// after the current primary dies, verifies the second in line proxy becomes
// the new primary, restore all proxies
func primaryAndNextCrash(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)
	origProxyCount := smap.CountProxies()

	if origProxyCount < 4 {
		t.Skip("The test requires at least 4 proxies, found only ", origProxyCount)
	}

	// get next primary
	firstPrimaryID, firstPrimaryURL, err := chooseNextProxy(smap)
	tassert.CheckFatal(t, err)
	// Cluster map is re-read to have a clone of original smap that the test
	// can modify in any way it needs. Because original smap got must be preserved
	smapNext := tutils.GetClusterMap(t, proxyURL)
	// get next next primary
	firstPrimary := smapNext.Pmap[firstPrimaryID]
	delete(smapNext.Pmap, firstPrimaryID)
	finalPrimaryID, finalPrimaryURL, err := chooseNextProxy(smapNext)
	tassert.CheckFatal(t, err)

	// kill the current primary
	oldPrimaryURL, oldPrimaryID := smap.Primary.PublicNet.DirectURL, smap.Primary.ID()
	tutils.Logf("Killing primary proxy: %s - %s\n", oldPrimaryURL, oldPrimaryID)
	cmdFirst, err := kill(smap.Primary)
	tassert.CheckFatal(t, err)

	// kill the next primary
	tutils.Logf("Killing next to primary proxy: %s - %s\n", firstPrimaryID, firstPrimaryURL)
	cmdSecond, errSecond := kill(firstPrimary)
	// if kill fails it does not make sense to wait for the cluster is stable
	if errSecond == nil {
		// the cluster should vote, so the smap version should be increased at
		// least by 100, that is why +99
		smap, err = tutils.WaitForPrimaryProxy(finalPrimaryURL, "to designate new primary",
			smap.Version+99, testing.Verbose(), origProxyCount-2)
		tassert.CheckFatal(t, err)
	}

	tutils.Logln("Checking current primary")
	if smap.Primary.ID() != finalPrimaryID {
		t.Errorf("Expected primary %s but real primary is %s",
			finalPrimaryID, smap.Primary.ID())
	}

	// restore next and prev primaries in the reversed order
	err = restore(cmdSecond, true, "proxy (next primary)")
	tassert.CheckFatal(t, err)
	smap, err = tutils.WaitForPrimaryProxy(finalPrimaryURL, "to restore next primary",
		smap.Version, testing.Verbose(), origProxyCount-1)
	tassert.CheckFatal(t, err)
	err = restore(cmdFirst, true, "proxy (prev primary)")
	tassert.CheckFatal(t, err)
	_, err = tutils.WaitForPrimaryProxy(finalPrimaryURL, "to restore prev primary",
		smap.Version, testing.Verbose(), origProxyCount)
	tassert.CheckFatal(t, err)
}

// TODO: make it long test after IC development completes
func TestIC(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)
	if smap.CountProxies() < 4 {
		t.Fatal("Not enough proxies to run proxy tests, must be more than 3")
	}

	for _, test := range icTests {
		t.Run(test.name, test.method)
		if t.Failed() {
			t.FailNow()
		}
	}
	clusterHealthCheck(t, smap)
}

func killRandNonPrimaryIC(t *testing.T, smap *cluster.Smap) (restoreCmd, *cluster.Smap) {
	origProxyCount := smap.CountProxies()
	primary := smap.Primary
	var killNode *cluster.Snode
	for sid := range smap.IC {
		if prx := smap.GetProxy(sid); !prx.Equals(primary) {
			killNode = prx
			break
		}
	}
	cmd, err := kill(killNode)
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForPrimaryProxy(primary.PublicNet.DirectURL, "to propagate new Smap",
		smap.Version, testing.Verbose(), origProxyCount-1)
	tassert.CheckError(t, err)
	return cmd, smap
}

func icMemberLeaveAndRejoin(t *testing.T) {
	smap := tutils.GetClusterMap(t, proxyURL)
	primary := smap.Primary

	tassert.Fatalf(t, len(smap.IC) == ais.ICGroupSize, "should have %d members in IC, has %d", ais.ICGroupSize, len(smap.IC))

	// Primary must be an IC member
	tassert.Fatalf(t, smap.IsIC(primary), "primary (%s) should be a IC member, (were: %s)", primary, smap.StrIC(primary))

	// killing an IC member, should add a new IC member
	// select IC member which is not primary and kill
	origIC := smap.IC
	cmd, smap := killRandNonPrimaryIC(t, smap)
	delete(origIC, cmd.node.DaemonID)

	tassert.Errorf(t, !smap.IsIC(cmd.node), "Killed daemon (%s) must be removed from IC", cmd.node.DaemonID)

	// should have remaining IC nodes
	for sid := range origIC {
		tassert.Errorf(t, smap.IsIC(smap.GetProxy(sid)), "Should not remove existing IC members (%s)", sid)
	}
	tassert.Errorf(t, len(smap.IC) == ais.ICGroupSize, "should have %d members in IC, has %d", ais.ICGroupSize, len(smap.IC))

	err := restore(cmd, false, "proxy")
	tassert.CheckFatal(t, err)

	updatedICs := smap.IC
	smap, err = api.WaitNodeAdded(tutils.BaseAPIParams(primary.PublicNet.DirectURL), cmd.node.ID())
	tassert.CheckFatal(t, err)

	// Adding a new node shouldn't change IC members
	tassert.Errorf(t, reflect.DeepEqual(updatedICs, smap.IC), "shouldn't update existing IC members")
}

func icKillAndRestorePrimary(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)
	newPrimaryID, newPrimaryURL, err := chooseNextProxy(smap)
	newPrimary := smap.GetProxy(newPrimaryID)
	tassert.CheckFatal(t, err)

	oldIC := smap.IC
	oldPrimary := smap.Primary
	oldPrimaryURL := smap.Primary.PublicNet.DirectURL
	oldPrimaryID := smap.Primary.ID()

	cmd, err := kill(smap.Primary)
	tassert.CheckFatal(t, err)

	smap, err = tutils.WaitForPrimaryProxy(newPrimaryURL, "to designate new primary",
		smap.Version, testing.Verbose())
	tassert.CheckFatal(t, err)

	// OLD primary shouldn't be in IC
	tassert.Errorf(t, !smap.IsIC(oldPrimary), "killed primary (%s) must be removed from IC", oldPrimaryID)

	// New primary should be part of IC
	tassert.Errorf(t, smap.IsIC(newPrimary), "new primary (%s) must be part of IC", newPrimaryID)

	// remaining IC member should be unchanged
	for sid := range oldIC {
		if sid != oldPrimaryID {
			tassert.Errorf(t, smap.IsIC(smap.GetProxy(sid)), "should not remove existing IC members (%s)", sid)
		}
	}

	changedIC := smap.IC
	_, maxVal := cmn.MaxValEntryInt(changedIC)
	tassert.CheckError(t, err)

	// Restore the killed primary back as primary
	err = restore(cmd, true, "proxy")
	tassert.CheckFatal(t, err)

	smap, err = api.WaitNodeAdded(tutils.BaseAPIParams(newPrimaryURL), oldPrimaryID)
	tassert.CheckFatal(t, err)

	smap = setPrimaryTo(t, oldPrimaryURL, smap, "", oldPrimaryID)

	// when a node added as primary, it should add itself to IC
	tassert.Fatalf(t, smap.IsIC(oldPrimary), "primary (%s) should be a IC member, (were: %s)", oldPrimaryID, smap.StrIC(newPrimary))
	tassert.Errorf(t, len(smap.IC) == ais.ICGroupSize, "should have %d members in IC, has %d", ais.ICGroupSize, len(smap.IC))

	// proxy with max version should be evicted
	for si, v := range changedIC {
		if _, ok := smap.IC[si]; !ok {
			tassert.Fatalf(t, v == maxVal,
				"should remove IC member with maxVersion v:(%d) - deleted v:(%d) instead", maxVal, v)
		}
	}
}

func icSyncOwnershipTable(t *testing.T) {
	var (
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)
		smap       = tutils.GetClusterMap(t, proxyURL)
		primary    = smap.Primary

		src = cmn.Bck{
			Name:     TestBucketName,
			Provider: cmn.ProviderAIS,
		}

		dstBck = cmn.Bck{
			Name:     TestBucketName + "_new",
			Provider: cmn.ProviderAIS,
		}
	)

	tutils.CreateFreshBucket(t, proxyURL, src)
	defer tutils.DestroyBucket(t, proxyURL, src)

	// Start any xaction and get ID
	xactID, err := api.CopyBucket(baseParams, src, dstBck)
	tassert.CheckFatal(t, err)
	defer tutils.DestroyBucket(t, proxyURL, dstBck)

	// killing an IC member, should add a new IC member
	// select IC member which is not primary and kill
	origIC := smap.IC
	cmd, smap := killRandNonPrimaryIC(t, smap)

	// try getting xaction status from new IC member
	var newICMemID string
	for sid := range smap.IC {
		if _, ok := origIC[sid]; !ok {
			tassert.Errorf(t, newICMemID == "", "should change only one IC member")
			newICMemID = sid
		}
	}

	newICNode := smap.GetProxy(newICMemID)

	baseParams = tutils.BaseAPIParams(newICNode.URL(cmn.NetworkPublic))
	xactArgs := api.XactReqArgs{ID: xactID, Kind: cmn.ActCopyBucket}
	_, err = api.GetXactionStatus(baseParams, xactArgs)
	tassert.CheckError(t, err)

	err = restore(cmd, false, "proxy")
	tassert.CheckFatal(t, err)

	smap, err = api.WaitNodeAdded(baseParams, cmd.node.ID())
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, !smap.IsIC(cmd.node), "newly joined node shouldn't be in IC (%s)", cmd.node)

	// should sync ownership table when non-ic member become primary
	smap = setPrimaryTo(t, primary.PublicNet.DirectURL, smap, "", cmd.node.ID())
	tassert.Fatalf(t, smap.IsIC(cmd.node), "primary (%s) should be a IC member, (were: %s)", primary, smap.StrIC(primary))

	baseParams = tutils.BaseAPIParams(cmd.node.URL(cmn.NetworkPublic))
	_, err = api.GetXactionStatus(baseParams, xactArgs)
	tassert.CheckError(t, err)
}

func icSinglePrimaryRevamp(t *testing.T) {
	var (
		proxyURL       = tutils.RandomProxyURL(t)
		smap           = tutils.GetClusterMap(t, proxyURL)
		origProxyCount = smap.CountProxies()

		src = cmn.Bck{
			Name:     TestBucketName,
			Provider: cmn.ProviderAIS,
		}

		dstBck = cmn.Bck{
			Name:     TestBucketName + "_new",
			Provider: cmn.ProviderAIS,
		}
	)

	nodesToRestore := make([]restoreCmd, 0, origProxyCount-1)

	// kill all nodes except primary
	for i := origProxyCount; i > 1; i-- {
		var cmd restoreCmd
		cmd, smap = killRandNonPrimaryIC(t, smap)
		nodesToRestore = append(nodesToRestore, cmd)
	}

	proxyURL = smap.Primary.URL(cmn.NetworkPublic)
	baseParams = tutils.BaseAPIParams(proxyURL)
	tutils.CreateFreshBucket(t, proxyURL, src)
	defer tutils.DestroyBucket(t, proxyURL, src)

	// Start any xaction and get ID
	xactID, err := api.CopyBucket(baseParams, src, dstBck)
	xactArgs := api.XactReqArgs{ID: xactID, Kind: cmn.ActCopyBucket}

	tassert.CheckFatal(t, err)
	defer tutils.DestroyBucket(t, proxyURL, dstBck)

	// Restart all killed nodes and check for xaction status
	for _, cmd := range nodesToRestore {
		err = restore(cmd, false, "proxy")
		tassert.CheckError(t, err)

		_, err = api.WaitNodeAdded(baseParams, cmd.node.ID())
		tassert.CheckError(t, err)

		baseParams = tutils.BaseAPIParams(cmd.node.URL(cmn.NetworkPublic))
		_, err = api.GetXactionStatus(baseParams, xactArgs)
		tassert.CheckError(t, err)
	}
}
