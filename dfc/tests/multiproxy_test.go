/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

import (
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/tutils"
	"github.com/OneOfOne/xxhash"
	jsoniter "github.com/json-iterator/go"
)

const (
	mockDaemonID    = "MOCK"
	localBucketDir  = "multipleproxy"
	localBucketName = "multipleproxytmp"
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
	}
)

func TestMultiProxy(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	proxyURL := getPrimaryURL(t, proxyURLRO)
	smap := getClusterMap(t, proxyURL)
	if len(smap.Pmap) < 3 {
		t.Fatal("Not enough proxies to run proxy tests, must be more than 2")
	}

	if len(smap.Tmap) < 1 {
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
func clusterHealthCheck(t *testing.T, smapBefore cluster.Smap) {
	proxyURL := getPrimaryURL(t, proxyURLRO)
	smapAfter := getClusterMap(t, proxyURL)
	if len(smapAfter.Tmap) != len(smapBefore.Tmap) {
		t.Fatalf("Number of targets mismatch, before = %d, after = %d",
			len(smapBefore.Tmap), len(smapAfter.Tmap))
	}

	if len(smapAfter.Pmap) != len(smapBefore.Pmap) {
		t.Fatalf("Number of proxies mismatch, before = %d, after = %d",
			len(smapBefore.Pmap), len(smapAfter.Pmap))
	}

	for _, b := range smapBefore.Tmap {
		a, ok := smapAfter.Tmap[b.DaemonID]
		if !ok {
			t.Fatalf("Failed to find target %s", b.DaemonID)
		}

		if !a.Equals(b) {
			t.Fatalf("Target %s changed, before = %+v, after = %+v", b.DaemonID, b, a)
		}
	}

	for _, b := range smapBefore.Pmap {
		a, ok := smapAfter.Pmap[b.DaemonID]
		if !ok {
			t.Fatalf("Failed to find proxy %s", b.DaemonID)
		}

		// note: can't compare Primary field unless primary is always reset to the original one
		if !a.Equals(b) {
			t.Fatalf("Proxy %s changed, before = %+v, after = %+v", b.DaemonID, b, a)
		}
	}

	if tutils.DockerRunning() {
		pCnt, tCnt := tutils.ContainerCount()
		if pCnt != len(smapAfter.Pmap) {
			t.Fatalf("Some proxy conatiners crashed: expected %d, found %d containers", len(smapAfter.Pmap), pCnt)
		}
		if tCnt != len(smapAfter.Tmap) {
			t.Fatalf("Some target conatiners crashed: expected %d, found %d containers", len(smapAfter.Tmap), tCnt)
		}
		return
	}

	// no proxy/target died (or not restored)
	for _, b := range smapBefore.Tmap {
		_, err := getPID(b.PublicNet.DaemonPort)
		tutils.CheckFatal(err, t)
	}

	for _, b := range smapBefore.Pmap {
		_, err := getPID(b.PublicNet.DaemonPort)
		tutils.CheckFatal(err, t)
	}
}

// primaryCrashElectRestart kills the current primary proxy, wait for the new primary prioxy is up and verifies it,
// restores the original primary proxy as non primary
func primaryCrashElectRestart(t *testing.T) {
	proxyURL := getPrimaryURL(t, proxyURLRO)
	smap := getClusterMap(t, proxyURL)
	newPrimaryID, newPrimaryURL, err := chooseNextProxy(&smap)
	tutils.CheckFatal(err, t)
	checkPmapVersions(t, proxyURL)

	oldPrimaryURL := smap.ProxySI.PublicNet.DirectURL
	oldPrimaryID := smap.ProxySI.DaemonID
	tutils.Logf("New primary: %s --> %s\nKilling primary: %s --> %s\n",
		newPrimaryID, newPrimaryURL, oldPrimaryURL, smap.ProxySI.PublicNet.DaemonPort)
	cmd, args, err := kill(smap.ProxySI.DaemonID, smap.ProxySI.PublicNet.DaemonPort)
	// cmd and args are the original command line of how the proxy is started
	// example: cmd = /Users/lid/go/bin/dfc, args = -config=/Users/lid/.dfc/dfc0.json -role=proxy -ntargets=3
	tutils.CheckFatal(err, t)

	smap, err = waitForPrimaryProxy(newPrimaryURL, "to designate new primary", smap.Version, testing.Verbose())
	tutils.CheckFatal(err, t)
	tutils.Logf("New primary elected: %s\n", newPrimaryID)

	if smap.ProxySI.DaemonID != newPrimaryID {
		t.Fatalf("Wrong primary proxy: %s, expecting: %s", smap.ProxySI.DaemonID, newPrimaryID)
	}

	// re-construct the command line to start the original proxy but add the current primary proxy to the args
	// example: cmd = /Users/lid/go/bin/dfc
	//          args = -config=/Users/lid/.dfc/dfc0.json -role=proxy -ntargets=3 -proxyurl=http://10.112.76.36:8082
	err = restore(cmd, args, false, "proxy (prev primary)")
	tutils.CheckFatal(err, t)

	smap, err = waitForPrimaryProxy(newPrimaryURL, "to restore", smap.Version, testing.Verbose())
	tutils.CheckFatal(err, t)

	if _, ok := smap.Pmap[oldPrimaryID]; !ok {
		t.Fatalf("Previous primary proxy did not rejoin the cluster")
	}
	checkPmapVersions(t, newPrimaryURL)
}

// primaryAndTargetCrash kills the primary p[roxy and one random target, verifies the next in
// line proxy becomes the new primary, restore the target and proxy, restore original primary.
func primaryAndTargetCrash(t *testing.T) {
	if tutils.DockerRunning() {
		t.Skip("Skipped because setting new primary URL in command line for docker is not supported")
	}

	proxyURL := getPrimaryURL(t, proxyURLRO)
	smap := getClusterMap(t, proxyURL)
	newPrimaryID, newPrimaryURL, err := chooseNextProxy(&smap)
	tutils.CheckFatal(err, t)

	oldPrimaryURL := smap.ProxySI.PublicNet.DirectURL
	tutils.Logf("Killing proxy %s - %s\n", oldPrimaryURL, smap.ProxySI.DaemonID)
	cmd, args, err := kill(smap.ProxySI.DaemonID, smap.ProxySI.PublicNet.DaemonPort)
	tutils.CheckFatal(err, t)

	// Select a random target
	var (
		targetURL       string
		targetPort      string
		targetID        string
		origTargetCount = len(smap.Tmap)
		origProxyCount  = len(smap.Pmap)
	)

	for _, v := range smap.Tmap {
		targetURL = v.PublicNet.DirectURL
		targetPort = v.PublicNet.DaemonPort
		targetID = v.DaemonID
		break
	}

	tutils.Logf("Killing target: %s - %s\n", targetURL, targetID)
	tcmd, targs, err := kill(targetID, targetPort)
	tutils.CheckFatal(err, t)

	smap, err = waitForPrimaryProxy(newPrimaryURL, "to designate new primary", smap.Version, testing.Verbose(), origProxyCount-1, origTargetCount-1)
	tutils.CheckFatal(err, t)

	if smap.ProxySI.DaemonID != newPrimaryID {
		t.Fatalf("Wrong primary proxy: %s, expecting: %s", smap.ProxySI.DaemonID, newPrimaryID)
	}

	err = restore(tcmd, targs, false, "target")
	tutils.CheckFatal(err, t)

	err = restore(cmd, args, false, "proxy (prev primary)")
	tutils.CheckFatal(err, t)

	_, err = waitForPrimaryProxy(newPrimaryURL, "to restore", smap.Version, testing.Verbose(), origProxyCount, origTargetCount)
	tutils.CheckFatal(err, t)
}

// A very simple test to check if a primary proxy can detect non-primary one
// dies and then update and sync SMap
func proxyCrash(t *testing.T) {
	proxyURL := getPrimaryURL(t, proxyURLRO)
	smap := getClusterMap(t, proxyURL)

	oldPrimaryURL, oldPrimaryID := smap.ProxySI.PublicNet.DirectURL, smap.ProxySI.DaemonID
	tutils.Logf("Primary proxy: %s\n", oldPrimaryURL)

	var (
		secondURL      string
		secondPort     string
		secondID       string
		origProxyCount = len(smap.Pmap)
	)

	// Select a random non-primary proxy
	for k, v := range smap.Pmap {
		if k != oldPrimaryID {
			secondURL = v.PublicNet.DirectURL
			secondPort = v.PublicNet.DaemonPort
			secondID = v.DaemonID
			break
		}
	}

	tutils.Logf("Killing non-primary proxy: %s - %s\n", secondURL, secondID)
	secondCmd, secondArgs, err := kill(secondID, secondPort)
	tutils.CheckFatal(err, t)

	smap, err = waitForPrimaryProxy(proxyURL, "to propagate new Smap", smap.Version, testing.Verbose(), origProxyCount-1)
	tutils.CheckFatal(err, t)

	err = restore(secondCmd, secondArgs, false, "proxy")
	tutils.CheckFatal(err, t)

	smap, err = waitForPrimaryProxy(proxyURL, "to restore", smap.Version, testing.Verbose(), origProxyCount)
	tutils.CheckFatal(err, t)

	if _, ok := smap.Pmap[secondID]; !ok {
		t.Fatalf("Non-primary proxy did not rejoin the cluster.")
	}
}

// primaryAndProxyCrash kills primary proxy and one another proxy(not the next in line primary)
// and restore them afterwards
func primaryAndProxyCrash(t *testing.T) {
	proxyURL := getPrimaryURL(t, proxyURLRO)
	smap := getClusterMap(t, proxyURL)
	newPrimaryID, newPrimaryURL, err := chooseNextProxy(&smap)
	tutils.CheckFatal(err, t)

	oldPrimaryURL, oldPrimaryID := smap.ProxySI.PublicNet.DirectURL, smap.ProxySI.DaemonID
	tutils.Logf("Killing primary proxy: %s - %s\n", oldPrimaryURL, oldPrimaryID)
	cmd, args, err := kill(smap.ProxySI.DaemonID, smap.ProxySI.PublicNet.DaemonPort)
	tutils.CheckFatal(err, t)

	var (
		secondURL      string
		secondPort     string
		secondID       string
		origProxyCount = len(smap.Pmap)
	)

	// Select a third random proxy
	// Do not choose the next primary in line, or the current primary proxy
	// This is because the system currently cannot recover if the next proxy in line is
	// also killed.
	for k, v := range smap.Pmap {
		if k != newPrimaryID && k != oldPrimaryID {
			secondURL = v.PublicNet.DirectURL
			secondPort = v.PublicNet.DaemonPort
			secondID = v.DaemonID
			break
		}
	}

	tutils.Logf("Killing non-primary proxy: %s - %s\n", secondURL, secondID)
	secondCmd, secondArgs, err := kill(secondID, secondPort)
	tutils.CheckFatal(err, t)

	smap, err = waitForPrimaryProxy(newPrimaryURL, "to designate new primary", smap.Version, testing.Verbose(), origProxyCount-2)
	tutils.CheckFatal(err, t)

	err = restore(cmd, args, true, "proxy (prev primary)")
	tutils.CheckFatal(err, t)

	smap, err = waitForPrimaryProxy(newPrimaryURL, "to designate new primary", smap.Version, testing.Verbose(), origProxyCount-1)
	tutils.CheckFatal(err, t)
	err = restore(secondCmd, secondArgs, false, "proxy")
	tutils.CheckFatal(err, t)

	smap, err = waitForPrimaryProxy(newPrimaryURL, "to restore", smap.Version, testing.Verbose(), origProxyCount)
	tutils.CheckFatal(err, t)

	if smap.ProxySI.DaemonID != newPrimaryID {
		t.Fatalf("Wrong primary proxy: %s, expecting: %s", smap.ProxySI.DaemonID, newPrimaryID)
	}

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
		id   string
		port string
	)

	proxyURL := getPrimaryURL(t, proxyURLRO)
	smap := getClusterMap(t, proxyURL)
	for _, v := range smap.Tmap {
		id = v.DaemonID
		port = v.PublicNet.DaemonPort
		break
	}

	cmd, args, err := kill(id, port)
	tutils.CheckFatal(err, t)
	smap, err = waitForPrimaryProxy(proxyURL, "to synchronize on 'target crashed'", smap.Version, testing.Verbose())
	tutils.CheckFatal(err, t)

	if _, ok := smap.Tmap[id]; ok {
		t.Fatalf("Killed target was not removed from the Smap: %v", id)
	}

	err = restore(cmd, args, false, "target")
	tutils.CheckFatal(err, t)

	smap, err = waitForPrimaryProxy(proxyURL, "to synchronize on 'target rejoined'", smap.Version, testing.Verbose())
	tutils.CheckFatal(err, t)

	if _, ok := smap.Tmap[id]; !ok {
		t.Fatalf("Restarted target %s did not rejoin the cluster", id)
	}
}

// crashAndFastRestore kills the primary and restores it before a new leader is elected
func crashAndFastRestore(t *testing.T) {
	proxyURL := getPrimaryURL(t, proxyURLRO)
	smap := getClusterMap(t, proxyURL)

	id := smap.ProxySI.DaemonID
	tutils.Logf("The current primary %s, Smap version %d\n", id, smap.Version)

	cmd, args, err := kill(smap.ProxySI.DaemonID, smap.ProxySI.PublicNet.DaemonPort)
	tutils.CheckFatal(err, t)

	// quick crash and recover
	time.Sleep(2 * time.Second)
	err = restore(cmd, args, true, "proxy (primary)")
	tutils.CheckFatal(err, t)

	tutils.Logf("The %s is currently restarting\n", id)

	// Note: using (version - 1) because the primary will restart with its old version,
	//       there will be no version change for this restore, so force beginning version to 1 less
	//       than the original version in order to use waitForPrimaryProxy
	smap, err = waitForPrimaryProxy(proxyURL, "to restore", smap.Version-1, testing.Verbose())
	tutils.CheckFatal(err, t)

	if smap.ProxySI.DaemonID != id {
		t.Fatalf("Wrong primary proxy: %s, expecting: %s", smap.ProxySI.DaemonID, id)
	}
}

func joinWhileVoteInProgress(t *testing.T) {
	if tutils.DockerRunning() {
		t.Skip("Skipping because mocking is not supported for docker cluster")
	}

	proxyURL := getPrimaryURL(t, proxyURLRO)
	smap := getClusterMap(t, proxyURL)
	newPrimaryID, newPrimaryURL, err := chooseNextProxy(&smap)
	oldTargetCnt := len(smap.Tmap)
	oldProxyCnt := len(smap.Pmap)
	tutils.CheckFatal(err, t)

	stopch := make(chan struct{})
	errCh := make(chan error, 10)
	mocktgt := &voteRetryMockTarget{
		voteInProgress: true,
		errCh:          errCh,
	}

	go runMockTarget(t, proxyURL, mocktgt, stopch, &smap)

	smap, err = waitForPrimaryProxy(proxyURL, "to synchronize on 'new mock target'", smap.Version, testing.Verbose(), 0, oldTargetCnt+1)
	tutils.CheckFatal(err, t)

	oldPrimaryID := smap.ProxySI.DaemonID
	cmd, args, err := kill(smap.ProxySI.DaemonID, smap.ProxySI.PublicNet.DaemonPort)
	tutils.CheckFatal(err, t)

	smap, err = waitForPrimaryProxy(newPrimaryURL, "to designate new primary", smap.Version, testing.Verbose(), oldProxyCnt-1, oldTargetCnt+1)
	tutils.CheckFatal(err, t)

	err = restore(cmd, args, true, "proxy (prev primary)")
	tutils.CheckFatal(err, t)

	// check if the previous primary proxy has not yet rejoined the cluster
	// it should be waiting for the mock target to return voteInProgress=false
	time.Sleep(5 * time.Second)
	smap = getClusterMap(t, newPrimaryURL)
	if smap.ProxySI.DaemonID != newPrimaryID {
		t.Fatalf("Wrong primary proxy: %s, expecting: %s", smap.ProxySI.DaemonID, newPrimaryID)
	}
	if _, ok := smap.Pmap[oldPrimaryID]; ok {
		t.Fatalf("Previous primary proxy rejoined the cluster during a vote")
	}

	mocktgt.voteInProgress = false

	smap, err = waitForPrimaryProxy(newPrimaryURL, "to synchronize new Smap", smap.Version, testing.Verbose(), oldProxyCnt, oldTargetCnt+1)
	tutils.CheckFatal(err, t)

	if smap.ProxySI.DaemonID != newPrimaryID {
		t.Fatalf("Wrong primary proxy: %s, expectinge: %s", smap.ProxySI.DaemonID, newPrimaryID)
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

	_, err = waitForPrimaryProxy(newPrimaryURL, "to kill mock target", smap.Version, testing.Verbose())
	tutils.CheckFatal(err, t)
}

func minorityTargetMapVersionMismatch(t *testing.T) {
	proxyURL := getPrimaryURL(t, proxyURLRO)
	targetMapVersionMismatch(
		func(i int) int {
			return i/4 + 1
		}, t, proxyURL)
}

func majorityTargetMapVersionMismatch(t *testing.T) {
	proxyURL := getPrimaryURL(t, proxyURLRO)
	targetMapVersionMismatch(
		func(i int) int {
			return i/2 + 1
		}, t, proxyURL)
}

// targetMapVersionMismatch updates map verison of a few targets, kill the primary proxy
// wait for the new leader to come online
func targetMapVersionMismatch(getNum func(int) int, t *testing.T, proxyURL string) {
	smap := getClusterMap(t, proxyURL)
	oldVer := smap.Version
	oldProxyCnt := len(smap.Pmap)

	smap.Version++
	jsonMap, err := jsoniter.Marshal(smap)
	tutils.CheckFatal(err, t)

	n := getNum(len(smap.Tmap) + len(smap.Pmap) - 1)
	for _, v := range smap.Tmap {
		if n == 0 {
			break
		}

		baseParams := tutils.BaseAPIParams(v.URL(cmn.NetworkPublic))
		baseParams.Method = http.MethodPut
		path := cmn.URLPath(cmn.Version, cmn.Daemon, cmn.SyncSmap)
		_, err = api.DoHTTPRequest(baseParams, path, jsonMap)
		tutils.CheckFatal(err, t)
		n--
	}

	nextProxyID, nextProxyURL, err := chooseNextProxy(&smap)
	tutils.CheckFatal(err, t)

	cmd, args, err := kill(smap.ProxySI.DaemonID, smap.ProxySI.PublicNet.DaemonPort)
	tutils.CheckFatal(err, t)

	smap, err = waitForPrimaryProxy(nextProxyURL, "to designate new primary", oldVer, testing.Verbose(), oldProxyCnt-1)
	tutils.CheckFatal(err, t)

	if smap.ProxySI == nil {
		t.Fatalf("Nil ProxySI in retrieved Smap")
	}

	if smap.ProxySI.DaemonID != nextProxyID {
		t.Fatalf("Wrong primary proxy: %s, expecting: %s", smap.ProxySI.DaemonID, nextProxyID)
	}

	err = restore(cmd, args, false, "proxy (prev primary)")
	tutils.CheckFatal(err, t)

	_, err = waitForPrimaryProxy(nextProxyURL, "to restore", smap.Version, testing.Verbose())
	tutils.CheckFatal(err, t)
}

// concurrentPutGetDel does put/get/del sequence against all proxies concurrently
func concurrentPutGetDel(t *testing.T) {
	proxyURL := getPrimaryURL(t, proxyURLRO)
	smap := getClusterMap(t, proxyURL)

	createLocalBucketIfNotExists(t, proxyURL, clibucket)

	var (
		errCh = make(chan error, len(smap.Pmap))
		wg    sync.WaitGroup
	)

	// cid = a goroutine ID to make filenames unique
	// otherwise it is easy to run into a trouble when 2 goroutines do:
	//   1PUT 2PUT 1DEL 2DEL
	// And the second goroutine fails with error "object does not exist"
	cid := int64(0)
	for _, v := range smap.Pmap {
		cid += 1
		wg.Add(1)
		go func(url string, cid int64) {
			defer wg.Done()
			errCh <- proxyPutGetDelete(int64(baseseed)+cid, 100, url)
		}(v.PublicNet.DirectURL, cid)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		tutils.CheckFatal(err, t)
	}
	destroyLocalBucket(t, proxyURL, clibucket)
}

// proxyPutGetDelete repeats put/get/del N times, all requests go to the same proxy
func proxyPutGetDelete(seed int64, count int, proxyURL string) error {
	random := rand.New(rand.NewSource(seed))
	baseParams := tutils.BaseAPIParams(proxyURL)
	for i := 0; i < count; i++ {
		reader, err := tutils.NewRandReader(fileSize, true /* withHash */)
		if err != nil {
			return fmt.Errorf("Error creating reader: %v", err)
		}
		fname := tutils.FastRandomFilename(random, fnlen)
		keyname := fmt.Sprintf("%s/%s", localBucketDir, fname)
		if err = api.PutObject(baseParams, clibucket, keyname, reader.XXHash(), reader); err != nil {
			return fmt.Errorf("Error executing put: %v", err)
		}
		if _, err = api.GetObject(baseParams, clibucket, keyname); err != nil {
			return fmt.Errorf("Error executing get: %v", err)
		}
		if err = tutils.Del(proxyURL, clibucket, keyname, nil /* wg */, nil /* errCh */, true /* silent */); err != nil {
			return fmt.Errorf("Error executing del: %v", err)
		}
	}

	return nil
}

// putGetDelWorker does put/get/del in sequence; if primary proxy change happens, it checks the failed delete
// channel and route the deletes to the new primary proxy
// stops when told to do so via the stop channel
func putGetDelWorker(proxyURL string, seed int64, stopch <-chan struct{}, proxyurlch <-chan string,
	errCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()

	random := rand.New(rand.NewSource(seed))
	missedDeleteCh := make(chan string, 100)
	baseParams := tutils.BaseAPIParams(proxyURL)
loop:
	for {
		select {
		case <-stopch:
			close(errCh)
			break loop

		case url := <-proxyurlch:
			// send failed deletes to the new primary proxy
		deleteLoop:
			for {
				select {
				case keyname := <-missedDeleteCh:
					err := tutils.Del(url, localBucketName, keyname, nil, errCh, true)
					if err != nil {
						missedDeleteCh <- keyname
					}

				default:
					break deleteLoop
				}
			}

		default:
		}

		reader, err := tutils.NewRandReader(fileSize, true /* withHash */)
		if err != nil {
			errCh <- err
			continue
		}

		fname := tutils.FastRandomFilename(random, fnlen)
		keyname := fmt.Sprintf("%s/%s", localBucketDir, fname)

		err = api.PutObject(baseParams, localBucketName, keyname, reader.XXHash(), reader)
		if err != nil {
			errCh <- err
			continue
		}
		_, err = api.GetObject(baseParams, localBucketName, keyname)
		if err != nil {
			errCh <- err
		}

		err = tutils.Del(proxyURL, localBucketName, keyname, nil, errCh, true)
		if err != nil {
			missedDeleteCh <- keyname
		}
	}

	// process left over not deleted objects
	close(missedDeleteCh)
	for n := range missedDeleteCh {
		tutils.Del(proxyURL, localBucketName, n, nil, nil, true)
	}
}

// primaryKiller kills primary proxy, notifies all workers, and restore it.
func primaryKiller(t *testing.T, proxyURL string, seed int64, stopch <-chan struct{}, proxyurlchs []chan string,
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

		smap := getClusterMap(t, proxyURL)
		_, nextProxyURL, err := chooseNextProxy(&smap)
		tutils.CheckFatal(err, t)

		cmd, args, err := kill(smap.ProxySI.DaemonID, smap.ProxySI.PublicNet.DaemonPort)
		tutils.CheckFatal(err, t)

		// let the workers go to the dying primary for a little while longer to generate errored requests
		time.Sleep(time.Second)

		smap, err = waitForPrimaryProxy(nextProxyURL, "to propagate 'primary crashed'", smap.Version, testing.Verbose())
		tutils.CheckFatal(err, t)

		for _, ch := range proxyurlchs {
			ch <- nextProxyURL
		}

		err = restore(cmd, args, false, "proxy (prev primary)")
		tutils.CheckFatal(err, t)

		_, err = waitForPrimaryProxy(nextProxyURL, "to synchronize on 'primary restored'", smap.Version, testing.Verbose())
		tutils.CheckFatal(err, t)
	}
}

// proxyStress starts a group of workers doing put/get/del in sequence against primary proxy,
// while the operations are on going, a separate go routine kills the primary proxy, notifies all
// workers about the proxy change, restart the killed proxy as a non-primary proxy.
// the process is repeated until a pre-defined time duration is reached.
func proxyStress(t *testing.T) {
	var (
		bs          = int64(baseseed)
		errChs      = make([]chan error, numworkers+1)
		stopchs     = make([]chan struct{}, numworkers+1)
		proxyurlchs = make([]chan string, numworkers)
		wg          sync.WaitGroup
		proxyURL    = getPrimaryURL(t, proxyURLRO)
	)

	createLocalBucketIfNotExists(t, proxyURL, localBucketName)

	// start all workers
	for i := 0; i < numworkers; i++ {
		errChs[i] = make(chan error, defaultChanSize)
		stopchs[i] = make(chan struct{}, defaultChanSize)
		proxyurlchs[i] = make(chan string, defaultChanSize)

		wg.Add(1)
		go putGetDelWorker(proxyURL, bs, stopchs[i], proxyurlchs[i], errChs[i], &wg)

		// stagger the workers so they don't always do the same operation at the same time
		n := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(999)
		time.Sleep(time.Duration(n+1) * time.Millisecond)
		bs++
	}

	errChs[numworkers] = make(chan error, defaultChanSize)
	stopchs[numworkers] = make(chan struct{}, defaultChanSize)
	wg.Add(1)
	go primaryKiller(t, proxyURL, bs, stopchs[numworkers], proxyurlchs, errChs[numworkers], &wg)

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
	for _, stopch := range stopchs {
		stopch <- struct{}{}
		close(stopch)
	}

	wg.Wait()
	destroyLocalBucket(t, proxyURL, localBucketName)
}

// smap 	- current Smap
// directURL	- DirectURL of the proxy that we send the request to
//           	  (not necessarily the current primary)
// toID, toURL 	- DaemonID and DirectURL of the proxy that must become the new primary
func setPrimaryTo(t *testing.T, proxyURL string, smap cluster.Smap, directURL, toID, toURL string) {
	if directURL == "" {
		directURL = smap.ProxySI.PublicNet.DirectURL
	}
	// http://host:8081/v1/cluster/proxy/15205:8080

	baseParams := tutils.BaseAPIParams(directURL)
	tutils.Logf("Setting primary from %s to %s\n", smap.ProxySI.DaemonID, toID)
	err := api.SetPrimaryProxy(baseParams, toID)
	tutils.CheckFatal(err, t)

	smap, err = waitForPrimaryProxy(proxyURL, "to designate new primary ID="+toID, smap.Version, testing.Verbose())
	tutils.CheckFatal(err, t)
	if smap.ProxySI.DaemonID != toID {
		t.Fatalf("Expected primary=%s, got %s", toID, smap.ProxySI.DaemonID)
	}
	checkPmapVersions(t, proxyURL)
}

func chooseNextProxy(smap *cluster.Smap) (proxyid, proxyURL string, err error) {
	pid, errstr := hrwProxyTest(smap, smap.ProxySI.DaemonID)
	pi := smap.Pmap[pid]
	if errstr != "" {
		return "", "", fmt.Errorf("%s", errstr)
	}

	return pi.DaemonID, pi.PublicNet.DirectURL, nil
}

func kill(daemonID, port string) (string, []string, error) {
	if tutils.DockerRunning() {
		tutils.Logf("Stopping container %s\n", daemonID)
		err := tutils.StopContainer(daemonID)
		return daemonID, nil, err
	}

	pid, cmd, args, errpid := getProcess(port)
	if errpid != nil {
		return "", nil, errpid
	}
	_, err := exec.Command("kill", "-2", pid).CombinedOutput()
	if err != nil {
		return "", nil, err
	}
	// wait for the process to actually disappear
	to := time.Now().Add(time.Second * 30)
	for {
		_, _, _, errpid := getProcess(port)
		if errpid != nil {
			break
		}
		if time.Now().After(to) {
			err = fmt.Errorf("Failed to kill -2 process pid=%s at port %s", pid, port)
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
			err = fmt.Errorf("Failed to kill -9 process pid=%s at port %s", pid, port)
		}
	}

	return cmd, args, err
}

func restore(cmd string, args []string, asPrimary bool, tag string) error {
	if tutils.DockerRunning() {
		tutils.Logf("Restarting %s container %s\n", tag, cmd)
		return tutils.RestartContainer(cmd)
	}
	tutils.Logf("Restoring %s: %s %+v\n", tag, cmd, args)

	ncmd := exec.Command(cmd, args...)
	// When using Ctrl-C on test, children (restored daemons) should not be
	// killed as well.
	// (see: https://groups.google.com/forum/#!topic/golang-nuts/shST-SDqIp4)
	ncmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
	if asPrimary {
		// Sets the environment variable to start as primary proxy to true
		env := os.Environ()
		env = append(env, "DFCPRIMARYPROXY=TRUE")
		ncmd.Env = env
	}

	err := ncmd.Start()
	ncmd.Process.Release()
	return err
}

// getPID uses 'lsof' to find the pid of the dfc process listening on a port
func getPID(port string) (string, error) {
	output, err := exec.Command("lsof", []string{"-sTCP:LISTEN", "-i", ":" + port}...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("Error executing LSOF command: %v", err)
	}

	// The output of 'lsof' might contain extra message in the begining like this one:
	// lsof: WARNING: can't stat() webdav file system /Volumes/10.2.161.46
	//       Output information may be incomplete.
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

// getProcess finds the dfc process by 'lsof' using a port number, it finds the dfc process's
// original command line by 'ps', returns the command line for later to restart(restore) the process.
func getProcess(port string) (string, string, []string, error) {
	pid, err := getPID(port)
	if err != nil {
		return "", "", nil, fmt.Errorf("Error getting pid on port: %v", err)
	}

	output, err := exec.Command("ps", "-p", pid, "-o", "command").CombinedOutput()
	if err != nil {
		return "", "", nil, fmt.Errorf("Error executing PS command: %v", err)
	}

	line := strings.Split(string(output), "\n")[1]
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return "", "", nil, fmt.Errorf("No returned fields")
	}

	return pid, fields[0], fields[1:], nil
}

// Read Pmap from all proxies and checks versions. If any proxy's smap version
// differs from primary's one then an error returned
func checkPmapVersions(t *testing.T, proxyURL string) {
	smapPrimary := getClusterMap(t, proxyURL)
	for proxyID, proxyInfo := range smapPrimary.Pmap {
		if proxyURL == proxyInfo.PublicNet.DirectURL {
			continue
		}
		smap := getClusterMap(t, proxyInfo.PublicNet.DirectURL)
		if smap.Version != smapPrimary.Version {
			err := fmt.Errorf("Proxy %s has version %d, but primary proxy has version %d of Pmap",
				proxyID, smap.Version, smapPrimary.Version)
			t.Error(err)
		}
	}
}

// waitForPrimaryProxy reads the current primary proxy(which is proxyurl)'s smap until its
// version changed at least once from the original version and then settles down(doesn't change anymore)
// if primary proxy is successfully updated, wait until the map is populated to all members of the cluster
// returns the latset smap of the cluster
//
// The function always waits until SMap's version increases but there are
// two optional parameters for extra checks: proxyCount and targetCount(nodeCnt...).
// If they are not zeroes then besides version check the function waits for given
// number of proxies and/or targets are present in SMap.
// It is useful if the test kills more than one proxy/target. In this case the
// primary proxy may run two metasync calls and we cannot tell if the current SMap
// is what we are waiting for only by looking at its version.
func waitForPrimaryProxy(proxyURL, reason string, origVersion int64, verbose bool, nodeCnt ...int) (cluster.Smap, error) {
	var (
		lastVersion          int64
		timeUntil, timeStart time.Time
		totalProxies         int
		totalTargets         int
	)
	timeStart = time.Now()
	timeUntil = timeStart.Add(proxyChangeLatency)
	if len(nodeCnt) > 0 {
		totalProxies = nodeCnt[0]
	}
	if len(nodeCnt) > 1 {
		totalTargets = nodeCnt[1]
	}

	if verbose {
		if totalProxies > 0 {
			tutils.Logf("Waiting for %d proxies\n", totalProxies)
		}
		if totalTargets > 0 {
			tutils.Logf("Waiting for %d targets\n", totalTargets)
		}
		tutils.Logf("Waiting for the cluster %s [Smap version > %d]\n", reason, origVersion)
	}

	var loopCnt int = 0
	baseParams := tutils.BaseAPIParams(proxyURL)
	for {
		smap, err := api.GetClusterMap(baseParams)
		if err != nil && !cmn.IsErrConnectionRefused(err) {
			return cluster.Smap{}, err
		}

		doCheckSMap := (totalTargets == 0 || len(smap.Tmap) == totalTargets) &&
			(totalProxies == 0 || len(smap.Pmap) == totalProxies)
		if !doCheckSMap {
			d := time.Since(timeStart)
			expectedTargets, expectedProxies := totalTargets, totalProxies
			if totalTargets == 0 {
				expectedTargets = len(smap.Tmap)
			}
			if totalProxies == 0 {
				expectedProxies = len(smap.Pmap)
			}
			tutils.Logf("Smap is not updated yet at %s, targets: %d/%d, proxies: %d/%d (%v)\n",
				proxyURL, len(smap.Tmap), expectedTargets, len(smap.Pmap), expectedProxies, d.Truncate(time.Second))
		}

		// if the primary's map changed to the state we want, wait for the map get populated
		if err == nil && smap.Version == lastVersion && smap.Version > origVersion && doCheckSMap {
			for {
				smap, err = api.GetClusterMap(baseParams)
				if err == nil {
					break
				}

				if !cmn.IsErrConnectionRefused(err) {
					return smap, err
				}

				if time.Now().After(timeUntil) {
					return smap, fmt.Errorf("primary proxy's Smap timed out")
				}
				time.Sleep(time.Second)
			}

			// skip primary proxy and mock targets
			var proxyID string
			for _, p := range smap.Pmap {
				if p.PublicNet.DirectURL == proxyURL {
					proxyID = p.DaemonID
				}
			}
			err = tutils.WaitMapVersionSync(timeUntil, smap, origVersion, []string{mockDaemonID, proxyID})
			return smap, err
		}

		if time.Now().After(timeUntil) {
			break
		}

		lastVersion = smap.Version
		loopCnt++
		time.Sleep(time.Second * time.Duration(loopCnt)) // sleep longer every loop
	}

	return cluster.Smap{}, fmt.Errorf("Timed out waiting for the cluster to stabilize")
}

// primarySetToOriginal reads original primary proxy from configuration and
// makes it a primary proxy again
// NOTE: This test cannot be run as separate test. It requires that original
// primary proxy was down and retuned back. So, the test should be executed
// after primaryCrashElectRestart test
func primarySetToOriginal(t *testing.T) {
	proxyURL := getPrimaryURL(t, proxyURLRO)
	smap := getClusterMap(t, proxyURL)
	var (
		currID, currURL       string
		byURL, byPort, origID string
	)
	currID = smap.ProxySI.DaemonID
	currURL = smap.ProxySI.PublicNet.DirectURL
	if currURL != proxyURL {
		t.Fatalf("Err in the test itself: expecting currURL %s == proxyurl %s", currURL, proxyURL)
	}
	tutils.Logf("Setting primary proxy %s back to the original, Smap version %d\n", currID, smap.Version)

	config := getDaemonConfig(t, proxyURL)
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

	setPrimaryTo(t, proxyURL, smap, "", origID, origURL)
}

// This is duplicated in the tests because the `idDigest` of `daemonInfo` is not
// exported. As a result of this, dfc.HrwProxy will not return the correct
// proxy since the `idDigest` will be initialized to 0. To avoid this, we
// compute the checksum directly in this method.
func hrwProxyTest(smap *cluster.Smap, idToSkip string) (pi string, errstr string) {
	if len(smap.Pmap) == 0 {
		errstr = "DFC cluster map is empty: no proxies"
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
		cs := xxhash.ChecksumString64S(snode.DaemonID, cluster.MLCG32)
		if cs > max {
			max = cs
			pi = id
		}
	}
	if pi == "" {
		errstr = fmt.Sprintf("Cannot HRW-select proxy: current count=%d, skipped=%d", len(smap.Pmap), skipped)
	}
	return
}

func networkFailureTarget(t *testing.T) {
	proxyURL := getPrimaryURL(t, proxyURLRO)
	smap := getClusterMap(t, proxyURL)
	if len(smap.Tmap) == 0 {
		t.Fatal("At least 1 target required")
	}
	proxyCount, targetCount := len(smap.Pmap), len(smap.Tmap)

	targetID := ""
	for id := range smap.Tmap {
		targetID = id
		break
	}

	tutils.Logf("Disconnecting target: %s\n", targetID)
	oldNetworks, err := tutils.DisconnectContainer(targetID)
	tutils.CheckFatal(err, t)

	smap, err = waitForPrimaryProxy(
		proxyURL,
		"target is down",
		smap.Version, testing.Verbose(),
		proxyCount,
		targetCount-1,
	)
	tutils.CheckFatal(err, t)

	tutils.Logf("Connecting target %s to networks again\n", targetID)
	err = tutils.ConnectContainer(targetID, oldNetworks)
	tutils.CheckFatal(err, t)

	smap, err = waitForPrimaryProxy(
		proxyURL,
		"to check cluster state",
		smap.Version, testing.Verbose(),
		proxyCount,
		targetCount,
	)
	tutils.CheckFatal(err, t)
}

func networkFailureProxy(t *testing.T) {
	proxyURL := getPrimaryURL(t, proxyURLRO)
	smap := getClusterMap(t, proxyURL)
	if len(smap.Pmap) < 2 {
		t.Fatal("At least 2 proxy required")
	}
	proxyCount, targetCount := len(smap.Pmap), len(smap.Tmap)

	oldPrimaryID := smap.ProxySI.DaemonID
	proxyID, _, err := chooseNextProxy(&smap)
	tutils.CheckFatal(err, t)

	tutils.Logf("Disconnecting proxy: %s\n", proxyID)
	oldNetworks, err := tutils.DisconnectContainer(proxyID)
	tutils.CheckFatal(err, t)

	smap, err = waitForPrimaryProxy(
		proxyURL,
		"proxy is down",
		smap.Version, testing.Verbose(),
		proxyCount-1,
		targetCount,
	)
	tutils.CheckFatal(err, t)

	tutils.Logf("Connecting proxy %s to networks again\n", proxyID)
	err = tutils.ConnectContainer(proxyID, oldNetworks)
	tutils.CheckFatal(err, t)

	smap, err = waitForPrimaryProxy(
		proxyURL,
		"to check cluster state",
		smap.Version, testing.Verbose(),
		proxyCount,
		targetCount,
	)
	tutils.CheckFatal(err, t)

	if oldPrimaryID != smap.ProxySI.DaemonID {
		t.Fatalf("Primary proxy changed from %s to %s",
			oldPrimaryID, smap.ProxySI.DaemonID)
	}
}

func networkFailurePrimary(t *testing.T) {
	proxyURL := getPrimaryURL(t, proxyURLRO)
	smap := getClusterMap(t, proxyURL)
	if len(smap.Pmap) < 2 {
		t.Fatal("At least 2 proxy required")
	}

	proxyCount, targetCount := len(smap.Pmap), len(smap.Tmap)
	oldPrimaryID, oldPrimaryURL := smap.ProxySI.DaemonID, smap.ProxySI.PublicNet.DirectURL
	newPrimaryID, newPrimaryURL, err := chooseNextProxy(&smap)
	tutils.CheckFatal(err, t)

	// Disconnect primary
	tutils.Logf("Disconnecting primary %s from all networks\n", oldPrimaryID)
	oldNetworks, err := tutils.DisconnectContainer(oldPrimaryID)
	tutils.CheckFatal(err, t)

	// Check smap
	smap, err = waitForPrimaryProxy(
		newPrimaryURL,
		"original primary is gone",
		smap.Version, testing.Verbose(),
		proxyCount-1,
		targetCount,
	)
	tutils.CheckFatal(err, t)

	if smap.ProxySI.DaemonID != newPrimaryID {
		t.Fatalf("wrong primary proxy: %s, expecting: %s after disconnecting", smap.ProxySI.DaemonID, newPrimaryID)
	}

	// Connect again
	tutils.Logf("Connecting primary %s to networks again\n", oldPrimaryID)
	err = tutils.ConnectContainer(oldPrimaryID, oldNetworks)
	tutils.CheckFatal(err, t)

	// give a little time to original primary, so it picks up the network
	// connections and starts talking to neighbors
	waitProgressBar("Wait for old primary is connected to network", 5*time.Second)

	oldSmap := getClusterMap(t, oldPrimaryURL)
	// the original primary still thinks that it is the primary, so its smap
	// should not change after the network is back
	if oldSmap.ProxySI.DaemonID != oldPrimaryID {
		tutils.Logf("Old primary changed its smap. Its current primary: %s (expected %s - self)\n", oldSmap.ProxySI.DaemonID, oldPrimaryID)
	}

	// Forcefully set new primary for the original one
	baseParams := tutils.BaseAPIParams(oldPrimaryURL)
	baseParams.Method = http.MethodPut
	path := cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Proxy, newPrimaryID) +
		fmt.Sprintf("?%s=true&%s=%s", cmn.URLParamForce, cmn.URLParamPrimaryCandidate, url.QueryEscape(newPrimaryURL))
	_, err = api.DoHTTPRequest(baseParams, path, nil)
	tutils.CheckFatal(err, t)

	smap, err = waitForPrimaryProxy(
		newPrimaryURL,
		"original primary joined the new primary",
		smap.Version, testing.Verbose(),
		proxyCount,
		targetCount,
	)
	tutils.CheckFatal(err, t)

	if smap.ProxySI.DaemonID != newPrimaryID {
		t.Fatalf("expected primary=%s, got %s after connecting again", newPrimaryID, smap.ProxySI.DaemonID)
	}
}

func networkFailure(t *testing.T) {
	if !tutils.DockerRunning() {
		t.Skip("Network failure test requires Docker cluster")
	}

	t.Run("Target network disconnect", networkFailureTarget)
	t.Run("Secondary proxy network disconnect", networkFailureProxy)
	t.Run("Primary proxy network disconnect", networkFailurePrimary)
}
