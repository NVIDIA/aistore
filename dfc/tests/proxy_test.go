package dfc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
	"github.com/NVIDIA/dfcpub/pkg/client/readers"
)

const (
	mockDaemonID    = "MOCK"
	localBucketDir  = "multipleproxy"
	localBucketName = "multipleproxytmp"
	defaultChanSize = 10
)

var (
	voteTests = []Test{
		Test{"PrimaryCrash", primaryCrash},
		Test{"PrimaryAndTargetCrash", primaryAndTargetCrash},
		Test{"PrimaryAndProxyCrash", primaryAndProxyCrash},
		Test{"CrashAndFastRestore", crashAndFastRestore},
		Test{"TargetRejoin", targetRejoin},
		Test{"JoinWhileVoteInProgress", joinWhileVoteInProgress},
		Test{"MinorityTargetMapVersionMismatch", minorityTargetMapVersionMismatch},
		Test{"MajorityTargetMapVersionMismatch", majorityTargetMapVersionMismatch},
		Test{"ConcurrentPutGetDel", concurrentPutGetDel},
		Test{"ProxyStress", proxyStress},
	}
)

// Note: This sounds stupid but it is true, proxyurl is a global variable used to get primary proxy
//       url from command line whern the tests start, but it is used globally everywhere, for example,
//       in getClusterMap(), for proxy tests, since the primary proxy will change, in order to use
//       functions like getClusterMap(), it has to maintain proxyurl always points to the current
//       primary proxy, the right way to do it is remove the dependency of proxyurl from getClusterMap(),
//       but who know how many such things exist.

func TestProxy(t *testing.T) {
	if testing.Short() {
		t.Skip("Long run only")
	}

	smap := getClusterMap(httpclient, t)
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

	// note: enable after set primary is fixed in dfc
	// resetPrimaryProxy(originalPrimaryProxyID, t)
	// proxyurl = originalPrimaryProxyURL
}

// clusterHealthCheck verifies the cluster has the same servers after tests
// note: add verify primary if primary is reset
func clusterHealthCheck(t *testing.T, smapBefore dfc.Smap) {
	smapAfter := getClusterMap(httpclient, t)
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

		if !reflect.DeepEqual(a, b) {
			t.Fatalf("Target %s changed, before = %+v, after = %+v", b.DaemonID, b, a)
		}
	}

	for _, b := range smapBefore.Pmap {
		a, ok := smapAfter.Pmap[b.DaemonID]
		if !ok {
			t.Fatalf("Failed to find proxy %s", b.DaemonID)
		}

		// note: can't compare Primary field unless primary is always reset to the original one
		if a.DaemonID != b.DaemonID ||
			a.DaemonPort != b.DaemonPort ||
			a.DirectURL != b.DirectURL ||
			a.NodeIPAddr != b.NodeIPAddr {
			t.Fatalf("Proxy %s changed, before = %+v, after = %+v", b.DaemonID, b, a)
		}
	}

	// no proxy/target died (or not restored)
	for _, b := range smapBefore.Tmap {
		_, err := getPID(b.DaemonPort)
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, b := range smapBefore.Pmap {
		_, err := getPID(b.DaemonPort)
		if err != nil {
			t.Fatal(err)
		}
	}
}

// primaryCrash kills the current primary proxy, wait for the new primary prioxy is up and verifies it,
// restoresa the original primary proxy as non primary, and then restore original primary proxy.
func primaryCrash(t *testing.T) {
	smap := getClusterMap(httpclient, t)
	newPrimaryID, newPrimaryURL, err := chooseNextProxy(&smap)
	if err != nil {
		t.Fatal(err)
	}

	oldPrimaryURL := smap.ProxySI.DirectURL
	oldPrimaryID := smap.ProxySI.DaemonID
	cmd, args, err := kill(httpclient, oldPrimaryURL, smap.ProxySI.DaemonPort)
	// cmd and args are the original command line of how the proxy is started
	// example: cmd = /Users/lid/go/bin/dfc, args = -config=/Users/lid/.dfc/dfc0.json -role=proxy -ntargets=3
	if err != nil {
		t.Fatal(err)
	}

	proxyurl = newPrimaryURL
	smap, err = waitForPrimaryProxy("new primary proxy", smap.Version, testing.Verbose())
	if err != nil {
		t.Fatal(err)
	}

	if smap.ProxySI.DaemonID != newPrimaryID {
		t.Fatalf("Incorrect Primary Proxy: %v, should be: %v", smap.ProxySI.DaemonID, newPrimaryID)
	}

	// re-construct the command line to start the original proxy but add the current primary proxy to the args
	// example: cmd = /Users/lid/go/bin/dfc
	//          args = -config=/Users/lid/.dfc/dfc0.json -role=proxy -ntargets=3 -proxyurl=http://10.112.76.36:8082
	args = setProxyURLArg(args, newPrimaryURL)
	err = restore(httpclient, oldPrimaryURL, cmd, args, false)
	if err != nil {
		t.Fatal(err)
	}

	smap, err = waitForPrimaryProxy("restore", smap.Version, testing.Verbose())
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := smap.Pmap[oldPrimaryID]; !ok {
		t.Fatalf("Previous Primary Proxy did not rejoin the cluster.")
	}
}

// primaryAndTargetCrash kills the primary p[roxy and one random target, verifies the next in
// line proxy becomes the new primary, restore the target and proxy, restore original primary.
func primaryAndTargetCrash(t *testing.T) {
	smap := getClusterMap(httpclient, t)
	newPrimaryID, newPrimaryURL, err := chooseNextProxy(&smap)
	if err != nil {
		t.Fatal(err)
	}

	oldPrimaryURL := smap.ProxySI.DirectURL
	cmd, args, err := kill(httpclient, oldPrimaryURL, smap.ProxySI.DaemonPort)
	if err != nil {
		t.Fatal(err)
	}

	// Select a random target
	var (
		targetURL  string
		targetPort string
	)

	for _, v := range smap.Tmap {
		targetURL = v.DirectURL
		targetPort = v.DaemonPort
		break
	}

	tcmd, targs, err := kill(httpclient, targetURL, targetPort)
	if err != nil {
		t.Fatal(err)
	}

	proxyurl = newPrimaryURL
	smap, err = waitForPrimaryProxy("new primary proxy", smap.Version, testing.Verbose())
	if err != nil {
		t.Fatal(err)
	}

	if smap.ProxySI.DaemonID != newPrimaryID {
		t.Fatalf("Incorrect Primary Proxy: %v, should be: %v", smap.ProxySI.DaemonID, newPrimaryID)
	}

	targs = setProxyURLArg(targs, newPrimaryURL)
	err = restore(httpclient, targetURL, tcmd, targs, false)
	if err != nil {
		t.Fatal(err)
	}

	args = setProxyURLArg(args, newPrimaryURL)
	err = restore(httpclient, oldPrimaryURL, cmd, args, false)
	if err != nil {
		t.Fatal(err)
	}

	_, err = waitForPrimaryProxy("restore", smap.Version, testing.Verbose())
	if err != nil {
		t.Fatal(err)
	}
}

// primaryAndProxyCrash kills primary proxy and one another proxy(not the next in line primary)
// and restore them afterwards
func primaryAndProxyCrash(t *testing.T) {
	smap := getClusterMap(httpclient, t)
	newPrimaryID, newPrimaryURL, err := chooseNextProxy(&smap)
	if err != nil {
		t.Fatal(err)
	}

	oldPrimaryURL, oldPrimaryID := smap.ProxySI.DirectURL, smap.ProxySI.DaemonID
	cmd, args, err := kill(httpclient, oldPrimaryURL, smap.ProxySI.DaemonPort)
	if err != nil {
		t.Fatal(err)
	}

	var (
		secondURL  string
		secondPort string
		secondID   string
	)

	// Select a third random proxy
	// Do not choose the next primary in line, or the current primary proxy
	// This is because the system currently cannot recover if the next proxy in line is
	// also killed.
	for k, v := range smap.Pmap {
		if k != newPrimaryID && k != oldPrimaryID {
			secondURL = v.DirectURL
			secondPort = v.DaemonPort
			secondID = v.DaemonID
			break
		}
	}

	secondCmd, secondArgs, err := kill(httpclient, secondURL, secondPort)
	if err != nil {
		t.Fatal(err)
	}

	proxyurl = newPrimaryURL
	smap, err = waitForPrimaryProxy("new primary", smap.Version, testing.Verbose())
	if err != nil {
		t.Fatal(err)
	}

	err = restore(httpclient, secondURL, secondCmd, secondArgs, true)
	if err != nil {
		t.Fatal(err)
	}

	err = restore(httpclient, oldPrimaryURL, cmd, args, true)
	if err != nil {
		t.Fatal(err)
	}

	smap, err = waitForPrimaryProxy("restore", smap.Version, testing.Verbose())
	if err != nil {
		t.Fatal(err)
	}

	if smap.ProxySI.DaemonID != newPrimaryID {
		t.Fatalf("Incorrect Primary Proxy: %v, should be: %v", smap.ProxySI.DaemonID, newPrimaryID)
	}

	if _, ok := smap.Pmap[oldPrimaryID]; !ok {
		t.Fatalf("Previous Primary Proxy did not rejoin the cluster.")
	}

	if _, ok := smap.Pmap[secondID]; !ok {
		t.Fatalf("Second Proxy did not rejoin the cluster.")
	}
}

// targetRejoin kills a random selected target, wait for it to rejoin and verifies it
func targetRejoin(t *testing.T) {
	var (
		url  string
		id   string
		port string
	)

	smap := getClusterMap(httpclient, t)
	for _, v := range smap.Tmap {
		url = v.DirectURL
		id = v.DaemonID
		port = v.DaemonPort
		break
	}

	cmd, args, err := kill(httpclient, url, port)
	smap, err = waitForPrimaryProxy("target crash", smap.Version, testing.Verbose())
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := smap.Tmap[id]; ok {
		t.Fatalf("Killed Target was not removed from the cluster map: %v", id)
	}

	args = removeProxyURLArg(args)
	err = restore(httpclient, url, cmd, args, false)
	if err != nil {
		t.Fatal(err)
	}

	smap, err = waitForPrimaryProxy("target rejoin", smap.Version, testing.Verbose())
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := smap.Tmap[id]; !ok {
		t.Fatalf("Restarted Target did not rejoin the cluster: %v", id)
	}
}

// crashAndFastRestore kills the primary and restore before a new leader is elected
func crashAndFastRestore(t *testing.T) {
	smap := getClusterMap(httpclient, t)

	url := smap.ProxySI.DirectURL
	id := smap.ProxySI.DaemonID
	cmd, args, err := kill(httpclient, url, smap.ProxySI.DaemonPort)
	if err != nil {
		t.Fatal(err)
	}

	// quick crash and recover
	time.Sleep(2 * time.Second)
	err = restore(httpclient, url, cmd, args, true)
	if err != nil {
		t.Fatal(err)
	}

	// Note: using (version - 1) because the primary will restart with its old version,
	//       there will be no version change for this restore, so force beginning version to 1 less
	//       than original version in order to use waitForPrimaryProxy
	smap, err = waitForPrimaryProxy("restore", smap.Version-1, testing.Verbose())
	if err != nil {
		t.Fatal(err)
	}

	if smap.ProxySI.DaemonID != id {
		t.Fatalf("incorrect primary proxy, exp = %s, act = %s", smap.ProxySI.DaemonID, id)
	}
}

func joinWhileVoteInProgress(t *testing.T) {
	smap := getClusterMap(httpclient, t)
	newPrimaryID, newPrimaryURL, err := chooseNextProxy(&smap)
	if err != nil {
		t.Fatal(err)
	}

	stopch := make(chan struct{})
	errch := make(chan error, 10)
	mocktgt := &voteRetryMockTarget{
		vote:  true,
		errch: errch,
	}

	go runMockTarget(mocktgt, stopch, &smap)

	smap, err = waitForPrimaryProxy("mock target", smap.Version, testing.Verbose())
	if err != nil {
		t.Fatal(err)
	}

	oldPrimaryURL := smap.ProxySI.DirectURL
	oldPrimaryID := smap.ProxySI.DaemonID
	cmd, args, err := kill(httpclient, oldPrimaryURL, smap.ProxySI.DaemonPort)
	if err != nil {
		t.Fatal(err)
	}

	proxyurl = newPrimaryURL
	smap, err = waitForPrimaryProxy("new primary", smap.Version, testing.Verbose())
	if err != nil {
		t.Fatal(err)
	}

	err = restore(httpclient, oldPrimaryURL, cmd, args, true)
	if err != nil {
		t.Fatal(err)
	}

	// check if the previous primary proxy has not yet rejoined the cluster
	// it should be waiting for the mock target to return vote=false
	time.Sleep(5 * time.Second)
	smap = getClusterMap(httpclient, t)

	if smap.ProxySI.DaemonID != newPrimaryID {
		t.Fatalf("Incorrect Primary Proxy: %v, should be: %v", smap.ProxySI.DaemonID, newPrimaryID)
	}

	if _, ok := smap.Pmap[oldPrimaryID]; ok {
		t.Fatalf("Previous Primary Proxy rejoined the cluster during a vote")
	}

	mocktgt.vote = false

	smap, err = waitForPrimaryProxy("rejoin", smap.Version, testing.Verbose())
	if err != nil {
		t.Fatal(err)
	}

	if smap.ProxySI.DaemonID != newPrimaryID {
		t.Fatalf("Incorrect Primary Proxy: %v, should be: %v", smap.ProxySI.DaemonID, newPrimaryID)
	}

	if _, ok := smap.Pmap[oldPrimaryID]; !ok {
		t.Fatalf("Previous Primary Proxy did not rejoin the cluster")
	}

	// time to kill the mock target, job well done
	var v struct{}
	stopch <- v
	close(stopch)
	select {
	case err := <-errch:
		t.Errorf("Mock Target Error: %v", err)

	default:
	}

	_, err = waitForPrimaryProxy("kill mock", smap.Version, testing.Verbose())
	if err != nil {
		t.Fatal(err)
	}
}

func minorityTargetMapVersionMismatch(t *testing.T) {
	targetMapVersionMismatch(
		func(i int) int {
			return i/4 + 1
		}, t)
}

func majorityTargetMapVersionMismatch(t *testing.T) {
	targetMapVersionMismatch(
		func(i int) int {
			return i/2 + 1
		}, t)
}

// targetMapVersionMismatch updates map verison of a few targets, kill the primary proxy
// wait for the new leader to come online
func targetMapVersionMismatch(getNum func(int) int, t *testing.T) {
	smap := getClusterMap(httpclient, t)
	oldVer := smap.Version

	smap.Version++
	jsonMap, err := json.Marshal(smap)
	if err != nil {
		t.Fatal(err)
	}

	n := getNum(len(smap.Tmap) + len(smap.Pmap) - 1)
	for _, v := range smap.Tmap {
		if n == 0 {
			break
		}

		url := fmt.Sprintf("%s/%s/%s/%s", v.DirectURL, dfc.Rversion, dfc.Rdaemon, dfc.Rsyncsmap)
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(jsonMap))
		if err != nil {
			t.Fatal(err)
		}

		r, err := httpclient.Do(req)
		if err != nil {
			t.Fatal(err)
		}

		defer func(r *http.Response) {
			if r.Body != nil {
				r.Body.Close()
			}
		}(r)

		_, err = ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}

		n--
	}

	nextProxyID, nextProxyURL, err := chooseNextProxy(&smap)
	if err != nil {
		t.Fatal(err)
	}

	primaryProxyURL := smap.ProxySI.DirectURL
	cmd, args, err := kill(httpclient, primaryProxyURL, smap.ProxySI.DaemonPort)
	if err != nil {
		t.Fatal(err)
	}

	proxyurl = nextProxyURL
	smap, err = waitForPrimaryProxy("new primary proxy", oldVer, testing.Verbose())
	if err != nil {
		t.Fatal(err)
	}

	if smap.ProxySI == nil {
		t.Fatalf("Nil ProxySI in retrieved cluster map.")
	}

	if smap.ProxySI.DaemonID != nextProxyID {
		t.Fatalf("Incorrect Primary Proxy: %v, should be: %v", smap.ProxySI.DaemonID, nextProxyID)
	}

	args = setProxyURLArg(args, nextProxyURL)
	err = restore(httpclient, primaryProxyURL, cmd, args, false)
	if err != nil {
		t.Fatal(err)
	}

	_, err = waitForPrimaryProxy("restore", smap.Version, testing.Verbose())
	if err != nil {
		t.Fatal(err)
	}
}

// concurrentPutGetDel does put/get/del sequence against all proxies concurrently
func concurrentPutGetDel(t *testing.T) {
	smap := getClusterMap(httpclient, t)

	var (
		errch = make(chan error, len(smap.Pmap))
		wg    sync.WaitGroup
	)

	for _, v := range smap.Pmap {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			errch <- proxyPutGetDelete(int64(baseseed), 100, url)
		}(v.DirectURL)
	}

	wg.Wait()
	close(errch)

	for err := range errch {
		if err != nil {
			t.Fatal(err)
		}
	}
}

// proxyPutGetDelete repeats put/get/del N times, all requests go to the same proxy
func proxyPutGetDelete(seed int64, count int, proxyurl string) error {
	random := rand.New(rand.NewSource(seed))
	for i := 0; i < count; i++ {
		reader, err := readers.NewRandReader(fileSize, true /* withHash */)
		if err != nil {
			return fmt.Errorf("Error creating reader: %v", err)
		}

		fname := client.FastRandomFilename(random, fnlen)
		keyname := fmt.Sprintf("%s/%s", localBucketDir, fname)

		err = client.Put(proxyurl, reader, clibucket, keyname, true /* silent */)
		if err != nil {
			return fmt.Errorf("Error executing put: %v", err)
		}

		client.Get(proxyurl, clibucket, keyname, nil /* wg */, nil /* errch */, true /* silent */, false /* validate */)
		if err != nil {
			return fmt.Errorf("Error executing get: %v", err)
		}

		err = client.Del(proxyurl, clibucket, keyname, nil /* wg */, nil /* errch */, true /* silent */)
		if err != nil {
			return fmt.Errorf("Error executing del: %v", err)
		}
	}

	return nil
}

// putGetDelWorker does put/get/del in sequence; if primary proxy change happens, it checks the failed delete
// channel and route the deletes to the new primary proxy
// stops when told to do so via the stop channel
func putGetDelWorker(seed int64, stopch <-chan struct{}, proxyurlch <-chan string,
	errch chan error, wg *sync.WaitGroup) {
	defer wg.Done()

	random := rand.New(rand.NewSource(seed))
	missedDeleteCh := make(chan string, 100)
loop:
	for {
		select {
		case <-stopch:
			close(errch)
			break loop

		case url := <-proxyurlch:
			// send failed deletes to the new primary proxy
		deleteLoop:
			for {
				select {
				case keyname := <-missedDeleteCh:
					err := client.Del(url, localBucketName, keyname, nil, errch, true)
					if err != nil {
						missedDeleteCh <- keyname
					}

				default:
					break deleteLoop
				}
			}

		default:
		}

		reader, err := readers.NewRandReader(fileSize, true /* withHash */)
		if err != nil {
			errch <- err
			continue
		}

		fname := client.FastRandomFilename(random, fnlen)
		keyname := fmt.Sprintf("%s/%s", localBucketDir, fname)

		err = client.Put(proxyurl, reader, localBucketName, keyname, true /* silent */)
		if err != nil {
			errch <- err
			continue
		}

		_, _, err = client.Get(proxyurl, localBucketName, keyname, nil, errch, true, false)
		if err != nil {
			errch <- err
		}

		err = client.Del(proxyurl, localBucketName, keyname, nil, errch, true)
		if err != nil {
			missedDeleteCh <- keyname
		}
	}

	// process left over not deleted objects
	close(missedDeleteCh)
	for n := range missedDeleteCh {
		client.Del(proxyurl, localBucketName, n, nil, nil, true)
	}
}

// primaryKiller kills primary proxy, notifies all workers, and restore it.
func primaryKiller(t *testing.T, seed int64, stopch <-chan struct{}, proxyurlchs []chan string,
	errch chan error, wg *sync.WaitGroup) {
	defer wg.Done()

loop:
	for {
		select {
		case <-stopch:
			close(errch)
			for _, ch := range proxyurlchs {
				close(ch)
			}

			break loop

		default:
		}

		smap := getClusterMap(httpclient, t)
		_, nextProxyURL, err := chooseNextProxy(&smap)
		if err != nil {
			t.Fatal(err)
		}

		primaryProxyURL := smap.ProxySI.DirectURL
		cmd, args, err := kill(httpclient, primaryProxyURL, smap.ProxySI.DaemonPort)
		if err != nil {
			t.Fatal(err)
		}

		// let the workers go to the dying primary for a little while longer to generate errored requests
		time.Sleep(time.Second)

		proxyurl = nextProxyURL
		smap, err = waitForPrimaryProxy("primary crash", smap.Version, testing.Verbose())
		if err != nil {
			t.Fatal(err)
		}

		for _, ch := range proxyurlchs {
			ch <- nextProxyURL
		}

		args = setProxyURLArg(args, nextProxyURL)
		err = restore(httpclient, primaryProxyURL, cmd, args, false)
		if err != nil {
			t.Fatal(err)
		}

		_, err = waitForPrimaryProxy("primary restore", smap.Version, testing.Verbose())
		if err != nil {
			t.Fatal(err)
		}
	}
}

// proxyStress starts a group of workers doing put/get/del in sequence against primary proxy,
// while the operations are on going, a separate go routine kills the primary proxy, notifies all
// workers about the proxy change, restart the killed proxy as a non-primary proxy.
// the process is repeated until a pre-defined time duration is reached.
func proxyStress(t *testing.T) {
	var (
		bs          = int64(baseseed)
		errchs      = make([]chan error, numworkers+1)
		stopchs     = make([]chan struct{}, numworkers+1)
		proxyurlchs = make([]chan string, numworkers)
		wg          sync.WaitGroup
	)

	err := createLocalBucketNoFail(httpclient, t, localBucketName)
	if err != nil {
		t.Fatal(err)
	}

	// start all workers
	for i := 0; i < numworkers; i++ {
		errchs[i] = make(chan error, defaultChanSize)
		stopchs[i] = make(chan struct{}, defaultChanSize)
		proxyurlchs[i] = make(chan string, defaultChanSize)

		wg.Add(1)
		go putGetDelWorker(bs, stopchs[i], proxyurlchs[i], errchs[i], &wg)

		// stagger the workers so they don't always do the same operation at the same time
		n := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(999)
		time.Sleep(time.Duration(n+1) * time.Millisecond)
		bs++
	}

	errchs[numworkers] = make(chan error, defaultChanSize)
	stopchs[numworkers] = make(chan struct{}, defaultChanSize)
	wg.Add(1)
	go primaryKiller(t, bs, stopchs[numworkers], proxyurlchs, errchs[numworkers], &wg)

	timer := time.After(multiProxyTestDuration)
loop:
	for {
		for _, ch := range errchs {
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
	client.DestroyLocalBucket(proxyurl, localBucketName)
}

func chooseNextProxy(smap *dfc.Smap) (proxyid, proxyurl string, err error) {
	pi, errstr := dfc.HrwProxy(smap, smap.ProxySI.DaemonID)
	if errstr != "" {
		return "", "", fmt.Errorf("%s", errstr)
	}

	return pi.DaemonID, pi.DirectURL, nil
}

func kill(httpclient *http.Client, url, port string) (string, []string, error) {
	pid, cmd, args, err := getProcess(port)
	if err != nil {
		return "", nil, err
	}

	// use a mixed of -9 and -2 to terminate the proxy/targets to test both clean termination and
	// sudden death
	howToDie := "-9"
	if time.Now().UnixNano()%2 == 0 {
		howToDie = "-2"
	}

	_, err = exec.Command("kill", howToDie, pid).CombinedOutput()
	if err != nil {
		return "", nil, err
	}

	// wait for the process to actually disappear
	to := time.Now().Add(time.Minute)
	for {
		_, _, _, err := getProcess(port)
		if err != nil {
			// assume it failed because the process is gone
			break
		}

		if time.Now().After(to) {
			return "", nil, fmt.Errorf("Failed to kill process at port %s", port)
		}

		time.Sleep(time.Second)
	}

	return cmd, args, err
}

func restore(httpclient *http.Client, url, cmd string, args []string, asPrimary bool) error {
	// note: when starting a process, it has a stderr pipe, not the standard fd = 2,
	//       when glog.Errorf() is called, it returned EPIPE, which causes the process to die,
	//       didn't find out why.
	//       as a work around, start the background process without logging to stderr.
	//       may be all dfc process should be deployed with this option.
	//       only needs to add the option once.
	var found bool
	for _, v := range args {
		if strings.Contains(v, "stderrthreshold") {
			found = true
		}
	}

	if !found {
		args = append(args, "-stderrthreshold=100")
	}

	cmdStart := exec.Command(cmd, args...)
	if asPrimary {
		// Sets the environment variable to start as Primary Proxy to true
		env := os.Environ()
		env = append(env, "DFCPRIMARYPROXY=TRUE")
		cmdStart.Env = env
	}

	var stderr bytes.Buffer
	cmdStart.Stderr = &stderr
	go func() {
		err := cmdStart.Run()
		if err != nil && !strings.HasPrefix(err.Error(), "signal:") {
			// Don't print signal errors, because they're generally created by this test.
			fmt.Printf("Error running command %v %v: %v (%v)\n", cmd, args, err, stderr.String())
		}
	}()

	return nil
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

// getOutboundIP taken from https://stackoverflow.com/a/37382208
func getOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()

	return conn.LocalAddr().(*net.UDPAddr).IP
}

// resetPrimaryProxy sends a http request to the current primary proxy process to ask it set primary
// proxy of the cluster to 'proxyid'.
func resetPrimaryProxy(proxyid string, t *testing.T) {
	smap := getClusterMap(httpclient, t)
	url := smap.ProxySI.DirectURL + "/" + dfc.Rversion + "/" + dfc.Rcluster + "/" + dfc.Rproxy + "/" + proxyid
	req, err := http.NewRequest(http.MethodPut, url, nil)
	if err != nil {
		t.Fatal(err)
	}

	r, err := httpclient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if r.Body != nil {
			r.Body.Close()
		}
	}()

	_, err = ioutil.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}

	_, err = waitForPrimaryProxy("restore primary proxy", smap.Version, testing.Verbose())
	if err != nil {
		t.Fatal(err)
	}
}

func removeProxyURLArg(args []string) []string {
	var idx int
	found := false
	for i, arg := range args {
		if strings.Contains(arg, "-proxyurl") {
			idx = i
			found = true
			break
		}
	}
	if found {
		args = append(args[:idx], args[idx+1:]...)
	}
	return args
}

func setProxyURLArg(args []string, proxyurl string) []string {
	args = removeProxyURLArg(args)
	args = append(args, "-proxyurl="+proxyurl)
	return args
}

// waitForPrimaryProxy reads the current primary proxy(which is proxyurl)'s smap until its
// version changed at least once from the beginning version and then settles down(doesn't change anymore)
// if primary proxy is successfully updated, wait until the map is populated to all members of the cluster
// returns the latset smap of the cluster
func waitForPrimaryProxy(reason string, beginingVersion int64, verbose bool) (dfc.Smap, error) {
	var (
		lastVersion int64
		to          = time.Now().Add(proxyChangeLatency)
	)

	if verbose {
		fmt.Printf("Waiting for cluster on %s .", reason)
		defer fmt.Println()
	}

	for {
		smap, err := client.GetClusterMap(proxyurl)
		if err != nil && !dfc.IsErrConnectionRefused(err) {
			return dfc.Smap{}, err
		}

		// if dthe primary's map changed to the state we want, wait for the map get populated
		if err == nil && smap.Version == lastVersion && smap.Version > beginingVersion {
			err = waitForMapSync()
			return smap, err
		}

		if time.Now().After(to) {
			break
		}

		lastVersion = smap.Version
		time.Sleep(time.Second)

		if verbose {
			fmt.Printf(".")
		}
	}

	return dfc.Smap{}, fmt.Errorf("Timed out while waiting for cluster to stablize")
}

// waitForMapSync querys ever target/proxy's smap, wait until they all have the same version
// (basically wait for smap get populated to all deamons)
func waitForMapSync() error {
	var (
		smap dfc.Smap
		err  error
	)

	to := time.Now().Add(proxyChangeLatency)
	// get primary proxy's map
	for {
		smap, err = client.GetClusterMap(proxyurl)
		if err == nil {
			break
		}

		if !dfc.IsErrConnectionRefused(err) {
			return err
		}

		if time.Now().After(to) {
			return fmt.Errorf("get primary proxy's smap timedout")
		}

		time.Sleep(time.Second)
	}

	urls := make(map[string]struct{})
	for _, v := range smap.Tmap {
		// mock targets do not handle all requests, ignore them
		if v.DaemonID != mockDaemonID {
			urls[v.DirectURL] = struct{}{}
		}
	}

	for _, v := range smap.Pmap {
		// skip primary proxy
		if v.DirectURL != proxyurl {
			urls[v.DirectURL] = struct{}{}
		}
	}

	verExpected := smap.Version
	for k, _ := range urls {
		smap, err = client.GetClusterMap(k)
		if err != nil && !dfc.IsErrConnectionRefused(err) {
			return err
		}

		if err == nil && smap.Version == verExpected {
			delete(urls, k)
			continue
		}

		if time.Now().After(to) {
			return fmt.Errorf("get server (%s) smap timedout", k)
		}

		time.Sleep(time.Second)
	}

	return nil
}

const (
	mockTargetPort = "8079"
)

type targetMocker interface {
	filehdlr(w http.ResponseWriter, r *http.Request)
	daemonhdlr(w http.ResponseWriter, r *http.Request)
	votehdlr(w http.ResponseWriter, r *http.Request)
}

func runMockTarget(mocktgt targetMocker, stopch chan struct{}, smap *dfc.Smap) {
	mux := http.NewServeMux()

	mux.HandleFunc("/"+dfc.Rversion+"/"+dfc.Rbuckets+"/", mocktgt.filehdlr)
	mux.HandleFunc("/"+dfc.Rversion+"/"+dfc.Robjects+"/", mocktgt.filehdlr)
	mux.HandleFunc("/"+dfc.Rversion+"/"+dfc.Rdaemon, mocktgt.daemonhdlr)
	mux.HandleFunc("/"+dfc.Rversion+"/"+dfc.Rdaemon+"/", mocktgt.daemonhdlr)
	mux.HandleFunc("/"+dfc.Rversion+"/"+dfc.Rvote+"/", mocktgt.votehdlr)
	mux.HandleFunc("/"+dfc.Rversion+"/"+dfc.Rhealth, func(w http.ResponseWriter, r *http.Request) {})
	s := &http.Server{Addr: ":" + mockTargetPort, Handler: mux}

	registerMockTarget(mocktgt, smap)

	go s.ListenAndServe()

	<-stopch
	unregisterMockTarget(mocktgt)
	s.Shutdown(context.Background())
}

func registerMockTarget(mocktgt targetMocker, smap *dfc.Smap) error {
	var (
		jsonDaemonInfo []byte
		err            error
	)

	// borrow a random target's ip but using a different port to register the mock target
	for _, v := range smap.Tmap {
		ip := getOutboundIP().String()

		v.DaemonID = mockDaemonID
		v.DaemonPort = mockTargetPort
		v.NodeIPAddr = ip
		v.DirectURL = "http://" + ip + ":" + mockTargetPort
		jsonDaemonInfo, err = json.Marshal(v)
		if err != nil {
			return err
		}

		break
	}

	url := proxyurl + "/" + dfc.Rversion + "/" + dfc.Rcluster
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonDaemonInfo))
	if err != nil {
		return err
	}

	r, err := httpclient.Do(req)
	if err != nil {
		return err
	}

	defer func() {
		if r.Body != nil {
			r.Body.Close()
		}
	}()

	_, err = ioutil.ReadAll(r.Body)
	return err
}

func unregisterMockTarget(mocktgt targetMocker) error {
	url := proxyurl + "/" + dfc.Rversion + "/" + dfc.Rcluster + "/" + dfc.Rdaemon + "/" + "MOCK"
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return err
	}

	r, err := httpclient.Do(req)
	if err != nil {
		return err
	}

	defer func() {
		if r.Body != nil {
			r.Body.Close()
		}
	}()

	_, err = ioutil.ReadAll(r.Body)
	return err
}

type voteRetryMockTarget struct {
	vote  bool
	errch chan error
}

func (*voteRetryMockTarget) filehdlr(w http.ResponseWriter, r *http.Request) {
	// Ignore all file requests
	return
}

func (p *voteRetryMockTarget) daemonhdlr(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		// Treat all Get requests as requests for a VoteMsg
		msg := dfc.SmapVoteMsg{
			VoteInProgress: p.vote,
			// The VoteMessage must have a Smap with non-zero version
			Smap: &dfc.Smap{Version: 1},
		}

		jsbytes, err := json.Marshal(msg)
		if err == nil {
			_, err = w.Write(jsbytes)
		}

		if err != nil {
			p.errch <- fmt.Errorf("Error writing message: %v\n", err)
		}

	default:
	}
}

func (p *voteRetryMockTarget) votehdlr(w http.ResponseWriter, r *http.Request) {
	// Always vote yes.
	w.Write([]byte(dfc.VoteYes))
}
