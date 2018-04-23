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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
	"github.com/NVIDIA/dfcpub/pkg/client/readers"
	"github.com/OneOfOne/xxhash"
	"golang.org/x/net/context/ctxhttp"
)

const (
	HRWmLCG32    = 1103515245
	pingtimeout  = 100 * time.Millisecond
	pollinterval = 500 * time.Millisecond
	maxpings     = 10
)

var (
	voteTests = []Test{
		Test{"Proxy Failure", proxy_failure},
		Test{"Multiple Failures", multiple_failures},
		Test{"Rejoin", rejoin},
		Test{"Primary Proxy Rejoin", primaryproxyrejoin},
		Test{"Minority Cluster Map Mismatch", minoritymismatchclustermap},
		Test{"Majority Cluster Map Mismatch", majoritymismatchclustermap},
		Test{"Multiple Proxy Operations", putgetmultipleproxies},
		Test{"Set Primary Proxy", setPrimaryProxy},
	}
)

func canRunMultipleProxyTests(t *testing.T) (proxyid string) {
	if testing.Short() {
		t.Skip("Long run only")
	}

	smap := getClusterMap(httpclient, t)
	if len(smap.Pmap) <= 1 {
		t.Errorf("Not enough proxies to run Test_vote: %v, must be more than 1", len(smap.Pmap))
	}

	return smap.ProxySI.DaemonID
}

func Test_vote(t *testing.T) {
	originalproxyid := canRunMultipleProxyTests(t)
	originalproxyurl := proxyurl

	for _, test := range voteTests {
		t.Run(test.name, test.method)
		if t.Failed() && abortonerr {
			t.FailNow()
		}
	}

	resetPrimaryProxy(originalproxyid, t)
	proxyurl = originalproxyurl
}

//==========
//
// Subtests
//
//==========

func proxy_failure(t *testing.T) {
	// Get Smap
	smap := getClusterMap(httpclient, t)

	// hrwProxy to find next proxy
	delete(smap.Pmap, smap.ProxySI.DaemonID)
	nextProxyID, nextProxyURL, err := hrwProxy(&smap)
	if err != nil {
		t.Errorf("Error performing HRW: %v", err)
	}

	// Kill original primary proxy
	primaryProxyURL := smap.ProxySI.DirectURL
	cmd, args, err := kill(httpclient, primaryProxyURL, smap.ProxySI.DaemonPort)
	if err != nil {
		t.Errorf("Error killing Primary Proxy: %v", err)
	}
	// Wait the maxmimum time it should take to switch.
	waitProgressBar("Primary Proxy Changing: ", time.Duration(2*keepaliveseconds)*time.Second)

	// Check if the next proxy is the one we found from hrw
	proxyurl = nextProxyURL
	smap = getClusterMap(httpclient, t)
	if smap.ProxySI.DaemonID != nextProxyID {
		t.Errorf("Incorrect Primary Proxy: %v, should be: %v", smap.ProxySI.DaemonID, nextProxyID)
	}

	args = append(args, "-proxyurl="+nextProxyURL)
	err = restore(httpclient, primaryProxyURL, cmd, args, false)
	if err != nil {
		t.Errorf("Error restoring proxy: %v", err)
	}
}

func multiple_failures(t *testing.T) {

	// Get Smap
	smap := getClusterMap(httpclient, t)

	// hrwProxy to find next proxy
	delete(smap.Pmap, smap.ProxySI.DaemonID)
	nextProxyID, nextProxyURL, err := hrwProxy(&smap)
	if err != nil {
		t.Errorf("Error performing HRW: %v", err)
	}

	// Kill original primary proxy and a target
	primaryProxyURL := smap.ProxySI.DirectURL
	pcmd, pargs, err := kill(httpclient, primaryProxyURL, smap.ProxySI.DaemonPort)
	if err != nil {
		t.Errorf("Error killing Primary Proxy: %v", err)
	}

	targetURLToKill := ""
	targetPortToKill := ""
	// Select a random target
	for _, tgtinfo := range smap.Smap {
		targetURLToKill = tgtinfo.DirectURL
		targetPortToKill = tgtinfo.DaemonPort
		break
	}
	tcmd, targs, err := kill(httpclient, targetURLToKill, targetPortToKill)
	if err != nil {
		t.Errorf("Error killing Target: %v", err)
	}

	// Wait the maxmimum time it should take to switch.
	waitProgressBar("Primary Proxy Changing: ", time.Duration(2*keepaliveseconds)*time.Second)

	// Check if the next proxy is the one we found from hrw
	proxyurl = nextProxyURL
	smap = getClusterMap(httpclient, t)
	if smap.ProxySI.DaemonID != nextProxyID {
		t.Errorf("Incorrect Primary Proxy: %v, should be: %v", smap.ProxySI.DaemonID, nextProxyID)
	}

	// Restore the killed target
	targs = append(targs, "-proxyurl="+nextProxyURL)
	err = restore(httpclient, targetURLToKill, tcmd, targs, false)
	if err != nil {
		t.Errorf("Error restoring target: %v", err)
	}
	pargs = append(pargs, "-proxyurl="+nextProxyURL)
	err = restore(httpclient, primaryProxyURL, pcmd, pargs, false)
	if err != nil {
		t.Errorf("Error restoring proxy: %v", err)
	}
}

func rejoin(t *testing.T) {
	// Get Smap
	smap := getClusterMap(httpclient, t)

	// hrwProxy to find next proxy
	delete(smap.Pmap, smap.ProxySI.DaemonID)
	nextProxyID, nextProxyURL, err := hrwProxy(&smap)
	if err != nil {
		t.Errorf("Error performing HRW: %v", err)
	}

	// Kill original primary proxy
	primaryProxyURL := smap.ProxySI.DirectURL
	pcmd, pargs, err := kill(httpclient, primaryProxyURL, smap.ProxySI.DaemonPort)
	if err != nil {
		t.Errorf("Error killing Primary Proxy: %v", err)
	}

	// Wait the maxmimum time it should take to switch.
	waitProgressBar("Primary Proxy Changing: ", time.Duration(2*keepaliveseconds)*time.Second)

	// Kill a Target
	targetURLToKill := ""
	targetIDToKill := ""
	targetPortToKill := ""
	// Select a random target
	for _, tgtinfo := range smap.Smap {
		targetURLToKill = tgtinfo.DirectURL
		targetIDToKill = tgtinfo.DaemonID
		targetPortToKill = tgtinfo.DaemonPort
		break
	}

	tcmd, targs, err := kill(httpclient, targetURLToKill, targetPortToKill)
	time.Sleep(5 * time.Second) // FIXME: Deterministic wait for smap propogation

	proxyurl = nextProxyURL
	smap = getClusterMap(httpclient, t)
	if smap.ProxySI == nil {
		t.Errorf("Nil primary proxy")
	} else if smap.ProxySI.DaemonID != nextProxyID {
		t.Errorf("Incorrect Primary Proxy: %v, should be: %v", smap.ProxySI.DaemonID, nextProxyID)
	}
	if _, ok := smap.Smap[targetIDToKill]; ok {
		t.Errorf("Killed Target was not removed from the cluster map: %v", targetIDToKill)
	}

	// Remove proxyurl CLI Variable
	var idx int
	found := false
	for i, arg := range targs {
		if strings.Contains(arg, "-proxyurl") {
			idx = i
			found = true
		}
	}
	if found {
		targs = append(targs[:idx], targs[idx+1:]...)
	}

	// Restart that Target
	err = restore(httpclient, targetURLToKill, tcmd, targs, false)
	if err != nil {
		t.Errorf("Error restoring target: %v", err)
	}
	time.Sleep(5 * time.Second)
	// See that it successfully rejoins the cluster
	smap = getClusterMap(httpclient, t)
	if _, ok := smap.Smap[targetIDToKill]; !ok {
		t.Errorf("Restarted Target did not rejoin the cluster: %v", targetIDToKill)
	}

	pargs = append(pargs, "-proxyurl="+nextProxyURL)
	err = restore(httpclient, primaryProxyURL, pcmd, pargs, false)
	if err != nil {
		t.Errorf("Error restoring target: %v", err)
	}
}

func primaryproxyrejoin(t *testing.T) {
	// Get Smap
	smap := getClusterMap(httpclient, t)

	// hrwProxy to find next proxy
	delete(smap.Pmap, smap.ProxySI.DaemonID)
	nextProxyID, nextProxyURL, err := hrwProxy(&smap)
	if err != nil {
		t.Errorf("Error performing HRW: %v", err)
	}

	// Get the PID of the original priamary proxy
	proxypid, err := getPidOnPort(smap.ProxySI.DaemonPort)
	if err != nil {
		t.Errorf("Error getting proxy PID: %v", err)
	}
	pidint, err := strconv.Atoi(proxypid)
	if err != nil {
		t.Errorf("Error converting proxypid to int: %v", err)
	}

	// Run a mock target to unpause the original primary proxy during the confirmation stage.
	stopch := make(chan struct{})
	smapch := make(chan struct{}, 10)
	mocktgt := &PrimaryProxyRejoinMockTarget{pid: pidint, smapsynch: smapch}
	go runMockTarget(mocktgt, stopch, &smap)

	<-smapch
	// Allow smap propagation
	waitProgressBar("Propagating Smap: ", time.Duration(keepaliveseconds)*time.Second)

	// Pause the original primary proxy
	err = syscall.Kill(pidint, syscall.SIGSTOP)
	if err != nil {
		t.Errorf("Error pausing primary proxy: %v", err)
	}

	waitProgressBar("Primary Proxy Changing: ", time.Duration(4*keepaliveseconds)*time.Second)

	// The expected behavior is that the original primary proxy exists with an old version of the SMap, but the rest of the cluster is now using a newer Smap version

	oldproxyurl := proxyurl
	proxyurl = nextProxyURL
	newsmap := getClusterMap(httpclient, t)
	if newsmap.ProxySI == nil {
		t.Errorf("Nil primary proxy")
	} else if newsmap.ProxySI.DaemonID != nextProxyID {
		t.Errorf("Incorrect Primary Proxy: %v, should be: %v", newsmap.ProxySI.DaemonID, nextProxyID)
	}

	// Kill the mock target
	var v struct{}
	stopch <- v
	close(stopch)

	// Restart the original primary proxy; it's now out of sync with the rest of the cluster.
	pcmd, pargs, err := kill(httpclient, oldproxyurl, smap.ProxySI.DaemonPort)
	if err != nil {
		t.Errorf("Error killing Primary Proxy: %v", err)
	}
	time.Sleep(5 * time.Second)
	pargs = append(pargs, "-proxyurl="+nextProxyURL)
	err = restore(httpclient, oldproxyurl, pcmd, pargs, false)
	if err != nil {
		t.Errorf("Error restoring target: %v", err)
	}
	time.Sleep(5 * time.Second)
}

func minoritymismatchclustermap(t *testing.T) {
	f := func(i int) int {
		return i/4 + 1
	}
	mismatchclustermap(f, t)
}

func majoritymismatchclustermap(t *testing.T) {
	f := func(i int) int {
		return i/2 + 1
	}
	mismatchclustermap(f, t)
}

func mismatchclustermap(getnumtargets func(int) int, t *testing.T) {
	// Get Smap
	smap := getClusterMap(httpclient, t)
	smapversion := smap.Version

	// Update the version of the Smap for some of the targets
	smap.Version = smapversion + 1
	targetstoupdate := getnumtargets(len(smap.Smap) + len(smap.Pmap) - 1)
	jsbytes, err := json.Marshal(smap)
	if err != nil {
		t.Fatalf("Unexpected failure to marshal Smap: %v", err)
	}
	for _, target := range smap.Smap {
		if targetstoupdate == 0 {
			break
		}
		url := fmt.Sprintf("%s/%s/%s/%s?%s=%t", target.DirectURL, dfc.Rversion, dfc.Rdaemon, dfc.Rsyncsmap, dfc.URLParamAutoReb, false)
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(jsbytes))
		if err != nil {
			t.Errorf("Unexpected failure to create request: %v", err)
			return
		}
		r, err := httpclient.Do(req)
		if err != nil {
			t.Errorf("Unexpected failure to do http request: %v", err)
			return
		}
		defer func(r *http.Response) {
			if r.Body != nil {
				r.Body.Close()
			}
		}(r)
		_, err = ioutil.ReadAll(r.Body)
		if err != nil {
			t.Errorf("Unexpected failure to read response body: %v", err)
			return
		}
		targetstoupdate--
	}

	// hrwProxy to find next proxy
	delete(smap.Pmap, smap.ProxySI.DaemonID)
	nextProxyID, nextProxyURL, err := hrwProxy(&smap)
	if err != nil {
		t.Errorf("Error performing HRW: %v", err)
	}

	// Kill original primary proxy
	primaryProxyURL := smap.ProxySI.DirectURL
	cmd, args, err := kill(httpclient, primaryProxyURL, smap.ProxySI.DaemonPort)
	if err != nil {
		t.Errorf("Error killing Primary Proxy: %v", err)
	}
	// Wait the maxmimum time it should take to switch. It is longer for these tests, because elections
	// Might fail due to cluster map mismatch, but one should eventually succeed.
	waitProgressBar("Primary Proxy Changing: ", time.Duration(5*keepaliveseconds)*time.Second)

	// Check if the next proxy is the one we found from hrw
	proxyurl = nextProxyURL
	smap = getClusterMap(httpclient, t)
	if smap.ProxySI == nil {
		t.Errorf("Nil ProxySI in retrieved cluster map.")
	} else if smap.ProxySI.DaemonID != nextProxyID {
		t.Errorf("Incorrect Primary Proxy: %v, should be: %v", smap.ProxySI.DaemonID, nextProxyID)
	}

	args = append(args, "-proxyurl="+nextProxyURL)
	err = restore(httpclient, primaryProxyURL, cmd, args, false)
	if err != nil {
		t.Errorf("Error restoring proxy: %v", err)
	}
}

func putgetmultipleproxies(t *testing.T) {
	// Get Smap
	smap := getClusterMap(httpclient, t)

	primaryURL := smap.ProxySI.DirectURL
	var secondaryURL string
	for _, si := range smap.Pmap {
		if si.DaemonID != smap.ProxySI.DaemonID {
			secondaryURL = si.DirectURL
			break
		}
	}

	// Put, Get, Delete from 2 different proxies at once.
	errch := make(chan error, 2)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		errch <- singleProxyPutGetDelete(int64(baseseed), 100, primaryURL, testing.Verbose())
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		errch <- singleProxyPutGetDelete(int64(baseseed)+1, 100, secondaryURL, testing.Verbose())
	}()
	wg.Wait()
	close(errch)
	for err := range errch {
		if err != nil {
			t.Errorf("Error executing PutGetDelete loop: %v", err)
		}
	}

}

func singleProxyPutGetDelete(seed int64, nloops int, proxyurl string, verbose bool) error {
	random := rand.New(rand.NewSource(seed))
	for i := 0; i < nloops; i++ {
		if verbose && i%10 == 0 {
			fmt.Printf("Requests to %s: %d%% done\n", proxyurl, int(float64(i)/float64(nloops)*100))
		}
		reader, err := readers.NewRandReader(fileSize, true)
		if err != nil {
			return fmt.Errorf("Error creating reader: %v", err)
		}
		fname := client.FastRandomFilename(random, fnlen)
		keyname := fmt.Sprintf("%s/%s", multiproxydir, fname)
		err = client.Put(proxyurl, reader, clibucket, keyname, true)
		if err != nil {
			return fmt.Errorf("Error executing put: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
		client.Get(proxyurl, clibucket, keyname, nil /* wg */, nil /* errch */, true /* silent */, false /* validate */)
		time.Sleep(250 * time.Millisecond)
		err = client.Del(proxyurl, clibucket, keyname, nil /* wg */, nil /* errch */, true /* silent */)
		if err != nil {
			return fmt.Errorf("Error executing del: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
	}

	return nil
}

func setPrimaryProxy(t *testing.T) {
	// Get Smap
	smap := getClusterMap(httpclient, t)
	// Set primary proxy to each proxy, in a random order:
	for _, si := range smap.Pmap {
		fmt.Printf("Setting primary proxy to %v\n", si.DaemonID)
		resetPrimaryProxy(si.DaemonID, t)
		proxyurl = si.DirectURL
		time.Sleep(5 * time.Second)
		smap = getClusterMap(httpclient, t)
		if smap.ProxySI.DaemonID != si.DaemonID {
			t.Errorf("Primary Proxy is %v; should be %v", smap.ProxySI.DaemonID, si.DaemonID)
		}
	}
}

//=========
//
// Helpers
//
//=========
func hrwProxy(smap *dfc.Smap) (proxyid, proxyurl string, err error) {
	var max uint64

	for id, sinfo := range smap.Pmap {
		cs := xxhash.ChecksumString64S(id, HRWmLCG32)
		if cs > max {
			max = cs
			proxyid = sinfo.DaemonID
			proxyurl = sinfo.DirectURL
		}
	}

	if proxyid == "" {
		err = fmt.Errorf("Smap has no non-skipped proxies: Cannot perform HRW")
	}

	return
}

func kill(httpclient *http.Client, url, port string) (cmd string, args []string, err error) {
	cmd, args, err = getProcessOnPort(port)
	if err != nil {
		err = fmt.Errorf("Error retrieving process on port %v: %v", port, err)
		return
	}

	killurl := url + "/" + dfc.Rversion + "/" + dfc.Rdaemon + "?" + dfc.URLParamForce + "=true"
	msg := &dfc.ActionMsg{Action: dfc.ActShutdown}
	jsbytes, err := json.Marshal(&msg)
	if err != nil {
		err = fmt.Errorf("Unexpected failure to marshal VoteMessage: %v", err)
		return
	}

	req, err := http.NewRequest(http.MethodPut, killurl, bytes.NewBuffer(jsbytes))
	if err != nil {
		err = fmt.Errorf("Unexpected failure to create http request %s %s, err: %v", http.MethodPut, killurl, err)
		return
	}

	r, err := httpclient.Do(req)
	if err != nil {
		err = fmt.Errorf("Error sending HTTP request %v %v: %v", http.MethodGet, killurl, err)
		return
	}
	defer func() {
		if r.Body != nil {
			r.Body.Close()
		}
	}()
	_, err = dfc.ReadToNull(r.Body)
	if err != nil {
		err = fmt.Errorf("Error reading HTTP Body: %v", err)
		return
	}

	return
}

func restore(httpclient *http.Client, url, cmd string, args []string, asPrimary bool) error {
	// Restart it
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
		if err != nil {
			fmt.Printf("Error running command %v %v: %v (%v)\n", cmd, args, err, stderr.String())
		}
	}()

	pingurl := url + "/" + dfc.Rversion + "/" + dfc.Rhealth
	// Wait until the proxy is back up
	var i int
	for i = 0; i < maxpings; i++ {
		if ping(httpclient, pingurl) {
			break
		}
		time.Sleep(pollinterval)
	}
	if i == maxpings {
		return fmt.Errorf("Failed to restore: client did not respond to any of %v pings", maxpings)
	}

	time.Sleep(1 * time.Second) // Add time for the smap to propogate
	return nil
}

func ping(httpclient *http.Client, url string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), pingtimeout)
	defer cancel()
	r, err := ctxhttp.Get(ctx, httpclient, url)
	if err == nil {
		ioutil.ReadAll(r.Body)
		r.Body.Close()
	}

	return err == nil
}

func getPidOnPort(port string) (string, error) {
	syscallLSOF := "lsof"
	argsLSOF := []string{"-sTCP:LISTEN", "-i", ":" + port}
	commandLSOF := exec.Command(syscallLSOF, argsLSOF...)
	output, err := commandLSOF.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("Error executing LSOF command: %v", err)
	}
	// Find process listening on the port:
	line := strings.Split(string(output), "\n")[1] // The first line will always be output parameters
	fields := strings.Fields(line)
	pid := fields[1] // PID is the second output paremeter

	return pid, nil
}

func getProcessOnPort(port string) (command string, args []string, err error) {
	pid, err := getPidOnPort(port)
	if err != nil {
		err = fmt.Errorf("Error getting pid on port: %v", err)
		return
	}

	syscallPS := "ps"
	argsPS := []string{"-p", pid, "-o", "command"}
	commandPS := exec.Command(syscallPS, argsPS...)

	output, err := commandPS.CombinedOutput()
	if err != nil {
		err = fmt.Errorf("Error executing PS command: %v", err)
		return
	}
	line := strings.Split(string(output), "\n")[1] // The first line will always be output parameters
	fields := strings.Fields(line)
	if len(fields) == 0 {
		err = fmt.Errorf("No returned fields")
		return
	}
	return fields[0], fields[1:], nil
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

func resetPrimaryProxy(proxyid string, t *testing.T) {
	smap := getClusterMap(httpclient, t)
	url := smap.ProxySI.DirectURL + "/" + dfc.Rversion + "/" + dfc.Rcluster + "/" + dfc.Rproxy + "/" + proxyid
	req, err := http.NewRequest(http.MethodPut, url, nil)
	if err != nil {
		t.Errorf("Unexpected failure to create HTTP Request: %v", err)
	}
	r, err := httpclient.Do(req)
	if err != nil {
		t.Errorf("Unexpected failure to do HTTP Request: %v", err)
	}
	defer func() {
		if r.Body != nil {
			r.Body.Close()
		}
	}()
	_, err = ioutil.ReadAll(r.Body)
	if err != nil {
		t.Errorf("Unexpected failure to read HTTP Response Body: %v", err)
	}
	time.Sleep(5 * time.Second)
	return
}

//=============
//
// Mock Target
// This allows tests to execute actions at specific parts of the voting process.
//
//=============

const (
	mocktgtport = "8079"
)

type targetmocker interface {
	// /version/files handler
	filehdlr(w http.ResponseWriter, r *http.Request)
	// /version/daemon handler
	daemonhdlr(w http.ResponseWriter, r *http.Request)
	// /version/vote handler
	votehdlr(w http.ResponseWriter, r *http.Request)
}

func runMockTarget(mocktgt targetmocker, stopch chan struct{}, smap *dfc.Smap) {
	mux := http.NewServeMux()

	mux.HandleFunc("/"+dfc.Rversion+"/"+dfc.Rbuckets+"/", mocktgt.filehdlr)
	mux.HandleFunc("/"+dfc.Rversion+"/"+dfc.Robjects+"/", mocktgt.filehdlr)
	mux.HandleFunc("/"+dfc.Rversion+"/"+dfc.Rdaemon, mocktgt.daemonhdlr)
	mux.HandleFunc("/"+dfc.Rversion+"/"+dfc.Rdaemon+"/", mocktgt.daemonhdlr)
	mux.HandleFunc("/"+dfc.Rversion+"/"+dfc.Rvote+"/", mocktgt.votehdlr)
	mux.HandleFunc("/"+dfc.Rversion+"/"+dfc.Rhealth, func(w http.ResponseWriter, r *http.Request) {})
	s := &http.Server{Addr: ":" + mocktgtport, Handler: mux}

	registerMockTarget(mocktgt, smap)
	go s.ListenAndServe()
	<-stopch
	unregisterMockTarget(mocktgt)
	s.Shutdown(context.Background())
}

func registerMockTarget(mocktgt targetmocker, smap *dfc.Smap) error {
	// Borrow a random DaemonInfo to register with:
	var (
		jsbytes []byte
		err     error
	)

	for _, di := range smap.Smap {
		outboundIP := getOutboundIP().String()

		di.DaemonID = "MOCK"
		di.DaemonPort = mocktgtport
		di.NodeIPAddr = outboundIP
		di.DirectURL = "http://" + outboundIP + ":" + mocktgtport
		jsbytes, err = json.Marshal(di)
		if err != nil {
			return err
		}
		break
	}

	url := proxyurl + "/" + dfc.Rversion + "/" + dfc.Rcluster
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsbytes))
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
	if err != nil {
		return err
	}

	return nil
}

func unregisterMockTarget(mocktgt targetmocker) error {
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
	if err != nil {
		return err
	}

	return nil
}

//=====================
//
// Concrete Mock Target
//
//=====================

type PrimaryProxyRejoinMockTarget struct {
	pid       int
	smapsynch chan struct{}
}

func (*PrimaryProxyRejoinMockTarget) filehdlr(w http.ResponseWriter, r *http.Request) {
	// Ignore all file requests
	return
}

func (p *PrimaryProxyRejoinMockTarget) daemonhdlr(w http.ResponseWriter, r *http.Request) {
	// Treat all daemonhdlr requests as smap syncs: notify on reciept
	var v struct{}
	p.smapsynch <- v
	return
}

func (p *PrimaryProxyRejoinMockTarget) votehdlr(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		w.Write([]byte(dfc.VoteYes))
	case http.MethodPut:
		// unpause target
		syscall.Kill(p.pid, syscall.SIGCONT)
	}
}
