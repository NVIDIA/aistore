package dfc_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/dfc"
)

var (
	primarystartupTests = []Test{
		Test{"Basic", primaryStartupBasic},
		Test{"Multiple", primaryStartupMultipleFailures},
		Test{"Fast Restore", primaryStartupFastRestore},
		Test{"Non Primary", primaryStartupNonPrimary},
		Test{"Vote Retry", primaryStartupVoteRetry},
	}
)

func Test_primarystartup(t *testing.T) {
	originalProxyID := canRunMultipleProxyTests(t)
	originalProxyURL := proxyurl

	for _, test := range primarystartupTests {
		t.Run(test.name, test.method)
		if t.Failed() && abortonerr {
			t.FailNow()
		}
	}

	time.Sleep(10 * time.Second)
	resetPrimaryProxy(originalProxyID, t)
	proxyurl = originalProxyURL
}

//==========
//
// Subtests
//
//==========

func primaryStartupBasic(t *testing.T) {
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
	primaryProxyID := smap.ProxySI.DaemonID
	cmd, args, err := kill(httpclient, primaryProxyURL, smap.ProxySI.DaemonPort)
	if err != nil {
		t.Errorf("Error killing Primary Proxy: %v", err)
	}
	// Wait the maxmimum time it should take to switch.
	waitProgressBar("Primary Proxy Changing: ", time.Duration(2*keepaliveSeconds)*time.Second)

	err = restore(httpclient, primaryProxyURL, cmd, args, true)
	if err != nil {
		t.Errorf("Error restoring proxy: %v", err)
	}

	waitProgressBar("Proxy Startup: ", time.Duration(startupGetSmapDelay+5)*time.Second)

	// Check if the previous primary proxy correctly rejoined the cluster
	proxyurl = nextProxyURL
	smap = getClusterMap(httpclient, t)
	if smap.ProxySI.DaemonID != nextProxyID {
		t.Errorf("Incorrect Primary Proxy: %v, should be: %v", smap.ProxySI.DaemonID, nextProxyID)
	}
	if _, ok := smap.Pmap[primaryProxyID]; !ok {
		t.Errorf("Previous Primary Proxy did not rejoin the cluster.")
	}
}

func primaryStartupFastRestore(t *testing.T) {
	// Get Smap
	smap := getClusterMap(httpclient, t)

	// Kill original primary proxy
	primaryProxyURL := smap.ProxySI.DirectURL
	primaryProxyID := smap.ProxySI.DaemonID
	cmd, args, err := kill(httpclient, primaryProxyURL, smap.ProxySI.DaemonPort)
	if err != nil {
		t.Errorf("Error killing Primary Proxy: %v", err)
	}
	time.Sleep(2 * time.Second)
	err = restore(httpclient, primaryProxyURL, cmd, args, true)
	if err != nil {
		t.Errorf("Error restoring proxy: %v", err)
	}

	waitProgressBar("Proxy Startup: ", time.Duration(startupGetSmapDelay+5)*time.Second)

	// Check if the previous primary proxy correctly remained primary
	smap = getClusterMap(httpclient, t)
	if smap.ProxySI.DaemonID != primaryProxyID {
		t.Errorf("Incorrect Primary Proxy: %v, should be: %v", smap.ProxySI.DaemonID, primaryProxyID)
	}
}

func primaryStartupMultipleFailures(t *testing.T) {
	// Get Smap
	smap := getClusterMap(httpclient, t)
	if len(smap.Pmap) < 3 {
		t.Errorf("Canot run Failback_multiple_failures with %d proxies; must be at least %d", len(smap.Pmap), 3)
	}

	// hrwProxy to find next proxy
	delete(smap.Pmap, smap.ProxySI.DaemonID)
	nextProxyID, nextProxyURL, err := hrwProxy(&smap)
	if err != nil {
		t.Errorf("Error performing HRW: %v", err)
	}

	// Kill original primary proxy, and the next proxy in the list that isn't the primary.
	primaryProxyURL, primaryProxyID := smap.ProxySI.DirectURL, smap.ProxySI.DaemonID
	pcmd, pargs, err := kill(httpclient, primaryProxyURL, smap.ProxySI.DaemonPort)
	if err != nil {
		t.Errorf("Error killing Primary Proxy: %v", err)
	}

	secondProxyURL, secondProxyPort, secondProxyID := "", "", ""
	// Select a random proxy
	for pid, pxyinfo := range smap.Pmap {
		if pid == nextProxyID || pid == primaryProxyID {
			// Do not choose the next primary in line, or this proxy
			// This is because the system currently cannot recover if the next proxy in line is
			// also killed.
			continue
		}
		secondProxyURL = pxyinfo.DirectURL
		secondProxyPort = pxyinfo.DaemonPort
		secondProxyID = pxyinfo.DaemonID
		break
	}
	spcmd, spargs, err := kill(httpclient, secondProxyURL, secondProxyPort)
	if err != nil {
		t.Errorf("Error killing Second Proxy: %v", err)
	}

	// Wait the maxmimum time it should take to switch.
	waitProgressBar("Primary Proxy Changing: ", time.Duration(2*keepaliveSeconds)*time.Second)

	// Restore the killed proxies
	err = restore(httpclient, secondProxyURL, spcmd, spargs, true)
	if err != nil {
		t.Errorf("Error restoring second proxy: %v", err)
	}
	waitProgressBar("Proxy Startup (second proxy): ", time.Duration(startupGetSmapDelay+5)*time.Second)
	err = restore(httpclient, primaryProxyURL, pcmd, pargs, true)
	if err != nil {
		t.Errorf("Error restoring first proxy: %v", err)
	}

	waitProgressBar("Proxy Startup (first proxy): ", time.Duration(startupGetSmapDelay+5)*time.Second)

	// Check if the killed proxies successfully rejoin the cluster.
	proxyurl = nextProxyURL
	smap = getClusterMap(httpclient, t)
	if smap.ProxySI.DaemonID != nextProxyID {
		t.Errorf("Incorrect Primary Proxy: %v, should be: %v", smap.ProxySI.DaemonID, nextProxyID)
	}
	if _, ok := smap.Pmap[primaryProxyID]; !ok {
		t.Errorf("Previous Primary Proxy did not rejoin the cluster.")
	}
	if _, ok := smap.Pmap[secondProxyID]; !ok {
		t.Errorf("Second Proxy did not rejoin the cluster.")
	}
}

func primaryStartupNonPrimary(t *testing.T) {
	// Get Smap
	smap := getClusterMap(httpclient, t)

	// Kill original primary proxy
	nonPrimaryURL, nonPrimaryID, nonPrimaryPort := "", "", ""
	for sid, si := range smap.Pmap {
		if sid == smap.ProxySI.DaemonID {
			// The point of this test is to kill a non-primary proxy.
			continue
		}

		nonPrimaryID = sid
		nonPrimaryURL = si.DirectURL
		nonPrimaryPort = si.DaemonPort
		break
	}
	cmd, args, err := kill(httpclient, nonPrimaryURL, nonPrimaryPort)
	if err != nil {
		t.Errorf("Error killing proxy: %v", err)
	}
	time.Sleep(2 * time.Second)
	err = restore(httpclient, nonPrimaryURL, cmd, args, true)
	if err != nil {
		t.Errorf("Error restoring proxy: %v", err)
	}

	waitProgressBar("Proxy Startup: ", time.Duration(startupGetSmapDelay+5)*time.Second)

	// Check if the previous primary proxy correctly remained primary
	smap = getClusterMap(httpclient, t)
	if _, ok := smap.Pmap[nonPrimaryID]; !ok {
		t.Errorf("Proxy did not rejoin the cluster.")
	}
}

func primaryStartupVoteRetry(t *testing.T) {
	// Get Smap
	smap := getClusterMap(httpclient, t)

	// hrwProxy to find next proxy
	delete(smap.Pmap, smap.ProxySI.DaemonID)
	nextProxyID, nextProxyURL, err := hrwProxy(&smap)
	if err != nil {
		t.Errorf("Error performing HRW: %v", err)
	}

	stopch := make(chan struct{})
	errch := make(chan error, 10)
	mocktgt := &primaryStartupVoteRetryMockTarget{
		vote:  true,
		errch: errch,
	}
	go runMockTarget(mocktgt, stopch, &smap)

	// Kill original primary proxy
	primaryProxyURL := smap.ProxySI.DirectURL
	primaryProxyID := smap.ProxySI.DaemonID
	cmd, args, err := kill(httpclient, primaryProxyURL, smap.ProxySI.DaemonPort)
	if err != nil {
		t.Errorf("Error killing Primary Proxy: %v", err)
	}
	// Wait the maxmimum time it should take to switch.
	waitProgressBar("Primary Proxy Changing: ", time.Duration(2*keepaliveSeconds)*time.Second)

	err = restore(httpclient, primaryProxyURL, cmd, args, true)
	if err != nil {
		t.Errorf("Error restoring proxy: %v", err)
	}

	waitProgressBar("Proxy Startup: ", time.Duration(startupGetSmapDelay+5)*time.Second)

	// Check if the previous primary proxy has not yet rejoined the cluster
	// It should be waiting for the mock target to return vote=false
	proxyurl = nextProxyURL
	smap = getClusterMap(httpclient, t)
	if smap.ProxySI.DaemonID != nextProxyID {
		t.Errorf("Incorrect Primary Proxy: %v, should be: %v", smap.ProxySI.DaemonID, nextProxyID)
	}
	if _, ok := smap.Pmap[primaryProxyID]; ok {
		t.Errorf("Previous Primary Proxy rejoined the cluster during a vote")
	}

	mocktgt.vote = false

	waitProgressBar("Proxy Rejoin: ", time.Duration(startupGetSmapDelay+5)*time.Second)
	// Check if the previous primary proxy has now rejoined the cluster
	// Mock target is now returning vote=false
	smap = getClusterMap(httpclient, t)
	if smap.ProxySI.DaemonID != nextProxyID {
		t.Errorf("Incorrect Primary Proxy: %v, should be: %v", smap.ProxySI.DaemonID, nextProxyID)
	}
	if _, ok := smap.Pmap[primaryProxyID]; !ok {
		t.Errorf("Previous Primary Proxy did not rejoin the cluster")
	}

	// Kill the mock target
	var v struct{}
	stopch <- v
	close(stopch)
	select {
	case err := <-errch:
		t.Errorf("Mock Target Error: %v", err)
	default:
	}
	time.Sleep(time.Duration(keepaliveSeconds) * time.Second)
}

//=======================
//
// Vote Retry Mock Target
//
//=======================

type primaryStartupVoteRetryMockTarget struct {
	vote  bool
	errch chan error
}

func (*primaryStartupVoteRetryMockTarget) filehdlr(w http.ResponseWriter, r *http.Request) {
	// Ignore all file requests
	return
}

func (p *primaryStartupVoteRetryMockTarget) daemonhdlr(w http.ResponseWriter, r *http.Request) {
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

func (p *primaryStartupVoteRetryMockTarget) votehdlr(w http.ResponseWriter, r *http.Request) {
	// Always vote yes.
	w.Write([]byte(dfc.VoteYes))
}
