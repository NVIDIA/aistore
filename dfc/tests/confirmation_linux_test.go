package dfc_test

import (
	"testing"
	"time"
)

var (
	confirmationTests = []Test{
		Test{"Basic", confirmation_basic},
		Test{"Multiple", confirmation_multiple_failures},
		Test{"Fast Restore", confirmation_fast_restore},
		Test{"Non Primary", confirmation_non_primary},
	}
)

func Test_confirmation(t *testing.T) {
	originalproxyid := canRunMultipleProxyTests(t)
	originalproxyurl := proxyurl

	for _, test := range confirmationTests {
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

func confirmation_basic(t *testing.T) {
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
	waitProgressBar("Primary Proxy Changing: ", time.Duration(2*keepaliveseconds)*time.Second)

	err = restore(httpclient, primaryProxyURL, cmd, args, true)
	if err != nil {
		t.Errorf("Error restoring proxy: %v", err)
	}

	waitProgressBar("Proxy Suspect Time: ", time.Duration(startupconfirmationseconds+5)*time.Second)

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

func confirmation_fast_restore(t *testing.T) {
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

	waitProgressBar("Proxy Suspect Time: ", time.Duration(startupconfirmationseconds+5)*time.Second)

	// Check if the previous primary proxy correctly remained primary
	smap = getClusterMap(httpclient, t)
	if smap.ProxySI.DaemonID != primaryProxyID {
		t.Errorf("Incorrect Primary Proxy: %v, should be: %v", smap.ProxySI.DaemonID, primaryProxyID)
	}
	// FIXME: Currently, map will be out of sync after running this test.
	// The proposed solution is sending back the cluster map when the version is higher.
	// For now, we fix this by resetting the primary proxy to another proxy/target

	for sid, si := range smap.Pmap {
		if sid == primaryProxyID {
			continue
		}
		resetPrimaryProxy(sid, t)
		proxyurl = si.DirectURL
		time.Sleep(5 * time.Second)
	}
}

func confirmation_multiple_failures(t *testing.T) {
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
	waitProgressBar("Primary Proxy Changing: ", time.Duration(2*keepaliveseconds)*time.Second)

	// Restore the killed proxies
	err = restore(httpclient, secondProxyURL, spcmd, spargs, true)
	if err != nil {
		t.Errorf("Error restoring second proxy: %v", err)
	}
	waitProgressBar("Startup Suspect Time (second proxy): ", time.Duration(startupconfirmationseconds+5)*time.Second)
	err = restore(httpclient, primaryProxyURL, pcmd, pargs, true)
	if err != nil {
		t.Errorf("Error restoring first proxy: %v", err)
	}

	waitProgressBar("Startup Suspect Time (first proxy): ", time.Duration(startupconfirmationseconds+5)*time.Second)

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
func confirmation_non_primary(t *testing.T) {
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

	waitProgressBar("Proxy Suspect Time: ", time.Duration(startupconfirmationseconds+5)*time.Second)

	// Check if the previous primary proxy correctly remained primary
	smap = getClusterMap(httpclient, t)
	if _, ok := smap.Pmap[nonPrimaryID]; !ok {
		t.Errorf("Proxy did not rejoin the cluster.")
	}
}
