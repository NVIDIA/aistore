// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/golang/glog"
)

const (
	VoteYes Vote = "YES"
	VoteNo  Vote = "NO"

	// xaction constant for Election
	ActElection      = "election"
	ProxyPingTimeout = 100 * time.Millisecond
)

type (
	Vote string

	VoteRecord struct {
		Candidate string    `json:"candidate"`
		Primary   string    `json:"primary"`
		Smap      Smap      `json:"smap"`
		lbmap     lbmap     `json:"lbmap"`
		StartTime time.Time `json:"starttime"`
		Initiator string    `json:"initiator"`
	}

	VoteInitiation VoteRecord
	VoteResult     VoteRecord

	VoteMessage struct {
		Record VoteRecord `json:"voterecord"`
	}

	VoteInitiationMessage struct {
		Request VoteInitiation `json:"voteinitiation"`
	}

	VoteResultMessage struct {
		Result VoteResult `json:"voteresult"`
	}

	// ErrPair contains an Error and the Daemon which caused the error
	ErrPair struct {
		err      error
		daemonID string
	}

	voteResult struct {
		yes      bool
		daemonID string
		err      error
	}
)

//==========
//
// Handlers
//
//==========

// "/"+Rversion+"/"+Rvote+"/"
func (t *targetrunner) votehdlr(w http.ResponseWriter, r *http.Request) {
	apitems := t.restAPIItems(r.URL.Path, 5)
	if apitems = t.checkRestAPI(w, r, apitems, 1, Rversion, Rvote); apitems == nil {
		return
	}

	switch {
	case r.Method == http.MethodGet && apitems[0] == Rproxy:
		t.httpproxyvote(w, r)
	case r.Method == http.MethodPut && apitems[0] == Rvoteres:
		t.httpsetprimaryproxy(w, r)
	default:
		s := fmt.Sprintf("Invalid HTTP Method: %v %s", r.Method, r.URL.Path)
		t.invalmsghdlr(w, r, s)
	}
}

// "/"+Rversion+"/"+Rvote+"/"
func (p *proxyrunner) votehdlr(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rvote); apitems == nil {
		return
	}

	switch {
	case r.Method == http.MethodGet && apitems[0] == Rproxy:
		p.httpproxyvote(w, r)
	case r.Method == http.MethodPut && apitems[0] == Rvoteres:
		p.primary = false
		p.httpsetprimaryproxy(w, r)
	case r.Method == http.MethodPut && apitems[0] == Rvoteinit:
		p.httpRequestNewPrimary(w, r)
	default:
		s := fmt.Sprintf("Invalid HTTP Method: %v %s", r.Method, r.URL.Path)
		p.invalmsghdlr(w, r, s)
	}
}

// GET /Rversion/Rvote
func (h *httprunner) httpproxyvote(w http.ResponseWriter, r *http.Request) {
	apitems := h.restAPIItems(r.URL.Path, 5)
	if apitems = h.checkRestAPI(w, r, apitems, 1, Rversion, Rvote); apitems == nil {
		return
	}

	msg := VoteMessage{}
	err := h.readJSON(w, r, &msg)
	if err != nil {
		s := fmt.Sprintf("Error reading Vote Request body: %v", err)
		h.invalmsghdlr(w, r, s)
		return
	}
	candidate := msg.Record.Candidate
	if candidate == "" {
		h.invalmsghdlr(w, r, fmt.Sprintln("Cannot request vote without Candidate field"))
		return
	}

	smapLock.Lock() // ==================
	v := h.smap.version()
	currPrimaryID := h.smap.ProxySI.DaemonID
	if v < msg.Record.Smap.version() {
		glog.Warningf("VoteRecord Smap Version (%v) is newer than local Smap (%v), updating Smap\n", msg.Record.Smap.version(), v)
		h.smap = &msg.Record.Smap
	} else if v > h.smap.version() {
		smapLock.Unlock()
		glog.Errorf("VoteRecord smap Version (%v) is older than local Smap (%v), voting No\n", msg.Record.Smap.version(), v)
		_, err = w.Write([]byte(VoteNo))
		if err != nil {
			h.invalmsghdlr(w, r, fmt.Sprintf("Error writing no vote: %v", err))
		}
		return
	}
	pi := h.smap.getProxy(candidate)
	smapLock.Unlock() // ==================

	if pi == nil {
		h.invalmsghdlr(w, r, fmt.Sprintf("Candidate not present in proxy smap: %s (%v)", candidate, h.smap.Pmap))
		return
	}

	lbmapLock.Lock()
	vlb := h.lbmap.version()
	if vlb < msg.Record.lbmap.version() {
		glog.Warningf("VoteRecord lbmap Version (%v) is newer than local lbmap (%v), updating Smap\n", msg.Record.lbmap.version(), vlb)
		h.lbmap = &msg.Record.lbmap
	}
	lbmapLock.Unlock()

	vote, err := h.voteOnProxy(pi.DaemonID, currPrimaryID)
	if err != nil {
		h.invalmsghdlr(w, r, err.Error())
		return
	}

	if vote {
		_, err = w.Write([]byte(VoteYes))
		if err != nil {
			s := fmt.Sprintf("Error writing yes vote: %v", err)
			h.invalmsghdlr(w, r, s)
		}
	} else {
		_, err = w.Write([]byte(VoteNo))
		if err != nil {
			s := fmt.Sprintf("Error writing no vote: %v", err)
			h.invalmsghdlr(w, r, s)
		}
	}
}

// PUT "/"+Rversion+"/"+Rvote+"/"+Rvoteres
func (h *httprunner) httpsetprimaryproxy(w http.ResponseWriter, r *http.Request) {
	apitems := h.restAPIItems(r.URL.Path, 5)
	if apitems = h.checkRestAPI(w, r, apitems, 1, Rversion, Rvote); apitems == nil {
		return
	}

	msg := VoteResultMessage{}
	err := h.readJSON(w, r, &msg)
	if err != nil {
		s := fmt.Sprintf("Error reading Vote Message body: %v", err)
		h.invalmsghdlr(w, r, s)
		return
	}

	vr := msg.Result
	glog.Infof("%v received vote result: %v\n", h.si.DaemonID, vr)

	err = h.setPrimaryProxyL(vr.Candidate, vr.Primary, false)
	if err != nil {
		s := fmt.Sprintf("Error setting primary proxy: %v", err)
		h.invalmsghdlr(w, r, s)
		return
	}
}

// PUT "/"+Rversion+"/"+Rvote+"/"+Rvoteinit
func (p *proxyrunner) httpRequestNewPrimary(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rvote); apitems == nil {
		return
	}

	msg := VoteInitiationMessage{}
	err := p.readJSON(w, r, &msg)
	if err != nil {
		s := fmt.Sprintf("Error reading Vote Request body: %v", err)
		p.invalmsghdlr(w, r, s)
		return
	}

	// If the passed Smap is newer, update our Smap. If it is older, update it.
	currPrimary := p.smap.ProxySI.DaemonID
	currPrimaryURL := p.smap.ProxySI.DirectURL
	if msg.Request.Smap.version() > p.smap.version() {
		p.smap = &msg.Request.Smap
	}

	psi, errstr := HrwProxy(p.smap, currPrimary)
	if errstr != "" {
		s := fmt.Sprintf("Error preforming HRW: %s", errstr)
		p.invalmsghdlr(w, r, s)
		return
	}

	// Only continue the election if this proxy is actually the next in line
	if psi.DaemonID != p.si.DaemonID {
		return
	}

	vr := &VoteRecord{
		Candidate: msg.Request.Candidate,
		Primary:   msg.Request.Primary,
		StartTime: time.Now(),
		Initiator: p.si.DaemonID,
	}
	p.smap.copyL(&vr.Smap)
	p.lbmap.copyL(&vr.lbmap)

	// The election should be started in a goroutine, as it must not hang the http handler
	go p.proxyElection(vr, currPrimaryURL)
}

//===================
//
// Election Functions
//
//===================

func (p *proxyrunner) proxyElection(vr *VoteRecord, currPrimaryURL string) {
	xele := p.xactinp.renewElection(p, vr)
	if xele == nil {
		glog.Infoln("An election is already in progress, returning.")
		return
	}
	defer func() {
		xele.etime = time.Now()
		glog.Infoln(xele.tostring())
		p.xactinp.del(xele.id)
	}()
	if p.primary {
		glog.Infoln("Already in Primary state.")
		return
	}
	// First, ping current proxy with a short timeout: (Primary? State)
	url := currPrimaryURL + "/" + Rversion + "/" + Rhealth
	proxyup, err := p.pingWithTimeout(url, ctx.config.Timeout.ProxyPing)
	if proxyup {
		// Move back to Idle state
		glog.Infoln("Moving back to Idle state")
		return
	}
	glog.Infof("%v: primary proxy %v is confirmed down\n", p.si.DaemonID, currPrimaryURL)
	glog.Infoln("Moving to Election state")
	// Begin Election State
	elected, votingErrors := p.electAmongProxies(vr)
	if !elected {
		// Move back to Idle state
		glog.Infoln("Moving back to Idle state")
		return
	}
	glog.Infoln("Moving to Election2 State")
	// Begin Election2 State
	confirmationErrors := p.confirmElectionVictory(vr)

	// Check for errors that did not occurr in the voting stage:
	for sid := range confirmationErrors {
		if _, ok := votingErrors[sid]; !ok {
			// A node errored while confirming that did not error while voting:
			glog.Errorf("An error occurred confirming the election with a node that was healthy when voting: %v", err)
		}
	}

	glog.Infoln("Moving to Primary state")
	// Begin Primary State
	p.becomePrimaryProxy(vr.Primary /* proxyidToRemove */)
}

func (p *proxyrunner) electAmongProxies(vr *VoteRecord) (winner bool, errors map[string]bool) {
	// Simple Majority Vote
	resch := p.requestVotes(vr)
	errors = make(map[string]bool)
	y, n := 0, 0

	for res := range resch {
		if res.err != nil {
			glog.Warningf("Error response from %s, err: %v", res.daemonID, res.err)
			errors[res.daemonID] = true
			n++
		} else {
			if res.yes {
				y++
			} else {
				n++
			}
		}
	}

	winner = y > n || (y+n == 0) // No Votes: Default Winner
	glog.Infof("Vote Results:\n Y: %v, N:%v\n Victory: %v\n", y, n, winner)
	return
}

func (p *proxyrunner) requestVotes(vr *VoteRecord) chan voteResult {
	smapLock.Lock()
	defer smapLock.Unlock()
	chansize := p.smap.count() + p.smap.countProxies() - 1
	resch := make(chan voteResult, chansize)

	msg := VoteMessage{Record: *vr}
	jsbytes, err := json.Marshal(&msg)
	assert(err == nil, err)
	urlfmt := fmt.Sprintf("%%s/%s/%s/%s?%s=%s", Rversion, Rvote, Rproxy, URLParamPrimaryCandidate, p.si.DaemonID)
	cb := func(res callResult) {
		if res.err != nil {
			e := fmt.Errorf("Error reading response from %s(%s): %v", res.si.DaemonID, res.si.DirectURL, res.err)
			resch <- voteResult{yes: false, daemonID: res.si.DaemonID, err: e}
		} else {
			resch <- voteResult{yes: (VoteYes == Vote(res.outjson)), daemonID: res.si.DaemonID, err: nil}
		}
	}

	p.broadcast(urlfmt, http.MethodGet, jsbytes, p.smap, cb, ctx.config.Timeout.CplaneOperation)
	close(resch)
	return resch
}

func (p *proxyrunner) confirmElectionVictory(vr *VoteRecord) map[string]bool {
	result := &VoteResult{
		Candidate: vr.Candidate,
		Primary:   vr.Primary,
		Smap:      vr.Smap,
		lbmap:     vr.lbmap,
		StartTime: time.Now(),
		Initiator: p.si.DaemonID,
	}

	smapLock.Lock()
	defer smapLock.Unlock()

	errch := make(chan ErrPair, p.smap.count()+p.smap.countProxies()-1)
	msg := VoteResultMessage{Result: *result}
	jsbytes, err := json.Marshal(&msg)
	assert(err == nil, err)
	urlfmt := fmt.Sprintf("%%s/%s/%s/%s", Rversion, Rvote, Rvoteres)
	cb := func(res callResult) {
		if res.err != nil {
			e := fmt.Errorf("Error committing result for %s(%s): %v", res.si.DaemonID, res.si.DirectURL, res.err)
			errch <- ErrPair{err: e, daemonID: res.si.DaemonID}
		}
	}

	p.broadcast(urlfmt, http.MethodPut, jsbytes, p.smap, cb, ctx.config.Timeout.CplaneOperation)
	close(errch)

	errors := make(map[string]bool)
	for errpair := range errch {
		if errpair.err != nil {
			glog.Warningf("Error broadcasting election victory: %v", errpair)
			errors[errpair.daemonID] = true
		}
	}
	return errors
}

func (p *proxyrunner) becomePrimaryProxy(proxyidToRemove string) {
	smapLock.Lock()
	defer smapLock.Unlock()

	p.primary = true
	if proxyidToRemove != "" {
		p.smap.delProxy(proxyidToRemove)
	}

	psi := p.smap.getProxy(p.si.DaemonID)
	// If psi == nil, then this proxy is not currently in the local cluster map. This should never happen.
	assert(psi != nil, "This proxy should always exist in the local Smap")
	psi.Primary = true
	p.smap.ProxySI = psi

	// Version is increased by 100 to make a clear distinction between smap versions before and after the primary proxy is updated.
	p.smap.Version += 100

	ctx.config.Proxy.Primary.ID = psi.DaemonID
	ctx.config.Proxy.Primary.URL = psi.DirectURL
	err := LocalSave(clivars.conffile, ctx.config)
	if err != nil {
		glog.Errorf("Error writing config file: %v", err)
	}
	if errstr := p.savesmapconf(); errstr != "" {
		glog.Errorf(errstr)
	}

	msg := &ActionMsg{Action: ActNewPrimary}
	pair := &revspair{p.smap.cloneU(), msg}
	p.metasyncer.sync(false, pair)
}

// Caller must lock smapLock
func (p *proxyrunner) becomeNonPrimaryProxy() {
	p.primary = false
	psi := p.smap.getProxy(p.si.DaemonID)
	if psi != nil {
		psi.Primary = false
	}
}

func (p *proxyrunner) onPrimaryProxyFailure() {
	glog.Infof("%v: primary proxy (%v @ %v) Failed\n", p.si.DaemonID, p.smap.ProxySI.DaemonID, p.smap.ProxySI.DirectURL)
	if p.smap.countProxies() <= 1 {
		glog.Warningf("No additional proxies to request vote from")
		return
	}
	nextPrimaryProxy, errstr := HrwProxy(p.smap, p.smap.ProxySI.DaemonID)
	if errstr != "" {
		glog.Errorf("Failed to execute hrwProxy after primary proxy Failure: %v", errstr)
		return
	}

	currPrimaryURL := p.smap.ProxySI.DirectURL
	if nextPrimaryProxy.DaemonID == p.si.DaemonID {
		// If this proxy is the next primary proxy candidate, it starts the election directly.
		vr := &VoteRecord{
			Candidate: nextPrimaryProxy.DaemonID,
			Primary:   p.smap.ProxySI.DaemonID,
			StartTime: time.Now(),
			Initiator: p.si.DaemonID,
		}
		p.smap.copyL(&vr.Smap)
		p.lbmap.copyL(&vr.lbmap)
		p.proxyElection(vr, currPrimaryURL)
	} else {
		glog.Infof("%v: Requesting Election from %v", p.si.DaemonID, nextPrimaryProxy.DaemonID)
		vr := &VoteInitiation{
			Candidate: nextPrimaryProxy.DaemonID,
			Primary:   p.smap.ProxySI.DaemonID,
			StartTime: time.Now(),
			Initiator: p.si.DaemonID,
		}
		p.smap.copyL(&vr.Smap)
		p.lbmap.copyL(&vr.lbmap)
		p.sendElectionRequest(vr, nextPrimaryProxy)
	}
}

func (t *targetrunner) onPrimaryProxyFailure() {
	glog.Infof("%v: primary proxy (%v @ %v) Failed\n", t.si.DaemonID, t.smap.ProxySI.DaemonID, t.smap.ProxySI.DirectURL)

	nextPrimaryProxy, errstr := HrwProxy(t.smap, t.smap.ProxySI.DaemonID)
	if errstr != "" {
		glog.Errorf("Failed to execute hrwProxy after primary proxy Failure: %v", errstr)
	}

	if nextPrimaryProxy == nil {
		// There is only one proxy, so we cannot select a next in line
		glog.Warningf("primary proxy failed, but there are no candidates to fall back on.")
		return
	}

	vr := &VoteInitiation{
		Candidate: nextPrimaryProxy.DaemonID,
		Primary:   t.smap.ProxySI.DaemonID,
		StartTime: time.Now(),
		Initiator: t.si.DaemonID,
	}
	t.smap.copyL(&vr.Smap)
	t.lbmap.copyL(&vr.lbmap)
	t.sendElectionRequest(vr, nextPrimaryProxy)
}

func (h *httprunner) sendElectionRequest(vr *VoteInitiation, nextPrimaryProxy *proxyInfo) {
	url := nextPrimaryProxy.DirectURL + "/" + Rversion + "/" + Rvote + "/" + Rvoteinit
	msg := VoteInitiationMessage{Request: *vr}
	jsbytes, err := json.Marshal(&msg)
	assert(err == nil, err)

	res := h.call(nil, &nextPrimaryProxy.daemonInfo, url, http.MethodPut, jsbytes)
	if res.err != nil {
		if IsErrConnectionRefused(err) {
			for i := 0; i < 2; i++ {
				time.Sleep(time.Second)
				res = h.call(nil, &nextPrimaryProxy.daemonInfo, url, http.MethodPut, jsbytes)
				if res.err == nil {
					break
				}
			}
		}
		glog.Errorf("Failed to request election from next primary proxy: %v", res.err)
		return
	}
}

func (h *httprunner) voteOnProxy(daemonID, currPrimaryID string) (bool, error) {
	// First: Check last keepalive timestamp. If the proxy was recently successfully reached,
	// this will always vote no, as we believe the original proxy is still alive.
	lastKeepaliveTime := h.kalive.getTimestamp(currPrimaryID)
	timeSinceLastKalive := time.Since(lastKeepaliveTime)
	if timeSinceLastKalive < ctx.config.Periodic.KeepAliveTime/2 {
		// KeepAliveTime/2 is the expected amount time since the last keepalive was sent
		return false, nil
	}

	// Second: Vote according to whether or not the candidate is the Highest Random Weight remaining
	// in the Smap
	hrwmax, errstr := HrwProxy(h.smap, currPrimaryID)
	if errstr != "" {
		return false, fmt.Errorf("Error executing HRW: %v", errstr)
	}
	return hrwmax.DaemonID == daemonID, nil
}

func (h *httprunner) setPrimaryProxyAndSmapL(smap *Smap) error {
	smapLock.Lock()
	defer smapLock.Unlock()
	h.smap = smap
	return h.setPrimaryProxy(smap.ProxySI.DaemonID, "" /* primaryToRemove */, false /* prepare */)
}

func (h *httprunner) setPrimaryProxyL(newPrimaryProxy, primaryToRemove string, prepare bool) error {
	smapLock.Lock()
	defer smapLock.Unlock()
	return h.setPrimaryProxy(newPrimaryProxy, primaryToRemove, prepare)
}

// setPrimaryProxy sets the primary proxy to the proxy in the cluster map with the ID newPrimaryProxy.
// removes primaryToRemove from the cluster map, if primaryToRemove is provided.
// if prepare is true, nothing is done but verify the new primary proxy is in the proxt map.
// caller must lock smapLock
func (h *httprunner) setPrimaryProxy(newPrimaryProxy, primaryToRemove string, prepare bool) error {
	proxyinfo, ok := h.smap.Pmap[newPrimaryProxy]
	if !ok {
		return fmt.Errorf("New Primary Proxy not present in proxy smap: %s", newPrimaryProxy)
	}

	if prepare {
		// If prepare=true, return before making any changes
		return nil
	}

	proxyinfo.Primary = true
	if primaryToRemove != "" {
		h.smap.delProxy(primaryToRemove)
	}

	h.smap.ProxySI = proxyinfo
	ctx.config.Proxy.Primary.ID = proxyinfo.DaemonID
	ctx.config.Proxy.Primary.URL = proxyinfo.DirectURL

	go func() {
		glog.Infof("Set primary proxy: %v (prepare: %t)", newPrimaryProxy, prepare)
		err := LocalSave(clivars.conffile, ctx.config)
		if err != nil {
			glog.Errorf("Error writing config file: %v", err)
		}
	}()

	return nil
}

// pingWithTimeout sends a http get to the server, returns true if the call returns in time;
// otherwise return false to indicate the server is not reachable.
func (p *proxyrunner) pingWithTimeout(url string, timeout time.Duration) (bool, error) {
	res := p.call(nil, nil, url, http.MethodGet, nil, timeout)
	if res.err == nil {
		return true, nil
	}

	if res.err == context.DeadlineExceeded || IsErrConnectionRefused(res.err) {
		return false, nil
	}

	return false, res.err
}
