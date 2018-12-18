// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dfc

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/json-iterator/go"
)

type Vote string

const (
	VoteYes Vote = "YES"
	VoteNo  Vote = "NO"
)

type (
	VoteRecord struct {
		Candidate string    `json:"candidate"`
		Primary   string    `json:"primary"`
		Smap      smapX     `json:"smap"`
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

// [METHOD] /v1/vote
func (t *targetrunner) voteHandler(w http.ResponseWriter, r *http.Request) {
	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Vote)
	if err != nil {
		return
	}

	switch {
	case r.Method == http.MethodGet && apitems[0] == cmn.Proxy:
		t.httpproxyvote(w, r)
	case r.Method == http.MethodPut && apitems[0] == cmn.Voteres:
		t.httpsetprimaryproxy(w, r)
	default:
		s := fmt.Sprintf("Invalid HTTP Method: %v %s", r.Method, r.URL.Path)
		t.invalmsghdlr(w, r, s)
	}
}

// [METHOD] /v1/vote
func (p *proxyrunner) voteHandler(w http.ResponseWriter, r *http.Request) {
	apitems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Vote)
	if err != nil {
		return
	}

	switch {
	case r.Method == http.MethodGet && apitems[0] == cmn.Proxy:
		p.httpproxyvote(w, r)
	case r.Method == http.MethodPut && apitems[0] == cmn.Voteres:
		p.httpsetprimaryproxy(w, r)
	case r.Method == http.MethodPut && apitems[0] == cmn.VoteInit:
		p.httpRequestNewPrimary(w, r)
	default:
		s := fmt.Sprintf("Invalid HTTP Method: %v %s", r.Method, r.URL.Path)
		p.invalmsghdlr(w, r, s)
	}
}

// GET /v1/vote/proxy
func (h *httprunner) httpproxyvote(w http.ResponseWriter, r *http.Request) {
	if _, err := h.checkRESTItems(w, r, 0, false, cmn.Version, cmn.Vote, cmn.Proxy); err != nil {
		return
	}

	msg := VoteMessage{}
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		s := fmt.Sprintf("Error reading Vote Request body: %v", err)
		h.invalmsghdlr(w, r, s)
		return
	}
	candidate := msg.Record.Candidate
	if candidate == "" {
		h.invalmsghdlr(w, r, fmt.Sprintln("Cannot request vote without Candidate field"))
		return
	}
	smap := h.smapowner.get()
	if smap.ProxySI == nil {
		h.invalmsghdlr(w, r, fmt.Sprintf("Cannot vote: current primary undefined, local Smap v%d", smap.version()))
		return
	}
	currPrimaryID := smap.ProxySI.DaemonID
	isproxy := smap.GetProxy(h.si.DaemonID) != nil
	if candidate == currPrimaryID {
		h.invalmsghdlr(w, r, fmt.Sprintf("Candidate %s == the current primary '%s'", candidate, currPrimaryID))
		return
	}
	newsmap := &msg.Record.Smap
	psi := newsmap.GetProxy(candidate)
	if psi == nil {
		h.invalmsghdlr(w, r, fmt.Sprintf("Candidate '%s' not present in the VoteRecord %s", candidate, newsmap.pp()))
		return
	}
	if !newsmap.isPresent(h.si, isproxy) {
		h.invalmsghdlr(w, r, fmt.Sprintf("Self '%s' not present in the VoteRecord Smap %s", h.si, newsmap.pp()))
		return
	}

	if s := h.smapowner.synchronize(newsmap, isproxy /*saveSmap*/, false /* lesserIsErr */); s != "" {
		glog.Errorf("Failed to synchronize VoteRecord Smap v%d, err %s - voting No", newsmap.version(), s)
		if _, err := w.Write([]byte(VoteNo)); err != nil {
			glog.Errorf("Error writing a No vote: %v", err)
		}
		return
	}

	vote, err := h.voteOnProxy(psi.DaemonID, currPrimaryID)
	if err != nil {
		h.invalmsghdlr(w, r, err.Error())
		return
	}
	if glog.V(4) {
		glog.Infof("Proxy voted '%v' for %s", vote, psi)
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

// PUT /v1/vote/result
func (h *httprunner) httpsetprimaryproxy(w http.ResponseWriter, r *http.Request) {
	if _, err := h.checkRESTItems(w, r, 0, false, cmn.Version, cmn.Vote, cmn.Voteres); err != nil {
		return
	}

	msg := VoteResultMessage{}
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		s := fmt.Sprintf("Error reading Vote Message body: %v", err)
		h.invalmsghdlr(w, r, s)
		return
	}

	vr := msg.Result
	glog.Infof("%s: received vote result: new primary %s", h.si, vr.Candidate)

	newprimary, oldprimary := vr.Candidate, vr.Primary

	h.smapowner.Lock()
	defer h.smapowner.Unlock()

	smap := h.smapowner.get()
	isproxy := smap.GetProxy(h.si.DaemonID) != nil
	psi := smap.GetProxy(newprimary)
	if psi == nil {
		s := fmt.Sprintf("New primary proxy %s not present in the local %s", newprimary, smap.pp())
		h.invalmsghdlr(w, r, s)
		return
	}
	clone := smap.clone()
	clone.ProxySI = psi
	if oldprimary != "" {
		clone.delProxy(oldprimary)
	}
	if s := h.smapowner.persist(clone, isproxy /*saveSmap*/); s != "" {
		h.invalmsghdlr(w, r, s)
		return
	}
	h.smapowner.put(clone)
	glog.Infof("resulting %s", clone.pp())
}

// PUT /v1/vote/init
func (p *proxyrunner) httpRequestNewPrimary(w http.ResponseWriter, r *http.Request) {
	if _, err := p.checkRESTItems(w, r, 0, false, cmn.Version, cmn.Vote, cmn.VoteInit); err != nil {
		return
	}

	msg := VoteInitiationMessage{}
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		s := fmt.Sprintf("Error reading Vote Request body: %v", err)
		p.invalmsghdlr(w, r, s)
		return
	}
	newsmap := &msg.Request.Smap
	if !newsmap.isValid() {
		s := fmt.Sprintf("Invalid Smap in the Vote Request: %s", newsmap.pp())
		p.invalmsghdlr(w, r, s)
		return
	}
	if !newsmap.isPresent(p.si, true) {
		s := fmt.Sprintf("Self '%s' not present in the Vote Request %s", p.si, newsmap.pp())
		p.invalmsghdlr(w, r, s)
		return
	}

	if s := p.smapowner.synchronize(newsmap, true /*saveSmap*/, false /* lesserIsErr */); s != "" {
		glog.Errorln(s)
	}

	smap := p.smapowner.get()
	psi, errstr := hrwProxy(smap, smap.ProxySI.DaemonID)
	if errstr != "" {
		s := fmt.Sprintf("Error preforming HRW: %s", errstr)
		p.invalmsghdlr(w, r, s)
		return
	}

	// only continue the election if this proxy is actually the next in line
	if psi.DaemonID != p.si.DaemonID {
		if glog.V(4) {
			glog.Warningf("This proxy is not next in line: %s. Received: %s",
				p.si.DaemonID, psi.DaemonID)
		}
		return
	}

	vr := &VoteRecord{
		Candidate: msg.Request.Candidate,
		Primary:   msg.Request.Primary,
		StartTime: time.Now(),
		Initiator: p.si.DaemonID,
	}
	// include resulting Smap in the response
	smap = p.smapowner.get()
	smap.deepcopy(&vr.Smap)

	// election should be started in a goroutine as it must not hang the http handler
	go p.proxyElection(vr, smap.ProxySI)
}

//===================
//
// Election Functions
//
//===================

func (p *proxyrunner) proxyElection(vr *VoteRecord, curPrimary *cluster.Snode) {
	if p.smapowner.get().isPrimary(p.si) {
		glog.Infoln("Already in primary state")
		return
	}
	xele := p.xactions.renewElection(p, vr)
	if xele == nil {
		return
	}
	glog.Infoln(xele.String())
	p.doProxyElection(vr, curPrimary, xele)
	xele.EndTime(time.Now())
}

func (p *proxyrunner) doProxyElection(vr *VoteRecord, curPrimary *cluster.Snode, xele *xactElection) {
	// First, ping current proxy with a short timeout: (Primary? State)
	primaryURL := curPrimary.PublicNet.DirectURL
	proxyup, err := p.pingWithTimeout(curPrimary, cmn.GCO.Get().Timeout.ProxyPing)
	if proxyup {
		// Move back to Idle state
		glog.Infof("current primary %s is up: moving back to idle", primaryURL)
		return
	}
	if err != nil {
		glog.Warningf("Error when pinging primary %s: %v", primaryURL, err)
	}
	glog.Infof("%s: primary proxy %v is confirmed down\n", p.si, primaryURL)
	glog.Infoln("Moving to election state phase 1")
	// Begin Election State
	elected, votingErrors := p.electAmongProxies(vr)
	if !elected {
		glog.Errorf("Election phase 1 (prepare) failed: primary remains %s, moving back to idle", primaryURL)
		return
	}
	glog.Infoln("Moving to election state phase 2 (commit)")
	// Begin Election2 State
	confirmationErrors := p.confirmElectionVictory(vr)

	// Check for errors that did not occur in the voting stage:
	for sid := range confirmationErrors {
		if _, ok := votingErrors[sid]; !ok {
			// A node errored while confirming that did not error while voting:
			glog.Errorf("An error occurred confirming the election with a node %s that was healthy when voting", sid)
		}
	}

	glog.Infof("Moving self %s to primary state", p.si)
	// Begin Primary State
	if s := p.becomeNewPrimary(vr.Primary /* proxyidToRemove */); s != "" {
		glog.Errorln(s)
	}
}

func (p *proxyrunner) electAmongProxies(vr *VoteRecord) (winner bool, errors map[string]bool) {
	// Simple Majority Vote
	resch := p.requestVotes(vr)
	errors = make(map[string]bool)
	y, n := 0, 0

	for res := range resch {
		if res.err != nil {
			if cmn.IsErrConnectionRefused(res.err) {
				if res.daemonID == vr.Primary {
					glog.Infof("Expected response from %s (current/failed primary): connection refused", res.daemonID)
				} else {
					glog.Warningf("Error response from %s: connection refused", res.daemonID)
				}
			} else {
				glog.Warningf("Error response from %s, err: %v", res.daemonID, res.err)
			}
			errors[res.daemonID] = true
			n++
		} else {
			if glog.V(4) {
				glog.Infof("Proxy %s responded with %v", res.daemonID, res.yes)
			}
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
	smap := p.smapowner.get()
	chansize := smap.CountTargets() + smap.CountProxies() - 1
	resch := make(chan voteResult, chansize)

	msg := VoteMessage{Record: *vr}
	jsbytes, err := jsoniter.Marshal(&msg)
	cmn.Assert(err == nil, err)

	q := url.Values{}
	q.Set(cmn.URLParamPrimaryCandidate, p.si.DaemonID)
	res := p.broadcastTo(
		cmn.URLPath(cmn.Version, cmn.Vote, cmn.Proxy),
		q,
		http.MethodGet,
		jsbytes,
		smap,
		cmn.GCO.Get().Timeout.CplaneOperation,
		cmn.NetworkIntraControl,
		cluster.AllNodes,
	)

	for r := range res {
		if r.err != nil {
			resch <- voteResult{
				yes:      false,
				daemonID: r.si.DaemonID,
				err:      r.err,
			}
		} else {
			resch <- voteResult{
				yes:      (VoteYes == Vote(r.outjson)),
				daemonID: r.si.DaemonID,
				err:      nil,
			}
		}
	}

	close(resch)
	return resch
}

func (p *proxyrunner) confirmElectionVictory(vr *VoteRecord) map[string]bool {
	jsbytes, err := jsoniter.Marshal(
		&VoteResultMessage{
			VoteResult{
				Candidate: vr.Candidate,
				Primary:   vr.Primary,
				Smap:      vr.Smap,
				StartTime: time.Now(),
				Initiator: p.si.DaemonID,
			}})
	cmn.Assert(err == nil, err)

	smap := p.smapowner.get()
	res := p.broadcastTo(
		cmn.URLPath(cmn.Version, cmn.Vote, cmn.Voteres),
		nil, // query
		http.MethodPut,
		jsbytes,
		smap,
		cmn.GCO.Get().Timeout.CplaneOperation,
		cmn.NetworkIntraControl,
		cluster.AllNodes,
	)

	errors := make(map[string]bool)
	for r := range res {
		if r.err != nil {
			glog.Warningf(
				"Broadcast committing result for %s(%s) failed: %v",
				r.si.DaemonID,
				r.si.PublicNet.DirectURL,
				r.err,
			)
			errors[r.si.DaemonID] = true
		}
	}
	return errors
}

func (p *proxyrunner) onPrimaryProxyFailure() {
	smap := p.smapowner.get()
	glog.Infof("%s: primary proxy (%s @ %v) has failed\n", p.si, smap.ProxySI, smap.ProxySI.PublicNet.DirectURL)
	if smap.CountProxies() <= 1 {
		glog.Warningf("No other proxies to elect")
		return
	}
	nextPrimaryProxy, errstr := hrwProxy(smap, smap.ProxySI.DaemonID)
	if errstr != "" {
		glog.Errorf("Failed to execute HRW selection upon primary proxy failure: %v", errstr)
		return
	}

	if glog.V(4) {
		glog.Infof("Primary proxy %s failure detected {url: %s}", smap.ProxySI, smap.ProxySI.PublicNet.DirectURL)
	}
	if nextPrimaryProxy.DaemonID == p.si.DaemonID {
		// If this proxy is the next primary proxy candidate, it starts the election directly.
		glog.Infof("%s: Starting election (candidate = self %s)", p.si, p.si)
		vr := &VoteRecord{
			Candidate: nextPrimaryProxy.DaemonID,
			Primary:   smap.ProxySI.DaemonID,
			StartTime: time.Now(),
			Initiator: p.si.DaemonID,
		}
		smap.deepcopy(&vr.Smap)
		p.proxyElection(vr, smap.ProxySI)
	} else {
		glog.Infof("%s: Requesting election (candidate = %s)", p.si, nextPrimaryProxy)
		vr := &VoteInitiation{
			Candidate: nextPrimaryProxy.DaemonID,
			Primary:   smap.ProxySI.DaemonID,
			StartTime: time.Now(),
			Initiator: p.si.DaemonID,
		}
		smap.deepcopy(&vr.Smap)
		p.sendElectionRequest(vr, nextPrimaryProxy)
	}
}

func (t *targetrunner) onPrimaryProxyFailure() {
	smap := t.smapowner.get()
	glog.Infof("%s: primary proxy (%s @ %v) failed\n", t.si, smap.ProxySI, smap.ProxySI.PublicNet.DirectURL)

	nextPrimaryProxy, errstr := hrwProxy(smap, smap.ProxySI.DaemonID)
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
		Primary:   smap.ProxySI.DaemonID,
		StartTime: time.Now(),
		Initiator: t.si.DaemonID,
	}
	smap.deepcopy(&vr.Smap)
	t.sendElectionRequest(vr, nextPrimaryProxy)
}

func (h *httprunner) sendElectionRequest(vr *VoteInitiation, nextPrimaryProxy *cluster.Snode) {
	msg := VoteInitiationMessage{Request: *vr}
	body, err := jsoniter.Marshal(&msg)
	cmn.Assert(err == nil, err)

	args := callArgs{
		si: nextPrimaryProxy,
		req: reqArgs{
			method: http.MethodPut,
			base:   nextPrimaryProxy.IntraControlNet.DirectURL,
			path:   cmn.URLPath(cmn.Version, cmn.Vote, cmn.VoteInit),
			body:   body,
		},
		timeout: defaultTimeout,
	}
	res := h.call(args)
	if res.err != nil {
		if cmn.IsErrConnectionRefused(res.err) {
			for i := 0; i < 2; i++ {
				time.Sleep(cmn.GCO.Get().Timeout.CplaneOperation)
				res = h.call(args)
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
	if !h.keepalive.isTimeToPing(currPrimaryID) {
		if glog.V(4) {
			glog.Warningf("Primary %s is still alive", currPrimaryID)
		}

		return false, nil
	}

	// Second: Vote according to whether or not the candidate is the Highest Random Weight remaining
	// in the Smap
	hrwmax, errstr := hrwProxy(h.smapowner.get(), currPrimaryID)
	if errstr != "" {
		return false, fmt.Errorf("Error executing HRW: %v", errstr)
	}
	if glog.V(4) {
		glog.Infof("Voting result for %s is %v. Expected primary: %s",
			daemonID, hrwmax.DaemonID == daemonID, daemonID)
	}
	return hrwmax.DaemonID == daemonID, nil
}

// pingWithTimeout sends a http get to the server, returns true if the call returns in time;
// otherwise return false to indicate the server is not reachable.
func (p *proxyrunner) pingWithTimeout(si *cluster.Snode, timeout time.Duration) (bool, error) {
	args := callArgs{
		si: si,
		req: reqArgs{
			method: http.MethodGet,
			base:   si.IntraControlNet.DirectURL,
			path:   cmn.URLPath(cmn.Version, cmn.Health),
		},
		timeout: timeout,
	}
	res := p.call(args)
	if res.err == nil {
		return true, nil
	}

	if res.err == context.DeadlineExceeded || cmn.IsErrConnectionRefused(res.err) {
		return false, nil
	}

	return false, res.err
}

//=========================
//
// test
//
//=========================
func NewVoteMsg(inp bool) SmapVoteMsg {
	return SmapVoteMsg{VoteInProgress: inp, Smap: &smapX{cluster.Smap{Version: 1}}}
}
