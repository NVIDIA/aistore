// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/xaction"
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
		StartTime time.Time `json:"start_time"`
		Initiator string    `json:"initiator"`
	}

	VoteInitiation VoteRecord
	VoteResult     VoteRecord

	VoteMessage struct {
		Record VoteRecord `json:"vote_record"`
	}

	VoteInitiationMessage struct {
		Request VoteInitiation `json:"vote_initiation"`
	}

	VoteResultMessage struct {
		Result VoteResult `json:"vote_result"`
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
		t.invalmsghdlrf(w, r, "Invalid HTTP Method: %v %s", r.Method, r.URL.Path)
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
		p.invalmsghdlrf(w, r, "Invalid HTTP Method: %v %s", r.Method, r.URL.Path)
	}
}

// GET /v1/vote/proxy
func (h *httprunner) httpproxyvote(w http.ResponseWriter, r *http.Request) {
	if _, err := h.checkRESTItems(w, r, 0, false, cmn.Version, cmn.Vote, cmn.Proxy); err != nil {
		return
	}

	msg := VoteMessage{}
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		return
	}
	candidate := msg.Record.Candidate
	if candidate == "" {
		h.invalmsghdlr(w, r, "Cannot request vote without Candidate field")
		return
	}
	smap := h.owner.smap.get()
	if smap.ProxySI == nil {
		h.invalmsghdlrf(w, r, "Cannot vote: current primary undefined, local %s", smap)
		return
	}
	currPrimaryID := smap.ProxySI.ID()
	if candidate == currPrimaryID {
		h.invalmsghdlrf(w, r, "Candidate %s == the current primary %q", candidate, currPrimaryID)
		return
	}
	newsmap := &msg.Record.Smap
	psi := newsmap.GetProxy(candidate)
	if psi == nil {
		h.invalmsghdlrf(w, r, "Candidate %q not present in the VoteRecord %s", candidate, newsmap.pp())
		return
	}
	if !newsmap.isPresent(h.si) {
		h.invalmsghdlrf(w, r, "Self %q not present in the VoteRecord Smap %s", h.si, newsmap.pp())
		return
	}

	if err := h.owner.smap.synchronize(newsmap, false /* lesserIsErr */); err != nil {
		glog.Errorf("Failed to synchronize VoteRecord %s, err %s - voting No", newsmap, err)
		if _, err := w.Write([]byte(VoteNo)); err != nil {
			glog.Errorf("Error writing a No vote: %v", err)
		}
		return
	}

	vote, err := h.voteOnProxy(psi.ID(), currPrimaryID)
	if err != nil {
		h.invalmsghdlr(w, r, err.Error())
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("Proxy voted '%v' for %s", vote, psi)
	}

	if vote {
		_, err = w.Write([]byte(VoteYes))
		if err != nil {
			h.invalmsghdlrf(w, r, "Error writing yes vote: %v", err)
		}
	} else {
		_, err = w.Write([]byte(VoteNo))
		if err != nil {
			h.invalmsghdlrf(w, r, "Error writing no vote: %v", err)
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
		return
	}

	vr := msg.Result
	glog.Infof("%s: received vote result: new primary %s", h.si, vr.Candidate)

	newPrimary, oldPrimary := vr.Candidate, vr.Primary
	err := h.owner.smap.modify(func(clone *smapX) error {
		psi := clone.GetProxy(newPrimary)
		if psi == nil {
			return &errNodeNotFound{"cannot accept new primary election", newPrimary, h.si, clone}
		}
		clone.ProxySI = psi
		if oldPrimary != "" && clone.GetProxy(oldPrimary) != nil {
			clone.delProxy(oldPrimary)
		}
		glog.Infof("resulting %s", clone.pp())
		return nil
	})
	if err != nil {
		h.invalmsghdlr(w, r, err.Error())
	}
}

// PUT /v1/vote/init
func (p *proxyrunner) httpRequestNewPrimary(w http.ResponseWriter, r *http.Request) {
	if _, err := p.checkRESTItems(w, r, 0, false, cmn.Version, cmn.Vote, cmn.VoteInit); err != nil {
		return
	}

	msg := VoteInitiationMessage{}
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		return
	}
	newsmap := &msg.Request.Smap
	if !newsmap.isValid() {
		p.invalmsghdlrf(w, r, "Invalid Smap in the Vote Request: %s", newsmap.pp())
		return
	}
	if !newsmap.isPresent(p.si) {
		p.invalmsghdlrf(w, r, "Self %q not present in the Vote Request %s", p.si, newsmap.pp())
		return
	}

	if err := p.owner.smap.synchronize(newsmap, false /* lesserIsErr */); err != nil {
		glog.Error(err)
	}

	smap := p.owner.smap.get()
	psi, err := cluster.HrwProxy(&smap.Smap, smap.ProxySI.ID())
	if err != nil {
		p.invalmsghdlrf(w, r, "Error preforming HRW: %s", err)
		return
	}

	// only continue the election if this proxy is actually the next in line
	if psi.ID() != p.si.ID() {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Warningf("This proxy is not next in line: %s. Received: %s",
				p.si.ID(), psi.ID())
		}
		return
	}

	vr := &VoteRecord{
		Candidate: msg.Request.Candidate,
		Primary:   msg.Request.Primary,
		StartTime: time.Now(),
		Initiator: p.si.ID(),
	}
	// include resulting Smap in the response
	smap = p.owner.smap.get()
	smap.deepCopy(&vr.Smap)

	// election should be started in a goroutine as it must not hang the http handler
	go p.proxyElection(vr, smap.ProxySI)
}

//===================
//
// Election Functions
//
//===================

func (p *proxyrunner) proxyElection(vr *VoteRecord, curPrimary *cluster.Snode) {
	if p.owner.smap.get().isPrimary(p.si) {
		glog.Infoln("Already in primary state")
		return
	}
	xele := xaction.Registry.RenewElection()
	if xele == nil {
		return
	}
	glog.Infoln(xele.String())
	p.doProxyElection(vr, curPrimary, xele)
	xele.SetEndTime(time.Now())
}

func (p *proxyrunner) doProxyElection(vr *VoteRecord, curPrimary *cluster.Snode, xact *xaction.Election) {
	var (
		err    = context.DeadlineExceeded
		config = cmn.GCO.Get()
	)
	// 1. ping current primary
	for i := time.Duration(2); i >= 1 && err != nil; i-- {
		timeout := config.Timeout.CplaneOperation / i
		_, err, _ = p.Health(curPrimary, timeout, nil)
	}
	if err == nil {
		// move back to idle
		glog.Infof("%s: the current primary %s is up, moving back to idle", p.si, curPrimary)
		return
	}
	glog.Infof("%s: primary %s is confirmed down(%v)", p.si, curPrimary, err)

	// 2. election phase 1
	glog.Info("Moving to election state phase 1 (prepare)")
	elected, votingErrors := p.electAmongProxies(vr, xact)
	if !elected {
		glog.Errorf("%s: election phase 1 (prepare) failed: primary remains %s, moving back to idle", p.si, curPrimary)
		return
	}

	// 3. election phase 2
	glog.Info("Moving to election state phase 2 (commit)")
	confirmationErrors := p.confirmElectionVictory(vr)
	for sid := range confirmationErrors {
		if _, ok := votingErrors[sid]; !ok {
			// NOTE: p of no return
			glog.Errorf("%s: error confirming the election with %s that was healthy when voting", p.si, sid)
		}
	}

	// 4. become!
	glog.Infof("%s: moving (self) to primary state", p.si)
	p.becomeNewPrimary(vr.Primary /* proxyIDToRemove */)
}

func (p *proxyrunner) electAmongProxies(vr *VoteRecord, xact *xaction.Election) (winner bool, errors map[string]bool) {
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
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("Proxy %s responded with %v", res.daemonID, res.yes)
			}
			if res.yes {
				y++
			} else {
				n++
			}
		}
	}

	xact.ObjectsAdd(int64(y + n))
	winner = y > n || (y+n == 0) // No Votes: Default Winner
	glog.Infof("Vote Results:\n Y: %v, N:%v\n Victory: %v\n", y, n, winner)
	return
}

func (p *proxyrunner) requestVotes(vr *VoteRecord) chan voteResult {
	var (
		msg  = VoteMessage{Record: *vr}
		body = cmn.MustMarshal(&msg)
		q    = url.Values{}
	)
	q.Set(cmn.URLParamPrimaryCandidate, p.si.ID())

	results := p.bcastGet(bcastArgs{
		req: cmn.ReqArgs{
			Path:  cmn.URLPath(cmn.Version, cmn.Vote, cmn.Proxy),
			Query: q,
			Body:  body,
		},
		to: cluster.AllNodes,
	})

	resCh := make(chan voteResult, len(results))
	for r := range results {
		if r.err != nil {
			resCh <- voteResult{
				yes:      false,
				daemonID: r.si.ID(),
				err:      r.err,
			}
		} else {
			resCh <- voteResult{
				yes:      (VoteYes == Vote(r.outjson)),
				daemonID: r.si.ID(),
				err:      nil,
			}
		}
	}

	close(resCh)
	return resCh
}

func (p *proxyrunner) confirmElectionVictory(vr *VoteRecord) map[string]bool {
	body := cmn.MustMarshal(&VoteResultMessage{
		VoteResult{
			Candidate: vr.Candidate,
			Primary:   vr.Primary,
			Smap:      vr.Smap,
			StartTime: time.Now(),
			Initiator: p.si.ID(),
		},
	})

	res := p.bcastPut(bcastArgs{
		req: cmn.ReqArgs{
			Path: cmn.URLPath(cmn.Version, cmn.Vote, cmn.Voteres),
			Body: body,
		},
		to: cluster.AllNodes,
	})

	errors := make(map[string]bool)
	for r := range res {
		if r.err != nil {
			glog.Warningf(
				"Broadcast commit result for %s(%s) failed: %v",
				r.si.ID(),
				r.si.IntraControlNet.DirectURL,
				r.err,
			)
			errors[r.si.ID()] = true
		}
	}
	return errors
}

func (p *proxyrunner) onPrimaryProxyFailure() {
	clone := p.owner.smap.get().clone()
	if !clone.isValid() {
		return
	}
	glog.Infof("%s: primary %s has failed\n", p.si, clone.ProxySI.NameEx())

	// Find out the first proxy (using HRW algorithm) that is running and can be
	// elected as the primary one.
	for {
		// Generate the next candidate
		nextPrimaryProxy, err := cluster.HrwProxy(&clone.Smap, clone.ProxySI.ID())
		if err != nil {
			glog.Errorf("Failed to execute HRW selection upon primary proxy failure: %v", err)
			return
		}
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("Trying %s as the primary candidate", nextPrimaryProxy.ID())
		}
		if nextPrimaryProxy.ID() == p.si.ID() {
			// If this proxy is the next primary proxy candidate, it starts the election directly.
			glog.Infof("%s: Starting election (candidate = self)", p.si)
			vr := &VoteRecord{
				Candidate: nextPrimaryProxy.ID(),
				Primary:   clone.ProxySI.ID(),
				StartTime: time.Now(),
				Initiator: p.si.ID(),
			}
			clone.deepCopy(&vr.Smap)
			p.proxyElection(vr, clone.ProxySI)
			return
		}

		// ask the current primary candidate to start election
		vr := &VoteInitiation{
			Candidate: nextPrimaryProxy.ID(),
			Primary:   clone.ProxySI.ID(),
			StartTime: time.Now(),
			Initiator: p.si.ID(),
		}
		clone.deepCopy(&vr.Smap)
		if p.sendElectionRequest(vr, nextPrimaryProxy) == nil {
			// the candidate has accepted request and started election
			return
		}

		// no response from the candidate or it failed to start election.
		// Remove it from the Smap and try the next candidate
		if clone.GetProxy(nextPrimaryProxy.ID()) != nil {
			clone.delProxy(nextPrimaryProxy.ID())
		}
	}
}

func (t *targetrunner) onPrimaryProxyFailure() {
	clone := t.owner.smap.get().clone()
	if !clone.isValid() {
		return
	}
	glog.Infof("%s: primary %s failed\n", t.si, clone.ProxySI.NameEx())

	// Find out the first proxy (using HRW algorithm) that is running and can be
	// elected as the primary one.
	for {
		// Generate the next candidate
		nextPrimaryProxy, err := cluster.HrwProxy(&clone.Smap, clone.ProxySI.ID())
		if err != nil {
			glog.Errorf("Failed to execute HRW selection upon primary proxy failure: %v", err)
			return
		}
		if nextPrimaryProxy == nil {
			// There is only one proxy, so we cannot select a next in line
			glog.Warningf("primary proxy failed, but there are no candidates to fall back on.")
			return
		}
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("Trying %s as the primary candidate", nextPrimaryProxy.ID())
		}

		// ask the current primary candidate to start election
		vr := &VoteInitiation{
			Candidate: nextPrimaryProxy.ID(),
			Primary:   clone.ProxySI.ID(),
			StartTime: time.Now(),
			Initiator: t.si.ID(),
		}
		clone.deepCopy(&vr.Smap)
		if t.sendElectionRequest(vr, nextPrimaryProxy) == nil {
			// the candidate has accepted request and started election
			break
		}

		// no response from the candidate or it failed to start election.
		// Remove it from the Smap
		if clone.GetProxy(nextPrimaryProxy.ID()) != nil {
			clone.delProxy(nextPrimaryProxy.ID())
		}
	}
}

func (h *httprunner) sendElectionRequest(vr *VoteInitiation, nextPrimaryProxy *cluster.Snode) error {
	msg := VoteInitiationMessage{Request: *vr}
	body := cmn.MustMarshal(&msg)

	args := callArgs{
		si: nextPrimaryProxy,
		req: cmn.ReqArgs{
			Method: http.MethodPut,
			Base:   nextPrimaryProxy.IntraControlNet.DirectURL,
			Path:   cmn.URLPath(cmn.Version, cmn.Vote, cmn.VoteInit),
			Body:   body,
		},
		timeout: cmn.DefaultTimeout,
	}
	res := h.call(args)
	if res.err != nil {
		sleepTime := cmn.GCO.Get().Timeout.CplaneOperation
		if cmn.IsErrConnectionRefused(res.err) {
			for i := 0; i < 2; i++ {
				time.Sleep(sleepTime)
				res = h.call(args)
				if res.err == nil {
					break
				}
				sleepTime += sleepTime / 2
			}
		}
		glog.Errorf("Failed to request election from next primary proxy: %v", res.err)
	}
	return res.err
}

func (h *httprunner) voteOnProxy(daemonID, currPrimaryID string) (bool, error) {
	// First: Check last keepalive timestamp. If the proxy was recently successfully reached,
	// this will always vote no, as we believe the original proxy is still alive.
	if !h.keepalive.isTimeToPing(currPrimaryID) {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Warningf("Primary %s is still alive", currPrimaryID)
		}

		return false, nil
	}

	// Second: Vote according to whether or not the candidate is the Highest Random Weight remaining
	// in the Smap
	smap := h.owner.smap.get()
	nextPrimaryProxy, err := cluster.HrwProxy(&smap.Smap, currPrimaryID)
	if err != nil {
		return false, fmt.Errorf("error executing HRW: %v", err)
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("Voting result for %s is %v. Expected primary: %s",
			daemonID, nextPrimaryProxy.ID() == daemonID, daemonID)
	}
	return nextPrimaryProxy.ID() == daemonID, nil
}

//=========================
//
// test
//
//=========================
func NewVoteMsg(inp bool) SmapVoteMsg {
	return SmapVoteMsg{VoteInProgress: inp, Smap: &smapX{cluster.Smap{Version: 1}}}
}
