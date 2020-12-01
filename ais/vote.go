// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/xaction/xreg"
)

const (
	VoteYes Vote = "YES"
	VoteNo  Vote = "NO"
)

type (
	Vote string

	VoteRecord struct {
		Candidate string    `json:"candidate"`
		Primary   string    `json:"primary"`
		Smap      *smapX    `json:"smap"`
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

///////////////////
// voting: proxy //
///////////////////

// [METHOD] /v1/vote
func (p *proxyrunner) voteHandler(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Vote)
	if err != nil {
		return
	}

	switch {
	case r.Method == http.MethodGet && apiItems[0] == cmn.Proxy:
		p.httpproxyvote(w, r)
	case r.Method == http.MethodPut && apiItems[0] == cmn.Voteres:
		p.httpsetprimaryproxy(w, r)
	case r.Method == http.MethodPut && apiItems[0] == cmn.VoteInit:
		p.httpRequestNewPrimary(w, r)
	default:
		p.invalmsghdlrf(w, r, "Invalid HTTP Method: %v %s", r.Method, r.URL.Path)
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
	newsmap := msg.Request.Smap
	if err := newsmap.validate(); err != nil {
		p.invalmsghdlrf(w, r, "%s: invalid %s in the Vote Request, err: %v", p.si, newsmap, err)
		return
	}
	if !newsmap.isPresent(p.si) {
		p.invalmsghdlrf(w, r, "%s: not present in the Vote Request, %s", p.si, newsmap)
		return
	}

	if err := p.owner.smap.synchronize(p.si, newsmap); err != nil {
		if isErrDowngrade(err) {
			psi := newsmap.GetProxy(msg.Request.Candidate)
			psi2 := p.owner.smap.get().GetProxy(msg.Request.Candidate)
			if psi2.Equals(psi) {
				err = nil
			}
		}
		if err != nil {
			p.invalmsghdlrf(w, r, "%s: failed to synch %s: %v", p.si, newsmap, err)
			return
		}
	}

	smap := p.owner.smap.get()
	psi, err := cluster.HrwProxy(&smap.Smap, smap.Primary.ID())
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	// proceed with election iff this proxy is actually the next in line
	if psi.ID() != p.si.ID() {
		glog.Warningf("%s: not next in line, received: %s", p.si, psi)
		return
	}

	vr := &VoteRecord{
		Candidate: msg.Request.Candidate,
		Primary:   msg.Request.Primary,
		StartTime: time.Now(),
		Initiator: p.si.ID(),
	}
	// include resulting Smap in the response
	vr.Smap = p.owner.smap.get()

	// election should be started in a goroutine as it must not hang the http handler
	go p.proxyElection(vr, vr.Smap.Primary)
}

// Election Functions

func (p *proxyrunner) proxyElection(vr *VoteRecord, curPrimary *cluster.Snode) {
	if p.owner.smap.get().isPrimary(p.si) {
		glog.Infoln("Already in primary state")
		return
	}
	xele := xreg.RenewElection()
	if xele == nil {
		return
	}
	glog.Infoln(xele.String())
	p.doProxyElection(vr, curPrimary, xele)
	xele.Finish()
}

func (p *proxyrunner) doProxyElection(vr *VoteRecord, curPrimary *cluster.Snode, xact cluster.Xact) {
	var (
		err    = context.DeadlineExceeded
		config = cmn.GCO.Get()
	)
	// 1. ping current primary
	for i := time.Duration(2); i >= 1 && err != nil; i-- {
		timeout := config.Timeout.CplaneOperation / i
		_, _, err = p.Health(curPrimary, timeout, nil)
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
		glog.Errorf("%s: election phase 1 (prepare) failed: primary remains %s, moving back to idle",
			p.si, curPrimary)
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

func (p *proxyrunner) electAmongProxies(vr *VoteRecord, xact cluster.Xact) (winner bool, errors map[string]bool) {
	// Simple Majority Vote
	resch := p.requestVotes(vr)
	errors = make(map[string]bool)
	y, n := 0, 0

	for res := range resch {
		if res.err != nil {
			if cmn.IsErrConnectionRefused(res.err) {
				if res.daemonID == vr.Primary {
					glog.Infof("Expected response from %s (failed primary): connection refused",
						res.daemonID)
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
		msg = VoteMessage{Record: *vr}
		q   = url.Values{}
	)
	q.Set(cmn.URLParamPrimaryCandidate, p.si.ID())
	results := p.callAll(http.MethodGet, cmn.JoinWords(cmn.Version, cmn.Vote, cmn.Proxy), cmn.MustMarshal(&msg), q)
	resCh := make(chan voteResult, len(results))
	for res := range results {
		if res.err != nil {
			resCh <- voteResult{
				yes:      false,
				daemonID: res.si.ID(),
				err:      res.err,
			}
		} else {
			resCh <- voteResult{
				yes:      VoteYes == Vote(res.bytes),
				daemonID: res.si.ID(),
				err:      nil,
			}
		}
	}

	close(resCh)
	return resCh
}

func (p *proxyrunner) confirmElectionVictory(vr *VoteRecord) map[string]bool {
	msg := &VoteResultMessage{
		VoteResult{
			Candidate: vr.Candidate,
			Primary:   vr.Primary,
			Smap:      vr.Smap,
			StartTime: time.Now(),
			Initiator: p.si.ID(),
		},
	}

	res := p.callAll(http.MethodPut, cmn.JoinWords(cmn.Version, cmn.Vote, cmn.Voteres), cmn.MustMarshal(msg))
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

////////////////////
// voting: target //
////////////////////

// [METHOD] /v1/vote
func (t *targetrunner) voteHandler(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Vote)
	if err != nil {
		return
	}

	switch {
	case r.Method == http.MethodGet && apiItems[0] == cmn.Proxy:
		t.httpproxyvote(w, r)
	case r.Method == http.MethodPut && apiItems[0] == cmn.Voteres:
		t.httpsetprimaryproxy(w, r)
	default:
		t.invalmsghdlrf(w, r, "Invalid HTTP Method: %v %s", r.Method, r.URL.Path)
	}
}

////////////////////////////
// voting: common methods //
////////////////////////////

func (h *httprunner) onPrimaryProxyFailure() {
	smap := h.owner.smap.get()
	if smap.validate() != nil {
		return
	}
	clone := smap.clone()
	glog.Infof("%s: primary %s has failed", h.si, clone.Primary.NameEx())

	for {
		// use HRW ordering
		nextPrimaryProxy, err := cluster.HrwProxy(&clone.Smap, clone.Primary.ID())
		if err != nil {
			glog.Errorf("%s: failed to execute HRW selection, err: %v", h.si, err)
			return
		}
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s: trying %s as the primary candidate", h.si, nextPrimaryProxy.ID())
		}

		// If this proxy is the next primary proxy candidate, it starts the election directly.
		if nextPrimaryProxy.ID() == h.si.ID() {
			cmn.Assert(h.si.IsProxy())
			glog.Infof("%s: starting election (candidate = self)", h.si)
			vr := &VoteRecord{
				Candidate: nextPrimaryProxy.ID(),
				Primary:   clone.Primary.ID(),
				StartTime: time.Now(),
				Initiator: h.si.ID(),
			}
			vr.Smap = clone
			h.electable.proxyElection(vr, clone.Primary)
			return
		}

		// ask the candidate to start election
		vr := &VoteInitiation{
			Candidate: nextPrimaryProxy.ID(),
			Primary:   clone.Primary.ID(),
			StartTime: time.Now(),
			Initiator: h.si.ID(),
		}
		vr.Smap = clone
		if h.sendElectionRequest(vr, nextPrimaryProxy) == nil {
			return // the candidate has accepted the request and started election
		}

		// No response from the candidate (or it failed to start election) - remove
		// it from the Smap and try the next candidate
		if clone.GetProxy(nextPrimaryProxy.ID()) != nil {
			clone.delProxy(nextPrimaryProxy.ID())
		}
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
		h.invalmsghdlrf(w, r, "%s: unexpected: empty candidate field [%v]", h.si, msg.Record)
		return
	}
	smap := h.owner.smap.get()
	if smap.Primary == nil {
		h.invalmsghdlrf(w, r, "%s: current primary undefined, %s", h.si, smap)
		return
	}
	currPrimaryID := smap.Primary.ID()
	if candidate == currPrimaryID {
		h.invalmsghdlrf(w, r, "%s: candidate %q _is_ the current primary, %s", h.si, candidate, smap)
		return
	}
	newsmap := msg.Record.Smap
	psi := newsmap.GetProxy(candidate)
	if psi == nil {
		h.invalmsghdlrf(w, r, "%s: candidate %q not present in the VoteRecord %s",
			h.si, candidate, newsmap)
		return
	}
	if !newsmap.isPresent(h.si) {
		h.invalmsghdlrf(w, r, "%s: not present in the VoteRecord %s", h.si, newsmap)
		return
	}

	if err := h.owner.smap.synchronize(h.si, newsmap); err != nil {
		// double-checking errDowngrade
		if isErrDowngrade(err) {
			newsmap2 := h.owner.smap.get()
			psi2 := newsmap2.GetProxy(candidate)
			if psi2.Equals(psi) {
				err = nil // not an error - can vote Yes
			}
		}
		if err != nil {
			glog.Errorf("%s: failed to synch %s, err %v - voting No", h.si, newsmap, err)
			if _, err := w.Write([]byte(VoteNo)); err != nil {
				glog.Errorf("%s: failed to write a No vote: %v", h.si, err)
			}
			return
		}
	}

	vote, err := h.voteOnProxy(psi.ID(), currPrimaryID)
	if err != nil {
		h.invalmsghdlr(w, r, err.Error())
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s: voted '%v' for %s", h.si, vote, psi)
	}

	if vote {
		_, err = w.Write([]byte(VoteYes))
		if err != nil {
			h.invalmsghdlrf(w, r, "%s: failed to write Yes vote: %v", h.si, err)
		}
	} else {
		_, err = w.Write([]byte(VoteNo))
		if err != nil {
			h.invalmsghdlrf(w, r, "%s: failed to write No vote: %v", h.si, err)
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
	glog.Infof("%s: received vote result: new primary %s (old %s)", h.si, vr.Candidate, vr.Primary)

	ctx := &smapModifier{
		pre: h._votedPrimary,
		nid: vr.Candidate,
		sid: vr.Primary,
	}
	err := h.owner.smap.modify(ctx)
	if err != nil {
		h.invalmsghdlr(w, r, err.Error())
	}
}

func (h *httprunner) _votedPrimary(ctx *smapModifier, clone *smapX) error {
	newPrimary, oldPrimary := ctx.nid, ctx.sid
	psi := clone.GetProxy(newPrimary)
	if psi == nil {
		return &errNodeNotFound{"cannot accept new primary election", newPrimary, h.si, clone}
	}
	clone.Primary = psi
	if oldPrimary != "" && clone.GetProxy(oldPrimary) != nil {
		clone.delProxy(oldPrimary)
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s: voted-primary result: %s", h.si, clone.pp())
	} else {
		glog.Infof("%s: voted-primary result: %s", h.si, clone)
	}
	return nil
}

func (h *httprunner) sendElectionRequest(vr *VoteInitiation, nextPrimaryProxy *cluster.Snode) error {
	msg := VoteInitiationMessage{Request: *vr}
	body := cmn.MustMarshal(&msg)

	args := callArgs{
		si: nextPrimaryProxy,
		req: cmn.ReqArgs{
			Method: http.MethodPut,
			Base:   nextPrimaryProxy.IntraControlNet.DirectURL,
			Path:   cmn.JoinWords(cmn.Version, cmn.Vote, cmn.VoteInit),
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

// test-only
func NewVoteMsg(inp bool) SmapVoteMsg {
	return SmapVoteMsg{VoteInProgress: inp, Smap: &smapX{cluster.Smap{Version: 1}}}
}
