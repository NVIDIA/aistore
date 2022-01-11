// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"net/url"
	"runtime"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/xact/xreg"
)

const (
	VoteYes Vote = "YES"
	VoteNo  Vote = "NO"
)

const maxRetryElectReq = 3

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

func voteInProgress() (xele cluster.Xact) {
	if e := xreg.GetRunning(xreg.XactFilter{Kind: cmn.ActElection}); e != nil {
		xele = e.Get()
	}
	return
}

///////////////////
// voting: proxy //
///////////////////

// [METHOD] /v1/vote
func (p *proxy) voteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPut {
		cmn.WriteErr405(w, r, http.MethodGet, http.MethodPut)
		return
	}
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.URLPathVote.L)
	if err != nil {
		return
	}
	if !p.NodeStarted() {
		w.WriteHeader(http.StatusServiceUnavailable)
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
		p.writeErrURL(w, r)
	}
}

// PUT /v1/vote/init
func (p *proxy) httpRequestNewPrimary(w http.ResponseWriter, r *http.Request) {
	if _, err := p.checkRESTItems(w, r, 0, false, cmn.URLPathVoteInit.L); err != nil {
		return
	}
	msg := VoteInitiationMessage{}
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		return
	}
	newSmap := msg.Request.Smap
	if err := newSmap.validate(); err != nil {
		p.writeErrf(w, r, "%s: invalid %s in the Vote Request, err: %v", p.si, newSmap, err)
		return
	}
	smap := p.owner.smap.get()
	caller := r.Header.Get(cmn.HdrCallerName)
	glog.Infof("[vote] receive %s from %q (local: %s)", newSmap.StringEx(), caller, smap.StringEx())

	if !newSmap.isPresent(p.si) {
		p.writeErrf(w, r, "%s: not present in the Vote Request, %s", p.si, newSmap)
		return
	}
	debug.Assert(!newSmap.isPrimary(p.si))

	if err := p.owner.smap.synchronize(p.si, newSmap, nil /*ms payload*/); err != nil {
		if isErrDowngrade(err) {
			psi := newSmap.GetProxy(msg.Request.Candidate)
			psi2 := p.owner.smap.get().GetProxy(msg.Request.Candidate)
			if psi2.Equals(psi) {
				err = nil
			}
		}
		if err != nil {
			p.writeErrf(w, r, cmn.FmtErrFailed, p.si, "sync", newSmap, err)
			return
		}
	}

	smap = p.owner.smap.get()
	psi, err := cluster.HrwProxy(&smap.Smap, smap.Primary.ID())
	if err != nil {
		p.writeErr(w, r, err)
		return
	}

	// proceed with election iff:
	if psi.ID() != p.si.ID() {
		glog.Warningf("%s: not next in line %s", p.si, psi)
		return
	} else if !p.ClusterStarted() {
		glog.Warningf("%s: not ready yet to be elected - starting up", p.si)
		w.WriteHeader(http.StatusServiceUnavailable)
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
	go p.proxyElection(vr)
}

// Election Functions

func (p *proxy) proxyElection(vr *VoteRecord) {
	if p.owner.smap.get().isPrimary(p.si) {
		glog.Infof("%s: already in primary state", p.si)
		return
	}
	rns := xreg.RenewElection()
	debug.AssertNoErr(rns.Err)
	if rns.IsRunning() {
		return
	}
	xele := rns.Entry.Get()
	glog.Infoln(xele.Name())
	p.doProxyElection(vr)
	xele.Finish(nil)
}

func (p *proxy) doProxyElection(vr *VoteRecord) {
	var (
		err        error
		curPrimary = vr.Smap.Primary
		config     = cmn.GCO.Get()
		timeout    = config.Timeout.CplaneOperation.D() / 2
	)
	// 1. ping current primary (not using cmn.URLParamAskPrimary as it might be transitioning)
	for i := 0; i < 2; i++ {
		if i > 0 {
			runtime.Gosched()
		}
		smap := p.owner.smap.get()
		if smap.version() > vr.Smap.version() {
			glog.Warningf("%s: %s updated from %s, moving back to idle", p.si, smap, vr.Smap)
			return
		}
		_, _, err = p.Health(curPrimary, timeout, nil /*ask primary*/)
		if err == nil {
			break
		}
		timeout = config.Timeout.CplaneOperation.D()
	}
	if err == nil {
		// move back to idle
		query := url.Values{cmn.URLParamAskPrimary: []string{"true"}}
		_, _, err = p.Health(curPrimary, timeout, query /*ask primary*/)
		if err == nil {
			glog.Infof("%s: current primary %s is up, moving back to idle", p.si, curPrimary)
		} else {
			glog.Errorf("%s: current primary(?) %s responds but does not consider itself primary",
				p.si, curPrimary)
		}
		return
	}
	glog.Infof("%s: primary %s is confirmed down: %v", p.si, curPrimary, err)

	// 2. election phase 1
	glog.Info("Moving to election state phase 1 (prepare)")
	elected, votingErrors := p.electAmongProxies(vr)
	if !elected {
		glog.Errorf("Election phase 1 (prepare) failed: primary remains %s, moving back to idle", curPrimary)
		return
	}

	// 3. election phase 2
	glog.Info("Moving to election state phase 2 (commit)")
	confirmationErrors := p.confirmElectionVictory(vr)
	for sid := range confirmationErrors {
		if !votingErrors.Contains(sid) {
			glog.Errorf("Error confirming the election: %s was healthy when voting", sid)
		}
	}

	// 4. become!
	glog.Infof("%s: moving (self) to primary state", p.si)
	p.becomeNewPrimary(vr.Primary /*proxyIDToRemove*/)
}

// Simple majority voting.
func (p *proxy) electAmongProxies(vr *VoteRecord) (winner bool, errors cos.StringSet) {
	var (
		resCh = p.requestVotes(vr)
		y, n  int
	)
	for res := range resCh {
		if res.err != nil {
			if errors == nil {
				errors = cos.NewStringSet(res.daemonID)
			} else {
				errors.Add(res.daemonID)
			}
			n++
		} else {
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("Node %s responded with (winner: %t)", res.daemonID, res.yes)
			}
			if res.yes {
				y++
			} else {
				n++
			}
		}
	}

	winner = y > n || (y+n == 0) // No Votes: Default Winner
	glog.Infof("Vote Results:\n Y: %d, N: %d\n Victory: %t\n", y, n, winner)
	return
}

func (p *proxy) requestVotes(vr *VoteRecord) chan voteResult {
	var (
		msg = VoteMessage{Record: *vr}
		q   = url.Values{}
	)
	q.Set(cmn.URLParamPrimaryCandidate, p.si.ID())
	args := allocBcastArgs()
	args.req = cmn.ReqArgs{
		Method: http.MethodGet,
		Path:   cmn.URLPathVoteProxy.S,
		Body:   cos.MustMarshal(&msg),
		Query:  q,
	}
	args.to = cluster.AllNodes
	results := p.bcastGroup(args)
	freeBcastArgs(args)
	resCh := make(chan voteResult, len(results))
	for _, res := range results {
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
	freeBcastRes(results)
	close(resCh)
	return resCh
}

func (p *proxy) confirmElectionVictory(vr *VoteRecord) cos.StringSet {
	var (
		errors = cos.NewStringSet()
		msg    = &VoteResultMessage{
			VoteResult{
				Candidate: vr.Candidate,
				Primary:   vr.Primary,
				Smap:      vr.Smap,
				StartTime: time.Now(),
				Initiator: p.si.ID(),
			},
		}
	)
	args := allocBcastArgs()
	args.req = cmn.ReqArgs{Method: http.MethodPut, Path: cmn.URLPathVoteVoteres.S, Body: cos.MustMarshal(msg)}
	args.to = cluster.AllNodes
	results := p.bcastGroup(args)
	freeBcastArgs(args)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		glog.Warningf("%s: failed to confirm election with %s: %v", p.si, res.si, res.err)
		errors.Add(res.si.ID())
	}
	freeBcastRes(results)
	return errors
}

////////////////////
// voting: target //
////////////////////

// [METHOD] /v1/vote
func (t *target) voteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPut {
		cmn.WriteErr405(w, r, http.MethodGet, http.MethodPut)
		return
	}
	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.URLPathVote.L)
	if err != nil {
		return
	}
	switch {
	case r.Method == http.MethodGet && apiItems[0] == cmn.Proxy:
		t.httpproxyvote(w, r)
	case r.Method == http.MethodPut && apiItems[0] == cmn.Voteres:
		t.httpsetprimaryproxy(w, r)
	default:
		t.writeErrURL(w, r)
	}
}

////////////////////////////
// voting: common methods //
////////////////////////////

func (h *htrun) onPrimaryFail() {
	smap := h.owner.smap.get()
	if smap.validate() != nil {
		return
	}
	clone := smap.clone()
	glog.Infof("%s: primary %s has FAILED", h.si, clone.Primary)

	for {
		// use HRW ordering
		nextPrimaryProxy, err := cluster.HrwProxy(&clone.Smap, clone.Primary.ID())
		if err != nil {
			if !daemon.stopping.Load() {
				glog.Errorf("%s: failed to execute HRW selection, err: %v", h.si, err)
			}
			return
		}
		glog.Infof("%s: trying %s as the primary candidate", h.si, nextPrimaryProxy.ID())

		// If this proxy is the next primary proxy candidate, it starts the election directly.
		if nextPrimaryProxy.ID() == h.si.ID() {
			cos.Assert(h.si.IsProxy())
			glog.Infof("%s: starting election (candidate = self)", h.si)
			vr := &VoteRecord{
				Candidate: nextPrimaryProxy.ID(),
				Primary:   clone.Primary.ID(),
				StartTime: time.Now(),
				Initiator: h.si.ID(),
			}
			vr.Smap = clone
			h.electable.proxyElection(vr)
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
		// TODO: handle http.StatusServiceUnavailable from the candidate that is currently starting up
		//       (see httpRequestNewPrimary)
		if clone.GetProxy(nextPrimaryProxy.ID()) != nil {
			clone.delProxy(nextPrimaryProxy.ID())
		}
	}
}

// GET /v1/vote/proxy
func (h *htrun) httpproxyvote(w http.ResponseWriter, r *http.Request) {
	if _, err := h.checkRESTItems(w, r, 0, false, cmn.URLPathVoteProxy.L); err != nil {
		return
	}
	msg := VoteMessage{}
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		return
	}
	candidate := msg.Record.Candidate
	if candidate == "" {
		h.writeErrf(w, r, "%s: unexpected: empty candidate field [%v]", h.si, msg.Record)
		return
	}
	smap := h.owner.smap.get()
	if smap.Primary == nil {
		h.writeErrf(w, r, "%s: current primary undefined, %s", h.si, smap)
		return
	}
	currPrimaryID := smap.Primary.ID()
	if candidate == currPrimaryID {
		h.writeErrf(w, r, "%s: candidate %q _is_ the current primary, %s", h.si, candidate, smap)
		return
	}
	newSmap := msg.Record.Smap
	psi := newSmap.GetProxy(candidate)
	if psi == nil {
		h.writeErrf(w, r, "%s: candidate %q not present in the VoteRecord %s",
			h.si, candidate, newSmap)
		return
	}
	if !newSmap.isPresent(h.si) {
		h.writeErrf(w, r, "%s: not present in the VoteRecord %s", h.si, newSmap)
		return
	}

	if err := h.owner.smap.synchronize(h.si, newSmap, nil /*ms payload*/); err != nil {
		// double-checking errDowngrade
		if isErrDowngrade(err) {
			newSmap2 := h.owner.smap.get()
			psi2 := newSmap2.GetProxy(candidate)
			if psi2.Equals(psi) {
				err = nil // not an error - can vote Yes
			}
		}
		if err != nil {
			glog.Errorf("%s: failed to synch %s, err %v - voting No", h.si, newSmap, err)
			if _, err := w.Write([]byte(VoteNo)); err != nil {
				glog.Errorf("%s: failed to write a No vote: %v", h.si, err)
			}
			return
		}
	}

	vote, err := h.voteOnProxy(psi.ID(), currPrimaryID)
	if err != nil {
		h.writeErr(w, r, err)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s: voted '%v' for %s", h.si, vote, psi)
	}

	if vote {
		_, err = w.Write([]byte(VoteYes))
		if err != nil {
			h.writeErrf(w, r, "%s: failed to write Yes vote: %v", h.si, err)
		}
	} else {
		_, err = w.Write([]byte(VoteNo))
		if err != nil {
			h.writeErrf(w, r, "%s: failed to write No vote: %v", h.si, err)
		}
	}
}

// PUT /v1/vote/result
func (h *htrun) httpsetprimaryproxy(w http.ResponseWriter, r *http.Request) {
	if _, err := h.checkRESTItems(w, r, 0, false, cmn.URLPathVoteVoteres.L); err != nil {
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
		h.writeErr(w, r, err)
	}
}

func (h *htrun) _votedPrimary(ctx *smapModifier, clone *smapX) error {
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

func (h *htrun) sendElectionRequest(vr *VoteInitiation, nextPrimaryProxy *cluster.Snode) (err error) {
	msg := VoteInitiationMessage{Request: *vr}
	body := cos.MustMarshal(&msg)
	args := callArgs{
		si: nextPrimaryProxy,
		req: cmn.ReqArgs{
			Method: http.MethodPut,
			Base:   nextPrimaryProxy.IntraControlNet.DirectURL,
			Path:   cmn.URLPathVoteInit.S,
			Body:   body,
		},
		timeout: cmn.DefaultTimeout,
	}
	res := h.call(args)
	err = res.err
	freeCR(res)
	if err == nil || !cos.IsRetriableConnErr(err) {
		return
	}
	// retry
	sleepTime := cmn.GCO.Get().Timeout.CplaneOperation.D() / 2
	for i := 0; i < maxRetryElectReq; i++ {
		time.Sleep(sleepTime)
		res = h.call(args)
		err = res.err
		freeCR(res)
		if err == nil {
			return
		}
		if !cos.IsRetriableConnErr(err) {
			break
		}
		sleepTime += sleepTime / 2
	}
	if !daemon.stopping.Load() {
		glog.Errorf("%s: failed to request election from the _next_ primary %s: %v",
			h.si, nextPrimaryProxy.StringEx(), err)
	}
	return
}

func (h *htrun) voteOnProxy(daemonID, currPrimaryID string) (bool, error) {
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
