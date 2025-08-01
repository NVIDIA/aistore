// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/NVIDIA/aistore/xact/xs"
)

const (
	VoteYes = "YES"
	VoteNo  = "NO"
)

const maxRetryElectReq = 3

type (
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
		err      error
		daemonID string
		yes      bool
	}
)

func voteInProgress() (xele core.Xact) {
	flt := xreg.Flt{Kind: apc.ActElection}
	if e := xreg.GetRunning(&flt); e != nil {
		xele = e.Get()
	}
	return
}

//
// voting: proxy
//

// [METHOD] /v1/vote
func (p *proxy) voteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPut {
		cmn.WriteErr405(w, r, http.MethodGet, http.MethodPut)
		return
	}
	apiItems, err := p.parseURL(w, r, apc.URLPathVote.L, 1, false)
	if err != nil {
		return
	}
	item := apiItems[0]
	if !p.NodeStarted() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	// MethodGet
	if r.Method == http.MethodGet {
		if item != apc.Proxy {
			p.writeErrURL(w, r)
			return
		}
		p.httpgetvote(w, r)
		return
	}
	// MethodPut
	switch item {
	case apc.Voteres:
		p.httpsetprimary(w, r)
	case apc.VoteInit:
		p.httpelect(w, r)
	case apc.PriStop:
		callerID := r.Header.Get(apc.HdrCallerID)
		p.onPrimaryDown(p, callerID)
	default:
		p.writeErrURL(w, r)
	}
}

// PUT /v1/vote/init (via sendElectionRequest)
func (p *proxy) httpelect(w http.ResponseWriter, r *http.Request) {
	const tag = "[httpelect]"
	var (
		pname  = p.String()
		pnameC = pname + ":"
	)
	if _, err := p.parseURL(w, r, apc.URLPathVoteInit.L, 0, false); err != nil {
		return
	}
	msg := VoteInitiationMessage{}
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		return
	}
	newSmap := msg.Request.Smap
	if err := newSmap.validate(); err != nil {
		p.writeErrf(w, r, "%s %s: invalid %s in the Vote Request, err: %v", tag, pname, newSmap, err)
		return
	}
	smap := p.owner.smap.get()
	caller := r.Header.Get(apc.HdrCallerName)

	nlog.Infoln(tag, pnameC, "receive", newSmap.StringEx(), "from", caller, "local [", smap.StringEx(), "]")

	if !newSmap.isPresent(p.si) {
		p.writeErrf(w, r, "%s %s: not present in the Vote Request, %s", tag, pname, newSmap)
		return
	}
	debug.Assert(!newSmap.isPrimary(p.si))

	if err := p.owner.smap.synchronize(p.si, newSmap, nil /*ms payload*/, p.htrun.smapUpdatedCB); err != nil {
		if isErrDowngrade(err) {
			psi := newSmap.GetProxy(msg.Request.Candidate)
			psi2 := p.owner.smap.get().GetProxy(msg.Request.Candidate)
			if psi2.Eq(psi) {
				err = nil
			}
		}
		if err != nil {
			p.writeErr(w, r, cmn.NewErrFailedTo(p, "synchronize", newSmap.StringEx(), err))
			return
		}
	}

	smap = p.owner.smap.get()
	psi, err := smap.HrwProxy(smap.Primary.ID() /*skip*/)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}

	// proceed with election iff:
	if psi.ID() != p.SID() {
		nlog.Warningln(tag, pnameC, "not the next in line [ vs", psi.StringEx(), "]")
		return
	}
	if !p.ClusterStarted() {
		nlog.Warningln(tag, pnameC, "not ready yet to be elected - starting up")
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	vr := &VoteRecord{
		Candidate: msg.Request.Candidate,
		Primary:   msg.Request.Primary,
		StartTime: time.Now(),
		Initiator: p.SID(),
	}
	// include resulting Smap in the response
	vr.Smap = p.owner.smap.get()

	// run xaction
	// minimal and, unlike all the rest (target) xactions, not visible via API (TODO)
	go p.startElection(vr, false /*cloned*/)
}

// Election Functions

func (p *proxy) startElection(vr *VoteRecord, cloned bool) {
	smap := p.owner.smap.get()
	if smap.isPrimary(p.si) {
		nlog.Infoln(p.String(), "already in primary state [", cloned, "]")
		return
	}
	if !cloned {
		vr.Smap = smap
	}
	rns := xreg.RenewElection()
	if rns.Err != nil {
		nlog.Errorf("FATAL: %s failed to start election: %+v %v", p, vr, rns.Err) // (unlikely)
		debug.AssertNoErr(rns.Err)
		return
	}
	if rns.IsRunning() {
		return
	}
	xctn := rns.Entry.Get()
	xele, ok := xctn.(*xs.Election)
	debug.Assert(ok)

	nlog.Infoln(xele.Name())

	p.elect(vr, xele)
	xele.Finish()
}

const retryCurPrimary = 2

func (p *proxy) elect(vr *VoteRecord, xele *xs.Election) {
	var (
		smap       *smapX
		err        error
		curPrimary = vr.Smap.Primary
		curName    = curPrimary.StringEx()
		pname      = p.String()
		pnameC     = pname + ":"
		tout       = cmn.Rom.CplaneOperation()
	)
	// 1. ping the current primary (not using `apc.QparamAskPrimary` as the latter might be transitioning)
	for i := range retryCurPrimary {
		if i > 0 {
			runtime.Gosched()
		}
		smap = p.owner.smap.get()
		if smap.version() > vr.Smap.version() {
			nlog.Warningln(pnameC, "[", smap.StringEx(), "version greater than Vote Req's", vr.Smap.StringEx(),
				"] - moving back to idle")
			return
		}

		_, _, err = p.reqHealth(curPrimary, tout, nil /*ask primary*/, smap, true /*retry via pub-addr, if different*/)
		if err == nil {
			break
		}
	}
	if err == nil {
		// move back to idle
		query := url.Values{apc.QparamAskPrimary: []string{"true"}}

		_, _, err = p.reqHealth(curPrimary, tout, query /*ask primary*/, smap, true /* ditto */)

		if err == nil {
			nlog.Infoln(pnameC, "the current primary", curName, "is up, moving back to idle")
		} else {
			errV := fmt.Errorf("%s: current primary(?) %s responds but does not consider itself primary", pname, curName)
			xele.AddErr(errV, 0)
		}
		return
	}

	nlog.Warningln(pnameC, "primary", curName, "is confirmed down: [", err, "] moving to election state phase 1 (prepare)")

	// 2. election phase 1
	elected, votingErrors := p.electPhase1(vr)
	if !elected {
		errV := fmt.Errorf("%s: election phase 1 (prepare) failed: primary still %s w/ status unknown", pname, curName)
		xele.AddErr(errV, 0)

		smap = p.owner.smap.get()
		if smap.version() > vr.Smap.version() {
			nlog.Warningln(pnameC, "[", smap.StringEx(), "version greater than vote-record's", vr.Smap.StringEx(),
				"] - moving back to idle")
			return
		}

		// best-effort
		svm, _, slowp := p.bcastMaxVer(smap, nil, nil)
		if svm.Smap != nil && !slowp {
			if svm.Smap.UUID == smap.UUID && svm.Smap.version() > smap.version() && svm.Smap.validate() == nil {
				nlog.Warningln(pnameC, "upgrading local", smap.StringEx(), "to cluster max-ver", svm.Smap.StringEx())

				if svm.Smap.Primary.ID() != smap.Primary.ID() {
					nlog.Warningln(pnameC, "new primary", svm.Smap.Primary.StringEx(), "is already elected ...")
				}
				errV := p.owner.smap.synchronize(p.si, svm.Smap, nil /*ms payload*/, p.smapUpdatedCB)
				if errV != nil {
					cos.ExitLog(errV)
				}
			}
		}

		return
	}

	// 3. election phase 2
	nlog.Infoln(pnameC, "moving to election state phase 2 (commit)")
	confirmationErrors := p.electPhase2(vr)
	for sid := range confirmationErrors {
		if !votingErrors.Contains(sid) {
			errV := fmt.Errorf("%s: error confirming the election: %s was healthy when voting", pname, sid)
			xele.AddErr(errV, 0)
		}
	}

	// 4. become!
	nlog.Infoln(pnameC, "becoming primary")
	p.becomeNewPrimary(vr.Primary /*proxyIDToRemove*/)
}

// phase 1: prepare (via simple majority voting)
func (p *proxy) electPhase1(vr *VoteRecord) (winner bool, errors cos.StrSet) {
	var (
		resCh = p.requestVotes(vr)
		y, n  int
	)
	for res := range resCh {
		if res.err != nil {
			if errors == nil {
				errors = cos.NewStrSet(res.daemonID)
			} else {
				errors.Set(res.daemonID)
			}
			n++
		} else {
			if cmn.Rom.FastV(4, cos.SmoduleAIS) {
				nlog.Infof("Node %s responded with (winner: %t)", res.daemonID, res.yes)
			}
			if res.yes {
				y++
			} else {
				n++
			}
		}
	}

	winner = y > n || (y+n == 0) // No Votes: Default Winner
	nlog.Infof("Vote Results:\n Y: %d, N: %d\n Victory: %t\n", y, n, winner)
	return
}

func (p *proxy) requestVotes(vr *VoteRecord) chan voteResult {
	var (
		msg = VoteMessage{Record: *vr}
		q   = url.Values{}
	)
	q.Set(apc.QparamPrimaryCandidate, p.SID())
	args := allocBcArgs()
	args.req = cmn.HreqArgs{
		Method: http.MethodGet,
		Path:   apc.URLPathVoteProxy.S,
		Body:   cos.MustMarshal(&msg),
		Query:  q,
	}
	args.to = core.AllNodes
	results := p.bcastGroup(args)
	freeBcArgs(args)
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
				yes:      VoteYes == cos.UnsafeS(res.bytes),
				daemonID: res.si.ID(),
				err:      nil,
			}
		}
	}
	freeBcastRes(results)
	close(resCh)
	return resCh
}

// phase 2: confirm and commit
func (p *proxy) electPhase2(vr *VoteRecord) cos.StrSet {
	var (
		errors = cos.StrSet{}
		msg    = &VoteResultMessage{
			VoteResult{
				Candidate: vr.Candidate,
				Primary:   vr.Primary,
				Smap:      vr.Smap,
				StartTime: time.Now(),
				Initiator: p.SID(),
			},
		}
	)
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathVoteVoteres.S, Body: cos.MustMarshal(msg)}
	args.to = core.AllNodes
	results := p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		nlog.Warningf("%s: failed to confirm election with %s: %v", p, res.si.StringEx(), res.err)
		errors.Set(res.si.ID())
	}
	freeBcastRes(results)
	return errors
}

//
// voting: target
//

// [METHOD] /v1/vote
func (t *target) voteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPut {
		cmn.WriteErr405(w, r, http.MethodGet, http.MethodPut)
		return
	}
	apiItems, err := t.parseURL(w, r, apc.URLPathVote.L, 1, false)
	if err != nil {
		return
	}
	switch {
	case r.Method == http.MethodGet && apiItems[0] == apc.Proxy:
		t.httpgetvote(w, r)
	case r.Method == http.MethodPut && apiItems[0] == apc.Voteres:
		t.httpsetprimary(w, r)
	default:
		t.writeErrURL(w, r)
	}
}

//
// voting: common methods
//

func (h *htrun) onPrimaryDown(self *proxy, callerID string) {
	smap := h.owner.smap.get()
	if smap.validate() != nil {
		return
	}
	clone := smap.clone()
	s := "via keepalive"
	if callerID != "" {
		s = "via direct call"
		if callerID != clone.Primary.ID() {
			nlog.Errorf("%s (%s): non-primary caller reporting primary down (%s, %s, %s)",
				h, s, callerID, clone.Primary.StringEx(), smap)
			return
		}
	}
	nlog.Infof("%s (%s): primary %s is no longer online and must be reelected", h, s, clone.Primary.StringEx())

	for {
		if nlog.Stopping() {
			return
		}
		// use HRW ordering
		nextPrimaryProxy, err := clone.HrwProxy(clone.Primary.ID())
		if err != nil {
			if !nlog.Stopping() {
				nlog.Errorf("%s failed to execute HRW selection: %v", h, err)
			}
			return
		}

		// If this proxy is the next primary proxy candidate, it starts the election directly.
		if nextPrimaryProxy.ID() == h.si.ID() {
			debug.Assert(h.si.IsProxy())
			debug.Assert(h.SID() == self.SID())
			nlog.Infof("%s: starting election (candidate = self)", h)
			vr := &VoteRecord{
				Candidate: nextPrimaryProxy.ID(),
				Primary:   clone.Primary.ID(),
				StartTime: time.Now(),
				Initiator: h.si.ID(),
			}
			vr.Smap = clone
			self.startElection(vr, true /*cloned*/)
			return
		}

		nlog.Infof("%s: trying %s as the new primary candidate", h, meta.Pname(nextPrimaryProxy.ID()))

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
		// (see httpelect)
		if clone.GetProxy(nextPrimaryProxy.ID()) != nil {
			clone.delProxy(nextPrimaryProxy.ID())
		}
	}
}

// GET /v1/vote/proxy
func (h *htrun) httpgetvote(w http.ResponseWriter, r *http.Request) {
	if _, err := h.parseURL(w, r, apc.URLPathVoteProxy.L, 0, false); err != nil {
		return
	}
	msg := VoteMessage{}
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		return
	}
	candidate := msg.Record.Candidate
	if candidate == "" {
		h.writeErrf(w, r, "%s: unexpected: empty candidate field [%v]", h, msg.Record)
		return
	}
	smap := h.owner.smap.get()
	if smap.Primary == nil {
		h.writeErrf(w, r, "%s: current primary undefined, %s", h, smap)
		return
	}
	currPrimaryID := smap.Primary.ID()
	if candidate == currPrimaryID {
		h.writeErrf(w, r, "%s: candidate %q _is_ the current primary, %s", h, candidate, smap)
		return
	}
	newSmap := msg.Record.Smap
	psi := newSmap.GetProxy(candidate)
	if psi == nil {
		h.writeErrf(w, r, "%s: candidate %q not present in the VoteRecord %s", h, candidate, newSmap)
		return
	}
	if !newSmap.isPresent(h.si) {
		h.writeErrf(w, r, "%s: not present in the VoteRecord %s", h, newSmap)
		return
	}

	if err := h.owner.smap.synchronize(h.si, newSmap, nil /*ms payload*/, h.smapUpdatedCB); err != nil {
		// double-checking errDowngrade
		if isErrDowngrade(err) {
			newSmap2 := h.owner.smap.get()
			psi2 := newSmap2.GetProxy(candidate)
			if psi2.Eq(psi) {
				err = nil // not an error - can vote Yes
			}
		}
		if err != nil {
			nlog.Errorf("%s: failed to synch %s, err %v - voting No", h, newSmap, err)
			w.Header().Set(cos.HdrContentLength, strconv.Itoa(len(VoteNo)))
			_, err := w.Write(cos.UnsafeB(VoteNo))
			debug.AssertNoErr(err)
			return
		}
	}

	vote, err := h.voteOnProxy(psi.ID(), currPrimaryID)
	if err != nil {
		h.writeErr(w, r, err)
		return
	}
	if vote {
		w.Header().Set(cos.HdrContentLength, strconv.Itoa(len(VoteYes)))
		_, err = w.Write(cos.UnsafeB(VoteYes))
	} else {
		w.Header().Set(cos.HdrContentLength, strconv.Itoa(len(VoteNo)))
		_, err = w.Write(cos.UnsafeB(VoteNo))
	}
	debug.AssertNoErr(err)
}

// PUT /v1/vote/result
func (h *htrun) httpsetprimary(w http.ResponseWriter, r *http.Request) {
	if _, err := h.parseURL(w, r, apc.URLPathVoteVoteres.L, 0, false); err != nil {
		return
	}
	msg := VoteResultMessage{}
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		return
	}
	vr := msg.Result
	nlog.Infof("%s: received vote result: new primary %s (old %s)", h.si, vr.Candidate, vr.Primary)

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
		return &errNodeNotFound{h.si, clone, "cannot accept new primary election:", newPrimary}
	}
	clone.Primary = psi
	if oldPrimary != "" && clone.GetProxy(oldPrimary) != nil {
		clone.delProxy(oldPrimary)
	}
	nlog.Infof("%s: voted-primary result: %s", h.si, clone)
	return nil
}

func (h *htrun) sendElectionRequest(vr *VoteInitiation, nextPrimaryProxy *meta.Snode) (err error) {
	var (
		msg   = VoteInitiationMessage{Request: *vr}
		body  = cos.MustMarshal(&msg)
		cargs = allocCargs()
	)
	{
		cargs.si = nextPrimaryProxy
		cargs.req = cmn.HreqArgs{
			Method: http.MethodPut,
			Base:   nextPrimaryProxy.ControlNet.URL,
			Path:   apc.URLPathVoteInit.S,
			Body:   body,
		}
		cargs.timeout = apc.DefaultTimeout
	}
	res := h.call(cargs, vr.Smap)
	err = res.err
	freeCR(res)
	defer freeCargs(cargs)
	if err == nil {
		return nil
	}
	if !cos.IsRetriableConnErr(err) {
		return err
	}
	// retry
	sleep := cmn.Rom.CplaneOperation() / 2
	for range maxRetryElectReq {
		time.Sleep(sleep)
		res = h.call(cargs, vr.Smap)
		err = res.err
		freeCR(res)
		if err == nil {
			return nil
		}
		if !cos.IsRetriableConnErr(err) {
			break
		}
		sleep += sleep / 2
	}
	if !nlog.Stopping() {
		nlog.Errorf("%s: failed to request election from the _next_ primary %s: %v",
			h.si, nextPrimaryProxy.StringEx(), err)
	}
	return err
}

func (h *htrun) voteOnProxy(daemonID, currPrimaryID string) (bool, error) {
	// First: Check last keepalive timestamp. If the proxy was recently successfully reached,
	// this will always vote no, as we believe the original proxy is still alive.
	if !h.keepalive.timeToPing(currPrimaryID) {
		if cmn.Rom.FastV(4, cos.SmoduleAIS) {
			nlog.Warningf("Primary %s is still alive", currPrimaryID)
		}
		return false, nil
	}

	// Second: Vote according to whether or not the candidate is the Highest Random Weight remaining
	// in the Smap
	smap := h.owner.smap.get()
	nextPrimaryProxy, err := smap.HrwProxy(currPrimaryID)
	if err != nil {
		nlog.Errorln(err)
		return false, err
	}

	vote := nextPrimaryProxy.ID() == daemonID
	if cmn.Rom.FastV(4, cos.SmoduleAIS) {
		nlog.Infof("%s: voting '%t' for %s", h, vote, daemonID)
	}
	return vote, nil
}
