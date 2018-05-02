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

	v := h.smap.versionLocked()
	if v < msg.Record.Smap.version() {
		glog.Errorf("VoteRecord Smap Version (%v) is newer than local Smap (%v), updating Smap\n", msg.Record.Smap.version(), v)
		h.smap = &msg.Record.Smap
	} else if v > h.smap.version() {
		glog.Errorf("VoteRecord smap Version (%v) is older than local Smap (%v), voting No\n", msg.Record.Smap.version(), v)
		_, err = w.Write([]byte(VoteNo))
		if err != nil {
			h.invalmsghdlr(w, r, fmt.Sprintf("Error writing no vote: %v", err))
		}
		return
	}

	candidate := msg.Record.Candidate
	if candidate == "" {
		h.invalmsghdlr(w, r, fmt.Sprintln("Cannot request vote without Candidate field"))
		return
	}

	h.smap.lock()
	pi := h.smap.getProxy(candidate)
	h.smap.unlock()
	if pi == nil {
		h.invalmsghdlr(w, r, fmt.Sprintf("Candidate not present in proxy smap: %s (%v)", candidate, h.smap.Pmap))
		return
	}

	vote, err := h.voteOnProxy(pi.DaemonID)
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

	err = h.setPrimaryProxyLocked(vr.Candidate, vr.Primary, false)
	if err != nil {
		s := fmt.Sprintf("Error setting Primary Proxy: %v", err)
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
	if msg.Request.Smap.version() > p.smap.version() {
		p.smap = &msg.Request.Smap
	}

	psi, errstr := hrwProxyWithSkip(p.smap, p.proxysi.DaemonID)
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
	p.smap.copyLocked(&vr.Smap)

	// The election should be started in a goroutine, as it must not hang the http handler
	go p.proxyElection(vr)
}

//===================
//
// Election Functions
//
//===================

func (p *proxyrunner) proxyElection(vr *VoteRecord) {
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
	url := p.proxysi.DirectURL + "/" + Rversion + "/" + Rhealth
	proxyup, err := p.pingWithTimeout(url, ctx.config.Timeout.ProxyPing)
	if proxyup {
		// Move back to Idle state
		glog.Infoln("Moving back to Idle state")
		return
	}
	glog.Infof("%v: Primary Proxy %v is confirmed down\n", p.si.DaemonID, p.proxysi.DaemonID)
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
			glog.Warningf("Error response from target: %v", res.err)
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
	p.smap.lock()
	defer p.smap.unlock()
	chansize := p.smap.count() + p.smap.countProxies() - 1
	resch := make(chan voteResult, chansize)

	msg := VoteMessage{Record: *vr}
	jsbytes, err := json.Marshal(&msg)
	assert(err == nil, err)
	urlfmt := fmt.Sprintf("%%s/%s/%s/%s?%s=%s", Rversion, Rvote, Rproxy, URLParamPrimaryCandidate, p.si.DaemonID)
	callback := func(si *daemonInfo, r []byte, err error, _ int) {
		if err != nil {
			e := fmt.Errorf("Error reading response from %s(%s): %v", si.DaemonID, si.DirectURL, err)
			resch <- voteResult{yes: false, daemonID: si.DaemonID, err: e}
		} else {
			resch <- voteResult{yes: (VoteYes == Vote(r)), daemonID: si.DaemonID, err: nil}
		}
	}

	p.broadcast(urlfmt, http.MethodGet, jsbytes, p.smap, callback, ctx.config.Timeout.VoteRequest)
	close(resch)
	return resch
}

func (p *proxyrunner) confirmElectionVictory(vr *VoteRecord) map[string]bool {
	result := &VoteResult{
		Candidate: vr.Candidate,
		Primary:   vr.Primary,
		Smap:      vr.Smap,
		StartTime: time.Now(),
		Initiator: p.si.DaemonID,
	}

	p.smap.lock()
	defer p.smap.unlock()

	errch := make(chan ErrPair, p.smap.count()+p.smap.countProxies()-1)
	msg := VoteResultMessage{Result: *result}
	jsbytes, err := json.Marshal(&msg)
	assert(err == nil, err)
	urlfmt := fmt.Sprintf("%%s/%s/%s/%s", Rversion, Rvote, Rvoteres)
	callback := func(si *daemonInfo, _ []byte, err error, _ int) {
		if err != nil {
			e := fmt.Errorf("Error committing result for %s(%s): %v", si.DaemonID, si.DirectURL, err)
			errch <- ErrPair{err: e, daemonID: si.DaemonID}
		}
	}

	p.broadcast(urlfmt, http.MethodPut, jsbytes, p.smap, callback, ctx.config.Timeout.VoteRequest)
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
	p.primary = true
	psi := p.updateSmapPrimaryProxy(proxyidToRemove)
	p.proxysi = psi
	ctx.config.Proxy.Primary.ID = psi.DaemonID
	ctx.config.Proxy.Primary.URL = psi.DirectURL
	err := writeConfigFile()
	if err != nil {
		glog.Errorf("Error writing config file: %v", err)
	}
	p.synchronizeMaps(0, "", nil)
}

// updateSmapPrimaryProxy is used by becomePrimaryProxy to perform the smap updates that must be locked.
func (p *proxyrunner) updateSmapPrimaryProxy(proxyidToRemove string) *proxyInfo {
	p.smap.lock()
	defer p.smap.unlock()
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

	return psi
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
	glog.Infof("%v: Primary Proxy (%v @ %v) Failed\n", p.si.DaemonID, p.proxysi.DaemonID, p.proxysi.DirectURL)
	if p.smap.countProxies() <= 1 {
		glog.Warningf("No additional proxies to request vote from")
		return
	}
	nextPrimaryProxy, errstr := hrwProxyWithSkip(p.smap, p.proxysi.DaemonID)
	if errstr != "" {
		glog.Errorf("Failed to execute hrwProxy after Primary Proxy Failure: %v", errstr)
		return
	}

	if nextPrimaryProxy.DaemonID == p.si.DaemonID {
		// If this proxy is the next primary proxy candidate, it starts the election directly.
		vr := &VoteRecord{
			Candidate: nextPrimaryProxy.DaemonID,
			Primary:   p.proxysi.DaemonID,
			StartTime: time.Now(),
			Initiator: p.si.DaemonID,
		}
		p.smap.copyLocked(&vr.Smap)
		p.proxyElection(vr)
	} else {
		glog.Infof("%v: Requesting Election from %v", p.si.DaemonID, nextPrimaryProxy.DaemonID)
		vr := &VoteInitiation{
			Candidate: nextPrimaryProxy.DaemonID,
			Primary:   p.proxysi.DaemonID,
			StartTime: time.Now(),
			Initiator: p.si.DaemonID,
		}
		p.smap.copyLocked(&vr.Smap)
		p.sendElectionRequest(vr, nextPrimaryProxy)
	}
}

func (t *targetrunner) onPrimaryProxyFailure() {
	glog.Infof("%v: Primary Proxy (%v @ %v) Failed\n", t.si.DaemonID, t.proxysi.DaemonID, t.proxysi.DirectURL)

	nextPrimaryProxy, errstr := hrwProxyWithSkip(t.smap, t.proxysi.DaemonID)
	if errstr != "" {
		glog.Errorf("Failed to execute hrwProxy after Primary Proxy Failure: %v", errstr)
	}

	if nextPrimaryProxy == nil {
		// There is only one proxy, so we cannot select a next in line
		glog.Warningf("Primary Proxy failed, but there are no candidates to fall back on.")
		return
	}

	vr := &VoteInitiation{
		Candidate: nextPrimaryProxy.DaemonID,
		Primary:   t.proxysi.DaemonID,
		StartTime: time.Now(),
		Initiator: t.si.DaemonID,
	}
	t.smap.copyLocked(&vr.Smap)
	t.sendElectionRequest(vr, nextPrimaryProxy)
}

func (h *httprunner) sendElectionRequest(vr *VoteInitiation, nextPrimaryProxy *proxyInfo) {
	url := nextPrimaryProxy.DirectURL + "/" + Rversion + "/" + Rvote + "/" + Rvoteinit
	msg := VoteInitiationMessage{Request: *vr}
	jsbytes, err := json.Marshal(&msg)
	assert(err == nil, err)

	_, err, _, _ = h.call(&nextPrimaryProxy.daemonInfo, url, http.MethodPut, jsbytes)
	if err != nil {
		glog.Errorf("Failed to request election from next Primary Proxy: %v", err)
		return
	}
}

func (h *httprunner) voteOnProxy(daemonID string) (bool, error) {
	// First: Check last keepalive timestamp. If the proxy was recently successfully reached,
	// this will always vote no, as we believe the original proxy is still alive.
	lastKeepaliveTime := h.kalive.getTimestamp(h.proxysi.DaemonID)
	timeSinceLastKalive := time.Since(lastKeepaliveTime)
	if timeSinceLastKalive < ctx.config.Periodic.KeepAliveTime/2 {
		// KeepAliveTime/2 is the expected amount time since the last keepalive was sent
		return false, nil
	}

	// Second: Vote according to whether or not the candidate is the Highest Random Weight remaining
	// in the Smap
	hrwmax, errstr := hrwProxyWithSkip(h.smap, h.proxysi.DaemonID)
	if errstr != "" {
		return false, fmt.Errorf("Error executing HRW: %v", errstr)
	}
	return hrwmax.DaemonID == daemonID, nil
}

func (h *httprunner) setPrimaryProxyAndSmap(smap *Smap) error {
	smapLock.Lock()
	defer smapLock.Unlock()
	return h.setPrimaryProxyAndSmapUnlocked(smap)
}
func (h *httprunner) setPrimaryProxyAndSmapUnlocked(smap *Smap) error {
	h.smap = smap
	return h.setPrimaryProxy(smap.ProxySI.DaemonID, "" /* primaryToRemove */, false /* prepare */)
}

func (h *httprunner) setPrimaryProxyLocked(newPrimaryProxy, primaryToRemove string, prepare bool) error {
	h.smap.lock()
	defer h.smap.unlock()
	return h.setPrimaryProxy(newPrimaryProxy, primaryToRemove, prepare)
}

// Sets the primary proxy to the proxy in the cluster map with the ID newPrimaryProxy.
// Removes primaryToRemove from the cluster map, if primaryToRemove is provided.
// Caller must lock smapLock
func (h *httprunner) setPrimaryProxy(newPrimaryProxy, primaryToRemove string, prepare bool) error {
	proxyinfo, ok := h.smap.Pmap[newPrimaryProxy]
	if !ok {
		return fmt.Errorf("New Primary Proxy not present in proxy smap: %s", newPrimaryProxy)
	} else if proxyinfo == nil {
		return fmt.Errorf("New Primary Proxy nil in Smap: %v", newPrimaryProxy)
	}
	if prepare {
		// If prepare=true, return before making any changes
		return nil
	}
	proxyinfo.Primary = true
	h.proxysi = proxyinfo
	if primaryToRemove != "" {
		h.smap.delProxy(primaryToRemove)
	}
	h.smap.ProxySI = proxyinfo
	ctx.config.Proxy.Primary.ID = proxyinfo.DaemonID
	ctx.config.Proxy.Primary.URL = proxyinfo.DirectURL
	go func() {
		glog.Infof("Set primary proxy: %v (prepare: %t)", newPrimaryProxy, prepare)
		err := writeConfigFile()
		if err != nil {
			glog.Errorf("Error writing config file: %v", err)
		}
	}()
	return nil
}

func (p *proxyrunner) pingWithTimeout(url string, timeout time.Duration) (bool, error) {
	_, err, _, _ := p.call(nil, url, http.MethodGet, nil, timeout)
	if err == nil {
		// There is no issue with the current Primary Proxy
		return true, nil
	}
	if err == context.DeadlineExceeded || IsErrConnectionRefused(err) {
		// Then the proxy is unreachable
		return false, nil
	}
	return false, err
}
