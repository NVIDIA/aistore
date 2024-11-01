// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/memsys"
	jsoniter "github.com/json-iterator/go"
)

//
// [TODO] retry to pub-addr, if different (currently, cmn.NetIntraControl only; see palive.retry)
//

// Metasync provides two methods to the rest of the `ais` code:
// * sync - to synchronize cluster-level metadata (the main method)
// * becomeNonPrimary - to be called when the current primary becomes non-primary
//
// All other methods and the metasync's own state are private and internal.
//
// Method `do()` does most of the work (comments inline).
//
// REVS (see interface below) stands for REplicated, Versioned and Shared/Synchronized.
//
// A REVS is an object that represents a certain kind of cluster-wide metadata and
// must be consistently replicated across the entire cluster. To that end, the
// "metasyncer" provides a generic transport to send an arbitrary payload that
// combines any number of data units having the following layout:
//
//         (shared-replicated-object, associated action-message),
//
// where `associated action-message` (aisMsg) provides receivers with the operation
// ("action") and other relevant context.
//
// Further, the metasyncer:
//
// 1) tracks the last synchronized REVS versions
// 2) makes sure all version updates are executed strictly in the non-decremental
//    order
// 3) makes sure that nodes that join the cluster get updated with the current set
//    of REVS objects
// 4) handles failures to update existing nodes, by periodically retrying
//    pending synchronizations (for as long as those members remain in the
//    most recent and current cluster map).
//
// Last but not the least, metasyncer checks that only the currently elected
// leader (aka "primary proxy") distributes the REVS objects, thus providing for
// simple serialization of the versioned updates.
//
// The usage is easy - there is a single sync() method that accepts variable
// number of parameters. Example sync-ing Smap and BMD
// asynchronously:
//
// metasyncer.sync(smapOwner.get(), action1, owner.bmd.get(), action2)
//
// To block until all the replicas get delivered:
//
// wg = metasyncer.sync(...)
// wg.Wait()
//
// On the receiving side, the payload (see above) gets extracted, validated,
// version-compared, and the corresponding Rx handler gets invoked
// with additional information that includes the per-replica action message.

const (
	revsSmapTag  = "Smap"
	revsRMDTag   = "RMD"
	revsBMDTag   = "BMD"
	revsConfTag  = "Conf"
	revsTokenTag = "token"
	revsEtlMDTag = "EtlMD"

	revsMaxTags   = 6         // NOTE
	revsActionTag = "-action" // prefix revs tag
)

const (
	reqSync = iota
	reqNotify
)

const failsync = "failing to sync"

const (
	retrySyncRefused   = 4
	retryNotifyRefused = 2
)

const workChanCap = 32

type (
	revs interface {
		tag() string         // enum { revsSmapTag, ... }
		version() int64      // the version
		marshal() (b []byte) // marshals the revs
		jit(p *proxy) revs   // current (just-in-time) instance
		sgl() *memsys.SGL    // jsp-encoded SGL
		String() string      // smap.String(), etc.
	}
	revsPair struct {
		revs revs
		msg  *aisMsg
	}
	revsReq struct {
		wg        *sync.WaitGroup
		failedCnt *atomic.Int32
		pairs     []revsPair
		ty        int // enum: { reqSync, reqNotify }
	}
	msPayload map[string][]byte     // tag => revs' body
	ndRevs    map[string]int64      // tag => version (see nodesRevs)
	tagl      map[int64]*memsys.SGL // version => SGL jsp-formatted

	// main
	metasyncer struct {
		p            *proxy            // parent
		nodesRevs    map[string]ndRevs // cluster-wide node ID => ndRevs sync-ed
		sgls         map[string]tagl   // tag => (version => SGL)
		lastSynced   map[string]revs   // tag => revs last/current sync-ed
		stopCh       chan struct{}     // stop channel
		workCh       chan revsReq      // work channel
		retryTimer   *time.Timer       // timer to sync pending
		timerStopped bool              // true if retryTimer has been stopped, false otherwise
	}
	// metasync Rx structured error
	errMsync struct {
		Message string            `json:"message"`
		Cii     cos.NodeStateInfo `json:"nsti"`
	}
)

// interface guard
var _ cos.Runner = (*metasyncer)(nil)

func (req revsReq) isNil() bool { return len(req.pairs) == 0 }

////////////////
// metasyncer //
////////////////

func (*metasyncer) Name() string { return "metasyncer" }

func newMetasyncer(p *proxy) (y *metasyncer) {
	y = &metasyncer{p: p}
	y.nodesRevs = make(map[string]ndRevs, 8)
	y.inigls()
	y.lastSynced = make(map[string]revs, revsMaxTags)

	y.stopCh = make(chan struct{}, 1)
	y.workCh = make(chan revsReq, workChanCap)

	y.retryTimer = time.NewTimer(time.Hour)
	y.retryTimer.Stop()
	y.timerStopped = true
	return
}

func (y *metasyncer) Run() error {
	nlog.Infoln("Starting", y.Name())
	for {
		select {
		case revsReq, ok := <-y.workCh:
			if !ok {
				break
			}
			if revsReq.isNil() { // <== see becomeNonPrimary()
				y.nodesRevs = make(map[string]ndRevs)
				y.free()
				y.lastSynced = make(map[string]revs)
				y.retryTimer.Stop()
				y.timerStopped = true
				break
			}
			failedCnt := y.do(revsReq.pairs, revsReq.ty)
			if revsReq.wg != nil {
				if revsReq.failedCnt != nil {
					revsReq.failedCnt.Store(int32(failedCnt))
				}
				revsReq.wg.Done()
			}

			// timed retry, via handlePending()
			if y.timerStopped && failedCnt > 0 && revsReq.ty != reqNotify {
				config := cmn.GCO.Get()
				y.retryTimer.Reset(config.Periodic.RetrySyncTime.D())
				y.timerStopped = false
			}
			for _, revs := range y.lastSynced {
				y.delold(revs, 1)
			}
		case <-y.retryTimer.C:
			failedCnt := y.handlePending()
			if failedCnt > 0 {
				config := cmn.GCO.Get()
				y.retryTimer.Reset(config.Periodic.RetrySyncTime.D())
				y.timerStopped = false

				if l, c := len(y.workCh), cap(y.workCh); l > c/2 {
					nlog.Errorln("Warning:", y.p.String(), "[hp]:", cos.ErrWorkChanFull, "len", l, "cap", c,
						"failed", failedCnt)
				}
			} else {
				y.timerStopped = true
			}
		case <-y.stopCh:
			y.retryTimer.Stop()
			return nil
		}
	}
}

func (y *metasyncer) Stop(err error) {
	if err == nil {
		nlog.Infoln("Stopping", y.Name())
	} else {
		nlog.Infoln("Stopping", y.Name()+":", err)
	}
	y.stopCh <- struct{}{}
	close(y.stopCh)
}

// notify only targets - see bcastTo below
func (y *metasyncer) notify(wait bool, pair revsPair) (failedCnt int) {
	var (
		failedCntAtomic = atomic.NewInt32(0)
		req             = revsReq{pairs: []revsPair{pair}, ty: reqNotify}
	)
	if y.isPrimary() != nil {
		return
	}
	if wait {
		req.wg = &sync.WaitGroup{}
		req.wg.Add(1)
		req.failedCnt = failedCntAtomic
	}
	y.workCh <- req

	if wait {
		req.wg.Wait()
		failedCnt = int(failedCntAtomic.Load())
	}
	return
}

func (y *metasyncer) sync(pairs ...revsPair) *sync.WaitGroup {
	debug.Assert(len(pairs) > 0)
	req := revsReq{pairs: pairs}
	req.wg = &sync.WaitGroup{}
	if err := y.isPrimary(); err != nil {
		nlog.Errorln(err)
		return req.wg
	}
	req.wg.Add(1)
	req.ty = reqSync
	y.workCh <- req
	return req.wg
}

// become non-primary (to serialize cleanup of the internal state and stop the timer)
func (y *metasyncer) becomeNonPrimary() {
drain:
	for {
		select {
		case revsReq, ok := <-y.workCh:
			if ok && revsReq.wg != nil {
				revsReq.wg.Done()
			}
		default:
			break drain
		}
	}
	y.workCh <- revsReq{}
	nlog.Infoln(y.p.String(), "becoming non-primary")
}

// main method; see top of the file; returns number of "sync" failures
func (y *metasyncer) do(pairs []revsPair, reqT int) (failedCnt int) {
	var (
		refused meta.NodeMap
		newTIDs []string
		method  = http.MethodPut
	)
	if reqT == reqNotify {
		method = http.MethodPost
	}
	if nlog.Stopping() {
		return 0
	}

	// step: build payload and update last sync-ed
	payload := make(msPayload, 2*len(pairs))
	for _, pair := range pairs {
		var (
			revsBody []byte
			msg, tag = pair.msg, pair.revs.tag()
			revs     = pair.revs
		)
		if reqT == reqNotify {
			revsBody = revs.marshal()
		} else {
			revs = y.jit(pair)

			// in an unlikely event, the revs may still carry sgl that has been freed
			// via becomeNonPrimary => y.free() sequence; checking sgl.IsNil() is a compromise
			if sgl := revs.sgl(); sgl != nil && !sgl.IsNil() {
				// fast path
				revsBody = sgl.Bytes()
				y.addnew(revs)
			} else {
				// slow path
				revsBody = revs.marshal()
				if sgl := revs.sgl(); sgl != nil {
					y.addnew(revs)
				}
			}
			y.lastSynced[tag] = revs
		}
		if tag == revsRMDTag {
			md := revs.(*rebMD)
			newTIDs = md.TargetIDs
		}
		payload[tag] = revsBody                           // payload
		payload[tag+revsActionTag] = cos.MustMarshal(msg) // action message always on the wire even when empty
	}

	// step: bcast
	var (
		urlPath = apc.URLPathMetasync.S
		body    = payload.marshal(y.p.gmm)
		to      = core.AllNodes
		smap    = y.p.owner.smap.get()
		retries = retrySyncRefused // connection-refused
	)
	defer body.Free()

	if reqT == reqNotify {
		to = core.Targets
		retries = retryNotifyRefused
	}
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: method, Path: urlPath, BodyR: body}
	args.smap = smap
	args.timeout = cmn.Rom.MaxKeepalive() // making exception for this critical op
	args.to = to
	args.ignoreMaintenance = true
	results := y.p.bcastGroup(args)
	freeBcArgs(args)

	// step: count failures and fill-in refused
	for _, res := range results {
		if res.err == nil {
			if reqT == reqSync {
				y.syncDone(res.si, pairs)
			}
			continue
		}
		sname := res.si.StringEx()
		err := res.unwrap()
		// failing to sync - not retrying, ignoring
		if res.si.InMaintOrDecomm() {
			nlog.Infof("%s: %s %s (flags %s): %v(%d)", y.p, failsync, sname, res.si.Fl2S(), err, res.status)
			continue
		}
		// - retrying, counting
		if cos.IsRetriableConnErr(err) || cos.StringInSlice(res.si.ID(), newTIDs) { // always retry newTIDs (joining)
			if refused == nil {
				refused = make(meta.NodeMap, 2)
			}
			refused.Add(res.si)
		} else {
			nlog.Warningf("%s: %s %s: %v(%d)", y.p, failsync, sname, err, res.status)
			failedCnt++
		}
	}
	freeBcastRes(results)

	// step: handle connection-refused right away
	lr := len(refused)
	for range retries {
		if len(refused) == 0 {
			if lr > 0 {
				nlog.Infof("%s: %d node%s sync-ed", y.p, lr, cos.Plural(lr))
			}
			break
		}
		time.Sleep(cmn.Rom.CplaneOperation())
		smap = y.p.owner.smap.get()
		if !smap.isPrimary(y.p.si) {
			y.becomeNonPrimary()
			return 0
		}
		if !y.handleRefused(method, urlPath, body, refused, pairs, smap) {
			break
		}
	}

	// step: housekeep and return new pending
	smap = y.p.owner.smap.get()
	for sid := range y.nodesRevs {
		si := smap.GetActiveNode(sid)
		if si == nil {
			delete(y.nodesRevs, sid)
		}
	}
	failedCnt += len(refused)
	return failedCnt
}

func (y *metasyncer) jit(pair revsPair) revs {
	var (
		s              string
		revs, msg, tag = pair.revs, pair.msg, pair.revs.tag()
		jitRevs        = revs.jit(y.p)
		skipping       bool
	)
	if jitRevs != nil && jitRevs.version() > revs.version() {
		revs = jitRevs
		skipping = true
	}
	if msg.Action != "" {
		s = ", " + msg.String()
	}
	if skipping {
		nlog.Infof("%s: newer %s v%d%s - skipping %s", y.p, tag, jitRevs.version(), s, revs)
	} else {
		nlog.Infof("%s: %s v%d%s", y.p, tag, revs.version(), s)
	}
	return revs
}

// keeping track of per-daemon versioning - TODO: extend to take care of aisMsg where pairs may be empty
func (y *metasyncer) syncDone(si *meta.Snode, pairs []revsPair) {
	ndr, ok := y.nodesRevs[si.ID()]
	smap := y.p.owner.smap.get()
	if smap.GetActiveNode(si.ID()) == nil {
		if ok {
			delete(y.nodesRevs, si.ID())
		}
		return
	}
	if !ok {
		ndr = make(map[string]int64, revsMaxTags)
		y.nodesRevs[si.ID()] = ndr
	}
	for _, revsPair := range pairs {
		revs := revsPair.revs
		ndr[revs.tag()] = revs.version()
	}
}

func (y *metasyncer) handleRefused(method, urlPath string, body io.Reader, refused meta.NodeMap, pairs []revsPair, smap *smapX) (ok bool) {
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: method, Path: urlPath, BodyR: body}
	args.network = cmn.NetIntraControl
	args.timeout = cmn.Rom.MaxKeepalive()
	args.nodes = []meta.NodeMap{refused}
	args.nodeCount = len(refused)
	args.smap = smap
	results := y.p.bcastNodes(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err == nil {
			delete(refused, res.si.ID())
			y.syncDone(res.si, pairs)
			continue
		}
		// failing to sync
		if res.status == http.StatusConflict {
			if e := err2MsyncErr(res.err); e != nil {
				msg := fmt.Sprintf("%s [hr]: %s %s: %s [%v]", y.p, failsync, res.si, e.Message, e.Cii)
				if !y.remainPrimary(e, res.si, smap) {
					nlog.Errorln(msg + " - aborting")
					freeBcastRes(results)
					return false
				}
				nlog.Warningln(msg)
				continue
			}
		}
		nlog.Warningf("%s [hr]: %s %s: %v(%d)", y.p, failsync, res.si, res.unwrap(), res.status)
	}
	freeBcastRes(results)
	return true
}

// pending (map), if requested, contains only those daemons that need
// to get at least one of the most recently sync-ed tag-ed revs
func (y *metasyncer) _pending() (pending meta.NodeMap, smap *smapX) {
	smap = y.p.owner.smap.get()
	if !smap.isPrimary(y.p.si) {
		y.becomeNonPrimary()
		return
	}
	for _, serverMap := range []meta.NodeMap{smap.Tmap, smap.Pmap} {
		for _, si := range serverMap {
			if si.ID() == y.p.SID() {
				continue
			}
			ndr, ok := y.nodesRevs[si.ID()]
			if !ok {
				y.nodesRevs[si.ID()] = make(map[string]int64, revsMaxTags)
			} else {
				inSync := true
				for tag, revs := range y.lastSynced {
					v, ok := ndr[tag]
					if !ok || v < revs.version() {
						inSync = false
						break
					} else if v > revs.version() {
						// skip older versions (TODO: don't skip sending associated aisMsg)
						nlog.Errorf("v: %d; revs.version: %d", v, revs.version())
					}
				}
				if inSync {
					continue
				}
			}
			if pending == nil {
				pending = make(meta.NodeMap, 2)
			}
			pending.Add(si)
		}
	}
	return
}

// gets invoked when retryTimer fires; returns updated number of still pending
// using MethodPut since reqT here is always reqSync
func (y *metasyncer) handlePending() (failedCnt int) {
	pending, smap := y._pending()
	if len(pending) == 0 {
		nlog.Infoln("no pending revs - all good")
		return 0
	}
	if nlog.Stopping() {
		return 0
	}
	var (
		l       = len(y.lastSynced)
		payload = make(msPayload, 2*l)
		pairs   = make([]revsPair, 0, l)
		msg     = y.p.newAmsgStr("metasync: handle-pending", nil) // NOTE: same msg for all revs
		msgBody = cos.MustMarshal(msg)
	)
	for tag, revs := range y.lastSynced {
		debug.Assert(tag == revs.tag())
		if sgl := revs.sgl(); sgl != nil && !sgl.IsNil() {
			payload[tag] = sgl.Bytes()
		} else {
			payload[tag] = revs.marshal()
			if sgl := revs.sgl(); sgl != nil {
				y.addnew(revs)
			}
		}
		payload[tag+revsActionTag] = msgBody
		pairs = append(pairs, revsPair{revs, msg})
	}
	var (
		urlPath = apc.URLPathMetasync.S
		body    = payload.marshal(y.p.gmm)
		args    = allocBcArgs()
	)
	args.req = cmn.HreqArgs{Method: http.MethodPut, Path: urlPath, BodyR: body}
	args.network = cmn.NetIntraControl
	args.timeout = cmn.Rom.MaxKeepalive()
	args.nodes = []meta.NodeMap{pending}
	args.nodeCount = len(pending)
	args.smap = smap
	defer body.Free()
	results := y.p.bcastNodes(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err == nil {
			y.syncDone(res.si, pairs)
			continue
		}
		failedCnt++
		// failing to sync
		if res.status == http.StatusConflict {
			if e := err2MsyncErr(res.err); e != nil {
				msg := fmt.Sprintf("%s [hp]: %s %s: %s [%v]", y.p.si, failsync, res.si, e.Message, e.Cii)
				if !y.remainPrimary(e, res.si, smap) {
					// return zero so that the caller stops retrying (y.retryTimer)
					nlog.Errorln(msg + " - aborting")
					freeBcastRes(results)
					return 0
				}
				nlog.Warningln(msg)
				continue
			}
		}
		nlog.Warningf("%s [hp]: %s %s: %v(%d)", y.p, failsync, res.si, res.err, res.status)
	}
	freeBcastRes(results)
	return failedCnt
}

// cie and isPrimary checks versus remote clusterInfo
func (y *metasyncer) remainPrimary(e *errMsync, from *meta.Snode, smap *smapX) bool /*yes*/ {
	if !cos.IsValidUUID(e.Cii.Smap.UUID) || e.Cii.Smap.Version == 0 {
		return true
	}
	if e.Cii.Smap.UUID != smap.UUID {
		// FATAL: cluster integrity error (cie) - TODO: handle rogue nodes
		cos.ExitLogf("%s: split-brain uuid [%s %s] vs %v from %s", ciError(90), y.p.si, smap.StringEx(),
			e.Cii, from)
	}
	if e.Cii.Smap.Primary.ID == "" || e.Cii.Smap.Primary.ID == y.p.SID() {
		return true
	}
	if e.Cii.Smap.Version > smap.Version {
		nlog.Warningf("%s: detected primary change: %s vs %s [%v] from %s", y.p, smap.StringEx(),
			e.Message, e.Cii, from)
		y.becomeNonPrimary()
		return false
	}
	if e.Cii.Smap.Version < smap.Version {
		return true
	}
	nlog.Errorf("%s: [%s %s] vs %v from %s", ciError(90), y.p, smap.StringEx(), e.Cii, from)
	return true
}

func (y *metasyncer) isPrimary() (err error) {
	smap := y.p.owner.smap.get()
	if smap.isPrimary(y.p.si) {
		return
	}
	err = newErrNotPrimary(y.p.si, smap)
	nlog.Errorln(err)
	return
}

////////////
// y.sgls //
////////////

func (y *metasyncer) inigls() {
	y.sgls = make(map[string]tagl, revsMaxTags)
	y.sgls[revsSmapTag] = tagl{}
	y.sgls[revsBMDTag] = tagl{}
	y.sgls[revsConfTag] = tagl{}
}

func (y *metasyncer) free() {
	for _, tagl := range y.sgls {
		for _, sgl := range tagl {
			sgl.Free()
		}
	}
	y.inigls()
}

func (y *metasyncer) addnew(revs revs) {
	vgl, ok := y.sgls[revs.tag()]
	if !ok {
		vgl = tagl{}
		y.sgls[revs.tag()] = vgl
	}
	if sgl, ok := vgl[revs.version()]; ok {
		if sgl == revs.sgl() {
			return
		}
		// free the duplicate (created previously via "slow path")
		sgl.Free()
	}
	vgl[revs.version()] = revs.sgl()
}

func (y *metasyncer) delold(revs revs, except int64) {
	vgl, ok := y.sgls[revs.tag()]
	if !ok {
		return
	}
	for v, sgl := range vgl {
		if v < revs.version()-except {
			if !sgl.IsNil() {
				sgl.Free()
			}
			delete(vgl, v)
		}
	}
}

///////////////////////////
// metasync jsp encoding //
///////////////////////////

var (
	msjspOpts = jsp.Options{Metaver: cmn.MetaverMetasync, Signature: true, Checksum: true}
	msimmSize int64
)

func (payload msPayload) marshal(mm *memsys.MMSA) (sgl *memsys.SGL) {
	sgl = mm.NewSGL(msimmSize)
	err := jsp.Encode(sgl, payload, msjspOpts)
	cos.AssertNoErr(err)
	msimmSize = max(msimmSize, sgl.Len())
	return sgl
}

func (payload msPayload) unmarshal(reader io.ReadCloser, tag string) (err error) {
	_, err = jsp.Decode(reader, &payload, msjspOpts, tag)
	return
}

//////////////
// errMsync //
//////////////

func (e *errMsync) Error() string { return e.Message }

func (e *errMsync) message(errs ...error) (retErr error) {
	joinErr := errors.Join(errs...)
	if joinErr == nil {
		return nil
	}
	var (
		u        = joinErr.(interface{ Unwrap() []error })
		filtered = u.Unwrap()
		l        = len(filtered)
	)
	if l == 1 {
		retErr = filtered[0]
		e.Message = retErr.Error()
	} else {
		e.Message = joinErr.Error()
		retErr = fmt.Errorf("%v (and %d more error%s)", filtered[0], l-1, cos.Plural(l-1))
	}

	nlog.Warningln(cos.MustMarshalToString(e)) // extended info
	return
}

func err2MsyncErr(err error) (e *errMsync) {
	ee := errMsync{}
	if errP := jsoniter.UnmarshalFromString(err.Error(), &ee); errP == nil {
		return &ee
	}
	return
}
