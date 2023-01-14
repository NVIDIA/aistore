// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/mirror"
	"github.com/NVIDIA/aistore/xact/xs"
)

// GC
const (
	gcTxnsInterval   = time.Hour
	gcTxnsNumKeep    = 16
	gcTxnsTimeotMult = 10

	TxnTimeoutMult = 2
)

type (
	txn interface {
		// accessors
		uuid() string
		started(phase string, tm ...time.Time) time.Time
		isDone() (done bool, err error)
		// triggers
		commitAfter(caller string, msg *aisMsg, err error, args ...any) (bool, error)
		rsvp(err error)
		// cleanup
		abort()
		commit()
		// log
		String() string
	}
	rndzvs struct { // rendezvous records
		timestamp  int64
		err        *txnError
		callerName string
	}
	transactions struct {
		t          *target
		m          map[string]txn    // by txn.uuid
		rendezvous map[string]rndzvs // ditto
		sync.RWMutex
	}
	txnBase struct { // generic base
		phase struct {
			begin  time.Time
			commit time.Time
		}
		xctn       cluster.Xact
		err        *txnError
		action     string
		callerName string
		callerID   string
		uid        string
		smapVer    int64
		bmdVer     int64
		sync.RWMutex
	}
	txnBckBase struct {
		bck  cluster.Bck
		nlps []cmn.NLP
		txnBase
	}
	txnError struct {
		err error
	}
	//
	// concrete transaction types
	//
	txnCreateBucket struct {
		txnBckBase
	}
	txnMakeNCopies struct {
		txnBckBase
		curCopies int64
		newCopies int64
	}
	txnSetBucketProps struct {
		bprops *cmn.BucketProps
		nprops *cmn.BucketProps
		txnBckBase
	}
	txnRenameBucket struct {
		bckFrom *cluster.Bck
		bckTo   *cluster.Bck
		txnBckBase
	}
	txnTCB struct {
		xtcb *mirror.XactTCB
		txnBckBase
	}
	txnTCObjs struct {
		xtco *xs.XactTCObjs
		msg  *cmn.TCObjsMsg
		txnBckBase
	}
	txnECEncode struct {
		txnBckBase
	}
	txnArchMultiObj struct {
		xarch *xs.XactArch
		msg   *cmn.ArchiveMsg
		txnBckBase
	}
	txnPromote struct {
		msg    *cluster.PromoteArgs
		xprm   *xs.XactDirPromote
		dirFQN string
		fqns   []string
		txnBckBase
		totalN int
		fshare bool
	}
)

// interface guard
var (
	_ txn = (*txnBckBase)(nil)
	_ txn = (*txnCreateBucket)(nil)
	_ txn = (*txnMakeNCopies)(nil)
	_ txn = (*txnSetBucketProps)(nil)
	_ txn = (*txnRenameBucket)(nil)
	_ txn = (*txnTCB)(nil)
	_ txn = (*txnTCObjs)(nil)
	_ txn = (*txnECEncode)(nil)
	_ txn = (*txnPromote)(nil)
)

//////////////////
// transactions //
//////////////////

func (txns *transactions) init(t *target) {
	txns.t = t
	txns.m = make(map[string]txn, 8)
	txns.rendezvous = make(map[string]rndzvs, 8)
	hk.Reg("cp-transactions"+hk.NameSuffix, txns.housekeep, gcTxnsInterval)
}

func (txns *transactions) begin(txn txn) (err error) {
	txns.Lock()
	defer txns.Unlock()
	if x, ok := txns.m[txn.uuid()]; ok {
		err = fmt.Errorf("%s: %s already exists (duplicate uuid?)", txns.t.si, x)
		debug.AssertNoErr(err)
		return
	}
	txn.started(apc.ActBegin, time.Now())
	txns.m[txn.uuid()] = txn
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s begin: %s", txns.t, txn)
	}
	return
}

func (txns *transactions) find(uuid, act string) (txn txn, err error) {
	var ok bool
	debug.Assert(act == "" /*simply find*/ || act == apc.ActAbort || act == apc.ActCommit)
	txns.Lock()
	if txn, ok = txns.m[uuid]; !ok {
		goto rerr
	} else if act != "" {
		delete(txns.m, uuid)
		delete(txns.rendezvous, uuid)
		if act == apc.ActAbort {
			txn.abort()
		} else {
			txn.commit()
		}
	}
	txns.Unlock()
	if act != "" && glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s %s: %s", txns.t, act, txn)
	}
	return
rerr:
	txns.Unlock()
	err = cmn.NewErrNotFound("%s: txn %q", txns.t.si, uuid)
	return
}

func (txns *transactions) commitBefore(caller string, msg *aisMsg) error {
	var (
		rndzvs rndzvs
		ok     bool
	)
	txns.Lock()
	if rndzvs, ok = txns.rendezvous[msg.UUID]; !ok {
		rndzvs.callerName, rndzvs.timestamp = caller, mono.NanoTime()
		txns.rendezvous[msg.UUID] = rndzvs
		txns.Unlock()
		return nil
	}
	txns.Unlock()
	return fmt.Errorf("rendezvous record %s:%d already exists", msg.UUID, rndzvs.timestamp)
}

func (txns *transactions) commitAfter(caller string, msg *aisMsg, err error, args ...any) (errDone error) {
	var running bool
	txns.Lock()

	if txn, ok := txns.m[msg.UUID]; ok {
		// Ignore downgrade error.
		if isErrDowngrade(err) {
			err = nil
			bmd := txns.t.owner.bmd.get()
			glog.Warningf("%s: commit with downgraded (current: %s)", txn, bmd)
		}
		if running, errDone = txn.commitAfter(caller, msg, err, args...); running {
			glog.Infof("committed: %s", txn)
		}
	}
	if !running {
		if rndzvs, ok := txns.rendezvous[msg.UUID]; ok {
			rndzvs.err = &txnError{err: err}
			txns.rendezvous[msg.UUID] = rndzvs
		} else {
			goto rerr
		}
	}
	txns.Unlock()
	return
rerr:
	txns.Unlock()
	errDone = cmn.NewErrNotFound("%s: rendezvous record %q", txns.t.si, msg.UUID) // can't happen
	return
}

// given txn, wait for its completion, handle timeout, and ultimately remove
func (txns *transactions) wait(txn txn, timeoutNetw, timeoutHost time.Duration) (err error) {
	const sleep = 100 * time.Millisecond
	var (
		rsvpErr           error
		done, found, rsvp bool
	)
	// timestamp
	txn.started(apc.ActCommit, time.Now())

	// RSVP
	txns.RLock()
	if rndzvs, ok := txns.rendezvous[txn.uuid()]; ok {
		if rndzvs.err != nil {
			rsvp, rsvpErr = true, rndzvs.err.err
		}
	}
	txns.RUnlock()
	if rsvp {
		txn.rsvp(rsvpErr)
	}
	// poll & check
	defer func() {
		act := apc.ActCommit
		if err != nil {
			act = apc.ActAbort
		}
		txns.find(txn.uuid(), act)
	}()
	for total := sleep; ; total += sleep {
		if done, err = txn.isDone(); done {
			return
		}
		// aborted?
		if _, err = txns.find(txn.uuid(), ""); err != nil {
			return
		}

		time.Sleep(sleep)
		// must be ready for rendezvous
		if !found {
			txns.RLock()
			_, found = txns.rendezvous[txn.uuid()]
			txns.RUnlock()
		}
		// two timeouts
		if found {
			if timeoutHost != 0 && total > timeoutHost {
				err = errors.New("timed out waiting for txn to complete")
				break
			}
		} else if timeoutNetw != 0 && total > timeoutNetw {
			err = errors.New("timed out waiting for commit message")
			break
		}
	}
	return
}

// GC orphaned transactions //
func (txns *transactions) housekeep() (d time.Duration) {
	var (
		errs    []string
		orphans []txn
		config  = cmn.GCO.Get()
		now     = time.Now()
	)
	d = gcTxnsInterval
	txns.RLock()
	l := len(txns.m)
	if l > gcTxnsNumKeep*10 && l > 16 {
		d = gcTxnsInterval / 10
	}
	for _, txn := range txns.m {
		elapsed := now.Sub(txn.started(apc.ActBegin))
		if commitTimestamp := txn.started(apc.ActCommit); !commitTimestamp.IsZero() {
			elapsed = now.Sub(commitTimestamp)
			if elapsed > gcTxnsTimeotMult*config.Timeout.MaxHostBusy.D() {
				errs = append(errs, fmt.Sprintf("GC %s: [commit - done] timeout", txn))
				orphans = append(orphans, txn)
			} else if elapsed >= TxnTimeoutMult*config.Timeout.MaxHostBusy.D() {
				errs = append(errs, fmt.Sprintf("GC %s: commit is taking too long...", txn))
			}
		} else {
			if elapsed > TxnTimeoutMult*config.Timeout.MaxHostBusy.D() {
				errs = append(errs, fmt.Sprintf("GC %s: [begin - start-commit] timeout", txn))
				orphans = append(orphans, txn)
			} else if elapsed >= TxnTimeoutMult*cmn.Timeout.MaxKeepalive() {
				errs = append(errs, fmt.Sprintf("GC %s: commit message is taking too long...", txn))
			}
		}
	}
	txns.RUnlock()

	if len(orphans) == 0 {
		return
	}
	txns.Lock()
	for _, txn := range orphans {
		txn.abort()
		delete(txns.m, txn.uuid())
		delete(txns.rendezvous, txn.uuid())
	}
	txns.Unlock()
	for _, s := range errs {
		glog.Errorln(s)
	}
	return
}

/////////////
// txnBase //
/////////////

func (txn *txnBase) uuid() string { return txn.uid }

func (txn *txnBase) started(phase string, tm ...time.Time) (ts time.Time) {
	switch phase {
	case apc.ActBegin:
		if len(tm) > 0 {
			txn.phase.begin = tm[0]
		}
		ts = txn.phase.begin
	case apc.ActCommit:
		if len(tm) > 0 {
			txn.phase.commit = tm[0]
		}
		ts = txn.phase.commit
	default:
		debug.Assert(false)
	}
	return
}

func (txn *txnBase) isDone() (done bool, err error) {
	txn.RLock()
	if txn.err != nil {
		err = txn.err.err
		done = true
	}
	txn.RUnlock()
	return
}

func (txn *txnBase) rsvp(err error) {
	txn.Lock()
	txn.err = &txnError{err: err}
	txn.Unlock()
}

func (txn *txnBase) fillFromCtx(c *txnServerCtx) {
	txn.uid = c.uuid
	txn.action = c.msg.Action
	txn.callerName = c.callerName
	txn.callerID = c.callerID
	txn.smapVer = c.t.owner.smap.get().version()
	txn.bmdVer = c.t.owner.bmd.get().version()
}

////////////////
// txnBckBase //
////////////////

func newTxnBckBase(bck *cluster.Bck) (txn *txnBckBase) {
	txn = &txnBckBase{}
	txn.init(bck)
	return
}

func (txn *txnBckBase) init(bck *cluster.Bck) { txn.bck = *bck }

func (txn *txnBckBase) cleanup() {
	for _, p := range txn.nlps {
		p.Unlock()
	}
	txn.nlps = txn.nlps[:0]
}

func (txn *txnBckBase) abort() {
	txn.cleanup()
	glog.Infof("aborted: %s", txn)
}

// NOTE: not keeping locks for the duration; see also: txnTCB
func (txn *txnBckBase) commit() { txn.cleanup() }

func (txn *txnBckBase) String() string {
	var res, tm string
	if done, err := txn.isDone(); done {
		if err == nil {
			res = " done"
		} else {
			res = fmt.Sprintf(" fail(%v)", err)
		}
	}
	if txn.xctn != nil {
		return fmt.Sprintf("txn-%s%s", txn.xctn, res)
	}
	if !txn.phase.commit.IsZero() {
		tm = "-" + cos.FormatTime(txn.phase.commit, cos.StampMicro)
	}
	return fmt.Sprintf("txn-%s[%s]-%s%s%s]", txn.action, txn.uid, txn.bck.Bucket().String(), tm, res)
}

func (txn *txnBckBase) commitAfter(caller string, msg *aisMsg, err error, args ...any) (found bool, errDone error) {
	if txn.callerName != caller || msg.UUID != txn.uuid() {
		return
	}
	bmd, _ := args[0].(*bucketMD)
	debug.Assert(bmd.version() >= txn.bmdVer)

	found = true
	txn.Lock()
	defer txn.Unlock()
	if txn.err != nil {
		errDone = fmt.Errorf("%s: already done with err=%v", txn, txn.err.err)
		return
	}
	txn.err = &txnError{err: err}
	return
}

/////////////////////
// txnCreateBucket //
/////////////////////

func newTxnCreateBucket(c *txnServerCtx) (txn *txnCreateBucket) {
	txn = &txnCreateBucket{}
	txn.init(c.bck)
	txn.fillFromCtx(c)
	return
}

////////////////////
// txnMakeNCopies //
////////////////////

func newTxnMakeNCopies(c *txnServerCtx, curCopies, newCopies int64) (txn *txnMakeNCopies) {
	txn = &txnMakeNCopies{curCopies: curCopies, newCopies: newCopies}
	txn.init(c.bck)
	txn.fillFromCtx(c)
	return
}

func (txn *txnMakeNCopies) String() string {
	s := txn.txnBckBase.String()
	return fmt.Sprintf("%s-copies(%d=>%d)", s, txn.curCopies, txn.newCopies)
}

///////////////////////
// txnSetBucketProps //
///////////////////////

func newTxnSetBucketProps(c *txnServerCtx, nprops *cmn.BucketProps) (txn *txnSetBucketProps) {
	cos.Assert(c.bck.Props != nil)
	bprops := c.bck.Props.Clone()
	txn = &txnSetBucketProps{bprops: bprops, nprops: nprops}
	txn.init(c.bck)
	txn.fillFromCtx(c)
	return
}

/////////////////////
// txnRenameBucket //
/////////////////////

func newTxnRenameBucket(c *txnServerCtx, bckFrom, bckTo *cluster.Bck) (txn *txnRenameBucket) {
	txn = &txnRenameBucket{bckFrom: bckFrom, bckTo: bckTo}
	txn.init(bckFrom)
	txn.fillFromCtx(c)
	return
}

////////////
// txnTCB //
////////////

func newTxnTCB(c *txnServerCtx, xtcb *mirror.XactTCB) (txn *txnTCB) {
	txn = &txnTCB{xtcb: xtcb}
	txn.init(xtcb.Args().BckFrom)
	txn.fillFromCtx(c)
	return
}

func (txn *txnTCB) abort() {
	txn.txnBckBase.abort()
	txn.xtcb.TxnAbort()
}

func (txn *txnTCB) String() string {
	txn.xctn = txn.xtcb
	return txn.txnBckBase.String()
}

///////////////
// txnTCObjs //
///////////////

func newTxnTCObjs(c *txnServerCtx, bckFrom *cluster.Bck, xtco *xs.XactTCObjs, msg *cmn.TCObjsMsg) (txn *txnTCObjs) {
	txn = &txnTCObjs{xtco: xtco, msg: msg}
	txn.init(bckFrom)
	txn.fillFromCtx(c)
	return
}

func (txn *txnTCObjs) abort() {
	txn.txnBckBase.abort()
	txn.xtco.TxnAbort()
}

func (txn *txnTCObjs) String() string {
	txn.xctn = txn.xtco
	return txn.txnBckBase.String()
}

/////////////////
// txnECEncode //
/////////////////

func newTxnECEncode(c *txnServerCtx, bck *cluster.Bck) (txn *txnECEncode) {
	txn = &txnECEncode{}
	txn.init(bck)
	txn.fillFromCtx(c)
	return
}

///////////////////////////
// txnCreateArchMultiObj //
///////////////////////////

func newTxnArchMultiObj(c *txnServerCtx, bckFrom *cluster.Bck, xarch *xs.XactArch, msg *cmn.ArchiveMsg) (txn *txnArchMultiObj) {
	txn = &txnArchMultiObj{xarch: xarch, msg: msg}
	txn.init(bckFrom)
	txn.fillFromCtx(c)
	return
}

func (txn *txnArchMultiObj) abort() {
	txn.txnBckBase.abort()
	txn.xarch.TxnAbort()
}

func (txn *txnArchMultiObj) String() string {
	txn.xctn = txn.xarch
	return txn.txnBckBase.String()
}

////////////////
// txnPromote //
////////////////

func newTxnPromote(c *txnServerCtx, msg *cluster.PromoteArgs, fqns []string, dirFQN string, totalN int) (txn *txnPromote) {
	txn = &txnPromote{msg: msg, fqns: fqns, dirFQN: dirFQN, totalN: totalN}
	txn.init(c.bck)
	txn.fillFromCtx(c)
	return
}

func (txn *txnPromote) String() (s string) {
	txn.xctn = txn.xprm
	return fmt.Sprintf("%s-src(%s)-N(%d)-fshare(%t)", txn.txnBckBase.String(), txn.dirFQN, txn.totalN, txn.fshare)
}
