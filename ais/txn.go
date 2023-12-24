// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"sync"
	ratomic "sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/hk"
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
		set(nlps []core.NLP)
		// triggers
		commitAfter(caller string, msg *aisMsg, err error, args ...any) (bool, error)
		rsvp(err error)
		// cleanup
		abort(error)
		unlock()
		// log
		String() string
	}
	rndzvs struct { // rendezvous records
		timestamp  int64
		err        *txnError
		callerName string
	}
	// two maps, two locks
	transactions struct {
		t          *target
		m          map[string]txn // by txn.uuid
		rendezvous struct {
			m   map[string]rndzvs // ditto
			mtx sync.Mutex
		}
		mtx sync.Mutex
	}
	txnError struct { // a wrapper which presence means: "done"
		err error
	}
	txnBase struct { // generic base
		phase struct {
			begin  time.Time
			commit time.Time
		}
		xctn       core.Xact
		err        ratomic.Pointer[txnError]
		action     string
		callerName string
		callerID   string
		uid        string
		smapVer    int64
		bmdVer     int64
		sync.RWMutex
	}
	txnBckBase struct {
		bck  meta.Bck
		nlps []core.NLP
		txnBase
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
		bprops *cmn.Bprops
		nprops *cmn.Bprops
		txnBckBase
	}
	txnRenameBucket struct {
		bckFrom *meta.Bck
		bckTo   *meta.Bck
		txnBckBase
	}
	txnTCB struct {
		xtcb *xs.XactTCB
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
		msg   *cmn.ArchiveBckMsg
		txnBckBase
	}
	txnPromote struct {
		msg    *core.PromoteArgs
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
	txns.rendezvous.m = make(map[string]rndzvs, 8)
	hk.Reg("txn"+hk.NameSuffix, txns.housekeep, gcTxnsInterval)
}

func (txns *transactions) begin(txn txn, nlps ...core.NLP) (err error) {
	txns.mtx.Lock()
	if x, ok := txns.m[txn.uuid()]; ok {
		txns.mtx.Unlock()
		for _, nlp := range nlps {
			nlp.Unlock()
		}
		err = fmt.Errorf("%s: %s already exists (duplicate uuid?)", txns.t.si, x)
		debug.AssertNoErr(err)
		return
	}
	txn.started(apc.ActBegin, time.Now())
	txn.set(nlps)
	txns.m[txn.uuid()] = txn
	txns.mtx.Unlock()

	if cmn.FastV(4, cos.SmoduleAIS) {
		nlog.Infof("%s begin: %s", txns.t, txn)
	}
	return
}

func (txns *transactions) find(uuid, act string) (txn, error) {
	txns.mtx.Lock()
	txn, ok := txns.m[uuid]
	if !ok {
		// a) not found (benign in an unlikely event of failing to commit)
		txns.mtx.Unlock()
		return nil, cos.NewErrNotFound("%s: txn %q", txns.t, uuid)
	}

	if act == "" {
		// b) just find & return
		txns.mtx.Unlock()
		return txn, nil
	}

	// or c) cleanup
	delete(txns.m, uuid)
	txns.mtx.Unlock()

	txns.rendezvous.mtx.Lock()
	delete(txns.rendezvous.m, uuid)
	txns.rendezvous.mtx.Unlock()

	if act == apc.ActAbort {
		txn.abort(errors.New("action: abort")) // NOTE: may call txn-specific abort, e.g. TxnAbort
	} else {
		debug.Assert(act == apc.ActCommit || act == ActCleanup, act)
		txn.unlock()
	}

	if cmn.FastV(4, cos.SmoduleAIS) {
		nlog.Infof("%s %s: %s", txns.t, act, txn)
	}
	return txn, nil
}

func (txns *transactions) commitBefore(caller string, msg *aisMsg) error {
	var (
		rndzvs rndzvs
		ok     bool
	)
	txns.rendezvous.mtx.Lock()
	if rndzvs, ok = txns.rendezvous.m[msg.UUID]; !ok {
		rndzvs.callerName, rndzvs.timestamp = caller, mono.NanoTime()
		txns.rendezvous.m[msg.UUID] = rndzvs
		txns.rendezvous.mtx.Unlock()
		return nil
	}
	txns.rendezvous.mtx.Unlock()
	return fmt.Errorf("rendezvous record %s:%d already exists", msg.UUID, rndzvs.timestamp)
}

func (txns *transactions) commitAfter(caller string, msg *aisMsg, err error, args ...any) (errDone error) {
	txns.mtx.Lock()
	txn, ok := txns.m[msg.UUID]
	txns.mtx.Unlock()

	var running bool
	if ok {
		// Ignore downgrade error.
		if isErrDowngrade(err) {
			err = nil
			bmd := txns.t.owner.bmd.get()
			nlog.Warningf("%s: commit with downgraded (current: %s)", txn, bmd)
		}
		if running, errDone = txn.commitAfter(caller, msg, err, args...); running {
			nlog.Infoln(txn.String())
		}
	}
	if !running {
		txns.rendezvous.mtx.Lock()
		rndzvs, ok := txns.rendezvous.m[msg.UUID]
		if !ok { // can't happen
			txns.rendezvous.mtx.Unlock()
			errDone = cos.NewErrNotFound("%s: rendezvous record %q (%v)", txns.t.si, msg.UUID, errDone)
			return
		}
		rndzvs.err = &txnError{err: err}
		txns.rendezvous.m[msg.UUID] = rndzvs
		txns.rendezvous.mtx.Unlock()
	}
	return
}

// given txn, wait for its completion, handle timeout, and ultimately remove
func (txns *transactions) wait(txn txn, timeoutNetw, timeoutHost time.Duration) (err error) {
	// timestamp
	txn.started(apc.ActCommit, time.Now())

	// transfer err rendezvous => txn
	txns.rendezvous.mtx.Lock()
	rndzvs, ok := txns.rendezvous.m[txn.uuid()]
	txns.rendezvous.mtx.Unlock()
	if ok && rndzvs.err != nil {
		txn.rsvp(rndzvs.err.err)
	}

	err = txns._wait(txn, timeoutNetw, timeoutHost)

	// cleanup or abort, depending on the returned err
	act := apc.ActCommit
	if err != nil {
		act = apc.ActAbort
	}
	txns.find(txn.uuid(), act)
	return err
}

// poll for 'done'
func (txns *transactions) _wait(txn txn, timeoutNetw, timeoutHost time.Duration) (err error) {
	var (
		sleep       = 100 * time.Millisecond
		done, found bool
	)
	for total := sleep; ; {
		if done, err = txn.isDone(); done {
			return err
		}
		// aborted?
		if _, err = txns.find(txn.uuid(), ""); err != nil {
			return err
		}

		time.Sleep(sleep)
		total += sleep
		// bump once
		if total == sleep<<4 {
			sleep *= 4
		}
		// must be ready for rendezvous
		if !found {
			txns.rendezvous.mtx.Lock()
			_, found = txns.rendezvous.m[txn.uuid()]
			txns.rendezvous.mtx.Unlock()
		}
		// two timeouts
		if found {
			// config.Timeout.MaxHostBusy (see p.prepTxnClient)
			if timeoutHost != 0 && total > timeoutHost {
				err = errors.New("timed out waiting for txn to complete")
				break
			}
		} else if timeoutNetw != 0 && total > timeoutNetw { // 2 * config.Timeout.MaxKeepalive (see p.prepTxnClient)
			err = errors.New("timed out waiting for commit message")
			break
		}
	}
	return err
}

// GC orphaned transactions
func (txns *transactions) housekeep() (d time.Duration) {
	var (
		errs    []error
		orphans []txn
		config  = cmn.GCO.Get()
	)
	d = gcTxnsInterval
	txns.mtx.Lock()
	l := len(txns.m)
	if l == 0 {
		txns.mtx.Unlock()
		return
	}
	if l > max(gcTxnsNumKeep*4, 16) {
		d = gcTxnsInterval / 10
	}
	now := time.Now()
	for _, txn := range txns.m {
		err, warn := checkTimeout(txn, now, config)
		if err != nil {
			errs = append(errs, err)
			txn.abort(err)
			delete(txns.m, txn.uuid())
			orphans = append(orphans, txn)
		} else if warn != nil {
			errs = append(errs, warn)
		}
	}
	txns.mtx.Unlock()

	if len(orphans) > 0 || len(errs) > 0 {
		go txns.cleanup(orphans, errs)
	}
	return
}

func (txns *transactions) cleanup(orphans []txn, errs []error) {
	if len(orphans) > 0 {
		txns.rendezvous.mtx.Lock()
		for _, txn := range orphans {
			delete(txns.rendezvous.m, txn.uuid())
		}
		txns.rendezvous.mtx.Unlock()
	}
	for _, e := range errs {
		nlog.Errorln(e)
	}
}

func checkTimeout(txn txn, now time.Time, config *cmn.Config) (err, warn error) {
	elapsed := now.Sub(txn.started(apc.ActBegin))
	if commitTimestamp := txn.started(apc.ActCommit); !commitTimestamp.IsZero() {
		elapsed = now.Sub(commitTimestamp)
		if elapsed > gcTxnsTimeotMult*config.Timeout.MaxHostBusy.D() {
			err = fmt.Errorf("gc %s: [commit - done] timeout", txn)
		} else if elapsed >= TxnTimeoutMult*config.Timeout.MaxHostBusy.D() {
			err = fmt.Errorf("gc %s: commit is taking too long", txn)
		}
	} else {
		if elapsed > TxnTimeoutMult*config.Timeout.MaxHostBusy.D() {
			err = fmt.Errorf("gc %s: [begin - start-commit] timeout", txn)
		} else if elapsed >= TxnTimeoutMult*cmn.Rom.MaxKeepalive() {
			warn = fmt.Errorf("gc %s: commit message is taking too long", txn)
		}
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
	if txnErr := txn.err.Load(); txnErr != nil {
		err = txnErr.err
		done = true
	}
	return
}

func (txn *txnBase) rsvp(err error) { txn.err.Store(&txnError{err: err}) }

func (txn *txnBase) fillFromCtx(c *txnSrv) {
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

func newTxnBckBase(bck *meta.Bck) (txn *txnBckBase) {
	txn = &txnBckBase{}
	txn.init(bck)
	return
}

func (txn *txnBckBase) init(bck *meta.Bck) { txn.bck = *bck }

func (txn *txnBckBase) set(nlps []core.NLP) {
	txn.nlps = nlps
}

func (txn *txnBckBase) unlock() {
	for _, p := range txn.nlps {
		p.Unlock()
	}
	txn.nlps = txn.nlps[:0]
}

func (txn *txnBckBase) abort(err error) {
	txn.unlock()
	nlog.Infoln(txn.String(), "aborted:", err)
}

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
	found = true
	debug.Func(func() {
		bmd, _ := args[0].(*bucketMD)
		debug.Assert(bmd.version() >= txn.bmdVer)
	})
	if txnErr := txn.err.Swap(&txnError{err: err}); txnErr != nil {
		errDone = fmt.Errorf("%s: already done with err=%v (%v)", txn, txnErr.err, err)
		txn.err.Store(txnErr)
	}
	return
}

/////////////////////
// txnCreateBucket //
/////////////////////

func newTxnCreateBucket(c *txnSrv) (txn *txnCreateBucket) {
	txn = &txnCreateBucket{}
	txn.init(c.bck)
	txn.fillFromCtx(c)
	return
}

////////////////////
// txnMakeNCopies //
////////////////////

func newTxnMakeNCopies(c *txnSrv, curCopies, newCopies int64) (txn *txnMakeNCopies) {
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

func newTxnSetBucketProps(c *txnSrv, nprops *cmn.Bprops) (txn *txnSetBucketProps) {
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

func newTxnRenameBucket(c *txnSrv, bckFrom, bckTo *meta.Bck) (txn *txnRenameBucket) {
	txn = &txnRenameBucket{bckFrom: bckFrom, bckTo: bckTo}
	txn.init(bckFrom)
	txn.fillFromCtx(c)
	return
}

////////////
// txnTCB //
////////////

func newTxnTCB(c *txnSrv, xtcb *xs.XactTCB) (txn *txnTCB) {
	txn = &txnTCB{xtcb: xtcb}
	txn.init(xtcb.Args().BckFrom)
	txn.fillFromCtx(c)
	return
}

func (txn *txnTCB) abort(err error) {
	txn.unlock()
	txn.xtcb.TxnAbort(err)
}

func (txn *txnTCB) String() string {
	txn.xctn = txn.xtcb
	return txn.txnBckBase.String()
}

///////////////
// txnTCObjs //
///////////////

func newTxnTCObjs(c *txnSrv, bckFrom *meta.Bck, xtco *xs.XactTCObjs, msg *cmn.TCObjsMsg) (txn *txnTCObjs) {
	txn = &txnTCObjs{xtco: xtco, msg: msg}
	txn.init(bckFrom)
	txn.fillFromCtx(c)
	return
}

func (txn *txnTCObjs) abort(err error) {
	txn.unlock()
	txn.xtco.TxnAbort(err)
}

func (txn *txnTCObjs) String() string {
	txn.xctn = txn.xtco
	return txn.txnBckBase.String()
}

/////////////////
// txnECEncode //
/////////////////

func newTxnECEncode(c *txnSrv, bck *meta.Bck) (txn *txnECEncode) {
	txn = &txnECEncode{}
	txn.init(bck)
	txn.fillFromCtx(c)
	return
}

///////////////////////////
// txnCreateArchMultiObj //
///////////////////////////

func newTxnArchMultiObj(c *txnSrv, bckFrom *meta.Bck, xarch *xs.XactArch, msg *cmn.ArchiveBckMsg) (txn *txnArchMultiObj) {
	txn = &txnArchMultiObj{xarch: xarch, msg: msg}
	txn.init(bckFrom)
	txn.fillFromCtx(c)
	return
}

func (txn *txnArchMultiObj) abort(err error) {
	txn.unlock()
	txn.xarch.TxnAbort(err)
}

func (txn *txnArchMultiObj) String() string {
	txn.xctn = txn.xarch
	return txn.txnBckBase.String()
}

////////////////
// txnPromote //
////////////////

func newTxnPromote(c *txnSrv, msg *core.PromoteArgs, fqns []string, dirFQN string, totalN int) (txn *txnPromote) {
	txn = &txnPromote{msg: msg, fqns: fqns, dirFQN: dirFQN, totalN: totalN}
	txn.init(c.bck)
	txn.fillFromCtx(c)
	return
}

func (txn *txnPromote) String() (s string) {
	txn.xctn = txn.xprm
	return fmt.Sprintf("%s-src(%s)-N(%d)-fshare(%t)", txn.txnBckBase.String(), txn.dirFQN, txn.totalN, txn.fshare)
}
