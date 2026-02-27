// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/xact/xs"
)

// GC
const (
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
		commitAfter(sender string, msg *actMsgExt, err error, args ...any) (bool, error)
		rsvp(err error)
		// cleanup
		abort(error)
		unlock()
		// log
		String() string
	}
	rndzvs struct { // rendezvous records
		err        *txnError
		senderName string
		timestamp  int64
	}
	// two maps, two locks
	txns struct {
		t          *target        // parent
		m          map[string]txn // by txn.uuid
		rendezvous struct {
			m   map[string]rndzvs // ditto
			mtx sync.Mutex
		}
		mtx sync.Mutex
	}
)

type (
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
		senderName string
		senderID   string
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
		xbmv *xs.BckRename
		txnBckBase
	}
	txnTCB struct {
		xtcb *xs.XactTCB
		txnBckBase
	}
	txnTCObjs struct {
		xtco *xs.XactTCO
		msg  *cmn.TCOMsg
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
		msg    *apc.PromoteArgs
		xprm   *xs.XactDirPromote
		dirFQN string
		fqns   []string
		txnBckBase
		totalN int
		fshare bool
	}
	txnCreateNBI struct {
		msg  *apc.CreateInvMsg
		xinv *xs.XactNBI
		txnBckBase
	}
	txnETLInit struct {
		msg etl.InitMsg
		txnBase
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
	_ txn = (*txnETLInit)(nil)
)

//////////////////
// txns //
//////////////////

func (txns *txns) init(t *target) {
	txns.t = t
	txns.m = make(map[string]txn, 8)
	txns.rendezvous.m = make(map[string]rndzvs, 8)
	hk.Reg("txn"+hk.NameSuffix, txns.housekeep, hk.DelOldIval)
}

func (txns *txns) begin(txn txn, nlps ...core.NLP) (err error) {
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
	txn.started(apc.Begin2PC, time.Now())
	txn.set(nlps)
	txns.m[txn.uuid()] = txn
	txns.mtx.Unlock()

	if cmn.Rom.V(4, cos.ModAIS) {
		nlog.Infof("%s begin: %s", txns.t, txn)
	}
	return
}

// find and term: [cleanup | Commit | Abort]
func (txns *txns) term(uuid, act string) {
	debug.Assert(act == actTxnCleanup || act == apc.Commit2PC || act == apc.Abort2PC, "invalid ", act)

	txns.mtx.Lock()
	txn, ok := txns.m[uuid]
	if !ok {
		txns.mtx.Unlock()
		return
	}

	delete(txns.m, uuid)
	txns.mtx.Unlock()

	txns.rendezvous.mtx.Lock()
	delete(txns.rendezvous.m, uuid)
	txns.rendezvous.mtx.Unlock()

	if act == apc.Abort2PC {
		txn.abort(errors.New("action: abort")) // NOTE: may call txn-specific abort, e.g. TxnAbort
	} else {
		txn.unlock()
	}
	if cmn.Rom.V(4, cos.ModAIS) {
		nlog.Infof("%s %s: %s", txns.t, act, txn)
	}
}

func (txns *txns) find(uuid string) (_ txn, err error) {
	txns.mtx.Lock()
	found, ok := txns.m[uuid]
	txns.mtx.Unlock()
	if !ok {
		err = cos.NewErrNotFound(txns.t, "txn["+uuid+"]")
	}
	return found, err
}

func (txns *txns) commitBefore(sender string, msg *actMsgExt) error {
	var (
		rndzvs rndzvs
		ok     bool
	)
	txns.rendezvous.mtx.Lock()
	if rndzvs, ok = txns.rendezvous.m[msg.UUID]; !ok {
		rndzvs.senderName, rndzvs.timestamp = sender, mono.NanoTime()
		txns.rendezvous.m[msg.UUID] = rndzvs
		txns.rendezvous.mtx.Unlock()
		return nil
	}
	txns.rendezvous.mtx.Unlock()
	return fmt.Errorf("rendezvous record %s:%d already exists", msg.UUID, rndzvs.timestamp)
}

func (txns *txns) commitAfter(sender string, msg *actMsgExt, err error, args ...any) (errDone error) {
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
		if running, errDone = txn.commitAfter(sender, msg, err, args...); running {
			nlog.Infoln(txn.String())
		}
	}
	if !running {
		txns.rendezvous.mtx.Lock()
		rndzvs, ok := txns.rendezvous.m[msg.UUID]
		if !ok { // can't happen
			txns.rendezvous.mtx.Unlock()
			errDone = cos.NewErrNotFound(txns.t, "rendezvous record "+msg.UUID)
			return
		}
		rndzvs.err = &txnError{err: err}
		txns.rendezvous.m[msg.UUID] = rndzvs
		txns.rendezvous.mtx.Unlock()
	}
	return
}

// given txn, wait for its completion, handle timeout, and ultimately remove
func (txns *txns) wait(txn txn, timeoutNetw, timeoutHost time.Duration) (err error) {
	// timestamp
	txn.started(apc.Commit2PC, time.Now())

	// transfer err rendezvous => txn
	txns.rendezvous.mtx.Lock()
	rndzvs, ok := txns.rendezvous.m[txn.uuid()]
	txns.rendezvous.mtx.Unlock()
	if ok && rndzvs.err != nil {
		txn.rsvp(rndzvs.err.err)
	}

	err = txns._wait(txn, timeoutNetw, timeoutHost)

	// cleanup or abort, depending on the returned err
	act := cos.Ternary(err != nil, apc.Abort2PC, apc.Commit2PC)
	txns.term(txn.uuid(), act)
	return err
}

// poll for 'done'
func (txns *txns) _wait(txn txn, timeoutNetw, timeoutHost time.Duration) (err error) {
	var (
		sleep       = cos.PollSleepShort
		done, found bool
	)
	for total := sleep; ; {
		if done, err = txn.isDone(); done {
			return err
		}
		// aborted?
		if _, err = txns.find(txn.uuid()); err != nil {
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

// GC orphaned txns
func (txns *txns) housekeep(int64) (d time.Duration) {
	var (
		errs    []error
		orphans []txn
		config  = cmn.GCO.Get()
	)
	d = hk.DelOldIval
	txns.mtx.Lock()
	l := len(txns.m)
	if l == 0 {
		txns.mtx.Unlock()
		return d
	}
	if l > max(gcTxnsNumKeep<<2, 32) {
		d >>= 2
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
	return d
}

func (txns *txns) cleanup(orphans []txn, errs []error) {
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
	elapsed := now.Sub(txn.started(apc.Begin2PC))
	if commitTimestamp := txn.started(apc.Commit2PC); !commitTimestamp.IsZero() {
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
	case apc.Begin2PC:
		if len(tm) > 0 {
			txn.phase.begin = tm[0]
		}
		ts = txn.phase.begin
	case apc.Commit2PC:
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
	txn.senderName = c.senderName
	txn.senderID = c.senderID
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

func (txn *txnBase) commitAfter(sender string, msg *actMsgExt, err error, args ...any) (found bool, errDone error) {
	if txn.senderName != sender || msg.UUID != txn.uuid() {
		return
	}
	found = true
	debug.Func(func() {
		bmd, _ := args[0].(*bucketMD)
		debug.Assert(bmd.version() >= txn.bmdVer)
	})
	if txnErr := txn.err.Swap(&txnError{err: err}); txnErr != nil {
		errDone = fmt.Errorf("%s: already done with err=%v (%v)", txn.uuid(), txnErr.err, err)
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

func newTxnRenameBucket(c *txnSrv, xbmv *xs.BckRename) (txn *txnRenameBucket) {
	txn = &txnRenameBucket{xbmv: xbmv}
	txn.init(xbmv.Args().BckFrom)
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

func newTxnTCObjs(c *txnSrv, bckFrom *meta.Bck, xtco *xs.XactTCO, msg *cmn.TCOMsg) (txn *txnTCObjs) {
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

func newTxnPromote(c *txnSrv, msg *apc.PromoteArgs, fqns []string, dirFQN string, totalN int) (txn *txnPromote) {
	txn = &txnPromote{msg: msg, fqns: fqns, dirFQN: dirFQN, totalN: totalN}
	txn.init(c.bck)
	txn.fillFromCtx(c)
	return
}

func (txn *txnPromote) String() (s string) {
	txn.xctn = txn.xprm
	return fmt.Sprintf("%s-src(%s)-N(%d)-fshare(%t)", txn.txnBckBase.String(), txn.dirFQN, txn.totalN, txn.fshare)
}

//////////////////
// txnCreateNBI //
//////////////////

func newTxnCreateNBI(c *txnSrv, msg *apc.CreateInvMsg, xinv *xs.XactNBI) (txn *txnCreateNBI) {
	txn = &txnCreateNBI{msg: msg, xinv: xinv}
	txn.init(c.bck)
	txn.fillFromCtx(c)
	return
}

////////////////
// txnETLInit //
////////////////

func newTxnETLInit(c *txnSrv, msg etl.InitMsg) (txn *txnETLInit) {
	txn = &txnETLInit{msg: msg}
	txn.fillFromCtx(c)
	return
}

func (txn *txnETLInit) abort(err error) {
	nlog.Infof("transaction %s aborted for %s, err: %v\n", txn.String(), txn.msg.Cname(), err)
	etl.StopByXid(txn.uuid(), err) // only stop the ETL created from this transaction
}

func (txn *txnETLInit) String() string { return txn.msg.String() }

// no-op: no bucket/resources needs to be locked for initializing ETL
func (*txnETLInit) set(_ []core.NLP) {}
func (*txnETLInit) unlock()          {}
