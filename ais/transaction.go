// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/transport/bundle"
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
		commitAfter(caller string, msg *aisMsg, err error, args ...interface{}) (bool, error)
		rsvp(err error)
		// cleanup
		abort()
		commit()
		// log
		String() string
	}
	rndzvs struct { // rendezvous records
		callerName string
		err        *txnError
		timestamp  time.Time
	}
	transactions struct {
		sync.RWMutex
		t          *targetrunner
		m          map[string]txn    // by txn.uuid
		rendezvous map[string]rndzvs // ditto
	}
	txnBase struct { // generic base
		sync.RWMutex
		uid   string
		phase struct {
			begin  time.Time
			commit time.Time
		}
		action     string
		smapVer    int64
		bmdVer     int64
		kind       string
		callerName string
		callerID   string
		err        *txnError
	}
	txnBckBase struct {
		txnBase
		bck  cluster.Bck
		nlps []cmn.NLP
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
		txnBckBase
		bprops *cmn.BucketProps
		nprops *cmn.BucketProps
	}
	txnRenameBucket struct {
		txnBckBase
		bckFrom *cluster.Bck
		bckTo   *cluster.Bck
	}
	txnTransferBucket struct {
		txnBckBase
		bckFrom *cluster.Bck
		bckTo   *cluster.Bck
		dm      *bundle.DataMover
		dp      cluster.LomReaderProvider // optional
		metaMsg *cmn.Bck2BckMsg           // optional, for object name translation.
	}
)

// interface guard
var (
	_ txn = (*txnBckBase)(nil)
	_ txn = (*txnCreateBucket)(nil)
	_ txn = (*txnMakeNCopies)(nil)
	_ txn = (*txnSetBucketProps)(nil)
	_ txn = (*txnRenameBucket)(nil)
	_ txn = (*txnTransferBucket)(nil)
)

//////////////////
// transactions //
//////////////////

func (txns *transactions) init(t *targetrunner) {
	txns.t = t
	txns.m = make(map[string]txn, 8)
	txns.rendezvous = make(map[string]rndzvs, 8)
	hk.Reg("cp.transactions.gc", txns.housekeep, gcTxnsInterval)
}

func (txns *transactions) begin(txn txn) error {
	txns.Lock()
	defer txns.Unlock()
	if x, ok := txns.m[txn.uuid()]; ok {
		return fmt.Errorf("%s: %s already exists (duplicate uuid?)", txns.t.si, x)
	}
	txn.started(cmn.ActBegin, time.Now())
	txns.m[txn.uuid()] = txn
	glog.Infof("begin: %s", txn)
	return nil
}

func (txns *transactions) find(uuid, act string) (txn txn, err error) {
	var ok bool
	cmn.Assert(act == "" /*simply find*/ || act == cmn.ActAbort || act == cmn.ActCommit)
	txns.Lock()
	if txn, ok = txns.m[uuid]; !ok {
		goto rerr
	} else if act != "" {
		delete(txns.m, uuid)
		delete(txns.rendezvous, uuid)
		if act == cmn.ActAbort {
			txn.abort()
		} else {
			txn.commit()
		}
	}
	txns.Unlock()
	return
rerr:
	txns.Unlock()
	err = fmt.Errorf("%s: Txn[%s] doesn't exist (aborted?)", txns.t.si, uuid)
	return
}

func (txns *transactions) commitBefore(caller string, msg *aisMsg) error {
	var (
		rndzvs rndzvs
		ok     bool
	)
	txns.Lock()
	if rndzvs, ok = txns.rendezvous[msg.UUID]; !ok {
		rndzvs.callerName, rndzvs.timestamp = caller, time.Now()
		txns.rendezvous[msg.UUID] = rndzvs
		txns.Unlock()
		return nil
	}
	txns.Unlock()
	return fmt.Errorf("rendezvous record %s:%s already exists",
		msg.UUID, cmn.FormatTimestamp(rndzvs.timestamp))
}

func (txns *transactions) commitAfter(caller string, msg *aisMsg, err error, args ...interface{}) (errDone error) {
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
		} else {
			goto rerr
		}
	}
	txns.Unlock()
	return
rerr:
	txns.Unlock()
	errDone = fmt.Errorf("rendezvous record %s does not exist", msg.UUID) // can't happen
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
	txn.started(cmn.ActCommit, time.Now())

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
		act := cmn.ActCommit
		if err != nil {
			act = cmn.ActAbort
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
		elapsed := now.Sub(txn.started(cmn.ActBegin))
		if commitTimestamp := txn.started(cmn.ActCommit); !commitTimestamp.IsZero() {
			elapsed = now.Sub(commitTimestamp)
			if elapsed > gcTxnsTimeotMult*config.Timeout.MaxHostBusy {
				errs = append(errs, fmt.Sprintf("GC %s: [commit - done] timeout", txn))
				orphans = append(orphans, txn)
			} else if elapsed >= TxnTimeoutMult*config.Timeout.MaxHostBusy {
				errs = append(errs, fmt.Sprintf("GC %s: commit is taking too long...", txn))
			}
		} else {
			if elapsed > TxnTimeoutMult*config.Timeout.MaxHostBusy {
				errs = append(errs, fmt.Sprintf("GC %s: [begin - start-commit] timeout", txn))
				orphans = append(orphans, txn)
			} else if elapsed >= TxnTimeoutMult*config.Timeout.MaxKeepalive {
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
	case cmn.ActBegin:
		if len(tm) > 0 {
			txn.phase.begin = tm[0]
		}
		ts = txn.phase.begin
	case cmn.ActCommit:
		if len(tm) > 0 {
			txn.phase.commit = tm[0]
		}
		ts = txn.phase.commit
	default:
		cmn.Assert(false)
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
	txn.smapVer = c.smapVer
	txn.bmdVer = c.bmdVer
	txn.callerName = c.callerName
	txn.callerID = c.callerID
}

////////////////
// txnBckBase //
////////////////

func newTxnBckBase(kind string, bck cluster.Bck) *txnBckBase {
	return &txnBckBase{txnBase: txnBase{kind: kind}, bck: bck}
}

func (txn *txnBckBase) cleanup() {
	for _, p := range txn.nlps {
		nlp, ok := p.(*cluster.NameLockPair)
		cmn.Assert(ok)
		nlp.Unlock()
	}
	txn.nlps = txn.nlps[:0]
}

func (txn *txnBckBase) abort() {
	txn.cleanup()
	glog.Infof("aborted: %s", txn)
}

// NOTE: not keeping locks for the duration; see also: txnTransferBucket
func (txn *txnBckBase) commit() { txn.cleanup() }

func (txn *txnBckBase) String() string {
	var (
		res string
		tm  = cmn.FormatTimestamp(txn.phase.begin)
	)
	if !txn.phase.commit.IsZero() {
		tm += "-" + cmn.FormatTimestamp(txn.phase.commit)
	}
	if done, err := txn.isDone(); done {
		if err == nil {
			res = "-done"
		} else {
			res = fmt.Sprintf("-fail(%v)", err)
		}
	}
	return fmt.Sprintf("txn-%s[%s-(v%d, v%d)-%s-%s-%s%s], bucket %s",
		txn.kind, txn.uid, txn.smapVer, txn.bmdVer, txn.action, txn.callerName, tm, res, txn.bck.Name)
}

func (txn *txnBckBase) commitAfter(caller string, msg *aisMsg, err error,
	args ...interface{}) (found bool, errDone error) {
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
	txn = &txnCreateBucket{*newTxnBckBase("crb", *c.bck)}
	txn.fillFromCtx(c)
	return
}

////////////////////
// txnMakeNCopies //
////////////////////

func newTxnMakeNCopies(c *txnServerCtx, curCopies, newCopies int64) (txn *txnMakeNCopies) {
	txn = &txnMakeNCopies{
		*newTxnBckBase("mnc", *c.bck),
		curCopies,
		newCopies,
	}
	txn.fillFromCtx(c)
	return
}

func (txn *txnMakeNCopies) String() string {
	s := txn.txnBckBase.String()
	return fmt.Sprintf("%s, copies %d => %d", s, txn.curCopies, txn.newCopies)
}

///////////////////////
// txnSetBucketProps //
///////////////////////

func newTxnSetBucketProps(c *txnServerCtx, nprops *cmn.BucketProps) (txn *txnSetBucketProps) {
	cmn.Assert(c.bck.Props != nil)
	bprops := c.bck.Props.Clone()
	txn = &txnSetBucketProps{
		*newTxnBckBase("spb", *c.bck),
		bprops,
		nprops,
	}
	txn.fillFromCtx(c)
	return
}

/////////////////////
// txnRenameBucket //
/////////////////////

func newTxnRenameBucket(c *txnServerCtx, bckFrom, bckTo *cluster.Bck) (txn *txnRenameBucket) {
	txn = &txnRenameBucket{
		*newTxnBckBase("rnb", *bckFrom),
		bckFrom,
		bckTo,
	}
	txn.fillFromCtx(c)
	return
}

///////////////////////
// txnTransferBucket //
///////////////////////

func newTxnTransferBucket(c *txnServerCtx, bckFrom, bckTo *cluster.Bck, dm *bundle.DataMover,
	dp cluster.LomReaderProvider, metaMsg *cmn.Bck2BckMsg) (txn *txnTransferBucket) {
	txn = &txnTransferBucket{
		*newTxnBckBase("bcp", *bckFrom),
		bckFrom,
		bckTo,
		dm,
		dp,
		metaMsg,
	}
	txn.fillFromCtx(c)
	return
}

func (txn *txnTransferBucket) abort() {
	txn.txnBckBase.abort()
	txn.dm.UnregRecv()
}
