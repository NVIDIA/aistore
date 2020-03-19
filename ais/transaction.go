// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/housekeep/hk"
)

const (
	txnsTimeoutGC = time.Hour
	txnsNumKeep   = 16
)

type (
	txn interface {
		// accessors
		uuid() string
		started(phase string, tm ...time.Time) time.Time
		timeout() time.Duration
		String() string
		fired() (err error)
		// triggers
		fire(err error) error
		callback(caller string, msgInt *actionMsgInternal, err error, args ...interface{}) (bool, error)
	}
	transactions struct {
		sync.Mutex
		t *targetrunner
		m map[string]txn // by txn.uuid
	}
	txnBase struct { // generic base
		txn
		sync.RWMutex
		uid   string
		tout  time.Duration
		phase struct {
			begin  time.Time
			commit time.Time
		}
		action    string
		smapVer   int64
		bmdVer    int64
		kind      string
		initiator string
		err       error
	}
	txnBckBase struct {
		txnBase
		bck cluster.Bck
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
)

var (
	errTxnTimeout = errors.New("timeout")
	errNil        = errors.New("nil")
)

//////////////////
// transactions //
//////////////////

func (txns *transactions) init(t *targetrunner) {
	txns.t = t
	txns.m = make(map[string]txn, 4)
	hk.Housekeeper.Register("cp.transactions.gc", txns.garbageCollect, txnsTimeoutGC)
}

func (txns *transactions) begin(txn txn) error {
	txns.Lock()
	defer txns.Unlock()
	if x, ok := txns.m[txn.uuid()]; ok {
		return fmt.Errorf("%s: %s already exists (duplicate uuid?)", txns.t.si, x)
	}
	txn.started(cmn.ActBegin, time.Now())
	txns.m[txn.uuid()] = txn
	return nil
}

func (txns *transactions) find(uuid string, remove bool) (txn txn, err error) {
	var ok bool
	txns.Lock()
	if txn, ok = txns.m[uuid]; !ok {
		err = fmt.Errorf("%s: Txn[%s] doesn't exist (aborted?)", txns.t.si, uuid)
	} else if remove {
		delete(txns.m, uuid)
	}
	txns.Unlock()
	return
}

func (txns *transactions) callback(caller string, msgInt *actionMsgInternal, err error,
	args ...interface{}) (found bool, errFired error) {
	txns.Lock()
	for _, txn := range txns.m {
		if found, errFired = txn.callback(caller, msgInt, err, args...); found {
			break // only one
		}
	}
	txns.Unlock()
	return
}

// given txn, wait for its completion, handle timeout, and ultimately remove
func (txns *transactions) wait(txn txn, timeout time.Duration) (err error) {
	// commit phase officially starts now
	txn.started(cmn.ActCommit, time.Now())

	sleep := cmn.MinDuration(100*time.Millisecond, timeout/10)
	for i := sleep; i < timeout; i += sleep {
		if err = txn.fired(); err != errNil {
			txns.find(txn.uuid(), true /* remove */)
			return
		}
		// aborted?
		if _, err = txns.find(txn.uuid(), false); err != nil {
			return
		}
		time.Sleep(sleep)
	}
	txns.find(txn.uuid(), true /* remove */)
	return errTxnTimeout
}

// GC orphaned transactions //
func (txns *transactions) garbageCollect() (d time.Duration) {
	var errs []string
	d = txnsTimeoutGC
	txns.Lock()
	l := len(txns.m)
	if l > txnsNumKeep*10 && l > 16 {
		d = txnsTimeoutGC / 10
	}
	now := time.Now()
	for uuid, txn := range txns.m {
		var (
			elapsed = now.Sub(txn.started(cmn.ActBegin))
			tout    = txn.timeout()
		)
		if commitTimestamp := txn.started(cmn.ActCommit); !commitTimestamp.IsZero() {
			elapsed = now.Sub(commitTimestamp)
		}
		if elapsed > 2*tout+time.Minute {
			errs = append(errs, fmt.Sprintf("GC %s: timeout", txn))
			delete(txns.m, uuid)
		} else if elapsed >= tout {
			errs = append(errs,
				fmt.Sprintf("GC %s: is taking longer than the specified timeout %v", txn, tout))
		}
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

func (txn *txnBase) uuid() string           { return txn.uid }
func (txn *txnBase) timeout() time.Duration { return txn.tout }
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

func (txn *txnBase) fired() (err error) {
	txn.RLock()
	err = txn.err
	txn.RUnlock()
	return
}

func (txn *txnBase) fire(err error) error {
	txn.Lock()
	defer txn.Unlock()
	if txn.err != errNil {
		return fmt.Errorf("%s: misfired", txn)
	}
	txn.err = err
	return nil
}

func (txn *txnBase) fillFromCtx(c *txnServerCtx) {
	txn.uid = c.uuid
	txn.action = c.msgInt.Action
	txn.tout = c.timeout
	txn.smapVer = c.smapVer
	txn.bmdVer = c.bmdVer
	txn.initiator = c.caller
}

////////////////
// txnBckBase //
////////////////

func (txn *txnBckBase) String() string {
	var (
		fired string
		tm    = cmn.FormatTimestamp(txn.phase.begin)
	)
	if !txn.phase.commit.IsZero() {
		tm += "-" + cmn.FormatTimestamp(txn.phase.commit)
	}
	if err := txn.fired(); err != errNil {
		fired = "-fired"
	}
	return fmt.Sprintf("txn-%s[%s-(v%d, v%d)-%s-%s-%s%s], bucket %s",
		txn.kind, txn.uid, txn.smapVer, txn.bmdVer, txn.action, txn.initiator, tm, fired, txn.bck.Name)
}

func (txn *txnBckBase) callback(caller string, msgInt *actionMsgInternal, err error,
	args ...interface{}) (found bool, errFired error) {
	if txn.initiator != caller || msgInt.TxnID != txn.uuid() {
		return
	}
	bmd, _ := args[0].(*bucketMD)
	cmn.Assert(bmd.version() > txn.bmdVer)

	errFired = txn.fire(err)
	found = true
	return
}

/////////////////////
// txnCreateBucket //
/////////////////////

var _ txn = &txnCreateBucket{}

// c-tor
// NOTE: errNill another kind of nil - here and elsewhere
func newTxnCreateBucket(c *txnServerCtx) (txn *txnCreateBucket) {
	txn = &txnCreateBucket{txnBckBase{txnBase{kind: "crb", err: errNil}, *c.bck}}
	txn.fillFromCtx(c)
	return
}

////////////////////
// txnMakeNCopies //
////////////////////

var _ txn = &txnMakeNCopies{}

// c-tor
func newTxnMakeNCopies(c *txnServerCtx, curCopies, newCopies int64) (txn *txnMakeNCopies) {
	txn = &txnMakeNCopies{
		txnBckBase{txnBase{kind: "mnc", err: errNil}, *c.bck},
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

var _ txn = &txnSetBucketProps{}

// c-tor
func newTxnSetBucketProps(c *txnServerCtx, nprops *cmn.BucketProps) (txn *txnSetBucketProps) {
	cmn.Assert(c.bck.Props != nil)
	bprops := c.bck.Props.Clone()
	txn = &txnSetBucketProps{
		txnBckBase{txnBase{kind: "mnc", err: errNil}, *c.bck},
		bprops,
		nprops,
	}
	txn.fillFromCtx(c)
	return
}

/////////////////////
// txnRenameBucket //
/////////////////////

var _ txn = &txnRenameBucket{}

// c-tor
func newTxnRenameBucket(c *txnServerCtx, bckFrom, bckTo *cluster.Bck) (txn *txnRenameBucket) {
	txn = &txnRenameBucket{
		txnBckBase{txnBase{kind: "rnb", err: errNil}, *bckFrom},
		bckFrom,
		bckTo,
	}
	txn.fillFromCtx(c)
	return
}

///////////////////
// txnCopyBucket //
///////////////////
