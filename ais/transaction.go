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
)

type (
	txnCtx interface {
		// accessors
		uuid() string
		started(tm time.Time)
		String() string
		fired() (err error)
		fire(err error) // only once
		callback(args ...interface{})
	}
	transactions struct {
		sync.Mutex
		t *targetrunner
		m map[string]txnCtx // by txnCtx.uuid
	}
	txnCtxBase struct { // generic base
		sync.RWMutex
		uid       string
		start     time.Time
		action    string
		smapVer   int64
		bmdVer    int64
		initiator string
		err       error
	}
	txnCtxBmdRecv struct {
		txnCtxBase
		bck cluster.Bck
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
	txns.m = make(map[string]txnCtx, 4)
}

func (txns *transactions) begin(txn txnCtx) error {
	txns.Lock()
	defer txns.Unlock()
	if x, ok := txns.m[txn.uuid()]; ok {
		return fmt.Errorf("%s: %s already started (duplicate uuid?)", txns.t.si, x)
	}
	txn.started(time.Now())
	txns.m[txn.uuid()] = txn
	return nil
}

func (txns *transactions) find(uuid string, remove bool) (txn txnCtx, err error) {
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

func (txns *transactions) callback(args ...interface{}) {
	txns.Lock()
	for _, txn := range txns.m {
		if err := txn.fired(); err != errNil {
			continue // only once
		}
		txn.callback(args...)
	}
	txns.Unlock()
}

// given txn, wait for its completion, handle timeout, and ultimately remove
func (txns *transactions) wait(txn txnCtx, timeout time.Duration) (err error) {
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

// TODO -- FIXME: register with hk to cleanup orphaned transactions

////////////////
// txnCtxBase //
////////////////

var _ txnCtx = &txnCtxBase{}

// c-tor
func newTxnCtxBmdRecv(uuid, action string, smapVer, bmdVer int64, initiator string, bck *cluster.Bck) *txnCtxBmdRecv {
	return &txnCtxBmdRecv{
		txnCtxBase{
			uid:       uuid,
			action:    action,
			smapVer:   smapVer,
			bmdVer:    bmdVer,
			initiator: initiator,
			err:       errNil, // NOTE: another kind of nil
		},
		*bck,
	}
}

func (txn *txnCtxBase) uuid() string                 { return txn.uid }
func (txn *txnCtxBase) callback(args ...interface{}) { cmn.Assert(false) }
func (txn *txnCtxBase) started(tm time.Time)         { txn.start = tm }

func (txn *txnCtxBase) fired() (err error) {
	txn.RLock()
	err = txn.err
	txn.RUnlock()
	return
}

func (txn *txnCtxBase) fire(err error) {
	txn.Lock()
	txn.err = err
	txn.Unlock()
}

func (txn *txnCtxBase) String() string {
	tm := cmn.FormatTimestamp(txn.start)
	return fmt.Sprintf("Txn[%s-(v%d, v%d)-%s-%s-%s]", txn.uid, txn.smapVer, txn.bmdVer, txn.action, txn.initiator, tm)
}

///////////////////
// txnCtxBmdRecv //
///////////////////

func (txn *txnCtxBmdRecv) String() string {
	s := txn.txnCtxBase.String()
	return fmt.Sprintf("%s, bucket %s", s, txn.bck.Name)
}

func (txn *txnCtxBmdRecv) callback(args ...interface{}) {
	if len(args) < 3 {
		return
	}
	bmd, ok := args[0].(*bucketMD)
	if !ok {
		return
	}
	err, _ := args[1].(error)
	caller, _ := args[2].(string)
	if txn.initiator == caller && bmd.version() > txn.bmdVer {
		if _, present := bmd.Get(&txn.bck); present {
			txn.fire(err)
			glog.Infof("%s: callback fired (BMD v%d, err %v)", txn, bmd.version(), err)
		}
	}
}
