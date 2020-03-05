// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/mirror"
	"github.com/NVIDIA/aistore/xaction"
)

// convenience structure to gather all (or most) of the relevant context in one place
// (compare with txnClientCtx & prepTxnClient)
type txnServerCtx struct {
	uuid    string
	timeout time.Duration
	phase   string
	msgInt  *actionMsgInternal
	caller  string
	bck     *cluster.Bck
}

// verb /v1/txn
func (t *targetrunner) txnHandler(w http.ResponseWriter, r *http.Request) {
	// 1. check
	if r.Method != http.MethodPost {
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /txn path")
		return
	}
	msgInt := &actionMsgInternal{}
	if cmn.ReadJSON(w, r, msgInt) != nil {
		return
	}
	apiItems, err := t.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Txn)
	if err != nil {
		return
	}
	if len(apiItems) < 2 {
		t.invalmsghdlr(w, r, "url too short: expecting bucket and txn phase", http.StatusBadRequest)
		return
	}
	// 2. gather context
	c, err := prepTxnServer(r, msgInt, apiItems)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	// 3. do
	switch msgInt.Action {
	case cmn.ActCreateLB:
	case cmn.ActRegisterCB:
		if err = t.createBucket(c); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
	case cmn.ActMakeNCopies:
		if err = t.makeNCopies(c); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
	default:
		t.invalmsghdlr(w, r, fmt.Sprintf(fmtUnknownAct, msgInt))
	}
}

func (t *targetrunner) createBucket(c *txnServerCtx) error {
	switch c.phase {
	case cmn.ActBegin:
		// don't allow creating buckets when capInfo.High (let alone OOS)
		config := cmn.GCO.Get()
		if capInfo := t.AvgCapUsed(config); capInfo.Err != nil {
			return capInfo.Err
		}
		txn := newTxnCreateBucket(
			c.uuid,
			c.msgInt.Action,
			t.owner.smap.get().version(),
			t.owner.bmd.get().version(),
			c.caller,
			c.bck,
		)
		if err := t.transactions.begin(txn); err != nil {
			return err
		}
	case cmn.ActAbort:
		t.transactions.find(c.uuid, true /* remove */)
	case cmn.ActCommit:
		txn, err := t.transactions.find(c.uuid, false)
		if err != nil {
			return fmt.Errorf("%s %s: %v", t.si, txn, err)
		}
		// wait for newBMD w/timeout
		if err = t.transactions.wait(txn, c.timeout); err != nil {
			return fmt.Errorf("%s %s: %v", t.si, txn, err)
		}
	default:
		cmn.Assert(false)
	}
	return nil
}

func (t *targetrunner) makeNCopies(c *txnServerCtx) error {
	if err := c.bck.Init(t.owner.bmd, t.si); err != nil {
		return err
	}
	switch c.phase {
	case cmn.ActBegin:
		curCopies, newCopies, err := t.validateMakeNCopies(c.bck, c.msgInt)
		if err != nil {
			return err
		}
		if isStatelessTxn(c) {
			break
		}
		txn := newTxnMakeNCopies(
			c.uuid,
			c.msgInt.Action,
			t.owner.smap.get().version(),
			t.owner.bmd.get().version(),
			c.caller,
			c.bck,
			curCopies, newCopies,
		)
		if err := t.transactions.begin(txn); err != nil {
			return err
		}
	case cmn.ActAbort:
		if !isStatelessTxn(c) {
			t.transactions.find(c.uuid, true /* remove */)
		}
	case cmn.ActCommit:
		copies, _ := t.parseNCopies(c.msgInt.Value)
		if !isStatelessTxn(c) {
			txn, err := t.transactions.find(c.uuid, false)
			if err != nil {
				return fmt.Errorf("%s %s: %v", t.si, txn, err)
			}
			txnMnc := txn.(*txnMakeNCopies)
			cmn.Assert(txnMnc.newCopies == copies)
			// wait for newBMD w/timeout
			if err = t.transactions.wait(txn, c.timeout); err != nil {
				return fmt.Errorf("%s %s: %v", t.si, txn, err)
			}
		}
		xaction.Registry.DoAbort(cmn.ActPutCopies, c.bck)
		xaction.Registry.RenewBckMakeNCopies(c.bck, t, int(copies))
	default:
		cmn.Assert(false)
	}
	return nil
}

func (t *targetrunner) validateMakeNCopies(bck *cluster.Bck, msgInt *actionMsgInternal) (curCopies, newCopies int64, err error) {
	curCopies = bck.Props.Mirror.Copies
	newCopies, err = t.parseNCopies(msgInt.Value)
	if err == nil {
		err = mirror.ValidateNCopies(t.si.Name(), int(newCopies))
	}
	if err != nil {
		return
	}
	// don't allow increasing num-copies when used cap is above high wm (let alone OOS)
	if bck.Props.Mirror.Copies < newCopies {
		capInfo := t.AvgCapUsed(nil)
		err = capInfo.Err
	}
	return
}

/////////////
// helpers //
/////////////

func isStatelessTxn(c *txnServerCtx) bool { return c.uuid == "" }

func prepTxnServer(r *http.Request, msgInt *actionMsgInternal, apiItems []string) (*txnServerCtx, error) {
	var (
		bucket string
		err    error
		query  = r.URL.Query()
		c      = &txnServerCtx{}
	)
	c.msgInt = msgInt
	c.caller = r.Header.Get(cmn.HeaderCallerName)
	bucket, c.phase = apiItems[0], apiItems[1]
	if c.bck, err = newBckFromQuery(bucket, query); err != nil {
		return c, err
	}
	c.uuid = query.Get(cmn.URLParamTxnID)
	if c.uuid == "" {
		return c, nil
	}
	c.timeout, err = cmn.S2Duration(query.Get(cmn.URLParamTxnTimeout))
	return c, err
}
