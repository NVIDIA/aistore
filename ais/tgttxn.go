// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/etl"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/mirror"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/reb"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/NVIDIA/aistore/xs"
	jsoniter "github.com/json-iterator/go"
)

// context structure to gather all (or most) of the relevant state in one place
// (compare with txnClientCtx)
type txnServerCtx struct {
	uuid    string
	timeout struct {
		netw time.Duration
		host time.Duration
	}
	phase      string
	msg        *aisMsg
	callerName string
	callerID   string
	bck        *cluster.Bck // aka bckFrom
	bckTo      *cluster.Bck
	query      url.Values
	t          *target
}

// TODO: return xaction ID (xactID) where applicable

// verb /v1/txn
func (t *target) txnHandler(w http.ResponseWriter, r *http.Request) {
	var bucket, phase string
	if r.Method != http.MethodPost {
		cmn.WriteErr405(w, r, http.MethodPost)
		return
	}
	msg, err := t.readAisMsg(w, r)
	if err != nil {
		return
	}

	xactRecord := xact.Table[msg.Action]
	onlyPrimary := xactRecord.Metasync
	if !t.ensureIntraControl(w, r, onlyPrimary) {
		return
	}

	apiItems, err := t.checkRESTItems(w, r, 0, true, cmn.URLPathTxn.L)
	if err != nil {
		return
	}
	switch len(apiItems) {
	case 1: // Global transaction.
		phase = apiItems[0]
	case 2: // Bucket-based transaction.
		bucket, phase = apiItems[0], apiItems[1]
	default:
		t.writeErrURL(w, r)
		return
	}
	c, err := t.prepTxnServer(r, msg, bucket, phase)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	switch msg.Action {
	case cmn.ActCreateBck, cmn.ActAddRemoteBck:
		err = t.createBucket(c)
	case cmn.ActMakeNCopies:
		err = t.makeNCopies(c)
	case cmn.ActSetBprops, cmn.ActResetBprops:
		err = t.setBucketProps(c)
	case cmn.ActMoveBck:
		err = t.renameBucket(c)
	case cmn.ActCopyBck, cmn.ActETLBck:
		var (
			xactID string
			dp     cluster.DP
			tcmsg  = &cmn.TCBMsg{}
		)
		if err := cos.MorphMarshal(c.msg.Value, tcmsg); err != nil {
			t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, msg.Action, c.msg.Value, err)
			return
		}
		if msg.Action == cmn.ActETLBck {
			var err error
			if dp, err = t.etlDP(tcmsg); err != nil {
				t.writeErr(w, r, err)
				return
			}
		}
		xactID, err = t.tcb(c, tcmsg, dp)
		if xactID != "" {
			w.Header().Set(cmn.HdrXactionID, xactID)
		}
	case cmn.ActCopyObjects, cmn.ActETLObjects:
		var (
			xactID string
			dp     cluster.DP
			tcoMsg = &cmn.TCObjsMsg{}
		)
		if err := cos.MorphMarshal(c.msg.Value, tcoMsg); err != nil {
			t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, msg.Action, c.msg.Value, err)
			return
		}
		if msg.Action == cmn.ActETLObjects {
			var err error
			if dp, err = t.etlDP(&tcoMsg.TCBMsg); err != nil {
				t.writeErr(w, r, err)
				return
			}
		}
		xactID, err = t.tcobjs(c, tcoMsg, dp)
		if xactID != "" {
			w.Header().Set(cmn.HdrXactionID, xactID)
		}
	case cmn.ActECEncode:
		err = t.ecEncode(c)
	case cmn.ActArchive:
		var xactID string
		xactID, err = t.createArchMultiObj(c)
		if xactID != "" {
			w.Header().Set(cmn.HdrXactionID, xactID)
		}
	case cmn.ActStartMaintenance, cmn.ActDecommissionNode, cmn.ActShutdownNode:
		err = t.startMaintenance(c)
	case cmn.ActDestroyBck, cmn.ActEvictRemoteBck:
		err = t.destroyBucket(c)
	case cmn.ActPromote:
		var (
			xactID string
			hdr    = w.Header()
		)
		xactID, err = t.promote(c, hdr)
		if xactID != "" {
			w.Header().Set(cmn.HdrXactionID, xactID)
		}
	default:
		t.writeErrAct(w, r, msg.Action)
	}
	if err == nil {
		return
	}
	if cmn.IsErrCapacityExceeded(err) {
		cs := t.OOS(nil)
		t.writeErrStatusf(w, r, http.StatusInsufficientStorage, "%s: %v", cs, err)
	} else {
		t.writeErr(w, r, err)
	}
}

//////////////////
// createBucket //
//////////////////

func (t *target) createBucket(c *txnServerCtx) error {
	switch c.phase {
	case cmn.ActBegin:
		txn := newTxnCreateBucket(c)
		if err := t.transactions.begin(txn); err != nil {
			return err
		}
		if c.msg.Action == cmn.ActCreateBck && c.bck.IsRemote() {
			if c.msg.Value != nil {
				if err := cos.MorphMarshal(c.msg.Value, &c.bck.Props); err != nil {
					return fmt.Errorf(cmn.FmtErrMorphUnmarshal, t.si, c.msg.Action, c.msg.Value, err)
				}
			}
			if _, err := t.Backend(c.bck).CreateBucket(c.bck); err != nil {
				return err
			}
		}
	case cmn.ActAbort:
		t.transactions.find(c.uuid, cmn.ActAbort)
	case cmn.ActCommit:
		t._commitCreateDestroy(c)
	default:
		debug.Assert(false)
	}
	return nil
}

func (t *target) _commitCreateDestroy(c *txnServerCtx) (err error) {
	txn, err := t.transactions.find(c.uuid, "")
	if err != nil {
		return err
	}
	// wait for newBMD w/timeout
	if err = t.transactions.wait(txn, c.timeout.netw, c.timeout.host); err != nil {
		return fmt.Errorf("%s %s: %v", t.si, txn, err)
	}
	return
}

/////////////////
// makeNCopies //
/////////////////

func (t *target) makeNCopies(c *txnServerCtx) error {
	if err := c.bck.Init(t.owner.bmd); err != nil {
		return err
	}
	switch c.phase {
	case cmn.ActBegin:
		curCopies, newCopies, err := t.validateMakeNCopies(c.bck, c.msg)
		if err != nil {
			return err
		}
		nlp := c.bck.GetNameLockPair()
		if !nlp.TryLock(c.timeout.netw / 2) {
			return cmn.NewErrBckIsBusy(c.bck.Bck)
		}
		txn := newTxnMakeNCopies(c, curCopies, newCopies)
		if err := t.transactions.begin(txn); err != nil {
			nlp.Unlock()
			return err
		}
		txn.nlps = []cmn.NLP{nlp}
	case cmn.ActAbort:
		t.transactions.find(c.uuid, cmn.ActAbort)
	case cmn.ActCommit:
		copies, err := _parseNCopies(c.msg.Value)
		debug.AssertNoErr(err)
		txn, err := t.transactions.find(c.uuid, "")
		if err != nil {
			return err
		}
		txnMnc := txn.(*txnMakeNCopies)
		debug.Assert(txnMnc.newCopies == copies)

		// wait for newBMD w/timeout
		if err = t.transactions.wait(txn, c.timeout.netw, c.timeout.host); err != nil {
			return fmt.Errorf("%s %s: %v", t.si, txn, err)
		}

		// do the work in xaction
		rns := xreg.RenewBckMakeNCopies(t, c.bck, c.uuid, "mnc-actmnc", int(copies))
		if rns.Err != nil {
			return fmt.Errorf("%s %s: %v", t.si, txn, rns.Err)
		}
		xctn := rns.Entry.Get()
		xreg.DoAbort(cmn.ActPutCopies, c.bck, errors.New("make-n-copies"))
		c.addNotif(xctn) // notify upon completion
		xact.GoRunW(xctn)
	default:
		debug.Assert(false)
	}
	return nil
}

func (t *target) validateMakeNCopies(bck *cluster.Bck, msg *aisMsg) (curCopies, newCopies int64, err error) {
	curCopies = bck.Props.Mirror.Copies
	newCopies, err = _parseNCopies(msg.Value)
	if err == nil {
		err = fs.ValidateNCopies(t.si.Name(), int(newCopies))
	}
	// NOTE: #791 "limited coexistence" here and elsewhere
	if err == nil {
		err = xreg.LimitedCoexistence(t.si, bck, msg.Action)
	}
	if err != nil {
		return
	}
	// don't allow increasing num-copies when used cap is above high wm (let alone OOS)
	if bck.Props.Mirror.Copies < newCopies {
		cs := fs.GetCapStatus()
		err = cs.Err
	}
	return
}

////////////////////
// setBucketProps //
////////////////////

func (t *target) setBucketProps(c *txnServerCtx) error {
	if err := c.bck.Init(t.owner.bmd); err != nil {
		return err
	}
	switch c.phase {
	case cmn.ActBegin:
		var (
			nprops *cmn.BucketProps
			err    error
		)
		if nprops, err = t.validateNprops(c.bck, c.msg); err != nil {
			return err
		}
		nlp := c.bck.GetNameLockPair()
		if !nlp.TryLock(c.timeout.netw / 2) {
			return cmn.NewErrBckIsBusy(c.bck.Bck)
		}
		txn := newTxnSetBucketProps(c, nprops)
		if err := t.transactions.begin(txn); err != nil {
			nlp.Unlock()
			return err
		}
		txn.nlps = []cmn.NLP{nlp}
	case cmn.ActAbort:
		t.transactions.find(c.uuid, cmn.ActAbort)
	case cmn.ActCommit:
		txn, err := t.transactions.find(c.uuid, "")
		if err != nil {
			return err
		}
		txnSetBprops := txn.(*txnSetBucketProps)
		// wait for newBMD w/timeout
		if err = t.transactions.wait(txn, c.timeout.netw, c.timeout.host); err != nil {
			return fmt.Errorf("%s %s: %v", t.si, txn, err)
		}
		if reMirror(txnSetBprops.bprops, txnSetBprops.nprops) {
			n := int(txnSetBprops.nprops.Mirror.Copies)
			rns := xreg.RenewBckMakeNCopies(t, c.bck, c.uuid, "mnc-setprops", n)
			if rns.Err != nil {
				return fmt.Errorf("%s %s: %v", t.si, txn, rns.Err)
			}
			xctn := rns.Entry.Get()
			xreg.DoAbort(cmn.ActPutCopies, c.bck, errors.New("re-mirror"))
			c.addNotif(xctn) // notify upon completion
			xact.GoRunW(xctn)
		}
		if reEC(txnSetBprops.bprops, txnSetBprops.nprops, c.bck) {
			xreg.DoAbort(cmn.ActECEncode, c.bck, errors.New("re-ec"))
			rns := xreg.RenewECEncode(t, c.bck, c.uuid, cmn.ActCommit)
			if rns.Err != nil {
				return rns.Err
			}
			xctn := rns.Entry.Get()
			c.addNotif(xctn) // ditto
			xact.GoRunW(xctn)
		}
	default:
		debug.Assert(false)
	}
	return nil
}

func (t *target) validateNprops(bck *cluster.Bck, msg *aisMsg) (nprops *cmn.BucketProps, err error) {
	var (
		body = cos.MustMarshal(msg.Value)
		cs   = fs.GetCapStatus()
	)
	nprops = &cmn.BucketProps{}
	if err = jsoniter.Unmarshal(body, nprops); err != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, t.si, "new bucket props", cmn.BytesHead(body), err)
		return
	}
	if nprops.Mirror.Enabled {
		mpathCount := fs.NumAvail()
		if int(nprops.Mirror.Copies) > mpathCount {
			err = fmt.Errorf(fmtErrInsuffMpaths1, t.si, mpathCount, bck, nprops.Mirror.Copies)
			return
		}
		if nprops.Mirror.Copies > bck.Props.Mirror.Copies && cs.Err != nil {
			return nprops, cs.Err
		}
	}
	if nprops.EC.Enabled && !bck.Props.EC.Enabled {
		err = cs.Err
	}
	return
}

//////////////////
// renameBucket //
//////////////////

func (t *target) renameBucket(c *txnServerCtx) error {
	if err := c.bck.Init(t.owner.bmd); err != nil {
		return err
	}
	switch c.phase {
	case cmn.ActBegin:
		bckFrom, bckTo := c.bck, c.bckTo
		if err := t.validateBckRenTxn(bckFrom, bckTo, c.msg); err != nil {
			return err
		}
		nlpFrom := bckFrom.GetNameLockPair()
		nlpTo := bckTo.GetNameLockPair()
		if !nlpFrom.TryLock(c.timeout.netw / 4) {
			return cmn.NewErrBckIsBusy(bckFrom.Bck)
		}
		if !nlpTo.TryLock(c.timeout.netw / 4) {
			nlpFrom.Unlock()
			return cmn.NewErrBckIsBusy(bckTo.Bck)
		}
		txn := newTxnRenameBucket(c, bckFrom, bckTo)
		if err := t.transactions.begin(txn); err != nil {
			nlpTo.Unlock()
			nlpFrom.Unlock()
			return err
		}
		txn.nlps = []cmn.NLP{nlpFrom, nlpTo}
	case cmn.ActAbort:
		t.transactions.find(c.uuid, cmn.ActAbort)
	case cmn.ActCommit:
		txn, err := t.transactions.find(c.uuid, "")
		if err != nil {
			return err
		}
		txnRenB := txn.(*txnRenameBucket)
		// wait for newBMD w/timeout
		if err = t.transactions.wait(txn, c.timeout.netw, c.timeout.host); err != nil {
			return fmt.Errorf("%s %s: %v", t.si, txn, err)
		}
		rns := xreg.RenewBckRename(t, txnRenB.bckFrom, txnRenB.bckTo, c.uuid, c.msg.RMDVersion, cmn.ActCommit)
		if rns.Err != nil {
			return rns.Err // must not happen at commit time
		}
		xctn := rns.Entry.Get()
		err = fs.RenameBucketDirs(txnRenB.bckFrom.Props.BID, txnRenB.bckFrom.Bck, txnRenB.bckTo.Bck)
		if err != nil {
			return err // ditto
		}
		c.addNotif(xctn) // notify upon completion

		reb.ActivateTimedGFN()
		xact.GoRunW(xctn) // run and wait until it starts running
	default:
		debug.Assert(false)
	}
	return nil
}

func (t *target) validateBckRenTxn(bckFrom, bckTo *cluster.Bck, msg *aisMsg) error {
	if cs := fs.GetCapStatus(); cs.Err != nil {
		return cs.Err
	}
	if err := xreg.LimitedCoexistence(t.si, bckFrom, msg.Action, bckTo); err != nil {
		return err
	}
	bmd := t.owner.bmd.get()
	if _, present := bmd.Get(bckFrom); !present {
		return cmn.NewErrBckNotFound(bckFrom.Bck)
	}
	if _, present := bmd.Get(bckTo); present {
		return cmn.NewErrBckAlreadyExists(bckTo.Bck)
	}
	availablePaths := fs.GetAvail()
	for _, mi := range availablePaths {
		path := mi.MakePathCT(bckTo.Bck, fs.ObjectType)
		if err := fs.Access(path); err != nil {
			if !os.IsNotExist(err) {
				return err
			}
			continue
		}
		if names, empty, err := fs.IsDirEmpty(path); err != nil {
			return err
		} else if !empty {
			return fmt.Errorf("directory %q already exists and is not empty (%v...)", path, names)
		}
	}
	return nil
}

func (t *target) etlDP(msg *cmn.TCBMsg) (dp cluster.DP, err error) {
	if err = k8s.Detect(); err != nil {
		return
	}
	if msg.ID == "" {
		err = cmn.ErrETLMissingUUID
		return
	}
	return etl.NewOfflineDataProvider(msg, t.si)
}

// common for both bucket copy and bucket transform - does the heavy lifting
func (t *target) tcb(c *txnServerCtx, msg *cmn.TCBMsg, dp cluster.DP) (string, error) {
	var xactID string
	if err := c.bck.Init(t.owner.bmd); err != nil {
		return xactID, err
	}
	switch c.phase {
	case cmn.ActBegin:
		bckTo, bckFrom := c.bckTo, c.bck
		if err := bckTo.Validate(); err != nil {
			return xactID, err
		}
		if err := bckFrom.Validate(); err != nil {
			return xactID, err
		}
		if cs := fs.GetCapStatus(); cs.Err != nil {
			return xactID, cs.Err
		}
		if err := xreg.LimitedCoexistence(t.si, bckFrom, c.msg.Action); err != nil {
			return xactID, err
		}
		bmd := t.owner.bmd.get()
		if _, present := bmd.Get(bckFrom); !present {
			return xactID, cmn.NewErrBckNotFound(bckFrom.Bck)
		}
		nlpTo, nlpFrom, xid, err := t._tcbBegin(c, msg, dp)
		if err != nil {
			if nlpFrom != nil {
				nlpFrom.Unlock()
			}
			if nlpTo != nil {
				nlpTo.Unlock()
			}
			return xactID, err
		}
		xactID = xid
	case cmn.ActAbort:
		t.transactions.find(c.uuid, cmn.ActAbort)
	case cmn.ActCommit:
		txn, err := t.transactions.find(c.uuid, "")
		if err != nil {
			return xactID, err
		}
		txnTcb := txn.(*txnTCB)
		if c.query.Get(cmn.URLParamWaitMetasync) != "" {
			if err = t.transactions.wait(txn, c.timeout.netw, c.timeout.host); err != nil {
				txnTcb.xtcb.TxnAbort()
				return xactID, fmt.Errorf("%s %s: %v", t.si, txn, err)
			}
		} else {
			t.transactions.find(c.uuid, cmn.ActCommit)
		}
		custom := txnTcb.xtcb.Args()
		debug.Assert(custom.Phase == cmn.ActBegin)
		custom.Phase = cmn.ActCommit
		rns := xreg.RenewTCB(t, c.uuid, c.msg.Action /*kind*/, txnTcb.xtcb.Args())
		if rns.Err != nil {
			txnTcb.xtcb.TxnAbort()
			return xactID, rns.Err
		}
		xctn := rns.Entry.Get()
		xactID = xctn.ID()
		debug.Assert(xactID == txnTcb.xtcb.ID())
		c.addNotif(xctn) // notify upon completion
		xact.GoRunW(xctn)
	default:
		debug.Assert(false)
	}
	return xactID, nil
}

func (t *target) _tcbBegin(c *txnServerCtx, msg *cmn.TCBMsg, dp cluster.DP) (nlpTo, nlpFrom *cluster.NameLockPair,
	xactID string, err error) {
	bckTo, bckFrom := c.bckTo, c.bck
	nlpFrom = bckFrom.GetNameLockPair()
	if !nlpFrom.TryRLock(c.timeout.netw / 4) {
		nlpFrom = nil
		err = cmn.NewErrBckIsBusy(bckFrom.Bck)
		return
	}
	if !msg.DryRun {
		nlpTo = bckTo.GetNameLockPair()
		if !nlpTo.TryLock(c.timeout.netw / 4) {
			nlpTo = nil
			err = cmn.NewErrBckIsBusy(bckTo.Bck)
			return
		}
	}
	custom := &xreg.TCBArgs{Phase: cmn.ActBegin, BckFrom: bckFrom, BckTo: bckTo, DP: dp, Msg: msg}
	rns := xreg.RenewTCB(t, c.uuid, c.msg.Action /*kind*/, custom)
	if err = rns.Err; err != nil {
		return
	}
	xctn := rns.Entry.Get()
	xtcb := xctn.(*mirror.XactTCB)
	xactID = xctn.ID()
	txn := newTxnTCB(c, xtcb)
	if err = t.transactions.begin(txn); err != nil {
		return
	}
	txn.nlps = []cmn.NLP{nlpFrom}
	if nlpTo != nil {
		txn.nlps = append(txn.nlps, nlpTo)
	}
	return
}

func (t *target) tcobjs(c *txnServerCtx, msg *cmn.TCObjsMsg, dp cluster.DP) (string, error) {
	var xactID string
	if err := c.bck.Init(t.owner.bmd); err != nil {
		return xactID, err
	}
	switch c.phase {
	case cmn.ActBegin:
		var (
			bckTo   = c.bckTo
			bckFrom = c.bck // from
		)
		// validate
		if err := bckTo.Validate(); err != nil {
			return xactID, err
		}
		if err := bckFrom.Validate(); err != nil {
			return xactID, err
		}
		if cs := fs.GetCapStatus(); cs.Err != nil {
			return xactID, cs.Err
		}
		if err := xreg.LimitedCoexistence(t.si, bckFrom, c.msg.Action); err != nil {
			return xactID, err
		}
		bmd := t.owner.bmd.get()
		if _, present := bmd.Get(bckFrom); !present {
			return xactID, cmn.NewErrBckNotFound(bckFrom.Bck)
		}
		// begin
		custom := &xreg.TCObjsArgs{BckFrom: bckFrom, BckTo: bckTo, DP: dp}
		rns := xreg.RenewTCObjs(t, c.uuid, c.msg.Action /*kind*/, custom)
		if rns.Err != nil {
			return xactID, rns.Err
		}
		xctn := rns.Entry.Get()
		xactID = xctn.ID()
		debug.Assert((!rns.IsRunning() && xactID == c.uuid) || (rns.IsRunning() && xactID == rns.UUID))

		xtco := xctn.(*xs.XactTCObjs)
		msg.TxnUUID = c.uuid
		txn := newTxnTCObjs(c, bckFrom, xtco, msg)
		if err := t.transactions.begin(txn); err != nil {
			return xactID, err
		}
		xtco.Begin(msg)
	case cmn.ActAbort:
		txn, err := t.transactions.find(c.uuid, cmn.ActAbort)
		if err == nil {
			txnTco := txn.(*txnTCObjs)
			// if _this_ transaction initiated _that_ on-demand
			if xtco := txnTco.xtco; xtco != nil && xtco.ID() == c.uuid {
				xactID = xtco.ID()
				xtco.Abort(nil)
			}
		}
	case cmn.ActCommit:
		txn, err := t.transactions.find(c.uuid, "")
		if err != nil {
			return xactID, err
		}
		txnTco := txn.(*txnTCObjs)
		txnTco.xtco.Do(txnTco.msg)
		xactID = txnTco.xtco.ID()
		t.transactions.find(c.uuid, cmn.ActCommit)
	default:
		debug.Assert(false)
	}
	return xactID, nil
}

//////////////
// ecEncode //
//////////////

func (t *target) ecEncode(c *txnServerCtx) error {
	if err := c.bck.Init(t.owner.bmd); err != nil {
		return err
	}
	switch c.phase {
	case cmn.ActBegin:
		if err := t.validateECEncode(c.bck, c.msg); err != nil {
			return err
		}
		nlp := c.bck.GetNameLockPair()
		if !nlp.TryLock(c.timeout.netw / 4) {
			return cmn.NewErrBckIsBusy(c.bck.Bck)
		}

		txn := newTxnECEncode(c, c.bck)
		if err := t.transactions.begin(txn); err != nil {
			nlp.Unlock()
			return err
		}
		txn.nlps = []cmn.NLP{nlp}
	case cmn.ActAbort:
		t.transactions.find(c.uuid, cmn.ActAbort)
	case cmn.ActCommit:
		txn, err := t.transactions.find(c.uuid, "")
		if err != nil {
			return err
		}
		// wait for newBMD w/timeout
		if err = t.transactions.wait(txn, c.timeout.netw, c.timeout.host); err != nil {
			return fmt.Errorf("%s %s: %v", t.si, txn, err)
		}
		rns := xreg.RenewECEncode(t, c.bck, c.uuid, cmn.ActCommit)
		if rns.Err != nil {
			return rns.Err
		}
		xctn := rns.Entry.Get()
		c.addNotif(xctn) // notify upon completion
		xact.GoRunW(xctn)
	default:
		debug.Assert(false)
	}
	return nil
}

func (t *target) validateECEncode(bck *cluster.Bck, msg *aisMsg) error {
	if cs := fs.GetCapStatus(); cs.Err != nil {
		return cs.Err
	}
	return xreg.LimitedCoexistence(t.si, bck, msg.Action)
}

////////////////////////
// createArchMultiObj //
////////////////////////

func (t *target) createArchMultiObj(c *txnServerCtx) (string /*xaction uuid*/, error) {
	var xactID string
	if err := c.bck.Init(t.owner.bmd); err != nil {
		return xactID, err
	}
	switch c.phase {
	case cmn.ActBegin:
		var (
			bckTo   = c.bckTo
			bckFrom = c.bck
		)
		if err := bckTo.Validate(); err != nil {
			return xactID, err
		}
		if !bckFrom.Equal(bckTo, false, false) {
			if err := bckFrom.Validate(); err != nil {
				return xactID, err
			}
		}
		archMsg := &cmn.ArchiveMsg{}
		if err := cos.MorphMarshal(c.msg.Value, archMsg); err != nil {
			return xactID, fmt.Errorf(cmn.FmtErrMorphUnmarshal, t.si, c.msg.Action, c.msg.Value, err)
		}
		mime, err := cos.Mime(archMsg.Mime, archMsg.ArchName)
		if err != nil {
			return xactID, err
		}
		archMsg.Mime = mime // set it for xarch

		if cs := fs.GetCapStatus(); cs.Err != nil {
			return xactID, cs.Err
		}

		rns := xreg.RenewPutArchive(c.uuid, t, bckFrom)
		if rns.Err != nil {
			return xactID, rns.Err
		}
		xctn := rns.Entry.Get()
		xactID = xctn.ID()
		debug.Assert((!rns.IsRunning() && xactID == c.uuid) || (rns.IsRunning() && xactID == rns.UUID))

		xarch := xctn.(*xs.XactCreateArchMultiObj)
		// finalize the message and begin local transaction
		archMsg.TxnUUID = c.uuid
		archMsg.FromBckName = bckFrom.Name
		if err := xarch.Begin(archMsg); err != nil {
			return xactID, err
		}
		txn := newTxnArchMultiObj(c, bckFrom, xarch, archMsg)
		if err := t.transactions.begin(txn); err != nil {
			return xactID, err
		}
	case cmn.ActAbort:
		txn, err := t.transactions.find(c.uuid, cmn.ActAbort)
		if err == nil {
			txnArch := txn.(*txnArchMultiObj)
			// if _this_ transaction initiated _that_ on-demand
			if xarch := txnArch.xarch; xarch != nil && xarch.ID() == c.uuid {
				xactID = xarch.ID()
				xarch.Abort(nil)
			}
		}
	case cmn.ActCommit:
		txn, err := t.transactions.find(c.uuid, "")
		if err != nil {
			return xactID, err
		}
		txnArch := txn.(*txnArchMultiObj)
		txnArch.xarch.Do(txnArch.msg)
		xactID = txnArch.xarch.ID()
		t.transactions.find(c.uuid, cmn.ActCommit)
	}
	return xactID, nil
}

//////////////////////
// startMaintenance //
//////////////////////

func (t *target) startMaintenance(c *txnServerCtx) error {
	switch c.phase {
	case cmn.ActBegin:
		var opts cmn.ActValRmNode
		if err := cos.MorphMarshal(c.msg.Value, &opts); err != nil {
			return fmt.Errorf(cmn.FmtErrMorphUnmarshal, t.si, c.msg.Action, c.msg.Value, err)
		}
		if err := xreg.LimitedCoexistence(t.si, nil, c.msg.Action); err != nil {
			return err
		}
		reb.ActivateTimedGFN()
	case cmn.ActAbort:
		reb.AbortTimedGFN()
	case cmn.ActCommit:
		var opts cmn.ActValRmNode
		if err := cos.MorphMarshal(c.msg.Value, &opts); err != nil {
			return fmt.Errorf(cmn.FmtErrMorphUnmarshal, t.si, c.msg.Action, c.msg.Value, err)
		}
		if c.msg.Action == cmn.ActDecommissionNode {
			if opts.DaemonID != t.si.ID() {
				err := fmt.Errorf("%s: invalid target ID %q", t.si, opts.DaemonID)
				debug.AssertNoErr(err)
				return err
			}
		}
	default:
		debug.Assert(false)
	}
	return nil
}

//////////////////////////////////////////////
// destroy local bucket / evict cloud buket //
//////////////////////////////////////////////

func (t *target) destroyBucket(c *txnServerCtx) error {
	switch c.phase {
	case cmn.ActBegin:
		nlp := c.bck.GetNameLockPair()
		if !nlp.TryLock(c.timeout.netw / 2) {
			return cmn.NewErrBckIsBusy(c.bck.Bck)
		}
		txn := newTxnBckBase("dlb", c.bck)
		txn.fillFromCtx(c)
		if err := t.transactions.begin(txn); err != nil {
			nlp.Unlock()
			return err
		}
		txn.nlps = []cmn.NLP{nlp}
	case cmn.ActAbort:
		t.transactions.find(c.uuid, cmn.ActAbort)
	case cmn.ActCommit:
		t._commitCreateDestroy(c)
	default:
		debug.Assert(false)
	}
	return nil
}

// execute synchronously (ie, wo/ xaction) if the number of files is less or equal
const promoteNumSync = 16

func (t *target) promote(c *txnServerCtx, hdr http.Header) (string, error) {
	if err := c.bck.Init(t.owner.bmd); err != nil {
		return "", err
	}
	switch c.phase {
	case cmn.ActBegin:
		prmMsg := &cmn.ActValPromote{}
		if err := cos.MorphMarshal(c.msg.Value, prmMsg); err != nil {
			err = fmt.Errorf(cmn.FmtErrMorphUnmarshal, t.si, c.msg.Action, c.msg.Value, err)
			return "", err
		}
		srcFQN := c.msg.Name
		finfo, err := os.Stat(srcFQN)
		if err != nil {
			return "", err
		}
		if !finfo.IsDir() {
			txn := newTxnPromote(c, prmMsg, []string{srcFQN}, "", 1)
			if err := t.transactions.begin(txn); err != nil {
				return "", err
			}
			hdr.Set(cmn.HdrPromoteNamesNum, "1")
			return "", nil
		}

		// directory
		fqns, totalN, cksumVal, err := _promoteScan(srcFQN, prmMsg.Recursive)
		if totalN == 0 {
			if err != nil {
				return "", err
			}
			return "", fmt.Errorf("%s: directory %q is empty", t.si, srcFQN)
		}
		txn := newTxnPromote(c, prmMsg, fqns, srcFQN /*dir*/, totalN)
		if err := t.transactions.begin(txn); err != nil {
			return "", err
		}
		hdr.Set(cmn.HdrPromoteNamesHash, cksumVal)
		hdr.Set(cmn.HdrPromoteNamesNum, strconv.Itoa(totalN))
	case cmn.ActAbort:
		t.transactions.find(c.uuid, cmn.ActAbort)
	case cmn.ActCommit:
		txn, err := t.transactions.find(c.uuid, "")
		if err != nil {
			return "", err
		}
		txnPrm := txn.(*txnPromote)
		if txnPrm.totalN == 0 {
			return "", nil
		}
		isFileShare := c.query.Get(cmn.URLParamPromoteFileShare) != ""

		// promote synchronously wo/ xaction
		if txnPrm.totalN == len(txnPrm.fqns) {
			err := t._promoteNumSync(c, txnPrm, isFileShare)
			return "", err
		}
		// with
		rns := xreg.RenewDirPromote(t, c.bck, txnPrm.dirFQN, txnPrm.msg, isFileShare)
		if rns.Err != nil {
			return "", rns.Err
		}
		xctn := rns.Entry.Get()
		txnPrm.xprm = xctn.(*xs.XactDirPromote)

		t.transactions.find(c.uuid, cmn.ActCommit)
		return xctn.ID(), nil
	default:
		debug.Assert(false)
	}
	return "", nil
}

func _promoteScan(dirFQN string, recurs bool) (fqns []string, totalN int, cksumVal string, err error) {
	cksum := cos.NewCksumHash(cos.ChecksumXXHash)
	cb := func(fqn string, de fs.DirEntry) (err error) {
		if de.IsDir() {
			return
		}
		if len(fqns) == 0 {
			fqns = make([]string, 0, promoteNumSync)
		}
		if len(fqns) < promoteNumSync {
			fqns = append(fqns, fqn)
		}
		totalN++
		cksum.H.Write([]byte(fqn))
		return
	}
	if recurs {
		opts := &fs.WalkOpts{Dir: dirFQN, Callback: cb, Sorted: true}
		err = fs.Walk(opts)
	} else {
		err = fs.WalkDir(dirFQN, cb)
	}

	if err != nil || totalN == 0 {
		return
	}
	cksum.Finalize()
	cksumVal = cksum.Value()
	return
}

func (t *target) _promoteNumSync(c *txnServerCtx, txnPrm *txnPromote, isFileShare bool) error {
	smap := t.owner.smap.Get()
	for _, fqn := range txnPrm.fqns {
		objName, err := cmn.PromotedObjDstName(fqn, txnPrm.dirFQN, txnPrm.msg.ObjName)
		if err != nil {
			return err
		}
		// file share => promote only the part that lands locally
		if isFileShare {
			si, err := cluster.HrwTarget(c.bck.MakeUname(objName), smap)
			if err != nil {
				return err
			}
			if si.ID() != t.si.ID() {
				continue
			}
		}
		params := cluster.PromoteFileParams{
			SrcFQN:    fqn,
			Bck:       c.bck,
			ObjName:   objName,
			Overwrite: txnPrm.msg.Overwrite,
			KeepOrig:  txnPrm.msg.KeepOrig,
		}
		if _, err := t.PromoteFile(params); err != nil {
			return err
		}
	}
	return nil
}

//////////
// misc //
//////////

func (t *target) prepTxnServer(r *http.Request, msg *aisMsg, bucket, phase string) (*txnServerCtx, error) {
	var (
		err   error
		query = r.URL.Query()
		c     = &txnServerCtx{}
	)
	c.msg = msg
	c.callerName = r.Header.Get(cmn.HdrCallerName)
	c.callerID = r.Header.Get(cmn.HdrCallerID)
	c.phase = phase

	if bucket != "" {
		if c.bck, err = newBckFromQuery(bucket, query); err != nil {
			return c, err
		}
	}
	c.bckTo, err = newBckFromQueryUname(query, false /*required*/)
	if err != nil {
		return c, err
	}

	// latency = (network) +- (clock drift)
	if phase == cmn.ActBegin || phase == cmn.ActCommit {
		if ptime := query.Get(cmn.URLParamUnixTime); ptime != "" {
			if delta := ptLatency(time.Now().UnixNano(), ptime); delta != 0 {
				bound := cmn.GCO.Get().Timeout.CplaneOperation / 2
				if delta > int64(bound) || delta < -int64(bound) {
					glog.Errorf("%s: txn %s[%s] latency=%v(!), caller %s, phase=%s, bucket %q",
						t.si, msg.Action, c.msg.UUID, time.Duration(delta),
						c.callerName, phase, bucket)
				}
			}
		}
	}

	c.uuid = c.msg.UUID
	if c.uuid == "" {
		return c, nil
	}
	if tout := query.Get(cmn.URLParamNetwTimeout); tout != "" {
		c.timeout.netw, err = cos.S2Duration(tout)
		debug.AssertNoErr(err)
	}
	if tout := query.Get(cmn.URLParamHostTimeout); tout != "" {
		c.timeout.host, err = cos.S2Duration(tout)
		debug.AssertNoErr(err)
	}
	c.query = query // operation-specific values, if any

	c.t = t
	return c, err
}

//
// notifications
//

func (c *txnServerCtx) addNotif(xctn cluster.Xact) {
	dsts, ok := c.query[cmn.URLParamNotifyMe]
	if !ok {
		return
	}
	xctn.AddNotif(&xact.NotifXact{
		NotifBase: nl.NotifBase{When: cluster.UponTerm, Dsts: dsts, F: c.t.callerNotifyFin},
		Xact:      xctn,
	})
}
