// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/reb"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/NVIDIA/aistore/xact/xs"
	jsoniter "github.com/json-iterator/go"
)

const ActCleanup = "cleanup" // in addition to (apc.ActBegin, ...)

// context structure to gather all (or most) of the relevant state in one place
// (compare with txnCln)
type txnSrv struct {
	t          *target
	msg        *aisMsg
	bck        *meta.Bck // aka bckFrom
	bckTo      *meta.Bck
	query      url.Values
	uuid       string
	phase      string
	callerName string
	callerID   string
	timeout    struct {
		netw time.Duration
		host time.Duration
	}
}

// TODO: return xaction ID (xid) where applicable

// verb /v1/txn
func (t *target) txnHandler(w http.ResponseWriter, r *http.Request) {
	var bucket, phase, xid string
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

	apiItems, err := t.parseURL(w, r, apc.URLPathTxn.L, 0, true)
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

	c := &txnSrv{t: t, msg: msg, phase: phase}
	if err := c.init(r, bucket); err != nil {
		t.writeErr(w, r, err)
		return
	}

	switch msg.Action {
	case apc.ActCreateBck, apc.ActAddRemoteBck:
		err = t.createBucket(c)
	case apc.ActMakeNCopies:
		xid, err = t.makeNCopies(c)
	case apc.ActSetBprops, apc.ActResetBprops:
		xid, err = t.setBprops(c)
	case apc.ActMoveBck:
		xid, err = t.renameBucket(c)
	case apc.ActCopyBck, apc.ActETLBck:
		var (
			dp     core.DP
			tcbmsg = &apc.TCBMsg{}
		)
		if err := cos.MorphMarshal(c.msg.Value, tcbmsg); err != nil {
			t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, msg.Action, c.msg.Value, err)
			return
		}
		if msg.Action == apc.ActETLBck {
			var err error
			if dp, err = etlDP(tcbmsg); err != nil {
				t.writeErr(w, r, err)
				return
			}
		}
		xid, err = t.tcb(c, tcbmsg, dp)
	case apc.ActCopyObjects, apc.ActETLObjects:
		var (
			dp     core.DP
			tcomsg = &cmn.TCObjsMsg{}
		)
		if err := cos.MorphMarshal(c.msg.Value, tcomsg); err != nil {
			t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, msg.Action, c.msg.Value, err)
			return
		}
		if msg.Action == apc.ActETLObjects {
			cs := fs.Cap()
			if err := cs.Err(); err != nil {
				t.writeErr(w, r, err, http.StatusInsufficientStorage)
				return
			}
			var err error
			if dp, err = etlDP(&tcomsg.TCBMsg); err != nil {
				t.writeErr(w, r, err)
				return
			}
		}
		xid, err = t.tcobjs(c, tcomsg, dp)
	case apc.ActECEncode:
		xid, err = t.ecEncode(c)
	case apc.ActArchive:
		xid, err = t.createArchMultiObj(c)
	case apc.ActStartMaintenance, apc.ActDecommissionNode, apc.ActShutdownNode:
		err = t.beginRm(c)
	case apc.ActDestroyBck, apc.ActEvictRemoteBck:
		err = t.destroyBucket(c)
	case apc.ActPromote:
		hdr := w.Header()
		xid, err = t.promote(c, hdr)
	default:
		t.writeErrAct(w, r, msg.Action)
	}
	if err == nil {
		if xid != "" {
			w.Header().Set(apc.HdrXactionID, xid)
		}
		return
	}

	// cleanup on error
	t.transactions.find(c.uuid, ActCleanup)

	if cmn.IsErrCapExceeded(err) {
		cs := t.OOS(nil)
		t.writeErrStatusf(w, r, http.StatusInsufficientStorage, "%s: %v", cs.String(), err)
	} else {
		t.writeErr(w, r, err)
	}
}

//
// createBucket
//

func (t *target) createBucket(c *txnSrv) error {
	switch c.phase {
	case apc.ActBegin:
		txn := newTxnCreateBucket(c)
		if err := t.transactions.begin(txn); err != nil {
			return err
		}
		if c.msg.Action == apc.ActCreateBck && c.bck.IsRemote() {
			if c.msg.Value != nil {
				if err := cos.MorphMarshal(c.msg.Value, &c.bck.Props); err != nil {
					return fmt.Errorf(cmn.FmtErrMorphUnmarshal, t, c.msg.Action, c.msg.Value, err)
				}
			}
			if _, err := t.Backend(c.bck).CreateBucket(c.bck); err != nil {
				return err
			}
		}
	case apc.ActAbort:
		t.transactions.find(c.uuid, apc.ActAbort)
	case apc.ActCommit:
		t._commitCreateDestroy(c)
	default:
		debug.Assert(false)
	}
	return nil
}

func (t *target) _commitCreateDestroy(c *txnSrv) (err error) {
	txn, err := t.transactions.find(c.uuid, "")
	if err != nil {
		return err
	}
	// wait for newBMD w/timeout
	if err = t.transactions.wait(txn, c.timeout.netw, c.timeout.host); err != nil {
		return cmn.NewErrFailedTo(t, "commit", txn, err)
	}
	return
}

//
// makeNCopies
//

func (t *target) makeNCopies(c *txnSrv) (string, error) {
	switch c.phase {
	case apc.ActBegin:
		if err := c.bck.Init(t.owner.bmd); err != nil {
			return "", err
		}
		curCopies, newCopies, err := t.validateMakeNCopies(c.bck, c.msg)
		if err != nil {
			return "", err
		}
		cs := fs.Cap()
		if err := cs.Err(); err != nil {
			return "", err
		}
		nlp := newBckNLP(c.bck)
		if !nlp.TryLock(c.timeout.netw / 2) {
			return "", cmn.NewErrBusy("bucket", c.bck.Cname(""))
		}
		txn := newTxnMakeNCopies(c, curCopies, newCopies)
		if err := t.transactions.begin(txn, nlp); err != nil {
			return "", err
		}
	case apc.ActAbort:
		t.transactions.find(c.uuid, apc.ActAbort)
	case apc.ActCommit:
		if err := c.bck.Init(t.owner.bmd); err != nil {
			return "", err
		}
		copies, err := _parseNCopies(c.msg.Value)
		debug.AssertNoErr(err)
		txn, err := t.transactions.find(c.uuid, "")
		if err != nil {
			return "", err
		}
		txnMnc := txn.(*txnMakeNCopies)
		debug.Assert(txnMnc.newCopies == copies)

		// wait for newBMD w/timeout
		if err = t.transactions.wait(txn, c.timeout.netw, c.timeout.host); err != nil {
			return "", cmn.NewErrFailedTo(t, "commit", txn, err)
		}

		// do the work in xaction
		rns := xreg.RenewBckMakeNCopies(c.bck, c.uuid, "mnc-actmnc", int(copies))
		if rns.Err != nil {
			return "", fmt.Errorf("%s %s: %v", t, txn, rns.Err)
		}
		xctn := rns.Entry.Get()
		flt := xreg.Flt{Kind: apc.ActPutCopies, Bck: c.bck}
		xreg.DoAbort(flt, errors.New("make-n-copies"))
		c.addNotif(xctn) // notify upon completion
		xact.GoRunW(xctn)

		return xctn.ID(), nil
	default:
		debug.Assert(false)
	}
	return "", nil
}

func (t *target) validateMakeNCopies(bck *meta.Bck, msg *aisMsg) (curCopies, newCopies int64, err error) {
	curCopies = bck.Props.Mirror.Copies
	newCopies, err = _parseNCopies(msg.Value)
	if err == nil {
		err = fs.ValidateNCopies(t.si.Name(), int(newCopies))
	}
	// (consider adding "force" option similar to CopyBckMsg.Force)
	if err == nil {
		err = xreg.LimitedCoexistence(t.si, bck, msg.Action)
	}
	if err != nil {
		return
	}
	// don't allow increasing num-copies when used cap is above high wm (let alone OOS)
	if bck.Props.Mirror.Copies < newCopies {
		cs := fs.Cap()
		err = cs.Err()
	}
	return
}

//
// setBprops
//

func (t *target) setBprops(c *txnSrv) (string, error) {
	switch c.phase {
	case apc.ActBegin:
		if err := c.bck.Init(t.owner.bmd); err != nil {
			return "", err
		}
		var (
			nprops *cmn.Bprops
			err    error
		)
		if nprops, err = t.validateNprops(c.bck, c.msg); err != nil {
			return "", err
		}
		nlp := newBckNLP(c.bck)
		if !nlp.TryLock(c.timeout.netw / 2) {
			return "", cmn.NewErrBusy("bucket", c.bck.Cname(""))
		}
		txn := newTxnSetBucketProps(c, nprops)
		if err := t.transactions.begin(txn, nlp); err != nil {
			return "", err
		}
	case apc.ActAbort:
		t.transactions.find(c.uuid, apc.ActAbort)
	case apc.ActCommit:
		if err := c.bck.Init(t.owner.bmd); err != nil {
			return "", err
		}
		var xid string
		txn, err := t.transactions.find(c.uuid, "")
		if err != nil {
			return "", err
		}
		txnSetBprops := txn.(*txnSetBucketProps)
		bprops, nprops := txnSetBprops.bprops, txnSetBprops.nprops
		// wait for newBMD w/timeout
		if err = t.transactions.wait(txn, c.timeout.netw, c.timeout.host); err != nil {
			return "", cmn.NewErrFailedTo(t, "commit", txn, err)
		}
		if _reMirror(bprops, nprops) {
			n := int(nprops.Mirror.Copies)
			rns := xreg.RenewBckMakeNCopies(c.bck, c.uuid, "mnc-setprops", n)
			if rns.Err != nil {
				return "", fmt.Errorf("%s %s: %v", t, txn, rns.Err)
			}
			xctn := rns.Entry.Get()
			flt := xreg.Flt{Kind: apc.ActPutCopies, Bck: c.bck}
			xreg.DoAbort(flt, errors.New("re-mirror"))
			c.addNotif(xctn) // notify upon completion
			xact.GoRunW(xctn)
			xid = xctn.ID()
		}
		if _, reec := _reEC(bprops, nprops, c.bck, nil /*smap*/); reec {
			flt := xreg.Flt{Kind: apc.ActECEncode, Bck: c.bck}
			xreg.DoAbort(flt, errors.New("re-ec"))
			rns := xreg.RenewECEncode(c.bck, c.uuid, apc.ActCommit)
			if rns.Err != nil {
				return "", rns.Err
			}
			xctn := rns.Entry.Get()
			c.addNotif(xctn) // ditto
			xact.GoRunW(xctn)

			if xid == "" {
				xid = xctn.ID()
			} else {
				xid = "" // not supporting multiple..
			}
		}
		return xid, nil
	default:
		debug.Assert(false)
	}
	return "", nil
}

func (t *target) validateNprops(bck *meta.Bck, msg *aisMsg) (nprops *cmn.Bprops, err error) {
	var (
		body = cos.MustMarshal(msg.Value)
		cs   = fs.Cap()
	)
	nprops = &cmn.Bprops{}
	if err = jsoniter.Unmarshal(body, nprops); err != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, t, "new bucket props", cos.BHead(body), err)
		return
	}
	err = cs.Err()
	if nprops.Mirror.Enabled {
		mpathCount := fs.NumAvail()
		if int(nprops.Mirror.Copies) > mpathCount {
			err = fmt.Errorf(fmtErrInsuffMpaths1, t, mpathCount, bck, nprops.Mirror.Copies)
			return
		}
		if nprops.Mirror.Copies < bck.Props.Mirror.Copies {
			err = nil
		}
	}
	if !nprops.EC.Enabled && bck.Props.EC.Enabled {
		err = nil
	}
	return
}

//
// renameBucket
//

func (t *target) renameBucket(c *txnSrv) (string, error) {
	switch c.phase {
	case apc.ActBegin:
		if err := c.bck.Init(t.owner.bmd); err != nil {
			return "", err
		}
		bckFrom, bckTo := c.bck, c.bckTo
		if err := t.validateBckRenTxn(bckFrom, bckTo, c.msg); err != nil {
			return "", err
		}
		nlpFrom := newBckNLP(bckFrom)
		nlpTo := newBckNLP(bckTo)
		if !nlpFrom.TryLock(c.timeout.netw / 4) {
			return "", cmn.NewErrBusy("bucket", bckFrom.Cname(""))
		}
		if !nlpTo.TryLock(c.timeout.netw / 4) {
			nlpFrom.Unlock()
			return "", cmn.NewErrBusy("bucket", bckTo.Cname(""))
		}
		txn := newTxnRenameBucket(c, bckFrom, bckTo)
		if err := t.transactions.begin(txn, nlpFrom, nlpTo); err != nil {
			return "", err
		}
	case apc.ActAbort:
		t.transactions.find(c.uuid, apc.ActAbort)
	case apc.ActCommit:
		if err := c.bck.Init(t.owner.bmd); err != nil {
			return "", err
		}
		txn, err := t.transactions.find(c.uuid, "")
		if err != nil {
			return "", err
		}
		txnRenB := txn.(*txnRenameBucket)
		// wait for newBMD w/timeout
		if err = t.transactions.wait(txn, c.timeout.netw, c.timeout.host); err != nil {
			return "", cmn.NewErrFailedTo(t, "commit", txn, err)
		}
		rns := xreg.RenewBckRename(txnRenB.bckFrom, txnRenB.bckTo, c.uuid, c.msg.RMDVersion, apc.ActCommit)
		if rns.Err != nil {
			nlog.Errorf("%s: %s %v", t, txn, rns.Err)
			return "", rns.Err // must not happen at commit time
		}
		xctn := rns.Entry.Get()
		err = fs.RenameBucketDirs(txnRenB.bckFrom.Bucket(), txnRenB.bckTo.Bucket())
		if err != nil {
			return "", err // ditto
		}
		c.addNotif(xctn) // notify upon completion

		reb.OnTimedGFN()
		xact.GoRunW(xctn) // run and wait until it starts running

		return xctn.ID(), nil
	default:
		debug.Assert(false)
	}
	return "", nil
}

func (t *target) validateBckRenTxn(bckFrom, bckTo *meta.Bck, msg *aisMsg) error {
	cs := fs.Cap()
	if err := cs.Err(); err != nil {
		return err
	}
	if err := xreg.LimitedCoexistence(t.si, bckFrom, msg.Action, bckTo); err != nil {
		return err
	}
	bmd := t.owner.bmd.get()
	if _, present := bmd.Get(bckFrom); !present {
		return cmn.NewErrBckNotFound(bckFrom.Bucket())
	}
	if _, present := bmd.Get(bckTo); present {
		return cmn.NewErrBckAlreadyExists(bckTo.Bucket())
	}
	availablePaths := fs.GetAvail()
	for _, mi := range availablePaths {
		path := mi.MakePathCT(bckTo.Bucket(), fs.ObjectType)
		if err := cos.Stat(path); err != nil {
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

func etlDP(msg *apc.TCBMsg) (core.DP, error) {
	if !k8s.IsK8s() {
		return nil, k8s.ErrK8sRequired
	}
	if err := msg.Validate(true); err != nil {
		return nil, err
	}
	return etl.NewOfflineDP(msg, cmn.GCO.Get())
}

// common for both bucket copy and bucket transform - does the heavy lifting
func (t *target) tcb(c *txnSrv, msg *apc.TCBMsg, dp core.DP) (string, error) {
	switch c.phase {
	case apc.ActBegin:
		if err := c.bck.Init(t.owner.bmd); err != nil {
			return "", err
		}
		bckTo, bckFrom := c.bckTo, c.bck
		if err := bckTo.Validate(); err != nil {
			return "", err
		}
		if err := bckFrom.Validate(); err != nil {
			return "", err
		}
		cs := fs.Cap()
		if err := cs.Err(); err != nil {
			return "", err
		}
		if err := xreg.LimitedCoexistence(t.si, bckFrom, c.msg.Action); err != nil {
			if !msg.Force {
				return "", err
			}
			nlog.Errorf("%s: %v - %q is \"forced\", proceeding anyway", t, err, c.msg.Action)
		}
		bmd := t.owner.bmd.get()
		if _, present := bmd.Get(bckFrom); !present {
			return "", cmn.NewErrBckNotFound(bckFrom.Bucket())
		}
		if err := t._tcbBegin(c, msg, dp); err != nil {
			return "", err
		}
	case apc.ActAbort:
		t.transactions.find(c.uuid, apc.ActAbort)
	case apc.ActCommit:
		if err := c.bck.Init(t.owner.bmd); err != nil {
			return "", err
		}
		txn, err := t.transactions.find(c.uuid, "")
		if err != nil {
			return "", err
		}
		txnTcb := txn.(*txnTCB)

		if c.query.Get(apc.QparamWaitMetasync) != "" {
			if err = t.transactions.wait(txn, c.timeout.netw, c.timeout.host); err != nil {
				txnTcb.xtcb.TxnAbort(err)
				return "", cmn.NewErrFailedTo(t, "commit", txn, err)
			}
		} else {
			t.transactions.find(c.uuid, apc.ActCommit)
		}

		custom := txnTcb.xtcb.Args()
		if custom.Phase != apc.ActBegin {
			err = fmt.Errorf("%s: %s is already running", t, txnTcb) // never here
			nlog.Errorln(err)
			return "", err
		}
		custom.Phase = apc.ActCommit
		rns := xreg.RenewTCB(c.uuid, c.msg.Action /*kind*/, txnTcb.xtcb.Args())
		if rns.Err != nil {
			if !cmn.IsErrXactUsePrev(rns.Err) {
				txnTcb.xtcb.TxnAbort(rns.Err)
				nlog.Errorf("%s: %s %v", t, txn, rns.Err)
			}
			return "", rns.Err
		}
		xctn := rns.Entry.Get()
		xid := xctn.ID()
		debug.Assert(xid == txnTcb.xtcb.ID())
		c.addNotif(xctn) // notify upon completion
		xact.GoRunW(xctn)
		return xid, nil
	default:
		debug.Assert(false)
	}
	return "", nil
}

func (t *target) _tcbBegin(c *txnSrv, msg *apc.TCBMsg, dp core.DP) (err error) {
	var (
		bckTo, bckFrom = c.bckTo, c.bck
		nlpFrom        = newBckNLP(bckFrom)
		nlpTo          core.NLP
	)
	if !nlpFrom.TryRLock(c.timeout.netw / 4) {
		return cmn.NewErrBusy("bucket", bckFrom.Cname(""))
	}
	if !msg.DryRun && !bckFrom.Equal(bckTo, true, true) {
		nlpTo = newBckNLP(bckTo)
		if !nlpTo.TryLock(c.timeout.netw / 4) {
			nlpFrom.Unlock()
			return cmn.NewErrBusy("bucket", bckTo.Cname(""))
		}
	}
	custom := &xreg.TCBArgs{Phase: apc.ActBegin, BckFrom: bckFrom, BckTo: bckTo, DP: dp, Msg: msg}
	rns := xreg.RenewTCB(c.uuid, c.msg.Action /*kind*/, custom)
	if err = rns.Err; err != nil {
		nlog.Errorf("%s: %q %+v %v", t, c.uuid, msg, rns.Err)
		return
	}

	var (
		xctn = rns.Entry.Get()
		xtcb = xctn.(*xs.XactTCB)
		txn  = newTxnTCB(c, xtcb)
		nlps = []core.NLP{nlpFrom}
	)
	if nlpTo != nil {
		nlps = append(nlps, nlpTo)
	}
	return t.transactions.begin(txn, nlps...)
}

// Two IDs:
// - TxnUUID: transaction (txn) ID
// - xid: xaction ID (will have "tco-" prefix)
func (t *target) tcobjs(c *txnSrv, msg *cmn.TCObjsMsg, dp core.DP) (xid string, _ error) {
	switch c.phase {
	case apc.ActBegin:
		var (
			bckTo   = c.bckTo
			bckFrom = c.bck // from
		)
		if err := c.bck.Init(t.owner.bmd); err != nil {
			return xid, err
		}
		// validate
		if err := bckTo.Validate(); err != nil {
			return xid, err
		}
		if err := bckFrom.Validate(); err != nil {
			return xid, err
		}
		cs := fs.Cap()
		if err := cs.Err(); err != nil {
			return xid, err
		}
		if err := xreg.LimitedCoexistence(t.si, bckFrom, c.msg.Action); err != nil {
			return xid, err
		}
		bmd := t.owner.bmd.get()
		if _, present := bmd.Get(bckFrom); !present {
			return xid, cmn.NewErrBckNotFound(bckFrom.Bucket())
		}
		// begin
		custom := &xreg.TCObjsArgs{BckFrom: bckFrom, BckTo: bckTo, DP: dp}
		rns := xreg.RenewTCObjs(c.msg.Action /*kind*/, custom)
		if rns.Err != nil {
			nlog.Errorf("%s: %q %+v %v", t, c.uuid, c.msg, rns.Err)
			return xid, rns.Err
		}
		xctn := rns.Entry.Get()
		xid = xctn.ID()

		xtco := xctn.(*xs.XactTCObjs)
		msg.TxnUUID = c.uuid
		txn := newTxnTCObjs(c, bckFrom, xtco, msg)
		if err := t.transactions.begin(txn); err != nil {
			return xid, err
		}
		xtco.Begin(msg)
	case apc.ActAbort:
		txn, err := t.transactions.find(c.uuid, apc.ActAbort)
		if err == nil {
			txnTco := txn.(*txnTCObjs)
			// if _this_ transaction initiated _that_ on-demand
			if xtco := txnTco.xtco; xtco != nil && xtco.ID() == c.uuid {
				xid = xtco.ID()
				xtco.Abort(nil)
			}
		}
	case apc.ActCommit:
		if err := c.bck.Init(t.owner.bmd); err != nil {
			return xid, err
		}
		txn, err := t.transactions.find(c.uuid, "")
		if err != nil {
			return xid, err
		}
		txnTco := txn.(*txnTCObjs)
		var done bool
		if c.query.Get(apc.QparamWaitMetasync) != "" {
			if err = t.transactions.wait(txn, c.timeout.netw, c.timeout.host); err != nil {
				txnTco.xtco.TxnAbort(err)
				return "", cmn.NewErrFailedTo(t, "commit", txn, err)
			}
			done = true
		}

		txnTco.xtco.Do(txnTco.msg)
		xid = txnTco.xtco.ID()
		if !done {
			t.transactions.find(c.uuid, apc.ActCommit)
		}
	default:
		debug.Assert(false)
	}
	return xid, nil
}

//
// ecEncode
//

func (t *target) ecEncode(c *txnSrv) (string, error) {
	switch c.phase {
	case apc.ActBegin:
		if err := c.bck.Init(t.owner.bmd); err != nil {
			return "", err
		}
		if err := t.validateECEncode(c.bck, c.msg); err != nil {
			return "", err
		}
		cs := fs.Cap()
		if err := cs.Err(); err != nil {
			return "", err
		}
		nlp := newBckNLP(c.bck)

		if !nlp.TryLock(c.timeout.netw / 4) {
			return "", cmn.NewErrBusy("bucket", c.bck.Cname(""))
		}
		txn := newTxnECEncode(c, c.bck)
		if err := t.transactions.begin(txn, nlp); err != nil {
			return "", err
		}
	case apc.ActAbort:
		t.transactions.find(c.uuid, apc.ActAbort)
	case apc.ActCommit:
		if err := c.bck.Init(t.owner.bmd); err != nil {
			return "", err
		}
		txn, err := t.transactions.find(c.uuid, "")
		if err != nil {
			return "", err
		}
		// wait for newBMD w/timeout
		if err = t.transactions.wait(txn, c.timeout.netw, c.timeout.host); err != nil {
			return "", cmn.NewErrFailedTo(t, "commit", txn, err)
		}
		rns := xreg.RenewECEncode(c.bck, c.uuid, apc.ActCommit)
		if rns.Err != nil {
			nlog.Errorf("%s: %s %v", t, txn, rns.Err)
			return "", rns.Err
		}
		xctn := rns.Entry.Get()
		c.addNotif(xctn) // notify upon completion
		xact.GoRunW(xctn)

		return xctn.ID(), rns.Err
	default:
		debug.Assert(false)
	}
	return "", nil
}

func (t *target) validateECEncode(bck *meta.Bck, msg *aisMsg) error {
	cs := fs.Cap()
	if err := cs.Err(); err != nil {
		return err
	}
	return xreg.LimitedCoexistence(t.si, bck, msg.Action)
}

//
// createArchMultiObj
//

func (t *target) createArchMultiObj(c *txnSrv) (string /*xaction uuid*/, error) {
	var xid string
	switch c.phase {
	case apc.ActBegin:
		var (
			bckTo   = c.bckTo
			bckFrom = c.bck
		)
		if err := c.bck.Init(t.owner.bmd); err != nil {
			return xid, err
		}
		if err := bckTo.Validate(); err != nil {
			return xid, err
		}
		if !bckFrom.Equal(bckTo, false, false) {
			if err := bckFrom.Validate(); err != nil {
				return xid, err
			}
		}
		archMsg := &cmn.ArchiveBckMsg{}
		if err := cos.MorphMarshal(c.msg.Value, archMsg); err != nil {
			return xid, fmt.Errorf(cmn.FmtErrMorphUnmarshal, t, c.msg.Action, c.msg.Value, err)
		}
		mime, err := archive.Mime(archMsg.Mime, archMsg.ArchName)
		if err != nil {
			return xid, err
		}
		archMsg.Mime = mime // set it for xarch

		cs := fs.Cap()
		if err := cs.Err(); err != nil {
			return xid, err
		}

		rns := xreg.RenewPutArchive(bckFrom, bckTo)
		if rns.Err != nil {
			nlog.Errorf("%s: %q %+v %v", t, c.uuid, archMsg, rns.Err)
			return xid, rns.Err
		}
		xctn := rns.Entry.Get()
		xid = xctn.ID()

		xarch := xctn.(*xs.XactArch)
		// finalize the message and begin local transaction
		archMsg.TxnUUID = c.uuid
		archMsg.FromBckName = bckFrom.Name
		archlom := core.AllocLOM(archMsg.ArchName)
		if err := xarch.Begin(archMsg, archlom); err != nil {
			core.FreeLOM(archlom) // otherwise is freed by x-archive
			return xid, err
		}
		txn := newTxnArchMultiObj(c, bckFrom, xarch, archMsg)
		if err := t.transactions.begin(txn); err != nil {
			return xid, err
		}
	case apc.ActAbort:
		txn, err := t.transactions.find(c.uuid, apc.ActAbort)
		if err == nil {
			txnArch := txn.(*txnArchMultiObj)
			// if _this_ transaction initiated _that_ on-demand
			if xarch := txnArch.xarch; xarch != nil && xarch.ID() == c.uuid {
				xid = xarch.ID()
				xarch.Abort(nil)
			}
		}
	case apc.ActCommit:
		if err := c.bck.Init(t.owner.bmd); err != nil {
			return xid, err
		}
		txn, err := t.transactions.find(c.uuid, "")
		if err != nil {
			return xid, err
		}
		txnArch := txn.(*txnArchMultiObj)
		txnArch.xarch.Do(txnArch.msg)
		xid = txnArch.xarch.ID()
		t.transactions.find(c.uuid, apc.ActCommit)
	}
	return xid, nil
}

//
// begin (maintenance -- decommission -- shutdown) via p.beginRmTarget
//

func (t *target) beginRm(c *txnSrv) error {
	var opts apc.ActValRmNode
	if c.phase != apc.ActBegin {
		return fmt.Errorf("%s: expecting begin phase, got %q", t, c.phase)
	}
	if err := cos.MorphMarshal(c.msg.Value, &opts); err != nil {
		return fmt.Errorf(cmn.FmtErrMorphUnmarshal, t, c.msg.Action, c.msg.Value, err)
	}
	return xreg.LimitedCoexistence(t.si, nil, c.msg.Action)
}

//
// destroy local bucket / evict cloud bucket
//

func (t *target) destroyBucket(c *txnSrv) error {
	switch c.phase {
	case apc.ActBegin:
		nlp := newBckNLP(c.bck)
		if !nlp.TryLock(c.timeout.netw / 2) {
			return cmn.NewErrBusy("bucket", c.bck.Cname(""))
		}
		txn := newTxnBckBase(c.bck)
		txn.fillFromCtx(c)
		if err := t.transactions.begin(txn, nlp); err != nil {
			return err
		}
	case apc.ActAbort:
		t.transactions.find(c.uuid, apc.ActAbort)
	case apc.ActCommit:
		t._commitCreateDestroy(c)
	default:
		debug.Assert(false)
	}
	return nil
}

func (t *target) promote(c *txnSrv, hdr http.Header) (string, error) {
	switch c.phase {
	case apc.ActBegin:
		if err := c.bck.Init(t.owner.bmd); err != nil {
			return "", err
		}
		cs := fs.Cap()
		if err := cs.Err(); err != nil {
			return "", err
		}
		prmMsg := &apc.PromoteArgs{}
		if err := cos.MorphMarshal(c.msg.Value, prmMsg); err != nil {
			err = fmt.Errorf(cmn.FmtErrMorphUnmarshal, t, c.msg.Action, c.msg.Value, err)
			return "", err
		}
		if strings.Contains(prmMsg.ObjName, "../") || strings.Contains(prmMsg.ObjName, "~/") {
			return "", fmt.Errorf("invalid object name or prefix %q", prmMsg.ObjName)
		}
		srcFQN := c.msg.Name
		finfo, err := os.Stat(srcFQN)
		if err != nil {
			return "", err
		}
		if !finfo.IsDir() {
			txn := newTxnPromote(c, prmMsg, []string{srcFQN}, "" /*dirFQN*/, 1)
			if err := t.transactions.begin(txn); err != nil {
				return "", err
			}
			hdr.Set(apc.HdrPromoteNamesNum, "1")
			return "", nil
		}

		// directory
		fqns, totalN, cksumVal, err := prmScan(srcFQN, prmMsg)
		if totalN == 0 {
			if err != nil {
				return "", err
			}
			return "", fmt.Errorf("%s: directory %q is empty", t, srcFQN)
		}
		txn := newTxnPromote(c, prmMsg, fqns, srcFQN /*dir*/, totalN)
		if err := t.transactions.begin(txn); err != nil {
			return "", err
		}
		hdr.Set(apc.HdrPromoteNamesHash, cksumVal)
		hdr.Set(apc.HdrPromoteNamesNum, strconv.Itoa(totalN))
	case apc.ActAbort:
		t.transactions.find(c.uuid, apc.ActAbort)
	case apc.ActCommit:
		if err := c.bck.Init(t.owner.bmd); err != nil {
			return "", err
		}
		txn, err := t.transactions.find(c.uuid, "")
		if err != nil {
			return "", err
		}
		txnPrm, ok := txn.(*txnPromote)
		debug.Assert(ok)
		defer t.transactions.find(c.uuid, apc.ActCommit)

		if txnPrm.totalN == 0 {
			nlog.Infof("%s: nothing to do (%s)", t, txnPrm)
			return "", nil
		}
		// set by controlling proxy upon collecting and comparing all the begin-phase results
		txnPrm.fshare = c.query.Get(apc.QparamConfirmFshare) != ""

		// promote synchronously wo/ xaction;
		// (set by proxy to eliminate any ambiguity vis-a-vis `promoteNumSync` special)
		if noXact := c.query.Get(apc.QparamActNoXact) != ""; noXact {
			nlog.Infof("%s: promote synchronously %s", t, txnPrm)
			err := t.prmNumFiles(c, txnPrm, txnPrm.fshare)
			return "", err
		}

		rns := xreg.RenewPromote(c.uuid, c.bck, txnPrm.msg)
		if rns.Err != nil {
			nlog.Errorf("%s: %s %v", t, txnPrm, rns.Err)
			return "", rns.Err
		}
		xprm := rns.Entry.Get().(*xs.XactDirPromote)
		xprm.SetFshare(txnPrm.fshare)
		txnPrm.xprm = xprm

		c.addNotif(xprm) // upon completion
		xact.GoRunW(xprm)
		return xprm.ID(), nil
	default:
		debug.Assert(false)
	}
	return "", nil
}

// scan and, optionally, auto-detect file-share
func prmScan(dirFQN string, prmMsg *apc.PromoteArgs) (fqns []string, totalN int, cksumVal string, err error) {
	var (
		cksum      *cos.CksumHash
		autoDetect = !prmMsg.SrcIsNotFshare || !cmn.Rom.Features().IsSet(feat.DontAutoDetectFshare)
	)
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
		if autoDetect {
			cksum.H.Write([]byte(fqn))
		}
		return
	}
	if autoDetect {
		cksum = cos.NewCksumHash(cos.ChecksumXXHash)
	}
	if prmMsg.Recursive {
		opts := &fs.WalkOpts{Dir: dirFQN, Callback: cb, Sorted: true}
		err = fs.Walk(opts)
	} else {
		err = fs.WalkDir(dirFQN, cb)
	}

	if err != nil || totalN == 0 || !autoDetect {
		return
	}
	cksum.Finalize()
	cksumVal = cksum.Value()
	return
}

// synchronously wo/ xaction
func (t *target) prmNumFiles(c *txnSrv, txnPrm *txnPromote, confirmedFshare bool) error {
	smap := t.owner.smap.Get()
	config := cmn.GCO.Get()
	for _, fqn := range txnPrm.fqns {
		objName, err := xs.PrmObjName(fqn, txnPrm.dirFQN, txnPrm.msg.ObjName)
		if err != nil {
			return err
		}
		// file share == true: promote only the part of the txnPrm.fqns that "lands" locally
		if confirmedFshare {
			si, err := smap.HrwName2T(c.bck.MakeUname(objName))
			if err != nil {
				return err
			}
			if si.ID() != t.SID() {
				continue
			}
		}
		params := core.PromoteParams{
			Bck:    c.bck,
			Config: config,
			PromoteArgs: apc.PromoteArgs{
				SrcFQN:       fqn,
				ObjName:      objName,
				OverwriteDst: txnPrm.msg.OverwriteDst,
				DeleteSrc:    txnPrm.msg.DeleteSrc,
			},
		}
		if _, err := t.Promote(&params); err != nil {
			return err
		}
	}
	return nil
}

////////////
// txnSrv //
////////////

func (c *txnSrv) init(r *http.Request, bucket string) (err error) {
	c.callerName = r.Header.Get(apc.HdrCallerName)
	c.callerID = r.Header.Get(apc.HdrCallerID)

	query := r.URL.Query()
	if bucket != "" {
		if c.bck, err = newBckFromQ(bucket, query, nil); err != nil {
			return err
		}
	}
	c.bckTo, err = newBckFromQuname(query, false /*required*/)
	if err != nil {
		return err
	}

	// latency = (network) +- (clock drift)
	if c.phase == apc.ActBegin {
		if ptime := query.Get(apc.QparamUnixTime); ptime != "" {
			now := time.Now().UnixNano()
			dur := ptLatency(now, ptime, r.Header.Get(apc.HdrCallerIsPrimary))
			lim := int64(cmn.Rom.CplaneOperation()) >> 1
			if dur > lim || dur < -lim {
				nlog.Errorf("Warning: clock drift %s <-> %s(self) = %v, txn %s[%s]",
					c.callerName, c.t, time.Duration(dur), c.msg.Action, c.msg.UUID)
			}
		}
	}

	c.uuid = c.msg.UUID
	if c.uuid == "" {
		return nil
	}
	if tout := query.Get(apc.QparamNetwTimeout); tout != "" {
		c.timeout.netw, err = cos.S2Duration(tout)
		debug.AssertNoErr(err)
	}
	if tout := query.Get(apc.QparamHostTimeout); tout != "" {
		c.timeout.host, err = cos.S2Duration(tout)
		debug.AssertNoErr(err)
	}
	c.query = query // operation-specific values, if any
	return err
}

func (c *txnSrv) addNotif(xctn core.Xact) {
	dsts, ok := c.query[apc.QparamNotifyMe]
	if !ok {
		return
	}
	xctn.AddNotif(&xact.NotifXact{
		Base: nl.Base{When: core.UponTerm, Dsts: dsts, F: c.t.notifyTerm},
		Xact: xctn,
	})
}
