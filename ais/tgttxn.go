// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/mirror"
	"github.com/NVIDIA/aistore/xaction"
	jsoniter "github.com/json-iterator/go"
)

// convenience structure to gather all (or most) of the relevant context in one place
// (compare with txnClientCtx & prepTxnClient)
type txnServerCtx struct {
	uuid       string
	timeout    time.Duration
	phase      string
	smapVer    int64
	bmdVer     int64
	msg        *aisMsg
	callerName string
	callerID   string
	bck        *cluster.Bck
	query      url.Values
	t          *targetrunner
}

// verb /v1/txn
func (t *targetrunner) txnHandler(w http.ResponseWriter, r *http.Request) {
	// 1. check
	if r.Method != http.MethodPost {
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /txn path")
		return
	}
	msg := &aisMsg{}
	if cmn.ReadJSON(w, r, msg) != nil {
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
	// 2. gather all context
	c, err := t.prepTxnServer(r, msg, apiItems)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	// 3. do
	switch msg.Action {
	case cmn.ActCreateLB, cmn.ActRegisterCB:
		if err = t.createBucket(c); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
	case cmn.ActMakeNCopies:
		if err = t.makeNCopies(c); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
	case cmn.ActSetBprops, cmn.ActResetBprops:
		if err = t.setBucketProps(c); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
	case cmn.ActRenameLB:
		if err = t.renameBucket(c); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
	case cmn.ActCopyBucket:
		if err = t.copyBucket(c); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
	case cmn.ActECEncode:
		if err = t.ecEncode(c); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
	default:
		t.invalmsghdlrf(w, r, fmtUnknownAct, msg)
	}
}

//////////////////
// createBucket //
//////////////////

func (t *targetrunner) createBucket(c *txnServerCtx) error {
	switch c.phase {
	case cmn.ActBegin:
		txn := newTxnCreateBucket(c)
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

/////////////////
// makeNCopies //
/////////////////

func (t *targetrunner) makeNCopies(c *txnServerCtx) error {
	if err := c.bck.Init(t.owner.bmd, t.si); err != nil {
		return err
	}
	switch c.phase {
	case cmn.ActBegin:
		curCopies, newCopies, err := t.validateMakeNCopies(c.bck, c.msg)
		if err != nil {
			return err
		}
		nlp := c.bck.GetNameLockPair()
		if !nlp.TryLock() {
			return cmn.NewErrorBucketIsBusy(c.bck.Bck, t.si.Name())
		}
		txn := newTxnMakeNCopies(c, curCopies, newCopies)
		if err := t.transactions.begin(txn); err != nil {
			nlp.Unlock()
			return err
		}
		txn.nlps = []cmn.NLP{nlp}
	case cmn.ActAbort:
		t.transactions.find(c.uuid, true /* remove */)
	case cmn.ActCommit:
		copies, _ := t.parseNCopies(c.msg.Value)
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

		// do the work in xaction
		xact, err := xaction.Registry.RenewBckMakeNCopies(c.bck, t, c.uuid, int(copies))
		if err != nil {
			return fmt.Errorf("%s %s: %v", t.si, txn, err)
		}

		xaction.Registry.DoAbort(cmn.ActPutCopies, c.bck)

		c.addNotif(xact) // notify upon completion
		go xact.Run()
	default:
		cmn.Assert(false)
	}
	return nil
}

func (t *targetrunner) validateMakeNCopies(bck *cluster.Bck, msg *aisMsg) (curCopies, newCopies int64, err error) {
	curCopies = bck.Props.Mirror.Copies
	newCopies, err = t.parseNCopies(msg.Value)
	if err == nil {
		err = mirror.ValidateNCopies(t.si.Name(), int(newCopies))
	}
	// NOTE: #791 "limited coexistence" here and elsewhere
	if err == nil {
		err = t.coExists(bck, msg)
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

func (t *targetrunner) setBucketProps(c *txnServerCtx) error {
	if err := c.bck.Init(t.owner.bmd, t.si); err != nil {
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
		if !nlp.TryLock() {
			return cmn.NewErrorBucketIsBusy(c.bck.Bck, t.si.Name())
		}
		txn := newTxnSetBucketProps(c, nprops)
		if err := t.transactions.begin(txn); err != nil {
			nlp.Unlock()
			return err
		}
		txn.nlps = []cmn.NLP{nlp}
	case cmn.ActAbort:
		t.transactions.find(c.uuid, true /* remove */)
	case cmn.ActCommit:
		txn, err := t.transactions.find(c.uuid, false)
		if err != nil {
			return fmt.Errorf("%s %s: %v", t.si, txn, err)
		}
		txnSetBprops := txn.(*txnSetBucketProps)
		// wait for newBMD w/timeout
		if err = t.transactions.wait(txn, c.timeout); err != nil {
			return fmt.Errorf("%s %s: %v", t.si, txn, err)
		}
		if remirror(txnSetBprops.bprops, txnSetBprops.nprops) {
			n := int(txnSetBprops.nprops.Mirror.Copies)
			xact, err := xaction.Registry.RenewBckMakeNCopies(c.bck, t, c.uuid, n)
			if err != nil {
				return fmt.Errorf("%s %s: %v", t.si, txn, err)
			}
			xaction.Registry.DoAbort(cmn.ActPutCopies, c.bck)

			c.addNotif(xact) // notify upon completion
			go xact.Run()
		}
		if reECEncode(txnSetBprops.bprops, txnSetBprops.nprops, c.bck) {
			xaction.Registry.DoAbort(cmn.ActECEncode, c.bck)
			xact, err := xaction.Registry.RenewECEncodeXact(t, c.bck, c.uuid, cmn.ActCommit)
			if err != nil {
				return err
			}

			c.addNotif(xact) // ditto
			go xact.Run()
		}
	default:
		cmn.Assert(false)
	}
	return nil
}

func (t *targetrunner) validateNprops(bck *cluster.Bck, msg *aisMsg) (nprops *cmn.BucketProps, err error) {
	var (
		body = cmn.MustMarshal(msg.Value)
		cs   = fs.GetCapStatus()
	)
	nprops = &cmn.BucketProps{}
	if err = jsoniter.Unmarshal(body, nprops); err != nil {
		return
	}
	if nprops.Mirror.Enabled {
		mpathCount := fs.NumAvail()
		if int(nprops.Mirror.Copies) > mpathCount {
			err = fmt.Errorf("%s: number of mountpaths %d is insufficient to configure %s as a %d-way mirror",
				t.si, mpathCount, bck, nprops.Mirror.Copies)
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

func remirror(bprops, nprops *cmn.BucketProps) bool {
	if !bprops.Mirror.Enabled && nprops.Mirror.Enabled {
		return true
	}
	if bprops.Mirror.Enabled && nprops.Mirror.Enabled {
		return bprops.Mirror.Copies != nprops.Mirror.Copies
	}
	return false
}

func reECEncode(bprops, nprops *cmn.BucketProps, bck *cluster.Bck) bool {
	// TODO: xaction to remove all data generated by EC encoder.
	// For now, do nothing if EC is disabled.
	if !nprops.EC.Enabled {
		if bprops.EC.Enabled {
			// kill running ec-encode xact if it is active
			xaction.Registry.DoAbort(cmn.ActECEncode, bck)
		}
		return false
	}
	if !bprops.EC.Enabled {
		return true
	}
	return bprops.EC.DataSlices != nprops.EC.DataSlices ||
		bprops.EC.ParitySlices != nprops.EC.ParitySlices
}

//////////////////
// renameBucket //
//////////////////

func (t *targetrunner) renameBucket(c *txnServerCtx) error {
	if err := c.bck.Init(t.owner.bmd, t.si); err != nil {
		return err
	}
	switch c.phase {
	case cmn.ActBegin:
		var (
			bckTo   *cluster.Bck
			bckFrom = c.bck
			err     error
		)
		if bckTo, err = t.validateBckRenTxn(bckFrom, c.msg); err != nil {
			return err
		}
		nlpFrom := bckFrom.GetNameLockPair()
		nlpTo := bckTo.GetNameLockPair()
		if !nlpFrom.TryLock() {
			return cmn.NewErrorBucketIsBusy(bckFrom.Bck, t.si.Name())
		}
		if !nlpTo.TryLock() {
			nlpFrom.Unlock()
			return cmn.NewErrorBucketIsBusy(bckTo.Bck, t.si.Name())
		}
		txn := newTxnRenameBucket(c, bckFrom, bckTo)
		if err := t.transactions.begin(txn); err != nil {
			nlpTo.Unlock()
			nlpFrom.Unlock()
			return err
		}
		txn.nlps = []cmn.NLP{nlpFrom, nlpTo}
	case cmn.ActAbort:
		t.transactions.find(c.uuid, true /* remove */)
	case cmn.ActCommit:
		var xact *xaction.FastRen
		txn, err := t.transactions.find(c.uuid, false)
		if err != nil {
			return fmt.Errorf("%s %s: %v", t.si, txn, err)
		}
		txnRenB := txn.(*txnRenameBucket)
		// wait for newBMD w/timeout
		if err = t.transactions.wait(txn, c.timeout); err != nil {
			return fmt.Errorf("%s %s: %v", t.si, txn, err)
		}
		xact, err = xaction.Registry.RenewBckFastRename(t, c.uuid, c.msg.RMDVersion,
			txnRenB.bckFrom, txnRenB.bckTo, cmn.ActCommit)
		if err != nil {
			return err // must not happen at commit time
		}

		err = fs.RenameBucketDirs(txnRenB.bckFrom.Bck, txnRenB.bckTo.Bck)
		if err != nil {
			return err // ditto
		}

		c.addNotif(xact) // notify upon completion

		t.gfn.local.Activate()
		t.gfn.global.activateTimed()
		go xact.Run()
	default:
		cmn.Assert(false)
	}
	return nil
}

func (t *targetrunner) validateBckRenTxn(bckFrom *cluster.Bck, msg *aisMsg) (bckTo *cluster.Bck, err error) {
	var (
		bTo               = &cmn.Bck{}
		body              = cmn.MustMarshal(msg.Value)
		availablePaths, _ = fs.Get()
	)
	if err = jsoniter.Unmarshal(body, bTo); err != nil {
		return
	}
	if cs := fs.GetCapStatus(); cs.Err != nil {
		return nil, cs.Err
	}
	if err = t.coExists(bckFrom, msg); err != nil {
		return
	}
	bckTo = cluster.NewBck(bTo.Name, bTo.Provider, bTo.Ns)
	bmd := t.owner.bmd.get()
	if _, present := bmd.Get(bckFrom); !present {
		return bckTo, cmn.NewErrorBucketDoesNotExist(bckFrom.Bck, t.si.String())
	}
	if _, present := bmd.Get(bckTo); present {
		return bckTo, cmn.NewErrorBucketAlreadyExists(bckTo.Bck, t.si.String())
	}
	for _, mpathInfo := range availablePaths {
		path := mpathInfo.MakePathCT(bckTo.Bck, fs.ObjectType)
		if err := fs.Access(path); err != nil {
			if !os.IsNotExist(err) {
				return bckTo, err
			}
			continue
		}
		if names, empty, err := fs.IsDirEmpty(path); err != nil {
			return bckTo, err
		} else if !empty {
			return bckTo, fmt.Errorf("directory %q already exists and is not empty (%v...)", path, names)
		}
	}
	return
}

////////////////
// copyBucket //
////////////////

func (t *targetrunner) copyBucket(c *txnServerCtx) error {
	if err := c.bck.Init(t.owner.bmd, t.si); err != nil {
		return err
	}
	switch c.phase {
	case cmn.ActBegin:
		var (
			bckTo   *cluster.Bck
			bckFrom = c.bck
			err     error
		)
		// TODO -- FIXME: mountpath validation when destination does not exist
		if bckTo, err = t.validateBckCpTxn(bckFrom, c.msg); err != nil {
			return err
		}
		nlpFrom := bckFrom.GetNameLockPair()
		nlpTo := bckTo.GetNameLockPair()
		if !nlpFrom.TryRLock() {
			return cmn.NewErrorBucketIsBusy(bckFrom.Bck, t.si.Name())
		}
		if !nlpTo.TryLock() {
			nlpFrom.Unlock()
			return cmn.NewErrorBucketIsBusy(bckTo.Bck, t.si.Name())
		}
		txn := newTxnCopyBucket(c, bckFrom, bckTo)
		if err := t.transactions.begin(txn); err != nil {
			nlpTo.Unlock()
			nlpFrom.Unlock()
			return err
		}
		txn.nlps = []cmn.NLP{nlpFrom, nlpTo}
	case cmn.ActAbort:
		t.transactions.find(c.uuid, true /* remove */)
	case cmn.ActCommit:
		var xact *mirror.XactBckCopy
		txn, err := t.transactions.find(c.uuid, false)
		if err != nil {
			return fmt.Errorf("%s %s: %v", t.si, txn, err)
		}
		txnCpBck := txn.(*txnCopyBucket)
		if c.query.Get(cmn.URLParamWaitMetasync) != "" {
			if err = t.transactions.wait(txn, c.timeout); err != nil {
				return fmt.Errorf("%s %s: %v", t.si, txn, err)
			}
		} else {
			t.transactions.find(c.uuid, true /* remove */)
		}
		xact, err = xaction.Registry.RenewBckCopy(t, txnCpBck.bckFrom, txnCpBck.bckTo, c.uuid, cmn.ActCommit)
		if err != nil {
			return err
		}

		c.addNotif(xact) // notify upon completion
		go xact.Run()
	default:
		cmn.Assert(false)
	}
	return nil
}

func (t *targetrunner) validateBckCpTxn(bckFrom *cluster.Bck, msg *aisMsg) (bckTo *cluster.Bck, err error) {
	var (
		bTo  = cmn.Bck{}
		body = cmn.MustMarshal(msg.Value)
	)
	if err = jsoniter.Unmarshal(body, &bTo); err != nil {
		return
	}
	if cs := fs.GetCapStatus(); cs.Err != nil {
		return nil, cs.Err
	}
	if err = t.coExists(bckFrom, msg); err != nil {
		return
	}
	bckTo = cluster.NewBckEmbed(bTo)
	bmd := t.owner.bmd.get()
	if _, present := bmd.Get(bckFrom); !present {
		return bckTo, cmn.NewErrorBucketDoesNotExist(bckFrom.Bck, t.si.String())
	}
	return
}

//////////////
// ecEncode //
//////////////

func (t *targetrunner) ecEncode(c *txnServerCtx) error {
	if err := c.bck.Init(t.owner.bmd, t.si); err != nil {
		return err
	}
	switch c.phase {
	case cmn.ActBegin:
		if err := t.validateEcEncode(c.bck, c.msg); err != nil {
			return err
		}
		nlp := c.bck.GetNameLockPair()
		if !nlp.TryLock() {
			return cmn.NewErrorBucketIsBusy(c.bck.Bck, t.si.Name())
		}
		nlp.Unlock() // TODO -- FIXME: introduce txn, unlock when done
		if _, err := xaction.Registry.RenewECEncodeXact(t, c.bck, c.uuid, cmn.ActBegin); err != nil {
			return err
		}
	case cmn.ActAbort:
		// do nothing
	case cmn.ActCommit:
		xact, err := xaction.Registry.RenewECEncodeXact(t, c.bck, c.uuid, cmn.ActCommit)
		if err != nil {
			glog.Error(err)
			return err
		}
		go xact.Run()

	default:
		cmn.Assert(false)
	}
	return nil
}

func (t *targetrunner) validateEcEncode(bck *cluster.Bck, msg *aisMsg) (err error) {
	if cs := fs.GetCapStatus(); cs.Err != nil {
		return cs.Err
	}
	err = t.coExists(bck, msg)
	return
}

//////////
// misc //
//////////

func (t *targetrunner) prepTxnServer(r *http.Request, msg *aisMsg, apiItems []string) (*txnServerCtx, error) {
	var (
		bucket string
		err    error
		query  = r.URL.Query()
		c      = &txnServerCtx{}
	)
	c.msg = msg
	c.callerName = r.Header.Get(cmn.HeaderCallerName)
	c.callerID = r.Header.Get(cmn.HeaderCallerID)
	bucket, c.phase = apiItems[0], apiItems[1]
	if c.bck, err = newBckFromQuery(bucket, query); err != nil {
		return c, err
	}
	c.uuid = c.msg.UUID
	if c.uuid == "" {
		return c, nil
	}
	c.timeout, err = cmn.S2Duration(query.Get(cmn.URLParamTxnTimeout))
	c.query = query // operation-specific values, if any

	c.smapVer = t.owner.smap.get().version()
	c.bmdVer = t.owner.bmd.get().version()

	c.t = t
	return c, err
}

// TODO: #791 "limited coexistence" - extend and unify
func (t *targetrunner) coExists(bck *cluster.Bck, msg *aisMsg) (err error) {
	g, l := xaction.GetRebMarked(), xaction.GetResilverMarked()
	if g.Xact != nil {
		err = fmt.Errorf("%s: %s, cannot run %q on bucket %s", t.si, g.Xact, msg.Action, bck)
	} else if l.Xact != nil {
		err = fmt.Errorf("%s: %s, cannot run %q on bucket %s", t.si, l.Xact, msg.Action, bck)
	}
	return
}

//
// notifications
//

func (c *txnServerCtx) addNotif(xact cmn.Xact) {
	d := c.query[cmn.URLParamNotifyMe]
	if len(d) == 0 {
		return
	}
	// validate
	smap := c.t.owner.smap.get()
re:
	for i, pid := range d {
		if !smap.containsID(pid) {
			glog.Errorf("%s: unknown notification listener %s: %s, %s", c.t.si, pid, smap, xact)
			if i < len(d)-1 {
				copy(d[i:], d[i+1:])
			}
			d = d[:len(d)-1]
			goto re
		}
	}
	if len(d) == 0 {
		glog.Errorf("%s: no notification listeners: %s, %s", c.t.si, smap, xact)
		return
	}
	xact.AddNotif(&cmn.NotifXact{
		NotifBase: cmn.NotifBase{When: cmn.UponTerm, Dsts: d, F: c.t.xactCallerNotify},
	})
}
