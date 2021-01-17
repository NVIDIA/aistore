// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/etl"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/mirror"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
	jsoniter "github.com/json-iterator/go"
)

const (
	recvObjTrname = "recvobjs"
)

// convenience structure to gather all (or most) of the relevant context in one place
// (compare with txnClientCtx & prepTxnClient)
type txnServerCtx struct {
	uuid    string
	timeout struct {
		netw time.Duration
		host time.Duration
	}
	phase      string
	smapVer    int64
	bmdVer     int64
	msg        *aisMsg
	callerName string
	callerID   string
	bck        *cluster.Bck // aka bckFrom
	bckTo      *cluster.Bck
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
	apiItems, err := t.checkRESTItems(w, r, 0, true, cmn.URLPathTxn.L)
	if err != nil {
		return
	}
	var bucket, phase string
	switch len(apiItems) {
	case 1:
		// Global transaction
		phase = apiItems[0]
	case 2:
		// Bucket-based transaction
		bucket, phase = apiItems[0], apiItems[1]
	default:
		cmn.InvalidHandlerWithMsg(w, r, "invalid /txn path")
		return
	}
	// 2. gather all context
	c, err := t.prepTxnServer(r, msg, bucket, phase)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	// 3. do
	switch msg.Action {
	case cmn.ActCreateBck, cmn.ActRegisterCB:
		debug.Infof("Starting transaction create-bucket (ts: %d, phase: %s, uuid: %s)", mono.NanoTime(), c.phase, c.uuid)
		if err = t.createBucket(c); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
		debug.Infof("Finished transaction create-bucket (ts: %d, phase: %s, uuid: %s)", mono.NanoTime(), c.phase, c.uuid)
	case cmn.ActMakeNCopies:
		if err = t.makeNCopies(c); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
	case cmn.ActSetBprops, cmn.ActResetBprops:
		if err = t.setBucketProps(c); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
	case cmn.ActMoveBck:
		if err = t.renameBucket(c); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
	case cmn.ActCopyBck, cmn.ActETLBck:
		bck2BckMsg := &cmn.Bck2BckMsg{}
		if err = cmn.MorphMarshal(c.msg.Value, bck2BckMsg); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
		if msg.Action == cmn.ActCopyBck {
			err = t.transferBucket(c, bck2BckMsg, nil /*LomReaderProvider*/)
		} else {
			err = t.etlBucket(c, bck2BckMsg) // calls t.transferBucket
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
	case cmn.ActECEncode:
		if err = t.ecEncode(c); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
	case cmn.ActStartMaintenance, cmn.ActDecommission:
		if err = t.startMaintenance(c); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
	case cmn.ActDestroyBck, cmn.ActEvictCB:
		debug.Infof("Starting transaction destroy-bucket (ts: %d, phase: %s, uuid: %s)", mono.NanoTime(), c.phase, c.uuid)
		if err = t.destroyBucket(c); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
		debug.Infof("Finished transaction destroy-bucket (ts: %d, phase: %s, uuid: %s)", mono.NanoTime(), c.phase, c.uuid)
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
		if c.msg.Action == cmn.ActCreateBck && c.bck.IsRemote() {
			if c.msg.Value != nil {
				if err := cmn.MorphMarshal(c.msg.Value, &c.bck.Props); err != nil {
					return err
				}
			}
			if _, err := t.Cloud(c.bck).CreateBucket(context.Background(), c.bck); err != nil {
				return err
			}
		}
	case cmn.ActAbort:
		t.transactions.find(c.uuid, cmn.ActAbort)
	case cmn.ActCommit:
		t._commitCreateDestroy(c)
	default:
		cmn.Assert(false)
	}
	return nil
}

func (t *targetrunner) _commitCreateDestroy(c *txnServerCtx) (err error) {
	txn, err := t.transactions.find(c.uuid, "")
	if err != nil {
		return fmt.Errorf("%s %s: %v", t.si, txn, err)
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
		if !nlp.TryLock(c.timeout.netw / 2) {
			return cmn.NewErrorBucketIsBusy(c.bck.Bck, t.si.Name())
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
		copies, _ := t.parseNCopies(c.msg.Value)
		txn, err := t.transactions.find(c.uuid, "")
		if err != nil {
			return fmt.Errorf("%s %s: %v", t.si, txn, err)
		}
		txnMnc := txn.(*txnMakeNCopies)
		cmn.Assert(txnMnc.newCopies == copies)

		// wait for newBMD w/timeout
		if err = t.transactions.wait(txn, c.timeout.netw, c.timeout.host); err != nil {
			return fmt.Errorf("%s %s: %v", t.si, txn, err)
		}

		// do the work in xaction
		xact, err := xreg.RenewBckMakeNCopies(t, c.bck, c.uuid, int(copies))
		if err != nil {
			return fmt.Errorf("%s %s: %v", t.si, txn, err)
		}

		xreg.DoAbort(cmn.ActPutCopies, c.bck)

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
		err = t.coExists(bck, msg.Action)
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
		if !nlp.TryLock(c.timeout.netw / 2) {
			return cmn.NewErrorBucketIsBusy(c.bck.Bck, t.si.Name())
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
			return fmt.Errorf("%s %s: %v", t.si, txn, err)
		}
		txnSetBprops := txn.(*txnSetBucketProps)
		// wait for newBMD w/timeout
		if err = t.transactions.wait(txn, c.timeout.netw, c.timeout.host); err != nil {
			return fmt.Errorf("%s %s: %v", t.si, txn, err)
		}
		if reMirror(txnSetBprops.bprops, txnSetBprops.nprops) {
			n := int(txnSetBprops.nprops.Mirror.Copies)
			xact, err := xreg.RenewBckMakeNCopies(t, c.bck, c.uuid, n)
			if err != nil {
				return fmt.Errorf("%s %s: %v", t.si, txn, err)
			}
			xreg.DoAbort(cmn.ActPutCopies, c.bck)

			c.addNotif(xact) // notify upon completion
			go xact.Run()
		}
		if reEC(txnSetBprops.bprops, txnSetBprops.nprops, c.bck) {
			xreg.DoAbort(cmn.ActECEncode, c.bck)
			xact, err := xreg.RenewECEncode(t, c.bck, c.uuid, cmn.ActCommit)
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

//////////////////
// renameBucket //
//////////////////

func (t *targetrunner) renameBucket(c *txnServerCtx) error {
	if err := c.bck.Init(t.owner.bmd, t.si); err != nil {
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
			return cmn.NewErrorBucketIsBusy(bckFrom.Bck, t.si.Name())
		}
		if !nlpTo.TryLock(c.timeout.netw / 4) {
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
		t.transactions.find(c.uuid, cmn.ActAbort)
	case cmn.ActCommit:
		txn, err := t.transactions.find(c.uuid, "")
		if err != nil {
			return fmt.Errorf("%s %s: %v", t.si, txn, err)
		}
		txnRenB := txn.(*txnRenameBucket)
		// wait for newBMD w/timeout
		if err = t.transactions.wait(txn, c.timeout.netw, c.timeout.host); err != nil {
			return fmt.Errorf("%s %s: %v", t.si, txn, err)
		}
		xact, err := xreg.RenewBckRename(t, txnRenB.bckFrom, txnRenB.bckTo, c.uuid, c.msg.RMDVersion, cmn.ActCommit)
		if err != nil {
			return err // must not happen at commit time
		}

		err = fs.RenameBucketDirs(txnRenB.bckFrom.Props.BID, txnRenB.bckFrom.Bck, txnRenB.bckTo.Bck)
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

func (t *targetrunner) validateBckRenTxn(bckFrom, bckTo *cluster.Bck, msg *aisMsg) error {
	availablePaths, _ := fs.Get()
	if cs := fs.GetCapStatus(); cs.Err != nil {
		return cs.Err
	}
	if err := t.coExists(bckFrom, msg.Action); err != nil {
		return err
	}
	bmd := t.owner.bmd.get()
	if _, present := bmd.Get(bckFrom); !present {
		return cmn.NewErrorBucketDoesNotExist(bckFrom.Bck, t.si.String())
	}
	if _, present := bmd.Get(bckTo); present {
		return cmn.NewErrorBucketAlreadyExists(bckTo.Bck, t.si.String())
	}
	for _, mpathInfo := range availablePaths {
		path := mpathInfo.MakePathCT(bckTo.Bck, fs.ObjectType)
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

////////////////////
// transferBucket //
////////////////////

func (t *targetrunner) transferBucket(c *txnServerCtx, bck2BckMsg *cmn.Bck2BckMsg, dp cluster.LomReaderProvider) error {
	if err := c.bck.Init(t.owner.bmd, t.si); err != nil {
		return err
	}
	switch c.phase {
	case cmn.ActBegin:
		var (
			bckTo   = c.bckTo
			bckFrom = c.bck
			dm      *bundle.DataMover
			config  = cmn.GCO.Get()
			err     error
			sizePDU int32

			nlpTo, nlpFrom *cluster.NameLockPair
		)
		if err := bckTo.Validate(); err != nil {
			return err
		}
		if err := bckFrom.Validate(); err != nil {
			return err
		}
		if err := t.validateTransferBckTxn(bckFrom, c.msg.Action); err != nil {
			return err
		}
		if c.msg.Action == cmn.ActETLBck {
			sizePDU = memsys.DefaultBufSize
		}
		// Reuse rebalance configuration to create a DM for copying objects.
		// TODO: implement separate configuration for Copy/ETL MD.
		if dm, err = c.newDM(&config.Rebalance, c.uuid, sizePDU); err != nil {
			return err
		}

		nlpFrom = bckFrom.GetNameLockPair()
		if !nlpFrom.TryRLock(c.timeout.netw / 4) {
			dm.UnregRecv()
			return cmn.NewErrorBucketIsBusy(bckFrom.Bck, t.si.Name())
		}

		if !bck2BckMsg.DryRun {
			nlpTo = bckTo.GetNameLockPair()
			if !nlpTo.TryLock(c.timeout.netw / 4) {
				dm.UnregRecv()
				nlpFrom.Unlock()
				return cmn.NewErrorBucketIsBusy(bckTo.Bck, t.si.Name())
			}
		}

		txn := newTxnTransferBucket(c, bckFrom, bckTo, dm, dp, bck2BckMsg)
		if err := t.transactions.begin(txn); err != nil {
			dm.UnregRecv()
			if nlpTo != nil {
				nlpTo.Unlock()
			}
			nlpFrom.Unlock()
			return err
		}
		txn.nlps = []cmn.NLP{nlpFrom}
		if nlpTo != nil {
			txn.nlps = append(txn.nlps, nlpTo)
		}
	case cmn.ActAbort:
		t.transactions.find(c.uuid, cmn.ActAbort)
	case cmn.ActCommit:
		txn, err := t.transactions.find(c.uuid, "")
		if err != nil {
			return fmt.Errorf("%s %s: %v", t.si, txn, err)
		}
		txnCp := txn.(*txnTransferBucket)
		if c.query.Get(cmn.URLParamWaitMetasync) != "" {
			if err = t.transactions.wait(txn, c.timeout.netw, c.timeout.host); err != nil {
				return fmt.Errorf("%s %s: %v", t.si, txn, err)
			}
		} else {
			t.transactions.find(c.uuid, cmn.ActCommit)
		}
		xact, err := xreg.RenewTransferBck(t, txnCp.bckFrom, txnCp.bckTo, c.uuid, c.msg.Action, cmn.ActCommit,
			txnCp.dm, txnCp.dp, txnCp.metaMsg)
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

func (t *targetrunner) validateTransferBckTxn(bckFrom *cluster.Bck, action string) (err error) {
	if cs := fs.GetCapStatus(); cs.Err != nil {
		return cs.Err
	}
	if err = t.coExists(bckFrom, action); err != nil {
		return
	}
	bmd := t.owner.bmd.get()
	if _, present := bmd.Get(bckFrom); !present {
		return cmn.NewErrorBucketDoesNotExist(bckFrom.Bck, t.si.String())
	}
	return nil
}

///////////////
// etlBucket //
///////////////

// etlBucket uses transferBucket xaction to transform the whole bucket. The only difference is that instead of copying the
// same bytes, it creates a reader based on given ETL transformation.
func (t *targetrunner) etlBucket(c *txnServerCtx, msg *cmn.Bck2BckMsg) (err error) {
	if err := k8s.Detect(); err != nil {
		return err
	}
	if msg.ID == "" {
		return etl.ErrMissingUUID
	}
	var dp cluster.LomReaderProvider

	if dp, err = etl.NewOfflineDataProvider(msg); err != nil {
		return nil
	}

	return t.transferBucket(c, msg, dp)
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
		if err := t.validateECEncode(c.bck, c.msg); err != nil {
			return err
		}
		nlp := c.bck.GetNameLockPair()
		if !nlp.TryLock(c.timeout.netw / 4) {
			return cmn.NewErrorBucketIsBusy(c.bck.Bck, t.si.Name())
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
			return fmt.Errorf("%s %s: %v", t.si, txn, err)
		}
		// wait for newBMD w/timeout
		if err = t.transactions.wait(txn, c.timeout.netw, c.timeout.host); err != nil {
			return fmt.Errorf("%s %s: %v", t.si, txn, err)
		}

		xact, err := xreg.RenewECEncode(t, c.bck, c.uuid, cmn.ActCommit)
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

func (t *targetrunner) validateECEncode(bck *cluster.Bck, msg *aisMsg) (err error) {
	if cs := fs.GetCapStatus(); cs.Err != nil {
		return cs.Err
	}
	err = t.coExists(bck, msg.Action)
	return
}

//////////////////////
// startMaintenance //
//////////////////////

func (t *targetrunner) startMaintenance(c *txnServerCtx) error {
	switch c.phase {
	case cmn.ActBegin:
		var opts cmn.ActValDecommision
		if err := cmn.MorphMarshal(c.msg.Value, &opts); err != nil {
			return err
		}
		if !opts.Force {
			g := xreg.GetRebMarked()
			if g.Xact != nil && !g.Xact.Finished() {
				return errors.New("cannot start maintenance: rebalance is in progress")
			}

			if cause := xreg.CheckBucketsBusy(); cause != nil {
				return fmt.Errorf("cannot start maintenance: (xaction: %q) is in progress", cause.Get())
			}
		}
		t.gfn.global.activateTimed()
	case cmn.ActAbort:
		t.gfn.global.abortTimed()
	case cmn.ActCommit:
		var opts cmn.ActValDecommision
		if err := cmn.MorphMarshal(c.msg.Value, &opts); err != nil {
			return err
		}
		if c.msg.Action == cmn.ActDecommission && opts.DaemonID == t.si.ID() {
			fs.ClearMDOnAllMpaths()
			fs.RemoveDaemonIDs()
		}
	default:
		cmn.Assert(false)
	}
	return nil
}

//////////////////////////////////////////////
// destroy local bucket / evict cloud buket //
//////////////////////////////////////////////

func (t *targetrunner) destroyBucket(c *txnServerCtx) error {
	switch c.phase {
	case cmn.ActBegin:
		nlp := c.bck.GetNameLockPair()
		if !nlp.TryLock(c.timeout.netw / 2) {
			return cmn.NewErrorBucketIsBusy(c.bck.Bck, t.si.Name())
		}
		txn := newTxnBckBase("dlb", *c.bck)
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
		cmn.Assert(false)
	}
	return nil
}

//////////
// misc //
//////////

func (t *targetrunner) prepTxnServer(r *http.Request, msg *aisMsg, bucket, phase string) (*txnServerCtx, error) {
	var (
		err   error
		query = r.URL.Query()
		c     = &txnServerCtx{}
	)
	c.msg = msg
	c.callerName = r.Header.Get(cmn.HeaderCallerName)
	c.callerID = r.Header.Get(cmn.HeaderCallerID)
	c.phase = phase

	if bucket != "" {
		if c.bck, err = newBckFromQuery(bucket, query); err != nil {
			return c, err
		}
	}
	c.bckTo, err = newBckFromQueryUname(query, cmn.URLParamBucketTo)
	if err != nil {
		return c, err
	}

	// latency = (network) +- (clock drift)
	if phase == cmn.ActBegin || phase == cmn.ActCommit {
		if ptime := query.Get(cmn.URLParamUnixTime); ptime != "" {
			if delta := requestLatency(time.Now(), ptime); delta != 0 {
				bound := cmn.GCO.Get().Timeout.CplaneOperation / 2
				if delta > int64(bound) || delta < -int64(bound) {
					glog.Errorf("%s: txn %s[%s] latency=%v(!), caller %s, phase=%s, bucket %q",
						t.si, msg.Action, c.msg.UUID, time.Duration(delta), c.callerName, phase, bucket)
				}
			}
		}
	}

	c.uuid = c.msg.UUID
	if c.uuid == "" {
		return c, nil
	}
	if tout := query.Get(cmn.URLParamNetwTimeout); tout != "" {
		c.timeout.netw, err = cmn.S2Duration(tout)
		debug.AssertNoErr(err)
	}
	if tout := query.Get(cmn.URLParamHostTimeout); tout != "" {
		c.timeout.host, err = cmn.S2Duration(tout)
		debug.AssertNoErr(err)
	}
	c.query = query // operation-specific values, if any

	c.smapVer = t.owner.smap.get().version()
	c.bmdVer = t.owner.bmd.get().version()

	c.t = t
	return c, err
}

// TODO: #791 "limited coexistence" - extend and unify
func (t *targetrunner) coExists(bck *cluster.Bck, action string) (err error) {
	const fmterr = "%s: [%s] is currently running, cannot run %q (bucket %s) concurrently"
	g, l := xreg.GetRebMarked(), xreg.GetResilverMarked()
	if g.Xact != nil {
		err = fmt.Errorf(fmterr, t.si, g.Xact, action, bck)
	} else if l.Xact != nil {
		err = fmt.Errorf(fmterr, t.si, l.Xact, action, bck)
	}
	if ren := xreg.GetXactRunning(cmn.ActMoveBck); ren != nil {
		err = fmt.Errorf(fmterr, t.si, ren, action, bck)
	}
	return
}

//
// notifications
//

func (c *txnServerCtx) addNotif(xact cluster.Xact) {
	dsts, ok := c.query[cmn.URLParamNotifyMe]
	if !ok {
		return
	}
	xact.AddNotif(&xaction.NotifXact{
		NotifBase: nl.NotifBase{When: cluster.UponTerm, Dsts: dsts, F: c.t.callerNotifyFin},
		Xact:      xact,
	})
}

func (c *txnServerCtx) recvObjDM(w http.ResponseWriter, hdr transport.ObjHdr, objReader io.Reader, err error) {
	defer transport.FreeRecv(objReader)
	if err != nil && !cmn.IsEOF(err) {
		glog.Error(err)
		return
	}
	defer cmn.DrainReader(objReader)
	lom := &cluster.LOM{ObjName: hdr.ObjName}
	if err := lom.Init(hdr.Bck); err != nil {
		glog.Error(err)
		return
	}
	lom.SetAtimeUnix(hdr.ObjAttrs.Atime)
	lom.SetVersion(hdr.ObjAttrs.Version)

	params := cluster.PutObjectParams{
		Tag:    fs.WorkfilePut,
		Reader: ioutil.NopCloser(objReader),
		// Transaction is used only by CopyBucket and ETL. In both cases new objects
		// are created at the destination. Setting `RegularPut` type informs `c.t.PutObject`
		// that it must PUT the object to the Cloud as well after the local data are
		// finalized in case of destination is Cloud.
		RecvType: cluster.RegularPut,
		Cksum:    cmn.NewCksum(hdr.ObjAttrs.CksumType, hdr.ObjAttrs.CksumValue),
		Started:  time.Now(),
	}
	if err := c.t.PutObject(lom, params); err != nil {
		glog.Error(err)
	}
}

func (c *txnServerCtx) newDM(rebcfg *cmn.RebalanceConf, uuid string, sizePDU ...int32) (*bundle.DataMover, error) {
	dmExtra := bundle.Extra{
		RecvAck:     nil,                    // NOTE: no ACKs
		Compression: rebcfg.Compression,     // TODO: define separately
		Multiplier:  int(rebcfg.Multiplier), // ditto
	}
	if len(sizePDU) > 0 {
		dmExtra.SizePDU = sizePDU[0]
	}
	dm, err := bundle.NewDataMover(c.t, recvObjTrname+"_"+uuid, c.recvObjDM, cluster.RegularPut, dmExtra)
	if err != nil {
		return nil, err
	}
	if err := dm.RegRecv(); err != nil {
		return nil, err
	}
	return dm, nil
}
