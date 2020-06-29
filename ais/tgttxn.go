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
		txn := newTxnMakeNCopies(c, curCopies, newCopies)
		if err := t.transactions.begin(txn); err != nil {
			return err
		}
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
		capInfo := t.AvgCapUsed(nil)
		err = capInfo.Err
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
		txn := newTxnSetBucketProps(c, nprops)
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
		body    = cmn.MustMarshal(msg.Value)
		capInfo = t.AvgCapUsed(cmn.GCO.Get())
	)
	nprops = &cmn.BucketProps{}
	if err = jsoniter.Unmarshal(body, nprops); err != nil {
		return
	}
	if nprops.Mirror.Enabled {
		mpathCount := fs.Mountpaths.NumAvail()
		if int(nprops.Mirror.Copies) > mpathCount {
			err = fmt.Errorf("%s: number of mountpaths %d is insufficient to configure %s as a %d-way mirror",
				t.si, mpathCount, bck, nprops.Mirror.Copies)
			return
		}
		if nprops.Mirror.Copies > bck.Props.Mirror.Copies && capInfo.Err != nil {
			return nprops, capInfo.Err
		}
	}
	if nprops.EC.Enabled && !bck.Props.EC.Enabled {
		err = capInfo.Err
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
		txn := newTxnRenameBucket(c, bckFrom, bckTo)
		if err := t.transactions.begin(txn); err != nil {
			return err
		}
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
		xact, err = xaction.Registry.RenewBckFastRename(t, c.msg.RMDVersion,
			txnRenB.bckFrom, txnRenB.bckTo, cmn.ActCommit, t.rebManager)
		if err != nil {
			return err // must not happen at commit time
		}

		err = fs.Mountpaths.RenameBucketDirs(txnRenB.bckFrom.Bck, txnRenB.bckTo.Bck)
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
		config            = cmn.GCO.Get()
		availablePaths, _ = fs.Mountpaths.Get()
	)
	if err = jsoniter.Unmarshal(body, bTo); err != nil {
		return
	}
	if capInfo := t.AvgCapUsed(config); capInfo.Err != nil {
		return nil, capInfo.Err
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
		txn := newTxnCopyBucket(c, bckFrom, bckTo)
		if err := t.transactions.begin(txn); err != nil {
			return err
		}
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
		bTo    = cmn.Bck{}
		body   = cmn.MustMarshal(msg.Value)
		config = cmn.GCO.Get()
	)
	if err = jsoniter.Unmarshal(body, &bTo); err != nil {
		return
	}
	if capInfo := t.AvgCapUsed(config); capInfo.Err != nil {
		return nil, capInfo.Err
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
		err := t.validateEcEncode(c.bck, c.msg)
		if err == nil {
			_, err = xaction.Registry.RenewECEncodeXact(t, c.bck, c.uuid, cmn.ActBegin)
		}
		if err != nil {
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
	if capInfo := t.AvgCapUsed(cmn.GCO.Get()); capInfo.Err != nil {
		return capInfo.Err
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
	if rebInfo := t.RebalanceInfo(); rebInfo.IsRebalancing {
		err = fmt.Errorf("%s: rebalancing(%d) is in progress, cannot run %q on bucket %s",
			t.si, rebInfo.RebID, msg.Action, bck)
	}
	return
}

//
// notifications
//

func (c *txnServerCtx) addNotif(xact cmn.Xact) {
	dsts := c.query[cmn.URLParamNotifyMe]
	if len(dsts) == 0 {
		return
	}
	// validate unless the dest is the caller
	cmn.Assert(dsts[0] != "")
	if dsts[0] != c.callerID {
		smap := c.t.owner.smap.get()
		if !smap.containsID(dsts[0]) {
			glog.Errorf("%s: unknown notification dst %s, %s", c.t.si, dsts[0], smap) // TODO: handle
		}
	}
	xact.AddNotif(&cmn.NotifXact{
		NotifBase: cmn.NotifBase{When: cmn.UponTerm, Dsts: dsts, F: c.xactCallerNotify},
	})
}

func (c *txnServerCtx) xactCallerNotify(n cmn.Notif, err error) {
	var (
		msg   = notifMsg{Ty: notifXact, Snode: c.t.si, Err: err}
		notif = n.(*cmn.NotifXact)
		pid   = notif.Dsts[0]
	)
	msg.Data = cmn.MustMarshal(notif.Xact.Stats())
	c.t.notify(pid, cmn.MustMarshal(&msg))
}
