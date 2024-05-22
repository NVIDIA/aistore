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
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/ais/backend"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/reb"
	"github.com/NVIDIA/aistore/res"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
	jsoniter "github.com/json-iterator/go"
)

const (
	bmdFixup = "fixup"
	bmdRecv  = "receive"
	bmdReg   = "register"
)

type delb struct {
	obck    *meta.Bck
	present bool
}

func (t *target) joinCluster(action string, primaryURLs ...string) (status int, err error) {
	res := t.join(nil, t, primaryURLs...)
	defer freeCR(res)
	if res.err != nil {
		status, err = res.status, res.err
		return
	}
	// not being sent at cluster startup and keepalive
	if len(res.bytes) == 0 {
		return
	}
	err = t.recvCluMetaBytes(action, res.bytes, "")
	return
}

const tagCM = "recv-clumeta"

// TODO: unify w/ p.recvCluMeta
// do not receive RMD: `receiveRMD` runs extra jobs and checks specific for metasync.
func (t *target) recvCluMetaBytes(action string, body []byte, caller string) error {
	var (
		cm   cluMeta
		errs []error
		self = t.String() + ":"
	)
	if err := jsoniter.Unmarshal(body, &cm); err != nil {
		return fmt.Errorf(cmn.FmtErrUnmarshal, t, tagCM, cos.BHead(body), err)
	}
	if cm.PrimeTime == 0 {
		err := errors.New(self + " zero prime_time (non-primary responded to an attempt to join?")
		nlog.Errorln(err)
		return err
	}

	xreg.PrimeTime.Store(cm.PrimeTime)
	xreg.MyTime.Store(time.Now().UnixNano())

	msg := t.newAmsgStr(action, cm.BMD)

	// Config
	if cm.Config == nil {
		err := fmt.Errorf(self+" invalid %T (nil config): %+v", cm, cm)
		nlog.Errorln(err)
		return err
	}
	if err := t.receiveConfig(cm.Config, msg, nil, caller); err != nil {
		if !isErrDowngrade(err) {
			errs = append(errs, err)
			nlog.Errorln(err)
		}
	} else {
		nlog.Infoln(self, tagCM, action, cm.Config.String())
	}

	// There's a window of time between:
	// a) target joining existing cluster and b) cluster starting to rebalance itself
	// The latter is driven by regMetasync (see regMetasync.go) distributing updated cluster map.
	// To handle incoming GETs within this window (which would typically take a few seconds or less)
	// we need to have the current cluster-wide regMetadata and the temporary gfn state:
	reb.OnTimedGFN()

	// BMD
	if err := t.receiveBMD(cm.BMD, msg, nil /*ms payload */, bmdReg, caller, true /*silent*/); err != nil {
		if !isErrDowngrade(err) {
			errs = append(errs, err)
			nlog.Errorln(err)
		}
	} else {
		nlog.Infoln(self, tagCM, action, cm.BMD.String())
	}
	// Smap
	if err := t.receiveSmap(cm.Smap, msg, nil /*ms payload*/, caller, t.htrun.smapUpdatedCB); err != nil {
		if !isErrDowngrade(err) {
			errs = append(errs, err)
			nlog.Errorln(cmn.NewErrFailedTo(t, "sync", cm.Smap, err))
		}
	} else if cm.Smap != nil {
		nlog.Infoln(self, tagCM, action, cm.Smap)
	}
	switch {
	case errs == nil:
		return nil
	case len(errs) == 1:
		return errs[0]
	default:
		return cmn.NewErrFailedTo(t, action, "clumeta", errors.Join(errs...))
	}
}

// [METHOD] /v1/daemon
func (t *target) daemonHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		t.httpdaeget(w, r)
	case http.MethodPut:
		t.httpdaeput(w, r)
	case http.MethodPost:
		t.httpdaepost(w, r)
	case http.MethodDelete:
		t.httpdaedelete(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodPost, http.MethodPut)
	}
}

func (t *target) httpdaeput(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.parseURL(w, r, apc.URLPathDae.L, 0, true)
	if err != nil {
		return
	}
	if len(apiItems) == 0 {
		t.daeputJSON(w, r)
	} else {
		t.daeputQuery(w, r, apiItems)
	}
}

func (t *target) daeputJSON(w http.ResponseWriter, r *http.Request) {
	msg, err := t.readActionMsg(w, r)
	if err != nil {
		return
	}
	switch msg.Action {
	case apc.ActSetConfig: // set-config #2 - via action message
		t.setDaemonConfigMsg(w, r, msg, r.URL.Query())
	case apc.ActResetConfig:
		if err := t.owner.config.resetDaemonConfig(); err != nil {
			t.writeErr(w, r, err)
		}
	case apc.ActRotateLogs:
		nlog.Flush(nlog.ActRotate)
	case apc.ActResetStats:
		errorsOnly := msg.Value.(bool)
		t.statsT.ResetStats(errorsOnly)

	case apc.ActStartMaintenance:
		if !t.ensureIntraControl(w, r, true /* from primary */) {
			return
		}
		t.termKaliveX(msg.Action, true)
	case apc.ActShutdownCluster, apc.ActShutdownNode:
		if !t.ensureIntraControl(w, r, true /* from primary */) {
			return
		}
		t.termKaliveX(msg.Action, false)
		t.shutdown(msg.Action)
	case apc.ActRmNodeUnsafe:
		if !t.ensureIntraControl(w, r, true /* from primary */) {
			return
		}
		t.termKaliveX(msg.Action, true)
	case apc.ActDecommissionCluster, apc.ActDecommissionNode:
		if !t.ensureIntraControl(w, r, true /* from primary */) {
			return
		}
		var opts apc.ActValRmNode
		if err := cos.MorphMarshal(msg.Value, &opts); err != nil {
			t.writeErr(w, r, err)
			return
		}
		t.termKaliveX(msg.Action, opts.NoShutdown)
		t.decommission(msg.Action, &opts)
	case apc.ActCleanupMarkers:
		if !t.ensureIntraControl(w, r, true /* from primary */) {
			return
		}
		var ctx cleanmark
		if err := cos.MorphMarshal(msg.Value, &ctx); err != nil {
			t.writeErr(w, r, err)
			return
		}
		t.cleanupMark(&ctx)
	default:
		t.writeErrAct(w, r, msg.Action)
	}
}

func (t *target) daeputQuery(w http.ResponseWriter, r *http.Request, apiItems []string) {
	switch apiItems[0] {
	case apc.Proxy:
		// PUT /v1/daemon/proxy/newprimaryproxyid
		t.daeSetPrimary(w, r, apiItems)
	case apc.SyncSmap:
		newsmap := &smapX{}
		if cmn.ReadJSON(w, r, newsmap) != nil {
			return
		}
		if err := t.owner.smap.synchronize(t.si, newsmap, nil /*ms payload*/, t.htrun.smapUpdatedCB); err != nil {
			t.writeErr(w, r, cmn.NewErrFailedTo(t, "synchronize", newsmap, err))
		}
		nlog.Infof("%s: %s %s done", t, apc.SyncSmap, newsmap)
	case apc.Mountpaths:
		t.handleMountpathReq(w, r)
	case apc.ActSetConfig: // set-config #1 - via query parameters and "?n1=v1&n2=v2..."
		t.setDaemonConfigQuery(w, r)
	}
}

func (t *target) daeSetPrimary(w http.ResponseWriter, r *http.Request, apiItems []string) {
	var (
		err     error
		prepare bool
	)
	if len(apiItems) != 2 {
		t.writeErrf(w, r, "Incorrect number of API items: %d, should be: %d", len(apiItems), 2)
		return
	}

	proxyID := apiItems[1]
	query := r.URL.Query()
	preparestr := query.Get(apc.QparamPrepare)
	if prepare, err = cos.ParseBool(preparestr); err != nil {
		t.writeErrf(w, r, "Failed to parse %s URL Parameter: %v", apc.QparamPrepare, err)
		return
	}

	if prepare {
		if cmn.Rom.FastV(4, cos.SmoduleAIS) {
			nlog.Infoln("Preparation step: do nothing")
		}
		return
	}
	ctx := &smapModifier{pre: t._setPrim, sid: proxyID}
	err = t.owner.smap.modify(ctx)
	if err != nil {
		t.writeErr(w, r, err)
	}
}

func (t *target) _setPrim(ctx *smapModifier, clone *smapX) (err error) {
	if clone.Primary.ID() == ctx.sid {
		return
	}
	psi := clone.GetProxy(ctx.sid)
	if psi == nil {
		return &errNodeNotFound{"cannot set new primary", ctx.sid, t.si, clone}
	}
	clone.Primary = psi
	return
}

func (t *target) httpdaeget(w http.ResponseWriter, r *http.Request) {
	var (
		query       = r.URL.Query()
		getWhat     = query.Get(apc.QparamWhat)
		httpdaeWhat = "httpdaeget-" + getWhat
	)
	switch getWhat {
	case apc.WhatNodeConfig, apc.WhatSmap, apc.WhatBMD, apc.WhatSmapVote,
		apc.WhatSnode, apc.WhatLog, apc.WhatMetricNames:
		t.htrun.httpdaeget(w, r, query, t /*htext*/)
	case apc.WhatSysInfo:
		tsysinfo := apc.TSysInfo{MemCPUInfo: apc.GetMemCPU(), CapacityInfo: fs.CapStatusGetWhat()}
		t.writeJSON(w, r, tsysinfo, httpdaeWhat)
	case apc.WhatMountpaths:
		t.writeJSON(w, r, fs.MountpathsToLists(), httpdaeWhat)

	case apc.WhatNodeStats:
		ds := t.statsAndStatus()
		daeStats := t.statsT.GetStats()
		ds.Tracker = daeStats.Tracker
		ds.TargetCDF = daeStats.TargetCDF
		t.writeJSON(w, r, ds, httpdaeWhat)
	case apc.WhatNodeStatsV322: // [backward compatibility] v3.22 and prior
		ds := t.statsAndStatusV322()
		daeStats := t.statsT.GetStatsV322()
		ds.Tracker = daeStats.Tracker
		t.writeJSON(w, r, ds, httpdaeWhat)
	case apc.WhatNodeStatsAndStatus:
		ds := t.statsAndStatus()
		ds.RebSnap = _rebSnap()
		daeStats := t.statsT.GetStats()
		ds.Tracker = daeStats.Tracker
		ds.TargetCDF = daeStats.TargetCDF
		t.fillNsti(&ds.Cluster)
		t.writeJSON(w, r, ds, httpdaeWhat)
	case apc.WhatNodeStatsAndStatusV322: // [ditto]
		ds := t.statsAndStatusV322()
		ds.RebSnap = _rebSnap()
		daeStats := t.statsT.GetStatsV322()
		ds.Tracker = daeStats.Tracker
		ds.TargetCDF = daeStats.TargetCDF
		t.writeJSON(w, r, ds, httpdaeWhat)

	case apc.WhatDiskStats:
		diskStats := make(ios.AllDiskStats)
		fs.FillDiskStats(diskStats)
		t.writeJSON(w, r, diskStats, httpdaeWhat)
	case apc.WhatRemoteAIS:
		var (
			aisbp   = t.aisbp()
			refresh = cos.IsParseBool(query.Get(apc.QparamClusterInfo))
		)
		if !refresh {
			t.writeJSON(w, r, aisbp.GetInfoInternal(), httpdaeWhat)
			return
		}

		anyConf := cmn.GCO.Get().Backend.Get(apc.AIS)
		if anyConf == nil {
			t.writeJSON(w, r, meta.RemAisVec{}, httpdaeWhat)
			return
		}
		aisConf, ok := anyConf.(cmn.BackendConfAIS)
		debug.Assert(ok)

		t.writeJSON(w, r, aisbp.GetInfo(aisConf), httpdaeWhat)
	default:
		t.htrun.httpdaeget(w, r, query, t /*htext*/)
	}
}

func _rebSnap() (rebSnap *core.Snap) {
	if entry := xreg.GetLatest(xreg.Flt{Kind: apc.ActRebalance}); entry != nil {
		if xctn := entry.Get(); xctn != nil {
			rebSnap = xctn.Snap()
		}
	}
	return rebSnap
}

// admin-join target | enable/disable mountpath
func (t *target) httpdaepost(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.parseURL(w, r, apc.URLPathDae.L, 0, true)
	if err != nil {
		return
	}
	if len(apiItems) == 0 {
		t.writeErrURL(w, r)
		return
	}
	apiOp := apiItems[0]
	if apiOp == apc.Mountpaths {
		t.handleMountpathReq(w, r)
		return
	}
	if apiOp != apc.AdminJoin {
		t.writeErrURL(w, r)
		return
	}

	// user request to join cluster (compare with `apc.SelfJoin`)
	if !t.regstate.disabled.Load() {
		if t.keepalive.paused() {
			t.keepalive.ctrl(kaResumeMsg)
		} else {
			nlog.Warningf("%s already joined (\"enabled\")- nothing to do", t)
		}
		return
	}
	if daemon.cli.target.standby {
		nlog.Infof("%s: transitioning standby => join", t)
	}
	t.keepalive.ctrl(kaResumeMsg)
	body, err := cmn.ReadBytes(r)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}

	caller := r.Header.Get(apc.HdrCallerName)
	if err := t.recvCluMetaBytes(apc.ActAdminJoinTarget, body, caller); err != nil {
		t.writeErr(w, r, err)
		return
	}
	if daemon.cli.target.standby {
		if err := t.endStartupStandby(); err != nil {
			nlog.Warningf("%s: err %v ending standby...", t, err)
		}
	}
}

func (t *target) httpdaedelete(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.parseURL(w, r, apc.URLPathDae.L, 1, false)
	if err != nil {
		return
	}
	switch apiItems[0] {
	case apc.Mountpaths:
		t.handleMountpathReq(w, r)
	default:
		t.writeErrURL(w, r)
	}
}

// called by p.cleanupMark
func (t *target) cleanupMark(ctx *cleanmark) {
	smap := t.owner.smap.get()
	if smap.version() > ctx.NewVer {
		nlog.Warningf("%s: %s is newer - ignoring (and dropping) %v", t, smap, ctx)
		return
	}
	if ctx.Interrupted {
		if err := fs.RemoveMarker(fname.RebalanceMarker); err == nil {
			nlog.Infof("%s: cleanmark 'rebalance', %s", t, smap)
		} else {
			nlog.Errorf("%s: failed to cleanmark 'rebalance': %v, %s", t, err, smap)
		}
	}
	if ctx.Restarted {
		if err := fs.RemoveMarker(fname.NodeRestartedPrev); err == nil {
			nlog.Infof("%s: cleanmark 'restarted', %s", t, smap)
		} else {
			nlog.Errorf("%s: failed to cleanmark 'restarted': %v, %s", t, err, smap)
		}
	}
}

func (t *target) handleMountpathReq(w http.ResponseWriter, r *http.Request) {
	msg, err := t.readActionMsg(w, r)
	if err != nil {
		return
	}
	mpath, ok := msg.Value.(string)
	if !ok {
		t.writeErrMsg(w, r, "invalid mountpath value in request")
		return
	}
	if mpath == "" {
		t.writeErrMsg(w, r, "mountpath is not defined")
		return
	}
	switch msg.Action {
	case apc.ActMountpathEnable:
		t.enableMpath(w, r, mpath)
	case apc.ActMountpathAttach:
		t.attachMpath(w, r, mpath)
	case apc.ActMountpathDisable:
		t.disableMpath(w, r, mpath)
	case apc.ActMountpathDetach:
		t.detachMpath(w, r, mpath)
	default:
		t.writeErrAct(w, r, msg.Action)
	}

	fs.ComputeDiskSize()
}

func (t *target) enableMpath(w http.ResponseWriter, r *http.Request, mpath string) {
	enabledMi, err := t.fsprg.enableMpath(mpath)
	if err != nil {
		if cmn.IsErrMountpathNotFound(err) {
			t.writeErr(w, r, err, http.StatusNotFound)
		} else {
			// cmn.ErrInvalidMountpath
			t.writeErr(w, r, err)
		}
		return
	}
	if enabledMi == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// create missing buckets dirs
	bmd := t.owner.bmd.get()
	bmd.Range(nil, nil, func(bck *meta.Bck) bool {
		err = enabledMi.CreateMissingBckDirs(bck.Bucket())
		return err != nil // break on error
	})
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
}

func (t *target) attachMpath(w http.ResponseWriter, r *http.Request, mpath string) {
	q := r.URL.Query()
	label := ios.Label(q.Get(apc.QparamMpathLabel))
	addedMi, err := t.fsprg.attachMpath(mpath, label)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	if addedMi == nil {
		return
	}
	// create missing buckets dirs, if any
	bmd := t.owner.bmd.get()
	bmd.Range(nil, nil, func(bck *meta.Bck) bool {
		err = addedMi.CreateMissingBckDirs(bck.Bucket())
		return err != nil // break on error
	})
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
}

func (t *target) disableMpath(w http.ResponseWriter, r *http.Request, mpath string) {
	dontResilver := cos.IsParseBool(r.URL.Query().Get(apc.QparamDontResilver))
	disabledMi, err := t.fsprg.disableMpath(mpath, dontResilver)
	if err != nil {
		if cmn.IsErrMountpathNotFound(err) {
			t.writeErr(w, r, err, http.StatusNotFound)
		} else {
			// cmn.ErrInvalidMountpath
			t.writeErr(w, r, err)
		}
		return
	}
	if disabledMi == nil {
		w.WriteHeader(http.StatusNoContent)
	}
}

func (t *target) detachMpath(w http.ResponseWriter, r *http.Request, mpath string) {
	dontResilver := cos.IsParseBool(r.URL.Query().Get(apc.QparamDontResilver))
	if _, err := t.fsprg.detachMpath(mpath, dontResilver); err != nil {
		t.writeErr(w, r, err)
	}
}

func (t *target) receiveBMD(newBMD *bucketMD, msg *aisMsg, payload msPayload, tag, caller string, silent bool) (err error) {
	var oldVer int64
	if msg.UUID == "" {
		oldVer, err = t.applyBMD(newBMD, msg, payload, tag)
		if newBMD.Version > oldVer {
			if err == nil {
				logmsync(oldVer, newBMD, msg, caller, newBMD.StringEx())
			}
		}
		return
	}

	// txn [before -- do -- after]
	if errDone := t.transactions.commitBefore(caller, msg); errDone != nil {
		err = fmt.Errorf("%s commit-before %s, errDone: %v", t, newBMD, errDone)
		if !silent {
			nlog.Errorln(err)
		}
		return
	}
	oldVer, err = t.applyBMD(newBMD, msg, payload, tag)
	// log
	switch {
	case err != nil:
		nlog.Errorf("%s: %v (receive %s from %q, action %q, uuid %q)", t, err, newBMD.StringEx(), caller, msg.Action, msg.UUID)
	case newBMD.Version > oldVer:
		logmsync(oldVer, newBMD, msg, caller, newBMD.StringEx())
	case newBMD.Version == oldVer:
		nlog.Warningf("%s (same version w/ txn commit): receive %s from %q (action %q, uuid %q)",
			t, newBMD.StringEx(), caller, msg.Action, msg.UUID)
	}
	// --after]
	if errDone := t.transactions.commitAfter(caller, msg, err, newBMD); errDone != nil {
		err = fmt.Errorf("%s commit-after %s, err: %v, errDone: %v", t, newBMD, err, errDone)
		if !silent {
			nlog.Errorln(err)
		}
	}
	return
}

func (t *target) applyBMD(newBMD *bucketMD, msg *aisMsg, payload msPayload, tag string) (int64, error) {
	var (
		smap = t.owner.smap.get()
		psi  *meta.Snode
	)
	if smap.validate() == nil {
		psi = smap.Primary // (caller?)
	}

	t.owner.bmd.Lock()
	rmbcks, oldVer, emsg, err := t._syncBMD(newBMD, msg, payload, psi)
	t.owner.bmd.Unlock()

	if err != nil {
		nlog.Errorln(err)
	} else if oldVer < newBMD.Version {
		t.regstate.prevbmd.Store(false)
		t._postBMD(newBMD, tag, rmbcks)
	}
	if emsg != "" {
		nlog.Errorln(emsg)
	}
	return oldVer, err
}

// executes under lock
func (t *target) _syncBMD(newBMD *bucketMD, msg *aisMsg, payload msPayload, psi *meta.Snode) (rmbcks []*meta.Bck,
	oldVer int64, emsg string, err error) {
	var (
		createErrs  []error
		destroyErrs []error
		bmd         = t.owner.bmd.get()
	)
	if err = bmd.validateUUID(newBMD, t.si, psi, ""); err != nil {
		cos.ExitLog(err) // FATAL: cluster integrity error (cie)
		return
	}
	// check downgrade
	if oldVer = bmd.version(); newBMD.version() <= oldVer {
		if newBMD.version() < oldVer {
			err = newErrDowngrade(t.si, bmd.StringEx(), newBMD.StringEx())
		}
		return
	}
	nilbmd := bmd.version() == 0 || t.regstate.prevbmd.Load()

	// 1. create
	newBMD.Range(nil, nil, func(bck *meta.Bck) bool {
		if _, present := bmd.Get(bck); present {
			return false
		}
		errs := fs.CreateBucket(bck.Bucket(), nilbmd)
		if len(errs) > 0 {
			createErrs = append(createErrs, errs...)
		}
		return false
	})
	if len(createErrs) > 0 {
		err = fmt.Errorf("%s: failed to add new buckets: %s, old/cur %s(%t): %v",
			t, newBMD, bmd, nilbmd, errors.Join(createErrs...))
		return
	}

	// 2. persist
	if err = t.owner.bmd.putPersist(newBMD, payload); err != nil {
		cos.ExitLog(err)
		return
	}

	// 3. delete, ignore errors
	bmd.Range(nil, nil, func(obck *meta.Bck) bool {
		f := &delb{obck: obck}
		newBMD.Range(nil, nil, f.do)
		if !f.present {
			rmbcks = append(rmbcks, obck)
			if errD := fs.DestroyBucket("recv-bmd-"+msg.Action, obck.Bucket(), obck.Props.BID); errD != nil {
				destroyErrs = append(destroyErrs, errD)
			}
		}
		return false
	})
	if len(destroyErrs) > 0 {
		emsg = fmt.Sprintf("%s: failed to cleanup destroyed buckets: %s, old/cur %s(%t): %v",
			t, newBMD, bmd, nilbmd, errors.Join(destroyErrs...))
	}
	return
}

func (f *delb) do(nbck *meta.Bck) bool {
	if !f.obck.Equal(nbck, false /*ignore BID*/, false /* ignore backend */) {
		return false // keep going
	}
	f.present = true

	// assorted props changed?
	if f.obck.Props.Mirror.Enabled && !nbck.Props.Mirror.Enabled {
		flt := xreg.Flt{Kind: apc.ActPutCopies, Bck: nbck}
		xreg.DoAbort(flt, errors.New("apply-bmd"))
		// NOTE: apc.ActMakeNCopies takes care of itself
	}
	if f.obck.Props.EC.Enabled && !nbck.Props.EC.Enabled {
		flt := xreg.Flt{Kind: apc.ActECEncode, Bck: nbck}
		xreg.DoAbort(flt, errors.New("apply-bmd"))
	}
	return true // break
}

func (t *target) _postBMD(newBMD *bucketMD, tag string, rmbcks []*meta.Bck) {
	// evict LOM cache
	if len(rmbcks) > 0 {
		errV := fmt.Errorf("[post-bmd] %s %s: remove bucket%s", tag, newBMD, cos.Plural(len(rmbcks)))
		xreg.AbortAllBuckets(errV, rmbcks...)
		go func(bcks ...*meta.Bck) {
			for _, b := range bcks {
				core.UncacheBck(b)
			}
		}(rmbcks...)
	}
	if tag != bmdReg {
		if err := ec.ECM.BMDChanged(); err != nil {
			nlog.Errorf("Failed to initialize EC manager: %v", err)
		}
	}
	// since some buckets may have been destroyed
	cs := fs.Cap()
	if cs.Err() != nil {
		_ = t.OOS(nil)
	}
}

// is called under lock
func (t *target) receiveRMD(newRMD *rebMD, msg *aisMsg) (err error) {
	rmd := t.owner.rmd.get()
	if newRMD.Version <= rmd.Version {
		if newRMD.Version < rmd.Version {
			err = newErrDowngrade(t.si, rmd.String(), newRMD.String())
		}
		return
	}
	smap := t.owner.smap.get()
	if err = smap.validate(); err != nil {
		return
	}
	if smap.GetNode(t.SID()) == nil {
		err = fmt.Errorf(fmtSelfNotPresent, t, smap.StringEx())
		return
	}
	for _, tsi := range rmd.TargetIDs {
		if smap.GetNode(tsi) == nil {
			nlog.Warningf("%s: %s (target_id) not present in %s (old %s, new %s)",
				t.si, tsi, smap.StringEx(), rmd, newRMD)
		}
	}
	if !t.regstate.disabled.Load() {
		//
		// run rebalance
		//
		notif := &xact.NotifXact{
			Base: nl.Base{When: core.UponTerm, Dsts: []string{equalIC}, F: t.notifyTerm},
		}
		if msg.Action == apc.ActRebalance {
			nlog.Infof("%s: starting user-requested rebalance[%s]", t, msg.UUID)
			go t.reb.RunRebalance(&smap.Smap, newRMD.Version, notif)
			return
		}

		switch msg.Action {
		case apc.ActStartMaintenance, apc.ActDecommissionNode, apc.ActShutdownNode, apc.ActRmNodeUnsafe:
			var opts apc.ActValRmNode
			if err := cos.MorphMarshal(msg.Value, &opts); err != nil {
				debug.AssertNoErr(err) // unlikely
			} else {
				var s string
				if opts.DaemonID == t.SID() {
					s = " (to subsequently deactivate or remove _this_ target)"
				}
				nlog.Infof("%s: starting '%s' triggered rebalance[%s]%s: %+v",
					t, msg.Action, xact.RebID2S(newRMD.Version), s, opts)
			}
		default:
			nlog.Infoln(t.String() + ": starting rebalance[" + xact.RebID2S(newRMD.Version) + "]")
		}
		go t.reb.RunRebalance(&smap.Smap, newRMD.Version, notif)

		if newRMD.Resilver != "" {
			nlog.Infoln(t.String() + ": ... and resilver")
			go t.runResilver(res.Args{UUID: newRMD.Resilver, SkipGlobMisplaced: true}, nil /*wg*/)
		}
		t.owner.rmd.put(newRMD)
		// TODO: move and refactor
	} else if msg.Action == apc.ActAdminJoinTarget && daemon.cli.target.standby && msg.Name == t.SID() {
		nlog.Warningln(t.String()+": standby => join", msg.String())
		if _, err = t.joinCluster(msg.Action); err == nil {
			err = t.endStartupStandby()
		}
		t.owner.rmd.put(newRMD)
	}
	return
}

func (t *target) ensureLatestBMD(msg *aisMsg, r *http.Request) {
	bmd, bmdVersion := t.owner.bmd.Get(), msg.BMDVersion
	if bmd.Version < bmdVersion {
		nlog.Errorf("%s: local %s < v%d %s - running fixup...", t, bmd, bmdVersion, msg)
		t.BMDVersionFixup(r)
	} else if bmd.Version > bmdVersion {
		// If metasync outraces the request, we end up here, just log it and continue.
		nlog.Warningf("%s: local %s > v%d %s", t, bmd, bmdVersion, msg)
	}
}

func (t *target) getPrimaryBMD(renamed string) (bmd *bucketMD, err error) {
	smap := t.owner.smap.get()
	if err = smap.validate(); err != nil {
		return nil, cmn.NewErrFailedTo(t, "get-primary-bmd", smap, err)
	}
	var (
		what    = apc.WhatBMD
		q       = url.Values{apc.QparamWhat: []string{what}}
		psi     = smap.Primary
		path    = apc.URLPathDae.S
		url     = psi.URL(cmn.NetIntraControl)
		timeout = cmn.Rom.CplaneOperation()
	)
	if renamed != "" {
		q.Set(whatRenamedLB, renamed)
	}
	cargs := allocCargs()
	{
		cargs.si = psi
		cargs.req = cmn.HreqArgs{Method: http.MethodGet, Base: url, Path: path, Query: q}
		cargs.timeout = timeout
		cargs.cresv = cresBM{}
	}
	res := t.call(cargs, smap)
	if res.err != nil {
		time.Sleep(timeout / 2)
		smap = t.owner.smap.get()
		res = t.call(cargs, smap)
		if res.err != nil {
			err = res.errorf("%s: failed to GET(%q)", t.si, what)
		}
	}
	if err == nil {
		bmd = res.v.(*bucketMD)
	}
	freeCargs(cargs)
	freeCR(res)
	return
}

func (t *target) BMDVersionFixup(r *http.Request, bcks ...cmn.Bck) {
	var (
		caller string
		bck    cmn.Bck
	)
	if len(bcks) > 0 {
		bck = bcks[0]
	}
	time.Sleep(200 * time.Millisecond)
	newBucketMD, err := t.getPrimaryBMD(bck.Name)
	if err != nil {
		nlog.Errorln(err)
		return
	}
	msg := t.newAmsgStr("get-what="+apc.WhatBMD, newBucketMD)
	if r != nil {
		caller = r.Header.Get(apc.HdrCallerName)
	}
	t.regstate.mu.Lock()
	if nlog.Stopping() {
		t.regstate.mu.Unlock()
		return
	}
	err = t.receiveBMD(newBucketMD, msg, nil, bmdFixup, caller, true /*silent*/)
	t.regstate.mu.Unlock()
	if err != nil && !isErrDowngrade(err) {
		nlog.Errorln(err)
	}
}

// [METHOD] /v1/metasync
func (t *target) metasyncHandler(w http.ResponseWriter, r *http.Request) {
	if nlog.Stopping() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodPut:
		t.regstate.mu.Lock()
		if nlog.Stopping() {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			t.metasyncPut(w, r)
		}
		t.regstate.mu.Unlock()
	case http.MethodPost:
		t.metasyncPost(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodPost, http.MethodPut)
	}
}

// PUT /v1/metasync
// compare w/ p.metasyncHandler (NOTE: executes under regstate lock)
func (t *target) metasyncPut(w http.ResponseWriter, r *http.Request) {
	var (
		err  = &errMsync{}
		nsti = &err.Cii
	)
	if r.Method != http.MethodPut {
		cmn.WriteErr405(w, r, http.MethodPut)
		return
	}
	payload := make(msPayload)
	if errP := payload.unmarshal(r.Body, "metasync put"); errP != nil {
		cmn.WriteErr(w, r, errP)
		return
	}
	// 1. extract
	var (
		caller                       = r.Header.Get(apc.HdrCallerName)
		newConf, msgConf, errConf    = t.extractConfig(payload, caller)
		newSmap, msgSmap, errSmap    = t.extractSmap(payload, caller, false /*skip validation*/)
		newBMD, msgBMD, errBMD       = t.extractBMD(payload, caller)
		newRMD, msgRMD, errRMD       = t.extractRMD(payload, caller)
		newEtlMD, msgEtlMD, errEtlMD = t.extractEtlMD(payload, caller)
	)
	// 2. apply
	if errConf == nil && newConf != nil {
		errConf = t.receiveConfig(newConf, msgConf, payload, caller)
	}
	if errSmap == nil && newSmap != nil {
		errSmap = t.receiveSmap(newSmap, msgSmap, payload, caller, t.htrun.smapUpdatedCB)
	}
	if errBMD == nil && newBMD != nil {
		errBMD = t.receiveBMD(newBMD, msgBMD, payload, bmdRecv, caller, false /*silent*/)
	}
	if errRMD == nil && newRMD != nil {
		rmd := t.owner.rmd.get()
		logmsync(rmd.Version, newRMD, msgRMD, caller)

		t.owner.rmd.Lock()
		errRMD = t.receiveRMD(newRMD, msgRMD)
		t.owner.rmd.Unlock()
	}
	if errEtlMD == nil && newEtlMD != nil {
		errEtlMD = t.receiveEtlMD(newEtlMD, msgEtlMD, payload, caller, _stopETLs)
	}
	// 3. respond
	if errConf == nil && errSmap == nil && errBMD == nil && errRMD == nil && errEtlMD == nil {
		return
	}
	t.fillNsti(nsti)
	retErr := err.message(errConf, errSmap, errBMD, errRMD, errEtlMD, nil)
	t.writeErr(w, r, retErr, http.StatusConflict)
}

func _stopETLs(newEtlMD, oldEtlMD *etlMD) {
	for id := range oldEtlMD.ETLs {
		if _, ok := newEtlMD.ETLs[id]; ok {
			continue
		}
		// TODO: stop only when running
		nlog.Infof("stopping (removed from md) etl[%s] (old md v%d, new v%d)", id, oldEtlMD.Version, newEtlMD.Version)
		etl.Stop(id, nil)
	}
}

// compare w/ p.receiveConfig
func (t *target) receiveConfig(newConfig *globalConfig, msg *aisMsg, payload msPayload, caller string) (err error) {
	oldConfig := cmn.GCO.Get()
	logmsync(oldConfig.Version, newConfig, msg, caller)

	t.owner.config.Lock()
	err = t._recvCfg(newConfig, payload)
	t.owner.config.Unlock()
	if err != nil {
		return
	}

	if !t.NodeStarted() {
		if msg.Action == apc.ActAttachRemAis || msg.Action == apc.ActDetachRemAis {
			nlog.Errorf("%s: cannot handle %s (%s => %s) - starting up...", t, msg, oldConfig, newConfig)
		}
		return
	}

	// special: remais update
	if msg.Action == apc.ActAttachRemAis || msg.Action == apc.ActDetachRemAis {
		return t.attachDetachRemAis(newConfig, msg)
	}

	if !newConfig.Backend.EqualRemAIS(&oldConfig.Backend, t.String()) {
		if aisConf := newConfig.Backend.Get(apc.AIS); aisConf != nil {
			err = t.attachDetachRemAis(newConfig, msg)
		} else {
			t.backend[apc.AIS] = backend.NewAIS(t)
		}
	}
	return
}

// NOTE: apply the entire config: add new and update existing entries (remote clusters)
func (t *target) attachDetachRemAis(newConfig *globalConfig, msg *aisMsg) (err error) {
	var (
		aisbp   *backend.AISbp
		aisConf = newConfig.Backend.Get(apc.AIS)
	)
	debug.Assert(aisConf != nil)
	aisbp = t.aisbp()
	return aisbp.Apply(aisConf, msg.Action, &newConfig.ClusterConfig)
}

// POST /v1/metasync
func (t *target) metasyncPost(w http.ResponseWriter, r *http.Request) {
	payload := make(msPayload)
	if err := payload.unmarshal(r.Body, "metasync post"); err != nil {
		cmn.WriteErr(w, r, err)
		return
	}
	caller := r.Header.Get(apc.HdrCallerName)
	newSmap, msg, err := t.extractSmap(payload, caller, true /*skip validation*/)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	ntid := msg.UUID
	if cmn.Rom.FastV(4, cos.SmoduleAIS) {
		nlog.Infof("%s %s: %s, join %s", t, msg, newSmap, meta.Tname(ntid)) // "start-gfn" | "stop-gfn"
	}
	switch msg.Action {
	case apc.ActStartGFN:
		reb.OnTimedGFN()
	case apc.ActStopGFN:
		detail := meta.Tname(ntid) + " " + newSmap.String()
		reb.OffTimedGFN(detail)
	default:
		debug.Assert(false, msg.String())
		t.writeErrAct(w, r, msg.Action)
	}
}

// GET /v1/health (apc.Health)
func (t *target) healthHandler(w http.ResponseWriter, r *http.Request) {
	if t.regstate.disabled.Load() && daemon.cli.target.standby {
		if cmn.Rom.FastV(4, cos.SmoduleAIS) {
			nlog.Warningf("[health] %s: standing by...", t)
		}
	} else if !t.NodeStarted() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if responded := t.externalWD(w, r); responded {
		return
	}

	t.uptime2hdr(w.Header())

	var (
		getCii, getRebStatus bool
	)
	if r.URL.RawQuery != "" {
		query := r.URL.Query()
		getCii = cos.IsParseBool(query.Get(apc.QparamClusterInfo))
		getRebStatus = cos.IsParseBool(query.Get(apc.QparamRebStatus))
	}

	// piggyback [cluster info]
	if getCii {
		nsti := &cos.NodeStateInfo{}
		t.fillNsti(nsti)
		t.writeJSON(w, r, nsti, "cluster-info")
		return
	}
	// valid?
	smap := t.owner.smap.get()
	if !smap.isValid() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	// return ok plus optional reb info
	var (
		err              error
		callerID         = r.Header.Get(apc.HdrCallerID)
		caller           = r.Header.Get(apc.HdrCallerName)
		callerSmapVer, _ = strconv.ParseInt(r.Header.Get(apc.HdrCallerSmapVer), 10, 64)
	)
	if smap.version() != callerSmapVer {
		s := "older"
		if smap.version() < callerSmapVer {
			s = "newer"
		}
		err = fmt.Errorf("health-ping from (%s, %s) with %s Smap v%d", callerID, caller, s, callerSmapVer)
		nlog.Warningf("%s[%s]: %v", t, smap.StringEx(), err)
	}
	if getRebStatus {
		status := &reb.Status{}
		t.reb.RebStatus(status)
		if !t.writeJS(w, r, status, "rebalance-status") {
			return
		}
		if smap.version() < callerSmapVer && status.Running {
			// NOTE: abort right away but don't broadcast
			t.reb.AbortLocal(smap.version(), err)
		}
	}
	if smap.GetProxy(callerID) != nil {
		t.keepalive.heardFrom(callerID)
	}
}

// unregisters the target and marks it as disabled by an internal event
func (t *target) disable(msg string) {
	t.regstate.mu.Lock()

	if t.regstate.disabled.Load() {
		t.regstate.mu.Unlock()
		return // nothing to do
	}
	if err := t.unregisterSelf(false); err != nil {
		t.regstate.mu.Unlock()
		nlog.Errorf("%s but failed to remove self from Smap: %v", msg, err)
		return
	}
	t.regstate.disabled.Store(true)
	t.regstate.mu.Unlock()
	nlog.Errorf("Warning: %s => disabled and removed self from Smap", msg)
}

// registers the target again if it was disabled by and internal event
func (t *target) enable() error {
	t.regstate.mu.Lock()

	if !t.regstate.disabled.Load() {
		t.regstate.mu.Unlock()
		return nil
	}
	if _, err := t.joinCluster(apc.ActSelfJoinTarget); err != nil {
		t.regstate.mu.Unlock()
		nlog.Infof("%s failed to re-join: %v", t, err)
		return err
	}
	t.regstate.disabled.Store(false)
	t.regstate.mu.Unlock()
	nlog.Infof("%s is now active", t)
	return nil
}

// checks with a given target to see if it has the object.
// target acts as a client - compare with api.HeadObject
func (t *target) headt2t(lom *core.LOM, tsi *meta.Snode, smap *smapX) (ok bool) {
	q := lom.Bck().NewQuery()
	q.Set(apc.QparamSilent, "true")
	q.Set(apc.QparamFltPresence, strconv.Itoa(apc.FltPresent))
	cargs := allocCargs()
	{
		cargs.si = tsi
		cargs.req = cmn.HreqArgs{
			Method: http.MethodHead,
			Header: http.Header{
				apc.HdrCallerID:   []string{t.SID()},
				apc.HdrCallerName: []string{t.callerName()},
			},
			Base:  tsi.URL(cmn.NetIntraControl),
			Path:  apc.URLPathObjects.Join(lom.Bck().Name, lom.ObjName),
			Query: q,
		}
		cargs.timeout = cmn.Rom.CplaneOperation()
	}
	res := t.call(cargs, smap)
	ok = res.err == nil
	freeCargs(cargs)
	freeCR(res)
	return
}

// headObjBcast broadcasts to all targets to find out if anyone has the specified object.
// NOTE: 1) apc.QparamCheckExistsAny to make an extra effort, 2) `ignoreMaintenance`
func (t *target) headObjBcast(lom *core.LOM, smap *smapX) *meta.Snode {
	q := lom.Bck().NewQuery()
	q.Set(apc.QparamSilent, "true")
	// lookup across all mountpaths and copy (ie., restore) if misplaced
	q.Set(apc.QparamFltPresence, strconv.Itoa(apc.FltPresentCluster))
	args := allocBcArgs()
	args.req = cmn.HreqArgs{
		Method: http.MethodHead,
		Header: http.Header{
			apc.HdrCallerID:   []string{t.SID()},
			apc.HdrCallerName: []string{t.callerName()},
		},
		Path:  apc.URLPathObjects.Join(lom.Bck().Name, lom.ObjName),
		Query: q,
	}
	args.ignoreMaintenance = true
	args.smap = smap
	results := t.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err == nil {
			si := res.si
			freeBcastRes(results)
			return si
		}
	}
	freeBcastRes(results)
	return nil
}

//
// termination(s)
//

func (t *target) termKaliveX(action string, abort bool) {
	t.keepalive.ctrl(kaSuspendMsg)

	if abort {
		err := fmt.Errorf("%s: term-kalive by %q", t, action)
		xreg.AbortAll(err) // all xactions
	}
}

func (t *target) shutdown(action string) {
	t.regstate.mu.Lock()
	nlog.SetStopping()
	t.regstate.mu.Unlock()

	t.Stop(&errNoUnregister{action})
}

func (t *target) decommission(action string, opts *apc.ActValRmNode) {
	t.regstate.mu.Lock()
	nlog.SetStopping()
	t.regstate.mu.Unlock()

	nlog.Infof("%s: %s %v", t, action, opts)
	fs.Decommission(!opts.RmUserData /*ais metadata only*/)
	cleanupConfigDir(t.Name(), opts.KeepInitialConfig)

	fpath := filepath.Join(cmn.GCO.Get().ConfigDir, dbName)
	if err := cos.RemoveFile(fpath); err != nil { // delete kvdb
		nlog.Errorf("failed to delete kvdb: %v", err)
	}
	if !opts.NoShutdown {
		t.Stop(&errNoUnregister{action})
	}
}

// stop gracefully, return from rungroup.run
func (t *target) Stop(err error) {
	if !nlog.Stopping() {
		// vs metasync
		t.regstate.mu.Lock()
		nlog.SetStopping()
		t.regstate.mu.Unlock()
	}
	if err == nil {
		nlog.Infoln("Stopping", t.String())
	} else {
		nlog.Warningln("Stopping", t.String()+":", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		core.Term()
		wg.Done()
	}()

	xreg.AbortAll(err)

	t.htrun.stop(wg, g.netServ.pub.s != nil && !isErrNoUnregister(err) /*rm from Smap*/)
}
