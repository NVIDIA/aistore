// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/ais/backend"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/ext/dsort"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/reb"
	"github.com/NVIDIA/aistore/res"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
	jsoniter "github.com/json-iterator/go"
)

const (
	bmdFixup = "fixup"
	bmdRecv  = "receive"
	bmdReg   = "register"
)

func (t *target) joinCluster(action string, primaryURLs ...string) (status int, err error) {
	res := t.join(nil, primaryURLs...)
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

func (t *target) recvCluMetaBytes(action string, body []byte, caller string) (err error) {
	var cm cluMeta
	err = jsoniter.Unmarshal(body, &cm)
	if err != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, t, "reg-meta", cos.BHead(body), err)
		return
	}
	msg := t.newAmsgStr(action, cm.BMD)

	// Config
	debug.Assert(cm.Config != nil)
	if err = t.receiveConfig(cm.Config, msg, nil, caller); err != nil {
		if isErrDowngrade(err) {
			err = nil
		} else {
			glog.Error(err)
		}
		// Received outdated/invalid config in cm, ignore by setting to `nil`.
		cm.Config = nil
		// fall through
	}

	// There's a window of time between:
	// a) target joining existing cluster and b) cluster starting to rebalance itself
	// The latter is driven by regMetasync (see regMetasync.go) distributing updated cluster map.
	// To handle incoming GETs within this window (which would typically take a few seconds or less)
	// we need to have the current cluster-wide regMetadata and the temporary gfn state:
	reb.ActivateTimedGFN()

	// BMD
	if err = t.receiveBMD(cm.BMD, msg, nil /*ms payload */, bmdReg, caller, true /*silent*/); err != nil {
		if isErrDowngrade(err) {
			err = nil
		} else {
			glog.Error(err)
		}
	}
	// Smap
	if err = t.receiveSmap(cm.Smap, msg, nil /*ms payload*/, caller, nil); err != nil {
		if isErrDowngrade(err) {
			err = nil
		} else {
			glog.Error(cmn.NewErrFailedTo(t, "sync", cm.Smap, err))
		}
	} else {
		glog.Infof("%s: synch %s", t, t.owner.smap.get())
	}
	return
	// Do not receive RMD: `receiveRMD` runs extra jobs and checks specific for metasync.
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
	apiItems, err := t.apiItems(w, r, 0, true, apc.URLPathDae.L)
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
	case apc.ActResetStats:
		errorsOnly := msg.Value.(bool)
		t.statsT.ResetStats(errorsOnly)
	case apc.ActShutdown:
		if !t.ensureIntraControl(w, r, true /* from primary */) {
			return
		}
		t.unreg(msg.Action, &apc.ActValRmNode{RmUserData: false, NoShutdown: false})
	case apc.ActDecommission:
		if !t.ensureIntraControl(w, r, true /* from primary */) {
			return
		}
		var opts apc.ActValRmNode
		if err := cos.MorphMarshal(msg.Value, &opts); err != nil {
			t.writeErr(w, r, err)
			return
		}
		t.unreg(msg.Action, &opts)
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
		if err := t.owner.smap.synchronize(t.si, newsmap, nil /*ms payload*/); err != nil {
			t.writeErr(w, r, cmn.NewErrFailedTo(t, "synchronize", newsmap, err))
		}
		glog.Infof("%s: %s %s done", t, apc.SyncSmap, newsmap)
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
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Info("Preparation step: do nothing")
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
	case apc.WhatConfig, apc.WhatSmap, apc.WhatBMD, apc.WhatSmapVote,
		apc.WhatSnode, apc.WhatLog, apc.WhatNodeStats, apc.WhatMetricNames:
		t.htrun.httpdaeget(w, r, query)
	case apc.WhatSysInfo:
		tsysinfo := apc.TSysInfo{MemCPUInfo: sys.GetMemCPU(), CapacityInfo: fs.CapStatusGetWhat()}
		t.writeJSON(w, r, tsysinfo, httpdaeWhat)
	case apc.WhatMountpaths:
		t.writeJSON(w, r, fs.MountpathsToLists(), httpdaeWhat)
	case apc.WhatNodeStatsAndStatus:
		var rebSnap *cluster.Snap
		if entry := xreg.GetLatest(xact.Flt{Kind: apc.ActRebalance}); entry != nil {
			if xctn := entry.Get(); xctn != nil {
				rebSnap = xctn.Snap()
			}
		}
		smap := t.owner.smap.get()
		msg := &stats.NodeStatus{
			Node: stats.Node{
				Snode: t.htrun.si,
			},
			SmapVersion:    smap.Version,
			MemCPUInfo:     sys.GetMemCPU(),
			RebSnap:        rebSnap,
			DeploymentType: deploymentType(),
			Version:        daemon.version,
			BuildTime:      daemon.buildTime,
			K8sPodName:     os.Getenv(env.AIS.K8sPod),
			Status:         t._status(smap),
		}
		// stats and capacity
		daeStats := t.statsT.GetStats()
		msg.Tracker = daeStats.Tracker
		msg.TargetCDF = daeStats.TargetCDF

		t.writeJSON(w, r, msg, httpdaeWhat)
	case apc.WhatDiskStats:
		diskStats := make(ios.AllDiskStats)
		fs.FillDiskStats(diskStats)
		t.writeJSON(w, r, diskStats, httpdaeWhat)
	case apc.WhatRemoteAIS:
		var (
			aisBackend = t.aisBackend()
			refresh    = cos.IsParseBool(query.Get(apc.QparamClusterInfo))
		)
		if !refresh {
			t.writeJSON(w, r, aisBackend.GetInfoInternal(), httpdaeWhat)
			return
		}

		anyConf := cmn.GCO.Get().Backend.Get(apc.AIS)
		if anyConf == nil {
			t.writeJSON(w, r, cluster.Remotes{}, httpdaeWhat)
			return
		}
		aisConf, ok := anyConf.(cmn.BackendConfAIS)
		debug.Assert(ok)

		t.writeJSON(w, r, aisBackend.GetInfo(aisConf), httpdaeWhat)
	default:
		t.htrun.httpdaeget(w, r, query)
	}
}

// admin-join target | enable/disable mountpath
func (t *target) httpdaepost(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.apiItems(w, r, 0, true, apc.URLPathDae.L)
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
			glog.Warningf("%s already joined (\"enabled\")- nothing to do", t.si)
		}
		return
	}
	if daemon.cli.target.standby {
		glog.Infof("%s: transitioning standby => join", t.si)
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
			glog.Warningf("%s: err %v ending standby...", t, err)
		}
	}
}

func (t *target) httpdaedelete(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.apiItems(w, r, 1, false, apc.URLPathDae.L)
	if err != nil {
		return
	}
	switch apiItems[0] {
	case apc.Mountpaths:
		t.handleMountpathReq(w, r)
	case apc.CallbackRmSelf:
		var (
			msg  apc.ActMsg
			opts apc.ActValRmNode
		)
		if err := readJSON(w, r, &msg); err != nil {
			return
		}
		if msg.Value == nil {
			glog.Warningf("%s: remove from Smap via (%q, action=%s)", t, apiItems[0], msg.Action)
			opts.NoShutdown = true
		} else if err := cos.MorphMarshal(msg.Value, &opts); err != nil {
			t.writeErr(w, r, err)
			return
		}
		t.unreg(msg.Action, &opts)
	default:
		t.writeErrURL(w, r)
	}
}

func (t *target) unreg(action string, opts *apc.ActValRmNode) {
	// Stop keepalive-ing
	t.keepalive.ctrl(kaSuspendMsg)

	errCause := errors.New("target is being removed from the cluster via '" + action + "'")
	dsort.Managers.AbortAll(errCause) // all dSort jobs
	xreg.AbortAll(errCause)           // all xactions

	if action == apc.ActStartMaintenance || action == apc.ActCallbackRmFromSmap {
		return // return without terminating http
	}

	// NOTE: vs metasync
	t.regstate.Lock()
	daemon.stopping.Store(true)
	t.regstate.Unlock()

	if action == apc.ActShutdown {
		debug.Assert(!opts.NoShutdown)
		t.Stop(&errNoUnregister{action})
		return
	}

	glog.Infof("%s: decommissioning %+v", t, opts)
	fs.Decommission(!opts.RmUserData /*ais metadata only*/)
	cleanupConfigDir(t.Name(), opts.KeepInitialConfig)

	// Delete DB file.
	err := cos.RemoveFile(filepath.Join(cmn.GCO.Get().ConfigDir, dbName))
	if err != nil {
		glog.Errorf("failed to delete database, err: %v", err)
	}
	if !opts.NoShutdown {
		t.Stop(&errNoUnregister{action})
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
	bmd.Range(nil, nil, func(bck *cluster.Bck) bool {
		err = enabledMi.CreateMissingBckDirs(bck.Bucket())
		return err != nil // break on error
	})
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
}

func (t *target) attachMpath(w http.ResponseWriter, r *http.Request, mpath string) {
	force := cos.IsParseBool(r.URL.Query().Get(apc.QparamForce))
	addedMi, err := t.fsprg.attachMpath(mpath, force)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	if addedMi == nil {
		return
	}
	// create missing buckets dirs, if any
	bmd := t.owner.bmd.get()
	bmd.Range(nil, nil, func(bck *cluster.Bck) bool {
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
		return
	}
}

func (t *target) detachMpath(w http.ResponseWriter, r *http.Request, mpath string) {
	dontResilver := cos.IsParseBool(r.URL.Query().Get(apc.QparamDontResilver))
	removedMi, err := t.fsprg.detachMpath(mpath, dontResilver)
	if err != nil {
		t.writeErrf(w, r, err.Error())
		return
	}
	if removedMi == nil {
		return
	}
}

func (t *target) receiveBMD(newBMD *bucketMD, msg *aisMsg, payload msPayload, tag, caller string, silent bool) (err error) {
	var (
		rmbcks []*cluster.Bck
		bmd    = t.owner.bmd.get()
	)
	glog.Infof("receive %s%s", newBMD.StringEx(), _msdetail(bmd.Version, msg, caller))
	if msg.UUID == "" {
		if rmbcks, err = t._applyBMD(newBMD, msg, payload); err == nil {
			t._postBMD(tag, rmbcks)
		}
		return
	}
	// before -- do -- after
	if errDone := t.transactions.commitBefore(caller, msg); errDone != nil {
		err = fmt.Errorf("%s commit-before %s, errDone: %v", t, newBMD, errDone)
		if !silent {
			glog.Error(err)
		}
		return
	}
	if rmbcks, err = t._applyBMD(newBMD, msg, payload); err == nil {
		t._postBMD(tag, rmbcks)
	}
	if errDone := t.transactions.commitAfter(caller, msg, err, newBMD); errDone != nil {
		err = fmt.Errorf("%s commit-after %s, err: %v, errDone: %v", t, newBMD, err, errDone)
		if !silent {
			glog.Error(err)
		}
	}
	return
}

// apply under lock
func (t *target) _applyBMD(newBMD *bucketMD, msg *aisMsg, payload msPayload) (rmbcks []*cluster.Bck, err error) {
	var (
		createErrs, destroyErrs []error
		_, psi                  = t.getPrimaryURLAndSI(nil)
	)
	t.owner.bmd.Lock()
	defer t.owner.bmd.Unlock()

	bmd := t.owner.bmd.get()
	if err = bmd.validateUUID(newBMD, t.si, psi, ""); err != nil {
		cos.ExitLogf("%v", err) // FATAL: cluster integrity error (cie)
		return
	}
	if v := bmd.version(); newBMD.version() <= v {
		if newBMD.version() < v {
			err = newErrDowngrade(t.si, bmd.StringEx(), newBMD.StringEx())
		}
		return
	}

	// 1. create
	newBMD.Range(nil, nil, func(bck *cluster.Bck) bool {
		if _, present := bmd.Get(bck); present {
			return false
		}
		errs := fs.CreateBucket("recv-bmd-"+msg.Action, bck.Bucket(), bmd.version() == 0 /*nilbmd*/)
		createErrs = append(createErrs, errs...)
		return false
	})
	if len(createErrs) > 0 {
		err = fmt.Errorf("%s: failed to receive %s: %v", t, newBMD, createErrs)
		return
	}

	// 2. persist
	if err = t.owner.bmd.putPersist(newBMD, payload); err != nil {
		cos.ExitLogf("%v", err)
		return
	}

	// 3. delete, ignore errors
	bmd.Range(nil, nil, func(obck *cluster.Bck) bool {
		var present bool
		newBMD.Range(nil, nil, func(nbck *cluster.Bck) bool {
			if !obck.Equal(nbck, false /*ignore BID*/, false /* ignore backend */) {
				return false
			}
			present = true
			if obck.Props.Mirror.Enabled && !nbck.Props.Mirror.Enabled {
				flt := xact.Flt{Kind: apc.ActPutCopies, Bck: nbck}
				xreg.DoAbort(flt, errors.New("apply-bmd"))
				// NOTE: apc.ActMakeNCopies takes care of itself
			}
			if obck.Props.EC.Enabled && !nbck.Props.EC.Enabled {
				flt := xact.Flt{Kind: apc.ActECEncode, Bck: nbck}
				xreg.DoAbort(flt, errors.New("apply-bmd"))
			}
			return true
		})
		if !present {
			rmbcks = append(rmbcks, obck)
			if errD := fs.DestroyBucket("recv-bmd-"+msg.Action, obck.Bucket(), obck.Props.BID); errD != nil {
				destroyErrs = append(destroyErrs, errD)
			}
		}
		return false
	})
	if len(destroyErrs) > 0 {
		glog.Errorf("%s: failed to cleanup destroyed buckets: %v", t, destroyErrs)
	}
	return
}

func (t *target) _postBMD(tag string, rmbcks []*cluster.Bck) {
	// evict LOM cache
	if len(rmbcks) > 0 {
		xreg.AbortAllBuckets(errors.New("post-bmd"), rmbcks...)
		go func(bcks ...*cluster.Bck) {
			for _, b := range bcks {
				cluster.EvictLomCache(b)
			}
		}(rmbcks...)
	}
	if tag != bmdReg {
		// ecmanager will get updated BMD upon its init()
		if err := ec.ECM.BucketsMDChanged(); err != nil {
			glog.Errorf("Failed to initialize EC manager: %v", err)
		}
	}
	// since some buckets may have been destroyed
	if cs := fs.Cap(); cs.Err != nil {
		_ = t.OOS(nil)
	}
}

func (t *target) receiveRMD(newRMD *rebMD, msg *aisMsg, caller string) (err error) {
	rmd := t.owner.rmd.get()
	glog.Infof("receive %s%s", newRMD, _msdetail(rmd.Version, msg, caller))

	t.owner.rmd.Lock()
	defer t.owner.rmd.Unlock()

	rmd = t.owner.rmd.get()
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
	if smap.GetNode(t.si.ID()) == nil {
		err = fmt.Errorf("%s (self) not present in %s", t, smap.StringEx())
		return
	}
	for _, tsi := range rmd.TargetIDs {
		if smap.GetNode(tsi) == nil {
			glog.Warningf("%s: %s (target_id) not present in %s (old %s, new %s)",
				t.si, tsi, smap.StringEx(), rmd, newRMD)
		}
	}
	if !t.regstate.disabled.Load() {
		notif := &xact.NotifXact{
			NotifBase: nl.NotifBase{When: cluster.UponTerm, Dsts: []string{equalIC}, F: t.callerNotifyFin},
		}
		if msg.Action == apc.ActRebalance {
			glog.Infof("%s: starting user-requested rebalance", t.si)
			go t.reb.RunRebalance(&smap.Smap, newRMD.Version, notif)
			return
		}
		glog.Infof("%s: starting auto-rebalance", t.si)
		go t.reb.RunRebalance(&smap.Smap, newRMD.Version, notif)
		if newRMD.Resilver != "" {
			glog.Infof("%s: ... and resilver", t.si)
			go t.runResilver(res.Args{UUID: newRMD.Resilver, SkipGlobMisplaced: true}, nil /*wg*/)
		}
		t.owner.rmd.put(newRMD)
		// TODO: move and refactor
	} else if msg.Action == apc.ActAdminJoinTarget && daemon.cli.target.standby && msg.Name == t.si.ID() {
		glog.Warningf("%s: standby => join %s", t, msg)
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
		glog.Errorf("%s: local %s < v%d %s - running fixup...", t, bmd, bmdVersion, msg)
		t.BMDVersionFixup(r)
	} else if bmd.Version > bmdVersion {
		// If metasync outraces the request, we end up here, just log it and continue.
		glog.Warningf("%s: local %s > v%d %s", t, bmd, bmdVersion, msg)
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
		timeout = cmn.Timeout.CplaneOperation()
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
	res := t.call(cargs)
	if res.err != nil {
		time.Sleep(timeout / 2)
		res = t.call(cargs)
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
		glog.Error(err)
		return
	}
	msg := t.newAmsgStr("get-what="+apc.WhatBMD, newBucketMD)
	if r != nil {
		caller = r.Header.Get(apc.HdrCallerName)
	}
	t.regstate.Lock()
	defer t.regstate.Unlock()
	if daemon.stopping.Load() {
		return
	}
	if err := t.receiveBMD(newBucketMD, msg, nil, bmdFixup, caller, true /*silent*/); err != nil && !isErrDowngrade(err) {
		glog.Error(err)
	}
}

// [METHOD] /v1/metasync
func (t *target) metasyncHandler(w http.ResponseWriter, r *http.Request) {
	t.regstate.Lock()
	defer t.regstate.Unlock()
	if daemon.stopping.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodPut:
		t.metasyncHandlerPut(w, r)
	case http.MethodPost:
		t.metasyncHandlerPost(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodPost, http.MethodPut)
	}
}

// PUT /v1/metasync
// (compare w/ p.metasyncHandler - minor differences around receiving Smap
// and RMD - code reduction must be doable)
func (t *target) metasyncHandlerPut(w http.ResponseWriter, r *http.Request) {
	var (
		err = &errMsync{}
		cii = &err.Cii
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
		newSmap, msgSmap, errSmap    = t.extractSmap(payload, caller)
		newBMD, msgBMD, errBMD       = t.extractBMD(payload, caller)
		newRMD, msgRMD, errRMD       = t.extractRMD(payload, caller)
		newEtlMD, msgEtlMD, errEtlMD = t.extractEtlMD(payload, caller)
	)
	// 2. apply
	if errConf == nil && newConf != nil {
		errConf = t.receiveConfig(newConf, msgConf, payload, caller)
	}
	if errSmap == nil && newSmap != nil {
		errSmap = t.receiveSmap(newSmap, msgSmap, payload, caller, nil)
	}
	if errBMD == nil && newBMD != nil {
		errBMD = t.receiveBMD(newBMD, msgBMD, payload, bmdRecv, caller, false /*silent*/)
	}
	if errRMD == nil && newRMD != nil {
		errRMD = t.receiveRMD(newRMD, msgRMD, caller)
	}
	if errEtlMD == nil && newEtlMD != nil {
		errEtlMD = t.receiveEtlMD(newEtlMD, msgEtlMD, payload, caller, t._etlMDChange)
	}
	// 3. respond
	if errConf == nil && errSmap == nil && errBMD == nil && errRMD == nil && errEtlMD == nil {
		return
	}
	cii.fill(&t.htrun)
	err.message(errConf, errSmap, errBMD, errRMD, errEtlMD, nil)
	t.writeErr(w, r, errors.New(cos.MustMarshalToString(err)), http.StatusConflict)
}

func (t *target) _etlMDChange(newEtlMD, oldEtlMD *etlMD, action string) {
	for key := range oldEtlMD.ETLs {
		if _, ok := newEtlMD.ETLs[key]; ok {
			continue
		}
		// TODO: stop only when running
		glog.Infoln("ETL MD change resulting from action: " + action)
		etl.Stop(t, key, nil)
	}
}

// compare w/ p.receiveConfig
func (t *target) receiveConfig(newConfig *globalConfig, msg *aisMsg, payload msPayload, caller string) (err error) {
	oldConfig := cmn.GCO.Get()
	if err = t._recvCfg(newConfig, msg, payload, caller); err != nil {
		return
	}

	if !t.NodeStarted() {
		if msg.Action == apc.ActAttachRemAis || msg.Action == apc.ActDetachRemAis {
			glog.Errorf("%s: cannot handle %s (%s => %s) - starting up...", t, msg, oldConfig, newConfig)
		}
		return
	}

	// special: remais update
	if msg.Action == apc.ActAttachRemAis || msg.Action == apc.ActDetachRemAis {
		return t.attachDetachRemAis(newConfig, msg)
	}

	if !newConfig.Backend.EqualRemAIS(&oldConfig.Backend) {
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
		aisBackend *backend.AISBackendProvider
		aisConf    = newConfig.Backend.Get(apc.AIS)
	)
	debug.Assert(aisConf != nil)
	aisBackend = t.aisBackend()
	return aisBackend.Apply(aisConf, msg.Action, &newConfig.ClusterConfig)
}

// POST /v1/metasync
func (t *target) metasyncHandlerPost(w http.ResponseWriter, r *http.Request) {
	payload := make(msPayload)
	if err := payload.unmarshal(r.Body, "metasync post"); err != nil {
		cmn.WriteErr(w, r, err)
		return
	}
	caller := r.Header.Get(apc.HdrCallerName)
	newSmap, msg, err := t.extractSmap(payload, caller)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}

	if newSmap != nil && msg.Action == apc.ActStartGFN {
		reb.ActivateTimedGFN()
	}
}

// GET /v1/health (apc.Health)
func (t *target) healthHandler(w http.ResponseWriter, r *http.Request) {
	if t.regstate.disabled.Load() && daemon.cli.target.standby {
		glog.Warningf("[health] %s: standing by...", t.si)
	} else if !t.NodeStarted() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if responded := t.healthByExternalWD(w, r); responded {
		return
	}

	t.uptime2hdr(w.Header())

	// cluster info piggy-back
	query := r.URL.Query()
	getCii := cos.IsParseBool(query.Get(apc.QparamClusterInfo))
	if getCii {
		debug.Assert(!query.Has(apc.QparamRebStatus))
		cii := &clusterInfo{}
		cii.fill(&t.htrun)
		_ = t.writeJSON(w, r, cii, "cluster-info")
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
		callerSmapVer, _ = strconv.ParseInt(r.Header.Get(apc.HdrCallerSmapVersion), 10, 64)
	)
	if smap.version() != callerSmapVer {
		s := "older"
		if smap.version() < callerSmapVer {
			s = "newer"
		}
		err = fmt.Errorf("health-ping from (%s, %s) with %s Smap v%d", callerID, caller, s, callerSmapVer)
		glog.Warningf("%s[%s]: %v", t, smap.StringEx(), err)
	}
	getRebStatus := cos.IsParseBool(query.Get(apc.QparamRebStatus))
	if getRebStatus {
		status := &reb.Status{}
		t.reb.RebStatus(status)
		if ok := t.writeJSON(w, r, status, "rebalance-status"); !ok {
			return
		}
		if smap.version() < callerSmapVer && status.Running {
			// NOTE: abort right away but don't broadcast
			t.reb.AbortLocal(smap.version(), err)
		}
	}
	if smap.GetProxy(callerID) != nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s: health-ping from %s", t, caller)
		}
		t.keepalive.heardFrom(callerID, false)
	} else if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s: health-ping from %s", t, caller)
	}
}

// unregisters the target and marks it as disabled by an internal event
func (t *target) disable(msg string) {
	t.regstate.Lock()
	defer t.regstate.Unlock()

	if t.regstate.disabled.Load() {
		return // nothing to do
	}
	if err := t.unregisterSelf(false); err != nil {
		glog.Errorf("%s but failed to remove self from Smap: %v", msg, err)
		return
	}
	t.regstate.disabled.Store(true)
	glog.Errorf("Warning: %s => disabled and removed self from Smap", msg)
}

// registers the target again if it was disabled by and internal event
func (t *target) enable() error {
	t.regstate.Lock()
	defer t.regstate.Unlock()

	if !t.regstate.disabled.Load() {
		return nil
	}
	glog.Infof("Enabling %s", t.si)
	if _, err := t.joinCluster(apc.ActSelfJoinTarget); err != nil {
		return err
	}
	t.regstate.disabled.Store(false)
	glog.Infof("%s has been enabled", t.si)
	return nil
}

//
// HeadObj* where target acts as a client
//

// HeadObjT2T checks with a given target to see if it has the object.
// (compare with api.HeadObject)
func (t *target) HeadObjT2T(lom *cluster.LOM, tsi *cluster.Snode) (ok bool) {
	q := lom.Bck().AddToQuery(nil)
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
		cargs.timeout = cmn.Timeout.CplaneOperation()
	}
	res := t.call(cargs)
	ok = res.err == nil
	freeCargs(cargs)
	freeCR(res)
	return
}

// headObjBcast broadcasts to all targets to find out if anyone has the specified object.
// NOTE: 1) apc.QparamCheckExistsAny to make an extra effort, 2) `ignoreMaintenance`
func (t *target) headObjBcast(lom *cluster.LOM, smap *smapX) *cluster.Snode {
	q := lom.Bck().AddToQuery(nil)
	q.Set(apc.QparamSilent, "true")
	// lookup across all mountpaths and copy (ie., restore) if misplaced
	q.Set(apc.QparamFltPresence, strconv.Itoa(apc.FltPresentAnywhere))
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
