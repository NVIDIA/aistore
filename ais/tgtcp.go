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
	"path/filepath"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/ais/backend"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/etl"
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

func (t *targetrunner) joinCluster(action string, primaryURLs ...string) (status int, err error) {
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
	err = t.applyRegMeta(action, res.bytes, "")
	return
}

func (t *targetrunner) applyRegMeta(action string, body []byte, caller string) (err error) {
	var regMeta cluMeta
	err = jsoniter.Unmarshal(body, &regMeta)
	if err != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, t.si, "reg-meta", cmn.BytesHead(body), err)
		return
	}
	msg := t.newAmsgStr(action, regMeta.BMD)

	// Config
	debug.Assert(regMeta.Config != nil)
	if err = t.receiveConfig(regMeta.Config, msg, nil, caller); err != nil {
		if isErrDowngrade(err) {
			err = nil
		} else {
			glog.Error(err)
		}
		// Received outdated/invalid config in regMeta, ignore by setting to `nil`.
		regMeta.Config = nil
		// fall through
	}

	// There's a window of time between:
	// a) target joining existing cluster and b) cluster starting to rebalance itself
	// The latter is driven by regMetasync (see regMetasync.go) distributing updated cluster map.
	// To handle incoming GETs within this window (which would typically take a few seconds or less)
	// we need to have the current cluster-wide regMetadata and the temporary gfn state:
	reb.ActivateTimedGFN()

	// BMD
	if err = t.receiveBMD(regMeta.BMD, msg, nil /*ms payload */, bmdReg, caller, true /*silent*/); err != nil {
		if isErrDowngrade(err) {
			err = nil
		} else {
			glog.Error(err)
		}
	}
	// Smap
	if err = t.receiveSmap(regMeta.Smap, msg, nil /*ms payload*/, caller, nil); err != nil {
		if isErrDowngrade(err) {
			err = nil
		} else {
			glog.Errorf(cmn.FmtErrFailed, t.si, "sync", regMeta.Smap, err)
		}
	} else {
		glog.Infof("%s: synch %s", t.si, t.owner.smap.get())
	}
	return
	// Do not receive RMD: `receiveRMD` runs extra jobs and checks specific for metasync.
}

// [METHOD] /v1/daemon
func (t *targetrunner) daemonHandler(w http.ResponseWriter, r *http.Request) {
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

func (t *targetrunner) httpdaeput(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 0, true, cmn.URLPathDaemon.L)
	if err != nil {
		return
	}
	if len(apiItems) == 0 {
		t.daeputJSON(w, r)
	} else {
		t.daeputQuery(w, r, apiItems)
	}
}

func (t *targetrunner) daeputJSON(w http.ResponseWriter, r *http.Request) {
	var msg cmn.ActionMsg
	if cmn.ReadJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case cmn.ActSetConfig: // set-config #2 - via action message
		t.setDaemonConfigMsg(w, r, &msg)
	case cmn.ActResetConfig:
		if err := t.owner.config.resetDaemonConfig(); err != nil {
			t.writeErr(w, r, err)
		}
	case cmn.ActShutdown:
		if !t.ensureIntraControl(w, r, true /* from primary */) {
			return
		}
		t.unreg(msg.Action, false /*rm user data*/, false /*no shutdown*/)
	case cmn.ActDecommission:
		if !t.ensureIntraControl(w, r, true /* from primary */) {
			return
		}
		var opts cmn.ActValRmNode
		if err := cos.MorphMarshal(msg.Value, &opts); err != nil {
			t.writeErr(w, r, err)
			return
		}
		t.unreg(msg.Action, opts.RmUserData, opts.NoShutdown)
	default:
		t.writeErrAct(w, r, msg.Action)
	}
}

func (t *targetrunner) daeputQuery(w http.ResponseWriter, r *http.Request, apiItems []string) {
	switch apiItems[0] {
	case cmn.Proxy:
		// PUT /v1/daemon/proxy/newprimaryproxyid
		t.daeSetPrimary(w, r, apiItems)
	case cmn.SyncSmap:
		newsmap := &smapX{}
		if cmn.ReadJSON(w, r, newsmap) != nil {
			return
		}
		if err := t.owner.smap.synchronize(t.si, newsmap, nil /*ms payload*/); err != nil {
			t.writeErrf(w, r, cmn.FmtErrFailed, t.si, "sync", newsmap, err)
		}
		glog.Infof("%s: %s %s done", t.si, cmn.SyncSmap, newsmap)
	case cmn.Mountpaths:
		t.handleMountpathReq(w, r)
	case cmn.ActSetConfig: // set-config #1 - via query parameters and "?n1=v1&n2=v2..."
		t.setDaemonConfigQuery(w, r)
	}
}

func (t *targetrunner) daeSetPrimary(w http.ResponseWriter, r *http.Request, apiItems []string) {
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
	preparestr := query.Get(cmn.URLParamPrepare)
	if prepare, err = cos.ParseBool(preparestr); err != nil {
		t.writeErrf(w, r, "Failed to parse %s URL Parameter: %v", cmn.URLParamPrepare, err)
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

func (t *targetrunner) _setPrim(ctx *smapModifier, clone *smapX) (err error) {
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

func (t *targetrunner) httpdaeget(w http.ResponseWriter, r *http.Request) {
	getWhat := r.URL.Query().Get(cmn.URLParamWhat)
	httpdaeWhat := "httpdaeget-" + getWhat
	switch getWhat {
	case cmn.GetWhatConfig, cmn.GetWhatSmap, cmn.GetWhatBMD, cmn.GetWhatSmapVote, cmn.GetWhatSnode, cmn.GetWhatLog, cmn.GetWhatStats:
		t.httprunner.httpdaeget(w, r)
	case cmn.GetWhatSysInfo:
		tsysinfo := cmn.TSysInfo{
			SysInfo:      sys.FetchSysInfo(),
			CapacityInfo: fs.CapStatusAux(),
		}
		t.writeJSON(w, r, tsysinfo, httpdaeWhat)
	case cmn.GetWhatMountpaths:
		t.writeJSON(w, r, fs.MountpathsToLists(), httpdaeWhat)
	case cmn.GetWhatDaemonStatus:
		var rebSnap *stats.RebalanceSnap
		if entry := xreg.GetLatest(xreg.XactFilter{Kind: cmn.ActRebalance}); entry != nil {
			var ok bool
			if xctn := entry.Get(); xctn != nil {
				rebSnap, ok = xctn.Snap().(*stats.RebalanceSnap)
				debug.Assert(ok)
			}
		}
		msg := &stats.DaemonStatus{
			Snode:       t.httprunner.si,
			SmapVersion: t.owner.smap.get().Version,
			SysInfo:     sys.FetchSysInfo(),
			Stats:       t.statsT.CoreStats(),
			RebSnap:     rebSnap,
			DeployedOn:  deploymentType(),
			Version:     daemon.version,
			BuildTime:   daemon.buildTime,
		}
		// capacity
		tstats := t.statsT.(*stats.Trunner)
		msg.Capacity = tstats.MPCap
		t.writeJSON(w, r, msg, httpdaeWhat)
	case cmn.GetWhatDiskStats:
		diskStats := make(ios.AllDiskStats)
		fs.FillDiskStats(diskStats)
		t.writeJSON(w, r, diskStats, httpdaeWhat)
	case cmn.GetWhatRemoteAIS:
		conf, ok := cmn.GCO.Get().Backend.ProviderConf(cmn.ProviderAIS)
		if !ok {
			t.writeJSON(w, r, cmn.BackendInfoAIS{}, httpdaeWhat)
			return
		}
		clusterConf, ok := conf.(cmn.BackendConfAIS)
		cos.Assert(ok)
		aisCloud := t.backend[cmn.ProviderAIS].(*backend.AISBackendProvider)
		t.writeJSON(w, r, aisCloud.GetInfo(clusterConf), httpdaeWhat)
	default:
		t.httprunner.httpdaeget(w, r)
	}
}

// admin-join target | enable/disable mountpath
func (t *targetrunner) httpdaepost(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 0, true, cmn.URLPathDaemon.L)
	if err != nil {
		return
	}
	if len(apiItems) == 0 {
		t.writeErrURL(w, r)
		return
	}
	apiOp := apiItems[0]
	if apiOp == cmn.Mountpaths {
		t.handleMountpathReq(w, r)
		return
	}
	if apiOp != cmn.AdminJoin {
		t.writeErrURL(w, r)
		return
	}

	// user request to join cluster (compare with `cmn.SelfJoin`)
	if !t.regstate.disabled.Load() {
		if t.keepalive.paused() {
			t.keepalive.send(kaResumeMsg)
		} else {
			glog.Warningf("%s already joined (\"enabled\")- nothing to do", t.si)
		}
		return
	}
	if daemon.cli.target.standby {
		glog.Infof("%s: transitioning standby => join", t.si)
	}
	t.keepalive.send(kaResumeMsg)
	body, err := cmn.ReadBytes(r)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}

	caller := r.Header.Get(cmn.HdrCallerName)
	if err := t.applyRegMeta(cmn.ActAdminJoinTarget, body, caller); err != nil {
		t.writeErr(w, r, err)
		return
	}
	if daemon.cli.target.standby {
		if err := t.endStartupStandby(); err != nil {
			glog.Warningf("%s: err %v ending standby...", t.si, err)
		}
	}
}

func (t *targetrunner) httpdaedelete(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.URLPathDaemon.L)
	if err != nil {
		return
	}
	switch apiItems[0] {
	case cmn.Mountpaths:
		t.handleMountpathReq(w, r)
		return
	case cmn.CallbackRmFromSmap:
		var (
			noShutdown, rmUserData bool
			opts, action, err      = t.parseUnregMsg(w, r)
		)
		if err != nil {
			return
		}
		if opts == nil {
			glog.Warningf("%s: remove from Smap via (%q, action=%s)", t.si, apiItems[0], action)
			noShutdown = true
		} else {
			noShutdown, rmUserData = opts.NoShutdown, opts.RmUserData
		}
		t.unreg(action, rmUserData, noShutdown)
		return
	default:
		t.writeErrURL(w, r)
		return
	}
}

func (t *targetrunner) unreg(action string, rmUserData, noShutdown bool) {
	// Stop keepalive-ing
	t.keepalive.send(kaSuspendMsg)

	errCause := errors.New("target is being removed from the cluster via '" + action + "'")
	dsort.Managers.AbortAll(errCause) // all dSort jobs
	xreg.AbortAll(errCause)           // all xactions

	if action == cmn.ActStartMaintenance || action == cmn.ActCallbackRmFromSmap {
		return // return without terminating http
	}

	// NOTE: vs metasync
	t.regstate.Lock()
	daemon.stopping.Store(true)
	t.regstate.Unlock()

	writeShutdownMarker()
	if action == cmn.ActShutdown {
		debug.Assert(!noShutdown)
		t.Stop(&errNoUnregister{action})
		return
	}

	glog.Infof("%s: decommissioning, (remove-user-data, no-shutdown) = (%t, %t)", t.si, rmUserData, noShutdown)
	fs.Decommission(!rmUserData /*ais metadata only*/)

	err := cleanupConfigDir()
	if err != nil {
		glog.Errorf("%s: failed to cleanup config dir, err: %v", t.si, err)
	}
	// Delete DB file.
	err = cos.RemoveFile(filepath.Join(cmn.GCO.Get().ConfigDir, dbName))
	if err != nil {
		glog.Errorf("failed to delete database, err: %v", err)
	}
	if !noShutdown {
		t.Stop(&errNoUnregister{action})
	}
}

func (t *targetrunner) handleMountpathReq(w http.ResponseWriter, r *http.Request) {
	msg := cmn.ActionMsg{}
	if cmn.ReadJSON(w, r, &msg) != nil {
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
	case cmn.ActMountpathEnable:
		t.enableMpath(w, r, mpath)
	case cmn.ActMountpathAttach:
		t.attachMpath(w, r, mpath)
	case cmn.ActMountpathDisable:
		t.disableMpath(w, r, mpath)
	case cmn.ActMountpathDetach:
		t.detachMpath(w, r, mpath)
	default:
		t.writeErrAct(w, r, msg.Action)
	}
}

func (t *targetrunner) enableMpath(w http.ResponseWriter, r *http.Request, mpath string) {
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
		err = enabledMi.CreateMissingBckDirs(bck.Bck)
		return err != nil // break on error
	})
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	// TODO: Currently, dSort doesn't handle adding/enabling mountpaths at runtime
	dsort.Managers.AbortAll(fmt.Errorf("mountpath %s has been enabled", enabledMi))
}

func (t *targetrunner) attachMpath(w http.ResponseWriter, r *http.Request, mpath string) {
	force := cos.IsParseBool(r.URL.Query().Get(cmn.URLParamForce))
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
		err = addedMi.CreateMissingBckDirs(bck.Bck)
		return err != nil // break on error
	})
	if err != nil {
		t.writeErr(w, r, err)
		return
	}

	// TODO: Currently, dSort doesn't handle adding/enabling mountpaths at runtime
	dsort.Managers.AbortAll(fmt.Errorf("attached %s", addedMi))
}

func (t *targetrunner) disableMpath(w http.ResponseWriter, r *http.Request, mpath string) {
	dontResilver := cos.IsParseBool(r.URL.Query().Get(cmn.URLParamDontResilver))
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
	dsort.Managers.AbortAll(fmt.Errorf("mountpath %s has been disabled", disabledMi))
}

func (t *targetrunner) detachMpath(w http.ResponseWriter, r *http.Request, mpath string) {
	dontResilver := cos.IsParseBool(r.URL.Query().Get(cmn.URLParamDontResilver))
	removedMi, err := t.fsprg.detachMpath(mpath, dontResilver)
	if err != nil {
		t.writeErrf(w, r, err.Error())
		return
	}
	if removedMi == nil {
		return
	}
	dsort.Managers.AbortAll(fmt.Errorf("detached %s", removedMi))
}

func (t *targetrunner) receiveBMD(newBMD *bucketMD, msg *aisMsg, payload msPayload, tag, caller string, silent bool) (err error) {
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
		err = fmt.Errorf("%s commit-before %s, errDone: %v", t.si, newBMD, errDone)
		if !silent {
			glog.Error(err)
		}
		return
	}
	if rmbcks, err = t._applyBMD(newBMD, msg, payload); err == nil {
		t._postBMD(tag, rmbcks)
	}
	if errDone := t.transactions.commitAfter(caller, msg, err, newBMD); errDone != nil {
		err = fmt.Errorf("%s commit-after %s, err: %v, errDone: %v", t.si, newBMD, err, errDone)
		if !silent {
			glog.Error(err)
		}
	}
	return
}

// apply under lock
func (t *targetrunner) _applyBMD(newBMD *bucketMD, msg *aisMsg, payload msPayload) (rmbcks []*cluster.Bck, err error) {
	var (
		createErrs, destroyErrs []error
		_, psi                  = t.getPrimaryURLAndSI()
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
		errs := fs.CreateBucket("recv-bmd-"+msg.Action, bck.Bck, bmd.version() == 0 /*nilbmd*/)
		createErrs = append(createErrs, errs...)
		return false
	})
	if len(createErrs) > 0 {
		err = fmt.Errorf("%s: failed to receive %s: %v", t.si, newBMD, createErrs)
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
				xreg.DoAbort(cmn.ActPutCopies, nbck, errors.New("apply-bmd"))
				// NOTE: cmn.ActMakeNCopies takes care of itself
			}
			if obck.Props.EC.Enabled && !nbck.Props.EC.Enabled {
				xreg.DoAbort(cmn.ActECEncode, nbck, errors.New("apply-bmd"))
			}
			return true
		})
		if !present {
			rmbcks = append(rmbcks, obck)
			if errD := fs.DestroyBucket("recv-bmd-"+msg.Action, obck.Bck, obck.Props.BID); errD != nil {
				destroyErrs = append(destroyErrs, errD)
			}
		}
		return false
	})
	if len(destroyErrs) > 0 {
		glog.Errorf("%s: failed to cleanup destroyed buckets: %v", t.si, destroyErrs)
	}
	return
}

func (t *targetrunner) _postBMD(tag string, rmbcks []*cluster.Bck) {
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
	if cs := fs.GetCapStatus(); cs.Err != nil {
		_ = t.OOS(nil)
	}
}

func (t *targetrunner) receiveRMD(newRMD *rebMD, msg *aisMsg, caller string) (err error) {
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
		err = fmt.Errorf("%s (self) not present in %s", t.si, smap.StringEx())
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
		if msg.Action == cmn.ActRebalance {
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
		// TODO -- FIXME: move and refactor
	} else if msg.Action == cmn.ActAdminJoinTarget && daemon.cli.target.standby && msg.Name == t.si.ID() {
		glog.Warningf("%s: standby => join (%q, %q)", t.si, msg.Action, msg.Name)
		if _, err = t.joinCluster(msg.Action); err == nil {
			err = t.endStartupStandby()
		}
		t.owner.rmd.put(newRMD)
	}
	return
}

func (t *targetrunner) ensureLatestBMD(msg *aisMsg, r *http.Request) {
	bmd, bmdVersion := t.owner.bmd.Get(), msg.BMDVersion
	if bmd.Version < bmdVersion {
		glog.Errorf("%s: local %s < v%d, action %q - fetching latest...", t.si, bmd, bmdVersion, msg.Action)
		t.BMDVersionFixup(r)
	} else if bmd.Version > bmdVersion {
		// If metasync outraces the request, we end up here, just log it and continue.
		glog.Warningf("%s: local %s > v%d, action %q", t.si, bmd, bmdVersion, msg.Action)
	}
}

func (t *targetrunner) fetchPrimaryMD(what string, outStruct interface{}, renamed string) (err error) {
	smap := t.owner.smap.get()
	if err = smap.validate(); err != nil {
		return fmt.Errorf("%s: %s is invalid: %v", t.si, smap, err)
	}
	psi := smap.Primary
	q := url.Values{}
	q.Set(cmn.URLParamWhat, what)
	if renamed != "" {
		q.Add(whatRenamedLB, renamed)
	}
	path := cmn.URLPathDaemon.S
	url := psi.URL(cmn.NetworkIntraControl)
	config := cmn.GCO.Get()
	timeout := config.Timeout.CplaneOperation.D()
	args := callArgs{
		si:      psi,
		req:     cmn.ReqArgs{Method: http.MethodGet, Base: url, Path: path, Query: q},
		timeout: timeout,
	}
	res := t.call(args)
	defer freeCR(res)
	if res.err != nil {
		time.Sleep(timeout / 2)
		res = t.call(args)
		if res.err != nil {
			err = res.errorf("%s: failed to GET(%q)", t.si, what)
			return
		}
	}
	err = jsoniter.Unmarshal(res.bytes, outStruct)
	if err != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, t.si, what, cmn.BytesHead(res.bytes), err)
	}
	return
}

func (t *targetrunner) BMDVersionFixup(r *http.Request, bcks ...cmn.Bck) {
	var (
		caller      string
		bck         cmn.Bck
		newBucketMD = &bucketMD{}
	)
	if len(bcks) > 0 {
		bck = bcks[0]
	}
	time.Sleep(200 * time.Millisecond)
	if err := t.fetchPrimaryMD(cmn.GetWhatBMD, newBucketMD, bck.Name); err != nil {
		glog.Error(err)
		return
	}
	msg := t.newAmsgStr("get-what="+cmn.GetWhatBMD, newBucketMD)
	if r != nil {
		caller = r.Header.Get(cmn.HdrCallerName)
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
func (t *targetrunner) metasyncHandler(w http.ResponseWriter, r *http.Request) {
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
// TODO: excepting small difference around receiveSmap and ReceiveBMD
//       the code is identical to p.metasyncHandler() - not reducing it to
//       one is lack of polymorphism to execute target-specific
//       receive* while adding interfaces won't do enough.
// NOTE: compare with receiveCluMeta() and p.metasyncHandlerPut
func (t *targetrunner) metasyncHandlerPut(w http.ResponseWriter, r *http.Request) {
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
		caller                       = r.Header.Get(cmn.HdrCallerName)
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
	cii.fill(&t.httprunner)
	err.message(errConf, errSmap, errBMD, errRMD, errEtlMD, nil)
	t.writeErr(w, r, errors.New(cos.MustMarshalToString(err)), http.StatusConflict)
}

func (t *targetrunner) _etlMDChange(newEtlMD, oldEtlMD *etlMD) {
	for key := range oldEtlMD.ETLs {
		if _, ok := newEtlMD.ETLs[key]; ok {
			continue
		}
		// TODO: stop only when running; specify cause
		etl.Stop(t, key, nil)
	}
}

func (t *targetrunner) receiveConfig(newConfig *globalConfig, msg *aisMsg, payload msPayload, caller string) (err error) {
	oldConfig := cmn.GCO.Get()
	err = t.httprunner.receiveConfig(newConfig, msg, payload, caller)
	if err != nil {
		return
	}
	if !t.NodeStarted() { // starting up
		debug.Assert(msg.Action != cmn.ActAttachRemote && msg.Action != cmn.ActDetachRemote)
		return
	}
	if msg.Action == cmn.ActAttachRemote || msg.Action == cmn.ActDetachRemote {
		// NOTE: apply the entire config: add new and _refresh_ existing
		aisConf, ok := newConfig.Backend.ProviderConf(cmn.ProviderAIS)
		cos.Assert(ok)
		aisCloud := t.backend[cmn.ProviderAIS].(*backend.AISBackendProvider)
		err = aisCloud.Apply(aisConf, msg.Action)
		if err != nil {
			glog.Errorf("%s: %v - proceeding anyway...", t.si, err)
		}
		return
	}
	if !newConfig.Backend.EqualRemAIS(&oldConfig.Backend) {
		t.backend.init(t, false /*starting*/) // re-init from scratch
		return
	}
	// NOTE: primary command-line flag `-override_backends` allows to choose
	//       cloud backends (and HDFS) at deployment time
	//       (via build tags) - see earlystart
	if !newConfig.Backend.EqualClouds(&oldConfig.Backend) {
		t.backend.initExt(t, false /*starting*/)
	}
	return
}

// POST /v1/metasync
func (t *targetrunner) metasyncHandlerPost(w http.ResponseWriter, r *http.Request) {
	payload := make(msPayload)
	if err := payload.unmarshal(r.Body, "metasync post"); err != nil {
		cmn.WriteErr(w, r, err)
		return
	}
	caller := r.Header.Get(cmn.HdrCallerName)
	newSmap, msg, err := t.extractSmap(payload, caller)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}

	if newSmap != nil && msg.Action == cmn.ActStartGFN {
		reb.ActivateTimedGFN()
	}
}

// GET /v1/health (cmn.Health)
func (t *targetrunner) healthHandler(w http.ResponseWriter, r *http.Request) {
	if t.regstate.disabled.Load() && daemon.cli.target.standby {
		glog.Warningf("[health] %s: standing by...", t.si)
	} else if !t.NodeStarted() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if responded := t.healthByExternalWD(w, r); responded {
		return
	}
	// cluster info piggy-back
	query := r.URL.Query()
	getCii := cos.IsParseBool(query.Get(cmn.URLParamClusterInfo))
	if getCii {
		debug.Assert(!query.Has(cmn.URLParamRebStatus))
		cii := &clusterInfo{}
		cii.fill(&t.httprunner)
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
	callerID := r.Header.Get(cmn.HdrCallerID)
	caller := r.Header.Get(cmn.HdrCallerName)
	callerSmapVer, _ := strconv.ParseInt(r.Header.Get(cmn.HdrCallerSmapVersion), 10, 64)
	if smap.version() != callerSmapVer {
		glog.Warningf("%s[%s]: health-ping from node (%s, %s) with different Smap v%d",
			t.si, smap.StringEx(), callerID, caller, callerSmapVer)
	}
	getRebStatus := cos.IsParseBool(query.Get(cmn.URLParamRebStatus))
	if getRebStatus {
		status := &reb.Status{}
		t.reb.RebStatus(status)
		if ok := t.writeJSON(w, r, status, "rebalance-status"); !ok {
			return
		}
	}
	if smap.GetProxy(callerID) != nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s: health-ping from %s", t.si, caller)
		}
		t.keepalive.heardFrom(callerID, false)
	} else if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s: health-ping from %s", t.si, caller)
	}
}

// unregisters the target and marks it as disabled by an internal event
func (t *targetrunner) disable(msg string) {
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
func (t *targetrunner) enable() error {
	t.regstate.Lock()
	defer t.regstate.Unlock()

	if !t.regstate.disabled.Load() {
		return nil
	}
	glog.Infof("Enabling %s", t.si)
	if _, err := t.joinCluster(cmn.ActSelfJoinTarget); err != nil {
		return err
	}
	t.regstate.disabled.Store(false)
	glog.Infof("%s has been enabled", t.si)
	return nil
}

// lookupRemoteSingle sends the message to the given target to see if it has the specific object.
func (t *targetrunner) LookupRemoteSingle(lom *cluster.LOM, tsi *cluster.Snode) (ok bool) {
	header := make(http.Header)
	header.Add(cmn.HdrCallerID, t.SID())
	header.Add(cmn.HdrCallerName, t.Sname())
	query := make(url.Values)
	query.Set(cmn.URLParamSilent, "true")
	config := cmn.GCO.Get()
	args := callArgs{
		si: tsi,
		req: cmn.ReqArgs{
			Method: http.MethodHead,
			Header: header,
			Base:   tsi.URL(cmn.NetworkIntraControl),
			Path:   cmn.URLPathObjects.Join(lom.Bck().Name, lom.ObjName),
			Query:  query,
		},
		timeout: config.Timeout.CplaneOperation.D(),
	}
	res := t.call(args)
	ok = res.err == nil
	return
}

// lookupRemoteAll sends the broadcast message to all targets to see if they
// have the specific object.
func (t *targetrunner) lookupRemoteAll(lom *cluster.LOM, smap *smapX) *cluster.Snode {
	header := make(http.Header)
	header.Add(cmn.HdrCallerID, t.SID())
	header.Add(cmn.HdrCallerName, t.Sname())
	query := make(url.Values)
	query.Set(cmn.URLParamSilent, "true")
	query.Set(cmn.URLParamCheckExistsAny, "true") // lookup all mountpaths _and_ copy if misplaced
	bck := lom.Bck().Bck
	args := allocBcastArgs()
	args.req = cmn.ReqArgs{
		Method: http.MethodHead,
		Header: header,
		Path:   cmn.URLPathObjects.Join(bck.Name, lom.ObjName),
		Query:  cmn.AddBckToQuery(query, bck),
	}
	args.ignoreMaintenance = true
	args.smap = smap
	results := t.bcastGroup(args)
	freeBcastArgs(args)
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
