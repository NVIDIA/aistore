// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/ais/backend"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/reb"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
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
	var cm *cluMeta
	cm, status, err = t.htrun.joinCluster(t /*htext*/, primaryURLs)

	if err == nil && cm != nil {
		err = t.recvCluMeta(cm, action, "")
	}
	return
}

// do not receive RMD: `receiveRMD` runs extra jobs and checks specific for metasync.
func (t *target) recvCluMeta(cm *cluMeta, action, sender string) error {
	var (
		errs []error
		self = t.String() + ":"
	)
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
	if err := t.receiveConfig(cm.Config, msg, nil, sender); err != nil {
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
	if err := t.receiveBMD(cm.BMD, msg, nil /*ms payload */, bmdReg, sender, true /*silent*/); err != nil {
		if !isErrDowngrade(err) {
			errs = append(errs, err)
			nlog.Errorln(err)
		}
	} else {
		nlog.Infoln(self, tagCM, action, cm.BMD.String())
	}
	// Smap
	if err := t.receiveSmap(cm.Smap, msg, nil /*ms payload*/, sender, t.htrun.smapUpdatedCB); err != nil {
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
		t.daeputMsg(w, r)
	} else {
		t.daeputItems(w, r, apiItems)
	}
}

func (t *target) daeputMsg(w http.ResponseWriter, r *http.Request) {
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
	case apc.ActClearLcache:
		core.LcacheClear()

	case apc.ActReloadBackendCreds:
		provider := msg.Name

		// all
		if provider == "" {
			if err := t.initBuiltTagged(cmn.GCO.Get(), false); err != nil {
				t.writeErr(w, r, err)
			}
			// rate-limited wrappers, respectively
			for provider, bp := range t.bps {
				t.rlbps[provider] = &rlbackend{Backend: bp, t: t}
			}
			return
		}
		// one
		var bp core.Backend
		switch provider {
		case apc.AWS:
			bp, err = backend.NewAWS(t, t.statsT, false /*starting up*/)
		case apc.GCP:
			bp, err = backend.NewGCP(t, t.statsT, false)
		case apc.Azure:
			bp, err = backend.NewAzure(t, t.statsT, false)
		case apc.OCI:
			bp, err = backend.NewOCI(t, t.statsT, false)
		}
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
		debug.Assert(bp != nil)
		t.bps[provider] = bp
		t.rlbps[provider] = &rlbackend{Backend: bp, t: t}
	case apc.ActStartMaintenance:
		if !t.ensureIntraControl(w, r, true /* from primary */) {
			return
		}
		t.statsT.SetFlag(cos.NodeAlerts, cos.MaintenanceMode)
		t.termKaliveX(msg.Action, true)
	case apc.ActShutdownCluster, apc.ActShutdownNode:
		if !t.ensureIntraControl(w, r, true /* from primary */) {
			return
		}
		t.statsT.SetFlag(cos.NodeAlerts, cos.MaintenanceMode)
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
	default:
		t.writeErrAct(w, r, msg.Action)
	}
}

func (t *target) daeputItems(w http.ResponseWriter, r *http.Request, apiItems []string) {
	switch act := apiItems[0]; act {
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
		t.handleMpathReq(w, r)
	case apc.ActSetConfig: // set-config #1 - via query parameters and "?n1=v1&n2=v2..."
		t.setDaemonConfigQuery(w, r)
	case apc.ActEnableBackend, apc.ActDisableBackend:
		t.regstate.mu.Lock()
		defer t.regstate.mu.Unlock()
		if len(apiItems) < 3 { // act, provider, phase
			t.writeErrURL(w, r)
			return
		}
		var (
			provider = apiItems[1]
			phase    = apiItems[2]
		)
		if !apc.IsCloudProvider(provider) {
			t.writeErrf(w, r, "expecting cloud storage provider (have %q)", provider)
			return
		}
		if phase != apc.Begin2PC && phase != apc.Commit2PC {
			t.writeErrf(w, r, "expecting 'begin' or 'commit' phase (have %q)", phase)
			return
		}
		if act == apc.ActEnableBackend {
			t.enableBackend(w, r, provider, phase)
		} else {
			t.disableBackend(w, r, provider, phase)
		}
	case apc.LoadX509:
		t.daeLoadX509(w, r)
	}
}

func (t *target) enableBackend(w http.ResponseWriter, r *http.Request, provider, phase string) {
	var (
		config = cmn.GCO.Get()
	)
	_, ok := config.Backend.Providers[provider]
	if !ok {
		t.writeErrf(w, r, "backend %q is not configured, cannot enable", provider)
		return
	}
	bp, k := t.bps[provider]
	debug.Assert(k, provider)

	switch {
	case bp != nil:
		t.writeErrf(w, r, "backend %q is already enabled, nothing to do", provider)
	case phase == apc.Begin2PC:
		nlog.Infof("ready to enable backend %q", provider)
	default:
		var err error
		switch provider {
		case apc.AWS:
			bp, err = backend.NewAWS(t, t.statsT, false /*starting up*/)
		case apc.GCP:
			bp, err = backend.NewGCP(t, t.statsT, false /*starting up*/)
		case apc.Azure:
			bp, err = backend.NewAzure(t, t.statsT, false /*starting up*/)
		case apc.OCI:
			bp, err = backend.NewOCI(t, t.statsT, false /*starting up*/)
		}
		if err != nil {
			debug.AssertNoErr(err) // (unlikely)
			t.writeErr(w, r, err)
			return
		}

		debug.Assert(bp != nil)
		t.bps[provider] = bp
		t.rlbps[provider] = &rlbackend{Backend: bp, t: t}

		nlog.Infof("enabled backend %q", provider)
	}
}

func (t *target) disableBackend(w http.ResponseWriter, r *http.Request, provider, phase string) {
	var (
		config = cmn.GCO.Get()
	)
	_, ok := config.Backend.Providers[provider]
	if !ok {
		t.writeErrf(w, r, "backend %q is not configured, nothing to do", provider)
		return
	}
	bp, k := t.bps[provider]
	debug.Assert(k, provider)

	switch {
	case bp == nil:
		t.writeErrf(w, r, "backend %q is already disabled, nothing to do", provider)
	case phase == apc.Begin2PC:
		nlog.Infof("ready to disable backend %q", provider)
	default:
		// NOTE: not locking bp := t.Backend()
		t.bps[provider] = nil
		nlog.Infof("disabled backend %q", provider)
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
		if cmn.Rom.V(4, cos.ModAIS) {
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
		return &errNodeNotFound{t.si, clone, "cannot set new primary", ctx.sid}
	}
	clone.Primary = psi
	return
}

func (t *target) httpdaeget(w http.ResponseWriter, r *http.Request) {
	var (
		query       = r.URL.Query()
		what        = query.Get(apc.QparamWhat)
		httpdaeWhat = "httpdaeget-" + what
	)
	switch what {
	case apc.WhatNodeConfig, apc.WhatSmap, apc.WhatBMD, apc.WhatSmapVote,
		apc.WhatSnode, apc.WhatLog, apc.WhatMetricNames:
		t.htrun.httpdaeget(w, r, query, t /*htext*/)
	case apc.WhatSysInfo:
		tsysinfo := apc.TSysInfo{MemCPUInfo: apc.GetMemCPU(), CapacityInfo: fs.CapStatusGetWhat()}
		t.writeJSON(w, r, tsysinfo, httpdaeWhat)
	case apc.WhatNodeStats:
		ds := t.statsAndStatus()
		daeStats := t.statsT.GetStats()
		ds.Tracker = daeStats.Tracker
		ds.Tcdf = daeStats.Tcdf
		t.writeJSON(w, r, ds, httpdaeWhat)
	case apc.WhatNodeStatsAndStatus:
		ds := t.statsAndStatus()
		ds.RebSnap = _rebSnap()
		daeStats := t.statsT.GetStats()
		ds.Tracker = daeStats.Tracker
		ds.Tcdf = daeStats.Tcdf
		t.fillNsti(&ds.Cluster)
		t.writeJSON(w, r, ds, httpdaeWhat)

	case apc.WhatMountpaths:
		var (
			num    = fs.NumAvail()
			dstats = make(cos.AllDiskStats, num)
			config = cmn.GCO.Get()
		)
		if num == 0 {
			nlog.Warningln(t.String(), cmn.ErrNoMountpaths)
		}
		fs.DiskStats(dstats, nil, config, true /*refresh cap*/)
		mpl := fs.ToMPL()
		t.writeJSON(w, r, mpl, httpdaeWhat)
	case apc.WhatDiskRWUtilCap:
		var (
			tcdfExt fs.TcdfExt
			num     = fs.NumAvail()
			config  = cmn.GCO.Get()
		)
		tcdfExt.AllDiskStats = make(cos.AllDiskStats, num)
		tcdfExt.Mountpaths = make(map[string]*fs.CDF, num)
		if num == 0 {
			nlog.Warningln(t.String(), cmn.ErrNoMountpaths)
		}
		fs.DiskStats(tcdfExt.AllDiskStats, &tcdfExt.Tcdf, config, true)
		t.writeJSON(w, r, tcdfExt, httpdaeWhat)

	case apc.WhatRemoteAIS:
		var (
			config  = cmn.GCO.Get()
			aisbp   = t.aisbp()
			refresh = cos.IsParseBool(query.Get(apc.QparamClusterInfo))
		)
		if !refresh {
			t.writeJSON(w, r, aisbp.GetInfoInternal(), httpdaeWhat)
			return
		}
		anyConf := config.Backend.Get(apc.AIS)
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
	flt := xreg.Flt{Kind: apc.ActRebalance}
	if entry := xreg.GetLatest(&flt); entry != nil {
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
	act := apiItems[0]
	switch act {
	case apc.Mountpaths:
		t.handleMpathReq(w, r)
	case apc.ActPrimaryForce:
		t.daeForceJoin(w, r)
	case apc.AdminJoin:
		t.adminJoin(w, r)
	default:
		t.writeErrURL(w, r)
	}
}

func (t *target) adminJoin(w http.ResponseWriter, r *http.Request) {
	// user request to join cluster (compare with `apc.SelfJoin`)
	if !t.regstate.disabled.Load() {
		if t.keepalive.paused() {
			t.keepalive.ctrl(kaResumeMsg)
		} else {
			// TODO: return http.StatusNoContent
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

	cm, err := t.htrun.recvCluMeta(body)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	sender := r.Header.Get(apc.HdrSenderName)
	if err := t.recvCluMeta(cm, apc.ActAdminJoinTarget, sender); err != nil {
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
		t.handleMpathReq(w, r)
	default:
		t.writeErrURL(w, r)
	}
}

func (t *target) handleMpathReq(w http.ResponseWriter, r *http.Request) {
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
	case apc.ActMountpathRescan:
		t.rescanMpath(w, r, mpath)
	case apc.ActMountpathFSHC:
		t.fshcMpath(w, r, mpath)
	default:
		t.writeErrAct(w, r, msg.Action)
	}

	fs.ComputeDiskSize()
}

func (t *target) enableMpath(w http.ResponseWriter, r *http.Request, mpath string) {
	enabledMi, err := t.fsprg.enableMpath(mpath)
	if err != nil {
		if cmn.IsErrMpathNotFound(err) {
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
	label := cos.MountpathLabel(q.Get(apc.QparamMpathLabel))
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
		if cmn.IsErrMpathNotFound(err) {
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

func (t *target) rescanMpath(w http.ResponseWriter, r *http.Request, mpath string) {
	dontResilver := cos.IsParseBool(r.URL.Query().Get(apc.QparamDontResilver))
	err := t.fsprg.rescanMpath(mpath, dontResilver)
	if err == nil {
		return
	}
	if cmn.IsErrMpathNotFound(err) {
		t.writeErr(w, r, err, http.StatusNotFound)
	} else {
		// cmn.ErrInvalidMountpath
		t.writeErr(w, r, err)
	}
}

func (t *target) fshcMpath(w http.ResponseWriter, r *http.Request, mpath string) {
	avail, disabled := fs.Get()
	mi, ok := avail[mpath]
	if !ok {
		what := mpath
		if mi, ok = disabled[mpath]; ok {
			what = mi.String()
		}
		t.writeErrf(w, r, "%s: not starting FSHC: %s is not available", t, what)
		return
	}

	nlog.Warningf("%s: manually starting FSHC to check %s", t, mi)
	t.fshc.OnErr(mi, "")
}

func (t *target) detachMpath(w http.ResponseWriter, r *http.Request, mpath string) {
	dontResilver := cos.IsParseBool(r.URL.Query().Get(apc.QparamDontResilver))
	if _, err := t.fsprg.detachMpath(mpath, dontResilver); err != nil {
		t.writeErr(w, r, err)
	}
}

func (t *target) receiveBMD(newBMD *bucketMD, msg *actMsgExt, payload msPayload, tag, sender string, silent bool) error {
	if msg.UUID == "" {
		oldVer, err := t.applyBMD(newBMD, msg, payload, tag)
		if err == nil && newBMD.Version > oldVer {
			logmsync(oldVer, newBMD, msg, sender, newBMD.StringEx())
		}
		return err
	}

	// txn [before -- do -- after]
	if errDone := t.txns.commitBefore(sender, msg); errDone != nil {
		err := fmt.Errorf("%s commit-before %s, errDone: %v", t, newBMD, errDone)
		if !silent {
			nlog.Errorln(err)
		}
		return err
	}

	oldVer, err := t.applyBMD(newBMD, msg, payload, tag)

	// log
	switch {
	case err != nil:
		nlog.Errorf("%s: %v (receive %s from %q, action %q, uuid %q)", t, err, newBMD.StringEx(), sender, msg.Action, msg.UUID)
	case newBMD.Version > oldVer:
		logmsync(oldVer, newBMD, msg, sender, newBMD.StringEx())
	case newBMD.Version == oldVer:
		nlog.Warningf("%s (same version w/ txn commit): receive %s from %q (action %q, uuid %q)",
			t, newBMD.StringEx(), sender, msg.Action, msg.UUID)
	}

	// --after]
	if errDone := t.txns.commitAfter(sender, msg, err, newBMD); errDone != nil {
		err = fmt.Errorf("%s commit-after %s, err: %v, errDone: %v", t, newBMD, err, errDone)
		if !silent {
			nlog.Errorln(err)
		}
	}
	return err
}

func (t *target) applyBMD(newBMD *bucketMD, msg *actMsgExt, payload msPayload, tag string) (int64, error) {
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
// returns: removed buckets, old (ie., prev) version, errmsg(remove buckets), error
func (t *target) _syncBMD(newBMD *bucketMD, msg *actMsgExt, payload msPayload, psi *meta.Snode) ([]*meta.Bck, int64, string, error) {
	var (
		bmd    = t.owner.bmd.get()
		oldVer int64
	)
	// special
	if msg.Action == apc.ActPrimaryForce {
		nlog.Warningln(t.String(), "sync BMD with force [", bmd.String(), bmd.UUID, "] <- [", newBMD.String(), newBMD.UUID, "]")
		goto skip
	}

	// validate; check downgrade
	if err := bmd.validateUUID(newBMD, t.si, psi, ""); err != nil {
		nlog.Errorln(msg.String(), "-> cluster integrity error")
		cos.ExitLog(err) // FATAL: cluster integrity error (cie)
		return nil, 0, "", err
	}
	if oldVer = bmd.version(); newBMD.version() <= oldVer {
		if newBMD.version() < oldVer {
			return nil, oldVer, "", newErrDowngrade(t.si, bmd.StringEx(), newBMD.StringEx())
		}
		return nil, oldVer, "", nil
	}

skip:
	// 1. create
	var (
		createErrs []error
		nilbmd     = bmd.version() == 0 || t.regstate.prevbmd.Load()
	)
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
		err := fmt.Errorf("%s: failed to add new buckets: %s, old/cur %s(%t): %v",
			t, newBMD, bmd, nilbmd, errors.Join(createErrs...))
		return nil, oldVer, "", err
	}

	// 2. persist
	if err := t.owner.bmd.putPersist(newBMD, payload); err != nil {
		cos.ExitLog(err)
		return nil, oldVer, "", err
	}

	// 3. delete, ignore errors
	var (
		destroyErrs []error
		rmbcks      []*meta.Bck
		emsg        string
	)
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
	return rmbcks, oldVer, emsg, nil
}

func (f *delb) do(nbck *meta.Bck) bool {
	if !f.obck.Equal(nbck, false /*ignore BID*/, false /* ignore backend */) {
		return false // keep going
	}
	f.present = true

	// assorted props changed?
	if f.obck.Props.Mirror.Enabled && !nbck.Props.Mirror.Enabled {
		flt := xreg.Flt{Kind: apc.ActPutCopies, Bck: nbck}
		xreg.DoAbort(&flt, errors.New("apply-bmd"))
		// NOTE: apc.ActMakeNCopies takes care of itself
	}
	if f.obck.Props.EC.Enabled && !nbck.Props.EC.Enabled {
		flt := xreg.Flt{Kind: apc.ActECEncode, Bck: nbck}
		xreg.DoAbort(&flt, errors.New("apply-bmd"))
	}
	return true // break
}

func (t *target) _postBMD(newBMD *bucketMD, tag string, rmbcks []*meta.Bck) {
	// evict LOM cache
	if len(rmbcks) > 0 {
		wg := &sync.WaitGroup{}
		core.LcacheClearBcks(wg, rmbcks...)

		errV := fmt.Errorf("[post-bmd] %s %s: remove bucket%s", tag, newBMD, cos.Plural(len(rmbcks)))
		xreg.AbortAllBuckets(errV, rmbcks...)

		defer wg.Wait()
	}
	// EC
	if tag != bmdReg {
		if err := ec.ECM.BMDChanged(); err != nil {
			nlog.Errorln("failed to initialize EC upon BMD change:", err)
		}
	}
	// capacity (since some buckets may have been destroyed)
	cs := fs.Cap()
	if cs.Err() != nil {
		_ = t.oos(cmn.GCO.Get())
	}
}

// is called under lock
func (t *target) receiveRMD(newRMD *rebMD, msg *actMsgExt) error {
	if msg.Action == apc.ActPrimaryForce {
		return t.owner.rmd.synch(newRMD, true)
	}

	rmd := t.owner.rmd.get()
	if newRMD.Version <= rmd.Version {
		if newRMD.Version < rmd.Version {
			return newErrDowngrade(t.si, rmd.String(), newRMD.String())
		}
		return nil
	}

	smap := t.owner.smap.get()
	if err := smap.validate(); err != nil {
		return err
	}
	if smap.GetNode(t.SID()) == nil {
		return fmt.Errorf(fmtSelfNotPresent, t, smap.StringEx())
	}
	for _, tsi := range rmd.TargetIDs {
		if smap.GetNode(tsi) == nil {
			nlog.Warningf("%s: %s (target_id) not present in %s (old %s, new %s)",
				t.si, tsi, smap.StringEx(), rmd, newRMD)
		}
	}
	if !t.regstate.disabled.Load() {
		oxid := xact.RebID2S(rmd.Version)
		t._runRe(newRMD, msg, smap, oxid)
		return nil
	}

	// standby => join
	if msg.Action == apc.ActAdminJoinTarget && daemon.cli.target.standby && msg.Name == t.SID() {
		nlog.Warningln(t.String(), "standby => join:", msg.String())
		_, err := t.joinCluster(msg.Action)
		if err == nil {
			err = t.endStartupStandby()
		}
		t.owner.rmd.put(newRMD)
		return err
	}

	return nil
}

func (t *target) _runRe(newRMD *rebMD, msg *actMsgExt, smap *smapX, oxid string) {
	const tag = "rebalance["
	var (
		nxid    = xact.RebID2S(newRMD.Version)
		tname   = t.String()
		extArgs = reb.ExtArgs{
			Notif: &xact.NotifXact{
				Base: nl.Base{When: core.UponTerm, Dsts: []string{equalIC}, F: t.notifyTerm},
			},
			Tstats: t.statsT,
			Oxid:   oxid,
			NID:    newRMD.Version,
		}
	)
	if msg.UUID != nxid && msg.UUID != "" {
		nlog.Warningln(tag, msg.UUID, "vs", nxid)
	}

	// 1. by user aka admin
	if msg.Action == apc.ActRebalance {
		xname := tag + msg.UUID + "]"

		if msg.Value != nil {
			var xargs xact.ArgsMsg
			if err := cos.MorphMarshal(msg.Value, &xargs); err == nil {
				extArgs.Bck = (*meta.Bck)(&xargs.Bck)
				extArgs.Prefix = msg.Name
				extArgs.Flags = xargs.Flags
			}
		}

		nlog.Infoln(tname, "starting user-requested", xname, nxid)

		// (##a)
		go t.reb.Run(&smap.Smap, &extArgs)
		return
	}

	// 2. by RMD
	xname := tag + nxid + "]"

	switch msg.Action {
	// 2.1. action => metasync(newRMD)
	case apc.ActStartMaintenance, apc.ActDecommissionNode, apc.ActShutdownNode, apc.ActRmNodeUnsafe:
		var opts apc.ActValRmNode
		if err := cos.MorphMarshal(msg.Value, &opts); err != nil {
			debug.AssertNoErr(err) // unlikely
			return
		}

		var s string
		if opts.DaemonID == t.SID() {
			s = " (to subsequently deactivate or remove _this_ target)"
		}
		nlog.Infoln(tname, "starting", msg.String(), "-triggered", xname, s, opts)
		// (##b)
		go t.reb.Run(&smap.Smap, &extArgs)

	// 2.2. "pure" metasync(newRMD) w/ no action - double-check with cluster config
	default:
		config := cmn.GCO.Get()
		debug.Assert(config.Version > 0 && config.UUID == smap.UUID, config.String(), " vs ", smap.StringEx())

		if config.Rebalance.Enabled {
			nlog.Infoln(tname, "starting", xname)
			// (##c)
			go t.reb.Run(&smap.Smap, &extArgs)
		} else {
			runtime.Gosched()

			go func() {
				smap := t.owner.smap.get()
				if smap.GetNode(t.SID()) == nil {
					nlog.Errorf(fmtSelfNotPresent, t, smap.StringEx())
					return
				}
				config := cmn.GCO.Get()
				if !config.Rebalance.Enabled {
					//
					// NOTE: trusting local copy of the config, _not_ checking with primary via reqHealth()
					//
					nlog.Warningln(tname, "not starting", xname, "- disabled in the", config.String())
					return
				}

				// (##d)
				nlog.Infoln(tname, "starting", xname)
				t.reb.Run(&smap.Smap, &extArgs)
			}()
		}
	}

	t.owner.rmd.put(newRMD)
}

func (t *target) ensureLatestBMD(msg *actMsgExt, r *http.Request) {
	bmd, bmdVersion := t.owner.bmd.Get(), msg.BMDVersion
	if bmd.Version < bmdVersion {
		nlog.Errorf("%s: local %s < v%d %s - running fixup...", t, bmd, bmdVersion, msg)
		t.BMDVersionFixup(r)
	} else if bmd.Version > bmdVersion {
		// If metasync outraces the request, we end up here, just log it and continue.
		nlog.Warningf("%s: local %s > v%d %s", t, bmd, bmdVersion, msg)
	}
}

func (t *target) getPrimaryBMD(renamed string) (*bucketMD, error) {
	smap := t.owner.smap.get()
	if err := smap.validate(); err != nil {
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
		cargs.cresv = cresjGeneric[bucketMD]{}
	}
	res := t.call(cargs, smap)
	if res.err != nil {
		freeCR(res)
		time.Sleep(timeout / 2)
		smap = t.owner.smap.get()
		res = t.call(cargs, smap)
	}

	bmd, err := _getbmd(res, t.String(), what)
	freeCargs(cargs)
	freeCR(res)
	return bmd, err
}

func _getbmd(res *callResult, tname, what string) (bmd *bucketMD, err error) {
	if res.err != nil {
		err = res.errorf("%s: failed to GET(%q)", tname, what)
	} else {
		bmd = res.v.(*bucketMD)
	}
	return bmd, err
}

func (t *target) BMDVersionFixup(r *http.Request, bcks ...cmn.Bck) {
	var (
		sender string
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
		sender = r.Header.Get(apc.HdrSenderName)
	}
	t.regstate.mu.Lock()
	if nlog.Stopping() {
		t.regstate.mu.Unlock()
		return
	}
	err = t.receiveBMD(newBucketMD, msg, nil, bmdFixup, sender, true /*silent*/)
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
		if nlog.Stopping() || !t.NodeStarted() {
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

	t.warnMsync(r, t.owner.smap.get())

	// 1. extract
	var (
		sender                       = r.Header.Get(apc.HdrSenderName)
		newConf, msgConf, errConf    = t.extractConfig(payload, sender)
		newSmap, msgSmap, errSmap    = t.extractSmap(payload, sender, false /*skip validation*/)
		newBMD, msgBMD, errBMD       = t.extractBMD(payload, sender)
		newRMD, msgRMD, errRMD       = t.extractRMD(payload, sender)
		newEtlMD, msgEtlMD, errEtlMD = t.extractEtlMD(payload, sender)
		newCSK, msgCSK, errCSK       = t.extractCSK(payload, sender)
	)

	// 2. apply
	if errConf == nil && newConf != nil {
		errConf = t.receiveConfig(newConf, msgConf, payload, sender)
	}
	if errSmap == nil && newSmap != nil {
		errSmap = t.receiveSmap(newSmap, msgSmap, payload, sender, t.htrun.smapUpdatedCB)
	}
	if errBMD == nil && newBMD != nil {
		errBMD = t.receiveBMD(newBMD, msgBMD, payload, bmdRecv, sender, false /*silent*/)
	}
	if errRMD == nil && newRMD != nil {
		t.owner.rmd.Lock()
		errRMD = t.receiveRMD(newRMD, msgRMD)
		t.owner.rmd.Unlock()
	}
	if errEtlMD == nil && newEtlMD != nil {
		errEtlMD = t.receiveEtlMD(newEtlMD, msgEtlMD, payload, sender, _stopETLs)
	}
	if errCSK == nil && newCSK != nil {
		errCSK = t.receiveCSK(newCSK, msgCSK, sender)
	}

	// 3. respond
	if errConf == nil && errSmap == nil && errBMD == nil && errRMD == nil && errEtlMD == nil && errCSK == nil {
		return
	}
	t.fillNsti(nsti)
	retErr := err.message(errConf, errSmap, errBMD, errRMD, errEtlMD, errCSK)
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
func (t *target) receiveConfig(newConfig *globalConfig, msg *actMsgExt, payload msPayload, caller string) error {
	oldConfig := cmn.GCO.Get()
	logmsync(oldConfig.Version, newConfig, msg, caller, newConfig.String(), oldConfig.UUID)

	t.owner.config.Lock()
	err := t._recvCfg(newConfig, msg, payload)
	t.owner.config.Unlock()
	if err != nil {
		return err
	}

	if !t.NodeStarted() {
		if msg.Action == apc.ActAttachRemAis || msg.Action == apc.ActDetachRemAis {
			nlog.Errorf("%s: cannot handle %s (%s => %s) - starting up...", t, msg, oldConfig, newConfig)
		}
		return nil
	}

	if oldConfig.Space != newConfig.Space {
		fs.ExpireCapCache()
	}

	// special: remais update
	if msg.Action == apc.ActAttachRemAis || msg.Action == apc.ActDetachRemAis {
		return t.attachDetachRemAis(newConfig, msg)
	}

	if newConfig.Backend.EqualRemAIS(&oldConfig.Backend, t.String()) {
		return nil
	}
	if aisConf := newConfig.Backend.Get(apc.AIS); aisConf != nil {
		return t.attachDetachRemAis(newConfig, msg)
	}
	aisbp := backend.NewAIS(t, t.statsT, false)
	t.bps[apc.AIS] = aisbp
	t.rlbps[apc.AIS] = &rlbackend{Backend: aisbp, t: t}

	return nil
}

// NOTE: apply the entire config: add new and update existing entries (remote clusters)
func (t *target) attachDetachRemAis(newConfig *globalConfig, msg *actMsgExt) (err error) {
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
	sender := r.Header.Get(apc.HdrSenderName)
	newSmap, msg, err := t.extractSmap(payload, sender, true /*skip validation*/)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	ntid := msg.UUID
	if cmn.Rom.V(4, cos.ModAIS) {
		nlog.Infoln(t.String(), msg.String(), newSmap.String(), "join", meta.Tname(ntid)) // "start-gfn" | "stop-gfn"
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
		if cmn.Rom.V(4, cos.ModAIS) {
			nlog.Warningln("[health]", t.String(), "standing by...")
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
		senderID         = r.Header.Get(apc.HdrSenderID)
		senderName       = r.Header.Get(apc.HdrSenderName)
		senderSmapVer, _ = strconv.ParseInt(r.Header.Get(apc.HdrSenderSmapVer), 10, 64)
	)
	if smap.version() != senderSmapVer {
		s := "older"
		if smap.version() < senderSmapVer {
			s = "newer"
		}
		err = fmt.Errorf("health-ping from (%s, %s) with %s Smap v%d", senderID, senderName, s, senderSmapVer)
		nlog.Warningf("%s[%s]: %v", t, smap.StringEx(), err)
	}
	if getRebStatus {
		status := &reb.Status{}
		t.reb.RebStatus(status)
		if !t.writeJS(w, r, status, "rebalance-status") {
			return
		}
		if smap.version() < senderSmapVer && status.Running {
			// NOTE: abort right away but don't broadcast
			t.reb.AbortLocal(smap.version(), err)
		}
	}
	if smap.GetProxy(senderID) != nil {
		t.keepalive.heardFrom(senderID)
	}
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
			Base:   tsi.URL(cmn.NetIntraControl),
			Path:   apc.URLPathObjects.Join(lom.Bck().Name, lom.ObjName),
			Query:  q,
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
		Path:   apc.URLPathObjects.Join(lom.Bck().Name, lom.ObjName),
		Query:  q,
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

// disable and remove self from cluster map
func (t *target) disable() {
	t.regstate.mu.Lock()

	if t.regstate.disabled.Load() {
		t.regstate.mu.Unlock()
		return // nothing to do
	}
	smap := t.owner.smap.get()
	if err := t.rmSelf(smap, false); err != nil {
		t.regstate.mu.Unlock()
		return
	}
	t.regstate.disabled.Store(true)
	t.regstate.mu.Unlock()
	nlog.Warningln(t.String(), "disabled and removed self from", smap.String(), "action:", apc.ActSelfRemove)
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
		nlog.Infoln(t.String(), "failed to re-join:", err)
		return err
	}
	t.regstate.disabled.Store(false)
	t.regstate.mu.Unlock()
	nlog.Infoln(t.String(), "is now active")
	return nil
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
	wg.Go(core.Term)

	xreg.AbortAll(err)

	t.htrun.stop(wg, g.netServ.pub.s != nil && !isErrNoUnregister(err) /*rm from Smap*/)
}
