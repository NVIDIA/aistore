// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
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
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/reb"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
	jsoniter "github.com/json-iterator/go"
)

func (t *targetrunner) initHostIP() {
	var hostIP string
	if hostIP = os.Getenv("AIS_HOST_IP"); hostIP == "" {
		return
	}
	var (
		config  = cmn.GCO.Get()
		port    = config.HostNet.Port
		extAddr = net.ParseIP(hostIP)
		extPort = port
	)
	if portStr := os.Getenv("AIS_HOST_PORT"); portStr != "" {
		portNum, err := cmn.ParsePort(portStr)
		cos.AssertNoErr(err)
		extPort = portNum
	}
	t.si.PublicNet.NodeHostname = extAddr.String()
	t.si.PublicNet.DaemonPort = strconv.Itoa(extPort)
	t.si.PublicNet.DirectURL = fmt.Sprintf("%s://%s:%d", config.Net.HTTP.Proto, extAddr.String(), extPort)
	glog.Infof("AIS_HOST_IP=%s; PubNetwork=%s", hostIP, t.si.URL(cmn.NetworkPublic))

	// applies to intra-cluster networks unless separately defined
	if !config.HostNet.UseIntraControl {
		t.si.IntraControlNet = t.si.PublicNet
	}
	if !config.HostNet.UseIntraData {
		t.si.IntraDataNet = t.si.PublicNet
	}
}

func (t *targetrunner) joinCluster(primaryURLs ...string) (status int, err error) {
	res := t.join(nil, primaryURLs...)
	defer _freeCallRes(res)
	if res.err != nil {
		status, err = res.status, res.err
		return
	}
	// not being sent at cluster startup and keepalive
	if len(res.bytes) == 0 {
		return
	}
	err = t.applyRegMeta(res.bytes, "")
	return
}

func (t *targetrunner) applyRegMeta(body []byte, caller string) (err error) {
	var regMeta nodeRegMeta
	if err = jsoniter.Unmarshal(body, &regMeta); err != nil {
		return fmt.Errorf(cmn.FmtErrUnmarshal, t.si, "reg-meta", cmn.BytesHead(body), err)
	}

	// There's a window of time between:
	// a) target joining existing cluster and b) cluster starting to rebalance itself
	// The latter is driven by regMetasync (see regMetasync.go) distributing updated cluster map.
	// To handle incoming GETs within this window (which would typically take a few seconds or less)
	// we need to have the current cluster-wide regMetadata and the temporary gfn state:
	t.gfn.global.activateTimed()

	// BMD
	msg := t.newAmsgStr(cmn.ActRegTarget, regMeta.Smap, regMeta.BMD)
	if err = t.receiveBMD(regMeta.BMD, msg, bucketMDRegister, caller); err != nil {
		if isErrDowngrade(err) {
			err = nil
		} else {
			glog.Error(err)
		}
	}

	// Smap
	if err = t.owner.smap.synchronize(t.si, regMeta.Smap); err != nil {
		if isErrDowngrade(err) {
			err = nil
		} else {
			glog.Errorf(cmn.FmtErrFailed, t.si, "sync", regMeta.Smap, err)
		}
	} else {
		glog.Infof("%s: synch %s", t.si, t.owner.smap.get())
	}
	return
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
	case cmn.ActSetConfig: // setconfig #2 - via action message
		t.setDaemonConfigMsg(w, r, &msg)
	case cmn.ActResetConfig:
		if err := t.owner.config.resetDaemonConfig(); err != nil {
			t.writeErr(w, r, err)
		}
	case cmn.ActShutdown:
		t.Stop(errShutdown)
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
		if err := t.owner.smap.synchronize(t.si, newsmap); err != nil {
			t.writeErrf(w, r, cmn.FmtErrFailed, t.si, "sync", newsmap, err)
		}
		glog.Infof("%s: %s %s done", t.si, cmn.SyncSmap, newsmap)
	case cmn.Mountpaths:
		t.handleMountpathReq(w, r)
	case cmn.ActSetConfig: // setconfig #1 - via query parameters and "?n1=v1&n2=v2..."
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
	case cmn.GetWhatConfig, cmn.GetWhatSmap, cmn.GetWhatBMD, cmn.GetWhatSmapVote, cmn.GetWhatSnode:
		t.httprunner.httpdaeget(w, r)
	case cmn.GetWhatSysInfo:
		tsysinfo := cmn.TSysInfo{
			SysInfo:      sys.FetchSysInfo(),
			CapacityInfo: fs.CapStatusAux(),
		}
		t.writeJSON(w, r, tsysinfo, httpdaeWhat)
	case cmn.GetWhatStats:
		ws := t.statsT.GetWhatStats()
		t.writeJSON(w, r, ws, httpdaeWhat)
	case cmn.GetWhatMountpaths:
		mpList := cmn.MountpathList{}
		availablePaths, disabledPaths := fs.Get()
		mpList.Available = make([]string, len(availablePaths))
		mpList.Disabled = make([]string, len(disabledPaths))

		idx := 0
		for mpath := range availablePaths {
			mpList.Available[idx] = mpath
			idx++
		}
		idx = 0
		for mpath := range disabledPaths {
			mpList.Disabled[idx] = mpath
			idx++
		}
		t.writeJSON(w, r, &mpList, httpdaeWhat)
	case cmn.GetWhatDaemonStatus:
		var (
			rebStats *stats.RebalanceTargetStats
			tstats   = t.statsT.(*stats.Trunner)
		)
		if entry := xreg.GetLatest(xreg.XactFilter{Kind: cmn.ActRebalance}); entry != nil {
			var ok bool
			if xact := entry.Get(); xact != nil {
				rebStats, ok = xact.Stats().(*stats.RebalanceTargetStats)
				debug.Assert(ok)
			}
		}
		msg := &stats.DaemonStatus{
			Snode:       t.httprunner.si,
			SmapVersion: t.owner.smap.get().Version,
			SysInfo:     sys.FetchSysInfo(),
			Stats:       tstats.CoreStats(),
			Capacity:    tstats.MPCap,
			TStatus:     &stats.TargetStatus{RebalanceStats: rebStats},
			DeployedOn:  deploymentType(),
			Version:     daemon.version,
			BuildTime:   daemon.buildTime,
		}
		t.writeJSON(w, r, msg, httpdaeWhat)
	case cmn.GetWhatDiskStats:
		diskStats := fs.GetSelectedDiskStats()
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

// register target
// enable/disable mountpath
func (t *targetrunner) httpdaepost(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 0, true, cmn.URLPathDaemon.L)
	if err != nil {
		return
	}

	if len(apiItems) > 0 {
		switch apiItems[0] {
		case cmn.UserRegister:
			t.keepalive.send(kaRegisterMsg)
			body, err := cmn.ReadBytes(r)
			if err != nil {
				t.writeErr(w, r, err)
				return
			}

			// TODO: remove setting DaemonID and Create VMD logic.
			// When a target is decommissioned, metadata (including VMD) is removed.
			// In tests, if we restore the decommissioned target without restarting, we need to re-create VMD to ensure correct node behavior.
			if err = fs.SetDaemonIDXattrAllMpaths(t.si.ID()); err != nil {
				cos.ExitLogf("%v", err)
			}

			if _, err = fs.CreateNewVMD(t.si.ID()); err != nil {
				cos.ExitLogf("%v", err)
			}

			caller := r.Header.Get(cmn.HeaderCallerName)
			if err := t.applyRegMeta(body, caller); err != nil {
				t.writeErr(w, r, err)
			}
			return
		case cmn.Mountpaths:
			t.handleMountpathReq(w, r)
			return
		default:
			t.writeErrURL(w, r)
			return
		}
	}

	if status, err := t.joinCluster(); err != nil {
		t.writeErrf(w, r, "%s: failed to register, status %d, err: %v", t.si, status, err)
		return
	}

	if glog.FastV(3, glog.SmoduleAIS) {
		glog.Infof("Registered %s(self)", t.si)
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
	case cmn.Unregister:
		t.handleUnregisterReq(w, r)
		return
	default:
		t.writeErrURL(w, r)
		return
	}
}

func (t *targetrunner) handleUnregisterReq(w http.ResponseWriter, r *http.Request) {
	if glog.V(3) {
		glog.Infoln("sending unregister on target keepalive control channel")
	}

	opts, ok, err := t.isDecommissionUnreg(w, r)
	if err != nil {
		cmn.WriteErr(w, r, err)
		return
	}

	// Stop keepaliving
	t.keepalive.send(kaUnregisterMsg)

	// Abort all dSort jobs
	dsort.Managers.AbortAll(errors.New("target was removed from the cluster"))

	// Stop all xactions
	xreg.AbortAll()

	if !ok {
		return
	}
	mdOnly := !opts.CleanData
	glog.Infof("%s: decommissioning, removing user data: %t", t.si, opts.CleanData)
	fs.Decommission(mdOnly)
	t.stopHTTPServer()
}

func (t *targetrunner) handleMountpathReq(w http.ResponseWriter, r *http.Request) {
	msg := cmn.ActionMsg{}
	if cmn.ReadJSON(w, r, &msg) != nil {
		return
	}
	mountpath, ok := msg.Value.(string)
	if !ok {
		t.writeErrMsg(w, r, "invalid mountpath value in request")
		return
	}
	if mountpath == "" {
		t.writeErrMsg(w, r, "mountpath is not defined")
		return
	}
	switch msg.Action {
	case cmn.ActMountpathEnable:
		t.handleEnableMountpathReq(w, r, mountpath)
	case cmn.ActMountpathDisable:
		t.handleDisableMountpathReq(w, r, mountpath)
	case cmn.ActMountpathAdd:
		t.handleAddMountpathReq(w, r, mountpath)
	case cmn.ActMountpathRemove:
		t.handleRemoveMountpathReq(w, r, mountpath)
	default:
		t.writeErrAct(w, r, msg.Action)
	}
}

func (t *targetrunner) handleEnableMountpathReq(w http.ResponseWriter, r *http.Request, mountpath string) {
	enabled, err := t.fsprg.enableMountpath(mountpath)
	if err != nil {
		if _, ok := err.(*cmn.ErrNoMountpath); ok {
			t.writeErr(w, r, err, http.StatusNotFound)
		} else {
			// cmn.InvalidMountpathError
			t.writeErr(w, r, err)
		}
		return
	}
	if !enabled {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	var (
		cleanMpath, _     = cmn.ValidateMpath(mountpath)
		availablePaths, _ = fs.Get()
		mi                = availablePaths[cleanMpath]
		bmd               = t.owner.bmd.get()
	)
	// create missing buckets dirs
	bmd.Range(nil, nil, func(bck *cluster.Bck) bool {
		err = mi.CreateMissingBckDirs(bck.Bck)
		return err != nil // break on error
	})
	if err != nil {
		t.writeErr(w, r, err)
		return
	}

	// TODO: Currently we must abort on enabling mountpath. In some places we open
	// files directly and put them directly on mountpaths. This can lead to
	// problems where we get from new mountpath without asking other (old)
	// mountpaths if they have it (resilver has not yet finished).
	dsort.Managers.AbortAll(fmt.Errorf("mountpath %q has been enabled during %s job - aborting due to possible errors",
		mountpath, cmn.DSortName))
}

func (t *targetrunner) handleDisableMountpathReq(w http.ResponseWriter, r *http.Request, mountpath string) {
	disabled, err := t.fsprg.disableMountpath(mountpath)
	if err != nil {
		if _, ok := err.(*cmn.ErrNoMountpath); ok {
			t.writeErr(w, r, err, http.StatusNotFound)
		} else {
			// cmn.InvalidMountpathError
			t.writeErr(w, r, err)
		}
		return
	}

	if !disabled {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	dsort.Managers.AbortAll(fmt.Errorf("mountpath %q has been disabled", mountpath))
}

func (t *targetrunner) handleAddMountpathReq(w http.ResponseWriter, r *http.Request, mountpath string) {
	err := t.fsprg.addMountpath(mountpath)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	var (
		cleanMpath, _     = cmn.ValidateMpath(mountpath)
		availablePaths, _ = fs.Get()
		mi                = availablePaths[cleanMpath]
		bmd               = t.owner.bmd.get()
	)
	// create missing buckets dirs
	bmd.Range(nil, nil, func(bck *cluster.Bck) bool {
		err = mi.CreateMissingBckDirs(bck.Bck)
		return err != nil // break on error
	})
	if err != nil {
		t.writeErr(w, r, err)
		return
	}

	// TODO: Currently we must abort on adding mountpath. In some places we open
	// files directly and put them directly on mountpaths. This can lead to
	// problems where we get from new mountpath without asking other (old)
	// mountpaths if they have it (resilver has not yet finished).
	dsort.Managers.AbortAll(fmt.Errorf("mountpath %q has been added during %s job - aborting due to possible errors",
		mountpath, cmn.DSortName))
}

func (t *targetrunner) handleRemoveMountpathReq(w http.ResponseWriter, r *http.Request, mountpath string) {
	if err := t.fsprg.removeMountpath(mountpath); err != nil {
		t.writeErrf(w, r, err.Error())
		return
	}

	dsort.Managers.AbortAll(fmt.Errorf("mountpath %q has been removed", mountpath))
}

func (t *targetrunner) receiveBMD(newBMD *bucketMD, msg *aisMsg, tag, caller string) (err error) {
	if msg.UUID == "" {
		err = t._recvBMD(newBMD, msg, tag, caller)
		return
	}
	// before -- do -- after
	if errDone := t.transactions.commitBefore(caller, msg); errDone != nil {
		err = fmt.Errorf("%s: unexpected commit-before, %s, err: %v", t.si, newBMD, errDone)
		glog.Error(err)
		return
	}

	err = t._recvBMD(newBMD, msg, tag, caller)
	if errDone := t.transactions.commitAfter(caller, msg, err, newBMD); errDone != nil {
		err = fmt.Errorf("%s: unexpected commit-after, %s, err: %v", t.si, newBMD, errDone)
		glog.Error(err)
	}
	return
}

func (t *targetrunner) _recvBMD(newBMD *bucketMD, msg *aisMsg, tag, caller string) (err error) {
	var (
		curVer                  int64
		createErrs, destroyErrs string
		bmd                     = t.owner.bmd.get()
	)
	glog.Infof(
		"[metasync] receive %s from %q (action: %q, uuid: %q)",
		newBMD.StringEx(), caller, msg.Action, msg.UUID,
	)
	t.owner.bmd.Lock()
	bmd = t.owner.bmd.get()
	curVer = bmd.version()
	var (
		bcksToDelete = make([]*cluster.Bck, 0, 4)
		_, psi       = t.getPrimaryURLAndSI()
	)
	if err = bmd.validateUUID(newBMD, t.si, psi, ""); err != nil {
		t.owner.bmd.Unlock()
		cos.ExitLogf("%v", err) // FATAL: cluster integrity error (cie)
		return
	}
	if newBMD.version() <= curVer {
		t.owner.bmd.Unlock()
		if newBMD.version() < curVer {
			err = newErrDowngrade(t.si, bmd.StringEx(), newBMD.StringEx())
		}
		return
	}

	// create buckets dirs under lock
	newBMD.Range(nil, nil, func(bck *cluster.Bck) bool {
		if _, present := bmd.Get(bck); present {
			return false
		}
		errs := fs.CreateBucket("recv-bmd-"+msg.Action, bck.Bck)
		for _, err := range errs {
			createErrs += "[" + err.Error() + "]"
		}
		return false
	})

	// NOTE: create-dir errors are _not_ ignored
	if createErrs != "" {
		t.owner.bmd.Unlock()
		glog.Errorf("[metasync] failed to receive BMD, create errs: %s", createErrs)
		err = errors.New(createErrs)
		return
	}
	// accept the new one
	if err = t.owner.bmd.put(newBMD); err != nil {
		t.owner.bmd.Unlock()
		cos.ExitLogf("%v", err)
		return
	}

	// delete buckets dirs under lock, ignore errors
	bmd.Range(nil, nil, func(obck *cluster.Bck) bool {
		var present bool
		newBMD.Range(nil, nil, func(nbck *cluster.Bck) bool {
			if !obck.Equal(nbck, false /*ignore BID*/, false /* ignore backend */) {
				return false
			}
			present = true
			if obck.Props.Mirror.Enabled && !nbck.Props.Mirror.Enabled {
				xreg.DoAbort(cmn.ActPutCopies, nbck)
				// NOTE: cmn.ActMakeNCopies takes care of itself
			}
			if obck.Props.EC.Enabled && !nbck.Props.EC.Enabled {
				xreg.DoAbort(cmn.ActECEncode, nbck)
			}
			return true
		})
		if !present {
			bcksToDelete = append(bcksToDelete, obck)
			// TODO: revisit error handling.
			// Consider performing DestroyBuckets asynchronously outside `bmd.Lock()`.
			if err := fs.DestroyBucket("recv-bmd-"+msg.Action, obck.Bck, obck.Props.BID); err != nil {
				destroyErrs += "[" + err.Error() + "]"
			}
		}
		return false
	})

	t.owner.bmd.Unlock()

	if destroyErrs != "" {
		// TODO: revisit error handling
		glog.Errorf("[metasync] failed to receive BMD, destroy errs: %s", destroyErrs)
	}

	// evict LOM cache
	if len(bcksToDelete) > 0 {
		xreg.AbortAllBuckets(bcksToDelete...)
		go func(bcks ...*cluster.Bck) {
			for _, b := range bcks {
				cluster.EvictLomCache(b)
			}
		}(bcksToDelete...)
	}
	if tag != bucketMDRegister {
		// ecmanager will get updated BMD upon its init()
		if err := ec.ECM.BucketsMDChanged(); err != nil {
			glog.Errorf("[metasync] Failed to initialize EC manager: %v", err)
		}
	}

	// refresh used/avail capacity and run LRU if need be (in part, to remove $trash)
	if _, err := fs.RefreshCapStatus(nil, nil); err != nil {
		go t.RunLRU("" /*uuid*/, false)
	}

	return
}

func (t *targetrunner) receiveSmap(newSmap *smapX, msg *aisMsg, caller string) (err error) {
	glog.Infof(
		"[metasync] receive %s from %q (primary: %s, action: %q, uuid: %q)",
		newSmap.StringEx(), caller, newSmap.Primary, msg.Action, msg.UUID,
	)

	if !newSmap.isPresent(t.si) {
		err = fmt.Errorf("%s: not finding self in the new %s", t.si, newSmap.StringEx())
		glog.Warningf("Error: %s\n%s", err, newSmap.pp())
		return
	}
	return t.owner.smap.synchronize(t.si, newSmap)
}

func (t *targetrunner) receiveRMD(newRMD *rebMD, msg *aisMsg, caller string) (err error) {
	loghdr := fmt.Sprintf("[metasync] %s from %q (action: %q, uuid: %q)",
		newRMD.String(), caller, msg.Action, msg.UUID)
	t.owner.rmd.Lock()
	defer t.owner.rmd.Unlock()

	rmd := t.owner.rmd.get()
	if newRMD.Version < rmd.Version {
		return newErrDowngrade(t.si, rmd.String(), newRMD.String())
	} else if newRMD.Version == rmd.Version {
		return
	}

	smap := t.owner.smap.get()
	if err = smap.validate(); err != nil {
		glog.Errorf("%s: cannot start rebalance %s: %v", t.si, loghdr, err) // TODO: remove
		return
	}
	if smap.GetNode(t.si.ID()) == nil {
		err = fmt.Errorf("%s (self) not present in %s", t.si, smap)
		glog.Errorf("%s - cannot start: %v", loghdr, err) // ditto
		return
	}
	for _, tsi := range rmd.TargetIDs {
		if smap.GetNode(tsi) == nil {
			err = fmt.Errorf("%s (target_id from rmd) not present in %s", tsi, smap)
			glog.Errorf("%s - cannot start: %v", loghdr, err) // ditto
			return
		}
	}
	notif := &xaction.NotifXact{
		NotifBase: nl.NotifBase{When: cluster.UponTerm, Dsts: []string{equalIC}, F: t.callerNotifyFin},
	}
	if msg.Action == cmn.ActRebalance {
		glog.Infof("%s - starting user-requested rebalance", loghdr)
		go t.rebManager.RunRebalance(&smap.Smap, newRMD.Version, notif)
		return
	}
	glog.Infof("%s - starting automated rebalance", loghdr)
	go t.rebManager.RunRebalance(&smap.Smap, newRMD.Version, notif)
	if newRMD.Resilver != "" {
		glog.Infof("%s - ... and resilver", loghdr)
		go t.runResilver(newRMD.Resilver, true /*skipGlobMisplaced*/)
	}
	return
}

func (t *targetrunner) ensureLatestBMD(msg *aisMsg, r *http.Request) {
	bmd, bmdVersion := t.owner.bmd.Get(), msg.BMDVersion
	if bmd.Version < bmdVersion {
		glog.Errorf("%s: own %s < v%d - fetching latest bmd for %v", t.si, bmd, bmdVersion, msg.Action)
		t.BMDVersionFixup(r)
	} else if bmd.Version > bmdVersion {
		// If metasync outraces the request, we end up here, just log it and continue.
		glog.Warningf("%s: own %s > v%d - encountered during %v", t.si, bmd, bmdVersion, msg.Action)
	}
}

// create local directories to test multiple fspaths
func (t *targetrunner) testCachepathMounts() {
	t.owner.config.Lock()
	config := cmn.GCO.Clone()
	config.FSpaths.Paths = make(cos.StringSet, config.TestFSP.Count)
	for i := 0; i < config.TestFSP.Count; i++ {
		mpath := filepath.Join(config.TestFSP.Root, fmt.Sprintf("mp%d", i+1))
		if config.TestFSP.Instance > 0 {
			mpath = filepath.Join(mpath, strconv.Itoa(config.TestFSP.Instance))
		}
		if err := cos.CreateDir(mpath); err != nil {
			cos.ExitLogf("Cannot create test cache dir %q, err: %s", mpath, err)
		}
		config.FSpaths.Paths.Add(mpath)
	}
	cmn.GCO.Put(config)
	t.owner.config.Unlock()
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
	timeout := cmn.GCO.Get().Timeout.CplaneOperation
	args := callArgs{
		si:      psi,
		req:     cmn.ReqArgs{Method: http.MethodGet, Base: url, Path: path, Query: q},
		timeout: timeout,
	}
	res := t.call(args)
	defer _freeCallRes(res)
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
	msg := t.newAmsgStr("get-what="+cmn.GetWhatBMD, nil, newBucketMD)
	if r != nil {
		caller = r.Header.Get(cmn.HeaderCallerName)
	}
	if err := t.receiveBMD(newBucketMD, msg, bucketMDFixup, caller); err != nil && !isErrDowngrade(err) {
		glog.Error(err)
	}
}

// handler for: /v1/tokens
func (t *targetrunner) tokenHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodDelete:
		t.httpTokenDelete(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete)
	}
}

// [METHOD] /v1/metasync
func (t *targetrunner) metasyncHandler(w http.ResponseWriter, r *http.Request) {
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
func (t *targetrunner) metasyncHandlerPut(w http.ResponseWriter, r *http.Request) {
	payload := make(msPayload)
	if err := payload.unmarshal(r.Body, "metasync put"); err != nil {
		cmn.WriteErr(w, r, err)
		return
	}
	var (
		errs   []error
		caller = r.Header.Get(cmn.HeaderCallerName)
	)

	newConf, msgConf, err := t.extractConfig(payload, caller)
	if err != nil {
		errs = append(errs, err)
	} else if newConf != nil {
		if err = t.receiveConfig(newConf, msgConf, caller); err != nil && !isErrDowngrade(err) {
			errs = append(errs, err)
		}
	}

	newSmap, msgSmap, err := t.extractSmap(payload, caller)
	if err != nil {
		errs = append(errs, err)
	} else if newSmap != nil {
		if err := t.receiveSmap(newSmap, msgSmap, caller); err != nil && !isErrDowngrade(err) {
			errs = append(errs, err)
		}
	}

	newBMD, msgBMD, err := t.extractBMD(payload, caller)
	if err != nil {
		errs = append(errs, err)
	} else if newBMD != nil {
		if err := t.receiveBMD(newBMD, msgBMD, bucketMDReceive, caller); err != nil && !isErrDowngrade(err) {
			errs = append(errs, err)
		}
	}

	newRMD, msgRMD, err := t.extractRMD(payload, caller)
	if err != nil {
		errs = append(errs, err)
	} else if newRMD != nil {
		if err := t.receiveRMD(newRMD, msgRMD, caller); err != nil {
			errs = append(errs, err)
		}
	}

	revokedTokens, err := t.extractRevokedTokenList(payload, caller)
	if err != nil {
		errs = append(errs, err)
	} else {
		t.authn.updateRevokedList(revokedTokens)
	}

	if len(errs) > 0 {
		t.writeErrf(w, r, "%v", errs)
		return
	}
}

func (t *targetrunner) receiveConfig(newConfig *globalConfig, msg *aisMsg, caller string) (err error) {
	err = t.httprunner.receiveConfig(newConfig, msg, caller)
	if err != nil {
		return
	}
	if msg.Action == cmn.ActAttach || msg.Action == cmn.ActDetach {
		// NOTE: apply the entire config: add new and _refresh_ existing
		aisConf, ok := newConfig.Backend.ProviderConf(cmn.ProviderAIS)
		cos.Assert(ok)
		aisCloud := t.backend[cmn.ProviderAIS].(*backend.AISBackendProvider)
		err = aisCloud.Apply(aisConf, msg.Action)
		if err != nil {
			glog.Errorf("%s: %v - proceeding anyway...", t.si, err)
		}
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
	caller := r.Header.Get(cmn.HeaderCallerName)
	newSmap, msg, err := t.extractSmap(payload, caller)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}

	if newSmap != nil && msg.Action == cmn.ActStartGFN {
		t.gfn.global.activateTimed()
	}
}

// GET /v1/health (cmn.Health)
func (t *targetrunner) healthHandler(w http.ResponseWriter, r *http.Request) {
	if !t.NodeStarted() {
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
		debug.Assert(query.Get(cmn.URLParamRebStatus) == "")
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
	callerID := r.Header.Get(cmn.HeaderCallerID)
	caller := r.Header.Get(cmn.HeaderCallerName)
	callerSmapVer, _ := strconv.ParseInt(r.Header.Get(cmn.HeaderCallerSmapVersion), 10, 64)
	if smap.version() != callerSmapVer {
		glog.Warningf("%s[%s]: health-ping from node (%s, %s) with different Smap v%d",
			t.si, smap.StringEx(), callerID, caller, callerSmapVer)
	}
	getRebStatus := cos.IsParseBool(query.Get(cmn.URLParamRebStatus))
	if getRebStatus {
		status := &reb.Status{}
		t.rebManager.RebStatus(status)
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

func (t *targetrunner) httpTokenDelete(w http.ResponseWriter, r *http.Request) {
	tokenList := &TokenList{}
	if _, err := t.checkRESTItems(w, r, 0, false, cmn.URLPathTokens.L); err != nil {
		return
	}

	if err := cmn.ReadJSON(w, r, tokenList); err != nil {
		return
	}

	t.authn.updateRevokedList(tokenList)
}

// unregisters the target and marks it as disabled by an internal event
func (t *targetrunner) disable() error {
	t.regstate.Lock()
	defer t.regstate.Unlock()

	if t.regstate.disabled {
		return nil
	}
	glog.Infof("Disabling %s", t.si)
	if err := t.unregisterSelf(false); err != nil {
		return err
	}
	t.regstate.disabled = true
	glog.Infof("%s has been disabled (unregistered)", t.si)
	return nil
}

// registers the target again if it was disabled by and internal event
func (t *targetrunner) enable() error {
	t.regstate.Lock()
	defer t.regstate.Unlock()

	if !t.regstate.disabled {
		return nil
	}
	glog.Infof("Enabling %s", t.si)
	if _, err := t.joinCluster(); err != nil {
		return err
	}
	t.regstate.disabled = false
	glog.Infof("%s has been enabled", t.si)
	return nil
}

// lookupRemoteSingle sends the message to the given target to see if it has the specific object.
func (t *targetrunner) LookupRemoteSingle(lom *cluster.LOM, tsi *cluster.Snode) (ok bool) {
	header := make(http.Header)
	header.Add(cmn.HeaderCallerID, t.SID())
	header.Add(cmn.HeaderCallerName, t.Sname())
	query := make(url.Values)
	query.Set(cmn.URLParamSilent, "true")
	args := callArgs{
		si: tsi,
		req: cmn.ReqArgs{
			Method: http.MethodHead,
			Header: header,
			Base:   tsi.URL(cmn.NetworkIntraControl),
			Path:   cmn.URLPathObjects.Join(lom.BckName(), lom.ObjName),
			Query:  query,
		},
		timeout: cmn.GCO.Get().Timeout.CplaneOperation,
	}
	res := t.call(args)
	ok = res.err == nil
	return
}

// lookupRemoteAll sends the broadcast message to all targets to see if they
// have the specific object.
func (t *targetrunner) lookupRemoteAll(lom *cluster.LOM, smap *smapX) *cluster.Snode {
	header := make(http.Header)
	header.Add(cmn.HeaderCallerID, t.SID())
	header.Add(cmn.HeaderCallerName, t.Sname())
	query := make(url.Values)
	query.Set(cmn.URLParamSilent, "true")
	query.Set(cmn.URLParamCheckExistsAny, "true") // lookup all mountpaths _and_ copy if misplaced
	args := allocBcastArgs()
	args.req = cmn.ReqArgs{
		Method: http.MethodHead,
		Header: header,
		Path:   cmn.URLPathObjects.Join(lom.BckName(), lom.ObjName),
		Query:  query,
	}
	args.ignoreMaintenance = true
	args.smap = smap
	results := t.bcastGroup(args)
	freeBcastArgs(args)
	for _, res := range results {
		if res.err == nil {
			si := res.si
			freeCallResults(results)
			return si
		}
	}
	freeCallResults(results)
	return nil
}
