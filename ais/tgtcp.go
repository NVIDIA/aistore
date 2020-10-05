// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/ais/cloud"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/reb"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/registry"
	jsoniter "github.com/json-iterator/go"
)

func (t *targetrunner) initHostIP() {
	var hostIP string
	if hostIP = os.Getenv("AIS_HOST_IP"); hostIP == "" {
		return
	}
	var (
		config  = cmn.GCO.Get()
		port    = config.Net.L4.Port
		extAddr = net.ParseIP(hostIP)
		extPort = port
	)
	if portStr := os.Getenv("AIS_HOST_PORT"); portStr != "" {
		portNum, err := cmn.ParsePort(portStr)
		cmn.AssertNoErr(err)
		extPort = portNum
	}
	t.si.PublicNet.NodeIPAddr = extAddr.String()
	t.si.PublicNet.DaemonPort = strconv.Itoa(extPort)
	t.si.PublicNet.DirectURL = fmt.Sprintf("%s://%s:%d", config.Net.HTTP.Proto, extAddr.String(), extPort)
	glog.Infof("AIS_HOST_IP=%s; PubNetwork=%s", hostIP, t.si.URL(cmn.NetworkPublic))

	// applies to intra-cluster networks unless separately defined
	if !config.Net.UseIntraControl {
		t.si.IntraControlNet = t.si.PublicNet
	}
	if !config.Net.UseIntraData {
		t.si.IntraDataNet = t.si.PublicNet
	}
}

func (t *targetrunner) joinCluster(primaryURLs ...string) (status int, err error) {
	res := t.join(nil, primaryURLs...)
	if res.err != nil {
		return res.status, res.err
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
	err = jsoniter.Unmarshal(body, &regMeta)
	if err != nil {
		return fmt.Errorf("unexpected: %s failed to unmarshal reg-meta, err: %v", t.si, err)
	}

	// There's a window of time between:
	// a) target joining existing cluster and b) cluster starting to rebalance itself
	// The latter is driven by regMetasync (see regMetasync.go) distributing updated cluster map.
	// To handle incoming GETs within this window (which would typically take a few seconds or less)
	// we need to have the current cluster-wide regMetadata and the temporary gfn state:
	t.gfn.global.activateTimed()

	// BMD
	msg := t.newAisMsgStr(cmn.ActRegTarget, regMeta.Smap, regMeta.BMD)
	if err = t.receiveBMD(regMeta.BMD, msg, bucketMDRegister, caller); err != nil {
		glog.Infof("%s: %s", t.si, t.owner.bmd.get())
	}
	// Smap
	if err := t.owner.smap.synchronize(regMeta.Smap, true /* lesserIsErr */); err != nil {
		glog.Errorf("%s: sync Smap err %v", t.si, err)
	} else {
		glog.Infof("%s: sync %s", t.si, t.owner.smap.get())
	}
	return
}

func (t *targetrunner) unregister(_ ...string) (int, error) {
	smap := t.owner.smap.get()
	if smap == nil || !smap.isValid() {
		return 0, nil
	}
	args := callArgs{
		si: smap.Primary,
		req: cmn.ReqArgs{
			Method: http.MethodDelete,
			Path:   cmn.JoinWords(cmn.Version, cmn.Cluster, cmn.Daemon, t.si.ID()),
		},
		timeout: cmn.DefaultTimeout,
	}
	res := t.call(args)
	return res.status, res.err
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
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /daemon path")
	}
}

func (t *targetrunner) httpdaeput(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Daemon)
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
		var (
			value string
			ok    bool
		)
		if value, ok = msg.Value.(string); !ok {
			t.invalmsghdlrf(w, r, "failed to parse action %q value: (%v, %T) not a string", cmn.ActSetConfig, msg.Value, msg.Value)
			return
		}
		kvs := cmn.NewSimpleKVs(cmn.SimpleKVsEntry{Key: msg.Name, Value: value})
		if err := t.setConfig(kvs); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
	case cmn.ActShutdown:
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	default:
		t.invalmsghdlrf(w, r, fmtUnknownAct, msg)
	}
}

func (t *targetrunner) daeputQuery(w http.ResponseWriter, r *http.Request, apiItems []string) {
	switch apiItems[0] {
	case cmn.Proxy:
		// PUT /v1/daemon/proxy/newprimaryproxyid
		t.httpdaesetprimaryproxy(w, r, apiItems)
	case cmn.SyncSmap:
		var newsmap = &smapX{}
		if cmn.ReadJSON(w, r, newsmap) != nil {
			return
		}
		if err := t.owner.smap.synchronize(newsmap, true /* lesserIsErr */); err != nil {
			t.invalmsghdlrf(w, r, "failed to sync Smap: %s", err)
		}
		glog.Infof("%s: %s %s done", t.si, cmn.SyncSmap, newsmap)
	case cmn.Mountpaths:
		t.handleMountpathReq(w, r)
	case cmn.ActSetConfig: // setconfig #1 - via query parameters and "?n1=v1&n2=v2..."
		kvs := cmn.NewSimpleKVsFromQuery(r.URL.Query())
		if err := t.setConfig(kvs); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
	case cmn.ActAttach, cmn.ActDetach:
		var (
			query  = r.URL.Query()
			what   = query.Get(cmn.URLParamWhat)
			action = apiItems[0]
		)
		if what != cmn.GetWhatRemoteAIS {
			t.invalmsghdlrf(w, r, fmtUnknownQue, what)
			return
		}
		if err := t.attachDetachRemoteAIS(query, action); err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
		// NOTE: once validated, save this config unconditionally, and prior to attempting attachment(s)
		// TODO: metasync
		if err := jsp.SaveConfig(fmt.Sprintf("%s(%s)", action, cmn.GetWhatRemoteAIS)); err != nil {
			glog.Error(err)
		}
		//
		// NOTE: apply the entire config: add new and _refresh_ existing
		//
		aisConf, ok := cmn.GCO.Get().Cloud.ProviderConf(cmn.ProviderAIS)
		cmn.Assert(ok)
		aisCloud := t.cloud[cmn.ProviderAIS].(*cloud.AisCloudProvider)
		if err := aisCloud.Apply(aisConf, action); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
	}
}

func (t *targetrunner) setConfig(kvs cmn.SimpleKVs) (err error) { return jsp.SetConfigMany(kvs) }

func (t *targetrunner) httpdaesetprimaryproxy(w http.ResponseWriter, r *http.Request, apiItems []string) {
	var (
		err     error
		prepare bool
	)
	if len(apiItems) != 2 {
		t.invalmsghdlrf(w, r, "Incorrect number of API items: %d, should be: %d", len(apiItems), 2)
		return
	}

	proxyID := apiItems[1]
	query := r.URL.Query()
	preparestr := query.Get(cmn.URLParamPrepare)
	if prepare, err = cmn.ParseBool(preparestr); err != nil {
		t.invalmsghdlrf(w, r, "Failed to parse %s URL Parameter: %v", cmn.URLParamPrepare, err)
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
		t.invalmsghdlr(w, r, err.Error())
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
		rst := getstorstatsrunner()
		ws := rst.GetWhatStats()
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
		tstats := getstorstatsrunner()

		var rebStats *stats.RebalanceTargetStats
		if entry := registry.Registry.GetLatest(registry.XactFilter{Kind: cmn.ActRebalance}); entry != nil {
			if xact := entry.Get(); xact != nil {
				var ok bool
				rebStats, ok = xact.Stats().(*stats.RebalanceTargetStats)
				cmn.Assert(ok)
			}
		}
		msg := &stats.DaemonStatus{
			Snode:       t.httprunner.si,
			SmapVersion: t.owner.smap.get().Version,
			SysInfo:     sys.FetchSysInfo(),
			Stats:       tstats.Core,
			Capacity:    tstats.MPCap,
			TStatus:     &stats.TargetStatus{RebalanceStats: rebStats},
		}
		t.writeJSON(w, r, msg, httpdaeWhat)
	case cmn.GetWhatDiskStats:
		diskStats := fs.GetSelectedDiskStats()
		t.writeJSON(w, r, diskStats, httpdaeWhat)
	case cmn.GetWhatRemoteAIS:
		conf, ok := cmn.GCO.Get().Cloud.ProviderConf(cmn.ProviderAIS)
		if !ok {
			t.writeJSON(w, r, cmn.CloudInfoAIS{}, httpdaeWhat)
			return
		}
		clusterConf, ok := conf.(cmn.CloudConfAIS)
		cmn.Assert(ok)
		aisCloud := t.cloud[cmn.ProviderAIS].(*cloud.AisCloudProvider)
		t.writeJSON(w, r, aisCloud.GetInfo(clusterConf), httpdaeWhat)
	default:
		t.httprunner.httpdaeget(w, r)
	}
}

// register target
// enable/disable mountpath
func (t *targetrunner) httpdaepost(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Daemon)
	if err != nil {
		return
	}

	if len(apiItems) > 0 {
		switch apiItems[0] {
		case cmn.UserRegister:
			gettargetkeepalive().keepalive.send(kaRegisterMsg)
			body, err := cmn.ReadBytes(r)
			if err != nil {
				t.invalmsghdlr(w, r, err.Error())
				return
			}
			caller := r.Header.Get(cmn.HeaderCallerName)
			if err := t.applyRegMeta(body, caller); err != nil {
				t.invalmsghdlr(w, r, err.Error())
			}
			return
		case cmn.Mountpaths:
			t.handleMountpathReq(w, r)
			return
		default:
			t.invalmsghdlr(w, r, "unrecognized path in /daemon POST")
			return
		}
	}

	if status, err := t.joinCluster(); err != nil {
		t.invalmsghdlrf(w, r, "%s: failed to register, status %d, err: %v", t.si, status, err)
		return
	}

	if glog.FastV(3, glog.SmoduleAIS) {
		glog.Infof("Registered %s(self)", t.si)
	}
}

// unregister
// remove mountpath
func (t *targetrunner) httpdaedelete(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Daemon)
	if err != nil {
		return
	}

	switch apiItems[0] {
	case cmn.Mountpaths:
		t.handleMountpathReq(w, r)
		return
	case cmn.Unregister:
		t.handleUnregisterReq()
		return
	default:
		t.invalmsghdlrf(w, r, "unrecognized path: %q in /daemon DELETE", apiItems[0])
		return
	}
}

func (t *targetrunner) handleUnregisterReq() {
	if glog.V(3) {
		glog.Infoln("sending unregister on target keepalive control channel")
	}

	// Stop keepaliving
	gettargetkeepalive().keepalive.send(kaUnregisterMsg)

	// Abort all dSort jobs
	dsort.Managers.AbortAll(errors.New("target was removed from the cluster"))

	// Stop all xactions
	registry.Registry.AbortAll()
}

func (t *targetrunner) handleMountpathReq(w http.ResponseWriter, r *http.Request) {
	msg := cmn.ActionMsg{}
	if cmn.ReadJSON(w, r, &msg) != nil {
		return
	}

	mountpath, ok := msg.Value.(string)
	if !ok {
		t.invalmsghdlr(w, r, "Invalid value in request")
		return
	}

	if mountpath == "" {
		t.invalmsghdlr(w, r, "Mountpath is not defined")
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
		t.invalmsghdlrf(w, r, fmtUnknownAct, msg)
	}
}

func (t *targetrunner) handleEnableMountpathReq(w http.ResponseWriter, r *http.Request, mountpath string) {
	enabled, err := t.fsprg.enableMountpath(mountpath)
	if err != nil {
		if _, ok := err.(cmn.NoMountpathError); ok {
			t.invalmsghdlr(w, r, err.Error(), http.StatusNotFound)
		} else {
			// cmn.InvalidMountpathError
			t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
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
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	// TODO: Currently we must abort on enabling mountpath. In some places we open
	// files directly and put them directly on mountpaths. This can lead to
	// problems where we get from new mountpath without asking other (old)
	// mountpaths if they have it (resilver has not yet finished).
	dsort.Managers.AbortAll(fmt.Errorf("mountpath %q has been enabled during %s job - aborting due to possible errors", mountpath, cmn.DSortName))
}

func (t *targetrunner) handleDisableMountpathReq(w http.ResponseWriter, r *http.Request, mountpath string) {
	disabled, err := t.fsprg.disableMountpath(mountpath)
	if err != nil {
		if _, ok := err.(*cmn.NoMountpathError); ok {
			t.invalmsghdlr(w, r, err.Error(), http.StatusNotFound)
		} else {
			// cmn.InvalidMountpathError
			t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		}
		return
	}

	if !disabled {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	dsort.Managers.AbortAll(fmt.Errorf("mountpath %q has been disabled and is unusable", mountpath))
}

func (t *targetrunner) handleAddMountpathReq(w http.ResponseWriter, r *http.Request, mountpath string) {
	err := t.fsprg.addMountpath(mountpath)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
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
		t.invalmsghdlr(w, r, err.Error())
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
		t.invalmsghdlrf(w, r, "Could not remove mountpath, error: %s", err.Error())
		return
	}

	dsort.Managers.AbortAll(fmt.Errorf("mountpath %q has been removed and is unusable", mountpath))
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
	const (
		downgrade = "attempt to downgrade"
		failed    = "failed to receive BMD"
	)
	var (
		curVer                  int64
		call, act               string
		createErrs, destroyErrs string
		bmd                     = t.owner.bmd.get()
	)
	if caller != "" {
		call = ", caller " + caller
	}
	if msg.Action != "" {
		act = ", action " + msg.Action
	}
	glog.Infof("%s: %s cur=%s, new=%s%s%s", t.si, tag, bmd, newBMD, act, call)

	t.owner.bmd.Lock()
	bmd = t.owner.bmd.get()
	curVer = bmd.version()
	var (
		bcksToDelete = make([]*cluster.Bck, 0, 4)
		_, psi       = t.getPrimaryURLAndSI()
	)
	if err = bmd.validateUUID(newBMD, t.si, psi, ""); err != nil {
		t.owner.bmd.Unlock()
		cmn.ExitLogf("%v", err) // FATAL: cluster integrity error (cie)
		return
	}
	if newBMD.version() <= curVer {
		t.owner.bmd.Unlock()
		if newBMD.version() < curVer {
			err = fmt.Errorf("%s: %s %s to %s", t.si, downgrade, bmd.StringEx(), newBMD.StringEx())
			glog.Error(err)
		}
		return
	}

	// create buckets dirs under lock
	newBMD.Range(nil, nil, func(bck *cluster.Bck) bool {
		if _, present := bmd.Get(bck); present {
			return false
		}
		errs := fs.CreateBuckets("recv-bmd-"+msg.Action, bck.Bck)
		for _, err := range errs {
			createErrs += "[" + err.Error() + "]"
		}
		return false
	})

	// NOTE: create-dir errors are _not_ ignored
	if createErrs != "" {
		t.owner.bmd.Unlock()
		glog.Errorf("%s: %s - create err: %s", t.si, failed, createErrs)
		err = errors.New(createErrs)
		return
	}

	// accept the new one
	t.owner.bmd.put(newBMD)

	// delete buckets dirs under lock, ignore errors
	bmd.Range(nil, nil, func(obck *cluster.Bck) bool {
		var present bool
		newBMD.Range(nil, nil, func(nbck *cluster.Bck) bool {
			if !obck.Equal(nbck, false /*ignore BID*/, false /* ignore backend */) {
				return false
			}
			present = true
			if obck.Props.Mirror.Enabled && !nbck.Props.Mirror.Enabled {
				registry.Registry.DoAbort(cmn.ActPutCopies, nbck)
				// NOTE: cmn.ActMakeNCopies takes care of itself
			}
			if obck.Props.EC.Enabled && !nbck.Props.EC.Enabled {
				registry.Registry.DoAbort(cmn.ActECEncode, nbck)
			}
			return true
		})
		if !present {
			bcksToDelete = append(bcksToDelete, obck)
			// TODO: revisit error handling
			if err := fs.DestroyBuckets("recv-bmd-"+msg.Action, obck.Bck); err != nil {
				destroyErrs = err.Error()
			}
		}
		return false
	})

	t.owner.bmd.Unlock()

	if destroyErrs != "" {
		// TODO: revisit error handling
		glog.Errorf("%s: %s - destroy err: %s", t.si, failed, destroyErrs)
	}

	// evict LOM cache
	if len(bcksToDelete) > 0 {
		registry.Registry.AbortAllBuckets(bcksToDelete...)
		go func(bcks ...*cluster.Bck) {
			for _, b := range bcks {
				cluster.EvictLomCache(b)
			}
		}(bcksToDelete...)
	}
	if tag != bucketMDRegister {
		// ecmanager will get updated BMD upon its init()
		ec.ECM.BucketsMDChanged()
	}

	// refresh used/avail capacity and run LRU if need be (in part, to remove $trash)
	if _, err := fs.RefreshCapStatus(nil, nil); err != nil {
		go t.RunLRU("" /*uuid*/, false)
	}

	return
}

func (t *targetrunner) receiveSmap(newSmap *smapX, msg *aisMsg, caller string) (err error) {
	var (
		s, from string
	)
	if caller != "" {
		from = " from " + caller
	}
	// proxy => target control protocol (see p.httpclupost)
	if msg.Action != "" {
		s = ", action " + msg.Action
	}
	glog.Infof("%s: receive %s%s, primary %s%s", t.si, newSmap.StringEx(), from, newSmap.Primary, s)
	if !newSmap.isPresent(t.si) {
		err = fmt.Errorf("%s: not finding self in the new %s", t.si, newSmap.StringEx())
		glog.Warningf("Error: %s\n%s", err, newSmap.pp())
		return
	}
	if err = t.owner.smap.synchronize(newSmap, true /* lesserIsErr */); err != nil {
		return
	}
	return
}

func (t *targetrunner) receiveRMD(newRMD *rebMD, msg *aisMsg, caller string) (err error) {
	var s string
	if caller != "" {
		s += "; from: " + caller
	}
	// proxy => target control protocol (see p.httpclupost)
	if msg.Action != "" {
		s += "; action: " + msg.Action
	}
	glog.Infof("%s: receive %s%s", t.si, newRMD.String(), s)

	t.owner.rmd.Lock()
	defer t.owner.rmd.Unlock()

	rmd := t.owner.rmd.get()
	if newRMD.Version < rmd.Version {
		return fmt.Errorf("attempt to downgrade local RMD v%d to v%d", rmd.Version, newRMD.Version)
	} else if newRMD.Version == rmd.Version {
		return
	}

	smap := t.owner.smap.Get()
	notif := &xaction.NotifXact{
		NotifBase: nl.NotifBase{
			When: cluster.UponTerm,
			Ty:   cmn.NotifXact,
			Dsts: []string{equalIC},
			F:    t.callerNotifyFin,
		},
	}
	if msg.Action == cmn.ActRebalance { // manual (triggered by user)
		glog.Infof("%s: manual rebalance (version: %d)", t.si, newRMD.version())
		go t.rebManager.RunRebalance(smap, newRMD.Version, notif)
		return
	}

	glog.Infof(
		"%s: rebalance (version: %d; new_targets: %v; resilver: %t)",
		t.si, newRMD.version(), newRMD.TargetIDs, newRMD.Resilver,
	)
	go t.rebManager.RunRebalance(smap, newRMD.Version, notif)
	if newRMD.Resilver {
		go t.runResilver("", true /*skipGlobMisplaced*/)
	}
	return
}

func (t *targetrunner) ensureLatestSmap(msg *aisMsg, r *http.Request) {
	smap, smapVersion := t.owner.smap.Get(), msg.SmapVersion
	if smap.Version < smapVersion {
		glog.Errorf("%s: own %s < v%d - fetching latest for %v", t.si, smap, smapVersion, msg.Action)
		t.smapVersionFixup(r)
	} else if smap.Version > smapVersion {
		// if metasync outraces the request, we end up here, just log it and continue
		glog.Errorf("%s: own %s > v%d - encountered during %v", t.si, smap, smapVersion, msg.Action)
		t.statsT.Add(stats.ErrMetadataCount, 1)
	}
}

// create local directories to test multiple fspaths
func (t *targetrunner) testCachepathMounts() {
	config := cmn.GCO.Get()
	var instpath string
	if config.TestFSP.Instance > 0 {
		instpath = filepath.Join(config.TestFSP.Root, strconv.Itoa(config.TestFSP.Instance))
	} else {
		// container, VM, etc.
		instpath = config.TestFSP.Root
	}
	for i := 0; i < config.TestFSP.Count; i++ {
		mpath := filepath.Join(instpath, strconv.Itoa(i+1))
		if err := cmn.CreateDir(mpath); err != nil {
			cmn.ExitLogf("Cannot create test cache dir %q, err: %s", mpath, err)
		}

		err := fs.Add(mpath)
		cmn.AssertNoErr(err)
	}
}

func (t *targetrunner) detectMpathChanges() {
	const mountpathFname = "mpaths"
	mpathconfigfqn := filepath.Join(cmn.GCO.Get().Confdir, mountpathFname)

	type mfs struct {
		Available cmn.StringSet `json:"available"`
		Disabled  cmn.StringSet `json:"disabled"`
	}

	// load old/prev and compare
	var (
		oldfs = mfs{}
		newfs = mfs{
			Available: make(cmn.StringSet),
			Disabled:  make(cmn.StringSet),
		}
	)

	availablePaths, disabledPath := fs.Get()
	for mpath := range availablePaths {
		newfs.Available[mpath] = struct{}{}
	}
	for mpath := range disabledPath {
		newfs.Disabled[mpath] = struct{}{}
	}

	if err := jsp.Load(mpathconfigfqn, &oldfs, jsp.Plain()); err != nil {
		if !os.IsNotExist(err) && err != io.EOF {
			glog.Errorf("Failed to load old mpath config %q, err: %v", mpathconfigfqn, err)
		}
		return
	}

	if !reflect.DeepEqual(oldfs, newfs) {
		oldfsPprint := fmt.Sprintf(
			"available: %v\ndisabled: %v",
			oldfs.Available.String(), oldfs.Disabled.String(),
		)
		newfsPprint := fmt.Sprintf(
			"available: %v\ndisabled: %v",
			newfs.Available.String(), newfs.Disabled.String(),
		)

		glog.Errorf("%s: detected change in the mountpath configuration at %s", t.si, mpathconfigfqn)
		glog.Errorln("OLD: ====================")
		glog.Errorln(oldfsPprint)
		glog.Errorln("NEW: ====================")
		glog.Errorln(newfsPprint)
	}
	// persist
	if err := jsp.Save(mpathconfigfqn, newfs, jsp.Plain()); err != nil {
		glog.Errorf("Error writing config file: %v", err)
	}
}

func (t *targetrunner) fetchPrimaryMD(what string, outStruct interface{}, renamed string) (err error) {
	smap := t.owner.smap.get()
	if smap == nil || !smap.isValid() {
		return fmt.Errorf("%s: Smap is nil or invalid", t.si)
	}
	psi := smap.Primary
	q := url.Values{}
	q.Add(cmn.URLParamWhat, what)
	if renamed != "" {
		q.Add(whatRenamedLB, renamed)
	}
	path := cmn.JoinWords(cmn.Version, cmn.Daemon)
	url := psi.URL(cmn.NetworkIntraControl)
	timeout := cmn.GCO.Get().Timeout.CplaneOperation
	args := callArgs{
		si:      psi,
		req:     cmn.ReqArgs{Method: http.MethodGet, Base: url, Path: path, Query: q},
		timeout: timeout,
	}
	res := t.call(args)
	if res.err != nil {
		time.Sleep(timeout / 2)
		res = t.call(args)
		if res.err != nil {
			return fmt.Errorf("%s: failed to GET(%q), err: %v", t.si, what, res.err)
		}
	}
	err = jsoniter.Unmarshal(res.bytes, outStruct)
	if err != nil {
		return fmt.Errorf("%s: unexpected: failed to unmarshal GET(%q) response: %v", t.si, what, err)
	}

	return nil
}

func (t *targetrunner) smapVersionFixup(r *http.Request) {
	var (
		newSmap = &smapX{}
		caller  string
	)
	err := t.fetchPrimaryMD(cmn.GetWhatSmap, newSmap, "")
	if err != nil {
		glog.Error(err)
		return
	}
	var msg = t.newAisMsgStr("get-what="+cmn.GetWhatSmap, newSmap, nil)
	if r != nil {
		caller = r.Header.Get(cmn.HeaderCallerName)
	}
	if err := t.receiveSmap(newSmap, msg, caller); err != nil {
		glog.Error(err)
	}
}

func (t *targetrunner) BMDVersionFixup(r *http.Request, bck cmn.Bck, sleep bool) {
	var (
		caller      string
		newBucketMD = &bucketMD{}
	)
	if sleep {
		time.Sleep(200 * time.Millisecond)
	}
	if err := t.fetchPrimaryMD(cmn.GetWhatBMD, newBucketMD, bck.Name); err != nil {
		glog.Error(err)
		return
	}
	msg := t.newAisMsgStr("get-what="+cmn.GetWhatBMD, nil, newBucketMD)
	if r != nil {
		caller = r.Header.Get(cmn.HeaderCallerName)
	}
	if err := t.receiveBMD(newBucketMD, msg, bucketMDFixup, caller); err != nil {
		glog.Error(err)
	}
}

// handler for: /v1/tokens
func (t *targetrunner) tokenHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodDelete:
		t.httpTokenDelete(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /tokens path")
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
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /metasync path")
	}
}

// PUT /v1/metasync
func (t *targetrunner) metasyncHandlerPut(w http.ResponseWriter, r *http.Request) {
	var payload = make(msPayload)
	if err := jsp.Decode(r.Body, &payload, jspMetasyncOpts, "metasync put"); err != nil {
		cmn.InvalidHandlerDetailed(w, r, err.Error())
		return
	}

	var (
		errs   []error
		caller = r.Header.Get(cmn.HeaderCallerName)
	)

	newSmap, msgSmap, err := t.extractSmap(payload, caller)
	if err != nil {
		errs = append(errs, err)
	} else if newSmap != nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("new %s from %s", newSmap.StringEx(), caller)
		}
		if err := t.receiveSmap(newSmap, msgSmap, caller); err != nil {
			errs = append(errs, err)
		}
	}

	newRMD, msgRMD, err := t.extractRMD(payload)
	if err != nil {
		errs = append(errs, err)
	} else if newRMD != nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("new %s from %s", newRMD.String(), caller)
		}
		if err := t.receiveRMD(newRMD, msgRMD, caller); err != nil {
			errs = append(errs, err)
		}
	}

	newBMD, msgBMD, err := t.extractBMD(payload)
	if err != nil {
		errs = append(errs, err)
	} else if newBMD != nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("new %s from %s", newBMD.StringEx(), caller)
		}
		if err := t.receiveBMD(newBMD, msgBMD, bucketMDReceive, caller); err != nil {
			errs = append(errs, err)
		}
	}

	revokedTokens, err := t.extractRevokedTokenList(payload)
	if err != nil {
		errs = append(errs, err)
	} else {
		t.authn.updateRevokedList(revokedTokens)
	}

	if len(errs) > 0 {
		t.invalmsghdlrf(w, r, "%v", errs)
		return
	}
}

// POST /v1/metasync
func (t *targetrunner) metasyncHandlerPost(w http.ResponseWriter, r *http.Request) {
	var payload = make(msPayload)
	if err := jsp.Decode(r.Body, &payload, jspMetasyncOpts, "metasync post"); err != nil {
		cmn.InvalidHandlerDetailed(w, r, err.Error())
		return
	}
	caller := r.Header.Get(cmn.HeaderCallerName)
	newSmap, msg, err := t.extractSmap(payload, caller)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	if newSmap != nil && msg.Action == cmn.ActStartGFN {
		t.gfn.global.activateTimed()
	}
}

// GET /v1/health (cmn.Health)
// TODO: clusterInfo - see p.healthHandler
func (t *targetrunner) healthHandler(w http.ResponseWriter, r *http.Request) {
	var (
		callerID = r.Header.Get(cmn.HeaderCallerID)
		caller   = r.Header.Get(cmn.HeaderCallerName)
		smap     = t.owner.smap.get()
		query    = r.URL.Query()
	)

	// external (i.e. not intra-cluster) call
	if callerID == "" && caller == "" {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s: external health-ping from %s", t.si, r.RemoteAddr)
		}
		if !t.ClusterStarted() {
			// respond with 503 as per https://tools.ietf.org/html/rfc7231#section-6.6.4
			// see also:
			// https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		return
	} else if callerID == "" || caller == "" {
		t.invalmsghdlrf(w, r, "%s: health-ping missing(%s, %s)", t.si, callerID, caller)
		return
	}

	if !smap.containsID(callerID) {
		glog.Warningf("%s: health-ping from a not-yet-registered (%s, %s)", t.si, callerID, caller)
	}

	callerSmapVer, _ := strconv.ParseInt(r.Header.Get(cmn.HeaderCallerSmapVersion), 10, 64)
	if smap.version() != callerSmapVer {
		glog.Warningf(
			"%s: health-ping from node (%s, %s) which has different smap version: our (%d), node's (%d)",
			t.si, callerID, caller, smap.version(), callerSmapVer,
		)
	}

	getRebStatus := cmn.IsParseBool(query.Get(cmn.URLParamRebStatus))
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
	if _, err := t.checkRESTItems(w, r, 0, false, cmn.Version, cmn.Tokens); err != nil {
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
	if err := t.withRetry(t.unregister, "unregister", false /* backoff */); err != nil {
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
	if err := t.withRetry(t.joinCluster, "register", false /* backoff */); err != nil {
		return err
	}
	t.regstate.disabled = false
	glog.Infof("%s has been enabled", t.si)
	return nil
}

// lookupRemoteSingle sends the message to the given target to see if it has the specific object.
func (t *targetrunner) LookupRemoteSingle(lom *cluster.LOM, tsi *cluster.Snode) (ok bool) {
	header := make(http.Header)
	header.Add(cmn.HeaderCallerID, t.Snode().ID())
	query := make(url.Values)
	query.Add(cmn.URLParamSilent, "true")
	args := callArgs{
		si: tsi,
		req: cmn.ReqArgs{
			Method: http.MethodHead,
			Header: header,
			Base:   tsi.URL(cmn.NetworkIntraControl),
			Path:   cmn.JoinWords(cmn.Version, cmn.Objects, lom.BckName(), lom.ObjName),
			Query:  query,
		},
		timeout: lom.Config().Timeout.CplaneOperation,
	}
	res := t.call(args)
	ok = res.err == nil
	return
}

// lookupRemoteAll sends the broadcast message to all targets to see if they
// have the specific object.
func (t *targetrunner) lookupRemoteAll(lom *cluster.LOM, smap *smapX) *cluster.Snode {
	header := make(http.Header)
	header.Add(cmn.HeaderCallerID, t.Snode().ID())
	query := make(url.Values)
	query.Add(cmn.URLParamSilent, "true")
	query.Add(cmn.URLParamCheckExistsAny, "true") // lookup all mountpaths _and_ copy if misplaced
	res := t.bcastToGroup(bcastArgs{
		req: cmn.ReqArgs{
			Method: http.MethodHead,
			Header: header,
			Path:   cmn.JoinWords(cmn.Version, cmn.Objects, lom.BckName(), lom.ObjName),
			Query:  query,
		},
		smap: smap,
	})
	for r := range res {
		if r.err == nil {
			return r.si
		}
	}
	return nil
}
