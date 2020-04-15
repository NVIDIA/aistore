// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
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
	"strings"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/ais/cloud"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/reb"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction"
	jsoniter "github.com/json-iterator/go"
)

func (t *targetrunner) initHostIP() {
	var hostIP string
	if hostIP = os.Getenv("AIS_HOSTIP"); hostIP == "" {
		return
	}
	var (
		config  = cmn.GCO.Get()
		port    = config.Net.L4.Port
		extAddr = net.ParseIP(hostIP)
		extPort = port
	)
	if portStr := os.Getenv("AIS_TARGET_HOSTPORT"); portStr != "" {
		portNum, err := strconv.Atoi(portStr)
		cmn.AssertNoErr(err)
		cmn.Assert(portNum >= 0)
		if portNum > 0 {
			extPort = portNum
		}
	}
	t.si.PublicNet.NodeIPAddr = extAddr.String()
	t.si.PublicNet.DaemonPort = strconv.Itoa(extPort)
	t.si.PublicNet.DirectURL = fmt.Sprintf("%s://%s:%d", config.Net.HTTP.Proto, extAddr.String(), extPort)
	glog.Infof("AIS_HOSTIP[Public Network]: %s[%s]", hostIP, t.si.URL(cmn.NetworkPublic))
}

// register (join | keepalive) with primary
func (t *targetrunner) register(keepalive bool, timeout time.Duration) (primaryURL string, status int, err error) {
	var (
		res      callResult
		url, psi = t.getPrimaryURLAndSI()
	)
	if !keepalive {
		primaryURL, res = t.join(nil)
	} else { // keepalive
		res = t.registerToURL(url, psi, timeout, nil, keepalive)
		primaryURL = url
	}
	if res.err != nil {
		if strings.Contains(res.err.Error(), ciePrefix) {
			cmn.ExitLogf("%v", res.err) // FATAL: cluster integrity error (cie)
		}
		return primaryURL, res.status, res.err
	}
	// not being sent at cluster startup and keepalive
	if len(res.outjson) == 0 {
		return
	}
	cmn.Assert(!keepalive)
	err = t.applyRegMeta(res.outjson, "")
	return
}

func (t *targetrunner) applyRegMeta(body []byte, caller string) (err error) {
	var meta targetRegMeta
	err = jsoniter.Unmarshal(body, &meta)
	cmn.AssertNoErr(err)

	// There's a window of time between:
	// a) target joining existing cluster and b) cluster starting to rebalance itself
	// The latter is driven by metasync (see metasync.go) distributing updated cluster map.
	// To handle incoming GETs within this window (which would typically take a few seconds or less)
	// we need to have the current cluster-wide metadata and the temporary gfn state:
	t.gfn.global.activateTimed()

	// BMD
	msg := t.newAisMsgStr(cmn.ActRegTarget, meta.Smap, meta.BMD)
	if err = t.receiveBMD(meta.BMD, msg, bucketMDRegister, caller); err != nil {
		glog.Infof("%s: %s", t.si, t.owner.bmd.get())
	}
	// Smap
	if err := t.owner.smap.synchronize(meta.Smap, true /* lesserIsErr */); err != nil {
		glog.Errorf("%s: sync Smap err %v", t.si, err)
	} else {
		glog.Infof("%s: sync %s", t.si, t.owner.smap.get())
	}
	return
}

func (t *targetrunner) unregister() (int, error) {
	smap := t.owner.smap.get()
	if smap == nil || !smap.isValid() {
		return 0, nil
	}
	args := callArgs{
		si: smap.ProxySI,
		req: cmn.ReqArgs{
			Method: http.MethodDelete,
			Path:   cmn.URLPath(cmn.Version, cmn.Cluster, cmn.Daemon, t.si.ID()),
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
	apitems, err := t.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Daemon)
	if err != nil {
		return
	}
	if len(apitems) == 0 {
		t.daeputJSON(w, r)
	} else {
		t.daeputQuery(w, r, apitems)
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
			t.invalmsghdlr(w, r, fmt.Sprintf("failed to parse action '%s' value: (%v, %T) not a string",
				cmn.ActSetConfig, msg.Value, msg.Value))
			return
		}
		kvs := cmn.NewSimpleKVs(cmn.SimpleKVsEntry{Key: msg.Name, Value: value})
		if err := t.setConfig(kvs); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
	case cmn.ActShutdown:
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	default:
		s := fmt.Sprintf(fmtUnknownAct, msg)
		t.invalmsghdlr(w, r, s)
	}
}

func (t *targetrunner) daeputQuery(w http.ResponseWriter, r *http.Request, apitems []string) {
	switch apitems[0] {
	case cmn.Proxy:
		// PUT /v1/daemon/proxy/newprimaryproxyid
		t.httpdaesetprimaryproxy(w, r, apitems)
	case cmn.SyncSmap:
		var newsmap = &smapX{}
		if cmn.ReadJSON(w, r, newsmap) != nil {
			return
		}
		if err := t.owner.smap.synchronize(newsmap, true /* lesserIsErr */); err != nil {
			t.invalmsghdlr(w, r, fmt.Sprintf("failed to sync Smap: %s", err))
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
			action = apitems[0]
		)
		if what != cmn.GetWhatRemoteAIS {
			t.invalmsghdlr(w, r, fmt.Sprintf(fmtUnknownQue, what))
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
		if err := t.cloud.ais.Apply(aisConf, action); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
	}
}

func (t *targetrunner) setConfig(kvs cmn.SimpleKVs) (err error) {
	prevConfig := cmn.GCO.Get()
	if err := jsp.SetConfigMany(kvs); err != nil {
		return err
	}

	config := cmn.GCO.Get()
	if prevConfig.LRU.Enabled && !config.LRU.Enabled {
		lruRunning := xaction.Registry.IsXactRunning(xaction.XactQuery{Kind: cmn.ActLRU})
		if lruRunning {
			glog.V(3).Infof("Aborting LRU due to lru.enabled config change")
			xaction.Registry.DoAbort(cmn.ActLRU, nil)
		}
	}
	return
}

func (t *targetrunner) httpdaesetprimaryproxy(w http.ResponseWriter, r *http.Request, apiItems []string) {
	var (
		err     error
		prepare bool
	)
	if len(apiItems) != 2 {
		s := fmt.Sprintf("Incorrect number of API items: %d, should be: %d", len(apiItems), 2)
		t.invalmsghdlr(w, r, s)
		return
	}

	proxyID := apiItems[1]
	query := r.URL.Query()
	preparestr := query.Get(cmn.URLParamPrepare)
	if prepare, err = cmn.ParseBool(preparestr); err != nil {
		s := fmt.Sprintf("Failed to parse %s URL Parameter: %v", cmn.URLParamPrepare, err)
		t.invalmsghdlr(w, r, s)
		return
	}

	smap := t.owner.smap.get()
	psi := smap.GetProxy(proxyID)
	if psi == nil {
		t.invalmsghdlr(w, r, fmt.Sprintf("new primary proxy %s not present in the local %s", proxyID, smap.pp()))
		return
	}

	if prepare {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Info("Preparation step: do nothing")
		}
		return
	}

	err = t.owner.smap.modify(func(clone *smapX) error {
		if clone.ProxySI.ID() != psi.ID() {
			clone.ProxySI = psi
		}
		return nil
	})
	cmn.AssertNoErr(err)
}

func (t *targetrunner) httpdaeget(w http.ResponseWriter, r *http.Request) {
	getWhat := r.URL.Query().Get(cmn.URLParamWhat)
	httpdaeWhat := "httpdaeget-" + getWhat
	switch getWhat {
	case cmn.GetWhatConfig, cmn.GetWhatSmap, cmn.GetWhatBMD, cmn.GetWhatSmapVote, cmn.GetWhatSnode:
		t.httprunner.httpdaeget(w, r)
	case cmn.GetWhatSysInfo:
		body := cmn.MustMarshal(cmn.TSysInfo{SysInfo: daemon.gmm.FetchSysInfo(), FSInfo: fs.Mountpaths.FetchFSInfo()})
		t.writeJSON(w, r, body, httpdaeWhat)
	case cmn.GetWhatStats:
		rst := getstorstatsrunner()
		body := rst.GetWhatStats()
		t.writeJSON(w, r, body, httpdaeWhat)
	case cmn.GetWhatMountpaths:
		mpList := cmn.MountpathList{}
		availablePaths, disabledPaths := fs.Mountpaths.Get()
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
		body := cmn.MustMarshal(&mpList)
		t.writeJSON(w, r, body, httpdaeWhat)
	case cmn.GetWhatDaemonStatus:
		tstats := getstorstatsrunner()

		var rebStats *stats.RebalanceTargetStats
		if entry := xaction.Registry.GetLatest(xaction.XactQuery{Kind: cmn.ActRebalance}); entry != nil {
			if xact := entry.Get(); xact != nil {
				var ok bool
				rebStats, ok = entry.Stats(xact).(*stats.RebalanceTargetStats)
				cmn.Assert(ok)
			}
		}
		msg := &stats.DaemonStatus{
			Snode:       t.httprunner.si,
			SmapVersion: t.owner.smap.get().Version,
			SysInfo:     daemon.gmm.FetchSysInfo(),
			Stats:       tstats.Core,
			Capacity:    tstats.Capacity,
			TStatus:     &stats.TargetStatus{RebalanceStats: rebStats},
		}
		body := cmn.MustMarshal(msg)
		t.writeJSON(w, r, body, httpdaeWhat)
	case cmn.GetWhatDiskStats:
		diskStats := fs.Mountpaths.GetSelectedDiskStats()
		body := cmn.MustMarshal(diskStats)
		t.writeJSON(w, r, body, httpdaeWhat)
	case cmn.GetWhatRemoteAIS:
		conf, ok := cmn.GCO.Get().Cloud.ProviderConf(cmn.ProviderAIS)
		if !ok {
			body := cmn.MustMarshal(cmn.CloudInfoAIS{})
			t.writeJSON(w, r, body, httpdaeWhat)
			return
		}
		clusterConf, ok := conf.(cmn.CloudConfAIS)
		cmn.Assert(ok)
		aisCloudInfo := t.cloud.ais.GetInfo(clusterConf)
		body := cmn.MustMarshal(aisCloudInfo)
		t.writeJSON(w, r, body, httpdaeWhat)
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

	if _, status, err := t.register(false, cmn.DefaultTimeout); err != nil {
		s := fmt.Sprintf("%s: failed to register, status %d, err: %v", t.si, status, err)
		t.invalmsghdlr(w, r, s)
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
		t.invalmsghdlr(w, r, fmt.Sprintf("unrecognized path: %q in /daemon DELETE", apiItems[0]))
		return
	}
}

// Decrypts token and retrieves userID from request header
// Returns empty userID in case of token is invalid
func (t *targetrunner) userFromRequest(header http.Header) (*authRec, error) {
	token := ""
	tokenParts := strings.SplitN(header.Get(cmn.HeaderAuthorization), " ", 2)
	if len(tokenParts) == 2 && tokenParts[0] == cmn.HeaderBearer {
		token = tokenParts[1]
	}

	if token == "" {
		// no token in header = use default credentials
		return nil, nil
	}

	authrec, err := t.authn.validateToken(token)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt token [%s]: %v", token, err)
	}

	return authrec, nil
}

// If Authn server is enabled then the function tries to read a user credentials
// (at this moment userID is enough) from HTTP request header: looks for
// 'Authorization' header and decrypts it.
// Extracted user information is put to context that is passed to all consumers
func (t *targetrunner) contextWithAuth(header http.Header) context.Context {
	ct := context.Background()
	config := cmn.GCO.Get()

	if config.Auth.CredDir == "" || !config.Auth.Enabled {
		return ct
	}

	user, err := t.userFromRequest(header)
	if err != nil {
		glog.Errorf("Failed to extract token: %v", err)
		return ct
	}

	if user != nil {
		ct = context.WithValue(ct, cloud.CtxUserID, user.userID)
		ct = context.WithValue(ct, cloud.CtxCredsDir, config.Auth.CredDir)
		ct = context.WithValue(ct, cloud.CtxUserCreds, user.creds)
	}

	return ct
}

func (t *targetrunner) handleUnregisterReq() {
	if glog.V(3) {
		glog.Infoln("Sending unregister on target keepalive control channel")
	}

	// Stop keepaliving
	gettargetkeepalive().keepalive.send(kaUnregisterMsg)

	// Abort all dSort jobs
	dsort.Managers.AbortAll(errors.New("target was removed from the cluster"))

	// Stop all xactions
	xaction.Registry.AbortAll()
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
		s := fmt.Sprintf(fmtUnknownAct, msg)
		t.invalmsghdlr(w, r, s)
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
		availablePaths, _ = fs.Mountpaths.Get()
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
		availablePaths, _ = fs.Mountpaths.Get()
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
		t.invalmsghdlr(w, r, fmt.Sprintf("Could not remove mountpath, error: %s", err.Error()))
		return
	}

	dsort.Managers.AbortAll(fmt.Errorf("mountpath %q has been removed and is unusable", mountpath))
}

func (t *targetrunner) receiveBMD(newBMD *bucketMD, msg *aisMsg, tag, caller string) (err error) {
	if msg.TxnID == "" {
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
	//
	// lock =======================
	//
	t.owner.bmd.Lock()
	bmd = t.owner.bmd.get()
	curVer = bmd.version()
	var (
		na           = bmd.NumAIS(nil /*all namespaces*/)
		bcksToDelete = make([]*cluster.Bck, 0, na)
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
		errs := fs.Mountpaths.CreateBuckets("recv-bmd-"+msg.Action, bck.Bck)
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
			if !obck.Equal(nbck, false /*ignore BID*/) {
				return false
			}
			present = true
			if obck.Props.Mirror.Enabled && !nbck.Props.Mirror.Enabled {
				xaction.Registry.DoAbort(cmn.ActPutCopies, nbck)
				// NOTE: cmn.ActMakeNCopies takes care of itself
			}
			if obck.Props.EC.Enabled && !nbck.Props.EC.Enabled {
				xaction.Registry.DoAbort(cmn.ActECEncode, nbck)
			}
			return true
		})
		if !present {
			bcksToDelete = append(bcksToDelete, obck)
			// TODO: revisit error handling
			if err := fs.Mountpaths.DestroyBuckets("recv-bmd-"+msg.Action, obck.Bck); err != nil {
				destroyErrs = err.Error()
			}
		}
		return false
	})

	t.owner.bmd.Unlock() // unlock ================

	if destroyErrs != "" {
		// TODO: revisit error handling
		glog.Errorf("%s: %s - destroy err: %s", t.si, failed, destroyErrs)
	}

	// evict LOM cache
	if len(bcksToDelete) > 0 {
		xaction.Registry.AbortAllBuckets(bcksToDelete...)
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
	glog.Infof("%s: receive %s%s, primary %s%s", t.si, newSmap.StringEx(), from, newSmap.ProxySI, s)
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
	glog.Infof("%s: receive %s%s%s", t.si, newRMD.String(), from, s)

	t.owner.rmd.Lock()
	defer t.owner.rmd.Unlock()

	rmd := t.owner.rmd.get()
	if newRMD.Version < rmd.Version {
		return fmt.Errorf("attempt to downgrade local RMD v%d to v%d", rmd.Version, newRMD.Version)
	} else if newRMD.Version == rmd.Version {
		return
	}

	smap := t.owner.smap.Get()
	if msg.Action == cmn.ActRebalance { // manual
		go t.rebManager.RunRebalance(smap, newRMD.Version)
		return
	}

	if !cmn.GCO.Get().Rebalance.Enabled {
		glog.Infoln("auto-rebalancing disabled")
		return
	}

	glog.Infof("%s receiveSmap: go rebalance(newTargetIDs=%v)", t.si, newRMD.TargetIDs)
	go t.rebManager.RunRebalance(smap, newRMD.Version)
	if newRMD.Resilver {
		go t.rebManager.RunResilver("", true /*skipGlobMisplaced*/)
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

		err := fs.Mountpaths.Add(mpath)
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

	availablePaths, disabledPath := fs.Mountpaths.Get()
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
		return errors.New("smap nil or missing")
	}
	psi := smap.ProxySI
	q := url.Values{}
	q.Add(cmn.URLParamWhat, what)
	if renamed != "" {
		q.Add(whatRenamedLB, renamed)
	}
	args := callArgs{
		si: psi,
		req: cmn.ReqArgs{
			Method: http.MethodGet,
			Base:   psi.IntraControlNet.DirectURL,
			Path:   cmn.URLPath(cmn.Version, cmn.Daemon),
			Query:  q,
		},
		timeout: cmn.GCO.Get().Timeout.CplaneOperation,
	}
	res := t.call(args)
	if res.err != nil {
		return fmt.Errorf("failed to %s=%s, err: %v", what, cmn.GetWhatBMD, res.err)
	}

	err = jsoniter.Unmarshal(res.outjson, outStruct)
	if err != nil {
		return fmt.Errorf("unexpected: failed to unmarshal %s=%s response, err: %v [%v]",
			what, psi.IntraControlNet.DirectURL, err, string(res.outjson))
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
	var caller string
	if sleep {
		time.Sleep(200 * time.Millisecond) // TODO -- FIXME: request proxy to execute syncCBmeta()
	}
	newBucketMD := &bucketMD{}
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
	if err := jsp.Decode(r.Body, &payload, jsp.CCSign(), "metasync put"); err != nil {
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
		msg := fmt.Sprintf("%v", errs)
		t.invalmsghdlr(w, r, msg)
		return
	}
}

// POST /v1/metasync
func (t *targetrunner) metasyncHandlerPost(w http.ResponseWriter, r *http.Request) {
	var payload = make(msPayload)
	if err := jsp.Decode(r.Body, &payload, jsp.CCSign(), "metasync post"); err != nil {
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
func (t *targetrunner) healthHandler(w http.ResponseWriter, r *http.Request) {
	var (
		callerID = r.Header.Get(cmn.HeaderCallerID)
		caller   = r.Header.Get(cmn.HeaderCallerName)
		smap     = t.owner.smap.get()
		query    = r.URL.Query()
	)

	if callerID == "" && caller == "" {
		// External call - eg. by user to check if target is alive or by kubernetes service.
		//
		// TODO: we most probably need to split the external API call and
		// internal one. Same goes for proxy - in proxy we also send some stats
		// which are most probably not relevant to the user as it only wants
		// to get 200 in case everything is fine.
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s: external health-ping from %s", t.si, r.RemoteAddr)
		}
		return
	} else if callerID == "" || caller == "" {
		t.invalmsghdlr(w, r, fmt.Sprintf("%s: health-ping missing(%s, %s)", t.si, callerID, caller))
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

	getRebStatus, _ := cmn.ParseBool(query.Get(cmn.URLParamRebStatus))
	if getRebStatus {
		status := &reb.Status{}
		t.rebManager.RebStatus(status)
		body := cmn.MustMarshal(status)
		if ok := t.writeJSON(w, r, body, "rebalance-status"); !ok {
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

func (t *targetrunner) pollClusterStarted(timeout time.Duration) {
	for i := 1; ; i++ {
		time.Sleep(time.Duration(i) * time.Second)
		smap := t.owner.smap.get()
		if !smap.isValid() {
			continue
		}
		psi := smap.ProxySI
		args := callArgs{
			si: psi,
			req: cmn.ReqArgs{
				Method: http.MethodGet,
				Base:   psi.IntraControlNet.DirectURL,
				Path:   cmn.URLPath(cmn.Version, cmn.Health),
			},
			timeout: timeout,
		}
		res := t.call(args)
		if res.err != nil {
			continue
		}

		if res.status == http.StatusOK {
			break
		}
	}
	t.clusterStarted.Store(true)
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
	var (
		status int
		eunreg error
	)

	t.regstate.Lock()
	defer t.regstate.Unlock()

	if t.regstate.disabled {
		return nil
	}

	glog.Infof("Disabling %s", t.si)
	for i := 0; i < maxRetryCnt; i++ {
		if status, eunreg = t.unregister(); eunreg != nil {
			if cmn.IsErrConnectionRefused(eunreg) || status == http.StatusRequestTimeout {
				glog.Errorf("%s: retrying unregistration...", t.si)
				time.Sleep(time.Second)
				continue
			}
		}
		break
	}

	if status >= http.StatusBadRequest || eunreg != nil {
		// do not update state on error
		if eunreg == nil {
			eunreg = fmt.Errorf("error unregistering %s, http status %d", t.si, status)
		}
		return eunreg
	}

	t.regstate.disabled = true
	glog.Infof("%s has been disabled (unregistered)", t.si)
	return nil
}

// registers the target again if it was disabled by and internal event
func (t *targetrunner) enable() error {
	var (
		status int
		ereg   error
	)

	t.regstate.Lock()
	defer t.regstate.Unlock()

	if !t.regstate.disabled {
		return nil
	}

	glog.Infof("Enabling %s", t.si)
	for i := 0; i < maxRetryCnt; i++ {
		if _, status, ereg = t.register(false, cmn.DefaultTimeout); ereg != nil {
			if cmn.IsErrConnectionRefused(ereg) || status == http.StatusRequestTimeout {
				glog.Errorf("%s: retrying registration...", t.si)
				time.Sleep(time.Second)
				continue
			}
		}
		break
	}

	if status >= http.StatusBadRequest || ereg != nil {
		// do not update state on error
		if ereg == nil {
			ereg = fmt.Errorf("error registering %s, http status: %d", t.si, status)
		}
		return ereg
	}

	t.regstate.disabled = false
	glog.Infof("%s has been enabled", t.si)
	return nil
}

func (t *targetrunner) beginECEncode(bck *cluster.Bck) (err error) {
	// Do not start the xaction if there is no enough drive space (OOS or High)
	capInfo := t.AvgCapUsed(cmn.GCO.Get())
	if capInfo.Err != nil {
		return capInfo.Err
	}

	// Do not start the xaction if any rebalance is running
	rbInfo := t.RebalanceInfo()
	if rbInfo.IsRebalancing {
		return fmt.Errorf("cannot start bucket %q encoding while rebalance is running", bck)
	}

	_, err = xaction.Registry.RenewECEncodeXact(t, bck, cmn.ActBegin)
	return err
}

func (t *targetrunner) commitECEncode(bckFrom *cluster.Bck) (err error) {
	var xact *ec.XactBckEncode
	xact, err = xaction.Registry.RenewECEncodeXact(t, bckFrom, cmn.ActCommit)
	if err != nil {
		glog.Error(err) // must not happen at commit time
		return err
	}
	go xact.Run()
	return nil
}

// lookupRemoteSingle sends the message to the given target to see if it has the specific object.
func (t *targetrunner) LookupRemoteSingle(lom *cluster.LOM, tsi *cluster.Snode) (ok bool) {
	query := make(url.Values)
	query.Add(cmn.URLParamSilent, "true")
	args := callArgs{
		si: tsi,
		req: cmn.ReqArgs{
			Method: http.MethodHead,
			Base:   tsi.URL(cmn.NetworkIntraControl),
			Path:   cmn.URLPath(cmn.Version, cmn.Objects, lom.BckName(), lom.ObjName),
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
	query := make(url.Values)
	query.Add(cmn.URLParamSilent, "true")
	query.Add(cmn.URLParamCheckExistsAny, "true") // lookup all mountpaths _and_ copy if misplaced
	res := t.bcastTo(bcastArgs{
		req: cmn.ReqArgs{
			Method: http.MethodHead,
			Path:   cmn.URLPath(cmn.Version, cmn.Objects, lom.BckName(), lom.ObjName),
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
