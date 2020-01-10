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

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/mirror"
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

// target registration with proxy
func (t *targetrunner) register(keepalive bool, timeout time.Duration) (status int, err error) {
	var (
		res      callResult
		meta     targetRegMeta
		tname    = t.si.Name()
		url, psi = t.getPrimaryURLAndSI()
	)
	if !keepalive {
		res = t.join(nil)
	} else { // keepalive
		res = t.registerToURL(url, psi, timeout, nil, keepalive)
	}
	if res.err != nil {
		if strings.Contains(res.err.Error(), ciePrefix) {
			cmn.ExitLogf("%v", res.err) // FATAL: cluster integrity error (cie)
		}
		return res.status, res.err
	}
	// not being sent at cluster startup and keepalive
	if len(res.outjson) == 0 {
		return
	}
	cmn.Assert(!keepalive)

	err = jsoniter.Unmarshal(res.outjson, &meta)
	cmn.AssertNoErr(err)

	// There's a window of time between:
	// a) target joining existing cluster and b) cluster starting to rebalance itself
	// The latter is driven by metasync (see metasync.go) distributing the new Smap.
	// To handle incoming GETs within this window (which would typically take a few seconds or less)
	// we need to have the current cluster-wide metadata and the temporary gfn state:
	t.gfn.global.activateTimed()

	// bmd
	msgInt := t.newActionMsgInternalStr(cmn.ActRegTarget, meta.Smap, meta.BMD)
	if err = t.receiveBucketMD(meta.BMD, msgInt, bucketMDRegister, ""); err != nil {
		glog.Errorf("%s: bad BMD, err: %v", tname, err)
		if _, incompat := err.(*errPrxBmdOriginDiffer); incompat {
			return
		}
		err = nil
	} else {
		glog.Infof("%s: %s", tname, t.bmdowner.get())
	}
	// smap
	if err := t.smapowner.synchronize(meta.Smap, true /* lesserIsErr */); err != nil {
		glog.Errorf("%s: sync Smap err %v", tname, err)
	} else {
		glog.Infof("%s: sync %s", tname, t.smapowner.get())
	}
	return
}

func (t *targetrunner) unregister() (int, error) {
	smap := t.smapowner.get()
	if smap == nil || !smap.isValid() {
		return 0, nil
	}
	args := callArgs{
		si: smap.ProxySI,
		req: cmn.ReqArgs{
			Method: http.MethodDelete,
			Path:   cmn.URLPath(cmn.Version, cmn.Cluster, cmn.Daemon, t.si.DaemonID),
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
	if len(apitems) > 0 {
		switch apitems[0] {
		// PUT /v1/daemon/proxy/newprimaryproxyid
		case cmn.Proxy:
			t.httpdaesetprimaryproxy(w, r, apitems)
			return
		case cmn.SyncSmap:
			var newsmap = &smapX{}
			if cmn.ReadJSON(w, r, newsmap) != nil {
				return
			}
			if err := t.smapowner.synchronize(newsmap, true /* lesserIsErr */); err != nil {
				t.invalmsghdlr(w, r, fmt.Sprintf("Failed to sync Smap: %s", err))
			}
			glog.Infof("%s: %s %s done", t.si.Name(), cmn.SyncSmap, newsmap)
			return
		case cmn.Mountpaths:
			t.handleMountpathReq(w, r)
			return
		case cmn.ActSetConfig: // setconfig #1 - via query parameters and "?n1=v1&n2=v2..."
			kvs := cmn.NewSimpleKVsFromQuery(r.URL.Query())
			if err := t.setConfig(kvs); err != nil {
				t.invalmsghdlr(w, r, err.Error())
				return
			}
			return
		}
	}
	//
	// other PUT /daemon actions
	//
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
			t.invalmsghdlr(w, r, fmt.Sprintf("Failed to parse action '%s' value: (%v, %T) not a string",
				cmn.ActSetConfig, msg.Value, msg.Value))
			return
		}
		kvs := cmn.NewSimpleKVs(cmn.SimpleKVsEntry{Key: msg.Name, Value: value})
		if err := t.setConfig(kvs); err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
	case cmn.ActShutdown:
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	case cmn.ActXactStart, cmn.ActXactStop:
		var (
			kind   = msg.Name
			bucket string
		)
		bucket, _ = msg.Value.(string)
		// TODO -- FIXME: enforce bucket != "" for xactionbucket kinds
		if msg.Action == cmn.ActXactStart {
			if kind == cmn.ActGlobalReb {
				// see p.httpcluput()
				t.invalmsghdlr(w, r, "Invalid API request: expecting action="+cmn.ActGlobalReb)
				return
			}
			if err := t.xactsStartRequest(kind, bucket); err != nil {
				t.invalmsghdlr(w, r, err.Error())
			}
		} else {
			xaction.Registry.DoAbort(kind, bucket)
		}
		return
	default:
		s := fmt.Sprintf(fmtUnknownAct, msg)
		t.invalmsghdlr(w, r, s)
	}
}

func (t *targetrunner) setConfig(kvs cmn.SimpleKVs) (err error) {
	prevConfig := cmn.GCO.Get()
	if err := cmn.SetConfigMany(kvs); err != nil {
		return err
	}

	config := cmn.GCO.Get()
	if prevConfig.LRU.Enabled && !config.LRU.Enabled {
		lruRunning := xaction.Registry.GlobalXactRunning(cmn.ActLRU)
		if lruRunning {
			glog.V(3).Infof("Aborting LRU due to lru.enabled config change")
			xaction.Registry.AbortGlobalXact(cmn.ActLRU)
		}
	}
	return
}

func (t *targetrunner) httpdaesetprimaryproxy(w http.ResponseWriter, r *http.Request, apitems []string) {
	var (
		err     error
		prepare bool
	)
	if len(apitems) != 2 {
		s := fmt.Sprintf("Incorrect number of API items: %d, should be: %d", len(apitems), 2)
		t.invalmsghdlr(w, r, s)
		return
	}

	proxyid := apitems[1]
	query := r.URL.Query()
	preparestr := query.Get(cmn.URLParamPrepare)
	if prepare, err = cmn.ParseBool(preparestr); err != nil {
		s := fmt.Sprintf("Failed to parse %s URL Parameter: %v", cmn.URLParamPrepare, err)
		t.invalmsghdlr(w, r, s)
		return
	}

	smap := t.smapowner.get()
	psi := smap.GetProxy(proxyid)
	if psi == nil {
		s := fmt.Sprintf("New primary proxy %s not present in the local %s", proxyid, smap.pp())
		t.invalmsghdlr(w, r, s)
		return
	}
	if prepare {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Info("Preparation step: do nothing")
		}
		return
	}
	t.smapowner.Lock()
	smap = t.smapowner.get()
	if smap.ProxySI.DaemonID != psi.DaemonID {
		clone := smap.clone()
		clone.ProxySI = psi
		if err := t.smapowner.persist(clone); err != nil {
			t.smapowner.Unlock()
			t.invalmsghdlr(w, r, err.Error())
			return
		}
		t.smapowner.put(clone)
	}
	t.smapowner.Unlock()
}

func (t *targetrunner) httpdaeget(w http.ResponseWriter, r *http.Request) {
	getWhat := r.URL.Query().Get(cmn.URLParamWhat)
	httpdaeWhat := "httpdaeget-" + getWhat
	switch getWhat {
	case cmn.GetWhatConfig, cmn.GetWhatSmap, cmn.GetWhatBMD, cmn.GetWhatSmapVote, cmn.GetWhatSnode:
		t.httprunner.httpdaeget(w, r)
	case cmn.GetWhatSysInfo:
		body := cmn.MustMarshal(cmn.TSysInfo{SysInfo: nodeCtx.mm.FetchSysInfo(), FSInfo: fs.Mountpaths.FetchFSInfo()})
		t.writeJSON(w, r, body, httpdaeWhat)
	case cmn.GetWhatStats:
		rst := getstorstatsrunner()
		body := rst.GetWhatStats()
		t.writeJSON(w, r, body, httpdaeWhat)
	case cmn.GetWhatXaction:
		var (
			body []byte
			err  error
			msg  cmn.ActionMsg
		)
		if cmn.ReadJSON(w, r, &msg) != nil {
			return
		}
		xactMsg, err := cmn.ReadXactionRequestMessage(&msg)
		if err != nil {
			t.invalmsghdlr(w, r, fmt.Sprintf("Could not parse xaction action message: %s", err.Error()), http.StatusBadRequest)
			return
		}

		kind, bucket, onlyRecent := msg.Name, xactMsg.Bucket, !xactMsg.All

		switch msg.Action {
		case cmn.ActXactStats:
			body, err = t.xactStatsRequest(kind, bucket, onlyRecent)
			if err != nil {
				if _, ok := err.(cmn.XactionNotFoundError); ok {
					t.invalmsghdlrsilent(w, r, err.Error(), http.StatusNotFound)
				} else {
					t.invalmsghdlr(w, r, err.Error())
				}
				return
			}
		default:
			t.invalmsghdlr(w, r, fmt.Sprintf("Unrecognized xaction action %s", msg.Action), http.StatusBadRequest)
			return
		}

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

		var globalRebStats *stats.RebalanceTargetStats
		entry := xaction.Registry.GetL(cmn.ActGlobalReb)
		if entry != nil {
			if xact := entry.Get(); xact != nil {
				var ok bool
				globalRebStats, ok = entry.Stats(xact).(*stats.RebalanceTargetStats)
				cmn.Assert(ok)
			}
		}

		msg := &stats.DaemonStatus{
			Snode:       t.httprunner.si,
			SmapVersion: t.smapowner.get().Version,
			SysInfo:     nodeCtx.mm.FetchSysInfo(),
			Stats:       tstats.Core,
			Capacity:    tstats.Capacity,
			TStatus:     &stats.TargetStatus{GlobalRebalanceStats: globalRebStats},
		}

		body := cmn.MustMarshal(msg)
		t.writeJSON(w, r, body, httpdaeWhat)
	case cmn.GetWhatDiskStats:
		diskStats := fs.Mountpaths.GetSelectedDiskStats()
		body := cmn.MustMarshal(diskStats)
		t.writeJSON(w, r, body, httpdaeWhat)
	default:
		t.httprunner.httpdaeget(w, r)
	}
}

func (t *targetrunner) xactStatsRequest(kind, bucket string, onlyRecent bool) ([]byte, error) {
	xactStatsMap, err := xaction.Registry.GetStats(kind, bucket, onlyRecent)
	if err != nil {
		return nil, err
	}

	xactStats := make([]stats.XactStats, 0, len(xactStatsMap))
	for _, stat := range xactStatsMap {
		if stat == nil {
			continue
		}
		xactStats = append(xactStats, stat)
	}

	return jsoniter.Marshal(xactStats)
}

func (t *targetrunner) xactsStartRequest(kind, bucket string) error {
	if kind == "" {
		return fmt.Errorf("kind of xaction to start not specified")
	}

	if bucket == "" {
		switch kind {
		case cmn.ActLRU:
			go t.RunLRU()
		case cmn.ActLocalReb:
			go t.rebManager.RunLocalReb(false /*skipGlobMisplaced*/)
		case cmn.ActPrefetch:
			go t.Prefetch()
		case cmn.ActDownload, cmn.ActEvictObjects, cmn.ActDelete:
			return fmt.Errorf("%q xaction start not supported", kind)
		case cmn.ActElection:
			return fmt.Errorf("%q not supported by target node", kind)
		case cmn.ActECPut, cmn.ActECGet, cmn.ActECRespond:
			return fmt.Errorf("%q requires a bucket to start", kind)
		default:
			return fmt.Errorf("unknown %q xaction", kind)
		}
		return nil
	}
	// henceforth supporting only ais buckets
	bck := &cluster.Bck{Name: bucket, Provider: cmn.ProviderFromBool(true /* is ais */)}
	if err := bck.Init(t.bmdowner); err != nil {
		return err
	}
	switch kind {
	case cmn.ActECPut:
		ec.ECM.RestoreBckPutXact(bck)
	case cmn.ActECGet:
		ec.ECM.RestoreBckGetXact(bck)
	case cmn.ActECRespond:
		ec.ECM.RestoreBckRespXact(bck)
	case cmn.ActMakeNCopies, cmn.ActECEncode:
		return fmt.Errorf("%s supported by /buckets/bucket-name endpoint", kind)
	case cmn.ActPutCopies:
		return fmt.Errorf("%s currently not supported", kind)
	}
	return nil
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
			// TODO: this is duplicated in `t.register`. The difference is that
			// here we are reregistered by the user and in `t.register` is
			// registering new target. This needs to be consolidated somehow.
			//
			// There's a window of time between:
			// a) target joining existing cluster and b) cluster starting to rebalance itself
			// The latter is driven by metasync (see metasync.go) distributing the new Smap.
			// To handle incoming GETs within this window (which would typically take a few seconds or less)
			t.gfn.global.activateTimed()

			if glog.V(3) {
				glog.Infoln("Sending register signal to target keepalive control channel")
			}
			gettargetkeepalive().keepalive.controlCh <- controlSignal{msg: register}

			// Receive most recent smap and bmd.
			var meta targetRegMeta
			if err := cmn.ReadJSON(w, r, &meta); err != nil {
				return
			}

			msgInt := t.newActionMsgInternalStr(cmn.ActRegTarget, meta.Smap, meta.BMD)
			if err := t.receiveBucketMD(meta.BMD, msgInt, bucketMDRegister, ""); err != nil {
				glog.Errorf("error registering %s: %v", t.si.Name(), err)
				if _, incompat := err.(*errPrxBmdOriginDiffer); incompat {
					t.invalmsghdlr(w, r, err.Error())
					return
				}
			}
			t.smapowner.synchronize(meta.Smap, true /* lesserIsErr */)
			return
		case cmn.Mountpaths:
			t.handleMountpathReq(w, r)
			return
		default:
			t.invalmsghdlr(w, r, "unrecognized path in /daemon POST")
			return
		}
	}

	if status, err := t.register(false, cmn.DefaultTimeout); err != nil {
		s := fmt.Sprintf("%s failed to register with proxy, status %d, err: %v", t.si, status, err)
		t.invalmsghdlr(w, r, s)
		return
	}

	if glog.V(3) {
		glog.Infof("Registered %s(self)", t.si.Name())
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
		ct = context.WithValue(ct, ctxUserID, user.userID)
		ct = context.WithValue(ct, ctxCredsDir, config.Auth.CredDir)
		ct = context.WithValue(ct, ctxUserCreds, user.creds)
	}

	return ct
}

func (t *targetrunner) handleUnregisterReq() {
	if glog.V(3) {
		glog.Infoln("Sending unregister on target keepalive control channel")
	}
	// Stop keepaliving
	gettargetkeepalive().keepalive.controlCh <- controlSignal{msg: unregister}

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
		t.invalmsghdlr(w, r, "Invalid action in request")
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

	// TODO: Currently we must abort on enabling mountpath. In some places we open
	// files directly and put them directly on mountpaths. This can lead to
	// problems where we get from new mountpath without asking other (old)
	// mountpaths if they have it (local rebalance has not yet finished).
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
	if err := t.fsprg.addMountpath(mountpath); err != nil {
		t.invalmsghdlr(w, r, fmt.Sprintf("Could not add mountpath, error: %s", err.Error()))
		return
	}

	// TODO: Currently we must abort on adding mountpath. In some places we open
	// files directly and put them directly on mountpaths. This can lead to
	// problems where we get from new mountpath without asking other (old)
	// mountpaths if they have it (local rebalance has not yet finished).
	dsort.Managers.AbortAll(fmt.Errorf("mountpath %q has been added during %s job - aborting due to possible errors", mountpath, cmn.DSortName))
}

func (t *targetrunner) handleRemoveMountpathReq(w http.ResponseWriter, r *http.Request, mountpath string) {
	if err := t.fsprg.removeMountpath(mountpath); err != nil {
		t.invalmsghdlr(w, r, fmt.Sprintf("Could not remove mountpath, error: %s", err.Error()))
		return
	}

	dsort.Managers.AbortAll(fmt.Errorf("mountpath %q has been removed and is unusable", mountpath))
}

// FIXME: use the message
func (t *targetrunner) receiveBucketMD(newBMD *bucketMD, msgInt *actionMsgInternal, tag, caller string) (err error) {
	from := ""
	tname := t.si.Name()
	if caller != "" {
		from = " from " + caller
	}
	if msgInt.Action == "" {
		glog.Infof("%s: %s %s%s", tname, tag, newBMD, from)
	} else {
		glog.Infof("%s: %s %s%s, action %s", tname, tag, newBMD, from, msgInt.Action)
	}

	t.bmdowner.Lock()
	bmd := t.bmdowner.get()
	_, psi := t.getPrimaryURLAndSI()
	if err = t.validateOriginBMD(bmd, psi, newBMD, ""); err != nil {
		cmn.ExitLogf("%v", err) // FATAL: cluster integrity error (cie)
		return
	}

	myver := bmd.version()
	if newBMD.version() <= myver {
		t.bmdowner.Unlock()
		if newBMD.version() < myver {
			err = fmt.Errorf("%s: attempt to downgrade %s to %s", tname, bmd.StringEx(), newBMD.StringEx())
		}
		return
	}
	t.bmdowner.put(newBMD)
	t.bmdowner.Unlock()

	if tag != bucketMDRegister {
		// Don't call ecmanager as it has not been initialized just yet
		// ecmanager will pick up fresh bucketMD when initialized
		ec.ECM.BucketsMDChanged()
	}

	// Delete buckets that do not exist in the new BMD
	bucketsToDelete := make([]string, 0, len(bmd.LBmap))
	for bucket := range bmd.LBmap {
		if nprops, ok := newBMD.LBmap[bucket]; !ok {
			bucketsToDelete = append(bucketsToDelete, bucket)
		} else if bprops, ok := bmd.LBmap[bucket]; ok && bprops != nil && nprops != nil {
			if bprops.Mirror.Enabled && !nprops.Mirror.Enabled {
				xaction.Registry.AbortBucketXact(cmn.ActPutCopies, bucket)
			}
		}
	}

	// TODO: add a separate API to stop ActPut and ActMakeNCopies xactions and/or
	//       disable mirroring (needed in part for cloud buckets)
	if len(bucketsToDelete) > 0 {
		xaction.Registry.AbortAllBuckets(true, bucketsToDelete...)
		go func(buckets ...string) {
			b := &cluster.Bck{Provider: cmn.AIS}
			for _, n := range buckets {
				b.Name = n
				cluster.EvictLomCache(b)
			}
		}(bucketsToDelete...)
		fs.Mountpaths.CreateDestroyBuckets("receive-bmd", false /*false=destroy*/, cmn.AIS, bucketsToDelete...)
	}

	// Create buckets that have been added
	bucketsToCreate := make([]string, 0, len(newBMD.LBmap))
	for bckName := range newBMD.LBmap {
		if _, ok := bmd.LBmap[bckName]; !ok {
			bucketsToCreate = append(bucketsToCreate, bckName)
		}
	}
	fs.Mountpaths.CreateDestroyBuckets("receive-bmd", true /*true=create*/, cmn.AIS, bucketsToCreate...)

	bucketsToCreate = make([]string, 0, len(newBMD.CBmap))
	for bckName := range newBMD.CBmap {
		if _, ok := bmd.CBmap[bckName]; !ok {
			bucketsToCreate = append(bucketsToCreate, bckName)
		}
	}
	fs.Mountpaths.CreateDestroyBuckets("receive-bmd", true /*true=create*/, cmn.Cloud, bucketsToCreate...)

	return
}

func (t *targetrunner) receiveSmap(newsmap *smapX, msgInt *actionMsgInternal, caller string) (err error) {
	var (
		newTargetID, s, from       string
		tname                      = t.si.Name()
		iPresent, newTargetPresent bool
	)
	if caller != "" {
		from = " from " + caller
	}
	// proxy => target control protocol (see p.httpclupost)
	action, newDaemonID := msgInt.Action, msgInt.NewDaemonID
	if action == cmn.ActRegTarget {
		newTargetID = newDaemonID
		s = fmt.Sprintf(", %s %s(new target ID)", msgInt.Action, newTargetID)
	} else if msgInt.Action != "" {
		s = ", action " + msgInt.Action
	}
	glog.Infof("%s: receive %s%s, primary %s%s", tname, newsmap.StringEx(), from, newsmap.ProxySI.Name(), s)
	for id, si := range newsmap.Tmap { // log
		if id == t.si.DaemonID {
			iPresent = true
			glog.Infof("%s <= self", si.Name())
		} else if id == newTargetID {
			newTargetPresent = true // only if not self
		} else {
			glog.Infof("%s", si.Name())
		}
	}
	if !iPresent {
		err = fmt.Errorf("%s: not finding self in the new %s", tname, newsmap.StringEx())
		glog.Warningf("Error: %s\n%s", err, newsmap.pp())
		return
	}
	if newTargetID != "" && newTargetID != t.si.DaemonID && !newTargetPresent {
		err = fmt.Errorf("not finding %s(new target) in the new v%d", newTargetID, newsmap.version())
		glog.Warningf("Error: %s\n%s", err, newsmap.pp())
		return
	}
	if err = t.smapowner.synchronize(newsmap, true /* lesserIsErr */); err != nil {
		return
	}
	if msgInt.Action == cmn.ActGlobalReb { // manual
		go t.rebManager.RunGlobalReb(t.smapowner.Get(), msgInt.GlobRebID)
		return
	}
	if !cmn.GCO.Get().Rebalance.Enabled {
		glog.Infoln("auto-rebalancing disabled")
		return
	}
	if newTargetID == "" {
		return
	}
	glog.Infof("%s receiveSmap: go rebalance(newTargetID=%s)", tname, newTargetID)
	go t.rebManager.RunGlobalReb(t.smapowner.Get(), msgInt.GlobRebID)
	return
}

func (t *targetrunner) ensureLatestMD(msgInt *actionMsgInternal) {
	smap := t.smapowner.Get()
	smapVersion := msgInt.SmapVersion
	if smap.Version < smapVersion {
		glog.Errorf("own %s < v%d - fetching latest for %v", smap, smapVersion, msgInt.Action)
		t.statsif.Add(stats.ErrMetadataCount, 1)
		t.smapVersionFixup()
	} else if smap.Version > smapVersion {
		//if metasync outraces the request, we end up here, just log it and continue
		glog.Errorf("own %s > v%d - encountered during %v", smap, smapVersion, msgInt.Action)
		t.statsif.Add(stats.ErrMetadataCount, 1)
	}

	bucketmd := t.bmdowner.Get()
	bmdVersion := msgInt.BMDVersion
	if bucketmd.Version < bmdVersion {
		glog.Errorf("own %s < v%d - fetching latest for %v", bucketmd, bmdVersion, msgInt.Action)
		t.statsif.Add(stats.ErrMetadataCount, 1)
		t.BMDVersionFixup("", false)
	} else if bucketmd.Version > bmdVersion {
		//if metasync outraces the request, we end up here, just log it and continue
		glog.Errorf("own %s > v%d - encountered during %v", bucketmd, bmdVersion, msgInt.Action)
		t.statsif.Add(stats.ErrMetadataCount, 1)
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

	if err := cmn.LocalLoad(mpathconfigfqn, &oldfs, false /* compression */); err != nil {
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

		glog.Errorf("%s: detected change in the mountpath configuration at %s", t.si.Name(), mpathconfigfqn)
		glog.Errorln("OLD: ====================")
		glog.Errorln(oldfsPprint)
		glog.Errorln("NEW: ====================")
		glog.Errorln(newfsPprint)
	}
	// persist
	if err := cmn.LocalSave(mpathconfigfqn, newfs, false /* compression */); err != nil {
		glog.Errorf("Error writing config file: %v", err)
	}
}

func (t *targetrunner) fetchPrimaryMD(what string, outStruct interface{}, renamed string) (err error) {
	smap := t.smapowner.get()
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

func (t *targetrunner) smapVersionFixup() {
	newSmap := &smapX{}
	err := t.fetchPrimaryMD(cmn.GetWhatSmap, newSmap, "")
	if err != nil {
		glog.Error(err)
		return
	}
	var msgInt = t.newActionMsgInternalStr("get-what="+cmn.GetWhatSmap, newSmap, nil)
	t.receiveSmap(newSmap, msgInt, "")
}

func (t *targetrunner) BMDVersionFixup(renamed string, sleep bool) {
	if sleep {
		time.Sleep(200 * time.Millisecond) // FIXME: request proxy to execute syncCBmeta()
	}
	newBucketMD := &bucketMD{}
	err := t.fetchPrimaryMD(cmn.GetWhatBMD, newBucketMD, renamed)
	if err != nil {
		glog.Error(err)
		return
	}
	msgInt := t.newActionMsgInternalStr("get-what="+cmn.GetWhatBMD, nil, newBucketMD)
	t.receiveBucketMD(newBucketMD, msgInt, bucketMDFixup, "")
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
	var payload = make(cmn.SimpleKVs)
	if err := cmn.ReadJSON(w, r, &payload); err != nil {
		return
	}
	caller := r.Header.Get(cmn.HeaderCallerName)
	newSmap, actionSmap, err := t.extractSmap(payload, caller)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	if newSmap != nil {
		err = t.receiveSmap(newSmap, actionSmap, caller)
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
	}

	newBMD, actionLB, err := t.extractBMD(payload)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	if newBMD != nil {
		caller := r.Header.Get(cmn.HeaderCallerName)
		if err = t.receiveBucketMD(newBMD, actionLB, bucketMDReceive, caller); err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
	}

	revokedTokens, err := t.extractRevokedTokenList(payload)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	t.authn.updateRevokedList(revokedTokens)
}

// POST /v1/metasync
func (t *targetrunner) metasyncHandlerPost(w http.ResponseWriter, r *http.Request) {
	var payload = make(cmn.SimpleKVs)
	if err := cmn.ReadJSON(w, r, &payload); err != nil {
		return
	}
	caller := r.Header.Get(cmn.HeaderCallerName)
	newSmap, msgInt, err := t.extractSmap(payload, caller)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	if newSmap != nil && msgInt.Action == cmn.ActStartGFN {
		t.gfn.global.activateTimed()
	}
}

// GET /v1/health (cmn.Health)
func (t *targetrunner) healthHandler(w http.ResponseWriter, r *http.Request) {
	var (
		callerID   = r.Header.Get(cmn.HeaderCallerID)
		callerName = r.Header.Get(cmn.HeaderCallerName)
		smap       = t.smapowner.get()
		query      = r.URL.Query()
	)

	if callerID == "" && callerName == "" {
		// External call - eg. by user to check if target is alive or by kubernetes service.
		//
		// TODO: we most probably need to split the external API call and
		// internal one. Same goes for proxy - in proxy we also send some stats
		// which are most probably not relevant to the user as it only wants
		// to get 200 in case everything is fine.
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s: external health-ping from %s", t.si.Name(), r.RemoteAddr)
		}
		return
	} else if callerID == "" || callerName == "" {
		t.invalmsghdlr(w, r, fmt.Sprintf("%s: health-ping missing(%s, %s)", t.si.Name(), callerID, callerName))
		return
	}

	if !smap.containsID(callerID) {
		glog.Warningf("%s: health-ping from a not-yet-registered (%s, %s)", t.si.Name(), callerID, callerName)
	}

	callerSmapVer, _ := strconv.ParseInt(r.Header.Get(cmn.HeaderCallerSmapVersion), 10, 64)
	if smap.version() != callerSmapVer {
		glog.Warningf(
			"%s: health-ping from node (%s, %s) which has different smap version: our (%d), node's (%d)",
			t.si.Name(), callerID, callerName, smap.version(), callerSmapVer,
		)
	}

	getRebStatus, _ := cmn.ParseBool(query.Get(cmn.URLParamRebStatus))
	if getRebStatus {
		status := &reb.Status{}
		t.rebManager.GetGlobStatus(status)
		body := cmn.MustMarshal(status)
		if ok := t.writeJSON(w, r, body, "rebalance-status"); !ok {
			return
		}
	}

	if smap.GetProxy(callerID) != nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s: health-ping from %s", t.si.Name(), callerName)
		}
		t.keepalive.heardFrom(callerID, false)
	} else if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s: health-ping from %s", t.si.Name(), callerName)
	}
}

func (t *targetrunner) pollClusterStarted(timeout time.Duration) {
	for i := 1; ; i++ {
		time.Sleep(time.Duration(i) * time.Second)
		smap := t.smapowner.get()
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

	glog.Infof("Disabling %s", t.si.Name())
	for i := 0; i < maxRetrySeconds; i++ {
		if status, eunreg = t.unregister(); eunreg != nil {
			if cmn.IsErrConnectionRefused(eunreg) || status == http.StatusRequestTimeout {
				glog.Errorf("%s: retrying unregistration...", t.si.Name())
				time.Sleep(time.Second)
				continue
			}
		}
		break
	}

	if status >= http.StatusBadRequest || eunreg != nil {
		// do not update state on error
		if eunreg == nil {
			eunreg = fmt.Errorf("error unregistering %s, http status %d", t.si.Name(), status)
		}
		return eunreg
	}

	t.regstate.disabled = true
	glog.Infof("%s has been disabled (unregistered)", t.si.Name())
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

	glog.Infof("Enabling %s", t.si.Name())
	for i := 0; i < maxRetrySeconds; i++ {
		if status, ereg = t.register(false, cmn.DefaultTimeout); ereg != nil {
			if cmn.IsErrConnectionRefused(ereg) || status == http.StatusRequestTimeout {
				glog.Errorf("%s: retrying registration...", t.si.Name())
				time.Sleep(time.Second)
				continue
			}
		}
		break
	}

	if status >= http.StatusBadRequest || ereg != nil {
		// do not update state on error
		if ereg == nil {
			ereg = fmt.Errorf("error registering %s, http status: %d", t.si.Name(), status)
		}
		return ereg
	}

	t.regstate.disabled = false
	glog.Infof("%s has been enabled", t.si.Name())
	return nil
}

//
// copy-rename bucket: 2-phase commit
//
func (t *targetrunner) beginCopyRenameLB(bckFrom *cluster.Bck, bucketTo, action string) (err error) {
	config := cmn.GCO.Get()
	if action == cmn.ActRenameLB && !config.Rebalance.Enabled {
		return fmt.Errorf("cannot %s %s bucket: global rebalancing disabled", action, bckFrom)
	}
	capInfo := t.AvgCapUsed(config)
	if capInfo.OOS {
		return capInfo.Err
	}
	if action == cmn.ActCopyBucket && capInfo.Err != nil {
		return capInfo.Err
	}

	if err = bckFrom.Init(t.bmdowner); err != nil {
		return
	}

	bckTo := &cluster.Bck{Name: bucketTo, Provider: cmn.AIS}
	if err = bckTo.Init(t.bmdowner); err != nil {
		return
	}

	if !bckFrom.Props.InProgress || !bckTo.Props.InProgress {
		err = fmt.Errorf("either source or destination bucket has not been updated correctly")
		return
	}

	switch action {
	case cmn.ActRenameLB:
		_, err = xaction.Registry.RenewBckFastRename(t, bckFrom, bckTo, cmn.ActBegin, t.rebManager)
		if err == nil {
			err = fs.Mountpaths.CreateBucketDirs(bckTo.Name, bckTo.Provider, true /*destroy*/)
		}
	case cmn.ActCopyBucket:
		_, err = xaction.Registry.RenewBckCopy(t, bckFrom, bckTo, cmn.ActBegin)
	default:
		cmn.Assert(false)
	}
	return
}

func (t *targetrunner) abortCopyRenameLB(bckFrom *cluster.Bck, bucketTo, action string) {
	bckTo := &cluster.Bck{Name: bucketTo, Provider: cmn.AIS}
	_, ok := t.bmdowner.get().Get(bckTo)
	if !ok {
		return
	}
	b := xaction.Registry.BucketsXacts(bckFrom)
	if b == nil {
		return
	}
	e := b.GetL(action /* kind */)
	if e == nil {
		return
	}
	ee := e.(*xaction.FastRenEntry)
	if ee.Bucket() != bckFrom.Name {
		return
	}
	e.Get().Abort()
}

func (t *targetrunner) commitCopyRenameLB(bckFrom *cluster.Bck, bucketTo string, msgInt *actionMsgInternal) (err error) {
	bckTo := &cluster.Bck{Name: bucketTo, Provider: cmn.AIS}
	if err := bckTo.Init(t.bmdowner); err != nil {
		return err
	}

	switch msgInt.Action {
	case cmn.ActRenameLB:
		var xact *xaction.FastRen
		xact, err = xaction.Registry.RenewBckFastRename(t, bckFrom, bckTo, cmn.ActCommit, t.rebManager)
		if err != nil {
			glog.Error(err) // must not happen at commit time
			break
		}
		err = fs.Mountpaths.RenameBucketDirs(bckFrom.Name, bckTo.Name, cmn.AIS)
		if err != nil {
			glog.Error(err) // ditto
			break
		}

		t.gfn.local.Activate()
		t.gfn.global.activateTimed()
		go xact.Run(msgInt.GlobRebID)      // do the work
		time.Sleep(100 * time.Millisecond) // FIXME: likely no need
	case cmn.ActCopyBucket:
		var xact *mirror.XactBckCopy
		xact, err = xaction.Registry.RenewBckCopy(t, bckFrom, bckTo, cmn.ActCommit)
		if err != nil {
			glog.Error(err) // unexpected at commit time
			break
		}
		go xact.Run()
	}
	return
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
		return fmt.Errorf("Cannot start bucket %q encoding while rebalance is running", bck.Name)
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

func (t *targetrunner) validateBckProps(bck *cluster.Bck, msgInt *actionMsgInternal) error {
	var (
		nprops     cmn.BucketProps
		capInfo    = t.AvgCapUsed(cmn.GCO.Get())
		mpathCount = fs.Mountpaths.NumAvail()
	)

	body := cmn.MustMarshal(msgInt.Value)
	if err := jsoniter.Unmarshal(body, &nprops); err != nil {
		return fmt.Errorf("invalid internal value: %v", err)
	}

	if nprops.Mirror.Enabled {
		if int(nprops.Mirror.Copies) > mpathCount {
			return fmt.Errorf(
				"number of mountpaths (%d) is not sufficient for requested number of copies (%d) on target %q",
				mpathCount, nprops.Mirror.Copies, t.si.DaemonID,
			)
		}

		if nprops.Mirror.Copies > bck.Props.Mirror.Copies {
			if capInfo.Err != nil {
				return capInfo.Err
			}
		}
	}
	if nprops.EC.Enabled {
		if capInfo.Err != nil {
			return capInfo.Err
		}
	}
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
			Path:   cmn.URLPath(cmn.Version, cmn.Objects, lom.Bucket(), lom.Objname),
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
			Path:   cmn.URLPath(cmn.Version, cmn.Objects, lom.Bucket(), lom.Objname),
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
