// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"fmt"
	"io"
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
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
	jsoniter "github.com/json-iterator/go"
)

// target registration with proxy
func (t *targetrunner) register(keepalive bool, timeout time.Duration) (status int, err error) {
	var (
		res  callResult
		meta targetRegMeta
	)
	if !keepalive {
		res = t.join(nil)
	} else { // keepalive
		url, psi := t.getPrimaryURLAndSI()
		res = t.registerToURL(url, psi, timeout, nil, keepalive)
	}
	if res.err != nil {
		return res.status, res.err
	}
	// not being sent at cluster startup and keepalive
	if len(res.outjson) == 0 {
		return
	}
	// There's a window of time between:
	// a) target joining existing cluster and b) cluster starting to rebalance itself
	// The latter is driven by metasync (see metasync.go) distributing the new Smap.
	// To handle incoming GETs within this window (which would typically take a few seconds or less)
	// we need to have the current cluster-wide metadata and the temporary gfn state:
	err = jsoniter.Unmarshal(res.outjson, &meta)
	cmn.AssertNoErr(err)

	t.gfn.global.activate()

	// bmd
	msgInt := t.newActionMsgInternalStr(cmn.ActRegTarget, meta.Smap, meta.BMD)
	if errstr := t.receiveBucketMD(meta.BMD, msgInt, bucketMDRegister, ""); errstr != "" {
		glog.Errorf("registered %s: %s", t.si.Name(), errstr)
	} else {
		glog.Infof("registered %s: %s v%d", t.si.Name(), bmdTermName, t.bmdowner.get().version())
	}
	// smap
	if errstr := t.smapowner.synchronize(meta.Smap, false /*saveSmap*/, true /* lesserIsErr */); errstr != "" {
		glog.Errorf("registered %s: %s", t.si.Name(), errstr)
	} else {
		glog.Infof("registered %s: Smap v%d", t.si.Name(), t.smapowner.get().version())
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
		timeout: defaultTimeout,
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
			if errstr := t.smapowner.synchronize(newsmap, false /*saveSmap*/, true /* lesserIsErr */); errstr != "" {
				t.invalmsghdlr(w, r, fmt.Sprintf("Failed to sync Smap: %s", errstr))
			}
			glog.Infof("%s: %s v%d done", t.si.Name(), cmn.SyncSmap, newsmap.version())
			return
		case cmn.Mountpaths:
			t.handleMountpathReq(w, r)
			return
		case cmn.ActSetConfig: // setconfig #1 - via query parameters and "?n1=v1&n2=v2..."
			kvs := cmn.NewSimpleKVsFromQuery(r.URL.Query())
			if errstr := t.setConfig(kvs); errstr != "" {
				t.invalmsghdlr(w, r, errstr)
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
		if errstr := t.setConfig(kvs); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
			return
		}
	case cmn.ActShutdown:
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	default:
		s := fmt.Sprintf("Unexpected cmn.ActionMsg <- JSON [%v]", msg)
		t.invalmsghdlr(w, r, s)
	}
}

func (t *targetrunner) setConfig(kvs cmn.SimpleKVs) (errstr string) {
	prevConfig := cmn.GCO.Get()
	if err := cmn.SetConfigMany(kvs); err != nil {
		return err.Error()
	}

	config := cmn.GCO.Get()
	if prevConfig.LRU.Enabled && !config.LRU.Enabled {
		lruRunning := t.xactions.globalXactRunning(cmn.ActLRU)
		if lruRunning {
			glog.V(3).Infof("Aborting LRU due to lru.enabled config change")
			t.xactions.abortGlobalXact(cmn.ActLRU)
		}
	}
	return
}

func (t *targetrunner) httpdaesetprimaryproxy(w http.ResponseWriter, r *http.Request, apitems []string) {
	var (
		prepare bool
		err     error
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
		if s := t.smapowner.persist(clone, false /*saveSmap*/); s != "" {
			t.smapowner.Unlock()
			t.invalmsghdlr(w, r, s)
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
	case cmn.GetWhatConfig, cmn.GetWhatSmap, cmn.GetWhatBucketMeta, cmn.GetWhatSmapVote, cmn.GetWhatSnode:
		t.httprunner.httpdaeget(w, r)
	case cmn.GetWhatSysInfo:
		body := cmn.MustMarshal(cmn.TSysInfo{SysInfo: gmem2.FetchSysInfo(), FSInfo: fs.Mountpaths.FetchFSInfo()})
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
		case cmn.ActXactStop:
			t.xactions.doAbort(kind, bucket)
			return
		case cmn.ActXactStart:
			if err := t.xactsStartRequest(kind, bucket); err != nil {
				t.invalmsghdlr(w, r, err.Error())
			}
			return
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
		entry := t.xactions.GetL(cmn.ActGlobalReb)
		if entry != nil && entry.Get() != nil {
			var ok bool
			globalRebStats, ok = entry.Stats().(*stats.RebalanceTargetStats)
			cmn.AssertMsg(ok, "Expected global rebalance stats to be of type stast.RebalanceTargetStats")
		}

		msg := &stats.DaemonStatus{
			Snode:       t.httprunner.si,
			SmapVersion: t.smapowner.get().Version,
			SysInfo:     gmem2.FetchSysInfo(),
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
	xactStatsMap, err := t.xactions.getStats(kind, bucket, onlyRecent)
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
			go t.rebManager.runLocalReb()
		case cmn.ActGlobalReb:
			go t.rebManager.runGlobalReb(t.smapowner.get())
		case cmn.ActPrefetch:
			go t.Prefetch()
		case cmn.ActDownload, cmn.ActEvictObjects, cmn.ActDelete:
			return fmt.Errorf("%s xaction start not supported", kind)
		case cmn.ActElection:
			return fmt.Errorf("%s not supported by target node", kind)
		case cmn.ActECPut, cmn.ActECGet, cmn.ActECRespond:
			return fmt.Errorf("%s requires a bucket to start", kind)
		default:
			return fmt.Errorf("unknown %s xaction", kind)
		}
		return nil
	}

	if !t.bmdowner.Get().IsLocal(bucket) {
		return fmt.Errorf("%s bucket is not a local bucket", bucket)
	}

	switch kind {
	case cmn.ActECPut:
		t.ecmanager.restoreBckPutXact(bucket)
	case cmn.ActECGet:
		t.ecmanager.restoreBckGetXact(bucket)
	case cmn.ActECRespond:
		t.ecmanager.restoreBckRespXact(bucket)
	case cmn.ActMakeNCopies:
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
		case cmn.Register:
			// TODO: this is duplicated in `t.register`. The difference is that
			// here we are reregistered by the user and in `t.register` is
			// registering new target. This needs to be consolidated somehow.
			//
			// There's a window of time between:
			// a) target joining existing cluster and b) cluster starting to rebalance itself
			// The latter is driven by metasync (see metasync.go) distributing the new Smap.
			// To handle incoming GETs within this window (which would typically take a few seconds or less)
			t.gfn.global.activate()

			if glog.V(3) {
				glog.Infoln("Sending register signal to target keepalive control channel")
			}
			gettargetkeepalive().keepalive.controlCh <- controlSignal{msg: register}
			return
		case cmn.Mountpaths:
			t.handleMountpathReq(w, r)
			return
		default:
			t.invalmsghdlr(w, r, "unrecognized path in /daemon POST")
			return
		}
	}

	if status, err := t.register(false, defaultTimeout); err != nil {
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
		if glog.V(3) {
			glog.Infoln("Sending unregister on target keepalive control channel")
		}
		gettargetkeepalive().keepalive.controlCh <- controlSignal{msg: unregister}
		return
	default:
		t.invalmsghdlr(w, r, fmt.Sprintf("unrecognized path: %q in /daemon DELETE", apiItems[0]))
		return
	}
}

// Decrypts token and retreives userID from request header
// Returns empty userID in case of token is invalid
func (t *targetrunner) userFromRequest(header http.Header) (*authRec, error) {
	token := ""
	tokenParts := strings.SplitN(header.Get("Authorization"), " ", 2)
	if len(tokenParts) == 2 && tokenParts[0] == tokenStart {
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
	t.gfn.local.activate()
	enabled, err := t.fsprg.enableMountpath(mountpath)

	if err != nil {
		if _, ok := err.(cmn.NoMountpathError); ok {
			t.invalmsghdlr(w, r, err.Error(), http.StatusNotFound)
		} else {
			// cmn.InvalidMountpathError
			t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		}
		t.gfn.local.deactivate()
		return
	}
	if !enabled {
		w.WriteHeader(http.StatusNoContent)
		t.gfn.local.deactivate()
		return
	}

	t.xactions.stopMountpathXactions()

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

	t.xactions.stopMountpathXactions()
	dsort.Managers.AbortAll(fmt.Errorf("mountpath %q has been disabled and is unusable", mountpath))
}

func (t *targetrunner) handleAddMountpathReq(w http.ResponseWriter, r *http.Request, mountpath string) {
	t.gfn.local.activate()
	err := t.fsprg.addMountpath(mountpath)
	if err != nil {
		t.invalmsghdlr(w, r, fmt.Sprintf("Could not add mountpath, error: %s", err.Error()))
		t.gfn.local.deactivate()
		return
	}

	t.xactions.stopMountpathXactions()

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

	t.xactions.stopMountpathXactions()
	dsort.Managers.AbortAll(fmt.Errorf("mountpath %q has been removed and is unusable", mountpath))
}

// FIXME: use the message
func (t *targetrunner) receiveBucketMD(newbucketmd *bucketMD, msgInt *actionMsgInternal, tag, caller string) (errstr string) {
	from := ""
	if caller != "" {
		from = " from " + caller
	}
	if msgInt.Action == "" {
		glog.Infof("%s: %s %s v%d%s", t.si.Name(), tag, bmdTermName, newbucketmd.version(), from)
	} else {
		glog.Infof("%s: %s %s v%d%s, action %s", t.si.Name(), tag, bmdTermName, newbucketmd.version(), from, msgInt.Action)
	}
	t.bmdowner.Lock()
	bucketmd := t.bmdowner.get()
	myver := bucketmd.version()
	if newbucketmd.version() <= myver {
		t.bmdowner.Unlock()
		if newbucketmd.version() < myver {
			errstr = fmt.Sprintf("Attempt to downgrade %s v%d to v%d", bmdTermName, myver, newbucketmd.version())
		}
		return
	}
	t.bmdowner.put(newbucketmd)
	t.bmdowner.Unlock()

	// Delete buckets that do not exist in the new bucket metadata
	bucketsToDelete := make([]string, 0, len(bucketmd.LBmap))
	for bucket := range bucketmd.LBmap {
		if nprops, ok := newbucketmd.LBmap[bucket]; !ok {
			bucketsToDelete = append(bucketsToDelete, bucket)
		} else if bprops, ok := bucketmd.LBmap[bucket]; ok && bprops != nil && nprops != nil {
			if bprops.Mirror.Enabled && !nprops.Mirror.Enabled {
				t.xactions.abortBucketXact(cmn.ActPutCopies, bucket)
			}
		}
	}

	// TODO: add a separate API to stop ActPut and ActMakeNCopies xactions and/or
	//       disable mirroring (needed in part for cloud buckets)
	t.xactions.abortAllBuckets(true, bucketsToDelete...)

	fs.Mountpaths.CreateDestroyLocalBuckets("receive-bucketmd", false /*false=destroy*/, bucketsToDelete...)

	// Create buckets that have been added
	bucketsToCreate := make([]string, 0, len(newbucketmd.LBmap))
	for bckName := range newbucketmd.LBmap {
		if _, ok := bucketmd.LBmap[bckName]; !ok {
			bucketsToCreate = append(bucketsToCreate, bckName)
		}
	}

	if tag != bucketMDRegister {
		// Don't call ecmanager as it has not benn initialized just yet
		// ecmanager will pick up fresh bucketMD when initialized
		t.ecmanager.BucketsMDChanged()
	}

	fs.Mountpaths.CreateDestroyLocalBuckets("receive-bucketmd", true /*true=create*/, bucketsToCreate...)
	return
}

func (t *targetrunner) receiveSmap(newsmap *smapX, msgInt *actionMsgInternal, caller string) (errstr string) {
	var (
		newTargetID, s, from       string
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
	glog.Infof("%s: receive Smap v%d%s, ntargets %d, primary %s%s",
		t.si.Name(), newsmap.version(), from, newsmap.CountTargets(), newsmap.ProxySI.Name(), s)
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
		errstr = fmt.Sprintf("Not finding %s(self) in the new %s", t.si.Name(), newsmap.pp())
		return
	}
	if newTargetID != "" && newTargetID != t.si.DaemonID && !newTargetPresent {
		errstr = fmt.Sprintf("Not finding %s(new target) in the new %s", newTargetID, newsmap.pp())
		return
	}
	if errstr = t.smapowner.synchronize(newsmap, false /*saveSmap*/, true /* lesserIsErr */); errstr != "" {
		return
	}
	if msgInt.Action == cmn.ActGlobalReb {
		if cmd, ok := msgInt.Value.(string); ok {
			switch cmd {
			case cmn.RebStart:
				go t.rebManager.runGlobalReb(newsmap)
			case cmn.RebAbort:
				t.rebManager.abortGlobalReb()
			default:
				errstr = fmt.Sprintf("Expecting %q or %q in the action message value, getting %s",
					cmn.RebStart, cmn.RebAbort, cmd)
			}
			return
		}
	}
	if !cmn.GCO.Get().Rebalance.Enabled {
		glog.Infoln("auto-rebalancing disabled")
		return
	}
	if newTargetID == "" {
		return
	}
	glog.Infof("%s receiveSmap: go rebalance(newTargetID=%s)", t.si.Name(), newTargetID)
	go t.rebManager.runGlobalReb(newsmap)
	return
}

func (t *targetrunner) ensureLatestMD(msgInt actionMsgInternal) {
	smap := t.smapowner.Get()
	smapVersion := msgInt.SmapVersion
	if smap.Version < smapVersion {
		glog.Errorf("own smap version %d < %d - fetching latest for %v", smap.Version, smapVersion, msgInt.Action)
		t.statsif.Add(stats.ErrMetadataCount, 1)
		t.smapVersionFixup()
	} else if smap.Version > smapVersion {
		//if metasync outraces the request, we end up here, just log it and continue
		glog.Errorf("own smap version %d > %d - encountered during %v", smap.Version, smapVersion, msgInt.Action)
		t.statsif.Add(stats.ErrMetadataCount, 1)
	}

	bucketmd := t.bmdowner.Get()
	bmdVersion := msgInt.BMDVersion
	if bucketmd.Version < bmdVersion {
		glog.Errorf("own bucket-metadata version %d < %d - fetching latest for %v", bucketmd.Version, bmdVersion, msgInt.Action)
		t.statsif.Add(stats.ErrMetadataCount, 1)
		t.bmdVersionFixup()
	} else if bucketmd.Version > bmdVersion {
		//if metasync outraces the request, we end up here, just log it and continue
		glog.Errorf("own bucket-metadata version %d > %d - encountered during %v", bucketmd.Version, bmdVersion, msgInt.Action)
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
	// mpath config dir
	mpathconfigfqn := filepath.Join(cmn.GCO.Get().Confdir, cmn.MountpathBackupFile)

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

	if err := cmn.LocalLoad(mpathconfigfqn, &oldfs); err != nil {
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
	if err := cmn.LocalSave(mpathconfigfqn, newfs); err != nil {
		glog.Errorf("Error writing config file: %v", err)
	}
}

func (t *targetrunner) fetchPrimaryMD(what string, outStruct interface{}) (errstr string) {
	smap := t.smapowner.get()
	if smap == nil || !smap.isValid() {
		return "smap nil or missing"
	}
	psi := smap.ProxySI
	q := url.Values{}
	q.Add(cmn.URLParamWhat, what)
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
		return fmt.Sprintf("failed to %s=%s, err: %v", what, cmn.GetWhatBucketMeta, res.err)
	}

	err := jsoniter.Unmarshal(res.outjson, outStruct)
	if err != nil {
		return fmt.Sprintf("unexpected: failed to unmarshal %s=%s response, err: %v [%v]",
			what, psi.IntraControlNet.DirectURL, err, string(res.outjson))
	}

	return ""
}

func (t *targetrunner) smapVersionFixup() {
	newsmap := &smapX{}
	errstr := t.fetchPrimaryMD(cmn.GetWhatSmap, newsmap)
	if errstr != "" {
		glog.Errorf(errstr)
		return
	}
	var msgInt = t.newActionMsgInternalStr("get-what="+cmn.GetWhatSmap, newsmap, nil)
	t.receiveSmap(newsmap, msgInt, "")
}

func (t *targetrunner) bmdVersionFixup() {
	newbucketmd := &bucketMD{}
	errstr := t.fetchPrimaryMD(cmn.GetWhatBucketMeta, newbucketmd)
	if errstr != "" {
		glog.Errorf(errstr)
		return
	}
	var msgInt = t.newActionMsgInternalStr("get-what="+cmn.GetWhatBucketMeta, nil, newbucketmd)
	t.receiveBucketMD(newbucketmd, msgInt, bucketMDFixup, "")
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

	newsmap, actionsmap, errstr := t.extractSmap(payload)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}

	if newsmap != nil {
		caller := r.Header.Get(cmn.HeaderCallerName)
		errstr = t.receiveSmap(newsmap, actionsmap, caller)
		if errstr != "" {
			t.invalmsghdlr(w, r, errstr)
			return
		}
	}

	newbmd, actionlb, errstr := t.extractbucketmd(payload)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}

	if newbmd != nil {
		caller := r.Header.Get(cmn.HeaderCallerName)
		if errstr = t.receiveBucketMD(newbmd, actionlb, bucketMDReceive, caller); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
			return
		}

		go func() {
			t.bmdowner.Lock()
			bmd := t.bmdowner.get()
			if bmd.Version == 1 && len(bmd.LBmap) == 0 && len(bmd.CBmap) == 0 {
				// do not overwrite xattrs if a cluster sends initial bmd
				t.bmdowner.Unlock()
				return
			}
			if err := bmd.Persist(); err != nil {
				glog.Errorf("%s: %v", t.si.Name(), err)
			}
			t.bmdowner.Unlock()
		}()
	}

	revokedTokens, errstr := t.extractRevokedTokenList(payload)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
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

	newSmap, msgInt, errstr := t.extractSmap(payload)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}

	if newSmap != nil && msgInt.Action == cmn.ActStartGFN {
		t.gfn.global.activate()
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
		status := &rebStatus{}
		t.rebManager.fillinStatus(status)
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
	for {
		time.Sleep(time.Second)
		smap := t.smapowner.get()
		if smap == nil || !smap.isValid() {
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
		proxystats := &stats.CoreStats{}
		err := jsoniter.Unmarshal(res.outjson, proxystats)
		if err != nil {
			glog.Errorf("Unexpected: failed to unmarshal %s response, err: %v [%v]", psi.Name(), err, string(res.outjson))
			continue
		}
		if proxystats.Tracker[stats.Uptime].Value != 0 {
			glog.Infof("%s startup: primary %s", t.si.Name(), psi.Name())
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
		if status, ereg = t.register(false, defaultTimeout); ereg != nil {
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

// Disable implements fspathDispatcher interface
func (t *targetrunner) Disable(mountpath string, why string) (disabled bool, err error) {
	// TODO: notify admin that the mountpath is gone
	glog.Warningf("Disabling mountpath %s: %s", mountpath, why)
	t.xactions.stopMountpathXactions()
	return t.fsprg.disableMountpath(mountpath)
}
