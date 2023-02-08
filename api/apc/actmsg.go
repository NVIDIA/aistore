// Package apc: API constant and control messages
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	jsoniter "github.com/json-iterator/go"
)

// ActMsg.Action
// includes Xaction.Kind == ActMsg.Action (when the action is asynchronous)
const (
	ActCreateBck      = "create-bck"  // NOTE: compare w/ ActAddRemoteBck below
	ActDestroyBck     = "destroy-bck" // destroy bucket data and metadata
	ActSummaryBck     = "summary-bck"
	ActCopyBck        = "copy-bck"
	ActDownload       = "download"
	ActECEncode       = "ec-encode" // erasure code a bucket
	ActECGet          = "ec-get"    // erasure decode objects
	ActECPut          = "ec-put"    // erasure encode objects
	ActECRespond      = "ec-resp"   // respond to other targets' EC requests
	ActETLInline      = "etl-inline"
	ActETLBck         = "etl-bck"
	ActElection       = "election"
	ActEvictRemoteBck = "evict-remote-bck" // evict remote bucket's data
	ActInvalListCache = "inval-listobj-cache"
	ActLRU            = "lru"
	ActList           = "list"
	ActLoadLomCache   = "load-lom-cache"
	ActMakeNCopies    = "make-n-copies"
	ActMoveBck        = "move-bck"
	ActNewPrimary     = "new-primary"
	ActPromote        = "promote"
	ActPutCopies      = "put-copies"
	ActRebalance      = "rebalance"
	ActRenameObject   = "rename-obj"
	ActResetBprops    = "reset-bprops"
	ActResetConfig    = "reset-config"
	ActResilver       = "resilver"
	ActResyncBprops   = "resync-bprops"
	ActSetBprops      = "set-bprops"
	ActSetConfig      = "set-config"
	ActShutdown       = "shutdown"
	ActStartGFN       = "start-gfn"
	ActStoreCleanup   = "cleanup-store"

	// multi-object (via `SelectObjsMsg`)
	ActCopyObjects     = "copy-listrange"
	ActDeleteObjects   = "delete-listrange"
	ActETLObjects      = "etl-listrange"
	ActEvictObjects    = "evict-listrange"
	ActPrefetchObjects = "prefetch-listrange"
	ActArchive         = "archive" // see ArchiveMsg

	ActAttachRemAis = "attach"
	ActDetachRemAis = "detach"

	// Node maintenance & cluster membership (see the corresponding URL path words below)
	ActStartMaintenance   = "start-maintenance"     // put into maintenance state
	ActStopMaintenance    = "stop-maintenance"      // cancel maintenance state
	ActShutdownNode       = "shutdown-node"         // shutdown node
	ActCallbackRmFromSmap = "callback-rm-from-smap" // set by primary when requested (internal use only)
	ActDecommissionNode   = "decommission-node"     // start rebalance and, when done, remove node from Smap

	ActDecommission = "decommission" // decommission all nodes in the cluster (cleanup system data)

	ActAdminJoinTarget = "admin-join-target"
	ActSelfJoinTarget  = "self-join-target"
	ActAdminJoinProxy  = "admin-join-proxy"
	ActSelfJoinProxy   = "self-join-proxy"
	ActKeepaliveUpdate = "keepalive-update"

	// IC
	ActSendOwnershipTbl  = "ic-send-own-tbl"
	ActListenToNotif     = "watch-xaction"
	ActMergeOwnershipTbl = "ic-merge-own-tbl"
	ActRegGlobalXaction  = "reg-global-xaction"
)

// internal - add (usually, on the fly) existing remote bucket to AIS BMD
const (
	ActAddRemoteBck = "add-remote-bck"
)

const (
	// Actions on mountpaths (/v1/daemon/mountpaths)
	ActMountpathAttach  = "attach-mp"
	ActMountpathEnable  = "enable-mp"
	ActMountpathDetach  = "detach-mp"
	ActMountpathDisable = "disable-mp"

	// Actions on xactions
	ActXactStop  = Stop
	ActXactStart = Start

	// auxiliary
	ActTransient = "transient" // transient - in-memory only
)

// xaction begin-commit phases
const (
	ActBegin  = "begin"
	ActCommit = "commit"
	ActAbort  = "abort"
)

const (
	NodeMaintenance  = "maintenance"
	NodeDecommission = "decommission"
)

// ActMsg is a JSON-formatted control structures used in a majority of API calls
type (
	ActMsg struct {
		Value  any    `json:"value"`  // action-specific and optional
		Action string `json:"action"` // ActShutdown, ActRebalance, and many more (see apc/const.go)
		Name   string `json:"name"`   // action-specific name (e.g., bucket name)
	}
	ActValRmNode struct {
		DaemonID          string `json:"sid"`
		SkipRebalance     bool   `json:"skip_rebalance"`
		RmUserData        bool   `json:"rm_user_data"`        // decommission-only
		KeepInitialConfig bool   `json:"keep_initial_config"` // ditto (to be able to restart a node from scratch)
		NoShutdown        bool   `json:"no_shutdown"`
	}
)

type (
	JoinNodeResult struct {
		DaemonID    string `json:"daemon_id"`
		RebalanceID string `json:"rebalance_id"`
	}
)

// MountpathList contains two lists:
//   - Available - list of local mountpaths available to the storage target
//   - WaitingDD - waiting for resilvering completion to be detached or disabled (moved to `Disabled`)
//   - Disabled  - list of disabled mountpaths, the mountpaths that generated
//     IO errors followed by (FSHC) health check, etc.
type (
	MountpathList struct {
		Available []string `json:"available"`
		WaitingDD []string `json:"waiting_dd"`
		Disabled  []string `json:"disabled"`
	}
)

// sysinfo
type (
	CapacityInfo struct {
		Used    uint64  `json:"fs_used,string"`
		Total   uint64  `json:"fs_capacity,string"`
		PctUsed float64 `json:"pct_fs_used"`
	}
	TSysInfo struct {
		cos.MemCPUInfo
		CapacityInfo
	}
	ClusterSysInfo struct {
		Proxy  map[string]*cos.MemCPUInfo `json:"proxy"`
		Target map[string]*TSysInfo       `json:"target"`
	}
	ClusterSysInfoRaw struct {
		Proxy  cos.JSONRawMsgs `json:"proxy"`
		Target cos.JSONRawMsgs `json:"target"`
	}
)

////////////
// ActMsg //
////////////

func (msg *ActMsg) String() string {
	s := "amsg[" + msg.Action
	if msg.Name != "" {
		s += ", name=" + msg.Name
	}
	if msg.Value == nil {
		return s + "]"
	}
	vs, err := jsoniter.Marshal(msg.Value)
	if err != nil {
		debug.AssertNoErr(err)
		s += "-<json err: " + err.Error() + ">"
		return s + "]"
	}
	s += ", val=" + strings.ReplaceAll(string(vs), ",", ", ") + "]"
	return s
}
