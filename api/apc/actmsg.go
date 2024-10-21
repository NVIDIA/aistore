// Package apc: API constant and control messages
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	jsoniter "github.com/json-iterator/go"
)

// ActMsg.Action
// includes Xaction.Kind == ActMsg.Action (when the action is asynchronous)
const (
	ActCreateBck   = "create-bck"  // NOTE: compare w/ ActAddRemoteBck below
	ActDestroyBck  = "destroy-bck" // destroy bucket data and metadata
	ActSetBprops   = "set-bprops"
	ActResetBprops = "reset-bprops"

	ActSummaryBck = "summary-bck"

	ActECEncode  = "ec-encode" // erasure code a bucket
	ActECGet     = "ec-get"    // read erasure coded objects
	ActECPut     = "ec-put"    // erasure code objects
	ActECRespond = "ec-resp"   // respond to other targets' EC requests

	ActCopyBck = "copy-bck"
	ActETLBck  = "etl-bck"

	ActETLInline = "etl-inline"

	ActDsort    = "dsort"
	ActDownload = "download"

	ActBlobDl = "blob-download"

	ActMakeNCopies = "make-n-copies"
	ActPutCopies   = "put-copies"

	ActRebalance = "rebalance"
	ActMoveBck   = "move-bck"

	ActResilver = "resilver"

	ActElection = "election"

	ActLRU          = "lru"
	ActStoreCleanup = "cleanup-store"

	ActEvictRemoteBck = "evict-remote-bck" // evict remote bucket's data
	ActInvalListCache = "inval-listobj-cache"
	ActList           = "list"
	ActLoadLomCache   = "load-lom-cache"
	ActNewPrimary     = "new-primary"
	ActPromote        = "promote"
	ActRenameObject   = "rename-obj"

	// cp (reverse)
	ActResetStats  = "reset-stats"
	ActResetConfig = "reset-config"
	ActSetConfig   = "set-config"

	ActRotateLogs = "rotate-logs"

	ActShutdownCluster = "shutdown" // see also: ActShutdownNode

	// multi-object (via `ListRange`)
	ActCopyObjects     = "copy-listrange"
	ActDeleteObjects   = "delete-listrange"
	ActETLObjects      = "etl-listrange"
	ActEvictObjects    = "evict-listrange"
	ActPrefetchObjects = "prefetch-listrange"
	ActArchive         = "archive" // see ArchiveMsg

	ActAttachRemAis = "attach"
	ActDetachRemAis = "detach"

	ActEnableBackend  = "enable-bend"
	ActDisableBackend = "disable-bend"

	// Node maintenance & cluster membership (see also ActRmNodeUnsafe below)
	ActStartMaintenance = "start-maintenance" // put into maintenance state
	ActStopMaintenance  = "stop-maintenance"  // cancel maintenance state
	ActShutdownNode     = "shutdown-node"     // shutdown node
	ActDecommissionNode = "decommission-node" // start rebalance and, when done, remove node from Smap

	ActDecommissionCluster = "decommission" // decommission all nodes in the cluster (cleanup system data)

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

// internal use
const (
	ActAddRemoteBck   = "add-remote-bck"         // add to BMD existing remote bucket, usually on the fly
	ActRmNodeUnsafe   = "rm-unsafe"              // primary => the node to be removed
	ActStartGFN       = "start-gfn"              // get-from-neighbor
	ActStopGFN        = "stop-gfn"               // off
	ActCleanupMarkers = "cleanup-markers"        // part of the target joining sequence
	ActSelfRemove     = "self-initiated-removal" // e.g., when losing last mountpath
)

const (
	// Actions on mountpaths (/v1/daemon/mountpaths)
	ActMountpathAttach  = "attach-mp"
	ActMountpathEnable  = "enable-mp"
	ActMountpathDetach  = "detach-mp"
	ActMountpathDisable = "disable-mp"

	ActMountpathRescan = "rescan-mp"
	ActMountpathFSHC   = "fshc-mp"

	// Actions on xactions
	ActXactStop  = Stop
	ActXactStart = Start

	// auxiliary
	ActTransient = "transient" // transient - in-memory only
)

// xaction begin-commit phases and related control
const (
	ActBegin  = "begin"
	ActCommit = "commit"
	ActAbort  = "abort"

	ActQuery = "query"
)

const (
	NodeMaintenance  = "maintenance"
	NodeDecommission = "decommission"
)

const (
	ActEcOpen    = "open-ec-streams"
	ActEcClose   = "close-ec-streams"
	ActEcRecover = "recover" // check and recover missing or corrupted EC metadata and/or slices, if any
)

// ActMsg is a JSON-formatted control structures used in a majority of API calls
type (
	ActMsg struct {
		Value  any    `json:"value"`  // action-specific and optional
		Action string `json:"action"` // ActShutdown, ActRebalance, and many more (see apc/const.go)
		Name   string `json:"name"`   // action-specific info of any kind (not necessarily "name")
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
		MemCPUInfo
		CapacityInfo
	}
	ClusterSysInfo struct {
		Proxy  map[string]*MemCPUInfo `json:"proxy"`
		Target map[string]*TSysInfo   `json:"target"`
	}
	ClusterSysInfoRaw struct {
		Proxy  cos.JSONRawMsgs `json:"proxy"`
		Target cos.JSONRawMsgs `json:"target"`
	}
)

////////////
// ActMsg //
////////////

func (msg *ActMsg) String() string   { return msg._string(false) }
func (msg *ActMsg) StringEx() string { return msg._string(true) }

func (msg *ActMsg) _string(wval bool) string {
	s := "actm[" + msg.Action
	if msg.Name != "" {
		s += ", name=" + msg.Name
	}
	if !wval || msg.Value == nil {
		return s + "]"
	}
	vs, err := jsoniter.Marshal(msg.Value)
	if err != nil {
		s += "-<json err: " + err.Error() + ">"
		return s + "]"
	}
	s += ", val=" + strings.ReplaceAll(string(vs), ",", ", ") + "]"
	return s
}
