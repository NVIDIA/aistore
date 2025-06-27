// Package apc: API control messages and constants
/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import "github.com/NVIDIA/aistore/cmn/cos"

// -------------------------------------------------------------
// RESTful URL path: levels l1/l2/l3
// -------------------------------------------------------------

// API version (l1)
const Version = "v1"

// API endpoints (l2)
const (
	Buckets  = "buckets"
	Objects  = "objects"
	EC       = "ec"
	Daemon   = "daemon"
	Metasync = "metasync"
	Health   = "health"
	Vote     = "vote"
	S3       = "s3"
	ML       = "ml"

	// extensions
	Download = "download" // downloader
	Sort     = "sort"     // dsort
	ETL      = "etl"

	// proxy only
	Cluster = "cluster" // primary
	Tokens  = "tokens"  // auth & access
	Reverse = "reverse" // as in: reverse proxy
	IC      = "ic"      // information center
	Notifs  = "notifs"  // intra-cluster notifications

	// target only
	ObjStream = "objstream" // transport streams
	Xactions  = "xactions"  // jobs
	Txn       = "txn"       // 2PC transactions
)

// AuthN server endpoints (l2)
const (
	Users    = "users"
	Clusters = "clusters"
	Roles    = "roles"
)

// l3 ---
const (
	Voteres  = "result"
	VoteInit = "init"
	PriStop  = "primary-stopping"

	// (see the corresponding action messages above)
	Keepalive = "keepalive"
	AdminJoin = "join-by-admin" // when node is added by admin
	SelfJoin  = "autoreg"       // self-joining cluster at node startup

	// target
	Mountpaths = "mountpaths"

	// Prometheus metrics
	Metrics = "metrics"

	// dsort, downloader
	Records     = "records"
	Shards      = "shards"
	FinishedAck = "finished_ack"
	UList       = "list"
	Remove      = "remove"

	LoadX509 = "load-x509"

	// ETL
	ETLLogs    = "logs"
	ETLObject  = "_object"
	ETLHealth  = "health"
	ETLMetrics = "metrics"

	// ETL proxy only
	ETLStart = Start
	ETLStop  = Stop

	// ETL target only
	ETLDetails = "details"

	// ML
	Moss = "moss"
)

// common
const (
	Init  = "init"
	Start = "start"
	Stop  = "stop"
	Abort = "abort"

	Finished = "finished"
	Progress = "progress"
)

// 2PC
const (
	Begin2PC  = "begin"
	Commit2PC = "commit"
	Abort2PC  = Abort

	Query2PC = "query"
)

const SyncSmap = "syncsmap" // obsolete (keeping it)

type URLPath struct {
	S string
	L []string
}

func urlpath(words ...string) URLPath {
	return URLPath{L: words, S: cos.JoinWords(words[0], words[1:]...)}
}

var (
	URLPathS3 = urlpath(S3) // URLPath{[]string{S3}, S3}

	URLPathBuckets  = urlpath(Version, Buckets)
	URLPathObjects  = urlpath(Version, Objects)
	URLPathEC       = urlpath(Version, EC)
	URLPathNotifs   = urlpath(Version, Notifs)
	URLPathTxn      = urlpath(Version, Txn)
	URLPathXactions = urlpath(Version, Xactions)
	URLPathIC       = urlpath(Version, IC)
	URLPathHealth   = urlpath(Version, Health)
	URLPathMetasync = urlpath(Version, Metasync)

	URLPathClu        = urlpath(Version, Cluster)
	URLPathCluProxy   = urlpath(Version, Cluster, Proxy)
	URLPathCluUserReg = urlpath(Version, Cluster, AdminJoin)
	URLPathCluAutoReg = urlpath(Version, Cluster, SelfJoin)
	URLPathCluKalive  = urlpath(Version, Cluster, Keepalive)
	URLPathCluDaemon  = urlpath(Version, Cluster, Daemon) // (internal)
	URLPathCluSetConf = urlpath(Version, Cluster, ActSetConfig)
	URLPathCluAttach  = urlpath(Version, Cluster, ActAttachRemAis)
	URLPathCluDetach  = urlpath(Version, Cluster, ActDetachRemAis)

	URLPathCluX509 = urlpath(Version, Cluster, LoadX509)

	URLPathCluBendDisable = urlpath(Version, Cluster, ActDisableBackend)
	URLPathCluBendEnable  = urlpath(Version, Cluster, ActEnableBackend)

	URLPathDae          = urlpath(Version, Daemon)
	URLPathDaeProxy     = urlpath(Version, Daemon, Proxy)
	URLPathDaeSetConf   = urlpath(Version, Daemon, ActSetConfig)
	URLPathDaeAdminJoin = urlpath(Version, Daemon, AdminJoin)
	URLPathDaeForceJoin = urlpath(Version, Daemon, ActPrimaryForce)

	URLPathDaeBendDisable = urlpath(Version, Daemon, ActDisableBackend)
	URLPathDaeBendEnable  = urlpath(Version, Daemon, ActEnableBackend)

	URLPathDaeX509 = urlpath(Version, Daemon, LoadX509)

	URLPathReverse    = urlpath(Version, Reverse)
	URLPathReverseDae = urlpath(Version, Reverse, Daemon)

	URLPathVote        = urlpath(Version, Vote)
	URLPathVoteInit    = urlpath(Version, Vote, Init)
	URLPathVoteProxy   = urlpath(Version, Vote, Proxy)
	URLPathVoteVoteres = urlpath(Version, Vote, Voteres)
	URLPathVotePriStop = urlpath(Version, Vote, PriStop)

	URLPathdSort        = urlpath(Version, Sort)
	URLPathdSortInit    = urlpath(Version, Sort, Init)
	URLPathdSortStart   = urlpath(Version, Sort, Start)
	URLPathdSortList    = urlpath(Version, Sort, UList)
	URLPathdSortAbort   = urlpath(Version, Sort, Abort)
	URLPathdSortShards  = urlpath(Version, Sort, Shards)
	URLPathdSortRecords = urlpath(Version, Sort, Records)
	URLPathdSortMetrics = urlpath(Version, Sort, Metrics)
	URLPathdSortAck     = urlpath(Version, Sort, FinishedAck)
	URLPathdSortRemove  = urlpath(Version, Sort, Remove)

	URLPathDownload       = urlpath(Version, Download)
	URLPathDownloadAbort  = urlpath(Version, Download, Abort)
	URLPathDownloadRemove = urlpath(Version, Download, Remove)

	URLPathETL       = urlpath(Version, ETL)
	URLPathETLObject = urlpath(Version, ETL, ETLObject)

	URLPathTokens   = urlpath(Version, Tokens) // authn
	URLPathUsers    = urlpath(Version, Users)
	URLPathClusters = urlpath(Version, Clusters)
	URLPathRoles    = urlpath(Version, Roles)

	URLPathML = urlpath(Version, ML)
)

func (u URLPath) Join(words ...string) string {
	return cos.JoinWords(u.S, words...)
}
