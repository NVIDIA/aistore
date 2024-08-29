// Package apc: API control messages and constants
/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import "github.com/NVIDIA/aistore/cmn/cos"

// RESTful URL path: l1/l2/l3
const (
	// l1
	Version = "v1"
	// l2
	Buckets   = "buckets"
	Objects   = "objects"
	EC        = "ec"
	Download  = "download"
	Daemon    = "daemon"
	Cluster   = "cluster"
	Tokens    = "tokens"
	Metasync  = "metasync"
	Health    = "health"
	Vote      = "vote"
	ObjStream = "objstream"
	MsgStream = "msgstream"
	Reverse   = "reverse"
	Xactions  = "xactions"
	S3        = "s3"
	Txn       = "txn"      // 2PC
	Notifs    = "notifs"   // intra-cluster notifications
	Users     = "users"    // AuthN
	Clusters  = "clusters" // AuthN
	Roles     = "roles"    // AuthN
	IC        = "ic"       // information center

	// l3 ---

	Voteres  = "result"
	VoteInit = "init"
	PriStop  = "primary-stopping"

	// (see the corresponding action messages above)
	Keepalive = "keepalive"
	AdminJoin = "join-by-admin" // when node is joined by admin ("manual join")
	SelfJoin  = "autoreg"       // auto-join cluster at startup

	// target
	Mountpaths = "mountpaths"

	// common
	Init     = "init"
	Start    = "start"
	Stop     = "stop"
	Abort    = "abort"
	Sort     = "sort"
	Finished = "finished"
	Progress = "progress"

	// dsort, dloader, query
	Metrics     = "metrics"
	Records     = "records"
	Shards      = "shards"
	FinishedAck = "finished_ack"
	UList       = "list"
	Remove      = "remove"
	Next        = "next"
	Peek        = "peek"
	Discard     = "discard"
	WorkerOwner = "worker" // TODO: it should be removed once get-next-bytes endpoint is ready

	LoadX509 = "load-x509"

	// ETL
	ETL        = "etl"
	ETLInfo    = "info"
	ETLList    = UList
	ETLLogs    = "logs"
	ETLObject  = "_object"
	ETLStop    = Stop
	ETLStart   = Start
	ETLHealth  = "health"
	ETLMetrics = "metrics"
)

// RESTful l3, internal use
const (
	SyncSmap = "syncsmap"
)

type URLPath struct {
	L []string
	S string
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
)

func (u URLPath) Join(words ...string) string {
	return cos.JoinWords(u.S, words...)
}
