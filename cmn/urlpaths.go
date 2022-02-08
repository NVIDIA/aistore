// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import "github.com/NVIDIA/aistore/cmn/cos"

type URLPath struct {
	L []string
	S string
}

func urlpath(words ...string) URLPath {
	return URLPath{L: words, S: cos.JoinWords(words[0], words[1:]...)}
}

var (
	URLPathS3 = urlpath(S3) // URLPath{[]string{S3}, S3}

	URLPathBuckets   = urlpath(Version, Buckets)
	URLPathObjects   = urlpath(Version, Objects)
	URLPathEC        = urlpath(Version, EC)
	URLPathNotifs    = urlpath(Version, Notifs)
	URLPathTxn       = urlpath(Version, Txn)
	URLPathXactions  = urlpath(Version, Xactions)
	URLPathIC        = urlpath(Version, IC)
	URLPathHealth    = urlpath(Version, Health)
	URLPathMetasync  = urlpath(Version, Metasync)
	URLPathRebalance = urlpath(Version, Rebalance)

	URLPathClu        = urlpath(Version, Cluster)
	URLPathCluProxy   = urlpath(Version, Cluster, Proxy)
	URLPathCluUserReg = urlpath(Version, Cluster, AdminJoin)
	URLPathCluAutoReg = urlpath(Version, Cluster, SelfJoin)
	URLPathCluKalive  = urlpath(Version, Cluster, Keepalive)
	URLPathCluDaemon  = urlpath(Version, Cluster, Daemon)
	URLPathCluSetConf = urlpath(Version, Cluster, ActSetConfig)
	URLPathCluAttach  = urlpath(Version, Cluster, ActAttachRemote)
	URLPathCluDetach  = urlpath(Version, Cluster, ActDetachRemote)

	URLPathDae          = urlpath(Version, Daemon)
	URLPathDaeProxy     = urlpath(Version, Daemon, Proxy)
	URLPathDaeSetConf   = urlpath(Version, Daemon, ActSetConfig)
	URLPathDaeAdminJoin = urlpath(Version, Daemon, AdminJoin)
	URLPathDaeRmSelf    = urlpath(Version, Daemon, CallbackRmSelf)

	URLPathReverse       = urlpath(Version, Reverse)
	URLPathReverseDaemon = urlpath(Version, Reverse, Daemon)

	URLPathVote        = urlpath(Version, Vote)
	URLPathVoteInit    = urlpath(Version, Vote, Init)
	URLPathVoteProxy   = urlpath(Version, Vote, Proxy)
	URLPathVoteVoteres = urlpath(Version, Vote, Voteres)

	URLPathdSort        = urlpath(Version, Sort)
	URLPathdSortInit    = urlpath(Version, Sort, Init)
	URLPathdSortStart   = urlpath(Version, Sort, Start)
	URLPathdSortList    = urlpath(Version, Sort, List)
	URLPathdSortAbort   = urlpath(Version, Sort, Abort)
	URLPathdSortShards  = urlpath(Version, Sort, Shards)
	URLPathdSortRecords = urlpath(Version, Sort, Records)
	URLPathdSortMetrics = urlpath(Version, Sort, Metrics)
	URLPathdSortAck     = urlpath(Version, Sort, FinishedAck)
	URLPathdSortRemove  = urlpath(Version, Sort, Remove)

	URLPathDownload       = urlpath(Version, Download)
	URLPathDownloadAbort  = urlpath(Version, Download, Abort)
	URLPathDownloadRemove = urlpath(Version, Download, Remove)

	URLPathETL         = urlpath(Version, ETL)
	URLPathETLInitSpec = urlpath(Version, ETL, ETLInitSpec)
	URLPathETLInitCode = urlpath(Version, ETL, ETLInitCode)
	URLPathETLStop     = urlpath(Version, ETL, ETLStop)
	URLPathETLObject   = urlpath(Version, ETL, ETLObject)

	URLPathTokens   = urlpath(Version, Tokens) // authn
	URLPathUsers    = urlpath(Version, Users)
	URLPathClusters = urlpath(Version, Clusters)
	URLPathRoles    = urlpath(Version, Roles)
)

func (u URLPath) Join(words ...string) string {
	return cos.JoinWords(u.S, words...)
}
