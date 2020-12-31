// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

var (
	URLPathS3 = []string{S3}

	URLPathBuckets  = []string{Version, Buckets}
	URLPathObjects  = []string{Version, Objects}
	URLPathEC       = []string{Version, EC}
	URLPathDownload = []string{Version, Download}
	URLPathNotifs   = []string{Version, Notifs}
	URLPathTokens   = []string{Version, Tokens}
	URLPathUsers    = []string{Version, Users}
	URLPathClusters = []string{Version, Clusters}
	URLPathRoles    = []string{Version, Roles}
	URLPathTxn      = []string{Version, Txn}
	URLPathDaemon   = []string{Version, Daemon}
	URLPathReverse  = []string{Version, Reverse}
	URLPathXactions = []string{Version, Xactions}

	URLPathdSort        = []string{Version, Sort}
	URLPathdSortInit    = []string{Version, Sort, Init}
	URLPathdSortStart   = []string{Version, Sort, Start}
	URLPathdSortAbort   = []string{Version, Sort, Abort}
	URLPathdSortShards  = []string{Version, Sort, Shards}
	URLPathdSortRecords = []string{Version, Sort, Records}
	URLPathdSortMetrics = []string{Version, Sort, Metrics}
	URLPathdSortAck     = []string{Version, Sort, FinishedAck}
	URLPathdSortRemove  = []string{Version, Sort, Remove}

	URLPathQuery        = []string{Version, Query}
	URLPathQueryInit    = []string{Version, Query, Init}
	URLPathQueryDiscard = []string{Version, Query, Discard}
	URLPathQueryNext    = []string{Version, Query, Next}

	URLPathETL       = []string{Version, ETL}
	URLPathETLInit   = []string{Version, ETL, ETLInit}
	URLPathETLStop   = []string{Version, ETL, ETLStop}
	URLPathETLList   = []string{Version, ETL, ETLList}
	URLPathETLLogs   = []string{Version, ETL, ETLLogs}
	URLPathETLObject = []string{Version, ETL, ETLObject}
	URLPathETLBuild  = []string{Version, ETL, ETLBuild}

	URLPathCluster       = []string{Version, Cluster}
	URLPathClusterProxy  = []string{Version, Cluster, Proxy}
	URLPathClusterDaemon = []string{Version, Cluster, Daemon}

	URLPathVote        = []string{Version, Vote}
	URLPathVoteInit    = []string{Version, Vote, Init}
	URLPathVoteProxy   = []string{Version, Vote, Proxy}
	URLPathVoteVoteres = []string{Version, Vote, Voteres}
)
