// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ext/dload"

	"github.com/urfave/cli"
)

// Contains common constants and global variables (including all command-line options aka flags).

// top-level commands (categories - nouns)
const (
	commandAdvanced  = "advanced"
	commandAlias     = "alias"
	commandArch      = "archive"
	commandAuth      = "auth"
	commandBucket    = "bucket"
	commandCluster   = "cluster"
	commandConfig    = "config"
	commandDashboard = "dashboard"
	commandETL       = apc.ETL
	commandJob       = "job"
	commandLog       = "log"
	commandObject    = "object"
	commandPerf      = "performance"
	commandStorage   = "storage"
	commandTLS       = "tls"

	commandSearch = "search"
)

// top-level `show`
const (
	commandShow = "show"
)

// 'ais advanced' subcommands
const (
	cmdGenShards     = "gen-shards"
	cmdPreload       = "preload"
	cmdRmSmap        = "remove-from-smap"
	cmdRandNode      = "random-node"
	cmdRandMountpath = "random-mountpath"
	cmdRotateLogs    = "rotate-logs"
)

const advancedUsageOnly = "(caution: advanced usage only)"

// - 2nd level subcommands (mostly, verbs)
// - show subcommands (`show <what>`)
// - 3rd level subcommands
const (
	commandCat       = "cat"
	commandConcat    = "concat"
	commandCopy      = "cp"
	commandCreate    = "create"
	commandGet       = "get"
	commandList      = "ls"
	commandSetCustom = "set-custom"
	commandPut       = "put"
	commandRemove    = "rm"
	commandRename    = "mv"
	commandSet       = "set"

	// multipart upload commands
	commandMptUpload = "multipart-upload"
	cmdMptCreate     = "create"
	cmdMptPut        = "put-part"
	cmdMptComplete   = "complete"
	cmdMptAbort      = "abort"

	// ml namespace and subcommands
	commandML         = "ml"
	cmdGetBatch       = "get-batch"
	cmdLhotseGetBatch = "lhotse-get-batch"

	// jobs
	commandStart = apc.ActXactStart
	commandStop  = apc.ActXactStop
	commandWait  = "wait"

	cmdSmap   = apc.WhatSmap
	cmdBMD    = apc.WhatBMD
	cmdConfig = "config" // apc.WhatNodeConfig and apc.WhatClusterConfig
	cmdLog    = apc.WhatLog

	cmdBucket = "bucket"
	cmdObject = "object"
	cmdProps  = "props"

	// NOTE implicit assumption: AIS xaction kind _eq_ the command name (e.g. "download")
	commandRebalance = apc.ActRebalance
	commandResilver  = apc.ActResilver

	commandPromote  = apc.ActPromote
	commandECEncode = apc.ActECEncode
	commandMirror   = "mirror"   // display name for apc.ActMakeNCopies
	commandEvict    = "evict"    // apc.ActEvictRemoteBck or apc.ActEvictObjects
	commandPrefetch = "prefetch" // apc.ActPrefetchObjects

	cmdBlobDownload = apc.ActBlobDl   // blob-download
	cmdDownload     = apc.ActDownload // download
	cmdDsort        = apc.ActDsort
	cmdRebalance    = apc.ActRebalance
	cmdLRU          = apc.ActLRU
	commandRechunk  = apc.ActRechunk
	cmdStgCleanup   = "cleanup" // display name for apc.ActStoreCleanup
	cmdScrub        = "validate"
	cmdSummary      = "summary" // ditto apc.ActSummaryBck

	cmdCluster    = commandCluster
	cmdDashboard  = commandDashboard
	cmdNode       = "node"
	cmdPrimary    = "set-primary"
	cmdList       = commandList
	cmdStop       = "stop"
	cmdStart      = "start"
	cmdMembership = "add-remove-nodes"
	cmdShutdown   = "shutdown"
	cmdAttach     = "attach"
	cmdDetach     = "detach"
	cmdResetStats = "reset-stats"
	cmdDropLcache = "drop-lcache"

	cmdReloadCreds = "reload-backend-creds"

	cmdDownloadLogs = "download-logs"
	cmdViewLogs     = "view-logs" // etl

	// Cluster subcommands
	cmdCluAttach = "remote-" + cmdAttach
	cmdCluDetach = "remote-" + cmdDetach
	cmdCluConfig = "configure"
	cmdReset     = "reset"

	// Mountpath commands
	cmdMpathAttach  = cmdAttach
	cmdMpathEnable  = "enable"
	cmdMpathDetach  = cmdDetach
	cmdMpathDisable = "disable"

	// mountpath commands (advanced)
	cmdMpathRescanDisks = "rescan-disks"
	cmdMpathFshc        = "fshc"
	// backend enable/disable (advanced)
	cmdBackendEnable  = "enable-backend"
	cmdBackendDisable = "disable-backend"
	// object-locked enum (advanced)
	cmdCheckLock = "check-lock"

	cmdLoadTLS     = "load-certificate"
	cmdValidateTLS = "validate-certificates"

	// Node subcommands
	cmdJoin                = "join"
	cmdStartMaint          = "start-maintenance"
	cmdStopMaint           = "stop-maintenance"
	cmdNodeDecommission    = "decommission"
	cmdClusterDecommission = "decommission"

	// Show subcommands (not all)
	cmdShowRemoteAIS  = "remote-cluster"
	cmdShowStats      = "stats"
	cmdMountpath      = "mountpath"
	cmdCapacity       = "capacity"
	cmdShowDisk       = "disk"
	cmdShowCounters   = "counters"
	cmdShowThroughput = "throughput"
	cmdShowLatency    = "latency"

	// Bucket properties subcommands
	cmdSetBprops   = "set"
	cmdResetBprops = cmdReset

	// AuthN subcommands
	cmdAuthAdd     = "add"
	cmdAuthShow    = "show"
	cmdAuthSet     = commandSet
	cmdAuthRemove  = commandRemove
	cmdAuthLogin   = "login"
	cmdAuthLogout  = "logout"
	cmdAuthUser    = "user"
	cmdAuthRole    = "role"
	cmdAuthCluster = cmdCluster
	cmdAuthToken   = "token"
	cmdAuthConfig  = cmdConfig
	cmdAuthOIDC    = "oidc"
	cmdAuthJWKS    = "jwks"

	// K8s subcommans
	cmdK8s        = "kubectl"
	cmdK8sSvc     = "svc"
	cmdK8sCluster = commandCluster

	// ETL subcommands
	cmdInit   = "init"
	cmdSpec   = "spec"
	cmdErrors = "errors"

	// config subcommands
	cmdCLI        = "cli"
	cmdCLIShow    = commandShow
	cmdCLISet     = cmdSetBprops
	cmdCLIReset   = cmdResetBprops
	cmdAliasShow  = commandShow
	cmdAliasRm    = commandRemove
	cmdAliasSet   = cmdCLISet
	cmdAliasReset = cmdResetBprops
)

// time constants
const (
	// e.g. xquery --all; see also xact/api
	longClientTimeout = 60 * time.Second

	// list-objects progress; list-objects with --summary
	listObjectsWaitTime = 8 * time.Second

	// default '--refresh' durations and counts
	refreshRateDefault = 5 * time.Second
	refreshRateMinDur  = time.Second
	countDefault       = 1
	countUnlimited     = -1

	execLinuxCommandTime     = 5 * time.Second
	execLinuxCommandTimeLong = 30 * time.Second

	logFlushTime = 10 * time.Second // as the name implies

	//  progress bar: when stats stop moving (increasing)
	timeoutNoChange = 10 * time.Second

	// download started
	dloadStartedTime = refreshRateDefault

	// job wait: start printing "."(s)
	wasFast = refreshRateDefault
)

//
// more constants (misc)
//

const flagPrefix = "--"

const (
	dfltStdinChunkSize = 10 * cos.MiB

	// PUT: multipart uploads (chunking)
	dfltObjSizeLimit = 1 * cos.GiB // the object size threshold that triggers auto-chunking and multipart uploads for PUT
	dfltChunkSize    = 1 * cos.MiB // the default chunk size for multipart uploads for PUT
	chunkSizeMin     = cos.KiB
	chunkSizeMax     = 5 * cos.GiB
)

const (
	NIY = "not implemented yet" // TODO potentially
)

const (
	timeUnits = `ns, us (or µs), ms, s (default), m, h`
)

const nodeLogFlushName = "log.flush_time"

const (
	tabtab     = "press <TAB-TAB> to select"
	tabHelpOpt = "press <TAB-TAB> to select, '--help' for more options"
	tabHelpDet = "press <TAB-TAB> to select, '--help' for details"
)

// indentation
const (
	indent1 = "   "
	indent2 = "      "       // repeat(indent1, 2)
	indent4 = "            " // repeat(indent1, 4)
)

const separatorLine = "---"

const (
	archFormats = ".tar, .tgz or .tar.gz, .zip, .tar.lz4" // namely, archive.FileExtensions
	archExts    = "(" + archFormats + ")"
)

const etlPipelineSeparator = ">>"

// `ArgsUsage`: argument placeholders in help messages
const (
	// Job IDs (download, dsort)
	jobIDArgument                 = "JOB_ID"
	optionalJobIDArgument         = "[JOB_ID]"
	optionalJobIDDaemonIDArgument = "[JOB_ID [NODE_ID]]"

	jobAnyArg                = "[NAME] [JOB_ID] [NODE_ID] [BUCKET]"
	jobShowRebalanceArgument = "[REB_ID] [NODE_ID]"

	// Perf
	showPerfArgument = "Show performance counters, throughput, latency, disks, used/available capacities (" + tabtab + " specific view)"

	// ETL
	etlNameArgument          = "ETL_NAME"
	optionalETLNameArgument  = "[ETL_NAME]"
	etlNameWithJobIDArgument = "ETL_NAME [JOB_ID]"
	etlNameListArgument      = "ETL_NAME [ETL_NAME ...]"
	etlNameOrSelectorArgs    = "[ETL_NAME ...] [--all] [-f <file-or-url>]"

	// key/value
	keyValuePairsArgument = "KEY=VALUE [KEY=VALUE...]"
	jsonKeyValueArgument  = "JSON-formatted-KEY-VALUE"

	// Buckets
	bucketArgument         = "BUCKET"
	optionalBucketArgument = "[BUCKET]"
	bucketsArgument        = "BUCKET [BUCKET...]"
	bucketPropsArgument    = bucketArgument + " " + jsonKeyValueArgument + " | " + keyValuePairsArgument
	bucketAndPropsArgument = "BUCKET [PROP_PREFIX]"

	bucketObjectOrTemplateMultiArg = "BUCKET[/OBJECT_NAME_or_TEMPLATE] [BUCKET[/OBJECT_NAME_or_TEMPLATE] ...]"

	// Lhotse: DST_ARCHIVE is optional when using --output-template (multi-batch mode)
	getBatchSpecArgument       = "[BUCKET[/NAME_or_TEMPLATE] ...] DST_ARCHIVE --spec [JSON_SPECIFICATION|YAML_SPECIFICATION]"
	getBatchLhotseSpecArgument = "[BUCKET[/NAME_or_TEMPLATE] ...] [DST_ARCHIVE] --spec [JSON_SPECIFICATION|YAML_SPECIFICATION]"

	bucketEmbeddedPrefixArg = "[BUCKET[/PREFIX]]"

	bucketSrcArgument       = "SRC_BUCKET"
	bucketObjectSrcArgument = "SRC_BUCKET[/OBJECT_NAME_or_TEMPLATE]"
	bucketDstArgument       = "DST_BUCKET"
	bucketNewArgument       = "NEW_BUCKET"

	dsortSpecArgument = "[SRC_BUCKET] [DST_BUCKET] --spec [JSON_SPECIFICATION|YAML_SPECIFICATION|-]"

	// Objects
	objectArgument          = "BUCKET/OBJECT_NAME"
	optionalObjectsArgument = "BUCKET[/OBJECT_NAME] ..."
	dstShardArgument        = bucketDstArgument + "/SHARD_NAME"

	// Multipart upload arguments
	mptCreateArgument   = objectArgument
	mptPutArgument      = objectArgument + " UPLOAD_ID PART_NUMBER FILE_PATH"
	mptCompleteArgument = objectArgument + " UPLOAD_ID PART_NUMBERS"
	mptAbortArgument    = objectArgument + " UPLOAD_ID"

	getObjectArgument = "BUCKET[/OBJECT_NAME] [OUT_FILE|OUT_DIR|-]"

	optionalPrefixArgument = "BUCKET[/OBJECT_NAME_or_PREFIX]"
	putObjectArgument      = "[-|FILE|DIRECTORY[/PATTERN]] " + optionalPrefixArgument
	promoteObjectArgument  = "FILE|DIRECTORY[/PATTERN] " + optionalPrefixArgument

	shardArgument         = "BUCKET/SHARD_NAME"
	optionalShardArgument = "BUCKET[/SHARD_NAME]"
	putApndArchArgument   = "[-|FILE|DIRECTORY[/PATTERN]] " + shardArgument
	getShardArgument      = optionalShardArgument + " [OUT_FILE|OUT_DIR|-]"

	concatObjectArgument = "FILE|DIRECTORY[/PATTERN] [ FILE|DIRECTORY[/PATTERN] ...] " + objectArgument

	renameObjectArgument = objectArgument + " NEW_OBJECT_NAME"

	// nodes
	nodeIDArgument            = "NODE_ID"
	optionalNodeIDArgument    = "[NODE_ID]"
	targetIDArgument          = "TARGET_ID"
	optionalTargetIDArgument  = "[TARGET_ID]"
	joinNodeArgument          = "IP:PORT"
	nodeMountpathPairArgument = "NODE_ID=MOUNTPATH [NODE_ID=MOUNTPATH...]"

	// node log
	showLogArgument = nodeIDArgument
	getLogArgument  = nodeIDArgument + " [OUT_FILE|OUT_DIR|-]"

	// job columnar values
	jobColNode   = "NODE"
	jobColID     = "ID"
	jobColKind   = "KIND"
	jobColBucket = "BUCKET"
	jobColState  = "STATE"

	// job states
	jobStateAborted  = "Aborted"
	jobStateRunning  = "Running"
	jobStateFinished = "Finished"

	// cluster
	showClusterArgument = "[NODE_ID] | [target [NODE_ID]] | [proxy [NODE_ID]] | [smap [NODE_ID]] | [bmd [NODE_ID]] | [config [NODE_ID]] | [stats [NODE_ID]]"

	// config
	showConfigArgument = "cli | cluster [CONFIG SECTION OR PREFIX] |\n" +
		"                NODE_ID [ inherited | local | all [CONFIG SECTION OR PREFIX]]"

	showClusterConfigArgument = "[CONFIG_SECTION]"
	showRemoteConfigArgument  = aliasArgument + " [CONFIG_SECTION]"
	configSectionNotFoundHint = "Try '%s' to see all sections, or remove --json flag for table format"
	nodeConfigArgument        = nodeIDArgument + " " + keyValuePairsArgument

	// remais
	attachRemoteAISArgument = aliasURLPairArgument
	detachRemoteAISArgument = aliasArgument

	startDownloadArgument = "SOURCE DESTINATION"
	showStatsArgument     = "[NODE_ID]"

	// backend enable/disable
	cloudProviderArg = "CLOUD_PROVIDER"

	// 'ais ls'
	lsAnyCommandArgument = bucketEmbeddedPrefixArg + " [PROVIDER]"

	// Auth
	userLoginArgument = "USER_NAME"

	addAuthUserArgument       = "USER_NAME [ROLE...]"
	deleteAuthUserArgument    = "USER_NAME"
	addAuthClusterArgument    = "CLUSTER_ID [ALIAS] URL [URL...]"
	deleteAuthClusterArgument = "CLUSTER_ID"
	showAuthClusterArgument   = "[CLUSTER_ID]"
	showAuthRoleArgument      = "[ROLE]"
	showAuthUserListArgument  = "[USER_NAME]"
	addSetAuthRoleArgument    = "ROLE [PERMISSION ...]"
	deleteAuthRoleArgument    = "ROLE"
	deleteAuthTokenArgument   = "TOKEN | TOKEN_FILE" //nolint:gosec // false positive G101

	// Alias
	aliasURLPairArgument = "ALIAS=URL (or UUID=URL)"
	aliasArgument        = "ALIAS (or UUID)"
	aliasCmdArgument     = "COMMAND"
	aliasSetCmdArgument  = "ALIAS COMMAND"

	// Search
	searchArgument = "KEYWORD [KEYWORD...]"
)

const scopeAll = "all"

const (
	cfgScopeAll       = scopeAll
	cfgScopeLocal     = "local"
	cfgScopeInherited = "inherited"
)

//
// Command-line Options aka Flags
//

var (
	//
	// scope 'all'
	//
	allPropsFlag        = cli.BoolFlag{Name: scopeAll, Usage: "Include all object properties: name, size, atime, location, copies, custom (user-defined), and more"}
	allJobsFlag         = cli.BoolFlag{Name: scopeAll, Usage: "Include all jobs: running, finished, and aborted"}
	allRunningJobsFlag  = cli.BoolFlag{Name: scopeAll, Usage: "Include all running jobs"}
	allFinishedJobsFlag = cli.BoolFlag{Name: scopeAll, Usage: "Include all finished jobs"}
	rmrfFlag            = cli.BoolFlag{Name: scopeAll, Usage: "Remove all objects (use with extreme caution!)"}
	rmAllBucketsFlag    = cli.BoolFlag{Name: scopeAll, Usage: "Remove all AIS buckets from the cluster (use with extreme caution - cannot be undone)"}
	allLogsFlag         = cli.BoolFlag{Name: scopeAll, Usage: "Download all logs"}
	evictAllBucketsFlag = cli.BoolFlag{Name: scopeAll, Usage: "Evict all remote buckets from the cluster (use with extreme caution)"}

	allObjsOrBcksFlag = cli.BoolFlag{
		Name: scopeAll,
		Usage: "Depending on the context, list:\n" +
			indent4 + "\t- all buckets, including accessible (visible) remote buckets that are not in-cluster\n" +
			indent4 + "\t- all objects in a given accessible (visible) bucket, including remote objects and misplaced copies",
	}
	copyAllObjsFlag = cli.BoolFlag{
		Name:  scopeAll,
		Usage: "Copy all objects from a remote bucket including those that are not present (not cached) in cluster",
	}
	etlAllObjsFlag = cli.BoolFlag{
		Name:  scopeAll,
		Usage: "Transform all objects from a remote bucket including those that are not present (not cached) in cluster",
	}

	// obj props
	objPropsFlag = cli.StringFlag{
		Name: "props",
		Usage: "Comma-separated list of object properties including name, size, version, copies, and more; e.g.:\n" +
			indent4 + "\t--props all\n" +
			indent4 + "\t--props name,size,cached\n" +
			indent4 + "\t--props \"ec, copies, custom, location\"",
	}

	// prefix (to match)
	listObjPrefixFlag = cli.StringFlag{
		Name: "prefix",
		Usage: "List objects with names starting with the specified prefix, e.g.:\n" +
			indent4 + "\t'--prefix a/b/c' - list virtual directory a/b/c and/or objects from the virtual directory\n" +
			indent4 + "\ta/b that have their names (relative to this directory) starting with the letter 'c'",
	}
	getObjPrefixFlag = cli.StringFlag{
		Name: listObjPrefixFlag.Name,
		Usage: "Get objects with names starting with the specified prefix, e.g.:\n" +
			indent4 + "\t'--prefix a/b/c' - get objects from the virtual directory a/b/c and objects from the virtual directory\n" +
			indent4 + "\ta/b that have their names (relative to this directory) starting with 'c';\n" +
			indent4 + "\t'--prefix \"\"' - get entire bucket (all objects)",
	}
	verbObjPrefixFlag = cli.StringFlag{
		Name: listObjPrefixFlag.Name,
		Usage: "Select virtual directories or objects with names starting with the specified prefix, e.g.:\n" +
			indent4 + "\t'--prefix a/b/c'\t- matches names 'a/b/c/d', 'a/b/cdef', and similar;\n" +
			indent4 + "\t'--prefix a/b/c/'\t- only matches objects from the virtual directory a/b/c/",
	}

	bsummPrefixFlag = cli.StringFlag{
		Name: listObjPrefixFlag.Name,
		Usage: "For each bucket, select only those objects (names) that start with the specified prefix, e.g.:\n" +
			indent4 + "\t'--prefix a/b/c' - sum up sizes of the virtual directory a/b/c and objects from the virtual directory\n" +
			indent4 + "\ta/b that have names (relative to this directory) starting with the letter c",
	}

	//
	// multipart upload flags
	//
	mptUploadIDFlag = cli.StringFlag{
		Name:  "upload-id",
		Usage: "Multipart upload ID returned from create operation",
	}
	mptPartNumberFlag = cli.IntFlag{
		Name:  "part-number",
		Usage: "Part number for multipart upload (starting from 1)",
	}
	mptPartNumbersFlag = cli.StringFlag{
		Name:  "part-numbers",
		Usage: "Comma-separated list of part numbers for completion, e.g.: '1,2,3,4'",
	}

	//
	// longRunFlags
	//
	refreshFlag = DurationFlag{
		Name: "refresh",
		Usage: "Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);\n" +
			indent4 + "\tvalid time units: " + timeUnits,
	}
	countFlag = cli.IntFlag{
		Name: "count",
		Usage: "Used together with " + qflprn(refreshFlag) + " to limit the number of generated reports, e.g.:\n" +
			indent4 + "\t '--refresh 10 --count 5' - run 5 times with 10s interval",
	}
	longRunFlags = []cli.Flag{refreshFlag, countFlag}

	//
	// regex and friends
	//
	regexFlag = cli.StringFlag{Name: "regex", Usage: "Regular expression to match and select items in question"}

	regexLsAnyFlag = cli.StringFlag{
		Name: regexFlag.Name,
		Usage: "Regular expression; use it to match either bucket names or objects in a given bucket, e.g.:\n" +
			indent4 + "\tais ls --regex \"(m|n)\"\t- match buckets such as ais://nnn, s3://mmm, etc.;\n" +
			indent4 + "\tais ls ais://nnn --regex \"^A\"\t- match object names starting with letter A",
	}

	// TODO: `--select` (to select columns) would sound more conventional/idiomatic

	regexColsFlag = cli.StringFlag{
		Name: regexFlag.Name,
		Usage: "Regular expression to select table columns (case-insensitive), e.g.:\n" +
			indent4 + "\t --regex \"put|err\" - show PUT (count), PUT (total size), and all supported error counters;\n" +
			indent4 + "\t --regex \"Put|ERR\" - same as above;\n" +
			indent4 + "\t --regex \"[a-z]\" - show all supported metrics, including those that have zero values across all nodes;\n" +
			indent4 + "\t --regex \"(AWS-GET$|VERSION-CHANGE$)\" - show the number object version changes (updates) and cold GETs from AWS\n" +
			indent4 + "\t --regex \"(gcp-get$|version-change$)\" - same as above for Google Cloud ('gs://')",
	}
	allColumnsFlag = cli.BoolFlag{
		Name:  "all-columns",
		Usage: "Show all columns, including those with only zero values",
	}

	regexJobsFlag = cli.StringFlag{
		Name:  regexFlag.Name,
		Usage: "Regular expression to select jobs by name, kind, or description, e.g.: --regex \"ec|mirror|elect\"",
	}

	columnFilterFlag = cli.StringFlag{
		Name: "filter",
		Usage: "Regular expression to filter job table rows based on column values, format: \"COLUMN=PATTERN\", e.g.:\n" +
			indent4 + "\t--filter \"STATE=Running\" - show only running jobs\n" +
			indent4 + "\t--filter \"NODE=(FFIt8090|UTat8088)\" - show jobs for specific nodes\n" +
			indent4 + "\t--filter \"BUCKET=.*ais-.*\" - show jobs for buckets matching pattern\n" +
			indent4 + "\t--filter \"KIND=rebalance.*\" - show only rebalance jobs\n" +
			indent4 + "\tnote: use --all to include finished jobs in any filter",
	}

	jsonFlag     = cli.BoolFlag{Name: "json,j", Usage: "JSON input/output"}
	noHeaderFlag = cli.BoolFlag{Name: "no-headers,H", Usage: "Display tables without headers"}
	noFooterFlag = cli.BoolFlag{Name: "no-footers,F", Usage: "Display tables without footers"}

	progressFlag = cli.BoolFlag{
		Name:  "progress",
		Usage: "Show progress bar(s) and progress of execution in real time; 'object get' with multiple objects: show number of objects processed",
	}
	dryRunFlag = cli.BoolFlag{Name: "dry-run", Usage: "Preview the results without really running the action"}

	verboseFlag    = cli.BoolFlag{Name: "verbose,v", Usage: "Verbose output"}
	verboseJobFlag = cli.BoolFlag{Name: verboseFlag.Name, Usage: "Show extended statistics"}

	nonverboseFlag = cli.BoolFlag{Name: "non-verbose,nv", Usage: "Non-verbose (quiet) output, minimized reporting, fewer warnings"}

	silentFlag = cli.BoolFlag{
		Name:  "silent",
		Usage: "Server-side flag, an indication for aistore _not_ to log assorted errors (e.g., HEAD(object) failures)",
	}

	averageSizeFlag = cli.BoolFlag{Name: "average-size", Usage: "Show average GET, PUT, etc. request size"}

	ignoreErrorFlag = cli.BoolFlag{
		Name:  "ignore-error",
		Usage: "Ignore \"soft\" failures such as \"bucket already exists\", etc.",
	}

	bucketPropsFlag = cli.StringFlag{
		Name: "props",
		Usage: "Create bucket with the specified (non-default) properties, e.g.:\n" +
			indent1 + "\t* ais create ais://mmm --props=\"versioning.validate_warm_get=false versioning.synchronize=true\"\n" +
			indent1 + "\t* ais create ais://nnn --props='mirror.enabled=true mirror.copies=4 checksum.type=md5'\n" +
			indent1 + "\t* ais create s3://bbb --props='extra.cloud.profile=prod extra.cloud.endpoint=https://s3.example.com'\n" +
			"\tTips:\n" +
			indent1 + "\t  1) Use '--props' to override properties that a new bucket would normally inherit from cluster config at creation time.\n" +
			indent1 + "\t  2) Use '--props' to set up an existing cloud bucket with a custom profile and/or custom endpoint/region.\n" +
			indent1 + "\tSee also: 'ais bucket props show' and 'ais bucket props set'",
	}

	forceFlag = cli.BoolFlag{Name: "force,f", Usage: "Force execution of the command " + advancedUsageOnly}

	// space cleanup

	forceClnFlag = cli.BoolFlag{
		Name: forceFlag.Name,
		Usage: "Proceed with removing misplaced objects even if global rebalance (or local resilver) is running or was interrupted,\n" +
			indent1 + "\tor the node has recently restarted. Does not override the 'dont_cleanup_time' window or other flags",
	}

	rmZeroSizeFlag = cli.BoolFlag{Name: "rm-zero-size", Usage: "Remove zero size objects " + advancedUsageOnly}

	keepMisplacedFlag = cli.BoolFlag{
		Name: "keep-misplaced",
		Usage: "Do not remove misplaced objects (default: remove after 'dont_cleanup_time' grace period)\n" +
			indent1 + "\tTip: use 'ais config cluster log.modules space' to enable logging for dry-run visibility",
	}

	smallSizeFlag = cli.StringFlag{
		Name:  "small-size",
		Usage: "Count and report all objects that are smaller or equal in size (e.g.: 4, 4b, 1k, 128kib; default: 0)",
	}
	largeSizeFlag = cli.StringFlag{
		Name:  "large-size",
		Usage: "Count and report all objects that are larger or equal in size  (e.g.: 4mb, 1MiB, 1048576, 128k; default: 5 GiB)",
	}

	// units enum { unitsIEC, unitsSI, unitsRaw }
	unitsFlag = cli.StringFlag{
		Name: "units",
		Usage: "Show statistics and/or parse command-line specified sizes using one of the following units of measurement:\n" +
			indent4 + "\tiec - IEC format, e.g.: KiB, MiB, GiB (default)\n" +
			indent4 + "\tsi  - SI (metric) format, e.g.: KB, MB, GB\n" +
			indent4 + "\traw - do not convert to (or from) human-readable format",
	}

	dateTimeFlag = cli.BoolFlag{
		Name:  "date-time",
		Usage: "Override the default hh:mm:ss (hours, minutes, seconds) time format - include calendar date as well",
	}

	topFlag = cli.IntFlag{
		Name:  "top",
		Usage: "Show top N most recent jobs (e.g., --top 5 to show the 5 most recent jobs)",
	}

	// list-objects
	startAfterFlag = cli.StringFlag{
		Name:  "start-after",
		Usage: "List bucket's content alphabetically starting with the first name _after_ the specified",
	}

	//
	// list-objects sizing and limiting
	//
	objLimitFlag = cli.IntFlag{
		Name: "limit",
		Usage: "The maximum number of objects to list, get, or otherwise handle (0 - unlimited; see also '--max-pages'),\n" +
			indent4 + "\te.g.:\n" +
			indent4 + "\t- 'ais ls gs://abc/dir --limit 1234 --cached --props size,custom,atime'\t- list no more than 1234 objects\n" +
			indent4 + "\t- 'ais get gs://abc /dev/null --prefix dir --limit 1234'\t- get --/--\n" +
			indent4 + "\t- 'ais scrub gs://abc/dir --limit 1234'\t- scrub --/--",
	}
	pageSizeFlag = cli.IntFlag{
		Name: "page-size",
		Usage: "Maximum number of object names per page; when the flag is omitted or 0\n" +
			indent4 + "\tthe maximum is defined by the corresponding backend; see also '--max-pages' and '--paged'",
	}
	maxPagesFlag = cli.IntFlag{
		Name: "max-pages",
		Usage: "Maximum number of pages to display (see also '--page-size' and '--limit')\n" +
			indent4 + "\te.g.: 'ais ls az://abc --paged --page-size 123 --max-pages 7",
	}
	pagedFlag = cli.BoolFlag{
		Name: "paged",
		Usage: "List objects page by page - one page at a time (see also '--page-size' and '--limit')\n" +
			indent4 + "\tnote: recommended for use with very large buckets",
	}
	countAndTimeFlag = cli.BoolFlag{
		Name:  "count-only",
		Usage: "Print only the resulting number of listed objects and elapsed time",
	}

	// show CHUNKED column (manifest-backed objects)
	chunkedColumnFlag = cli.BoolFlag{
		Name:  "chunked",
		Usage: "Include CHUNKED column indicating chunked storage. Also enabled by '--props all'.",
	}

	// bucket summary
	bckSummaryFlag = cli.BoolFlag{
		Name: "summary",
		Usage: "Show object numbers, bucket sizes, and used capacity;\n" +
			indent4 + "\tnote: applies only to buckets and objects that are _present_ in the cluster",
	}

	showUnmatchedFlag = cli.BoolFlag{
		Name:  "show-unmatched",
		Usage: "List also objects that were not matched by regex and/or template (range)",
	}
	diffFlag = cli.BoolFlag{
		Name: "diff",
		Usage: "Perform a bidirectional diff between in-cluster and remote content, which further entails:\n" +
			indent4 + "\t- detecting remote version changes (a.k.a. out-of-band updates), and\n" +
			indent4 + "\t- remotely deleted objects (out-of-band deletions (*));\n" +
			indent4 + "\t  the option requires remote backends supporting some form of versioning (e.g., object version, checksum, and/or ETag);\n" +
			indent4 + "\tsee related:\n" +
			indent4 + "\t     (*) options: --cached; --latest\n" +
			indent4 + "\t     commands:    'ais get --latest'; 'ais cp --sync'; 'ais prefetch --latest'",
	}

	useInventoryFlag = cli.BoolFlag{
		Name: "inventory",
		Usage: "List objects using _bucket inventory_ (docs/s3compat.md); requires s3:// backend; will provide significant performance\n" +
			indent4 + "\tboost when used with very large s3 buckets; e.g. usage:\n" +
			indent4 + "\t  1) 'ais ls s3://abc --inventory'\n" +
			indent4 + "\t  2) 'ais ls s3://abc --inventory --paged --prefix=subdir/'\n" +
			indent4 + "\t(see also: docs/s3compat.md)",
	}
	invNameFlag = cli.StringFlag{
		Name:  "inv-name", // compare w/ HdrInvName
		Usage: "Bucket inventory name (optional; system default name is '.inventory')",
	}
	invIDFlag = cli.StringFlag{
		Name:  "inv-id", // cpmpare w/ HdrInvID
		Usage: "Bucket inventory ID (optional; by default, we use bucket name as the bucket's inventory ID)",
	}

	keepMDFlag = cli.BoolFlag{Name: "keep-md,k", Usage: "Keep bucket metadata"}

	copiesFlag = cli.IntFlag{Name: "copies", Usage: "Number of object replicas", Value: 1, Required: true}

	dataSlicesFlag   = cli.IntFlag{Name: "data-slices,d", Usage: "Number of data slices"}
	paritySlicesFlag = cli.IntFlag{Name: "parity-slices,p", Usage: "Number of parity slices"}

	checkAndRecoverFlag = cli.BoolFlag{
		Name:  "recover",
		Usage: "Check and make sure that each and every object is properly erasure coded",
	}

	compactPropFlag = cli.BoolFlag{Name: "compact,c", Usage: "Display properties grouped in human-readable mode"}

	nameOnlyFlag = cli.BoolFlag{
		Name:  "name-only",
		Usage: "Faster request to retrieve only the names of objects (if defined, '--props' flag will be ignored)",
	}

	// Log severity (cmn.LogInfo, ....) enum
	logSevFlag = cli.StringFlag{
		Name: "severity",
		Usage: "Log severity is either 'i' or 'info' (default, can be omitted), or 'error', whereby error logs contain\n" +
			indent4 + "\tonly errors and warnings, e.g.: '--severity info', '--severity error', '--severity e'",
	}
	logFlushFlag = DurationFlag{
		Name:  "log-flush",
		Usage: "Can be used in combination with " + qflprn(refreshFlag) + " to override configured '" + nodeLogFlushName + "'",
		Value: logFlushTime,
	}

	// Download
	descJobFlag = cli.StringFlag{Name: "description,desc", Usage: "job description"}

	dloadTimeoutFlag = DurationFlag{
		Name: "download-timeout",
		Usage: "Server-side time limit for downloading a single file from remote source;\n" +
			indent4 + "\tvalid time units: " + timeUnits,
	}
	dloadProgressFlag = DurationFlag{
		Name: "progress-interval",
		Usage: "Download progress interval for continuous monitoring;\n" +
			indent4 + "\tvalid time units: " + timeUnits,
		Value: dload.DownloadProgressInterval,
	}

	limitConnectionsFlag = cli.IntFlag{
		Name:  "max-conns",
		Usage: "Maximum number of connections each target can make concurrently (up to num mountpaths)",
	}
	limitBytesPerHourFlag = cli.StringFlag{
		Name: "limit-bph",
		Usage: "Maximum download speed, or more exactly: maximum download size per target (node) per hour, e.g.:\n" +
			indent4 + "\t'--limit-bph 1GiB' (or same: '--limit-bph 1073741824');\n" +
			indent4 + "\tthe value is parsed in accordance with the '--units' (see '--units' for details);\n" +
			indent4 + "\tomitting the flag or specifying '--limit-bph 0' means that download won't be throttled",
	}
	objectsListFlag = cli.StringFlag{
		Name:  "object-list,from",
		Usage: "Path to file containing JSON array of object names to download",
	}

	// HuggingFace flags for downloading convenience
	hfModelFlag = cli.StringFlag{
		Name: "hf-model",
		Usage: "HuggingFace model repository name, e.g.:\n" +
			indent4 + "\t--hf-model bert-base-uncased\n" +
			indent4 + "\t--hf-model microsoft/DialoGPT-medium\n" +
			indent4 + "\t--hf-model openai/whisper-large-v2",
	}
	hfDatasetFlag = cli.StringFlag{
		Name: "hf-dataset",
		Usage: "HuggingFace dataset repository name, e.g.:\n" +
			indent4 + "\t--hf-dataset squad\n" +
			indent4 + "\t--hf-dataset glue\n" +
			indent4 + "\t--hf-dataset lhoestq/demo1",
	}
	hfFileFlag = cli.StringFlag{
		Name:  "hf-file",
		Usage: "Specific file to download from HF repository (optional, downloads entire repository if not specified)",
	}
	hfRevisionFlag = cli.StringFlag{
		Name:  "hf-revision",
		Value: "main",
		Usage: "HuggingFace repository revision/branch/tag (default: main)",
	}
	hfAuthFlag = cli.StringFlag{
		Name:   "hf-auth",
		Usage:  "HuggingFace authentication token for private repositories",
		EnvVar: "HF_TOKEN",
	}

	// latestVer and sync
	latestVerFlag = cli.BoolFlag{
		Name: "latest",
		Usage: "Check in-cluster metadata and, possibly, GET, download, prefetch, or otherwise copy the latest object version\n" +
			indent1 + "\tfrom the associated remote bucket;\n" +
			indent1 + "\tthe option provides operation-level control over object versioning (and version synchronization)\n" +
			indent1 + "\twithout the need to change the corresponding bucket configuration: 'versioning.validate_warm_get';\n" +
			indent1 + "\tsee also:\n" +
			indent1 + "\t\t- 'ais show bucket BUCKET versioning'\n" +
			indent1 + "\t\t- 'ais bucket props set BUCKET versioning'\n" +
			indent1 + "\t\t- 'ais ls --check-versions'\n" +
			indent1 + "\tsupported commands include:\n" +
			indent1 + "\t\t- 'ais cp', 'ais prefetch', 'ais get', 'ais start rebalance'",
	}
	syncFlag = cli.BoolFlag{
		Name: "sync",
		Usage: "Fully synchronize in-cluster content of a given remote bucket with its (Cloud or remote AIS) source;\n" +
			indent1 + "\tthe option is, effectively, a stronger variant of the '--latest' (option):\n" +
			indent1 + "\tin addition to bringing existing in-cluster objects in-sync with their respective out-of-band updates (if any)\n" +
			indent1 + "\tit also entails removing in-cluster objects that are no longer present remotely;\n" +
			indent1 + "\tlike '--latest', this option provides operation-level control over synchronization\n" +
			indent1 + "\twithout requiring to change the corresponding bucket configuration: 'versioning.synchronize';\n" +
			indent1 + "\tsee also:\n" +
			indent1 + "\t\t- 'ais show bucket BUCKET versioning'\n" +
			indent1 + "\t\t- 'ais bucket props set BUCKET versioning'\n" +
			indent1 + "\t\t- 'ais start rebalance'\n" +
			indent1 + "\t\t- 'ais ls --check-versions'",
	}

	// gen-shards
	fsizeFlag  = cli.StringFlag{Name: "fsize", Value: "1024", Usage: "Size of the files in a shard"}
	fcountFlag = cli.IntFlag{Name: "fcount", Value: 5, Usage: "Number of files in a shard"}

	dfltFext  = ".test"
	fextsFlag = cli.StringFlag{
		Name: "fext",
		Usage: "Comma-separated list of file extensions (default \"" + dfltFext + "\"), e.g.:\n" +
			indent4 + "\t--fext .mp3\n" +
			indent4 + "\t--fext '.mp3,.json,.cls' (or, same: \".mp3,  .json,  .cls\")",
	}

	dfltTform = "Unknown"
	tformFlag = cli.StringFlag{
		Name:  "tform",
		Usage: "TAR file format selection (one of \"" + dfltTform + "\", \"USTAR\", \"PAX\", or \"GNU\")",
	}

	// (ETL, dSort, get-batch) specification
	// see also: lhotseManifestFlag
	specFlag = cli.StringFlag{Name: "spec,f", Value: "", Usage: "Path to JSON or YAML request specification"}

	dsortLogFlag = cli.StringFlag{Name: "log", Usage: "Filename to log metrics (statistics)"}

	cleanupFlag = cli.BoolFlag{
		Name:  "cleanup",
		Usage: "Remove old bucket and create it again (warning: removes the entire content of the old bucket)",
	}

	// waiting
	waitJobXactFinishedFlag = DurationFlag{
		Name: "timeout",
		Usage: "Maximum time to wait for a job to finish; if omitted: wait forever or until Ctrl-C;\n" +
			indent4 + "\tvalid time units: " + timeUnits,
	}
	waitPodReadyTimeoutFlag = DurationFlag{
		Name: "init-timeout",
		Usage: "AIS target waiting time for POD to become ready;\n" +
			indent4 + "\tvalid time units: " + timeUnits,
	}
	waitFlag = cli.BoolFlag{
		Name:  "wait",
		Usage: "Wait for an asynchronous operation to finish (optionally, use '--timeout' to limit the waiting time)",
	}
	dontWaitFlag = cli.BoolFlag{
		Name: "dont-wait",
		Usage: "When _summarizing_ buckets do not wait for the respective job to finish -\n" +
			indent4 + "\tuse the job's UUID to query the results interactively",
	}

	// multi-object / multi-file
	listFlag = cli.StringFlag{
		Name: "list",
		Usage: "Comma-separated list of object or file names, e.g.:\n" +
			indent4 + "\t--list 'o1,o2,o3'\n" +
			indent4 + "\t--list \"abc/1.tar, abc/1.cls, abc/1.jpeg\"\n" +
			indent4 + "\tor, when listing files and/or directories:\n" +
			indent4 + "\t--list \"/home/docs, /home/abc/1.tar, /home/abc/1.jpeg\"",
	}
	templateFlag = cli.StringFlag{ // see also: outputTemplateFlag
		Name: "template",
		Usage: "Template to match object or file names; may contain prefix (that could be empty) with zero or more ranges\n" +
			"\t(with optional steps and gaps), e.g.:\n" +
			indent4 + "\t--template \"\" # (an empty or '*' template matches everything)\n" +
			indent4 + "\t--template 'dir/subdir/'\n" +
			indent4 + "\t--template 'shard-{1000..9999}.tar'\n" +
			indent4 + "\t--template \"prefix-{0010..0013..2}-gap-{1..2}-suffix\"\n" +
			indent4 + "\tand similarly, when specifying files and directories:\n" +
			indent4 + "\t--template '/home/dir/subdir/'\n" +
			indent4 + "\t--template \"/abc/prefix-{0010..9999..2}-suffix\"",
	}

	listRangeProgressWaitFlags = []cli.Flag{
		listFlag,
		templateFlag,
		waitFlag,
		waitJobXactFinishedFlag,
		progressFlag,
		refreshFlag,
	}

	// read range (aka range read)
	offsetFlag = cli.StringFlag{
		Name:  "offset",
		Usage: "Object read offset; must be used together with '--length'; default formatting: IEC (use '--units' to override)"}
	lengthFlag = cli.StringFlag{
		Name:  "length",
		Usage: "Object read length; default formatting: IEC (use '--units' to override)",
	}

	// NOTE:
	// In many cases, stating that a given object "is present" will sound more appropriate and,
	// in fact, accurate then "object is cached". The latter comes with a certain implied sense
	// that, if not accessed for a while, the object may suddenly disappear. This is, generally
	// speaking, not true for AIStore where LRU eviction is per-bucket configurable with default
	// settings inherited from the cluster config, etc.
	// See also: apc.Flt* enum.
	headObjPresentFlag = cli.BoolFlag{
		Name: "check-cached",
		Usage: "Check whether a given named object is present in cluster\n" +
			indent1 + "\t(applies only to buckets with remote backend)",
	}

	//
	// a group of (only cached | only not-cached) flags
	//

	_onlyin        = "in-cluster objects, i.e., objects from the respective remote bucket that are present (\"cached\") in the cluster"
	_onlyout       = "not in-cluster objects, i.e., objects from the respective remote bucket that are not present (\"not cached\") in the cluster"
	listCachedFlag = cli.BoolFlag{
		Name:  "cached",
		Usage: "Only list " + _onlyin,
	}
	getObjCachedFlag = cli.BoolFlag{
		Name:  listCachedFlag.Name,
		Usage: "Only get " + _onlyin,
	}
	scrubObjCachedFlag = cli.BoolFlag{
		Name:  listCachedFlag.Name,
		Usage: "Only visit " + _onlyin,
	}

	// when '--all' is used for/by another flag
	objNotCachedPropsFlag = cli.BoolFlag{
		Name:  "not-cached",
		Usage: "Show properties of _all_ objects from a remote bucket including those (objects) that are not present (not \"cached\")",
	}
	listNotCachedFlag = cli.BoolFlag{
		Name:  "not-cached",
		Usage: "Only list " + _onlyout,
	}

	dontHeadSrcDstBucketsFlag = cli.BoolFlag{
		Name:  "skip-lookup",
		Usage: "Skip checking source and destination buckets' existence (trading off extra lookup for performance)\n",
	}
	dontHeadRemoteFlag = cli.BoolFlag{
		Name: "skip-lookup",
		Usage: "Do not execute HEAD(bucket) request to lookup remote bucket and its properties; possible usage scenarios include:\n" +
			indent4 + "\t 1) adding remote bucket to aistore without first checking the bucket's accessibility\n" +
			indent4 + "\t    (e.g., to configure the bucket's aistore properties with alternative security profile and/or endpoint)\n" +
			indent4 + "\t 2) listing public-access Cloud buckets where certain operations (e.g., 'HEAD(bucket)') may be disallowed",
	}
	dontAddRemoteFlag = cli.BoolFlag{
		Name: "dont-add",
		Usage: "List remote bucket without adding it to cluster's metadata - e.g.:\n" +
			indent1 + "\t  - let's say, s3://abc is accessible but not present in the cluster (e.g., 'ais ls' returns error);\n" +
			indent1 + "\t  - then, if we ask aistore to list remote buckets: `ais ls s3://abc --all'\n" +
			indent1 + "\t    the bucket will be added (in effect, it'll be created);\n" +
			indent1 + "\t  - to prevent this from happening, either use this '--dont-add' flag or run 'ais evict' command later",
	}
	addRemoteFlag = cli.BoolFlag{
		Name: "add",
		Usage: "Add remote bucket to cluster's metadata\n" +
			indent1 + "\t  - let's say, s3://abc is accessible but not present in the cluster (e.g., 'ais ls' returns error);\n" +
			indent1 + "\t  - most of the time, there's no need to worry about it as aistore handles presence/non-presence\n" +
			indent1 + "\t    transparently behind the scenes;\n" +
			indent1 + "\t  - but if you do want to (explicltly) add the bucket, you could also use '--add' option",
	}

	enableFlag  = cli.BoolFlag{Name: "enable", Usage: "Enable"}
	disableFlag = cli.BoolFlag{Name: "disable", Usage: "Disable"}
	recursFlag  = cli.BoolFlag{Name: "recursive,r", Usage: "Recursive operation"}

	nonRecursFlag = cli.BoolFlag{
		Name: "non-recursive,nr",
		Usage: "Non-recursive operation, e.g.:\n" +
			"\t- 'ais ls gs://bck/sub --nr'\t- list objects and/or virtual subdirectories with names starting with the specified prefix;\n" +
			"\t- 'ais ls gs://bck/sub/ --nr'\t- list only immediate contents of 'sub/' subdirectory (non-recursive);\n" +
			"\t- 'ais prefetch s3://bck/abcd --nr'\t- prefetch a single named object;\n" +
			"\t- 'ais evict gs://bck/sub/ --nr'\t- evict only immediate contents of 'sub/' subdirectory (non-recursive);\n" +
			"\t- 'ais evict gs://bck --prefix=sub/ --nr'\t- same as above",
	}
	noDirsFlag = cli.BoolFlag{Name: "no-dirs", Usage: "Do not return virtual subdirectories (applies to remote buckets only)"}

	overwriteFlag = cli.BoolFlag{Name: "overwrite-dst,o", Usage: "Overwrite destination, if exists"}
	deleteSrcFlag = cli.BoolFlag{Name: "delete-src", Usage: "Delete successfully promoted source"}
	targetIDFlag  = cli.StringFlag{Name: "target-id", Usage: "AIS target designated to carry out the entire operation"}

	notFshareFlag = cli.BoolFlag{
		Name: "not-file-share",
		Usage: "Each target must act autonomously skipping file-share auto-detection and promoting the entire source " +
			"(as seen from the target)",
	}

	yesFlag = cli.BoolFlag{Name: "yes,y", Usage: "Assume 'yes' to all questions"}

	// usage: STDIN, blob
	chunkSizeFlag = cli.StringFlag{
		Name:  "chunk-size",
		Usage: "Chunk size in IEC or SI units, or \"raw\" bytes (e.g.: 4mb, 1MiB, 1048576, 128k; see '--units')",
	}

	// usage: rechunk
	objSizeLimitFlag = cli.StringFlag{
		Name:  "objsize-limit",
		Usage: "Object size threshold for chunking in IEC or SI units (e.g.: 50MiB, 100mb); objects >= this size will be chunked",
	}

	blobThresholdFlag = cli.StringFlag{
		Name: "blob-threshold",
		Usage: "Utilize built-in blob-downloader for remote objects greater than the specified (threshold) size\n" +
			indent1 + "\tin IEC or SI units, or \"raw\" bytes (e.g.: 4mb, 1MiB, 1048576, 128k; see '--units')",
	}

	blobDownloadFlag = cli.BoolFlag{
		Name:  apc.ActBlobDl,
		Usage: "Use blob-downloader to fetch large objects from remote backend into AIStore cluster (see docs/blob_downloader.md)",
	}

	mpdFlag = cli.BoolFlag{
		Name: "mpd",
		Usage: "Use multipart download to read large objects from AIStore cluster to the client-side;\n" +
			indent4 + "\tfor single-object download only; use '--chunk-size' and '--num-workers' to configure",
	}

	// num-workers
	noWorkers = indent4 + "\tuse (-1) to indicate single-threaded serial execution (ie., no workers);\n"

	numWorkersFlag = cli.IntFlag{
		Name: "num-workers",
		Usage: "Number of concurrent workers; if omitted or zero defaults to a number of target mountpaths (disks);\n" +
			noWorkers +
			indent4 + "\tany positive value will be adjusted _not_ to exceed the number of target CPUs",
	}
	numBlobWorkersFlag = cli.IntFlag{
		Name:  "num-workers",
		Usage: "Number of concurrent blob-downloading workers (readers); system default when omitted or zero",
	}
	numGenShardWorkersFlag = cli.IntFlag{
		Name:  "num-workers",
		Value: 10,
		Usage: "Number of concurrent shard-creating workers",
	}
	numPutWorkersFlag = cli.IntFlag{
		Name:  numBlobWorkersFlag.Name,
		Value: 10,
		Usage: "Number of concurrent client-side workers (to execute PUT or append requests);\n" +
			noWorkers +
			indent4 + "\tany positive value will be adjusted _not_ to exceed twice the number of client CPUs",
	}

	// validate
	cksumFlag = cli.BoolFlag{Name: "checksum", Usage: "Validate checksum"}

	// ais put
	putObjCksumText     = indent4 + "\tand provide it as part of the PUT request for subsequent validation on the server side"
	putObjCksumFlags    = initPutObjCksumFlags()
	putObjDfltCksumFlag = cli.BoolFlag{
		Name: "compute-checksum",
		Usage: "Compute client-side checksum - one of the supported checksum types that is currently configured for the destination bucket -\n" +
			putObjCksumText + "\n" +
			indent4 + "\t(see also: \"end-to-end protection\")",
	}
	putRetriesFlag = cli.IntFlag{
		Name:  "retries",
		Value: 1,
		Usage: "When failing to PUT retry the operation up to so many times (with increasing timeout if timed out)",
	}

	appendConcatFlag = cli.BoolFlag{
		Name:  "append",
		Usage: "Concatenate files: append a file or multiple files as a new _or_ to an existing object",
	}

	skipVerCksumFlag = cli.BoolFlag{
		Name:  "skip-vc",
		Usage: "Skip loading object metadata (and the associated checksum & version related processing)",
	}

	// auth
	descRoleFlag      = cli.StringFlag{Name: "description,desc", Usage: "Role description"}
	clusterRoleFlag   = cli.StringFlag{Name: "cluster", Usage: "Associate role with the specified AIS cluster"}
	clusterTokenFlag  = cli.StringFlag{Name: "cluster", Usage: "Issue token for the cluster"}
	bucketRoleFlag    = cli.StringFlag{Name: "bucket", Usage: "Associate a role with the specified bucket"}
	clusterFilterFlag = cli.StringFlag{
		Name:  "cluster",
		Usage: "Comma-separated list of AIS cluster IDs (type ',' for an empty cluster ID)",
	}

	// archive
	listArchFlag = cli.BoolFlag{Name: "archive", Usage: "List archived content (see docs/archive.md for details)"}

	archpathFlag = cli.StringFlag{ // for apc.QparamArchpath; PUT/append => shard
		Name:  "archpath",
		Usage: "Filename in an object (\"shard\") formatted as: " + archFormats,
	}
	archpathGetFlag = cli.StringFlag{ // for apc.QparamArchpath; GET from shard
		Name: archpathFlag.Name,
		Usage: "Extract the specified file from an object (\"shard\") formatted as: " + archFormats + ";\n" +
			indent4 + "\tsee also: '--archregx'",
	}
	archmimeFlag = cli.StringFlag{ // for apc.QparamArchmime
		Name: "archmime",
		Usage: "Expected format (mime type) of an object (\"shard\") formatted as " + archFormats + ";\n" +
			indent4 + "\tespecially usable for shards with non-standard extensions",
	}
	archregxFlag = cli.StringFlag{ // for apc.QparamArchregx
		Name: "archregx",
		Usage: "Specifies prefix, suffix, substring, WebDataset key, _or_ a general-purpose regular expression\n" +
			indent4 + "\tto select possibly multiple matching archived files from a given shard;\n" +
			indent4 + "\tis used in combination with '--archmode' (\"matching mode\") option",
	}
	archmodeFlag = cli.StringFlag{ // for apc.QparamArchmode
		Name: "archmode",
		Usage: "Enumerated \"matching mode\" that tells aistore how to handle '--archregx', one of:\n" +
			indent4 + "\t  * regexp - general purpose regular expression;\n" +
			indent4 + "\t  * prefix - matching filename starts with;\n" +
			indent4 + "\t  * suffix - matching filename ends with;\n" +
			indent4 + "\t  * substr - matching filename contains;\n" +
			indent4 + "\t  * wdskey - WebDataset key\n" +
			indent4 + "\texample:\n" +
			indent4 + "\t\tgiven a shard containing (subdir/aaa.jpg, subdir/aaa.json, subdir/bbb.jpg, subdir/bbb.json, ...)\n" +
			indent4 + "\t\tand wdskey=subdir/aaa, aistore will match and return (subdir/aaa.jpg, subdir/aaa.json)",
	}

	// client side
	extractFlag = cli.BoolFlag{
		Name:  "extract,x",
		Usage: "Extract all files from archive(s)",
	}

	inclSrcBucketNameFlag = cli.BoolFlag{
		Name:  "include-src-bck",
		Usage: "Prefix the names of archived files with the source bucket name",
	}
	omitSrcBucketNameFlag = cli.BoolFlag{
		Name:  "omit-src-bck",
		Usage: "When set, strip source bucket names from paths inside the archive (ie., use object names only)",
	}

	archSrcDirNameFlag = cli.BoolFlag{
		Name:  "include-src-dir",
		Usage: "Prefix the names of archived files with the (root) source directory",
	}
	putSrcDirNameFlag = cli.BoolFlag{
		Name:  archSrcDirNameFlag.Name,
		Usage: "Prefix destination object names with the source directory",
	}

	// 'ais archive put': conditional APPEND
	archAppendOrPutFlag = cli.BoolFlag{
		Name: "append-or-put",
		Usage: "Append to an existing destination object (\"archive\", \"shard\") iff exists; otherwise PUT a new archive (shard);\n" +
			indent4 + "\tnote that PUT (with subsequent overwrite if the destination exists) is the default behavior when the flag is omitted",
	}
	// 'ais archive put': unconditional APPEND: destination must exist
	archAppendOnlyFlag = cli.BoolFlag{
		Name:  "append",
		Usage: "Add newly archived content to the destination object (\"archive\", \"shard\") that must exist",
	}

	continueOnErrorFlag = cli.BoolFlag{
		Name:  "cont-on-err",
		Usage: "Keep running archiving xaction (job) in presence of errors in any given multi-object transaction",
	}
	// end archive

	// AuthN
	tokenFileFlag = cli.StringFlag{Name: "file,f", Value: "", Usage: "Path to file"}
	passwordFlag  = cli.StringFlag{Name: "password,p", Value: "", Usage: "User password"}
	expireFlag    = DurationFlag{
		Name: "expire,e",
		Usage: "Token expiration time, '0' - for never-expiring token;\n" +
			indent4 + "\tvalid time units: " + timeUnits,
		Value: 24 * time.Hour,
	}

	// Copy Bucket
	copyDryRunFlag = cli.BoolFlag{
		Name:  "dry-run",
		Usage: "Show total size of new objects without really creating them",
	}
	copyPrependFlag = cli.StringFlag{
		Name: "prepend",
		Usage: "Prefix to prepend to every object name during operation (copy or transform), e.g.:\n" +
			indent4 + "\t--prepend=abc\t- prefix all object names with \"abc\"\n" +
			indent4 + "\t--prepend=abc/\t- use \"abc\" as a virtual directory (note trailing filepath separator)\n" +
			indent4 + "\t\t- during 'copy', this flag applies to copied objects\n" +
			indent4 + "\t\t- during 'transform', this flag applies to transformed objects",
	}

	// ETL
	etlExtFlag  = cli.StringFlag{Name: "ext", Usage: "Mapping from old to new extensions of transformed objects' names"}
	etlNameFlag = cli.StringFlag{
		Name:  "name",
		Usage: "unique ETL name (leaving this field empty will have unique ID auto-generated)",
	}
	etlObjectRequestTimeout = DurationFlag{
		Name: "object-timeout",
		Usage: "Server-side timeout of transforming a single object;\n" +
			indent4 + "\tvalid time units: " + timeUnits,
	}
	etlTransformArgsFlag = cli.StringFlag{
		Name: "args",
		Usage: "Additional arguments applying to transform a single object;\n" +
			indent4 + "\t--args=abc\t- send \"etl_args=abc\" as query parameter in the single object transformation request",
	}
	commTypeFlag = cli.StringFlag{
		Name: "comm-type",
		Usage: "Enumerated communication type used between aistore cluster and ETL containers that run custom transformations:\n" +
			indent4 + "\t - 'hpush' or 'hpush://' - ETL container provides HTTP PUT handler that'll be invoked upon every request to transform\n" +
			indent4 + "\t -  '' - same as 'hpush://' (default, can be omitted)\n" +
			indent4 + "\t - 'hpull' or 'hpull://' - same, but ETL container is expected to provide HTTP GET endpoint\n" +
			indent4 + "\t - 'io' or 'io://' - for each request an aistore node will: run ETL container locally, write data\n" +
			indent4 + "\t   to its standard input and then read transformed data from the standard output\n" +
			indent4 + "\t For more details, see https://github.com/NVIDIA/aistore/blob/main/docs/etl.md#communication-mechanisms",
	}

	// Node
	roleFlag = cli.StringFlag{
		Name: "role", Required: true,
		Usage: "Role of this AIS daemon: proxy or target",
	}
	nonElectableFlag = cli.BoolFlag{
		Name:  "non-electable",
		Usage: "This proxy must not be elected as primary " + advancedUsageOnly,
	}
	noRebalanceFlag = cli.BoolFlag{
		Name:  "no-rebalance",
		Usage: "Do _not_ run global rebalance after putting node in maintenance " + advancedUsageOnly,
	}
	mountpathLabelFlag = cli.StringFlag{
		Name: "label",
		Usage: "An optional _mountpath label_ to facilitate extended functionality and context, including:\n" +
			indent2 + "1. device sharing (or non-sharing) between multiple mountpaths;\n" +
			indent2 + "2. associated storage class (one of the enumerated ones, as in: \"different storage media for different datasets\");\n" +
			indent2 + "3. parallelism multiplier - a number of goroutines to concurrently read, write, and/or traverse the mountpath in question\n" +
			indent2 + "   (e.g.: 'local-NVMe = 8', 'remote-NFS = 1', etc.);\n" +
			indent2 + "4. mapping of the mountpath to underlying block device(s)\n" +
			indent2 + "   (potentially useful in virtualized/containerized environments where '/sys/block/' wouldn't show a thing);\n" +
			indent2 + "5. user-defined grouping of the target mountpaths",
	}
	noResilverFlag = cli.BoolFlag{
		Name:  "no-resilver",
		Usage: "Do _not_ resilver data off of the mountpaths that are being disabled or detached",
	}
	noShutdownFlag = cli.BoolFlag{
		Name:  "no-shutdown",
		Usage: "Do not shutdown node upon decommissioning it from the cluster",
	}
	rmUserDataFlag = cli.BoolFlag{
		Name:  "rm-user-data",
		Usage: "Remove all user data when decommissioning node from the cluster",
	}
	keepInitialConfigFlag = cli.BoolFlag{
		Name: "keep-initial-config",
		Usage: "Keep the original plain-text configuration the node was deployed with\n" +
			indent4 + "\t(the option can be used to restart aisnode from scratch)",
	}

	transientFlag = cli.BoolFlag{
		Name:  "transient",
		Usage: "Update config in memory without storing the change(s) on disk",
	}

	setNewCustomMDFlag = cli.BoolFlag{
		Name:  "set-new-custom",
		Usage: "Remove existing custom keys (if any) and store new custom metadata",
	}

	cliConfigPathFlag = cli.BoolFlag{
		Name:  "path",
		Usage: "Display path to the AIS CLI configuration",
	}

	errorsOnlyFlag = cli.BoolFlag{
		Name:  "errors-only",
		Usage: "Reset only error counters",
	}

	diskSummaryFlag = cli.BoolFlag{
		Name:  "summary",
		Usage: "Tally up target disks to show per-target read/write summary stats and average utilizations",
	}
	mountpathFlag = cli.BoolFlag{
		Name:  "mountpath",
		Usage: "Show target mountpaths with underlying disks and used/available capacities",
	}

	// LRU
	lruBucketsFlag = cli.StringFlag{
		Name: "buckets",
		Usage: "Comma-separated list of bucket names, e.g.:\n" +
			indent1 + "\t\t\t--buckets 'ais://b1,ais://b2,ais://b3'\n" +
			indent1 + "\t\t\t--buckets \"gs://b1, s3://b2\"",
	}

	// special symbols (usage examples in docs/unicode.md)
	encodeObjnameFlag = cli.BoolFlag{
		Name:  "encode-objname",
		Usage: "Encode object names that contain special symbols (; : ' \" < > / \\ | ? #) that may otherwise break shell parsing or URL interpretation",
	}

	streamingGetFlag = cli.BoolFlag{
		Name:  "streaming",
		Usage: "stream the resulting archive prior to finalizing it in memory",
	}

	//
	// Lhotse
	//
	lhotseManifestFlag = cli.StringFlag{ // see also: specFlag
		Name:     "cuts",
		Usage:    "path to Lhotse cuts.jsonl or cuts.jsonl.gz or cuts.jsonl.lz4",
		Required: true,
	}
	sampleRateFlag = cli.IntFlag{
		Name:  "sample-rate",
		Usage: "audio sample-rate (Hz); used to convert sample offsets (in seconds) to byte offsets",
	}
	batchSizeFlag = cli.IntFlag{
		Name:  "batch-size",
		Usage: "number of cuts per output file",
	}
	outputTemplateFlag = cli.StringFlag{ // see also: (input) templateFlag
		Name:  "output-template",
		Usage: "template for multiple output files (e.g. 'batch-{001..999}.tar')",
	}

	outputTemplateForGenShards = cli.StringFlag{
		Name:  outputTemplateFlag.Name,
		Usage: "template for file names inside each shard (e.g. 'audio-{01..10}.wav')",
	}
)
