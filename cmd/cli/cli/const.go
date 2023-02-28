// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
//
// This file contains common constants and global variables
// (including all command-line options aka flags).
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/ext/dload"
	"github.com/NVIDIA/aistore/ext/dsort"
	"github.com/urfave/cli"
)

// top-level commands (categories - nouns)
const (
	commandAuth     = "auth"
	commandAdvanced = "advanced"
	commandBucket   = "bucket"
	commandObject   = "object"
	commandCluster  = "cluster"
	commandConfig   = "config"
	commandJob      = "job"
	commandLog      = "log"
	commandPerf     = "performance"
	commandStorage  = "storage"
	commandETL      = apc.ETL   // TODO: add `ais show etl`
	commandAlias    = "alias"   // TODO: ditto alias
	commandArch     = "archive" // TODO: ditto archive

	commandSearch = "search"
)

// top-level `show`
const (
	commandShow = "show"
)

// advanced command and subcommands
const (
	cmdGenShards = "gen-shards"
	cmdPreload   = "preload"
	cmdRmSmap    = "remove-from-smap"
)

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
	commandStart     = apc.ActXactStart
	commandStop      = apc.ActXactStop

	cmdSmap   = apc.WhatSmap
	cmdBMD    = apc.WhatBMD
	cmdConfig = apc.WhatConfig
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

	cmdDownload    = apc.ActDownload
	cmdRebalance   = apc.ActRebalance
	cmdLRU         = apc.ActLRU
	cmdStgCleanup  = "cleanup" // display name for apc.ActStoreCleanup
	cmdStgValidate = "validate"
	cmdSummary     = "summary" // ditto apc.ActSummaryBck

	cmdDsort = dsort.DSortName

	cmdCluster    = commandCluster
	cmdNode       = "node"
	cmdPrimary    = "set-primary"
	cmdList       = commandList
	cmdLogs       = "logs"
	cmdStop       = "stop"
	cmdStart      = "start"
	cmdMembership = "add-remove-nodes"
	cmdShutdown   = "shutdown"
	cmdAttach     = "attach"
	cmdDetach     = "detach"
	cmdResetStats = "reset-stats"

	// Cluster subcommands
	cmdCluAttach = "remote-" + cmdAttach
	cmdCluDetach = "remote-" + cmdDetach
	cmdCluConfig = "configure"
	cmdReset     = "reset"

	// Mountpath (disk) actions
	cmdMpathAttach  = cmdAttach
	cmdMpathEnable  = "enable"
	cmdMpathDetach  = cmdDetach
	cmdMpathDisable = "disable"

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
	cmdShowDisk       = apc.WhatDiskStats
	cmdShowCounters   = "counters"
	cmdShowThroughput = "throughput"
	cmdShowLatency    = "latency"

	// Bucket properties subcommands
	cmdSetBprops   = "set"
	cmdResetBprops = cmdReset

	// Archive subcommands
	cmdAppend = "append"

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

	// K8s subcommans
	cmdK8s        = "kubectl"
	cmdK8sSvc     = "svc"
	cmdK8sCluster = commandCluster

	// ETL subcommands
	cmdInit = "init"
	cmdSpec = "spec"
	cmdCode = "code"
	cmdSrc  = "source"

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

//
// misc constants
//

const (
	cluTotal = "---- CLUSTER:"
	tgtTotal = "-------- SUM:"
)

const (
	commandWait = "wait"

	// Default values for long running operations
	refreshRateDefault = 3 * time.Second
	refreshRateMinDur  = time.Second
	countDefault       = 1
	countUnlimited     = -1

	NilValue = "none"
)

const (
	timeUnits    = `ns, us (or µs), ms, s (default), m, h`
	sizeUnitsIEC = `(IEC units, e.g.: b or B, KB or KiB, MiB or mb, g or GB or GiB, etc.)`
)

const nodeLogFlushName = "log.flush_time"

const (
	tabtab     = "press <TAB-TAB> to select"
	tabHelpOpt = "press <TAB-TAB> to select, '--help' for options"
	tabHelpDet = "press <TAB-TAB> to select, '--help' for details"
)

// `ArgsUsage`: argument placeholders in help messages
const (
	argsUsageIndent = "   "

	// Job IDs (download, dsort)
	jobIDArgument                 = "JOB_ID"
	optionalJobIDArgument         = "[JOB_ID]"
	optionalJobIDDaemonIDArgument = "[JOB_ID [NODE_ID]]"

	jobShowStopWaitArgument  = "[NAME] [JOB_ID] [NODE_ID] [BUCKET]"
	jobShowRebalanceArgument = "[REB_ID] [NODE_ID]"

	// Perf
	showPerfArgument = "show performance counters, throughput, latency, and more (" + tabtab + " specific view)"

	// ETL
	etlNameArgument     = "ETL_NAME"
	etlNameListArgument = "ETL_NAME [ETL_NAME ...]"

	// key/value
	keyValuePairsArgument = "KEY=VALUE [KEY=VALUE...]"
	jsonKeyValueArgument  = "JSON-formatted-KEY-VALUE"
	jsonSpecArgument      = "JSON_SPECIFICATION"

	// Buckets
	bucketArgument         = "BUCKET"
	optionalBucketArgument = "[BUCKET]"
	bucketsArgument        = "BUCKET [BUCKET...]"
	bucketPropsArgument    = bucketArgument + " " + jsonKeyValueArgument + " | " + keyValuePairsArgument
	bucketAndPropsArgument = "BUCKET [PROP_PREFIX]"
	bucketSrcArgument      = "SRC_BUCKET"
	bucketDstArgument      = "DST_BUCKET"
	bucketNewArgument      = "NEW_BUCKET"

	// Objects
	getObjectArgument        = "BUCKET/OBJECT_NAME [OUT_FILE|-]"
	putPromoteObjectArgument = "FILE|DIRECTORY BUCKET/[OBJECT_NAME]"
	concatObjectArgument     = "FILE|DIRECTORY [FILE|DIRECTORY...] BUCKET/OBJECT_NAME"
	objectArgument           = "BUCKET/OBJECT_NAME"
	optionalObjectsArgument  = "BUCKET/[OBJECT_NAME]..."
	renameObjectArgument     = "BUCKET/OBJECT_NAME NEW_OBJECT_NAME"
	appendToArchArgument     = "FILE BUCKET/OBJECT_NAME"

	setCustomArgument = objectArgument + " " + jsonKeyValueArgument + " | " + keyValuePairsArgument + ", e.g.:\n" +
		argsUsageIndent +
		"mykey1=value1 mykey2=value2 OR '{\"mykey1\":\"value1\", \"mykey2\":\"value2\"}'"

	// nodes
	nodeIDArgument            = "NODE_ID"
	optionalNodeIDArgument    = "[NODE_ID]"
	optionalTargetIDArgument  = "[TARGET_ID]"
	joinNodeArgument          = "IP:PORT"
	nodeMountpathPairArgument = "NODE_ID=MOUNTPATH [NODE_ID=MOUNTPATH...]"

	// cluster
	showClusterArgument = "[NODE_ID] | [target [NODE_ID]] | [proxy [NODE_ID]] | " +
		"[smap [NODE_ID]] | [bmd [NODE_ID]] | [config [NODE_ID]] | [stats [NODE_ID]]"

	// config
	showConfigArgument = "cli | cluster [CONFIG SECTION OR PREFIX] |\n" +
		"      NODE_ID [ inherited | local | all [CONFIG SECTION OR PREFIX ] ]"
	showClusterConfigArgument = "[CONFIG_SECTION]"
	nodeConfigArgument        = nodeIDArgument + " " + keyValuePairsArgument

	// remais
	attachRemoteAISArgument = aliasURLPairArgument
	detachRemoteAISArgument = aliasArgument

	startDownloadArgument = "SOURCE DESTINATION"
	showStatsArgument     = "[NODE_ID]"

	// List command
	listAnyCommandArgument = "[PROVIDER://][BUCKET_NAME]"
	listObjCommandArgument = "[PROVIDER://]BUCKET_NAME"

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
	deleteAuthTokenArgument   = "TOKEN | TOKEN_FILE"

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
	// scope 'all'
	allPropsFlag        = cli.BoolFlag{Name: scopeAll, Usage: "all object properties"}
	allJobsFlag         = cli.BoolFlag{Name: scopeAll, Usage: "all jobs, including finished and aborted"}
	allRunningJobsFlag  = cli.BoolFlag{Name: scopeAll, Usage: "all running jobs"}
	allFinishedJobsFlag = cli.BoolFlag{Name: scopeAll, Usage: "all finished jobs"}
	allColumnsFlag      = cli.BoolFlag{
		Name:  scopeAll,
		Usage: "when printing tables, show all columns including those that have only zero values",
	}

	allObjsOrBcksFlag = cli.BoolFlag{
		Name: scopeAll,
		Usage: "depending on context: all objects (including misplaced ones and copies) or " +
			"all buckets (including remote buckets that are not present in the cluster)",
	}

	objPropsFlag = cli.StringFlag{
		Name: "props",
		Usage: "comma-separated list of object properties including name, size, version, copies, and more; e.g.: " +
			"'--props all', '--props name,size,cached', '--props ec,copies,custom,location'",
	}

	prefixFlag = cli.StringFlag{Name: "prefix", Usage: "prefix to match"}

	//
	// longRunFlags
	//
	refreshFlag = DurationFlag{
		Name:  "refresh,repeat",
		Usage: "interval for continuous monitoring (valid time units: " + timeUnits + ")",
	}
	countFlag = cli.IntFlag{
		Name:  "count",
		Usage: "used together with " + qflprn(refreshFlag) + " to limit the number of generated reports",
	}
	longRunFlags = []cli.Flag{refreshFlag, countFlag}

	//
	// regex and friends
	//
	regexFlag     = cli.StringFlag{Name: "regex", Usage: "regular expression to match and select items in question"}
	regexColsFlag = cli.StringFlag{
		Name:  regexFlag.Name,
		Usage: "regular expression to select table columns (case-insensitive), e.g.: --regex \"put|err\"",
	}
	regexStatsFlag = cli.StringFlag{
		Name:  regexFlag.Name,
		Usage: "regular expression to select stats metrics, e.g.: --regex \"put|get\"",
	}
	regexJobsFlag = cli.StringFlag{
		Name:  regexFlag.Name,
		Usage: "regular expression to select jobs by name, kind, or description, e.g.: --regex \"ec|mirror|elect\"",
	}

	jsonFlag     = cli.BoolFlag{Name: "json,j", Usage: "json input/output"}
	noHeaderFlag = cli.BoolFlag{Name: "no-headers,no-header,H", Usage: "display tables without headers"}
	noFooterFlag = cli.BoolFlag{Name: "no-footers,no-footer", Usage: "display tables without footers"}

	progressBarFlag = cli.BoolFlag{Name: "progress", Usage: "display progress bar"}
	dryRunFlag      = cli.BoolFlag{Name: "dry-run", Usage: "preview the results without really running the action"}
	verboseFlag     = cli.BoolFlag{Name: "verbose,v", Usage: "verbose"}
	nonverboseFlag  = cli.BoolFlag{Name: "non-verbose,nv", Usage: "non-verbose"}

	averageSizeFlag = cli.BoolFlag{Name: "average-size", Usage: "show average GET, PUT, etc. request size"}

	ignoreErrorFlag = cli.BoolFlag{
		Name:  "ignore-error",
		Usage: "ignore \"soft\" failures, such as \"bucket already exists\", etc.",
	}

	bucketPropsFlag = cli.StringFlag{
		Name:  "props",
		Usage: "bucket properties, e.g. --props=\"mirror.enabled=true mirror.copies=4 checksum.type=md5\"",
	}

	forceFlag = cli.BoolFlag{Name: "force,f", Usage: "force an action"}

	// units enum { unitsIEC, unitsSI, unitsRaw }
	unitsFlag = cli.StringFlag{
		Name: "units",
		Usage: "show statistics using on of the following units of measurement: (iec, si, raw), where:\n" +
			argsUsageIndent + argsUsageIndent + "iec - IEC format, e.g.: KiB, MiB, GiB (default)\n" +
			argsUsageIndent + argsUsageIndent + "si  - SI (metric) format, e.g.: KB, MB, GB\n" +
			argsUsageIndent + argsUsageIndent + "raw - do not convert to human-readable format, show exact (raw) values (nanoseconds, bytes)",
	}

	// Bucket
	startAfterFlag = cli.StringFlag{
		Name:  "start-after",
		Usage: "list bucket's content alphabetically starting with the first name _after_ the specified",
	}
	objLimitFlag = cli.IntFlag{Name: "limit", Usage: "limit object name count (0 - unlimited)", Value: 0}
	pageSizeFlag = cli.IntFlag{
		Name:  "page-size",
		Usage: "maximum number of names per page (0 - the maximum is defined by the corresponding backend)",
		Value: 0,
	}
	copiesFlag   = cli.IntFlag{Name: "copies", Usage: "number of object replicas", Value: 1, Required: true}
	maxPagesFlag = cli.IntFlag{Name: "max-pages", Usage: "display up to this number pages of bucket objects"}

	validateSummaryFlag = cli.BoolFlag{
		Name:  "validate",
		Usage: "perform checks (correctness of placement, number of copies, and more) and show the corresponding error counts",
	}
	bckSummaryFlag = cli.BoolFlag{
		Name: "summary",
		Usage: "show bucket sizes and used capacity; by default, applies only to the buckets that are _present_ in the cluster " +
			"(use '--all' option to override)",
	}
	pagedFlag = cli.BoolFlag{
		Name:  "paged",
		Usage: "list objects page by page, one page at a time (see also '--page-size' and '--limit')",
	}
	showUnmatchedFlag = cli.BoolFlag{Name: "show-unmatched", Usage: "list objects that were not matched by regex and template"}

	keepMDFlag       = cli.BoolFlag{Name: "keep-md", Usage: "keep bucket metadata"}
	dataSlicesFlag   = cli.IntFlag{Name: "data-slices,data,d", Usage: "number of data slices", Required: true}
	paritySlicesFlag = cli.IntFlag{Name: "parity-slices,parity,p", Usage: "number of parity slices", Required: true}
	listBucketsFlag  = cli.StringFlag{Name: "buckets", Usage: "comma-separated list of bucket names, e.g.: 'b1,b2,b3'"}
	compactPropFlag  = cli.BoolFlag{Name: "compact,c", Usage: "display properties grouped in human-readable mode"}

	nameOnlyFlag = cli.BoolFlag{
		Name:  "name-only",
		Usage: "fast request to retrieve only the names of objects (if defined, '--props' value will be ignored)",
	}

	// Log severity (cmn.LogInfo, ....) enum
	logSevFlag   = cli.StringFlag{Name: "severity", Usage: "show the specified log, one of: 'i[nfo]','w[arning]','e[rror]'"}
	logFlushFlag = DurationFlag{
		Name:  "log-flush",
		Usage: "can be used in combination with " + qflprn(refreshFlag) + " to override configured '" + nodeLogFlushName + "'",
		Value: 10 * time.Second,
	}

	// Download
	descJobFlag = cli.StringFlag{Name: "description,desc", Usage: "job description"}

	timeoutFlag = cli.StringFlag{ // TODO -- FIXME: must be DurationFlag
		Name:  "timeout",
		Usage: "timeout (valid time units: " + timeUnits + ")",
	}
	progressIntervalFlag = cli.StringFlag{ // TODO ditto
		Name:  "progress-interval",
		Usage: "progress interval for continuous monitoring (valid time units: " + timeUnits + ")",
		Value: dload.DownloadProgressInterval.String(),
	}

	limitConnectionsFlag = cli.IntFlag{
		Name:  "max-conns",
		Usage: "max number of connections each target can make concurrently (up to num mountpaths)",
	}
	limitBytesPerHourFlag = cli.StringFlag{
		Name:  "limit-bph",
		Usage: "max downloaded size per target per hour " + sizeUnitsIEC,
	}
	objectsListFlag = cli.StringFlag{
		Name:  "object-list,from",
		Usage: "path to file containing JSON array of object names to download",
	}
	syncFlag = cli.BoolFlag{Name: "sync", Usage: "sync bucket with Cloud"}

	// dSort
	dsortFsizeFlag  = cli.StringFlag{Name: "fsize", Value: "1024", Usage: "size of the files in a shard"}
	dsortLogFlag    = cli.StringFlag{Name: "log", Usage: "path to file where the metrics will be saved"}
	dsortFcountFlag = cli.IntFlag{Name: "fcount", Value: 5, Usage: "number of files inside single shard"}
	dsortSpecFlag   = cli.StringFlag{Name: "file,f", Value: "", Usage: "path to file with dSort specification"}

	cleanupFlag = cli.BoolFlag{
		Name:  "cleanup",
		Usage: "remove old bucket and create it again (warning: removes the entire content of the old bucket)",
	}
	concurrencyFlag = cli.IntFlag{
		Name: "conc", Value: 10,
		Usage: "limits number of concurrent put requests and number of concurrent shards created",
	}

	// multi-object
	listFlag     = cli.StringFlag{Name: "list", Usage: "comma-separated list of object names, e.g.: 'o1,o2,o3'"}
	templateFlag = cli.StringFlag{Name: "template", Usage: "template to select (matching) objects, e.g.: 'shard-{900..999}.tar'"}

	// Object
	offsetFlag = cli.StringFlag{Name: "offset", Usage: "object read offset " + sizeUnitsIEC}
	lengthFlag = cli.StringFlag{Name: "length", Usage: "object read length " + sizeUnitsIEC}

	// NOTE:
	// In many cases, stating that a given object "is present" will sound more appropriate and,
	// in fact, accurate then "object is cached". The latter comes with a certain implied sense
	// that, if not accessed for a while, the object may suddenly disappear. This is, generally
	// speaking, not true for AIStore where LRU eviction is per-bucket configurable with default
	// settings inherited from the cluster config, etc. etc.
	// See also: apc.Flt* enum.
	checkObjCachedFlag = cli.BoolFlag{
		Name:  "cached",
		Usage: "check if object from a remote bucket is present (aka \"cached\")",
	}
	listObjCachedFlag = cli.BoolFlag{
		Name:  "cached",
		Usage: "list only those objects from a remote bucket that are present (\"cached\")",
	}
	objNotCachedFlag = cli.BoolFlag{
		Name:  "not-cached",
		Usage: "list or show properties of all objects from a remote bucket, including those that are not present (not \"cached\")",
	}

	// to anonymously list public-access Cloud buckets
	listAnonymousFlag = cli.BoolFlag{
		Name:  "anonymous",
		Usage: "list public-access Cloud buckets that may disallow certain operations (e.g., 'HEAD(bucket)')",
	}

	enableFlag    = cli.BoolFlag{Name: "enable", Usage: "enable"}
	disableFlag   = cli.BoolFlag{Name: "disable", Usage: "disable"}
	recursiveFlag = cli.BoolFlag{Name: "recursive,r", Usage: "recursive operation"}
	rmRfFlag      = cli.BoolFlag{Name: scopeAll, Usage: "remove all objects (use it with extreme caution!)"}

	overwriteFlag = cli.BoolFlag{Name: "overwrite-dst,o", Usage: "overwrite destination, if exists"}
	deleteSrcFlag = cli.BoolFlag{Name: "delete-src", Usage: "delete successfully promoted source"}
	targetIDFlag  = cli.StringFlag{Name: "target-id", Usage: "ais target designated to carry out the entire operation"}

	notFshareFlag = cli.BoolFlag{
		Name: "not-file-share",
		Usage: "each target must act autonomously skipping file-share auto-detection and promoting the entire source " +
			"(as seen from the target)",
	}

	yesFlag = cli.BoolFlag{Name: "yes,y", Usage: "assume 'yes' for all questions"}

	chunkSizeFlag = cli.StringFlag{
		Name:  "chunk-size",
		Usage: "chunk size " + sizeUnitsIEC, Value: "10MiB",
	}

	cksumFlag        = cli.BoolFlag{Name: "checksum", Usage: "validate checksum"}
	computeCksumFlag = cli.BoolFlag{Name: "compute-checksum", Usage: "compute checksum configured for the bucket"}
	skipVerCksumFlag = cli.BoolFlag{
		Name:  "skip-vc",
		Usage: "skip loading object metadata (and the associated checksum & version related processing)",
	}
	supportedCksumFlags = initSupportedCksumFlags()

	// auth
	descRoleFlag      = cli.StringFlag{Name: "description,desc", Usage: "role description"}
	clusterRoleFlag   = cli.StringFlag{Name: "cluster", Usage: "associate role with the specified AIS cluster"}
	clusterTokenFlag  = cli.StringFlag{Name: "cluster", Usage: "issue token for the cluster"}
	bucketRoleFlag    = cli.StringFlag{Name: "bucket", Usage: "associate a role with the specified bucket"}
	clusterFilterFlag = cli.StringFlag{
		Name:  "cluster",
		Usage: "comma-separated list of AIS cluster IDs (type ',' for an empty cluster ID)",
	}

	// archive
	listArchFlag   = cli.BoolFlag{Name: "archive", Usage: "list archived content (see docs/archive.md for details)"}
	createArchFlag = cli.BoolFlag{Name: "archive", Usage: "archive a list or a range of objects"}

	archpathOptionalFlag = cli.StringFlag{
		Name:  "archpath",
		Usage: "filename in archive",
	}
	archpathRequiredFlag = cli.StringFlag{
		Name:     archpathOptionalFlag.Name,
		Usage:    archpathOptionalFlag.Usage,
		Required: true,
	}

	includeSrcBucketNameFlag = cli.BoolFlag{
		Name:  "include-src-bck",
		Usage: "prefix names of archived objects with the source bucket name",
	}
	allowAppendToExistingFlag = cli.BoolFlag{
		Name:  "append-to-arch",
		Usage: "allow adding a list or a range of objects to an existing archive",
	}
	continueOnErrorFlag = cli.BoolFlag{
		Name:  "cont-on-err",
		Usage: "keep running archiving xaction in presence of errors in a any given multi-object transaction",
	}
	// end archive

	sourceBckFlag = cli.StringFlag{Name: "source-bck", Usage: "source bucket"}

	// AuthN
	tokenFileFlag = cli.StringFlag{Name: "file,f", Value: "", Usage: "path to file"}
	passwordFlag  = cli.StringFlag{Name: "password,p", Value: "", Usage: "user password"}
	expireFlag    = DurationFlag{
		Name:  "expire,e",
		Usage: "token expiration time, '0' - for never-expiring token (valid time units: " + timeUnits + ")",
		Value: 24 * time.Hour,
	}

	// Copy Bucket
	cpBckDryRunFlag = cli.BoolFlag{
		Name:  "dry-run",
		Usage: "show total size of new objects without really creating them",
	}
	cpBckPrefixFlag = cli.StringFlag{
		Name:  "prefix",
		Usage: "string to prepend every copied object's name, e.g.: '--prefix=abc/'",
	}

	// ETL
	etlExtFlag  = cli.StringFlag{Name: "ext", Usage: "mapping from old to new extensions of transformed objects' names"}
	etlNameFlag = cli.StringFlag{
		Name:     "name",
		Usage:    "unique ETL name (leaving this field empty will have unique ID auto-generated)",
		Required: true,
	}
	etlBucketRequestTimeout = DurationFlag{
		Name:  "request-timeout",
		Usage: "timeout for transforming a single object (valid time units: " + timeUnits + ")",
	}
	fromFileFlag = cli.StringFlag{
		Name:     "from-file",
		Usage:    "absolute path to the file with the spec/code for ETL",
		Required: true,
	}
	depsFileFlag = cli.StringFlag{
		Name:  "deps-file",
		Usage: "absolute path to the file with dependencies that must be installed before running the code",
	}
	runtimeFlag = cli.StringFlag{
		Name:  "runtime",
		Usage: "runtime which should be used when running the provided code", Required: true,
	}
	commTypeFlag = cli.StringFlag{
		Name:  "comm-type",
		Usage: "communication type which should be used when running the provided code",
	}
	funcTransformFlag = cli.StringFlag{
		Name:  "transform",
		Value: "transform", // NOTE: default name of the transform() function
		Usage: "receives and _transforms_ the payload",
	}

	waitTimeoutFlag = DurationFlag{
		Name:  "wait-timeout",
		Usage: "ais target waiting time for POD to become ready (valid time units: " + timeUnits + ")",
	}
	waitFlag = cli.BoolFlag{
		Name:  "wait",
		Usage: "wait until the operation is finished",
	}

	// Node
	roleFlag = cli.StringFlag{
		Name: "role", Required: true,
		Usage: "role of this AIS daemon: proxy or target",
	}
	noRebalanceFlag = cli.BoolFlag{
		Name:  "no-rebalance",
		Usage: "do _not_ run global rebalance after putting node in maintenance (advanced usage only!)",
	}
	noResilverFlag = cli.BoolFlag{
		Name:  "no-resilver",
		Usage: "do _not_ resilver data off of the mountpaths that are being disabled or detached",
	}
	noShutdownFlag = cli.BoolFlag{
		Name:  "no-shutdown",
		Usage: "do not shutdown node upon decommissioning it from the cluster",
	}
	rmUserDataFlag = cli.BoolFlag{
		Name:  "rm-user-data",
		Usage: "remove all user data when decommissioning node from the cluster",
	}

	baseLstRngFlags = []cli.Flag{
		listFlag,
		templateFlag,
	}

	transientFlag = cli.BoolFlag{
		Name:  "transient",
		Usage: "update config in memory without storing the change(s) on disk",
	}

	setNewCustomMDFlag = cli.BoolFlag{
		Name:  "set-new-custom",
		Usage: "remove existing custom keys (if any) and store new custom metadata",
	}

	cliConfigPathFlag = cli.BoolFlag{
		Name:  "path",
		Usage: "display path to the AIS CLI configuration",
	}

	errorsOnlyFlag = cli.BoolFlag{
		Name:  "errors-only",
		Usage: "reset only error counters",
	}

	diskSummaryFlag = cli.BoolFlag{
		Name:  "summary",
		Usage: "tally up target disks to show per-target read/write summary stats and average utilizations",
	}
	mountpathFlag = cli.BoolFlag{
		Name:  "mountpath",
		Usage: "show target mountpaths with underlying disks and used/available capacities",
	}
)
