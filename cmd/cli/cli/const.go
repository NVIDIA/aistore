// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
//
// This file contains common constants and global variables
// (including all command-line options aka flags).
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
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
	cmdGenShards     = "gen-shards"
	cmdPreload       = "preload"
	cmdRmSmap        = "remove-from-smap"
	cmdRandNode      = "random-node"
	cmdRandMountpath = "random-mountpath"
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
	commandWait      = "wait"

	cmdSmap   = apc.WhatSmap
	cmdBMD    = apc.WhatBMD
	cmdConfig = "config"
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
	fileStdIO = "-" // STDIN (for `ais put`), STDOUT (for `ais put`)
	discardIO = "/dev/null"
)

const (
	cluTotal = "--- Cluster:"
	tgtTotal = "------- Sum:"
)

const NilValue = "none"

const (
	defaultChunkSize = 10 * cos.MiB
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
	indent1 = "   " // indent4 et al.

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
	getObjectArgument       = "BUCKET[/OBJECT_NAME] [OUT_FILE|-]"
	putObjectArgument       = "[-|FILE|DIRECTORY[/PATTERN]] BUCKET[/OBJECT_NAME]"
	promoteObjectArgument   = "FILE|DIRECTORY[/PATTERN] BUCKET[/OBJECT_NAME]"
	concatObjectArgument    = "FILE|DIRECTORY[/PATTERN] [ FILE|DIRECTORY[/PATTERN] ...] BUCKET/OBJECT_NAME"
	objectArgument          = "BUCKET/OBJECT_NAME"
	optionalObjectsArgument = "BUCKET[/OBJECT_NAME]..."
	renameObjectArgument    = "BUCKET/OBJECT_NAME NEW_OBJECT_NAME"
	appendToArchArgument    = "FILE BUCKET/SHARD_NAME"

	setCustomArgument = objectArgument + " " + jsonKeyValueArgument + " | " + keyValuePairsArgument + ", e.g.:\n" +
		indent1 +
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
	listAnyCommandArgument = "PROVIDER:[//BUCKET_NAME]"
	listObjCommandArgument = "PROVIDER://BUCKET_NAME"

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
	indent2 = strings.Repeat(indent1, 2)
	indent4 = strings.Repeat(indent1, 4)

	archFormats = ".tar, .tgz or .tar.gz, .zip, .tar.lz4" // namely, archive.FileExtensions
	archExts    = "(" + archFormats + ")"

	//
	// scope 'all'
	//
	allPropsFlag        = cli.BoolFlag{Name: scopeAll, Usage: "all object properties including custom (user-defined)"}
	allJobsFlag         = cli.BoolFlag{Name: scopeAll, Usage: "all jobs, including finished and aborted"}
	allRunningJobsFlag  = cli.BoolFlag{Name: scopeAll, Usage: "all running jobs"}
	allFinishedJobsFlag = cli.BoolFlag{Name: scopeAll, Usage: "all finished jobs"}
	rmrfFlag            = cli.BoolFlag{Name: scopeAll, Usage: "remove all objects (use it with extreme caution!)"}

	allColumnsFlag = cli.BoolFlag{
		Name:  scopeAll,
		Usage: "when printing tables, show all columns including those that have only zero values",
	}
	allObjsOrBcksFlag = cli.BoolFlag{
		Name: scopeAll,
		Usage: "depending on the context:\n" +
			indent4 + "\t- all objects in a given bucket, including misplaced and copies, or\n" +
			indent4 + "\t- all buckets, including accessible (visible) remote buckets that are _not present_ in the cluster",
	}
	allBcksFlag = cli.BoolFlag{
		Name:  scopeAll,
		Usage: "all buckets, including accessible remote buckets that are not present in the cluster",
	}
	copyAllObjsFlag = cli.BoolFlag{
		Name:  scopeAll,
		Usage: "copy all objects from a remote bucket including those that are not present (not \"cached\") in the cluster",
	}

	// obj props
	objPropsFlag = cli.StringFlag{
		Name: "props",
		Usage: "comma-separated list of object properties including name, size, version, copies, and more; e.g.:\n" +
			indent4 + "\t--props all\n" +
			indent4 + "\t--props name,size,cached\n" +
			indent4 + "\t--props \"ec, copies, custom, location\"",
	}

	// prefix (to match)
	listObjPrefixFlag = cli.StringFlag{
		Name: "prefix",
		Usage: "list objects that start with the specified prefix, e.g.:\n" +
			indent4 + "\t'--prefix a/b/c' - list virtual directory a/b/c and/or objects from the virtual directory\n" +
			indent4 + "\ta/b that have their names (relative to this directory) starting with the letter c",
	}
	getObjPrefixFlag = cli.StringFlag{
		Name: listObjPrefixFlag.Name,
		Usage: "get objects that start with the specified prefix, e.g.:\n" +
			indent4 + "\t'--prefix a/b/c' - get objects from the virtual directory a/b/c and objects from the virtual directory\n" +
			indent4 + "\ta/b that have their names (relative to this directory) starting with c;\n" +
			indent4 + "\t'--prefix \"\"' - get entire bucket",
	}
	copyObjPrefixFlag = cli.StringFlag{
		Name: "prefix",
		Usage: "copy objects that start with the specified prefix, e.g.:\n" +
			indent4 + "\t'--prefix a/b/c' - copy virtual directory a/b/c and/or objects from the virtual directory\n" +
			indent4 + "\ta/b that have their names (relative to this directory) starting with the letter c",
	}
	bsummPrefixFlag = cli.StringFlag{
		Name: "prefix",
		Usage: "for each bucket, select only those objects (names) that start with the specified prefix, e.g.:\n" +
			indent4 + "\t'--prefix a/b/c' - sum-up sizes of the virtual directory a/b/c and objects from the virtual directory\n" +
			indent4 + "\ta/b that have names (relative to this directory) starting with the letter c",
	}

	//
	// longRunFlags
	//
	refreshFlag = DurationFlag{
		Name: "refresh",
		Usage: "interval for continuous monitoring;\n" +
			indent4 + "\tvalid time units: " + timeUnits,
	}
	countFlag = cli.IntFlag{
		Name:  "count",
		Usage: "used together with " + qflprn(refreshFlag) + " to limit the number of generated reports",
	}
	longRunFlags = []cli.Flag{refreshFlag, countFlag}

	//
	// regex and friends
	//
	regexFlag = cli.StringFlag{Name: "regex", Usage: "regular expression to match and select items in question"}

	regexLsAnyFlag = cli.StringFlag{
		Name: regexFlag.Name,
		Usage: "regular expression; use it to match either bucket names or objects in a given bucket, e.g.:\n" +
			indent4 + "\tais ls --regex \"(m|n)\"\t- match buckets such as ais://nnn, s3://mmm, etc.;\n" +
			indent4 + "\tais ls ais://nnn --regex \"^A\"\t- match object names starting with letter A",
	}
	regexColsFlag = cli.StringFlag{
		Name:  regexFlag.Name,
		Usage: "regular expression to select table columns (case-insensitive), e.g.: --regex \"put|err\"",
	}
	regexJobsFlag = cli.StringFlag{
		Name:  regexFlag.Name,
		Usage: "regular expression to select jobs by name, kind, or description, e.g.: --regex \"ec|mirror|elect\"",
	}

	jsonFlag     = cli.BoolFlag{Name: "json,j", Usage: "json input/output"}
	noHeaderFlag = cli.BoolFlag{Name: "no-headers,H", Usage: "display tables without headers"}
	noFooterFlag = cli.BoolFlag{Name: "no-footers", Usage: "display tables without footers"}

	progressFlag   = cli.BoolFlag{Name: "progress", Usage: "show progress bar(s) and progress of execution in real time"}
	dryRunFlag     = cli.BoolFlag{Name: "dry-run", Usage: "preview the results without really running the action"}
	verboseFlag    = cli.BoolFlag{Name: "verbose,v", Usage: "verbose"}
	nonverboseFlag = cli.BoolFlag{Name: "non-verbose,nv", Usage: "non-verbose"}

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
		Usage: "show statistics and/or parse command-line specified sizes using one of the following _units of measurement_:\n" +
			indent4 + "\tiec - IEC format, e.g.: KiB, MiB, GiB (default)\n" +
			indent4 + "\tsi  - SI (metric) format, e.g.: KB, MB, GB\n" +
			indent4 + "\traw - do not convert to (or from) human-readable format",
	}

	// Bucket
	startAfterFlag = cli.StringFlag{
		Name:  "start-after",
		Usage: "list bucket's content alphabetically starting with the first name _after_ the specified",
	}
	objLimitFlag = cli.IntFlag{Name: "limit", Usage: "limit object name count (0 - unlimited)"}
	pageSizeFlag = cli.IntFlag{
		Name:  "page-size",
		Usage: "maximum number of names per page (0 - the maximum is defined by the corresponding backend)",
	}
	copiesFlag   = cli.IntFlag{Name: "copies", Usage: "number of object replicas", Value: 1, Required: true}
	maxPagesFlag = cli.IntFlag{Name: "max-pages", Usage: "display up to this number pages of bucket objects"}

	validateSummaryFlag = cli.BoolFlag{
		Name:  "validate",
		Usage: "perform checks (correctness of placement, number of copies, and more) and show the corresponding error counts",
	}
	bckSummaryFlag = cli.BoolFlag{
		Name:  "summary",
		Usage: "show bucket sizes and used capacity; applies _only_ to buckets and objects that are _present_ in the cluster",
	}
	pagedFlag = cli.BoolFlag{
		Name:  "paged",
		Usage: "list objects page by page, one page at a time (see also '--page-size' and '--limit')",
	}
	showUnmatchedFlag = cli.BoolFlag{Name: "show-unmatched", Usage: "list objects that were not matched by regex and template"}

	keepMDFlag       = cli.BoolFlag{Name: "keep-md", Usage: "keep bucket metadata"}
	dataSlicesFlag   = cli.IntFlag{Name: "data-slices,data,d", Usage: "number of data slices", Required: true}
	paritySlicesFlag = cli.IntFlag{Name: "parity-slices,parity,p", Usage: "number of parity slices", Required: true}
	compactPropFlag  = cli.BoolFlag{Name: "compact,c", Usage: "display properties grouped in human-readable mode"}

	nameOnlyFlag = cli.BoolFlag{
		Name:  "name-only",
		Usage: "faster request to retrieve only the names of objects (if defined, '--props' flag will be ignored)",
	}

	// Log severity (cmn.LogInfo, ....) enum
	logSevFlag   = cli.StringFlag{Name: "severity", Usage: "show the specified log, one of: 'i[nfo]','w[arning]','e[rror]'"}
	logFlushFlag = DurationFlag{
		Name:  "log-flush",
		Usage: "can be used in combination with " + qflprn(refreshFlag) + " to override configured '" + nodeLogFlushName + "'",
		Value: logFlushTime,
	}

	// Download
	descJobFlag = cli.StringFlag{Name: "description,desc", Usage: "job description"}

	dloadTimeoutFlag = cli.StringFlag{ // TODO -- FIXME: must be DurationFlag
		Name: "download-timeout",
		Usage: "server-side time limit for downloading a single file from remote source;\n" +
			indent4 + "\tvalid time units: " + timeUnits,
	}
	dloadProgressFlag = cli.StringFlag{ // TODO ditto
		Name: "progress-interval",
		Usage: "download progress interval for continuous monitoring;\n" +
			indent4 + "\tvalid time units: " + timeUnits,
		Value: dload.DownloadProgressInterval.String(),
	}

	limitConnectionsFlag = cli.IntFlag{
		Name:  "max-conns",
		Usage: "max number of connections each target can make concurrently (up to num mountpaths)",
	}
	limitBytesPerHourFlag = cli.StringFlag{
		Name: "limit-bph",
		Usage: "maximum download speed, or more exactly: maximum download size per target (node) per hour, e.g.:\n" +
			indent4 + "\t'--limit-bph 1GiB' (or same: '--limit-bph 1073741824');\n" +
			indent4 + "\tthe value is parsed in accordance with the '--units' (see '--units' for details);\n" +
			indent4 + "\tomitting the flag or (same) specifying '--limit-bph 0' means that download won't be throttled",
	}
	objectsListFlag = cli.StringFlag{
		Name:  "object-list,from",
		Usage: "path to file containing JSON array of object names to download",
	}
	syncFlag = cli.BoolFlag{Name: "sync", Usage: "sync bucket with Cloud"}

	// dSort
	dsortFsizeFlag  = cli.StringFlag{Name: "fsize", Value: "1024", Usage: "size of the files in a shard"}
	dsortLogFlag    = cli.StringFlag{Name: "log", Usage: "path to file where the metrics will be saved"}
	dsortFcountFlag = cli.IntFlag{Name: "fcount", Value: 5, Usage: "number of files in a shard"}
	dsortSpecFlag   = cli.StringFlag{Name: "file,f", Value: "", Usage: "path to file with dSort specification"}

	cleanupFlag = cli.BoolFlag{
		Name:  "cleanup",
		Usage: "remove old bucket and create it again (warning: removes the entire content of the old bucket)",
	}
	concurrencyFlag = cli.IntFlag{
		Name: "conc", Value: 10,
		Usage: "limits number of concurrent put requests and number of concurrent shards created",
	}

	// waiting
	waitPodReadyTimeoutFlag = DurationFlag{
		Name: "timeout",
		Usage: "ais target waiting time for POD to become ready;\n" +
			indent4 + "\tvalid time units: " + timeUnits,
	}
	waitJobXactFinishedFlag = DurationFlag{
		Name: "timeout",
		Usage: "maximum time to wait for a job to finish; if omitted: wait forever or until Ctrl-C;\n" +
			indent4 + "\tvalid time units: " + timeUnits,
	}
	waitFlag = cli.BoolFlag{
		Name:  "wait",
		Usage: "wait for an asynchronous operation to finish (optionally, use '--timeout' to limit the waiting time)",
	}

	// multi-object
	listFlag = cli.StringFlag{
		Name: "list",
		Usage: "comma-separated list of object names, e.g.:\n" +
			indent4 + "\t--list 'o1,o2,o3'\n" +
			indent4 + "\t--list \"abc/1.tar, abc/1.cls, abc/1.jpeg\"",
	}
	listFileFlag = cli.StringFlag{
		Name: "list",
		Usage: "comma-separated list of file names, e.g.:\n" +
			indent4 + "\t--list 'f1,f2,f3'\n" +
			indent4 + "\t--list \"/home/abc/1.tar, /home/abc/1.cls, /home/abc/1.jpeg\"",
	}
	templateFlag = cli.StringFlag{
		Name: "template",
		Usage: "template to match object names; may contain prefix with zero or more ranges (with optional steps and gaps), e.g.:\n" +
			indent4 + "\t--template 'dir/subdir/'\n" +
			indent4 + "\t--template 'shard-{1000..9999}.tar'\n" +
			indent4 + "\t--template \"prefix-{0010..0013..2}-gap-{1..2}-suffix\"\n" +
			indent4 + "\t--template \"prefix-{0010..9999..2}-suffix\"",
	}
	templateFileFlag = cli.StringFlag{
		Name: "template",
		Usage: "template to match file names; may contain prefix with zero or more ranges (with optional steps and gaps), e.g.:\n" +
			indent4 + "\t--template '/home/dir/subdir/'\n" +
			indent4 + "\t--template 'shard-{1000..9999}.tar'\n" +
			indent4 + "\t--template \"prefix-{0010..0013..2}-gap-{1..2}-suffix\"\n" +
			indent4 + "\t--template \"prefix-{0010..9999..2}-suffix\"",
	}

	listrangeFlags = []cli.Flag{
		listFlag,
		templateFlag,
		waitFlag,
		waitJobXactFinishedFlag,
		progressFlag,
		refreshFlag,
	}
	listrangeFileFlags = []cli.Flag{
		listFileFlag,
		templateFileFlag,
		progressFlag,
		refreshFlag,
	}

	// read range (aka range read)
	offsetFlag = cli.StringFlag{
		Name:  "offset",
		Usage: "object read offset; must be used together with '--length'; default formatting: IEC (use '--units' to override)"}
	lengthFlag = cli.StringFlag{
		Name:  "length",
		Usage: "object read length; default formatting: IEC (use '--units' to override)",
	}

	// NOTE:
	// In many cases, stating that a given object "is present" will sound more appropriate and,
	// in fact, accurate then "object is cached". The latter comes with a certain implied sense
	// that, if not accessed for a while, the object may suddenly disappear. This is, generally
	// speaking, not true for AIStore where LRU eviction is per-bucket configurable with default
	// settings inherited from the cluster config, etc. etc.
	// See also: apc.Flt* enum.
	checkObjCachedFlag = cli.BoolFlag{
		Name:  "check-cached",
		Usage: "check if a given object from a remote bucket is present (\"cached\") in AIS",
	}
	listObjCachedFlag = cli.BoolFlag{
		Name:  "cached",
		Usage: "list only those objects from a remote bucket that are present (\"cached\")",
	}
	getObjCachedFlag = cli.BoolFlag{
		Name:  "cached",
		Usage: "get only those objects from a remote bucket that are present (\"cached\") in AIS",
	}
	// when '--all' is used for/by another flag
	objNotCachedPropsFlag = cli.BoolFlag{
		Name:  "not-cached",
		Usage: "show properties of _all_ objects from a remote bucket including those (objects) that are not present (not \"cached\")",
	}
	// to anonymously list public-access Cloud buckets
	listAnonymousFlag = cli.BoolFlag{
		Name:  "anonymous",
		Usage: "list public-access Cloud buckets that may disallow certain operations (e.g., 'HEAD(bucket)')",
	}

	enableFlag  = cli.BoolFlag{Name: "enable", Usage: "enable"}
	disableFlag = cli.BoolFlag{Name: "disable", Usage: "disable"}
	recursFlag  = cli.BoolFlag{Name: "recursive,r", Usage: "recursive operation"}

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
		Usage: "chunk size in IEC or SI units, or \"raw\" bytes (e.g.: 1MiB or 1048576; see '--units')",
	}

	cksumFlag = cli.BoolFlag{Name: "checksum", Usage: "validate checksum"}

	putObjCksumText     = indent4 + "\tand provide it as part of the PUT request for subsequent validation on the server side"
	putObjCksumFlags    = initPutObjCksumFlags()
	putObjDfltCksumFlag = cli.BoolFlag{
		Name: "compute-checksum",
		Usage: "[end-to-end protection] compute client-side checksum configured for the destination bucket\n" +
			putObjCksumText,
	}

	skipVerCksumFlag = cli.BoolFlag{
		Name:  "skip-vc",
		Usage: "skip loading object metadata (and the associated checksum & version related processing)",
	}

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
	listArchFlag = cli.BoolFlag{Name: "archive", Usage: "list archived content (see docs/archive.md for details)"}
	putArchFlag  = cli.BoolFlag{Name: "archive", Usage: "archive a given list ('--list') or range ('--template') of objects"}

	archpathFlag = cli.StringFlag{
		Name:  "archpath",
		Usage: "filename in archive",
	}

	includeSrcBucketNameFlag = cli.BoolFlag{
		Name:  "include-src-bck",
		Usage: "prefix names of archived objects with the source bucket name",
	}
	// via 'ais archive': PUT arch operation with an option to APPEND if already exists
	apndArchIf1Flag = cli.BoolFlag{
		Name: "append",
		Usage: "if destination object (\"archive\", \"shard\") already exists, append to it\n" +
			indent4 + "\t(instead of creating a new one)",
	}
	// same as above via 'ais put'
	apndArchIf2Flag = cli.BoolFlag{
		Name: "arch-append-if-exists",
		Usage: "if destination object (\"archive\", \"shard\") already exists, append to it\n" +
			indent4 + "\t(instead of creating a new one)",
	}
	// APPEND to arch operation: option to PUT (ie., create) if doesn't exist (compare with the above)
	putArchIfNotExistFlag = cli.BoolFlag{
		Name:  "put",
		Usage: "if destination object (\"archive\", \"shard\") does not exist, create a new one",
	}

	continueOnErrorFlag = cli.BoolFlag{
		Name:  "cont-on-err",
		Usage: "keep running archiving xaction in presence of errors in a any given multi-object transaction",
	}
	// end archive

	// AuthN
	tokenFileFlag = cli.StringFlag{Name: "file,f", Value: "", Usage: "path to file"}
	passwordFlag  = cli.StringFlag{Name: "password,p", Value: "", Usage: "user password"}
	expireFlag    = DurationFlag{
		Name: "expire,e",
		Usage: "token expiration time, '0' - for never-expiring token;\n" +
			indent4 + "\tvalid time units: " + timeUnits,
		Value: 24 * time.Hour,
	}

	// Copy Bucket
	copyDryRunFlag = cli.BoolFlag{
		Name:  "dry-run",
		Usage: "show total size of new objects without really creating them",
	}
	copyPrependFlag = cli.StringFlag{
		Name: "prepend",
		Usage: "prefix to prepend to every copied object name, e.g.:\n" +
			indent4 + "\t--prepend=abc\t- prefix all copied object names with \"abc\"\n" +
			indent4 + "\t--prepend=abc/\t- copy objects into a virtual directory \"abc\" (note trailing filepath separator)",
	}

	// ETL
	etlExtFlag  = cli.StringFlag{Name: "ext", Usage: "mapping from old to new extensions of transformed objects' names"}
	etlNameFlag = cli.StringFlag{
		Name:     "name",
		Usage:    "unique ETL name (leaving this field empty will have unique ID auto-generated)",
		Required: true,
	}
	etlBucketRequestTimeout = DurationFlag{
		Name: "etl-timeout",
		Usage: "server-side timeout transforming a single object;\n" +
			indent4 + "\tvalid time units: " + timeUnits,
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
		Name:     "runtime",
		Usage:    "environment used to run the provided code (currently supported: python3.8v2, python3.10v2, python3.11v2)",
		Required: true,
	}
	commTypeFlag = cli.StringFlag{
		Name:  "comm-type",
		Usage: "communication type which should be used when running the provided code (defaults to hpush)",
	}
	transformURLFlag = cli.BoolFlag{
		Name:  "transform-url",
		Usage: "rather than contents (bytes), pass the URL of the objects to be transformed to the user-defined transform function (note: usage is limited to '--comm-type=hpull' only)",
	}
	funcTransformFlag = cli.StringFlag{
		Name:  "transform",
		Value: "transform", // NOTE: default name of the transform() function
		Usage: "receives and _transforms_ the payload",
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
	keepInitialConfigFlag = cli.BoolFlag{
		Name: "keep-initial-config",
		Usage: "keep the original plain-text configuration the node was deployed with\n" +
			indent4 + "\t(the option can be used to restart aisnode from scratch)",
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

	// LRU
	lruBucketsFlag = cli.StringFlag{
		Name: "buckets",
		Usage: "comma-separated list of bucket names, e.g.:\n" +
			indent1 + "\t\t\t--buckets 'ais://b1,ais://b2,ais://b3'\n" +
			indent1 + "\t\t\t--buckets \"gs://b1, s3://b2\"",
	}
)
