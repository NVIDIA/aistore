// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ext/dload"
	"github.com/urfave/cli"
)

// Contains common constants and global variables (including all command-line options aka flags).

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
	cmdRotateLogs    = "rotate-logs"
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
	cmdStgCleanup   = "cleanup" // display name for apc.ActStoreCleanup
	cmdStgValidate  = "validate"
	cmdSummary      = "summary" // ditto apc.ActSummaryBck

	cmdCluster    = commandCluster
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

	cmdDownloadLogs = "download-logs"
	cmdViewLogs     = "view-logs" // etl

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
	cmdInit    = "init"
	cmdSpec    = "spec"
	cmdCode    = "code"
	cmdDetails = "details"

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
// more constants
//

const flagPrefix = "--"

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
	tabHelpOpt = "press <TAB-TAB> to select, '--help' for more options"
	tabHelpDet = "press <TAB-TAB> to select, '--help' for details"
)

// `ArgsUsage`: argument placeholders in help messages
const (
	indent1 = "   " // indent4 et al.

	// Job IDs (download, dsort)
	jobIDArgument                 = "JOB_ID"
	optionalJobIDArgument         = "[JOB_ID]"
	optionalJobIDDaemonIDArgument = "[JOB_ID [NODE_ID]]"

	jobAnyArg                = "[NAME] [JOB_ID] [NODE_ID] [BUCKET]"
	jobShowRebalanceArgument = "[REB_ID] [NODE_ID]"

	// Perf
	showPerfArgument = "show performance counters, throughput, latency, and more (" + tabtab + " specific view)"

	// ETL
	etlNameArgument     = "ETL_NAME"
	etlNameListArgument = "ETL_NAME [ETL_NAME ...]"

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

	bucketSrcArgument       = "SRC_BUCKET"
	bucketObjectSrcArgument = "SRC_BUCKET[/OBJECT_NAME_or_TEMPLATE]"
	bucketDstArgument       = "DST_BUCKET"
	bucketNewArgument       = "NEW_BUCKET"

	dsortSpecArgument = "[JSON_SPECIFICATION|YAML_SPECIFICATION|-] [SRC_BUCKET] [DST_BUCKET]"

	// Objects
	objectArgument          = "BUCKET/OBJECT_NAME"
	optionalObjectsArgument = "BUCKET[/OBJECT_NAME] ..."
	dstShardArgument        = bucketDstArgument + "/SHARD_NAME"

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

	setCustomArgument = objectArgument + " " + jsonKeyValueArgument + " | " + keyValuePairsArgument + ", e.g.:\n" +
		indent1 +
		"mykey1=value1 mykey2=value2 OR '{\"mykey1\":\"value1\", \"mykey2\":\"value2\"}'"

	// nodes
	nodeIDArgument            = "NODE_ID"
	optionalNodeIDArgument    = "[NODE_ID]"
	optionalTargetIDArgument  = "[TARGET_ID]"
	joinNodeArgument          = "IP:PORT"
	nodeMountpathPairArgument = "NODE_ID=MOUNTPATH [NODE_ID=MOUNTPATH...]"

	// node log
	showLogArgument = nodeIDArgument
	getLogArgument  = nodeIDArgument + " [OUT_FILE|OUT_DIR|-]"

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
	allLogsFlag         = cli.BoolFlag{Name: scopeAll, Usage: "download all logs"}

	allObjsOrBcksFlag = cli.BoolFlag{
		Name: scopeAll,
		Usage: "depending on the context, list:\n" +
			indent4 + "\t- all buckets, including accessible (visible) remote buckets that are _not present_ in the cluster\n" +
			indent4 + "\t- all objects in a given accessible (visible) bucket, including remote objects and misplaced copies",
	}
	copyAllObjsFlag = cli.BoolFlag{
		Name:  scopeAll,
		Usage: "copy all objects from a remote bucket including those that are not present (not \"cached\") in cluster",
	}
	etlAllObjsFlag = cli.BoolFlag{
		Name:  scopeAll,
		Usage: "transform all objects from a remote bucket including those that are not present (not \"cached\") in cluster",
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
		Usage: "list objects that have names starting with the specified prefix, e.g.:\n" +
			indent4 + "\t'--prefix a/b/c' - list virtual directory a/b/c and/or objects from the virtual directory\n" +
			indent4 + "\ta/b that have their names (relative to this directory) starting with the letter 'c'",
	}
	getObjPrefixFlag = cli.StringFlag{
		Name: listObjPrefixFlag.Name,
		Usage: "get objects that start with the specified prefix, e.g.:\n" +
			indent4 + "\t'--prefix a/b/c' - get objects from the virtual directory a/b/c and objects from the virtual directory\n" +
			indent4 + "\ta/b that have their names (relative to this directory) starting with 'c';\n" +
			indent4 + "\t'--prefix \"\"' - get entire bucket (all objects)",
	}
	verbObjPrefixFlag = cli.StringFlag{
		Name: "prefix",
		Usage: "select objects that have names starting with the specified prefix, e.g.:\n" +
			indent4 + "\t'--prefix a/b/c'\t- matches names 'a/b/c/d', 'a/b/cdef', and similar;\n" +
			indent4 + "\t'--prefix a/b/c/'\t- only matches objects from the virtual directory a/b/c/",
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
		Name: "count",
		Usage: "used together with " + qflprn(refreshFlag) + " to limit the number of generated reports, e.g.:\n" +
			indent4 + "\t '--refresh 10 --count 5' - run 5 times with 10s interval",
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
		Name: regexFlag.Name,
		Usage: "regular expression select table columns (case-insensitive), e.g.:\n" +
			indent4 + "\t --regex \"put|err\" - show PUT (count), PUT (total size), and all supported error counters;\n" +
			indent4 + "\t --regex \"[a-z]\" - show all supported metrics, including those that have zero values across all nodes;\n" +
			indent4 + "\t --regex \"(GET-COLD$|VERSION-CHANGE$)\" - show the number of cold GETs and object version changes (updates)",
	}
	regexJobsFlag = cli.StringFlag{
		Name:  regexFlag.Name,
		Usage: "regular expression to select jobs by name, kind, or description, e.g.: --regex \"ec|mirror|elect\"",
	}

	jsonFlag     = cli.BoolFlag{Name: "json,j", Usage: "json input/output"}
	noHeaderFlag = cli.BoolFlag{Name: "no-headers,H", Usage: "display tables without headers"}
	noFooterFlag = cli.BoolFlag{Name: "no-footers", Usage: "display tables without footers"}

	progressFlag = cli.BoolFlag{Name: "progress", Usage: "show progress bar(s) and progress of execution in real time"}
	dryRunFlag   = cli.BoolFlag{Name: "dry-run", Usage: "preview the results without really running the action"}

	verboseFlag    = cli.BoolFlag{Name: "verbose,v", Usage: "verbose output"}
	nonverboseFlag = cli.BoolFlag{Name: "non-verbose,nv", Usage: "non-verbose (quiet) output, minimized reporting"}
	verboseJobFlag = cli.BoolFlag{
		Name:  verboseFlag.Name,
		Usage: "show extended statistics",
	}
	silentFlag = cli.BoolFlag{
		Name:  "silent",
		Usage: "server-side flag, an indication for aistore _not_ to log assorted errors (e.g., HEAD(object) failures)",
	}

	averageSizeFlag = cli.BoolFlag{Name: "average-size", Usage: "show average GET, PUT, etc. request size"}

	ignoreErrorFlag = cli.BoolFlag{
		Name:  "ignore-error",
		Usage: "ignore \"soft\" failures such as \"bucket already exists\", etc.",
	}

	bucketPropsFlag = cli.StringFlag{
		Name: "props",
		Usage: "create bucket with the specified (non-default) properties, e.g.:\n" +
			indent1 + "\t* ais create ais://mmm --props=\"versioning.validate_warm_get=false versioning.synchronize=true\"\n" +
			indent1 + "\t* ais create ais://nnn --props='mirror.enabled=true mirror.copies=4 checksum.type=md5'\n" +
			indent1 + "\t(tip: use '--props' to override properties that a new bucket inherits from cluster config at creation time;\n" +
			indent1 + "\t see also: 'ais bucket props show' and 'ais bucket props set')",
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

	// list-objects
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
		Name: "summary",
		Usage: "show object numbers, bucket sizes, and used capacity;\n" +
			indent4 + "\tnote: applies only to buckets and objects that are _present_ in the cluster",
	}
	pagedFlag = cli.BoolFlag{
		Name:  "paged",
		Usage: "list objects page by page, one page at a time (see also '--page-size' and '--limit')",
	}
	showUnmatchedFlag = cli.BoolFlag{
		Name:  "show-unmatched",
		Usage: "list also objects that were _not_ matched by regex and/or template (range)",
	}
	verChangedFlag = cli.BoolFlag{
		Name: "check-versions",
		Usage: "check whether listed remote objects and their in-cluster copies are identical, ie., have the same versions\n" +
			indent4 + "\t- applies to remote backends that maintain at least some form of versioning information (e.g., version, checksum, ETag)\n" +
			indent4 + "\t- see related: 'ais get --latest', 'ais cp --sync', 'ais prefetch --latest'",
	}

	keepMDFlag       = cli.BoolFlag{Name: "keep-md", Usage: "keep bucket metadata"}
	dataSlicesFlag   = cli.IntFlag{Name: "data-slices,data,d", Usage: "number of data slices", Required: true}
	paritySlicesFlag = cli.IntFlag{Name: "parity-slices,parity,p", Usage: "number of parity slices", Required: true}
	compactPropFlag  = cli.BoolFlag{Name: "compact,c", Usage: "display properties grouped in human-readable mode"}

	nameOnlyFlag = cli.BoolFlag{
		Name:  "name-only",
		Usage: "faster request to retrieve only the names of objects (if defined, '--props' flag will be ignored)",
	}

	// Log severity (cmn.LogInfo, ....) enum
	logSevFlag = cli.StringFlag{
		Name: "severity",
		Usage: "log severity is either 'i' or 'info' (default, can be omitted), or 'error', whereby error logs contain\n" +
			indent4 + "\tonly errors and warnings, e.g.: '--severity info', '--severity error', '--severity e'",
	}
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

	// sync
	latestVerFlag = cli.BoolFlag{
		Name: "latest",
		Usage: "check in-cluster metadata and, possibly, GET, download, prefetch, or copy the latest object version\n" +
			indent1 + "\tfrom the associated remote bucket:\n" +
			indent1 + "\t- provides operation-level control over object versioning (and version synchronization)\n" +
			indent1 + "\t  without requiring to change bucket configuration\n" +
			indent1 + "\t- the latter can be done using 'ais bucket props set BUCKET versioning'\n" +
			indent1 + "\t- see also: 'ais ls --check-versions', 'ais cp', 'ais prefetch', 'ais get'",
	}
	syncFlag = cli.BoolFlag{
		Name: "sync",
		Usage: "synchronize destination bucket with its remote (e.g., Cloud or remote AIS) source;\n" +
			indent1 + "\tthe option is a stronger variant of the '--latest' (option) - in addition it entails\n" +
			indent1 + "\tremoving of the objects that no longer exist remotely\n" +
			indent1 + "\t(see also: 'ais show bucket versioning' and the corresponding documentation)",
	}

	// dsort
	dsortFsizeFlag  = cli.StringFlag{Name: "fsize", Value: "1024", Usage: "size of the files in a shard"}
	dsortLogFlag    = cli.StringFlag{Name: "log", Usage: "filename to log metrics (statistics)"}
	dsortFcountFlag = cli.IntFlag{Name: "fcount", Value: 5, Usage: "number of files in a shard"}
	dsortSpecFlag   = cli.StringFlag{Name: "file,f", Value: "", Usage: "path to JSON or YAML job specification"}

	cleanupFlag = cli.BoolFlag{
		Name:  "cleanup",
		Usage: "remove old bucket and create it again (warning: removes the entire content of the old bucket)",
	}
	concurrencyFlag = cli.IntFlag{
		Name:  "conc",
		Value: 10,
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
	dontWaitFlag = cli.BoolFlag{
		Name: "dont-wait",
		Usage: "when _summarizing_ buckets do not wait for the respective job to finish -\n" +
			indent4 + "\tuse the job's UUID to query the results interactively",
	}

	// multi-object / multi-file
	listFlag = cli.StringFlag{
		Name: "list",
		Usage: "comma-separated list of object or file names, e.g.:\n" +
			indent4 + "\t--list 'o1,o2,o3'\n" +
			indent4 + "\t--list \"abc/1.tar, abc/1.cls, abc/1.jpeg\"\n" +
			indent4 + "\tor, when listing files and/or directories:\n" +
			indent4 + "\t--list \"/home/docs, /home/abc/1.tar, /home/abc/1.jpeg\"",
	}
	templateFlag = cli.StringFlag{
		Name: "template",
		Usage: "template to match object or file names; may contain prefix (that could be empty) with zero or more ranges\n" +
			"\t(with optional steps and gaps), e.g.:\n" +
			indent4 + "\t--template \"\" # (an empty or '*' template matches eveything)\n" +
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
	headObjPresentFlag = cli.BoolFlag{
		Name: "check-cached",
		Usage: "check whether a given named object is present in cluster\n" +
			indent1 + "\t(applies only to buckets with remote backend)",
	}
	listObjCachedFlag = cli.BoolFlag{
		Name:  "cached",
		Usage: "list only in-cluster objects - only those objects from a remote bucket that are present (\"cached\")",
	}
	getObjCachedFlag = cli.BoolFlag{
		Name:  "cached",
		Usage: "get only in-cluster objects - only those objects from a remote bucket that are present (\"cached\")",
	}
	// when '--all' is used for/by another flag
	objNotCachedPropsFlag = cli.BoolFlag{
		Name:  "not-cached",
		Usage: "show properties of _all_ objects from a remote bucket including those (objects) that are not present (not \"cached\")",
	}

	dontHeadSrcDstBucketsFlag = cli.BoolFlag{
		Name:  "skip-lookup",
		Usage: "skip checking source and destination buckets' existence (trading off extra lookup for performance)\n",
	}
	dontHeadRemoteFlag = cli.BoolFlag{
		Name: "skip-lookup",
		Usage: "do not execute HEAD(bucket) request to lookup remote bucket and its properties; possible usage scenarios include:\n" +
			indent4 + "\t 1) adding remote bucket to aistore without first checking the bucket's accessibility\n" +
			indent4 + "\t    (e.g., to configure the bucket's aistore properties with alternative security profile and/or endpoint)\n" +
			indent4 + "\t 2) listing public-access Cloud buckets where certain operations (e.g., 'HEAD(bucket)') may be disallowed",
	}
	dontAddRemoteFlag = cli.BoolFlag{
		Name: "dont-add",
		Usage: "list remote bucket without adding it to cluster's metadata - e.g.:\n" +
			indent1 + "\t  - let's say, s3://abc is accessible but not present in the cluster (e.g., 'ais ls' returns error);\n" +
			indent1 + "\t  - then, if we ask aistore to list remote buckets: `ais ls s3://abc --all'\n" +
			indent1 + "\t    the bucket will be added (in effect, it'll be created);\n" +
			indent1 + "\t  - to prevent this from happening, either use this '--dont-add' flag or run 'ais evict' command later",
	}
	addRemoteFlag = cli.BoolFlag{
		Name: "add",
		Usage: "add remote bucket to cluster's metadata\n" +
			indent1 + "\t  - let's say, s3://abc is accessible but not present in the cluster (e.g., 'ais ls' returns error);\n" +
			indent1 + "\t  - most of the time, there's no need to worry about it as aistore handles presence/non-presence\n" +
			indent1 + "\t    transparently behind the scenes;\n" +
			indent1 + "\t  - but if you do want to (explicltly) add the bucket, you could also use '--add' option",
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

	yesFlag = cli.BoolFlag{Name: "yes,y", Usage: "assume 'yes' to all questions"}

	chunkSizeFlag = cli.StringFlag{
		Name:  "chunk-size",
		Usage: "chunk size in IEC or SI units, or \"raw\" bytes (e.g.: 4mb, 1MiB, 1048576, 128k; see '--units')",
	}

	blobDownloadFlag = cli.BoolFlag{
		Name:  apc.ActBlobDl,
		Usage: "utilize built-in blob-downloader (and the corresponding alternative datapath) to read very large remote objects",
	}

	numWorkersFlag = cli.IntFlag{
		Name:  "num-workers",
		Usage: "number of concurrent blob-downloading workers (readers); system default when omitted or zero",
	}

	cksumFlag = cli.BoolFlag{Name: "checksum", Usage: "validate checksum"}

	putObjCksumText     = indent4 + "\tand provide it as part of the PUT request for subsequent validation on the server side"
	putObjCksumFlags    = initPutObjCksumFlags()
	putObjDfltCksumFlag = cli.BoolFlag{
		Name: "compute-checksum",
		Usage: "[end-to-end protection] compute client-side checksum configured for the destination bucket\n" +
			putObjCksumText,
	}

	appendConcatFlag = cli.BoolFlag{
		Name:  "append",
		Usage: "concatenate files: append a file or multiple files as a new _or_ to an existing object",
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

	archpathFlag = cli.StringFlag{
		Name:  "archpath",
		Usage: "filename in archive (shard)",
	}
	archpathGetFlag = cli.StringFlag{
		Name:  archpathFlag.Name,
		Usage: "extract the specified file from an archive (shard)",
	}
	extractFlag = cli.BoolFlag{
		Name:  "extract,x",
		Usage: "extract all files from archive(s)",
	}

	inclSrcBucketNameFlag = cli.BoolFlag{
		Name:  "include-src-bck",
		Usage: "prefix the names of archived files with the source bucket name",
	}
	inclSrcDirNameFlag = cli.BoolFlag{
		Name:  "include-src-dir",
		Usage: "prefix the names of archived files with the (root) source directory (omitted by default)",
	}
	// 'ais archive put': conditional APPEND
	archAppendOrPutFlag = cli.BoolFlag{
		Name: "append-or-put",
		Usage: "append to an existing destination object (\"archive\", \"shard\") iff exists; otherwise PUT a new archive (shard);\n" +
			indent4 + "\tnote that PUT (with subsequent overwrite if the destination exists) is the default behavior when the flag is omitted",
	}
	// 'ais archive put': unconditional APPEND: destination must exist
	archAppendOnlyFlag = cli.BoolFlag{
		Name:  "append",
		Usage: "add newly archived content to the destination object (\"archive\", \"shard\") that must exist",
	}

	continueOnErrorFlag = cli.BoolFlag{
		Name:  "cont-on-err",
		Usage: "keep running archiving xaction (job) in presence of errors in a any given multi-object transaction",
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
		Name: "comm-type",
		Usage: "enumerated communication type used between aistore cluster and ETL containers that run custom transformations:\n" +
			indent4 + "\t - 'hpush' or 'hpush://' - ETL container provides HTTP PUT handler that'll be invoked upon every request to transform\n" +
			indent4 + "\t -  '' - same as 'hpush://' (default, can be omitted)\n" +
			indent4 + "\t - 'hpull' or 'hpull://' - same, but ETL container is expected to provide HTTP GET endpoint\n" +
			indent4 + "\t - 'hrev' or 'hrev://' - same, but aistore nodes will reverse-proxy requests to their respective ETL containers)\n" +
			indent4 + "\t - 'io' or 'io://' - for each request an aistore node will: run ETL container locally, write data\n" +
			indent4 + "\t   to its standard input and then read transformed data from the standard output\n" +
			indent4 + "\t For more defails, see https://aiatscale.org/docs/etl#communication-mechanisms\n",
	}

	funcTransformFlag = cli.StringFlag{
		Name:  "transform",
		Value: "transform", // NOTE: default name of the transform() function
		Usage: "receives and _transforms_ the payload",
	}
	argTypeFlag = cli.StringFlag{
		Name: "arg-type",
		Usage: "Specifies _how_ an object to transform gets passed from aistore to ETL container:\n" +
			indent4 + "\t - \"\" - The default option (that can be omitted), whereby ETL container receives an entire payload (bytes) to transform\n" +
			indent4 + "\t - url - URL that points towards the data to transform (the support is currently limited to '--comm-type=hpull')\n" +
			indent4 + "\t - fqn - Fully-qualified name (FQN) of a locally stored object (requires trusted ETL container, might not be always available)",
	}

	// Node
	roleFlag = cli.StringFlag{
		Name: "role", Required: true,
		Usage: "role of this AIS daemon: proxy or target",
	}
	noRebalanceFlag = cli.BoolFlag{
		Name:  "no-rebalance",
		Usage: "do _not_ run global rebalance after putting node in maintenance (caution: advanced usage only!)",
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
