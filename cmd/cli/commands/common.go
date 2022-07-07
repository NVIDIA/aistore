// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file contains common constants and variables used in other files.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/urfave/cli"
)

const (
	// Commands (top-level) - preferably categories (nouns)
	commandAdvanced  = "advanced"
	commandAuth      = "auth"
	commandBucket    = "bucket"
	commandObject    = "object"
	commandCluster   = "cluster"
	commandConfig    = "config"
	commandMountpath = "mountpath"
	commandJob       = "job"
	commandShow      = "show"
	commandSearch    = "search"
	commandETL       = apc.ETL
	commandSource    = "source"
	commandLog       = "log"
	commandRebalance = "rebalance"
	commandCat       = "cat"
	commandConcat    = "concat"
	commandCopy      = "cp"
	commandCreate    = "create"
	commandECEncode  = "ec-encode"
	commandEvict     = "evict"
	commandPrefetch  = "prefetch"
	commandGet       = "get"
	commandList      = "ls"
	commandPromote   = "promote"
	commandPut       = "put"
	commandSetCustom = "set-custom"
	commandRemove    = "rm"
	commandRename    = "mv"
	commandSet       = "set"
	commandMirror    = "mirror"
	commandStart     = apc.ActXactStart
	commandStop      = apc.ActXactStop
	commandWait      = "wait"
	commandAlias     = "alias"
	commandStorage   = "storage"
	commandArch      = "archive"

	commandGenShards = "gen-shards"

	// Common Subcommands
	// NOTE: second level subcommands are preferably verbs
	subcmdDsort      = dsort.DSortNameLowercase
	subcmdSmap       = apc.GetWhatSmap
	subcmdBMD        = apc.GetWhatBMD
	subcmdMpath      = apc.GetWhatDiskStats
	subcmdConfig     = apc.GetWhatConfig
	subcmdLog        = apc.GetWhatLog
	subcmdRebalance  = apc.ActRebalance
	subcmdBucket     = "bucket"
	subcmdObject     = "object"
	subcmdProps      = "props"
	subcmdDownload   = "download"
	subcmdXaction    = "xaction"
	subcmdMountpath  = "mountpath"
	subcmdCluster    = commandCluster
	subcmdNode       = "node"
	subcmdPrimary    = "set-primary"
	subcmdList       = commandList
	subcmdLogs       = "logs"
	subcmdStop       = "stop"
	subcmdStart      = "start"
	subcmdLRU        = apc.ActLRU
	subcmdMembership = "add-remove-nodes"
	subcmdShutdown   = "shutdown"
	subcmdAttach     = "attach"
	subcmdDetach     = "detach"
	subcmdRmSmap     = "remove-from-smap"

	// Cluster subcommands
	subcmdCluAttach = "remote-" + subcmdAttach
	subcmdCluDetach = "remote-" + subcmdDetach
	subcmdCluConfig = "configure"
	subcmdReset     = "reset"

	// Mountpath (disk) actions
	subcmdMpathAttach  = subcmdAttach
	subcmdMpathEnable  = "enable"
	subcmdMpathDetach  = subcmdDetach
	subcmdMpathDisable = "disable"

	// Node subcommands
	subcmdJoin                = "join"
	subcmdStartMaint          = "start-maintenance"
	subcmdStopMaint           = "stop-maintenance"
	subcmdNodeDecommission    = "decommission"
	subcmdClusterDecommission = "decommission"

	// Show subcommands
	subcmdShowDisk         = subcmdMpath
	subcmdShowDownload     = subcmdDownload
	subcmdShowDsort        = subcmdDsort
	subcmdShowObject       = subcmdObject
	subcmdShowXaction      = subcmdXaction
	subcmdShowRebalance    = subcmdRebalance
	subcmdShowBucket       = subcmdBucket
	subcmdShowConfig       = subcmdConfig
	subcmdShowLog          = subcmdLog
	subcmdShowRemoteAIS    = "remote-cluster"
	subcmdShowCluster      = subcmdCluster
	subcmdShowClusterStats = "stats"

	subcmdShowStorage  = commandStorage
	subcmdShowMpath    = subcmdMountpath
	subcmdShowJob      = commandJob
	subcmdStgSummary   = subcmdSummary
	subcmdStgValidate  = "validate"
	subcmdStgMountpath = subcmdMountpath
	subcmdStgCleanup   = "cleanup"

	// Remove subcommands
	subcmdRemoveDownload = subcmdDownload
	subcmdRemoveDsort    = subcmdDsort

	// Start subcommands
	subcmdStartXaction  = subcmdXaction
	subcmdStartDsort    = subcmdDsort
	subcmdStartDownload = subcmdDownload

	// Stop subcommands
	subcmdStopXaction  = subcmdXaction
	subcmdStopDsort    = subcmdDsort
	subcmdStopDownload = subcmdDownload

	// Bucket subcommands
	subcmdSummary = "summary"

	// Bucket properties subcommands
	subcmdSetProps   = "set"
	subcmdResetProps = "reset"

	// Archive subcommands
	subcmdAppend = "append"

	// Wait subcommands
	subcmdWaitXaction  = subcmdXaction
	subcmdWaitDownload = subcmdDownload
	subcmdWaitDSort    = subcmdDsort

	// AuthN subcommands
	subcmdAuthAdd     = "add"
	subcmdAuthShow    = "show"
	subcmdAuthSet     = commandSet
	subcmdAuthRemove  = commandRemove
	subcmdAuthLogin   = "login"
	subcmdAuthLogout  = "logout"
	subcmdAuthUser    = "user"
	subcmdAuthRole    = "role"
	subcmdAuthCluster = subcmdCluster
	subcmdAuthToken   = "token"
	subcmdAuthConfig  = subcmdConfig

	// Warm up subcommands
	subcmdPreload = "preload"

	// k8s subcommans
	subcmdK8s        = "kubectl"
	subcmdK8sSvc     = "svc"
	subcmdK8sCluster = commandCluster

	// ETL subcommands
	subcmdInit = "init"
	subcmdSpec = "spec"
	subcmdCode = "code"

	// CLI config subcommands
	subcmdCLI           = "cli"
	subcmdCLIShow       = commandShow
	subcmdCLISet        = subcmdSetProps
	subcmdCLIAliasShow  = commandShow
	subcmdCLIAliasRm    = commandRemove
	subcmdCLIAliasSet   = subcmdCLISet
	subcmdCLIAliasReset = subcmdResetProps

	// Default values for long running operations
	refreshRateDefault = time.Second
	countDefault       = 1
)

const sizeUnits = "(all IEC and SI units are supported, e.g.: b, B, KB, KiB, k, MiB, mb, etc.)"

// Argument placeholders in help messages
// Name format: *Argument
const (
	// Common
	noArguments                 = " "
	keyValuePairsArgument       = "KEY=VALUE [KEY=VALUE...]"
	aliasURLPairArgument        = "ALIAS=URL (or UUID=URL)"
	aliasArgument               = "ALIAS (or UUID)"
	daemonMountpathPairArgument = "DAEMON_ID=MOUNTPATH [DAEMON_ID=MOUNTPATH...]"

	// Job IDs (download, dsort)
	jobIDArgument                 = "JOB_ID"
	optionalJobIDArgument         = "[JOB_ID]"
	optionalJobIDDaemonIDArgument = "[JOB_ID [DAEMON_ID]]"

	// Buckets
	bucketArgument         = "BUCKET"
	optionalBucketArgument = "[BUCKET]"
	bucketsArgument        = "BUCKET [BUCKET...]"
	bucketPropsArgument    = bucketArgument + " " + jsonSpecArgument + "|" + keyValuePairsArgument
	bucketAndPropsArgument = "BUCKET [PROP_PREFIX]"

	// Objects
	getObjectArgument        = "BUCKET/OBJECT_NAME [OUT_FILE|-]"
	putPromoteObjectArgument = "FILE|DIRECTORY BUCKET/[OBJECT_NAME]"
	concatObjectArgument     = "FILE|DIRECTORY [FILE|DIRECTORY...] BUCKET/OBJECT_NAME"
	objectArgument           = "BUCKET/OBJECT_NAME"
	optionalObjectsArgument  = "BUCKET/[OBJECT_NAME]..."

	// Daemons
	daemonIDArgument          = "DAEMON_ID"
	optionalDaemonIDArgument  = "[DAEMON_ID]"
	optionalTargetIDArgument  = "[TARGET_ID]"
	showConfigArgument        = "cluster|DAEMON_ID [CONFIG_PREFIX]"
	showClusterConfigArgument = "[CONFIG_SECTION]"
	nodeConfigArgument        = daemonIDArgument + " " + keyValuePairsArgument
	attachRemoteAISArgument   = aliasURLPairArgument
	detachRemoteAISArgument   = aliasArgument
	joinNodeArgument          = "IP:PORT"
	startDownloadArgument     = "SOURCE DESTINATION"
	jsonSpecArgument          = "JSON_SPECIFICATION"
	showStatsArgument         = "[DAEMON_ID] [STATS_FILTER]"

	// Xactions
	xactionArgument = "XACTION_NAME"

	// List command
	listCommandArgument = "[PROVIDER://][BUCKET_NAME]"

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
	aliasCmdArgument    = "AIS_COMMAND"
	aliasSetCmdArgument = "ALIAS AIS_COMMAND"

	// Search
	searchArgument = "KEYWORD [KEYWORD...]"
)

// Flags
var (
	// Global
	noColorFlag = cli.BoolFlag{
		Name:  "no-color",
		Usage: "disable colored output",
	}

	// Common
	objPropsFlag = cli.StringFlag{
		Name:  "props",
		Usage: "comma-separated list of object properties including name, size, version, ##copies, EC data and parity info, custom props",
		Value: strings.Join(apc.GetPropsDefault, ","),
	}
	objPropsLsFlag = cli.StringFlag{
		Name:  objPropsFlag.Name,
		Usage: objPropsFlag.Usage,
		Value: strings.Join(apc.GetPropsMinimal, ","),
	}
	allPropsFlag = cli.BoolFlag{Name: "all", Usage: "show all object properties"}
	prefixFlag   = cli.StringFlag{Name: "prefix", Usage: "list objects matching the given prefix"}
	refreshFlag  = cli.DurationFlag{
		Name:  "refresh",
		Usage: "refresh interval for continuous monitoring, valid time units: 'ns', 'us', 'ms', 's', 'm', and 'h'",
		Value: refreshRateDefault,
	}
	regexFlag       = cli.StringFlag{Name: "regex", Usage: "regex pattern for matching"}
	jsonFlag        = cli.BoolFlag{Name: "json,j", Usage: "json input/output"}
	noHeaderFlag    = cli.BoolFlag{Name: "no-headers,H", Usage: "display tables without headers"}
	progressBarFlag = cli.BoolFlag{Name: "progress", Usage: "display progress bar"}
	dryRunFlag      = cli.BoolFlag{Name: "dry-run", Usage: "preview the results without really running the action"}
	verboseFlag     = cli.BoolFlag{Name: "verbose,v", Usage: "verbose"}
	nonverboseFlag  = cli.BoolFlag{Name: "non-verbose,nv", Usage: "non-verbose"}
	ignoreErrorFlag = cli.BoolFlag{
		Name:  "ignore-error",
		Usage: "ignore \"soft\" failures, such as \"bucket already exists\", etc.",
	}
	bucketPropsFlag = cli.StringFlag{Name: "bucket-props", Usage: "bucket properties"}
	forceFlag       = cli.BoolFlag{Name: "force,f", Usage: "force an action"}
	rawFlag         = cli.BoolFlag{Name: "raw", Usage: "display exact values instead of human-readable ones"}

	allXactionsFlag = cli.BoolFlag{Name: "all", Usage: "show all xactions, including finished"}
	allItemsFlag    = cli.BoolFlag{Name: "all", Usage: "list all items"} // TODO: differentiate bucket names vs objects
	allJobsFlag     = cli.BoolFlag{Name: "all", Usage: "remove all finished jobs"}
	allETLStopFlag  = cli.BoolFlag{Name: "all", Usage: "stop all ETLs"}

	// Bucket
	startAfterFlag = cli.StringFlag{
		Name:  "start-after",
		Usage: "list bucket's content alphabetically starting with the first name *after* the specified",
	}
	objLimitFlag = cli.IntFlag{Name: "limit", Usage: "limit object count", Value: 0} // TODO: specify default as unlimited
	pageSizeFlag = cli.IntFlag{Name: "page-size", Usage: "maximum number of entries by list objects call", Value: 1000}
	copiesFlag   = cli.IntFlag{Name: "copies", Usage: "number of object replicas", Value: 1, Required: true}
	maxPagesFlag = cli.IntFlag{Name: "max-pages", Usage: "display up to this number pages of bucket objects"}

	fastFlag = cli.BoolTFlag{
		Name:  "fast",
		Usage: "use faster logic to count objects and report disk usage (default: true)",
	}
	validateSummaryFlag = cli.BoolFlag{
		Name:  "validate",
		Usage: "perform checks (correctness of placement, number of copies, and more) and show the corresponding error counts",
	}
	pagedFlag         = cli.BoolFlag{Name: "paged", Usage: "fetch and print the bucket list page by page, ignored in fast mode"}
	showUnmatchedFlag = cli.BoolFlag{Name: "show-unmatched", Usage: "list objects that were not matched by regex and template"}
	activeFlag        = cli.BoolFlag{Name: "active", Usage: "show only running xactions"}
	keepMDFlag        = cli.BoolFlag{Name: "keep-md", Usage: "keep bucket metadata"}
	dataSlicesFlag    = cli.IntFlag{Name: "data-slices,data,d", Usage: "number of data slices", Required: true}
	paritySlicesFlag  = cli.IntFlag{Name: "parity-slices,parity,p", Usage: "number of parity slices", Required: true}
	listBucketsFlag   = cli.StringFlag{Name: "buckets", Usage: "comma-separated list of bucket names, e.g.: 'b1,b2,b3'"}
	compactPropFlag   = cli.BoolFlag{Name: "compact,c", Usage: "display properties grouped in human-readable mode"}
	nameOnlyFlag      = cli.BoolFlag{Name: "name-only", Usage: "show only object names"}

	// Config
	configTypeFlag = cli.StringFlag{Name: "type", Usage: "show the specified configuration, one of: 'all','cluster','local'"}

	// Log severity (cmn.LogInfo, ....) enum
	logSevFlag = cli.StringFlag{Name: "severity", Usage: "show the specified log, one of: 'i[nfo]','w[arning]','e[rror]'"}

	// Daeclu
	countFlag = cli.IntFlag{Name: "count", Usage: "total number of generated reports", Value: countDefault}

	// Download
	descJobFlag          = cli.StringFlag{Name: "description,desc", Usage: "job description"}
	timeoutFlag          = cli.StringFlag{Name: "timeout", Usage: "timeout, e.g. '30m'"}
	limitConnectionsFlag = cli.IntFlag{
		Name:  "limit-connections,conns",
		Usage: "number of connections each target can make concurrently (each target can handle at most #mountpaths connections)",
	}
	limitBytesPerHourFlag = cli.StringFlag{
		Name:  "limit-bytes-per-hour,limit-bph,bph",
		Usage: "number of bytes (can end with suffix (k, MB, GiB, ...)) that all targets can maximally download in hour",
	}
	objectsListFlag = cli.StringFlag{
		Name:  "object-list,from",
		Usage: "path to file containing JSON array of strings with object names to download",
	}
	syncFlag             = cli.BoolFlag{Name: "sync", Usage: "sync bucket with cloud"}
	progressIntervalFlag = cli.StringFlag{
		Name:  "progress-interval",
		Value: downloader.DownloadProgressInterval.String(),
		Usage: "progress interval for continuous monitoring, valid time units: 'ns', 'us', 'ms', 's', 'm', and 'h' (e.g. '10s')",
	}
	// dSort
	fileSizeFlag = cli.StringFlag{Name: "fsize", Value: "1024", Usage: "size of file in a shard"}
	logFlag      = cli.StringFlag{Name: "log", Usage: "path to file where the metrics will be saved"}
	cleanupFlag  = cli.BoolFlag{
		Name:  "cleanup",
		Usage: "remove old bucket and create it again (warning: removes all objects that were present in the old bucket)",
	}
	concurrencyFlag = cli.IntFlag{
		Name: "conc", Value: 10,
		Usage: "limits number of concurrent put requests and number of concurrent shards created",
	}
	fileCountFlag = cli.IntFlag{Name: "fcount", Value: 5, Usage: "number of files inside single shard"}
	specFileFlag  = cli.StringFlag{Name: "file,f", Value: "", Usage: "path to file with dSort specification"}

	// multi-object
	listFlag     = cli.StringFlag{Name: "list", Usage: "comma-separated list of object names, e.g.: 'o1,o2,o3'"}
	templateFlag = cli.StringFlag{Name: "template", Usage: "template for matching object names, e.g.: 'shard-{900..999}.tar'"}

	// Object
	offsetFlag      = cli.StringFlag{Name: "offset", Usage: "object read offset " + sizeUnits}
	lengthFlag      = cli.StringFlag{Name: "length", Usage: "object read length " + sizeUnits}
	checkCachedFlag = cli.BoolFlag{
		Name:  "check-cached",
		Usage: "check if object from a remote bucket is present (ie., cached) in the cluster",
	}
	listCachedFlag = cli.BoolFlag{
		Name:  "cached",
		Usage: "list only those objects from a remote bucket that are present (ie., cached) in the cluster",
	}
	enableFlag    = cli.BoolFlag{Name: "enable", Usage: "enable"}
	disableFlag   = cli.BoolFlag{Name: "disable", Usage: "disable"}
	recursiveFlag = cli.BoolFlag{Name: "recursive,r", Usage: "recursive operation"}
	rmRfFlag      = cli.BoolFlag{Name: "all", Usage: "remove all objects (use it with extreme caution!)"}

	overwriteFlag = cli.BoolFlag{Name: "overwrite-dst,o", Usage: "overwrite destination, if exists"}
	deleteSrcFlag = cli.BoolFlag{Name: "delete-src", Usage: "delete successfully promoted source"}
	targetIDFlag  = cli.StringFlag{Name: "target-id", Usage: "ais target designated to carry out the entire operation"}

	notFshareFlag = cli.BoolFlag{
		Name:  "not-file-share",
		Usage: "each target must act autonomously skipping file-share auto-detection and promoting the entire source (as seen from _the_ target)",
	}

	yesFlag       = cli.BoolFlag{Name: "yes,y", Usage: "assume 'yes' for all questions"}
	chunkSizeFlag = cli.StringFlag{
		Name:  "chunk-size",
		Usage: "chunk size used for each request " + sizeUnits, Value: "10MB",
	}

	cksumFlag        = cli.BoolFlag{Name: "checksum", Usage: "validate checksum"}
	computeCksumFlag = cli.BoolFlag{Name: "compute-checksum", Usage: "compute checksum configured for the bucket"}
	skipVerCksumFlag = cli.BoolFlag{Name: "skip-vc", Usage: "skip loading object metadata (and the associated checksum & version related processing)"}

	supportedCksumFlags = initSupportedCksumFlags()

	// auth
	descRoleFlag     = cli.StringFlag{Name: "description,desc", Usage: "role description"}
	clusterRoleFlag  = cli.StringFlag{Name: "cluster", Usage: "associate role with the specified AIS cluster"}
	clusterTokenFlag = cli.StringFlag{Name: "cluster", Usage: "issue token for the cluster"}
	bucketRoleFlag   = cli.StringFlag{Name: "bucket", Usage: "associate role with the specified bucket"}

	// begin archive
	listArchFlag             = cli.BoolFlag{Name: "archive", Usage: "list archived content"}
	createArchFlag           = cli.BoolFlag{Name: "archive", Usage: "archive a list or a range of objects"}
	archpathFlag             = cli.StringFlag{Name: "archpath", Usage: "filename in archive"}
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
	expireFlag    = cli.DurationFlag{
		Name:  "expire,e",
		Usage: "token expiration time, '0' - for never-expiring token. Default expiration time is 24 hours",
	}

	// Copy Bucket
	cpBckDryRunFlag = cli.BoolFlag{
		Name:  "dry-run",
		Usage: "show total size of new objects without really creating them",
	}
	cpBckPrefixFlag = cli.StringFlag{Name: "prefix", Usage: "prefix added to every new object's name"}

	// ETL
	etlExtFlag = cli.StringFlag{Name: "ext", Usage: "mapping from old to new extensions of transformed objects' names"}
	etlUUID    = cli.StringFlag{
		Name:     "name",
		Usage:    "unique ETL name (leaving this field empty will have unique ID auto-generated)",
		Required: true,
	}
	etlBucketRequestTimeout = cli.DurationFlag{Name: "request-timeout", Usage: "timeout for a transformation of a single object"}
	fromFileFlag            = cli.StringFlag{
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
	waitTimeoutFlag = cli.DurationFlag{
		Name:  "wait-timeout",
		Usage: "determines how long ais target should wait for pod to become ready",
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

	longRunFlags = []cli.Flag{refreshFlag, countFlag}

	baseLstRngFlags = []cli.Flag{
		listFlag,
		templateFlag,
	}

	transientFlag = cli.BoolFlag{
		Name:  "transient",
		Usage: "to update config temporarily",
	}

	setNewCustomMDFlag = cli.BoolFlag{
		Name:  "set-new-custom",
		Usage: "remove existing custom keys (if any) and store new custom metadata",
	}

	cliConfigPathFlag = cli.BoolFlag{
		Name:  "path",
		Usage: "display path to the AIS CLI configuration",
	}
)
