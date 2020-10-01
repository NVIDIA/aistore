// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file contains common constants and variables used in other files.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/urfave/cli"
)

const (
	// Commands (top-level) - preferably verbs
	commandAttach    = "attach"
	commandAuth      = "auth"
	commandCat       = "cat"
	commandConcat    = "concat"
	commandCopy      = "cp"
	commandCreate    = "create"
	commandDetach    = "detach"
	commandECEncode  = "ec-encode"
	commandEvict     = "evict"
	commandGenShards = "gen-shards"
	commandGet       = "get"
	commandJoin      = "join"
	commandList      = "ls"
	commandPrefetch  = cmn.ActPrefetch
	commandPromote   = "promote"
	commandPut       = "put"
	commandRemove    = "rm"
	commandRename    = "rename"
	commandSet       = "set"
	commandSetCopies = "set-copies"
	commandShow      = "show"
	commandStart     = cmn.ActXactStart
	commandStop      = cmn.ActXactStop
	commandWait      = "wait"
	commandSearch    = "search"
	commandETL       = cmn.ETL

	// Subcommands - preferably nouns
	subcmdDsort     = cmn.DSortNameLowercase
	subcmdSmap      = cmn.GetWhatSmap
	subcmdDisk      = cmn.GetWhatDiskStats
	subcmdConfig    = cmn.GetWhatConfig
	subcmdRebalance = cmn.ActRebalance
	subcmdBucket    = "bucket"
	subcmdObject    = "object"
	subcmdProps     = "props"
	subcmdDownload  = "download"
	subcmdXaction   = "xaction"
	subcmdNode      = "node"
	subcmdProxy     = "proxy"
	subcmdTarget    = "target"
	subcmdRemoteAIS = cmn.GetWhatRemoteAIS
	subcmdMountpath = "mountpath"
	subcmdCluster   = "cluster"
	subcmdPrimary   = "primary"
	subcmdInit      = "init"
	subcmdBuild     = "build"
	subcmdList      = commandList
	subcmdLogs      = "logs"
	subcmdStop      = "stop"
	subcmdLRU       = cmn.ActLRU

	// Show subcommands
	subcmdShowBucket    = subcmdBucket
	subcmdShowDisk      = subcmdDisk
	subcmdShowDownload  = subcmdDownload
	subcmdShowDsort     = subcmdDsort
	subcmdShowObject    = subcmdObject
	subcmdShowXaction   = subcmdXaction
	subcmdShowRebalance = subcmdRebalance
	subcmdShowBckProps  = subcmdProps
	subcmdShowConfig    = subcmdConfig
	subcmdShowRemoteAIS = subcmdRemoteAIS
	subcmdShowCluster   = subcmdCluster
	subcmdShowMpath     = subcmdMountpath

	// Create subcommands
	subcmdCreateBucket = subcmdBucket

	// Rename subcommands
	subcmdRenameBucket = subcmdBucket
	subcmdRenameObject = subcmdObject

	// Remove subcommands
	subcmdRemoveBucket   = subcmdBucket
	subcmdRemoveObject   = subcmdObject
	subcmdRemoveNode     = subcmdNode
	subcmdRemoveDownload = subcmdDownload
	subcmdRemoveDsort    = subcmdDsort

	// Copy subcommands
	subcmdCopyBucket = subcmdBucket

	// Start subcommands
	subcmdStartXaction  = subcmdXaction
	subcmdStartDsort    = subcmdDsort
	subcmdStartDownload = subcmdDownload

	// Stop subcommands
	subcmdStopXaction  = subcmdXaction
	subcmdStopDsort    = subcmdDsort
	subcmdStopDownload = subcmdDownload

	// Set subcommand
	subcmdSetConfig  = subcmdConfig
	subcmdSetProps   = subcmdProps
	subcmdSetPrimary = subcmdPrimary

	// Attach/Detach subcommand
	subcmdAttachRemoteAIS = subcmdRemoteAIS
	subcmdAttachMountpath = subcmdMountpath
	subcmdDetachRemoteAIS = subcmdRemoteAIS
	subcmdDetachMountpath = subcmdMountpath

	// Join subcommands
	subcmdJoinProxy  = subcmdProxy
	subcmdJoinTarget = subcmdTarget

	// Wait subcommands
	subcmdWaitXaction  = subcmdXaction
	subcmdWaitDownload = subcmdDownload
	subcmdWaitDSort    = subcmdDsort

	// AuthN subcommands
	subcmdAuthAdd     = "add"
	subcmdAuthShow    = "show"
	subcmdAuthUpdate  = "update"
	subcmdAuthRemove  = commandRemove
	subcmdAuthLogin   = "login"
	subcmdAuthLogout  = "logout"
	subcmdAuthUser    = "user"
	subcmdAuthRole    = "role"
	subcmdAuthCluster = "cluster"

	// Default values for long running operations
	refreshRateDefault = time.Second
	countDefault       = 1
)

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
	bucketArgument         = "BUCKET_NAME"
	optionalBucketArgument = "[BUCKET_NAME]"
	bucketsArgument        = "BUCKET_NAME [BUCKET_NAME...]"
	bucketOldNewArgument   = bucketArgument + " NEW_NAME"
	bucketPropsArgument    = bucketArgument + " " + jsonSpecArgument + "|" + keyValuePairsArgument
	bucketAndPropsArgument = "BUCKET_NAME [PROP_PREFIX]"

	// Objects
	getObjectArgument        = "BUCKET_NAME/OBJECT_NAME OUT_FILE"
	putPromoteObjectArgument = "FILE|DIRECTORY BUCKET_NAME/[OBJECT_NAME]"
	concatObjectArgument     = "FILE|DIRECTORY [FILE|DIRECTORY...] BUCKET_NAME/OBJECT_NAME"
	objectArgument           = "BUCKET_NAME/OBJECT_NAME"
	optionalObjectsArgument  = "BUCKET_NAME/[OBJECT_NAME]..."
	objectOldNewArgument     = "BUCKET_NAME/OBJECT_NAME NEW_OBJECT_NAME"

	// Daemons
	daemonIDArgument         = "DAEMON_ID"
	optionalDaemonIDArgument = "[DAEMON_ID]"
	optionalTargetIDArgument = "[TARGET_ID]"
	showConfigArgument       = "DAEMON_ID [CONFIG_SECTION]"
	setConfigArgument        = optionalDaemonIDArgument + " " + keyValuePairsArgument
	attachRemoteAISArgument  = aliasURLPairArgument
	detachRemoteAISArgument  = aliasArgument
	attachMountpathArgument  = daemonMountpathPairArgument
	detachMountpathArgument  = daemonMountpathPairArgument
	joinNodeArgument         = "IP:PORT " + optionalDaemonIDArgument
	startDownloadArgument    = "SOURCE DESTINATION"
	jsonSpecArgument         = "JSON_SPECIFICATION"

	// Xactions
	xactionArgument = "XACTION_NAME"

	// List command
	listCommandArgument = "[PROVIDER://][BUCKET_NAME]"

	// Auth
	addUserArgument           = "USER_NAME [ROLE...]"
	deleteUserArgument        = "USER_NAME"
	userLoginArgument         = "USER_NAME"
	addAuthClusterArgument    = "CLUSTER_ID [ALIAS] URL [URL...]"
	deleteAuthClusterArgument = "CLUSTER_ID"
	showAuthClusterArgument   = "[CLUSTER_ID]"
	showAuthRoleArgument      = "[ROLE]"
	addAuthRoleArgument       = "ROLE [CLUSTER_ID PERMISSION ...]"
	deleteRoleArgument        = "ROLE"

	// Search
	searchArgument = "KEYWORD [KEYWORD...]"
)

// Flags
var (
	// Common
	objPropsFlag = cli.StringFlag{
		Name:  "props",
		Usage: "properties to return with object names, comma separated",
		Value: cmn.GetPropsName + "," + cmn.GetPropsSize,
	}
	prefixFlag      = cli.StringFlag{Name: "prefix", Usage: "prefix for string matching"}
	refreshFlag     = cli.DurationFlag{Name: "refresh", Usage: "refresh period", Value: refreshRateDefault}
	regexFlag       = cli.StringFlag{Name: "regex", Usage: "regex pattern for matching"}
	jsonFlag        = cli.BoolFlag{Name: "json,j", Usage: "json input/output"}
	noHeaderFlag    = cli.BoolFlag{Name: "no-headers,H", Usage: "display tables without headers"}
	progressBarFlag = cli.BoolFlag{Name: "progress", Usage: "display progress bar"}
	resetFlag       = cli.BoolFlag{Name: "reset", Usage: "reset to original state"}
	dryRunFlag      = cli.BoolFlag{Name: "dry-run", Usage: "preview the action without really doing it"}
	verboseFlag     = cli.BoolFlag{Name: "verbose,v", Usage: "verbose"}
	ignoreErrorFlag = cli.BoolFlag{
		Name:  "ignore-error",
		Usage: "ignore error on soft failures like bucket already exists, bucket does not exist etc.",
	}
	bucketPropsFlag = cli.StringFlag{Name: "bucket-props", Usage: "value represents custom properties of a bucket"}
	forceFlag       = cli.BoolFlag{Name: "force,f", Usage: "force an action"}

	allFlag         = cli.BoolFlag{Name: "all", Usage: "list all properties"}
	allXactionsFlag = cli.BoolTFlag{Name: "all", Usage: "show all xactions including finished"}
	allItemsFlag    = cli.BoolTFlag{Name: "all", Usage: "list all items"} // TODO: differentiate bucket names vs objects
	allJobsFlag     = cli.BoolTFlag{Name: "all", Usage: "remove all finished jobs"}

	// Bucket
	startAfterFlag = cli.StringFlag{
		Name:  "start-after",
		Usage: "list objects alphabetically starting from the object after given provided key",
	}
	objLimitFlag = cli.IntFlag{Name: "limit", Usage: "limit object count", Value: 0}
	pageSizeFlag = cli.IntFlag{Name: "page-size", Usage: "maximum number of entries by list objects call", Value: 1000}
	templateFlag = cli.StringFlag{Name: "template", Usage: "template for matching object names"}
	copiesFlag   = cli.IntFlag{Name: "copies", Usage: "number of object replicas", Value: 1, Required: true}
	maxPagesFlag = cli.IntFlag{Name: "max-pages", Usage: "display up to this number pages of bucket objects"}
	fastFlag     = cli.BoolTFlag{
		Name:  "fast",
		Usage: "use fast algorithm to compute the result (warning: the result can be inaccurate)",
	}
	pagedFlag         = cli.BoolFlag{Name: "paged", Usage: "fetch and print the bucket list page by page, ignored in fast mode"}
	showUnmatchedFlag = cli.BoolTFlag{Name: "show-unmatched", Usage: "list objects that were not matched by regex and template"}
	activeFlag        = cli.BoolFlag{Name: "active", Usage: "show only running xactions"}
	dataSlicesFlag    = cli.IntFlag{Name: "data-slices,data,d", Usage: "number of data slices", Required: true}
	paritySlicesFlag  = cli.IntFlag{Name: "parity-slices,parity,p", Usage: "number of parity slices", Required: true}
	listBucketsFlag   = cli.StringFlag{Name: "buckets", Usage: "comma-separated list of bucket names, eg. 'b1,b2,b3'"}

	// Daeclu
	countFlag = cli.IntFlag{Name: "count", Usage: "total number of generated reports", Value: countDefault}

	// Download
	descriptionFlag = cli.StringFlag{
		Name:  "description,desc",
		Usage: "description of the job (useful when listing all downloads)",
	}
	timeoutFlag          = cli.StringFlag{Name: "timeout", Usage: "timeout for request to external resource, eg. '30m'"}
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
	progressIntervalFlag = cli.StringFlag{Name: "progress-interval", Value: downloader.DownloadProgressInterval.String(), Usage: "interval(in secs) at which progress will be monitored, e.g. '10s'"}

	// dSort
	dsortBucketFlag = cli.StringFlag{
		Name:  "bucket",
		Value: cmn.DSortNameLowercase + "-testing", Usage: "bucket where shards will be put",
	}
	dsortTemplateFlag = cli.StringFlag{Name: "template", Value: "shard-{0..9}", Usage: "template of input shard name"}
	extFlag           = cli.StringFlag{Name: "ext", Value: ".tar", Usage: "extension for shards (either '.tar' or '.tgz')"}
	fileSizeFlag      = cli.StringFlag{Name: "fsize", Value: "1024", Usage: "single file size inside the shard"}
	logFlag           = cli.StringFlag{Name: "log", Usage: "path to file where the metrics will be saved"}
	cleanupFlag       = cli.BoolFlag{
		Name:  "cleanup",
		Usage: "remove old bucket and create it again. WARNING: it removes all objects that were present in the old bucket",
	}
	concurrencyFlag = cli.IntFlag{
		Name: "conc", Value: 10,
		Usage: "limits number of concurrent put requests and number of concurrent shards created",
	}
	fileCountFlag = cli.IntFlag{Name: "fcount", Value: 5, Usage: "number of files inside single shard"}
	specFileFlag  = cli.StringFlag{Name: "file,f", Value: "", Usage: "path to file with dSort specification"}

	// Object
	listFlag      = cli.StringFlag{Name: "list", Usage: "comma separated list of object names, eg. 'o1,o2,o3'"}
	offsetFlag    = cli.StringFlag{Name: "offset", Usage: "object read offset, can contain prefix 'b', 'KiB', 'MB'"}
	lengthFlag    = cli.StringFlag{Name: "length", Usage: "object read length, can contain prefix 'b', 'KiB', 'MB'"}
	isCachedFlag  = cli.BoolFlag{Name: "is-cached", Usage: "check if an object is cached"}
	cachedFlag    = cli.BoolFlag{Name: "cached", Usage: "list only cached objects"}
	checksumFlag  = cli.BoolFlag{Name: "checksum", Usage: "validate checksum"}
	recursiveFlag = cli.BoolFlag{Name: "recursive,r", Usage: "recursive operation"}
	overwriteFlag = cli.BoolFlag{Name: "overwrite,o", Usage: "overwrite destination if exists"}
	keepOrigFlag  = cli.BoolFlag{Name: "keep", Usage: "keep original file", Required: true}
	targetFlag    = cli.StringFlag{Name: "target", Usage: "ais target ID"}
	yesFlag       = cli.BoolFlag{Name: "yes,y", Usage: "assume 'yes' for all questions"}
	chunkSizeFlag = cli.StringFlag{
		Name:  "chunk-size",
		Usage: "chunk size used for each request, can contain prefix 'b', 'KiB', 'MB'", Value: "10MB",
	}
	computeCksumFlag = cli.BoolFlag{Name: "compute-cksum", Usage: "compute the checksum with the type configured for the bucket"}
	useCacheFlag     = cli.BoolFlag{Name: "use-cache", Usage: "use proxy cache to speed up list object request"}
	checksumFlags    = getCksumFlags()

	// AuthN
	tokenFileFlag = cli.StringFlag{Name: "file,f", Value: "", Usage: "save token to file"}
	passwordFlag  = cli.StringFlag{Name: "password,p", Value: "", Usage: "user password"}

	// Copy Bucket
	cpBckDryRunFlag = cli.BoolFlag{
		Name:  "dry-run",
		Usage: "show total size of new objects without really creating them",
	}
	cpBckPrefixFlag = cli.StringFlag{Name: "prefix", Usage: "prefix added to every new object's name"}

	// ETL
	etlExtFlag = cli.StringFlag{Name: "ext", Usage: "mapping from old to new extensions of transformed objects' names"}

	fromFileFlag = cli.StringFlag{Name: "from-file", Usage: "absolute path to the file with the code for ETL", Required: true}
	depsFileFlag = cli.StringFlag{
		Name:  "deps-file",
		Usage: "absolute path to the file with dependencies that must be installed before running the code",
	}
	runtimeFlag = cli.StringFlag{
		Name:  "runtime",
		Usage: "runtime which should be used when running the provided code", Required: true,
	}
	waitTimeoutFlag = cli.DurationFlag{
		Name:  "wait-timeout",
		Usage: "determines how long ais target should wait for pod to become ready",
	}
	maintenanceModeFlag = cli.StringFlag{
		Name: "mode", Required: true,
		Usage: "node maintenance mode: start-maintenance, stop-maintenance, decommission",
	}
	noRebalanceFlag = cli.BoolFlag{
		Name:  "no-rebalance",
		Usage: "do not run rebalance after putting a node under maintenance",
	}

	longRunFlags = []cli.Flag{refreshFlag, countFlag}

	baseLstRngFlags = []cli.Flag{
		listFlag,
		templateFlag,
	}
)

func getCksumFlags() []cli.Flag {
	var (
		checksums = cmn.SupportedChecksums()
		flags     = make([]cli.Flag, 0, len(checksums)-1)
	)
	for _, cksum := range checksums {
		if cksum == cmn.ChecksumNone {
			continue
		}
		flags = append(flags, cli.StringFlag{
			Name:  cksum,
			Usage: fmt.Sprintf("hex encoded string of the %s checksum", cksum),
		})
	}
	return flags
}
