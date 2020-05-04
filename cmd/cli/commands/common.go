// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file contains common constants and variables used in other files.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"time"

	"github.com/NVIDIA/aistore/cmn"
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
	jobIDArgument         = "JOB_ID"
	optionalJobIDArgument = "[JOB_ID]"

	// Buckets
	bucketArgument         = "BUCKET_NAME"
	optionalBucketArgument = "[BUCKET_NAME]"
	bucketsArgument        = "BUCKET_NAME [BUCKET_NAME...]"
	bucketOldNewArgument   = bucketArgument + " NEW_NAME"
	bucketPropsArgument    = bucketArgument + " " + keyValuePairsArgument
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
	addUserArgument    = "USER_NAME USER_PASSWORD"
	deleteUserArgument = "USER_NAME"
	userLoginArgument  = "USER_NAME USER_PASSWORD"
)

// Flags
var (
	// Common
	objPropsFlag    = cli.StringFlag{Name: "props", Usage: "properties to return with object names, comma separated", Value: "size,version"}
	prefixFlag      = cli.StringFlag{Name: "prefix", Usage: "prefix for string matching"}
	refreshFlag     = cli.DurationFlag{Name: "refresh", Usage: "refresh period", Value: refreshRateDefault}
	regexFlag       = cli.StringFlag{Name: "regex", Usage: "regex pattern for matching"}
	jsonFlag        = cli.BoolFlag{Name: "json,j", Usage: "json input/output"}
	noHeaderFlag    = cli.BoolFlag{Name: "no-headers,H", Usage: "display tables without headers"}
	progressBarFlag = cli.BoolFlag{Name: "progress", Usage: "display progress bar"}
	resetFlag       = cli.BoolFlag{Name: "reset", Usage: "reset to original state"}
	dryRunFlag      = cli.BoolFlag{Name: "dry-run", Usage: "preview the action without really doing it"}
	verboseFlag     = cli.BoolFlag{Name: "verbose,v", Usage: "verbose"}

	// Bucket
	jsonspecFlag      = cli.StringFlag{Name: "jsonspec", Usage: "bucket properties in JSON format"}
	markerFlag        = cli.StringFlag{Name: "marker", Usage: "list objects alphabetically starting from the object after the marker"}
	objLimitFlag      = cli.IntFlag{Name: "limit", Usage: "limit object count", Value: 0}
	pageSizeFlag      = cli.IntFlag{Name: "page-size", Usage: "maximum number of entries by list objects call", Value: 1000}
	templateFlag      = cli.StringFlag{Name: "template", Usage: "template for matching object names"}
	copiesFlag        = cli.IntFlag{Name: "copies", Usage: "number of object replicas", Value: 1, Required: true}
	maxPagesFlag      = cli.IntFlag{Name: "max-pages", Usage: "display up to this number pages of bucket objects"}
	allItemsFlag      = cli.BoolTFlag{Name: "all-items", Usage: "show all items including old and duplicated"}
	fastFlag          = cli.BoolTFlag{Name: "fast", Usage: "use fast API to list all object names in a bucket. Flags 'props', 'all-items', 'limit', and 'page-size' are ignored in this mode"}
	fastDetailsFlag   = cli.BoolFlag{Name: "fast", Usage: "enforce using faster methods to find out the buckets' details, note: the output may not be accurate"}
	pagedFlag         = cli.BoolFlag{Name: "paged", Usage: "fetch and print the bucket list page by page, ignored in fast mode"}
	showUnmatchedFlag = cli.BoolTFlag{Name: "show-unmatched", Usage: "list objects that were not matched by regex and template"}
	activeFlag        = cli.BoolFlag{Name: "active", Usage: "show only running xactions"}

	// Daeclu
	countFlag = cli.IntFlag{Name: "count", Usage: "total number of generated reports", Value: countDefault}

	// Download
	descriptionFlag  = cli.StringFlag{Name: "description,desc", Usage: "description of the job - can be useful when listing all downloads"}
	timeoutFlag      = cli.StringFlag{Name: "timeout", Usage: "timeout for request to external resource, eg. '30m'"}
	limitConnections = cli.IntFlag{Name: "limit-connections,conns", Usage: "number of connections each target can make concurrently (each target can handle at most #mountpaths connections)"}

	// dSort
	dsortBucketFlag   = cli.StringFlag{Name: "bucket", Value: cmn.DSortNameLowercase + "-testing", Usage: "bucket where shards will be put"}
	dsortTemplateFlag = cli.StringFlag{Name: "template", Value: "shard-{0..9}", Usage: "template of input shard name"}
	extFlag           = cli.StringFlag{Name: "ext", Value: ".tar", Usage: "extension for shards (either '.tar' or '.tgz')"}
	fileSizeFlag      = cli.StringFlag{Name: "fsize", Value: "1024", Usage: "single file size inside the shard"}
	logFlag           = cli.StringFlag{Name: "log", Usage: "path to file where the metrics will be saved"}
	cleanupFlag       = cli.BoolFlag{Name: "cleanup", Usage: "remove old bucket and create it again. WARNING: it removes all objects that were present in the old bucket"}
	concurrencyFlag   = cli.IntFlag{Name: "conc", Value: 10, Usage: "limits number of concurrent put requests and number of concurrent shards created"}
	fileCountFlag     = cli.IntFlag{Name: "fcount", Value: 5, Usage: "number of files inside single shard"}
	specFileFlag      = cli.StringFlag{Name: "file,f", Value: "", Usage: "path to file with dSort specification"}

	// Object
	listFlag      = cli.StringFlag{Name: "list", Usage: "comma separated list of object names, eg. 'o1,o2,o3'"}
	offsetFlag    = cli.StringFlag{Name: "offset", Usage: "object read offset, can contain prefix 'b', 'KiB', 'MB'"}
	lengthFlag    = cli.StringFlag{Name: "length", Usage: "object read length, can contain prefix 'b', 'KiB', 'MB'"}
	isCachedFlag  = cli.BoolFlag{Name: "is-cached", Usage: "check if an object is cached"}
	cachedFlag    = cli.BoolFlag{Name: "cached", Usage: "list only cached objects"}
	checksumFlag  = cli.BoolFlag{Name: "checksum", Usage: "validate checksum"}
	recursiveFlag = cli.BoolFlag{Name: "recursive,r", Usage: "recursive operation"}
	overwriteFlag = cli.BoolFlag{Name: "overwrite,o", Usage: "overwrite destination if exists"}
	targetFlag    = cli.StringFlag{Name: "target", Usage: "ais target ID"}
	yesFlag       = cli.BoolFlag{Name: "yes,y", Usage: "assume 'yes' for all questions"}

	longRunFlags = []cli.Flag{refreshFlag, countFlag}

	baseLstRngFlags = []cli.Flag{
		listFlag,
		templateFlag,
	}
)
