// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file contains common constants and variables used in other files.
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
	commandRename    = cmn.ActRename
	commandPrefetch  = cmn.ActPrefetch
	commandStart     = cmn.ActXactStart
	commandStop      = cmn.ActXactStop
	commandStatus    = cmn.GetWhatDaemonStatus
	commandStats     = cmn.GetWhatStats
	commandShow      = "show"
	commandGenShards = "gen-shards"
	commandAuth      = "auth"
	commandList      = "ls"
	commandCreate    = "create"
	commandSet       = "set"
	commandSetCopies = "set-copies"
	commandRemove    = "rm"
	commandEvict     = "evict"
	commandCopy      = "cp"
	commandRegister  = "register"
	commandGet       = "get"
	commandPut       = "put"

	// Subcommands - preferably nouns
	subcmdDsort     = cmn.DSortNameLowercase
	subcmdSmap      = cmn.GetWhatSmap
	subcmdDisk      = cmn.GetWhatDiskStats
	subcmdConfig    = cmn.GetWhatConfig
	subcmdRebalance = cmn.ActGlobalReb
	subcmdAIS       = "ais"
	subcmdCloud     = "cloud"
	subcmdBucket    = "bucket"
	subcmdObject    = "object"
	subcmdProps     = "props"
	subcmdDownload  = "download"
	subcmdXaction   = "xaction"
	subcmdNode      = "node"
	subcmdProxy     = "proxy"
	subcmdTarget    = "target"

	// List subcommands
	subcmdListAIS      = subcmdAIS
	subcmdListCloud    = subcmdCloud
	subcmdListBckProps = subcmdProps
	subcmdListConfig   = subcmdConfig
	subcmdListSmap     = subcmdSmap

	// Show subcommands
	subcmdShowBucket    = subcmdBucket
	subcmdShowDisk      = subcmdDisk
	subcmdShowDownload  = subcmdDownload
	subcmdShowDsort     = subcmdDsort
	subcmdShowObject    = subcmdObject
	subcmdShowNode      = subcmdNode
	subcmdShowXaction   = subcmdXaction
	subcmdShowRebalance = subcmdRebalance

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

	// Set subcommands
	subcmdSetConfig = subcmdConfig
	subcmdSetProps  = subcmdProps

	// Register subcommands
	subcmdRegisterProxy  = subcmdProxy
	subcmdRegisterTarget = subcmdTarget

	// Env. var. related constants
	aisBucketEnvVar         = "AIS_BUCKET"
	aisBucketProviderEnvVar = "AIS_BUCKET_PROVIDER"

	// Default values for long running operations
	refreshRateDefault = time.Second
	countDefault       = 1
)

// Argument placeholders in help messages
// Name format: *Argument
const (
	// Common
	noArguments           = " "
	allArgument           = "all"
	keyValuePairsArgument = "KEY=VALUE [KEY=VALUE...]"

	// Job IDs (download, dsort)
	jobIDArgument         = "JOB_ID"
	optionalJobIDArgument = "[JOB_ID]"

	// Buckets
	bucketArgument                      = "BUCKET_NAME"
	optionalBucketArgument              = "[BUCKET_NAME]"
	optionalBucketWithSeparatorArgument = "[BUCKET_NAME/]"
	bucketsArgument                     = "BUCKET_NAME [BUCKET_NAME...]"
	bucketOldNewArgument                = bucketArgument + " NEW_NAME"
	bucketPropsArgument                 = bucketArgument + " " + keyValuePairsArgument

	// Objects
	getObjectArgument            = "BUCKET_NAME/OBJECT_NAME OUT_FILE"
	putObjectArgument            = "FILE|DIRECTORY BUCKET_NAME/[OBJECT_NAME]"
	objectArgument               = "BUCKET_NAME/OBJECT_NAME"
	optionalObjectsArgument      = "BUCKET_NAME/[OBJECT_NAME]..."
	prefetchObjectBucketArgument = "BUCKET_NAME/"
	objectOldNewArgument         = "BUCKET_NAME/OBJECT_NAME NEW_OBJECT_NAME"

	// Daemons
	daemonIDArgument           = "DAEMON_ID"
	optionalDaemonIDArgument   = "[DAEMON_ID]"
	optionalTargetIDArgument   = "[TARGET_ID]"
	optionalDaemonTypeArgument = "[DAEMON_TYPE]"
	daemonStatusArgument       = optionalDaemonTypeArgument + "|" + optionalDaemonIDArgument
	listConfigArgument         = "DAEMON_ID [CONFIG_SECTION]"
	setConfigArgument          = optionalDaemonIDArgument + " " + keyValuePairsArgument
	registerNodeArgument       = "IP:PORT " + optionalDaemonIDArgument
	startDownloadArgument      = "SOURCE DESTINATION"
	jsonSpecArgument           = "JSON_SPECIFICATION"

	// Xactions
	xactionArgument                           = "XACTION_NAME"
	xactionWithOptionalBucketArgument         = "XACTION_NAME [BUCKET_NAME]"
	optionalXactionWithOptionalBucketArgument = "[XACTION_NAME] [BUCKET_NAME]"
	stopCommandXactionArgument                = "XACTION_NAME|" + allArgument + " [BUCKET_NAME]"

	// List command
	listCommandArgument = "[COMMAND | BUCKET_NAME/]"

	// Auth
	addUserArgument    = "USER_NAME USER_PASSWORD"
	deleteUserArgument = "USER_NAME"
	userLoginArgument  = "USER_NAME USER_PASSWORD"
)

// Help command templates
const (
	AISHelpTemplate = `DESCRIPTION:
   {{ .Name }}{{ if .Usage }} - {{ .Usage }}{{end}}
   Ver. {{ if .Version }}{{ if not .HideVersion }}{{ .Version }}{{end}}{{end}}

USAGE:
   {{ .Name }} [GLOBAL OPTIONS...] COMMAND

COMMANDS:{{ range .VisibleCategories }}{{ if .Name }}
   {{ .Name }}:{{end}}{{ range .VisibleCommands }}
     {{ join .Names ", " }}{{ "\t" }}{{ .Usage }}{{end}}{{end}}

GLOBAL OPTIONS:
   {{- range .VisibleFlags }}
   {{.}}
   {{- end}}
`

	AISCommandHelpTemplate = `NAME:
   {{.HelpName}} - {{.Usage}}

USAGE:
   {{if .UsageText}}{{.UsageText}}{{else}}{{.HelpName}} {{if .ArgsUsage}}{{.ArgsUsage}}{{else}}[ARGUMENTS...]{{end}}{{if .VisibleFlags}} [OPTIONS...]{{end}}{{end}}{{if .Description}}

DESCRIPTION:
   {{.Description}}{{end}}{{if .VisibleFlags}}

OPTIONS:
   {{- range .VisibleFlags}}
   {{.}}
   {{- end}}
{{end}}`

	AISSubcommandHelpTemplate = `NAME:
   {{.HelpName}} - {{if .Description}}{{.Description}}{{else}}{{.Usage}}{{end}}

USAGE:
   {{.HelpName}} {{if .ArgsUsage}}{{.ArgsUsage}}{{else}}COMMAND{{end}}{{if .VisibleFlags}} [COMMAND OPTIONS...]{{end}}

COMMANDS:{{range .VisibleCategories}}{{if .Name}}
   {{.Name}}:{{end}}{{range .VisibleCommands}}
     {{join .Names ", "}}{{"\t"}}{{.Usage}}{{end}}
{{end}}{{if .VisibleFlags}}
OPTIONS:
   {{- range .VisibleFlags}}
   {{.}}
   {{- end}}
{{end}}`
)

// Flags
var (
	// Common
	providerFlag = cli.StringFlag{Name: "provider",
		Usage:  "determines which type bucket ('ais' or 'cloud') should be used. Other supported values include '' (empty), 'gcp', and 'aws'. By default (i.e., when unspecified), provider of the bucket is determined automatically",
		EnvVar: aisBucketProviderEnvVar,
	}
	objPropsFlag    = cli.StringFlag{Name: "props", Usage: "properties to return with object names, comma separated", Value: "size,version"}
	prefixFlag      = cli.StringFlag{Name: "prefix", Usage: "prefix for string matching"}
	refreshFlag     = cli.StringFlag{Name: "refresh", Usage: "refresh period", Value: refreshRateDefault.String()}
	regexFlag       = cli.StringFlag{Name: "regex", Usage: "regex pattern for matching"}
	jsonFlag        = cli.BoolFlag{Name: "json,j", Usage: "json input/output"}
	noHeaderFlag    = cli.BoolFlag{Name: "no-headers,H", Usage: "display tables without headers"}
	progressBarFlag = cli.BoolFlag{Name: "progress", Usage: "display progress bar"}
	resetFlag       = cli.BoolFlag{Name: "reset", Usage: "reset to original state"}

	// Bucket
	jsonspecFlag      = cli.StringFlag{Name: "jsonspec", Usage: "bucket properties in JSON format"}
	markerFlag        = cli.StringFlag{Name: "marker", Usage: "start listing bucket objects starting from the object that follows the marker(alphabetically), ignored in fast mode"}
	objLimitFlag      = cli.StringFlag{Name: "limit", Usage: "limit object count", Value: "0"}
	pageSizeFlag      = cli.StringFlag{Name: "page-size", Usage: "maximum number of entries by list bucket call", Value: "1000"}
	templateFlag      = cli.StringFlag{Name: "template", Usage: "template for matching object names"}
	copiesFlag        = cli.IntFlag{Name: "copies", Usage: "number of object replicas", Value: 1}
	maxPagesFlag      = cli.IntFlag{Name: "max-pages", Usage: "display up to this number pages of bucket objects"}
	allItemsFlag      = cli.BoolTFlag{Name: "all-items", Usage: "show all items including old, duplicated etc"}
	fastFlag          = cli.BoolTFlag{Name: "fast", Usage: "use fast API to list all object names in a bucket. Flags 'props', 'all-items', 'limit', and 'page-size' are ignored in this mode"}
	pagedFlag         = cli.BoolFlag{Name: "paged", Usage: "fetch and print the bucket list page by page, ignored in fast mode"}
	showUnmatchedFlag = cli.BoolTFlag{Name: "show-unmatched", Usage: "also list objects that were not matched by regex and template"}
	activeFlag        = cli.BoolFlag{Name: "active", Usage: "show only running xactions"}

	// Daeclu
	countFlag = cli.IntFlag{Name: "count", Usage: "total number of generated reports", Value: countDefault}

	// Download
	descriptionFlag = cli.StringFlag{Name: "description,desc", Usage: "description of the job - can be useful when listing all downloads"}
	timeoutFlag     = cli.StringFlag{Name: "timeout", Usage: "timeout for request to external resource, eg. '30m'"}
	verboseFlag     = cli.BoolFlag{Name: "verbose,v", Usage: "verbose"}

	// dSort
	dsortBucketFlag   = cli.StringFlag{Name: "bucket", Value: cmn.DSortNameLowercase + "-testing", Usage: "bucket where shards will be put"}
	dsortTemplateFlag = cli.StringFlag{Name: "template", Value: "shard-{0..9}", Usage: "template of input shard name"}
	extFlag           = cli.StringFlag{Name: "ext", Value: ".tar", Usage: "extension for shards (either '.tar' or '.tgz')"}
	fileSizeFlag      = cli.StringFlag{Name: "fsize", Value: "1024", Usage: "single file size inside the shard"}
	logFlag           = cli.StringFlag{Name: "log", Usage: "path to file where the metrics will be saved"}
	cleanupFlag       = cli.BoolFlag{Name: "cleanup", Usage: "when set, the old bucket will be deleted and created again"}
	concurrencyFlag   = cli.IntFlag{Name: "conc", Value: 10, Usage: "limits number of concurrent put requests and number of concurrent shards created"}
	fileCountFlag     = cli.IntFlag{Name: "fcount", Value: 5, Usage: "number of files inside single shard"}

	// Object
	deadlineFlag  = cli.StringFlag{Name: "deadline", Usage: "amount of time (Go Duration string) before the request expires", Value: "0s"}
	lengthFlag    = cli.StringFlag{Name: "length", Usage: "object read length"}
	listFlag      = cli.StringFlag{Name: "list", Usage: "comma separated list of object names, eg. 'o1,o2,o3'"}
	offsetFlag    = cli.StringFlag{Name: "offset", Usage: "object read offset"}
	rangeFlag     = cli.StringFlag{Name: "range", Usage: "colon separated interval of object indices, eg. <START>:<STOP>"}
	isCachedFlag  = cli.BoolFlag{Name: "is-cached", Usage: "check if an object is cached"}
	cachedFlag    = cli.BoolFlag{Name: "cached", Usage: "list only cached objects"}
	checksumFlag  = cli.BoolFlag{Name: "checksum", Usage: "validate checksum"}
	waitFlag      = cli.BoolTFlag{Name: "wait", Usage: "wait for operation to finish before returning response"}
	recursiveFlag = cli.BoolFlag{Name: "recursive,r", Usage: "recursive operation"}
	baseFlag      = cli.StringFlag{Name: "base", Usage: "a common part of a path for all objects that is not used to generate object name"}
	yesFlag       = cli.BoolFlag{Name: "yes,y", Usage: "assume 'yes' for all questions"}

	longRunFlags = []cli.Flag{refreshFlag, countFlag}

	baseLstRngFlags = []cli.Flag{
		listFlag,
		rangeFlag,
		prefixFlag,
		regexFlag,
		waitFlag,
		deadlineFlag,
	}
)
