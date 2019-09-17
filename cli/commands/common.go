// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that interact with buckets in the cluster
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

// Top level Command names
const (
	commandBucket = "bucket"
	commandConfig = "config"
	// commandDaeclu - all subcommands are top-level commands
	commandDownload = "download"
	commandDsort    = cmn.DSortNameLowercase
	commandObject   = "object"
	commandXaction  = "xaction"
	commandNode     = "node"

	//
	// VERBs
	//
	commandRename    = cmn.ActRename
	commandPrefetch  = cmn.ActPrefetch
	commandStart     = cmn.ActXactStart
	commandStop      = cmn.ActXactStop
	commandList      = "ls"
	commandCreate    = "create"
	commandSet       = "set"
	commandSetCopies = "set-copies"
	commandRemove    = "rm"
	commandEvict     = "evict"
	commandCopy      = "cp"
)

// Subcommand names
const (
	// Common
	subcommandRename = "rename"
	subcommandEvict  = "evict"
	subcommandStart  = "start"
	subcommandStatus = "status"
	subcommandAbort  = "abort"
	subcommandRemove = "remove"
	subcommandList   = "ls"
	subcommandSet    = "set"
	subcommandGet    = "get"

	// Bucket
	bucketCreate     = "create"
	bucketDestroy    = "destroy"
	bucketNWayMirror = cmn.ActMakeNCopies
	bucketEvict      = subcommandEvict
	bucketSummary    = "summary"
	bucketObjects    = "objects"
	bucketCopy       = "cp"

	// Bucket props
	commandBucketProps = "props"
	propsList          = subcommandList
	propsReset         = "reset"
	propsSet           = subcommandSet

	// Config
	configGet = subcommandGet
	configSet = subcommandSet

	// Daeclu
	daecluSmap       = cmn.GetWhatSmap
	daecluStats      = cmn.GetWhatStats
	daecluDiskStats  = cmn.GetWhatDiskStats
	daecluStatus     = cmn.GetWhatDaemonStatus
	daecluAddNode    = "add"
	daecluRemoveNode = "remove"

	// Download
	downloadStart  = subcommandStart
	downloadStatus = subcommandStatus
	downloadAbort  = subcommandAbort
	downloadRemove = subcommandRemove
	downloadList   = subcommandList

	// dSort
	dsortGen    = "gen"
	dsortStart  = subcommandStart
	dsortStatus = subcommandStatus
	dsortAbort  = subcommandAbort
	dsortRemove = subcommandRemove
	dsortList   = subcommandList

	// Object
	objGet      = subcommandGet
	objPut      = "put"
	objDel      = "delete"
	objStat     = "stat"
	objPrefetch = "prefetch"
	objEvict    = subcommandEvict

	// Xaction
	xactStart = cmn.ActXactStart
	xactStop  = cmn.ActXactStop
	xactStats = cmn.ActXactStats

	//
	// OBJECT subcommands
	//
	subcmdBuckets   = cmn.Buckets
	subcmdBucket    = "bucket"
	subcmdObjects   = cmn.Objects
	subcmdObject    = "object"
	subcmdProps     = "props"
	subcmdDownloads = "downloads"
	subcmdDownload  = "download"
	subcmdDsort     = cmn.DSortNameLowercase
	subcmdXaction   = "xaction"
	subcmdConfig    = "config"

	// List
	subcmdListBuckets   = subcmdBuckets
	subcmdListBckProps  = subcmdProps
	subcmdListObjects   = subcmdObjects
	subcmdListDownloads = subcmdDownloads
	subcmdListDsort     = subcmdDsort
	subcmdListConfig    = subcmdConfig

	// Create
	subcmdCreateBucket = subcmdBucket

	// Rename
	subcmdRenameBucket = subcmdBucket
	subcmdRenameObject = subcmdObject

	// Remove
	subcmdRemoveBucket = subcmdBucket
	subcmdRemoveObject = subcmdObject

	// Copy
	subcmdCopyBucket = subcmdBucket

	// Start
	subcmdStartXaction  = subcmdXaction
	subcmdStartDsort    = subcmdDsort
	subcmdStartDownload = subcmdDownload

	// Stop
	subcmdStopXaction  = subcmdXaction
	subcmdStopDsort    = subcmdDsort
	subcmdStopDownload = subcmdDownload

	// Set
	subcmdSetConfig = subcmdConfig
	subcmdSetProps  = subcmdProps
)

// Flag related constants
const (
	aisBucketEnvVar         = "AIS_BUCKET"
	aisBucketProviderEnvVar = "AIS_BUCKET_PROVIDER"

	refreshRateDefault = time.Second

	countDefault = 1
)

// Flags
var (
	// Common
	bucketFlag      = cli.StringFlag{Name: "bucket", Usage: "bucket where the objects are stored, eg. 'imagenet'", EnvVar: aisBucketEnvVar}
	bckProviderFlag = cli.StringFlag{Name: "provider",
		Usage:  "determines which bucket ('local' or 'cloud') should be used. By default, locality is determined automatically",
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
	allFlag           = cli.BoolTFlag{Name: "all", Usage: "show all items including old, duplicated etc"}
	fastFlag          = cli.BoolTFlag{Name: "fast", Usage: "use fast API to list all object names in a bucket. Flags 'props', 'all', 'limit', and 'page-size' are ignored in this mode"}
	pagedFlag         = cli.BoolFlag{Name: "paged", Usage: "fetch and print the bucket list page by page, ignored in fast mode"}
	propsFlag         = cli.BoolFlag{Name: "props", Usage: "properties of a bucket"}
	showUnmatchedFlag = cli.BoolTFlag{Name: "show-unmatched", Usage: "also list objects that were not matched by regex and template"}
	activeFlag        = cli.BoolFlag{Name: "active", Usage: "show only running xaction"}

	// Daeclu
	countFlag      = cli.IntFlag{Name: "count", Usage: "total number of generated reports", Value: countDefault}
	daemonIDFlag   = cli.StringFlag{Name: "daemon-id", Usage: "specifies the unique name for node (default: random string)"}
	daemonTypeFlag = cli.StringFlag{Name: "daemon-type", Usage: "type of the node, either 'proxy' or 'target'", Value: "target"}
	publicAddrFlag = cli.StringFlag{Name: "public-addr", Usage: "public socket address to communicate with the node, needs to be in format: 'IP:PORT'"}

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
	fileFlag      = cli.StringFlag{Name: "file", Usage: "filepath for content of the object"}
	lengthFlag    = cli.StringFlag{Name: "length", Usage: "object read length"}
	listFlag      = cli.StringFlag{Name: "list", Usage: "comma separated list of object names, eg. 'o1,o2,o3'"}
	nameFlag      = cli.StringFlag{Name: "name", Usage: "name of object"}
	newNameFlag   = cli.StringFlag{Name: "new-name", Usage: "new name of object"}
	outFileFlag   = cli.StringFlag{Name: "out-file", Usage: "name of the file where the contents will be saved"}
	offsetFlag    = cli.StringFlag{Name: "offset", Usage: "object read offset"}
	rangeFlag     = cli.StringFlag{Name: "range", Usage: "colon separated interval of object indices, eg. <START>:<STOP>"}
	cachedFlag    = cli.BoolFlag{Name: "cached", Usage: "check if an object is cached"}
	checksumFlag  = cli.BoolFlag{Name: "checksum", Usage: "validate checksum"}
	waitFlag      = cli.BoolTFlag{Name: "wait", Usage: "wait for operation to finish before returning response"}
	recursiveFlag = cli.BoolFlag{Name: "recursive,r", Usage: "recursive operation"}
	baseFlag      = cli.StringFlag{Name: "base", Usage: "a common part of a path for all objects that is not used to generate object name"}
	yesFlag       = cli.BoolFlag{Name: "yes,y", Usage: "assume 'yes' for all questions"}
)

// Command argument texts
const (
	// Common
	idArgumentText            = "ID"
	noArgumentsText           = " "
	keyValuePairArgumentsText = "KEY=VALUE [KEY=VALUE...]"

	// Bucket
	bucketArgumentText       = "BUCKET_NAME"
	bucketsArgumentText      = "BUCKET_NAME [BUCKET_NAME...]"
	bucketOldNewArgumentText = bucketArgumentText + " NEW_NAME"
	bucketPropsArgumentText  = bucketArgumentText + " " + keyValuePairArgumentsText

	// Object
	objectsOptionalArgumentText      = "BUCKET_NAME/[OBJECT_NAME]..."
	objectPrefetchBucketArgumentText = "BUCKET_NAME/"
	objectOldNewArgumentText         = "BUCKET_NAME/OBJECT_NAME NEW_OBJECT_NAME"

	// Provider
	providerOptionalArgumentText = "[BUCKET_PROVIDER]"

	// Config
	daemonIDArgumentText  = "DAEMON_ID"
	configSetArgumentText = "[DAEMON_ID] " + keyValuePairArgumentsText

	// Daeclu
	daemonTypeArgumentText = "[DAEMON_TYPE]"
	targetIDArgumentText   = "[TARGET_ID]"

	// Download
	downloadStartArgumentText = "SOURCE DESTINATION"

	// dSort
	jsonSpecArgumentText = "JSON_SPECIFICATION"

	// Xaction
	xactionArgumentText                   = "XACTION_NAME"
	xactionOptionalArgumentText           = "[XACTION_NAME]"
	xactionWithOptionalBucketArgumentText = "XACTION_NAME [BUCKET_NAME]"
	xactionStopAll                        = "all"
	xactionStopCommandArgumentText        = "XACTION_NAME|" + xactionStopAll + " [BUCKET_NAME]"
)

// Command help templates
// TODO: review constant names (#2 & #3 should maybe be switched)
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
   {{if .UsageText}}{{.UsageText}}{{else}}{{.HelpName}} {{if .ArgsUsage}}{{.ArgsUsage}}{{else}}[arguments...]{{end}}{{if .VisibleFlags}} [OPTIONS...]{{end}}{{end}}{{if .Description}}

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
   {{if .UsageText}}{{.UsageText}}{{else}}{{.HelpName}} COMMAND{{if .VisibleFlags}} [COMMAND OPTIONS...]{{end}}{{end}}

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
