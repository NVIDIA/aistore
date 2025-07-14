// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that control running jobs in the cluster.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/hf"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ext/dload"
	"github.com/NVIDIA/aistore/ext/dsort"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/xact"

	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
)

const prefetchUsage = "Prefetch one remote bucket, multiple remote buckets, or\n" +
	indent1 + "selected objects in a given remote bucket or buckets, e.g.:\n" +
	indent1 + "\t- 'prefetch gs://abc'\t- prefetch entire bucket (all gs://abc objects that are _not_ in-cluster);\n" +
	indent1 + "\t- 'prefetch gs://abc --num-workers 32'\t- same as above with 32 concurrent (prefetching) workers;\n" +
	indent1 + "\t- 'prefetch gs:'\t- prefetch all visible/accessible GCP buckets;\n" +
	indent1 + "\t- 'prefetch gs: --num-workers=48'\t- same as above employing 48 workers;\n" +
	indent1 + "\t- 'prefetch gs://abc --prefix images/'\t- prefetch all objects from the virtual subdirectory \"images\";\n" +
	indent1 + "\t- 'prefetch gs://abc --prefix images/ --nr'\t- prefetch only immediate contents of \"images/\" (non-recursive);\n" +
	indent1 + "\t- 'prefetch gs://abc --template images/'\t- same as above;\n" +
	indent1 + "\t- 'prefetch gs://abc/images/'\t- same as above;\n" +
	indent1 + "\t- 'prefetch gs://abc --template \"shard-{0000..9999}.tar.lz4\"'\t- prefetch the matching range (prefix + brace expansion);\n" +
	indent1 + "\t- 'prefetch \"gs://abc/shard-{0000..9999}.tar.lz4\"'\t- same as above (notice double quotes)"

const blobDownloadUsage = "Download a large object or multiple objects from remote storage, e.g.:\n" +
	indent1 + "\t- 'blob-download s3://ab/largefile --chunk-size=2mb --progress'\t- download one blob at a given chunk size\n" +
	indent1 + "\t- 'blob-download s3://ab --list \"f1, f2\" --num-workers=4 --progress'\t- run 4 concurrent readers to download 2 (listed) blobs\n" +
	indent1 + "When _not_ using '--progress' option, run 'ais show job' to monitor."

const downloadUsage = "Download files and objects from remote sources, e.g.:\n" +
	indent1 + "\t- 'ais download http://example.com/file.tar ais://bucket/'\t- download from HTTP into AIS bucket;\n" +
	indent1 + "\t- 'ais download s3://bucket/file.tar ais://local-bucket/'\t- download from S3 into AIS bucket;\n" +
	indent1 + "\t- 'ais download gs://bucket/file.tar ais://local/ --progress'\t- download from GCS with progress monitoring;\n" +
	indent1 + "\t- 'ais download s3://bucket ais://local --sync'\t- download and sync entire S3 bucket;\n" +
	indent1 + "\t- 'ais download \"gs://bucket/file-{001..100}.tar\" ais://local/'\t- download range of files using template;\n" +
	indent1 + "\t- 'ais download --hf-model bert-base-uncased --hf-file config.json ais://local/'\t- download specific file from HuggingFace model;\n" +
	indent1 + "\t- 'ais download --hf-dataset squad --hf-file train-v1.1.json ais://local/ --hf-auth'\t- download dataset file with authentication;\n" +
	indent1 + "\t- 'ais download --hf-model bert-large-uncased ais://local/ --blob-threshold 100MB'\t- download model with size-based routing (large files get individual jobs)."

const resilverUsage = "Resilver user data on a given target (or all targets in the cluster); entails:\n" +
	indent1 + "\t- fix data redundancy with respect to bucket configuration;\n" +
	indent1 + "\t- remove migrated objects and old/obsolete workfiles."

const stopUsage = "Terminate a single batch job or multiple jobs, e.g.:\n" +
	indent1 + "\t- 'stop tco-cysbohAGL'\t- terminate a given (multi-object copy/transform) job identified by its unique ID;\n" +
	indent1 + "\t- 'stop copy-listrange'\t- terminate all multi-object copies;\n" +
	indent1 + "\t- 'stop copy-objects'\t- same as above (using display name);\n" +
	indent1 + "\t- 'stop list'\t- stop all list-objects jobs;\n" +
	indent1 + "\t- 'stop ls'\t- same as above;\n" +
	indent1 + "\t- 'stop prefetch-listrange'\t- stop all prefetch jobs;\n" +
	indent1 + "\t- 'stop prefetch'\t- same as above;\n" +
	indent1 + "\t- 'stop g731 --force'\t- forcefully abort global rebalance g731 (advanced usage only);\n" +
	indent1 + "\t- 'stop --all'\t- terminate all running jobs\n" +
	indent1 + tabHelpOpt + "."

// top-level job command
var (
	jobCmd = cli.Command{
		Name:        commandJob,
		Usage:       "Monitor, query, start/stop and manage jobs and eXtended actions (xactions)",
		Subcommands: jobSub,
	}
	// NOTE: `appendJobSub` (below) expects jobSub[0] to be the `jobStartSub`
	jobSub = []cli.Command{
		jobStartSub,
		jobStopSub,
		jobWaitSub,
		jobRemoveSub,
		makeAlias(&showCmdJob, &mkaliasOpts{newName: commandShow}),
	}
)

// job start
var (
	startCommonFlags = []cli.Flag{
		waitFlag,
		waitJobXactFinishedFlag,
		nonverboseFlag,
	}
	startSpecialFlags = map[string][]cli.Flag{
		commandRebalance: {
			verbObjPrefixFlag,
		},
		cmdDownload: {
			dloadTimeoutFlag,
			descJobFlag,
			limitConnectionsFlag,
			objectsListFlag,
			dloadProgressFlag,
			progressFlag,
			waitFlag,
			waitJobXactFinishedFlag,
			limitBytesPerHourFlag,
			syncFlag,
			unitsFlag,
			blobThresholdFlag,
			// huggingface flags
			hfModelFlag,
			hfDatasetFlag,
			hfFileFlag,
			hfRevisionFlag,
			hfAuthFlag,
		},
		cmdDsort: {
			specFlag,
			verboseFlag,
		},
		commandPrefetch: append(
			listRangeProgressWaitFlags,
			dryRunFlag,
			verbObjPrefixFlag,
			latestVerFlag,
			nonRecursFlag, // (embedded prefix dopOLTP)
			blobThresholdFlag,
			yesFlag,
			numWorkersFlag,
			dontHeadRemoteFlag,
		),
		cmdBlobDownload: {
			refreshFlag,
			progressFlag,
			listFlag,
			chunkSizeFlag,
			numBlobWorkersFlag,
			waitFlag,
			waitJobXactFinishedFlag,
			latestVerFlag,
			nonverboseFlag,
		},
		cmdLRU: {
			lruBucketsFlag,
			forceFlag,
			nonverboseFlag,
		},
	}

	jobStartRebalance = cli.Command{
		Name:      commandRebalance,
		Usage:     "Rebalance ais cluster",
		ArgsUsage: bucketEmbeddedPrefixArg,
		Flags:     sortFlags(startSpecialFlags[commandRebalance]),
		Action:    startRebHandler,
	}
	jobStartResilver = cli.Command{
		Name:         commandResilver,
		Usage:        resilverUsage,
		ArgsUsage:    optionalTargetIDArgument,
		Flags:        sortFlags(startCommonFlags),
		Action:       startResilverHandler,
		BashComplete: suggestTargets,
	}

	prefetchStartCmd = cli.Command{
		Name:         commandPrefetch,
		Usage:        prefetchUsage,
		ArgsUsage:    bucketObjectOrTemplateMultiArg,
		Flags:        sortFlags(startSpecialFlags[commandPrefetch]),
		Action:       startPrefetchHandler,
		BashComplete: bucketCompletions(bcmplop{multiple: true}),
	}
	blobDownloadCmd = cli.Command{
		Name:         cmdBlobDownload,
		Usage:        blobDownloadUsage,
		ArgsUsage:    objectArgument,
		Flags:        sortFlags(startSpecialFlags[cmdBlobDownload]),
		Action:       blobDownloadHandler,
		BashComplete: remoteBucketCompletions(bcmplop{multiple: true}),
	}

	jobStartSub = cli.Command{
		Name:  commandStart,
		Usage: "Run batch job",
		Subcommands: []cli.Command{
			prefetchStartCmd,
			blobDownloadCmd,
			{
				Name:      cmdDownload,
				Usage:     downloadUsage,
				ArgsUsage: startDownloadArgument,
				Flags:     sortFlags(startSpecialFlags[cmdDownload]),
				Action:    startDownloadHandler,
			},
			dsortStartCmd,
			{
				Name:         cmdLRU,
				Usage:        "Run LRU eviction",
				Flags:        sortFlags(startSpecialFlags[cmdLRU]),
				Action:       startLRUHandler,
				BashComplete: bucketCompletions(bcmplop{}),
			},
			{
				Name:  commandETL,
				Usage: "Start ETL",
				Subcommands: []cli.Command{
					initCmdETL,
					objCmdETL,
					bckCmdETL,
				},
			},

			jobStartRebalance,
			jobStartResilver,

			cleanupCmd,

			// NOTE: append all `startableXactions`
		},
	}
)

// ais stop
var (
	stopCmdsFlags = []cli.Flag{
		allRunningJobsFlag,
		regexJobsFlag,
		forceFlag,
		yesFlag,
	}
	jobStopSub = cli.Command{
		Name:         commandStop,
		Usage:        stopUsage,
		ArgsUsage:    jobAnyArg,
		Flags:        sortFlags(stopCmdsFlags),
		Action:       stopJobHandler,
		BashComplete: runningJobCompletions,
	}
)

// ais wait
var (
	waitCmdsFlags = []cli.Flag{
		refreshFlag,
		progressFlag,
		waitJobXactFinishedFlag,
	}
	jobWaitSub = cli.Command{
		Name:         commandWait,
		Usage:        "wait for a specific batch job to complete (" + tabHelpOpt + ")",
		ArgsUsage:    jobAnyArg,
		Flags:        sortFlags(waitCmdsFlags),
		Action:       waitJobHandler,
		BashComplete: runningJobCompletions,
	}
)

// ais job remove
var (
	removeCmdsFlags = []cli.Flag{
		allFinishedJobsFlag,
		regexJobsFlag,
	}
	jobRemoveSub = cli.Command{
		Name:  commandRemove,
		Usage: "cleanup finished jobs",
		Subcommands: []cli.Command{
			{
				Name:         cmdDownload,
				Usage:        "remove finished download job(s)",
				ArgsUsage:    optionalJobIDArgument,
				Flags:        sortFlags(removeCmdsFlags),
				Action:       removeDownloadHandler,
				BashComplete: downloadIDFinishedCompletions,
			},
			{
				Name:         cmdDsort,
				Usage:        "remove finished dsort job(s)",
				ArgsUsage:    optionalJobIDArgument,
				Flags:        sortFlags(removeCmdsFlags),
				Action:       removeDsortHandler,
				BashComplete: dsortIDFinishedCompletions,
			},
		},
	}
)

func appendJobSub(jobcmd *cli.Command) {
	debug.Assert(jobcmd.Subcommands[0].Name == commandStart)

	// append to `jobStartSub`, add to `ais start` (== `ais job start`)
	cmds := jobcmd.Subcommands[0].Subcommands
	cmds = append(cmds, storageSvcCmds...)
	cmds = append(cmds, startableXactions(cmds)...)
	jobcmd.Subcommands[0].Subcommands = cmds
}

func startableXactions(explDefinedCmds cli.Commands) cli.Commands {
	var (
		cmds      = make(cli.Commands, 0, 8)
		startable = xact.ListDisplayNames(true /*onlyStartable*/)
	)
outer:
	for _, xname := range startable {
		for i := range explDefinedCmds {
			cmd := &explDefinedCmds[i]
			kind, _ := xact.GetKindName(xname)
			// CLI is allowed to make it even shorter..
			if strings.HasPrefix(xname, cmd.Name) || strings.HasPrefix(kind, cmd.Name) {
				// the following x-s, even through startable, have their own custom CLI handlers:
				// - cleanup-store
				// - ec-encode
				// - lru
				// - make-n-copies
				// - prefetch-listrange
				// - blob-download
				// - rebalance
				// - resilver
				continue outer
			}
		}
		// add generic start handler on the fly
		cmd := cli.Command{
			Name:   xname,
			Usage:  "start " + xname,
			Flags:  sortFlags(startCommonFlags),
			Action: startXactionHandler,
		}
		if xact.IsSameScope(xname, xact.ScopeB) { // with a single arg: bucket
			cmd.ArgsUsage = bucketArgument
			cmd.BashComplete = bucketCompletions(bcmplop{})
		} else if xact.IsSameScope(xname, xact.ScopeGB) { // with a single optional arg: bucket
			cmd.ArgsUsage = optionalBucketArgument
			cmd.BashComplete = bucketCompletions(bcmplop{})
		}
		cmds = append(cmds, cmd)
	}

	// and in addition:
	cmd := bucketObjCmdCopy
	cmd.Name, _ = xact.GetKindName(apc.ActCopyBck)
	cmds = append(cmds, cmd)

	cmd = bucketCmdRename
	cmd.Name, _ = xact.GetKindName(apc.ActMoveBck)
	cmds = append(cmds, cmd)

	return cmds
}

//
// job start
//

// via `startableXactions()`
func startXactionHandler(c *cli.Context) (err error) {
	xname := c.Command.Name
	return startXactionKind(c, xname)
}

func startXactionKind(c *cli.Context, xname string) (err error) {
	var (
		bck   cmn.Bck
		extra string
	)
	if c.NArg() == 0 && xact.IsSameScope(xname, xact.ScopeB) {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	if c.NArg() > 0 && xact.IsSameScope(xname, xact.ScopeB, xact.ScopeGB) {
		uri := c.Args().Get(0)
		if xname == apc.ActBlobDl {
			bck, extra /*objName*/, err = parseBckObjURI(c, uri, false)
		} else {
			bck, err = parseBckURI(c, uri, false)
		}
		if err != nil {
			return err
		}
	}
	xargs := xact.ArgsMsg{Kind: xname, Bck: bck}
	return startXaction(c, &xargs, extra)
}

func startResilverHandler(c *cli.Context) error {
	var tid string
	if c.NArg() > 0 {
		tsi, _, err := getNode(c, c.Args().Get(0))
		if err != nil {
			return err
		}
		tid = tsi.ID()
	}
	xargs := xact.ArgsMsg{Kind: apc.ActResilver, DaemonID: tid}
	return startXaction(c, &xargs, "")
}

func startXaction(c *cli.Context, xargs *xact.ArgsMsg, extra string) error {
	if !xargs.Bck.IsQuery() {
		if _, err := headBucket(xargs.Bck, false /* don't add */); err != nil {
			return err
		}
	} else if xact.IsSameScope(xargs.Kind, xact.ScopeB) {
		return fmt.Errorf("%q requires bucket to run", xargs.Kind)
	}

	xid, err := xstart(c, xargs, extra)
	if err != nil {
		return err
	}
	if xid == "" {
		warn := fmt.Sprintf("The operation returned an empty UUID (a no-op?). %s\n",
			toShowMsg(c, "", "To investigate", false))
		actionWarn(c, warn)
		return nil
	}

	debug.Assert(xact.IsValidUUID(xid), xid)

	if xargs.Kind == apc.ActRebalance {
		actionDone(c, "Started global rebalance. To monitor the progress, run 'ais show rebalance'")
	} else {
		actionX(c, xargs, "")
	}

	if !flagIsSet(c, waitFlag) && !flagIsSet(c, waitJobXactFinishedFlag) {
		return nil
	}
	return waitJob(c, xargs.Kind, xid, xargs.Bck)
}

// downloadRequest holds parsed and validated download arguments
type downloadRequest struct {
	source          dlSource
	destination     cmn.Bck
	pathSuffix      string
	objectsListPath string
	basePayload     dload.Base
}

// parseDownloadRequest handles argument parsing and validation
func parseDownloadRequest(c *cli.Context) (*downloadRequest, error) {
	var (
		description      = parseStrFlag(c, descJobFlag)
		timeout          = parseDurationFlag(c, dloadTimeoutFlag).String()
		objectsListPath  = parseStrFlag(c, objectsListFlag)
		progressInterval = parseDurationFlag(c, dloadProgressFlag).String()
	)
	hasHFFlags := hasHuggingFaceRepoFlags(c)

	if c.NArg() == 0 {
		return nil, missingArgumentsError(c, c.Command.ArgsUsage)
	}

	// Validate argument count based on HF flags
	if hasHFFlags {
		if c.NArg() != 1 {
			if c.NArg() > 1 {
				return nil, fmt.Errorf("when using HuggingFace flags (--hf-model, --hf-dataset), provide only the destination argument - got %d arguments", c.NArg())
			}
			return nil, missingArgumentsError(c, "destination")
		}
	} else {
		if c.NArg() == 1 {
			return nil, missingArgumentsError(c, "destination")
		}
		if c.NArg() > 2 {
			const q = "For range download, enclose source in quotation marks, e.g.: \"gs://imagenet/train-{00..99}.tgz\""
			s := fmt.Sprintf("too many arguments - expected 2, got %d.\n%s", len(c.Args()), q)
			return nil, &errUsage{
				context:      c,
				message:      s,
				helpData:     c.Command,
				helpTemplate: cli.CommandHelpTemplate,
			}
		}
	}

	// Extract source and destination
	var src, dst string
	if hasHFFlags {
		dst = c.Args().Get(0)
	} else {
		src, dst = c.Args().Get(0), c.Args().Get(1)
	}

	// Parse download source
	source, err := parseDlSource(c, src)
	if err != nil {
		return nil, err
	}

	bck, pathSuffix, err := parseBckObjAux(c, dst)
	if err != nil {
		return nil, err
	}

	limitBPH, err := parseSizeFlag(c, limitBytesPerHourFlag)
	if err != nil {
		return nil, err
	}

	basePayload := dload.Base{
		Bck:              bck,
		Timeout:          timeout,
		Description:      description,
		ProgressInterval: progressInterval,
		Headers:          source.headers,
		Limits: dload.Limits{
			Connections:  parseIntFlag(c, limitConnectionsFlag),
			BytesPerHour: int(limitBPH),
		},
	}

	// Check bucket existence
	if basePayload.Bck.Props, err = api.HeadBucket(apiBP, basePayload.Bck, true /* don't add */); err != nil {
		if !cmn.IsStatusNotFound(err) {
			return nil, err
		}
		warn := fmt.Sprintf("destination bucket %s doesn't exist. Bucket with default properties will be created.",
			basePayload.Bck.Cname(""))
		actionWarn(c, warn)
	}

	return &downloadRequest{
		source:          source,
		destination:     bck,
		pathSuffix:      pathSuffix,
		objectsListPath: objectsListPath,
		basePayload:     basePayload,
	}, nil
}

type JobDefinition interface {
	Start(apiBP api.BaseParams) ([]string, error) // Returns job IDs
}

type (
	// SingleDownloadJobDef handles single file downloads
	SingleDownloadJobDef struct {
		payload dload.SingleBody
	}

	// HFDownloadJobDef handles HuggingFace repository downloads
	HFDownloadJobDef struct {
		payload   dload.MultiBody
		hfContext *hfDownloadContext
	}

	hfDownloadContext struct {
		files   cos.StrKVs
		token   string
		context *cli.Context
	}

	// MultiDownloadJobDef handles file-list downloads
	MultiDownloadJobDef struct {
		payload dload.MultiBody
	}

	// RangeDownloadJobDef handles range downloads
	RangeDownloadJobDef struct {
		payload dload.RangeBody
	}

	// BackendDownloadJobDef handles backend downloads
	BackendDownloadJobDef struct {
		payload dload.BackendBody
	}
)

func startJob(apiBP api.BaseParams, dloadType dload.Type, payload any) ([]string, error) {
	id, err := api.DownloadWithParam(apiBP, dloadType, payload)
	if err != nil {
		return nil, err
	}
	return []string{id}, nil
}

// applyHFBatching splits files into batches based on blob threshold
func applyHFBatching(fileInfos []hf.FileInfo, blobThreshold int64, basePayload *dload.Base) []JobDefinition {
	// Separate files by size
	var smallFiles, largeFiles []hf.FileInfo

	for _, info := range fileInfos {
		if info.Size != nil && *info.Size >= blobThreshold {
			largeFiles = append(largeFiles, info)
		} else {
			smallFiles = append(smallFiles, info)
		}
	}

	jobs := make([]JobDefinition, 0, len(largeFiles)+1) // Pre-allocate for large files + potential multi-job

	if len(smallFiles) > 0 {
		smallFileURLs := make(cos.StrKVs, len(smallFiles))
		for _, info := range smallFiles {
			smallFileURLs[hf.ExtractFileName(info.URL)] = info.URL
		}

		job := &MultiDownloadJobDef{
			payload: dload.MultiBody{
				Base:           *basePayload,
				ObjectsPayload: smallFileURLs,
			},
		}
		jobs = append(jobs, job)
	}

	// Create individual jobs for large files
	for _, info := range largeFiles {
		job := &SingleDownloadJobDef{
			payload: dload.SingleBody{
				Base: *basePayload,
				SingleObj: dload.SingleObj{
					Link:    info.URL,
					ObjName: hf.ExtractFileName(info.URL),
				},
			},
		}
		jobs = append(jobs, job)
	}

	return jobs
}

func (j *SingleDownloadJobDef) Start(apiBP api.BaseParams) ([]string, error) {
	return startJob(apiBP, dload.TypeSingle, j.payload)
}
func (j *MultiDownloadJobDef) Start(apiBP api.BaseParams) ([]string, error) {
	return startJob(apiBP, dload.TypeMulti, j.payload)
}
func (j *RangeDownloadJobDef) Start(apiBP api.BaseParams) ([]string, error) {
	return startJob(apiBP, dload.TypeRange, j.payload)
}
func (j *BackendDownloadJobDef) Start(apiBP api.BaseParams) ([]string, error) {
	return startJob(apiBP, dload.TypeBackend, j.payload)
}

// Start starts a HuggingFace Download job and the associated blob download jobs if requested
func (j *HFDownloadJobDef) Start(apiBP api.BaseParams) ([]string, error) {
	blobThreshold, err := parseSizeFlag(j.hfContext.context, blobThresholdFlag)
	if err != nil {
		return nil, err
	}

	if blobThreshold == 0 {
		return startJob(apiBP, dload.TypeMulti, j.payload)
	}

	fileInfos, err := hf.GetFileSizes(j.hfContext.files, j.hfContext.token)
	if err != nil {
		return nil, fmt.Errorf("failed to get file sizes: %v", err)
	}

	// Apply batching logic
	jobs := applyHFBatching(fileInfos, blobThreshold, &j.payload.Base)

	if len(jobs) == 1 {
		return jobs[0].Start(apiBP)
	}

	// Start multiple jobs
	var allJobIDs []string
	for _, job := range jobs {
		jobIDs, err := job.Start(apiBP)
		if err != nil {
			return allJobIDs, err
		}
		allJobIDs = append(allJobIDs, jobIDs...)
	}
	return allJobIDs, nil
}

func newSingleDownloadJobDef(req *downloadRequest) *SingleDownloadJobDef {
	return &SingleDownloadJobDef{
		payload: dload.SingleBody{
			Base: req.basePayload,
			SingleObj: dload.SingleObj{
				Link:    req.source.link,
				ObjName: req.pathSuffix,
			},
		},
	}
}

func newHFDownloadJobDef(c *cli.Context, req *downloadRequest) (*HFDownloadJobDef, error) {
	isDataset := strings.Contains(req.source.link, "huggingface.co/datasets/")

	var (
		identifier string
		files      cos.StrKVs
		err        error
		repoType   string
	)

	if isDataset {
		identifier, err = hf.ExtractDatasetFromHFMarker(req.source.link)
		if err != nil {
			return nil, fmt.Errorf("invalid HuggingFace dataset marker: %v", err)
		}
		repoType = "dataset"
	} else {
		identifier, err = hf.ExtractModelFromHFMarker(req.source.link)
		if err != nil {
			return nil, fmt.Errorf("invalid HuggingFace model marker: %v", err)
		}
		repoType = "model"
	}

	token := ""
	if req.source.headers != nil {
		if auth := req.source.headers.Get(apc.HdrAuthorization); strings.HasPrefix(auth, apc.AuthenticationTypeBearer+" ") {
			token = strings.TrimPrefix(auth, apc.AuthenticationTypeBearer+" ")
		}
	}

	if isDataset {
		files, err = hf.GetHFDatasetParquetFiles(identifier, token)
	} else {
		files, err = hf.GetHFModelFiles(identifier, token)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to fetch HuggingFace %s '%s': %v", repoType, identifier, err)
	}

	if isDataset {
		actionDone(c, fmt.Sprintf("Found %d parquet files in dataset '%s'", len(files), identifier))
	} else {
		actionDone(c, fmt.Sprintf("Found %d files in model '%s'", len(files), identifier))
	}

	return &HFDownloadJobDef{
		payload: dload.MultiBody{
			Base:           req.basePayload,
			ObjectsPayload: files,
		},
		hfContext: &hfDownloadContext{
			files:   files,
			token:   token,
			context: c,
		},
	}, nil
}

func newMultiDownloadJobDef(req *downloadRequest) (*MultiDownloadJobDef, error) {
	file, err := os.Open(req.objectsListPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var objects []string
	if err := jsoniter.NewDecoder(file).Decode(&objects); err != nil {
		return nil, fmt.Errorf("file %q doesn't seem to contain JSON array of strings: %v", req.objectsListPath, err)
	}

	// For file-based downloads, prepend the base URL
	for i, object := range objects {
		objects[i] = req.source.link + "/" + object
	}

	return &MultiDownloadJobDef{
		payload: dload.MultiBody{
			Base:           req.basePayload,
			ObjectsPayload: objects,
		},
	}, nil
}

func newRangeDownloadJobDef(req *downloadRequest) *RangeDownloadJobDef {
	return &RangeDownloadJobDef{
		payload: dload.RangeBody{
			Base:     req.basePayload,
			Subdir:   req.pathSuffix,
			Template: req.source.link,
		},
	}
}

// createBackendDownloadJobDefinition handles backend download routing logic
func createBackendDownloadJobDefinition(c *cli.Context, req *downloadRequest) (JobDefinition, error) {
	backends, err := api.GetConfiguredBackends(apiBP)
	if err != nil {
		return nil, err
	}

	switch {
	case cos.StringInSlice(req.source.backend.bck.Provider, backends):
		p, err := api.HeadBucket(apiBP, req.basePayload.Bck, false /* don't add */)
		if err != nil {
			return nil, V(err)
		}
		if !p.BackendBck.Equal(&req.source.backend.bck) {
			warn := fmt.Sprintf("%s does not have Cloud bucket %s as its *backend* - proceeding to download anyway.",
				req.basePayload.Bck.String(), req.source.backend.bck.String())
			actionWarn(c, warn)
			return newSingleDownloadJobDef(req), nil
		}
		return newBackendDownloadJobDef(c, req), nil
	case req.source.backend.prefix == "":
		return nil, fmt.Errorf("cluster is not configured with %q provider: cannot download remote bucket",
			req.source.backend.bck.Provider)
	default:
		if req.source.link == "" {
			return nil, fmt.Errorf("cluster is not configured with %q provider: cannot download bucket's objects",
				req.source.backend.bck.Provider)
		}
		// If `prefix` is not empty then possibly it is just a single object
		// which we can download without cloud to be configured (web link).
		return newSingleDownloadJobDef(req), nil
	}
}

// createDownloadJobDefinition creates the appropriate JobDefinition based on request
func createDownloadJobDefinition(c *cli.Context, req *downloadRequest) (JobDefinition, error) {
	switch {
	case strings.HasPrefix(req.source.link, hf.HfFullRepoMarker):
		return newHFDownloadJobDef(c, req)
	case req.objectsListPath != "":
		return newMultiDownloadJobDef(req)
	case strings.Contains(req.source.link, "{") && strings.Contains(req.source.link, "}"):
		return newRangeDownloadJobDef(req), nil
	case req.source.backend.bck.IsEmpty():
		return newSingleDownloadJobDef(req), nil
	default:
		return createBackendDownloadJobDefinition(c, req)
	}
}

// newBackendDownloadJobDef creates a BackendDownloadJobDef
func newBackendDownloadJobDef(c *cli.Context, req *downloadRequest) *BackendDownloadJobDef {
	return &BackendDownloadJobDef{
		payload: dload.BackendBody{
			Base:   req.basePayload,
			Sync:   flagIsSet(c, syncFlag),
			Prefix: req.source.backend.prefix,
		},
	}
}

func startDownloadHandler(c *cli.Context) error {
	req, err := parseDownloadRequest(c)
	if err != nil {
		return err
	}

	jobDef, err := createDownloadJobDefinition(c, req)
	if err != nil {
		return err
	}

	allJobIDs, err := jobDef.Start(apiBP)
	if err != nil {
		return err
	}

	// Display message for any multi-job scenario with blob threshold
	if len(allJobIDs) > 1 {
		blobThreshold, _ := parseSizeFlag(c, blobThresholdFlag)
		if blobThreshold > 0 {
			individualJobs := len(allJobIDs) - 1
			if individualJobs > 0 {
				fmt.Fprintf(c.App.Writer, "Created %d individual jobs for files >= %s\n", individualJobs, cos.ToSizeIEC(blobThreshold, 0))
			}
		}
	}
	fmt.Fprintf(c.App.Writer, "Started download job %s\n", allJobIDs[0])

	if flagIsSet(c, progressFlag) {
		return pbDownload(c, allJobIDs)
	}

	if flagIsSet(c, waitFlag) || flagIsSet(c, waitJobXactFinishedFlag) {
		return wtDownload(c, allJobIDs[0])
	}

	return bgDownload(c, allJobIDs[0])
}

// downloadRefreshRate returns the refresh interval for download operations.
// Checks --refresh flag first, then falls back to --progress-interval if not set.
// This allows --progress-interval to work for download monitoring operations.
func downloadRefreshRate(c *cli.Context) time.Duration {
	// First try the common refresh flag
	if flagIsSet(c, refreshFlag) {
		return max(parseDurationFlag(c, refreshFlag), refreshRateMinDur)
	}
	// Fall back to download-specific progress-interval flag
	if flagIsSet(c, dloadProgressFlag) {
		return max(parseDurationFlag(c, dloadProgressFlag), refreshRateMinDur)
	}
	return refreshRateDefault
}

// pbDownload monitors download job(s) with progress bar
func pbDownload(c *cli.Context, jobIDs []string) error {
	if len(jobIDs) == 0 {
		return errors.New("pbDownload requires at least one job ID")
	}

	if len(jobIDs) == 1 {
		// Check if this is truly a single-file download or multi-file job
		resp, err := api.DownloadStatus(apiBP, jobIDs[0], false /*onlyActive*/)
		if err == nil && resp.Total <= 1 {
			return singleJobProgress(c, jobIDs[0])
		}
	}

	// Multiple jobs
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	refreshRate := downloadRefreshRate(c)
	completed := make(map[string]struct{}) // Set of completed job IDs
	lastUpdate := time.Now()
	updateInterval := 30 * time.Second

	fmt.Fprintf(c.App.Writer, "Monitoring %d download jobs...\n", len(jobIDs))

	for {
		select {
		case <-ctx.Done():
			handleJobMonitoringTimeout(c, jobIDs, completed)
			return nil
		default:
		}

		for _, jobID := range jobIDs {
			if _, isCompleted := completed[jobID]; isCompleted {
				continue // Already completed
			}

			checkJobCompletion(c, jobID, completed)
		}

		if len(completed) == len(jobIDs) {
			fmt.Fprintln(c.App.Writer, "All jobs completed!")
			return nil
		}

		if time.Since(lastUpdate) > updateInterval {
			elapsed := time.Since(lastUpdate).Truncate(time.Second)
			fmt.Fprintf(c.App.Writer, "Progress: %d/%d jobs completed (elapsed: %v)\n",
				len(completed), len(jobIDs), elapsed)
			lastUpdate = time.Now()
		}

		time.Sleep(refreshRate)
	}
}

// singleJobProgress handles single job progress monitoring
func singleJobProgress(c *cli.Context, id string) error {
	refreshRate := downloadRefreshRate(c)
	// Note: timeout=0 for progress monitoring (server-side download-timeout still applies)
	downloadingResult, err := newDownloaderPB(apiBP, id, refreshRate, 0).run()
	if err != nil {
		return err
	}

	fmt.Fprintln(c.App.Writer, downloadingResult)
	return nil
}

// checkJobCompletion checks if a job is completed and updates the completed set
func checkJobCompletion(c *cli.Context, jobID string, completed map[string]struct{}) {
	resp, err := api.DownloadStatus(apiBP, jobID, false /*onlyActive*/)
	if err != nil {
		actionWarn(c, fmt.Sprintf("Failed to check status for job %s: %v", jobID, err))
		return
	}

	if resp.JobFinished() {
		completed[jobID] = struct{}{}
		fmt.Fprintf(c.App.Writer, "Job %s completed\n", jobID)
	}
}

func wtDownload(c *cli.Context, id string) error {
	if err := waitDownloadHandler(c, id); err != nil {
		return err
	}
	resp, err := api.DownloadStatus(apiBP, id, true /*only active*/)
	if err != nil {
		return V(err)
	}
	if resp.ErrorCnt != 0 {
		msg := toShowMsg(c, id, "For details", true)
		warn := fmt.Sprintf("%d of %d download jobs failed. %s", resp.ErrorCnt, resp.ScheduledCnt, msg)
		actionWarn(c, warn)
	} else {
		actionDownloaded(c, resp.FinishedCnt)
	}
	return nil
}

func actionDownloaded(c *cli.Context, cnt int) {
	var msg string
	if cnt == 1 {
		msg = "File successfully downloaded"
	} else {
		msg = fmt.Sprintf("All %d files successfully downloaded", cnt)
	}
	actionDone(c, msg)
}

func bgDownload(c *cli.Context, id string) (err error) {
	var (
		resp           *dload.StatusResp
		startedTimeout = max(dloadStartedTime, refreshRateMinDur*2)
	)

	// In a non-interactive mode, allow downloader to start before checking
	for elapsed := time.Duration(0); elapsed < startedTimeout; elapsed += refreshRateMinDur {
		time.Sleep(refreshRateMinDur)
		resp, err = api.DownloadStatus(apiBP, id, true /*only active*/)
		if err != nil {
			return err
		}
		if resp.ErrorCnt != 0 || resp.FinishedCnt != 0 || resp.FinishedTime.UnixNano() != 0 {
			break
		}
	}

	switch {
	case resp.ErrorCnt != 0:
		msg := toShowMsg(c, id, "For details", true)
		warn := fmt.Sprintf("%d of %d download jobs failed. %s", resp.ErrorCnt, resp.ScheduledCnt, msg)
		actionWarn(c, warn)
	case resp.FinishedTime.UnixNano() != 0:
		actionDownloaded(c, resp.FinishedCnt)
	default:
		msg := toMonitorMsg(c, id, flprn(progressFlag)+"'")
		actionDone(c, msg)
	}

	return err
}

func startLRUHandler(c *cli.Context) error {
	if !flagIsSet(c, lruBucketsFlag) {
		return startXactionHandler(c)
	}

	if flagIsSet(c, forceFlag) {
		warn := fmt.Sprintf("LRU eviction with %s option will evict buckets _ignoring_ their respective `lru.enabled` properties.",
			qflprn(forceFlag))
		if !confirm(c, "Would you like to continue?", warn) {
			return nil
		}
	}

	s := parseStrFlag(c, lruBucketsFlag)
	bckArgs := splitCsv(s)
	buckets := make([]cmn.Bck, len(bckArgs))
	for idx, bckArg := range bckArgs {
		bck, err := parseBckURI(c, bckArg, false)
		if err != nil {
			return err
		}
		buckets[idx] = bck
	}

	xargs := xact.ArgsMsg{Kind: apc.ActLRU, Buckets: buckets, Force: flagIsSet(c, forceFlag)}
	xid, err := xstart(c, &xargs, "")
	if err != nil {
		return err
	}

	actionX(c, &xact.ArgsMsg{Kind: apc.ActLRU, ID: xid}, "")
	return nil
}

//
// job stop
//

func stopJobHandler(c *cli.Context) error {
	var shift int
	if c.Args().Get(0) == commandJob {
		shift = 1
	}
	name, xid, daemonID, bck, err := jobArgs(c, shift, true /*ignore daemonID*/)
	if err != nil {
		return err
	}
	if name == "" && xid == "" && !flagIsSet(c, allRunningJobsFlag) {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	if daemonID != "" {
		warn := fmt.Sprintf("node ID %q will be ignored (stopping job on a given node not supported)\n",
			daemonID)
		actionWarn(c, warn)
	}

	// warn
	var (
		warn  string
		regex = parseStrFlag(c, regexJobsFlag)
	)
	switch {
	case flagIsSet(c, allRunningJobsFlag) && regex != "":
		warn = fmt.Sprintf("flags %s and %s", qflprn(allRunningJobsFlag), qflprn(regexJobsFlag))
	case flagIsSet(c, allRunningJobsFlag):
		warn = "flag " + qflprn(allRunningJobsFlag)
	case regex != "":
		warn = "option " + qflprn(regexJobsFlag)
	}
	if warn != "" {
		switch {
		case xid != "":
			actionWarn(c, fmt.Sprintf("ignoring %s in presence of %s argument ('%s')", warn, jobIDArgument, xid))
		case name == commandRebalance:
			actionWarn(c, "global rebalance is _global_ - ignoring"+warn)
		case regex != "" && name != cmdDownload && name != cmdDsort:
			actionWarn(c, "ignoring "+warn+" -"+NIY)
		}
	}

	var (
		otherID         string
		xactID          = xid
		xname, xactKind string
		multimatch      bool
	)
	if name == "" && xid != "" {
		name, otherID, multimatch = xid2Name(xid)
	}

	if multimatch {
		var (
			prefix = xid
			cnt    int
		)
		names := xact.ListDisplayNames(false /*only-startable*/)
		for _, name = range names {
			if !strings.HasPrefix(name, prefix) { // filter
				continue
			}
			errV := stopXactionKindOrAll(c, name, name, bck)
			if errV != nil {
				actionWarn(c, errV.Error())
				err = errV
				cnt++
				if cnt > 1 {
					break
				}
			}
		}
		return err
	}

	if name != "" {
		xactKind, xname = xact.GetKindName(name)
		if xactKind == "" {
			return incorrectUsageMsg(c, "unrecognized or misplaced option '%s'", name)
		}
	}

	// confirm unless
	if xactID == "" && name != "" {
		if name != commandRebalance && !flagIsSet(c, yesFlag) && !flagIsSet(c, allRunningJobsFlag) {
			prompt := fmt.Sprintf("Stop all '%s' jobs?", name)
			if !confirm(c, prompt) {
				return nil
			}
		}
	}

	// specialized stop
	switch name {
	case cmdDownload:
		if xid == "" {
			return stopDownloadRegex(c, regex)
		}
		return stopDownloadHandler(c, xid)
	case cmdDsort:
		if xid == "" {
			return stopDsortRegex(c, regex)
		}
		return stopDsortHandler(c, xid)
	case commandETL:
		return stopETLs(c, otherID /*etl name*/)
	case commandRebalance:
		if xid == "" {
			return stopRebHandler(c)
		}
		return stopReb(c, xid)
	}

	// generic xstop

	if xactID == "" {
		return stopXactionKindOrAll(c, xactKind, xname, bck)
	}

	// query
	msg := formatXactMsg(xactID, xname, bck)
	xargs := xact.ArgsMsg{ID: xactID, Kind: xactKind}
	xs, cms, err := queryXactions(&xargs, true /*summarize*/)
	if err != nil {
		return fmt.Errorf("cannot stop %s: %v", msg, err)
	}
	if len(xs) == 0 {
		actionWarn(c, msg+" not found, nothing to do")
		return nil
	}

	// reformat
	msg = formatXactMsg(xid, xname, bck)

	// nothing to do?
	if cms.aborted {
		fmt.Fprintf(c.App.Writer, "%s is aborted, nothing to do\n", msg)
		return nil
	}
	if !cms.running {
		fmt.Fprintf(c.App.Writer, "%s already finished, nothing to do\n", msg)
		return nil
	}

	// call to abort
	args := xact.ArgsMsg{ID: xactID, Kind: xactKind, Bck: bck}
	if err := xstop(&args); err != nil {
		return err
	}
	actionDone(c, fmt.Sprintf("Stopped %s\n", msg))
	return nil
}

// NOTE: the '--all' case when both (xactKind == "" && xname == "") - is also handled here
// TODO: aistore supports `bck` for additional filtering (NIY)
func stopXactionKindOrAll(c *cli.Context, xactKind, xname string, bck cmn.Bck) error {
	cnames, err := api.GetAllRunningXactions(apiBP, xactKind)
	if err != nil {
		return V(err)
	}
	if len(cnames) == 0 {
		var what string
		if xname != "" {
			what = " '" + xname + "'"
		}
		actionDone(c, fmt.Sprintf("No running%s jobs, nothing to do", what))
		return nil
	}
	for _, cname := range cnames {
		xactKind, xactID, err := xact.ParseCname(cname)
		if err != nil {
			debug.AssertNoErr(err)
			continue
		}
		args := xact.ArgsMsg{ID: xactID, Kind: xactKind, Bck: bck}
		if err := xstop(&args); err != nil {
			actionWarn(c, fmt.Sprintf("failed to stop %s: %v", cname, err))
		} else {
			actionDone(c, "Stopped "+cname)
		}
	}
	return nil
}

func formatXactMsg(xactID, xactKind string, bck cmn.Bck) string {
	var sb string
	if !bck.IsQuery() {
		sb = ", " + bck.Cname("")
	}
	switch {
	case xactKind != "" && xactID != "":
		return fmt.Sprintf("%s%s", xact.Cname(xactKind, xactID), sb)
	case xactKind != "" && sb != "":
		return fmt.Sprintf("%s%s", xactKind, sb)
	case xactKind != "":
		return xactKind
	default:
		return fmt.Sprintf("[%s]%s", xactID, sb)
	}
}

//nolint:dupl // stop downloads and dsorts: different API methods justify seemingly duplicated code
func stopDownloadRegex(c *cli.Context, regex string) error {
	dlList, err := api.DownloadGetList(apiBP, regex, true /*onlyActive*/)
	if err != nil {
		return V(err)
	}

	var cnt int
	for _, dl := range dlList {
		if err = api.AbortDownload(apiBP, dl.ID); err == nil {
			actionDone(c, "Stopped download job "+dl.ID)
			cnt++
		} else {
			actionWarn(c, fmt.Sprintf("failed to stop download job %q: %v", dl.ID, err))
		}
	}
	if err != nil {
		return err
	}
	if cnt == 0 {
		actionDone(c, "No running download jobs, nothing to do")
	}
	return nil
}

func stopDownloadHandler(c *cli.Context, id string) (err error) {
	if err = api.AbortDownload(apiBP, id); err != nil {
		return
	}
	actionDone(c, fmt.Sprintf("Stopped download job %s\n", id))
	return
}

//nolint:dupl // stop downloads and dsorts: different API methods justify seemingly duplicated code
func stopDsortRegex(c *cli.Context, regex string) error {
	dsortLst, err := api.ListDsort(apiBP, regex, true /*onlyActive*/)
	if err != nil {
		return V(err)
	}

	var cnt int
	for _, dsort := range dsortLst {
		if err = api.AbortDsort(apiBP, dsort.ID); err == nil {
			actionDone(c, "Stopped dsort job "+dsort.ID)
			cnt++
		} else {
			actionWarn(c, fmt.Sprintf("failed to stop dsort job %q: %v", dsort.ID, err))
		}
	}
	if err != nil {
		return err
	}
	if cnt == 0 {
		actionDone(c, "No running dsort jobs, nothing to do")
	}
	return nil
}

func stopDsortHandler(c *cli.Context, id string) (err error) {
	if err = api.AbortDsort(apiBP, id); err != nil {
		return
	}
	actionDone(c, fmt.Sprintf("Stopped dsort job %s\n", id))
	return
}

//
// `job wait`
//

func waitJobHandler(c *cli.Context) error {
	var shift int
	if c.Args().Get(0) == commandJob {
		shift = 1
	}

	name, xid, daemonID, bck, err := jobArgs(c, shift, true /*ignore daemonID*/)
	if err != nil {
		return err
	}
	if name == "" && xid == "" {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	if daemonID != "" {
		actionWarn(c, fmt.Sprintf("node ID %q will be ignored (waiting for a single target not supported)\n", daemonID))
	}

	if name == "" && xid != "" {
		var multimatch bool
		name, _, multimatch = xid2Name(xid) // TODO: add waitETL
		if multimatch {
			return fmt.Errorf("cannot wait on %q prefix", xid)
		}
	}

	return waitJob(c, name, xid, bck)
}

func waitJob(c *cli.Context, name, xid string, bck cmn.Bck) error {
	// special wait
	switch name {
	case cmdDownload:
		if xid == "" {
			return missingArgumentsError(c, jobIDArgument)
		}
		return waitDownloadHandler(c, xid /*job ID*/)
	case cmdDsort:
		if xid == "" {
			return missingArgumentsError(c, jobIDArgument)
		}
		return waitDsortHandler(c, xid /*job ID*/)
	}
	// TODO: niy
	if flagIsSet(c, refreshFlag) {
		warn := fmt.Sprintf("ignoring flag %s  - not fully implemented yet", qflprn(refreshFlag))
		actionWarn(c, warn)
	} else if flagIsSet(c, progressFlag) {
		warn := fmt.Sprintf("ignoring flag %s  - not fully implemented yet", qflprn(progressFlag))
		actionWarn(c, warn)
	}
	// x-wait
	var (
		xactID, xname = xid, name
		xactKind      string
	)
	if name != "" {
		xactKind, xname = xact.GetKindName(name)
		if xactKind == "" {
			return incorrectUsageMsg(c, "unrecognized or misplaced option '%s'", name)
		}
	}
	xargs := xact.ArgsMsg{ID: xactID, Kind: xactKind}
	if flagIsSet(c, waitJobXactFinishedFlag) {
		xargs.Timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}

	// find out the kind if not provided
	if xargs.Kind == "" {
		_, snap, err := getAnyXactSnap(&xargs)
		if err != nil {
			return err
		}
		xargs.Kind = snap.Kind
		_, xname = xact.GetKindName(xargs.Kind)
	}

	msg := formatXactMsg(xactID, xname, bck)
	fmt.Fprintln(c.App.Writer, "Waiting for "+msg+" ...")
	err := waitXact(&xargs)
	if err == nil {
		actionDone(c, "Done.")
	}
	return nil
}

func waitDownloadHandler(c *cli.Context, id string) error {
	refreshRate := downloadRefreshRate(c)
	timeout := parseDurationFlag(c, dloadTimeoutFlag)
	if flagIsSet(c, progressFlag) {
		downloadingResult, err := newDownloaderPB(apiBP, id, refreshRate, timeout).run()
		if err != nil {
			return err
		}
		actionDone(c, downloadingResult.String())
		return nil
	}

	// poll at a refresh rate
	var (
		total time.Duration
		qn    = xact.Cname(cmdDownload, id)
	)
	if flagIsSet(c, waitJobXactFinishedFlag) {
		timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}
	for {
		resp, err := api.DownloadStatus(apiBP, id, true /*onlyActive*/)
		if err != nil {
			return V(err)
		}
		if resp.Aborted {
			if total > wasFast {
				fmt.Fprintln(c.App.Writer)
			}
			return fmt.Errorf("%s was aborted", qn)
		}
		if resp.JobFinished() {
			break
		}
		time.Sleep(refreshRate)
		total += refreshRate
		if timeout != 0 && total > timeout {
			return fmt.Errorf("timed out waiting for %s", qn)
		}
	}
	actionDone(c, "\n"+qn+" finished")
	return nil
}

func waitDsortHandler(c *cli.Context, id string) error {
	refreshRate := _refreshRate(c)
	if flagIsSet(c, progressFlag) {
		dsortResult, err := newDsortPB(apiBP, id, refreshRate).run()
		if err != nil {
			return err
		}
		actionDone(c, dsortResult.String())
		return nil
	}

	// poll at refresh rate
	var (
		total   time.Duration
		timeout time.Duration
		qn      = xact.Cname(cmdDsort, id)
	)
	if flagIsSet(c, waitJobXactFinishedFlag) {
		timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}
	for {
		resp, err := api.MetricsDsort(apiBP, id)
		if err != nil {
			return V(err)
		}
		finished := true
		for _, targetMetrics := range resp {
			if targetMetrics.Metrics.Aborted.Load() {
				if total > wasFast {
					fmt.Fprintln(c.App.Writer)
				}
				return fmt.Errorf("%s was aborted", qn)
			}
			finished = finished && targetMetrics.Metrics.Creation.Finished
		}
		if finished {
			break
		}
		time.Sleep(refreshRate)
		total += refreshRate
		if timeout != 0 && total > timeout {
			return fmt.Errorf("timed out waiting for %s", qn)
		}
	}
	if total > wasFast {
		fmt.Fprintln(c.App.Writer)
	}
	actionDone(c, qn+" finished")
	return nil
}

//
// job remove
//

//nolint:dupl // remove download and dsort jobs: different API methods justify seemingly duplicated code
func removeDownloadHandler(c *cli.Context) error {
	regex := parseStrFlag(c, regexJobsFlag)
	if flagIsSet(c, allFinishedJobsFlag) || regex != "" {
		return removeDownloadRegex(c, regex)
	}

	// by job ID
	if c.NArg() < 1 {
		msg := fmt.Sprintf("Expecting either %s argument or %s option (with or without regular expression)",
			jobIDArgument, qflprn(allFinishedJobsFlag))
		return cannotExecuteError(c, errors.New("missing "+jobIDArgument), msg)
	}
	id := c.Args().Get(0)
	if err := api.RemoveDownload(apiBP, id); err != nil {
		return V(err)
	}
	actionDone(c, fmt.Sprintf("Removed finished download job %q", id))
	return nil
}

func removeDownloadRegex(c *cli.Context, regex string) error {
	dlList, err := api.DownloadGetList(apiBP, regex, false /*onlyActive*/)
	if err != nil {
		return V(err)
	}

	var cnt int
	for _, dl := range dlList {
		if !dl.JobFinished() {
			continue
		}
		err = api.RemoveDownload(apiBP, dl.ID)
		if err == nil {
			actionDone(c, fmt.Sprintf("Removed download job %q", dl.ID))
			cnt++
		} else {
			actionWarn(c, fmt.Sprintf("failed to remove download job %q: %v", dl.ID, err))
		}
	}
	if err != nil {
		return err
	}
	if cnt == 0 {
		actionDone(c, "No finished download jobs, nothing to do")
	}
	return nil
}

//nolint:dupl // remove download and dsort jobs: different API methods justify seemingly duplicated code
func removeDsortHandler(c *cli.Context) error {
	regex := parseStrFlag(c, regexJobsFlag)
	if flagIsSet(c, allFinishedJobsFlag) || regex != "" {
		return removeDsortRegex(c, regex)
	}

	// by job ID
	if c.NArg() < 1 {
		msg := fmt.Sprintf("Expecting either %s argument or %s option (with or without regular expression)",
			jobIDArgument, qflprn(allFinishedJobsFlag))
		return cannotExecuteError(c, errors.New("missing "+jobIDArgument), msg)
	}
	id := c.Args().Get(0)
	if err := api.RemoveDsort(apiBP, id); err != nil {
		return V(err)
	}

	actionDone(c, fmt.Sprintf("Removed finished dsort job %q", id))
	return nil
}

func removeDsortRegex(c *cli.Context, regex string) error {
	dsortLst, err := api.ListDsort(apiBP, regex, false /*onlyActive*/)
	if err != nil {
		return V(err)
	}

	var cnt int
	for _, dsort := range dsortLst {
		if dsort.IsRunning() {
			continue
		}
		err = api.RemoveDsort(apiBP, dsort.ID)
		if err == nil {
			actionDone(c, fmt.Sprintf("Removed dsort job %q", dsort.ID))
			cnt++
		} else {
			actionWarn(c, fmt.Sprintf("failed to remove dsort job %q: %v", dsort.ID, err))
		}
	}
	if err != nil {
		return err
	}
	if cnt == 0 {
		actionDone(c, "No finished dsort jobs, nothing to do")
	}
	return nil
}

//
// utility functions
//

// facilitate flexibility and common-sense argument omissions, do away with rigid name -> id -> ... ordering
func jobArgs(c *cli.Context, shift int, ignoreDaemonID bool) (name, xid, daemonID string, bck cmn.Bck, _ error) {
	// prelim. assignments
	name = c.Args().Get(shift)
	xid = c.Args().Get(shift + 1)
	daemonID = c.Args().Get(shift + 2)

	// validate and reassign
	if name == commandCopy {
		err := fmt.Errorf("'%s' is ambiguous and may correspond to copying multiple objects (%s) or entire bucket (%s)",
			commandCopy, apc.ActCopyObjects, apc.ActCopyBck)
		return "", "", "", cmn.Bck{}, err
	}

	if name == commandList {
		name = apc.ActList
	} else if name != "" && name != apc.ActDsort {
		if xactKind, xactName := xact.GetSimilar(name); xactKind == "" {
			daemonID = xid
			xid = name
			name = ""
		} else {
			name = cos.Left(xactName, xactKind)
		}
	}
	if xid != "" {
		var err error
		if bck, err = parseBckURI(c, xid, false); err == nil {
			xid = "" // arg #1 is a bucket
		}
	}
	if daemonID != "" && bck.IsEmpty() {
		var err error
		if bck, err = parseBckURI(c, daemonID, false); err == nil {
			daemonID = "" // ditto arg #2
		}
	}

	if xid != "" && daemonID == "" {
		if node, _, err := getNode(c, xid); err == nil {
			daemonID, xid = node.ID(), ""
			return name, xid, daemonID, bck, nil
		}
	}

	if ignoreDaemonID {
		return name, xid, daemonID, bck, nil
	}

	// sname => sid
	if daemonID != "" {
		node, _, err := getNode(c, daemonID)
		if err == nil {
			daemonID = node.ID()
		}
	}

	return name, xid, daemonID, bck, nil
}

// 1. support assorted system job prefixes via multi-match
// 2. disambiguate download/dsort/etl job ID vs xaction UUID
func xid2Name(xid string) (name, otherID string, multimatch bool) {
	switch xid {
	case "evict", "prefetch", "delete":
		return "", "", true
	case "copy", "cp":
		return "", "copy-", true
	case "ec", "encode":
		return "", "ec-", true
	case "etl", "transform":
		return "", "etl-", true
	}

	switch {
	case strings.HasPrefix(xid, dload.PrefixJobID):
		if _, err := api.DownloadStatus(apiBP, xid, false /*onlyActive*/); err == nil {
			name = cmdDownload
		}
	case strings.HasPrefix(xid, dsort.PrefixJobID):
		if _, err := api.MetricsDsort(apiBP, xid); err == nil {
			name = cmdDsort
		}
	case strings.HasPrefix(xid, etl.PrefixXactID):
		if l := findETL("", xid); l != nil {
			name = commandETL
			otherID = l.Name
		}
	}
	return name, otherID, false
}

// handleJobMonitoringTimeout handles the timeout case for multi-job monitoring
func handleJobMonitoringTimeout(c *cli.Context, jobIDs []string, completed map[string]struct{}) {
	fmt.Fprintf(c.App.Writer, "Timeout reached. %d/%d jobs completed.\n", len(completed), len(jobIDs))

	var completedJobs, inProgressJobs []string
	for _, jobID := range jobIDs {
		if _, isCompleted := completed[jobID]; isCompleted {
			completedJobs = append(completedJobs, jobID)
		} else {
			inProgressJobs = append(inProgressJobs, jobID)
		}
	}

	if len(completedJobs) > 0 {
		fmt.Fprintf(c.App.Writer, "Completed jobs: %s\n", strings.Join(completedJobs, ", "))
	}
	if len(inProgressJobs) > 0 {
		fmt.Fprintf(c.App.Writer, "Jobs still in progress: %s\n", strings.Join(inProgressJobs, ", "))
		fmt.Fprintln(c.App.Writer, "Stopped monitoring. Jobs may still be running in the background.")
	}
}
