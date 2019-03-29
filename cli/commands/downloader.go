// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that interact with objects in the cluster
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cli/templates"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
)

const (
	downloadStatus = "status"
	downloadCancel = "cancel"
	downloadRemove = "rm"
	downloadList   = "ls"

	progressBarRefreshRateUnit    = time.Millisecond
	progressBarRefreshRateDefault = 1000
	progressBarRefreshRateMin     = 500
	totalBarText                  = "Files downloaded:"
	unknownTotalIncrement         = 2048
	progressBarWidth              = 64
)

var (
	baseDownloadFlags = []cli.Flag{
		bckProviderFlag,
		timeoutFlag,
		descriptionFlag,
	}

	downloadFlags = map[string][]cli.Flag{
		downloadStatus: {
			idFlag,
			progressBarFlag,
			refreshRateFlag,
			verboseFlag,
		},
		downloadCancel: {
			idFlag,
		},
		downloadRemove: {
			idFlag,
		},
		downloadList: {
			regexFlag,
		},
	}

	downloadUsage       = fmt.Sprintf("%s download <source> <dest>", cliName)
	downloadStatusUsage = fmt.Sprintf("%s download %s --id <value> [STATUS FLAGS...]", cliName, downloadStatus)
	downloadCancelUsage = fmt.Sprintf("%s download %s --id <value>", cliName, downloadCancel)
	downloadRemoveUsage = fmt.Sprintf("%s download %s --id <value>", cliName, downloadRemove)
	downloadListUsage   = fmt.Sprintf("%s download %s --id <value> --regex <value>", cliName, downloadList)

	DownloaderCmds = []cli.Command{
		{
			Name:         "download",
			Usage:        "download objects from external source",
			Flags:        baseDownloadFlags,
			Action:       downloadHandler,
			UsageText:    downloadUsage,
			BashComplete: flagList,
			Subcommands: []cli.Command{
				{
					Name:         downloadStatus,
					Usage:        "fetch status of download job with given id",
					UsageText:    downloadStatusUsage,
					Flags:        downloadFlags[downloadStatus],
					Action:       downloadAdminHandler,
					BashComplete: flagList,
				},
				{
					Name:         downloadCancel,
					Usage:        "cancel download job with given id",
					UsageText:    downloadCancelUsage,
					Flags:        downloadFlags[downloadCancel],
					Action:       downloadAdminHandler,
					BashComplete: flagList,
				},
				{
					Name:         downloadRemove,
					Usage:        "remove download job with given id from the list",
					UsageText:    downloadRemoveUsage,
					Flags:        downloadFlags[downloadRemove],
					Action:       downloadAdminHandler,
					BashComplete: flagList,
				},
				{
					Name:         downloadList,
					Usage:        "list all download jobs which match provided regex",
					UsageText:    downloadListUsage,
					Flags:        downloadFlags[downloadList],
					Action:       downloadAdminHandler,
					BashComplete: flagList,
				},
			},
		},
	}
)

func downloadHandler(c *cli.Context) error {
	var (
		baseParams  = cliAPIParams(ClusterURL)
		description = parseFlag(c, descriptionFlag.Name)
		timeout     = parseFlag(c, timeoutFlag.Name)

		id string
	)

	bckProvider, err := cluster.TranslateBckProvider(parseFlag(c, bckProviderFlag.Name))
	if err != nil {
		return err
	}

	basePayload := cmn.DlBase{
		BckProvider: bckProvider,
		Timeout:     timeout,
		Description: description,
	}

	if c.NArg() != 2 {
		return fmt.Errorf("expected two arguments: source and destination, got %d", c.NArg())
	}
	source, dest := c.Args().Get(0), c.Args().Get(1)
	link, err := parseSource(source)
	if err != nil {
		return err
	}
	bucket, objName, err := parseDest(dest)
	if err != nil {
		return err
	}
	basePayload.Bucket = bucket

	if strings.Contains(source, "{") && strings.Contains(source, "}") {
		// Range
		payload := cmn.DlRangeBody{
			DlBase:   basePayload,
			Template: link,
		}
		id, err = api.DownloadRangeWithParam(baseParams, payload)
		if err != nil {
			return err
		}
	} else {
		// Single
		payload := cmn.DlSingleBody{
			DlBase: basePayload,
			DlObj: cmn.DlObj{
				Link:    link,
				Objname: objName,
			},
		}
		id, err = api.DownloadSingleWithParam(baseParams, payload)
		if err != nil {
			return err
		}
	}
	fmt.Println(id)
	return nil
}

func downloadAdminHandler(c *cli.Context) error {
	var (
		baseParams = cliAPIParams(ClusterURL)
		id         = parseFlag(c, idFlag.Name)
		regex      = parseFlag(c, regexFlag.Name)
	)

	commandName := c.Command.Name
	switch commandName {
	case downloadStatus:
		if err := checkFlags(c, idFlag.Name); err != nil {
			return err
		}

		showProgressBar := c.Bool(progressBarFlag.Name)

		if showProgressBar {
			downloadingResult, err := newProgressBar(baseParams, id, progressBarRefreshRate(c)).run()
			if err != nil {
				return err
			}

			fmt.Println(downloadingResult)
		} else {
			resp, err := api.DownloadStatus(baseParams, id)
			if err != nil {
				return err
			}

			verbose := flagIsSet(c, verboseFlag.Name)
			fmt.Println(resp.Print(verbose))
		}
	case downloadCancel:
		if err := checkFlags(c, idFlag.Name); err != nil {
			return err
		}

		if err := api.DownloadCancel(baseParams, id); err != nil {
			return err
		}
		fmt.Printf("download canceled: %s\n", id)
	case downloadRemove:
		if err := checkFlags(c, idFlag.Name); err != nil {
			return err
		}

		if err := api.DownloadRemove(baseParams, id); err != nil {
			return err
		}
		fmt.Printf("download removed: %s\n", id)
	case downloadList:
		list, err := api.DownloadGetList(baseParams, regex)
		if err != nil {
			return err
		}

		err = templates.DisplayOutput(list, templates.DownloadListTmpl)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid command name '%s'", commandName)
	}
	return nil
}

func progressBarRefreshRate(c *cli.Context) time.Duration {
	refreshRate := progressBarRefreshRateDefault

	if flagIsSet(c, refreshRateFlag.Name) {
		if flagVal := c.Int(refreshRateFlag.Name); flagVal > 0 {
			refreshRate = cmn.Max(flagVal, progressBarRefreshRateMin)
		}
	}

	return time.Duration(refreshRate) * progressBarRefreshRateUnit
}

type downloadingResult struct {
	totalFiles        int
	finishedFiles     int
	downloadingErrors map[string]string
	cancelled         bool
}

func (d downloadingResult) String() string {
	if d.cancelled {
		return "Download was cancelled."
	}

	if d.finishedFiles == d.totalFiles {
		return "All files successfully downloaded."
	}

	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("Downloaded %d out of %d files.\n", d.finishedFiles, d.totalFiles))
	sb.WriteString("Following files caused downloading errors:")
	for file, err := range d.downloadingErrors {
		sb.WriteString(fmt.Sprintf("\n%s: %s", file, err))
	}

	return sb.String()
}

type fileDownloadingState struct {
	total      int64
	downloaded int64
	bar        *mpb.Bar
}

type progressBar struct {
	id          string
	params      *api.BaseParams
	refreshTime time.Duration

	states map[string]*fileDownloadingState

	p        *mpb.Progress
	totalBar *mpb.Bar

	totalFiles    int
	finishedFiles int

	cancelled bool

	errors map[string]string
}

func newProgressBar(baseParams *api.BaseParams, id string, refreshTime time.Duration) *progressBar {
	return &progressBar{
		id:          id,
		params:      baseParams,
		refreshTime: refreshTime,
		states:      make(map[string]*fileDownloadingState),
		p:           mpb.New(mpb.WithWidth(progressBarWidth)),
		errors:      make(map[string]string),
	}
}

func (b *progressBar) run() (downloadingResult, error) {
	finished, err := b.start()
	if err != nil {
		return downloadingResult{}, err
	}
	if finished {
		return b.result(), nil
	}

	// All files = finished ones + ones that had downloading errors
	for b.finishedFiles+len(b.errors) < b.totalFiles {
		time.Sleep(b.refreshTime)

		resp, err := api.DownloadStatus(b.params, b.id)
		if err != nil {
			b.cleanBars()
			return downloadingResult{}, err
		}
		if resp.Cancelled {
			b.cancelled = true
			break
		}

		b.updateDownloadingErrors(resp.Errs)
		b.updateBars(resp)
	}

	b.cleanBars()
	return b.result(), nil
}

func (b *progressBar) start() (bool, error) {
	resp, err := api.DownloadStatus(b.params, b.id)
	if err != nil {
		return false, err
	}

	b.updateDownloadingErrors(resp.Errs)
	b.finishedFiles = resp.Finished
	b.totalFiles = resp.Total
	b.cancelled = resp.Cancelled

	if b.finishedFiles+len(b.errors) == b.totalFiles || b.cancelled {
		return true, err
	}

	b.totalBar = b.p.AddBar(
		int64(b.totalFiles),
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Name(totalBarText, decor.WC{W: len(totalBarText) + 1, C: decor.DSyncWidthR}),
			decor.CountersNoUnit("%d/%d", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(decor.Percentage(decor.WCSyncWidth)),
	)
	b.totalBar.IncrBy(b.finishedFiles)

	b.updateBars(resp)

	return false, nil
}

func (b *progressBar) updateBars(downloadStatus cmn.DlStatusResp) {
	fileStates := downloadStatus.CurrentTasks

	b.updateFinishedFiles(fileStates)

	for _, newState := range fileStates {
		oldState, ok := b.states[newState.Name]

		if !ok {
			b.trackNewFile(newState)
			continue
		}

		b.updateFileBar(newState, oldState)
	}

	b.updateTotalBar(downloadStatus.Finished)
}

func (b *progressBar) updateFinishedFiles(fileStates []cmn.TaskDlInfo) {
	// The finished files are those that are in b.states, but were not included in CurrentTasks of the status response
	fileStatesMap := make(map[string]struct{})
	for _, file := range fileStates {
		fileStatesMap[file.Name] = struct{}{}
	}

	finishedFiles := make([]string, 0)
	for name, state := range b.states {
		_, ok := fileStatesMap[name]
		if !ok {
			state.bar.SetTotal(state.total, true) // This completes the bar
			finishedFiles = append(finishedFiles, name)
		}
	}

	for _, finishedFile := range finishedFiles {
		delete(b.states, finishedFile)
	}
}

func (b *progressBar) trackNewFile(state cmn.TaskDlInfo) {
	bar := b.p.AddBar(
		state.Total,
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Name(state.Name, decor.WC{W: len(state.Name) + 1, C: decor.DSyncWidthR}),
			decor.Counters(decor.UnitKiB, "% .1f/% .1f", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(decor.Percentage(decor.WCSyncWidth)),
	)

	bar.IncrBy(int(state.Downloaded), 0)

	b.states[state.Name] = &fileDownloadingState{
		downloaded: state.Downloaded,
		total:      state.Total,
		bar:        bar,
	}
}

func (b *progressBar) updateFileBar(newState cmn.TaskDlInfo, state *fileDownloadingState) {
	if state.total == 0 && newState.Total != 0 {
		state.total = newState.Total
		state.bar.SetTotal(newState.Total, false)
	}

	progress := newState.Downloaded - state.downloaded
	state.downloaded = newState.Downloaded

	if progress > 0 {
		if state.total == 0 {
			// If the total size of the file is unknown, keep setting a fake value to keep the bar running.
			state.bar.SetTotal(state.downloaded+unknownTotalIncrement, false)
		}
		state.bar.IncrBy(int(progress))
	}
}

func (b *progressBar) updateTotalBar(newFinished int) {
	progress := newFinished - b.finishedFiles

	if progress > 0 {
		b.totalBar.IncrBy(progress)
	}

	b.finishedFiles = newFinished
}

func (b *progressBar) updateDownloadingErrors(dlErrs []cmn.TaskErrInfo) {
	for _, dlErr := range dlErrs {
		b.errors[dlErr.Name] = dlErr.Err
	}
}

func (b *progressBar) cleanBars() {
	for _, state := range b.states {
		// Complete the bars
		state.bar.SetTotal(state.total, true)
	}

	b.totalBar.SetTotal(int64(b.totalFiles), true)

	b.p.Wait()
}

func (b *progressBar) result() downloadingResult {
	return downloadingResult{totalFiles: b.totalFiles, finishedFiles: b.finishedFiles, downloadingErrors: b.errors, cancelled: b.cancelled}
}
