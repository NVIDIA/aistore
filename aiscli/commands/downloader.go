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

	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

const (
	DownloadSingle = "single"
	DownloadRange  = "range"
	DownloadCloud  = "cloud"
	DownloadStatus = "status"
	DownloadCancel = "cancel"

	progressBarRefreshRateUnit    = time.Millisecond
	progressBarRefreshRateDefault = 1000
	progressBarRefreshRateMin     = 500
	totalBarText                  = "Files downloaded:"
	unknownTotalIncrement         = 2048
	progressBarWidth              = 64
)

var (
	BaseDownloadFlags = []cli.Flag{
		bucketFlag,
		bckProviderFlag,
		timeoutFlag,
	}

	DownloadFlags = map[string][]cli.Flag{
		DownloadSingle: append(
			[]cli.Flag{
				objNameFlag,
				linkFlag,
			},
			BaseDownloadFlags...,
		),
		DownloadRange: append(
			[]cli.Flag{
				baseFlag,
				templateFlag,
			},
			BaseDownloadFlags...,
		),
		DownloadCloud: append(
			[]cli.Flag{
				dlPrefixFlag,
				dlSuffixFlag,
			},
			BaseDownloadFlags...,
		),
		DownloadStatus: []cli.Flag{
			idFlag,
			progressBarFlag,
			refreshRateFlag,
		},
		DownloadCancel: []cli.Flag{
			idFlag,
		},
	}

	DownloadSingleUsage = fmt.Sprintf("aiscli download --bucket <value> [FLAGS...] %s --objname <value> --link <value>", DownloadSingle)
	DownloadRangeUsage  = fmt.Sprintf("aiscli download --bucket <value> [FLAGS...] %s --base <value> --template <value>", DownloadRange)
	DownloadCloudUsage  = fmt.Sprintf("aiscli download --bucket <value> [FLAGS...] %s --prefix <value> --suffix <value>", DownloadCloud)
	DownloadStatusUsage = fmt.Sprintf("aiscli download %s --id <value> [STATUS FLAGS...]", DownloadStatus)
	DownloadCancelUsage = fmt.Sprintf("aiscli download %s --id <value>", DownloadCancel)
)

func DownloadHandler(c *cli.Context) error {
	var (
		baseParams = cliAPIParams(ClusterURL)
		bucket     = parseFlag(c, bucketFlag.Name)
		timeout    = parseFlag(c, timeoutFlag.Name)

		id string
	)

	if err := checkFlags(c, bucketFlag.Name); err != nil {
		return err
	}

	bckProvider, err := cluster.TranslateBckProvider(parseFlag(c, bckProviderFlag.Name))
	if err != nil {
		return err
	}

	basePayload := cmn.DlBase{
		Bucket:      bucket,
		BckProvider: bckProvider,
		Timeout:     timeout,
	}

	commandName := c.Command.Name
	switch commandName {
	case DownloadSingle:
		if err := checkFlags(c, objNameFlag.Name, linkFlag.Name); err != nil {
			return err
		}

		objName := parseFlag(c, objNameFlag.Name)
		link := parseFlag(c, linkFlag.Name)
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
	case DownloadRange:
		if err := checkFlags(c, baseFlag.Name, templateFlag.Name); err != nil {
			return err
		}

		base := parseFlag(c, baseFlag.Name)
		template := parseFlag(c, templateFlag.Name)
		payload := cmn.DlRangeBody{
			DlBase:   basePayload,
			Base:     base,
			Template: template,
		}

		id, err = api.DownloadRangeWithParam(baseParams, payload)
		if err != nil {
			return err
		}
	case DownloadCloud:
		prefix := parseFlag(c, dlPrefixFlag.Name)
		suffix := parseFlag(c, dlSuffixFlag.Name)
		payload := cmn.DlCloudBody{
			DlBase: basePayload,
			Prefix: prefix,
			Suffix: suffix,
		}
		id, err = api.DownloadCloudWithParam(baseParams, payload)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid command name '%s'", commandName)
	}
	fmt.Println(id)
	return nil
}

func DownloadAdminHandler(c *cli.Context) error {
	var (
		baseParams = cliAPIParams(ClusterURL)
		id         = parseFlag(c, idFlag.Name)
	)

	if err := checkFlags(c, idFlag.Name); err != nil {
		return err
	}

	commandName := c.Command.Name
	switch commandName {
	case DownloadStatus:
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

			fmt.Println(resp.String())
		}
	case DownloadCancel:
		if err := api.DownloadCancel(baseParams, id); err != nil {
			return err
		}
		fmt.Printf("download canceled: %s\n", id)
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
}

func (d downloadingResult) String() string {
	var sb strings.Builder

	if d.finishedFiles == d.totalFiles {
		sb.WriteString("All files successfully downloaded.")
		return sb.String()
	}

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
	err := b.start()
	if err != nil {
		return downloadingResult{}, err
	}

	// All files = finished ones + ones that had downloading errors
	for b.finishedFiles+len(b.errors) < b.totalFiles {
		time.Sleep(b.refreshTime)

		resp, err := api.DownloadStatus(b.params, b.id)
		if err != nil {
			b.cleanBars()
			return downloadingResult{}, err
		}

		b.updateDownloadingErrors(resp.Errs)
		b.updateBars(resp)
	}

	b.cleanBars()
	return downloadingResult{totalFiles: b.totalFiles, finishedFiles: b.finishedFiles, downloadingErrors: b.errors}, nil
}

func (b *progressBar) start() error {
	resp, err := api.DownloadStatus(b.params, b.id)
	if err != nil {
		return err
	}

	b.updateDownloadingErrors(resp.Errs)
	b.finishedFiles = resp.Finished
	b.totalFiles = resp.Total

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

	return nil
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
