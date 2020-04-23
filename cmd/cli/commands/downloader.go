// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles download jobs in the cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmd/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/urfave/cli"
	"github.com/vbauerster/mpb/v4"
	"github.com/vbauerster/mpb/v4/decor"
)

type (
	downloadingResult struct {
		totalFiles        int
		finishedFiles     int
		downloadingErrors map[string]string
		aborted           bool
	}

	fileDownloadingState struct {
		total      int64
		downloaded int64
		bar        *mpb.Bar
	}

	downloaderPB struct {
		id          string
		params      api.BaseParams
		refreshTime time.Duration

		states map[string]*fileDownloadingState

		p        *mpb.Progress
		totalBar *mpb.Bar

		totalFiles     int
		finishedFiles  int
		scheduledFiles int

		aborted       bool
		allDispatched bool

		errors map[string]string
	}
)

const (
	totalBarText          = "Files downloaded:"
	unknownTotalIncrement = 2048
	progressBarWidth      = 64
)

func (d downloadingResult) String() string {
	if d.aborted {
		return "Download was aborted."
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

func newDownloaderPB(baseParams api.BaseParams, id string, refreshTime time.Duration) *downloaderPB {
	return &downloaderPB{
		id:          id,
		params:      baseParams,
		refreshTime: refreshTime,
		states:      make(map[string]*fileDownloadingState),
		p:           mpb.New(mpb.WithWidth(progressBarWidth)),
		errors:      make(map[string]string),
	}
}

func (b *downloaderPB) run() (downloadingResult, error) {
	finished, err := b.start()
	if err != nil {
		return downloadingResult{}, err
	}
	if finished {
		return b.result(), nil
	}

	// All files = finished ones + ones that had downloading errors
	for !b.jobFinished() {
		time.Sleep(b.refreshTime)

		resp, err := api.DownloadStatus(b.params, b.id)
		if err != nil {
			b.cleanBars()
			return downloadingResult{}, err
		}
		if resp.Aborted {
			b.aborted = true
			break
		}

		b.updateDownloadingErrors(resp.Errs)
		b.updateBars(resp)
	}

	b.cleanBars()
	return b.result(), nil
}

func (b *downloaderPB) start() (bool, error) {
	resp, err := api.DownloadStatus(b.params, b.id)
	if err != nil {
		return false, err
	}

	b.updateDownloadingErrors(resp.Errs)

	b.finishedFiles = resp.Finished
	b.totalFiles = resp.Total
	b.scheduledFiles = resp.Scheduled

	b.allDispatched = resp.AllDispatched
	b.aborted = resp.Aborted

	if b.jobFinished() {
		return true, err
	}

	barText := totalBarText
	if b.totalFilesCnt() != b.totalFiles {
		barText += " (total estimated)"
	}

	b.totalBar = b.p.AddBar(
		int64(b.totalFilesCnt()),
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Name(barText, decor.WC{W: len(totalBarText) + 2, C: decor.DSyncWidthR}),
			decor.CountersNoUnit("%d/%d", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(decor.Percentage(decor.WCSyncWidth)),
	)
	b.totalBar.IncrBy(b.finishedFiles)

	b.updateBars(resp)

	return false, nil
}

func (b *downloaderPB) updateBars(downloadStatus downloader.DlStatusResp) {
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

	b.updateTotalBar(downloadStatus.Finished+len(downloadStatus.Errs), downloadStatus.TotalCnt())
}

func (b *downloaderPB) updateFinishedFiles(fileStates []downloader.TaskDlInfo) {
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

func (b *downloaderPB) trackNewFile(state downloader.TaskDlInfo) {
	bar := b.p.AddBar(
		state.Total,
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Name(state.Name+" ", decor.WC{W: len(state.Name) + 1, C: decor.DSyncWidthR}),
			decor.Counters(decor.UnitKiB, "%.1f/%.1f", decor.WCSyncWidth),
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

func (b *downloaderPB) updateFileBar(newState downloader.TaskDlInfo, state *fileDownloadingState) {
	if (state.total == 0 && newState.Total != 0) || (state.total != newState.Total) {
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
	} else {
		// If progress was reverted due error and retry we must update progress
		state.bar.SetCurrent(state.downloaded)
	}
}

func (b *downloaderPB) updateTotalBar(newFinished, total int) {
	progress := newFinished - b.finishedFiles

	if progress > 0 {
		b.totalBar.IncrBy(progress)
	}

	b.totalBar.SetTotal(int64(total), false)
	b.finishedFiles = newFinished
}

func (b *downloaderPB) updateDownloadingErrors(dlErrs []downloader.TaskErrInfo) {
	for _, dlErr := range dlErrs {
		b.errors[dlErr.Name] = dlErr.Err
	}
}

func (b *downloaderPB) cleanBars() {
	for _, state := range b.states {
		// Complete the bars
		state.bar.SetTotal(state.total, true)
	}

	b.totalBar.SetTotal(int64(b.totalFiles), true)

	b.p.Wait()
}

func (b *downloaderPB) result() downloadingResult {
	return downloadingResult{totalFiles: b.totalFiles, finishedFiles: b.finishedFiles, downloadingErrors: b.errors, aborted: b.aborted}
}

func (b *downloaderPB) jobFinished() bool {
	return b.aborted || (b.allDispatched && b.scheduledFiles == b.finishedFiles+len(b.errors))
}

func (b *downloaderPB) totalFilesCnt() int {
	if b.totalFiles <= 0 {
		// totalFiles <= 0 - we don't know exact number of files to be downloaded
		// we show as total number of files, number of files which were scheduled to be downloaded
		// still, the new ones might be discovered
		return b.scheduledFiles
	}
	return b.totalFiles
}

func downloadJobsList(c *cli.Context, regex string) error {
	list, err := api.DownloadGetList(defaultAPIParams, regex)
	if err != nil {
		return err
	}

	return templates.DisplayOutput(list, c.App.Writer, templates.DownloadListTmpl)
}

func downloadJobStatus(c *cli.Context, id string) error {
	// with progress bar
	if flagIsSet(c, progressBarFlag) {
		refreshRate := calcRefreshRate(c)
		downloadingResult, err := newDownloaderPB(defaultAPIParams, id, refreshRate).run()
		if err != nil {
			return err
		}

		fmt.Fprintln(c.App.Writer, downloadingResult)
		return nil
	}

	// without progress bar
	resp, err := api.DownloadStatus(defaultAPIParams, id)
	if err != nil {
		return err
	}

	verbose := flagIsSet(c, verboseFlag)
	printDownloadStatus(c.App.Writer, resp, verbose)
	return nil
}

func printDownloadStatus(w io.Writer, d downloader.DlStatusResp, verbose bool) {
	if d.Aborted {
		fmt.Fprintln(w, "Download aborted")
		return
	}

	errCount := len(d.Errs)
	if d.JobFinished() {
		fmt.Fprintf(w, "Done: %d file%s downloaded, %d error%s\n",
			d.Finished, cmn.NounEnding(d.Finished), errCount, cmn.NounEnding(errCount))

		if verbose {
			for _, e := range d.Errs {
				fmt.Fprintf(w, "%s: %s\n", e.Name, e.Err)
			}
		}
		return
	}

	realFinished := d.Finished + errCount
	fmt.Fprintf(w, "Download progress: %d/%d (%.2f%%)\n",
		realFinished, d.TotalCnt(), 100*float64(realFinished)/float64(d.TotalCnt()))
	if !verbose || len(d.CurrentTasks) == 0 {
		return
	}

	sort.Slice(d.CurrentTasks, func(i, j int) bool {
		return d.CurrentTasks[i].Name < d.CurrentTasks[j].Name
	})

	fmt.Fprintln(w, "Progress of files that are currently being downloaded:")
	for _, task := range d.CurrentTasks {
		fmt.Fprintf(w, "\t%s: ", task.Name)
		if task.Total == 0 {
			fmt.Fprintln(w, cmn.B2S(task.Downloaded, 2))
		} else {
			pctDownloaded := 100 * float64(task.Downloaded) / float64(task.Total)
			fmt.Fprintf(w, "%s/%s (%.2f%%)\n", cmn.B2S(task.Downloaded, 2), cmn.B2S(task.Total, 2), pctDownloaded)
		}
	}
}
