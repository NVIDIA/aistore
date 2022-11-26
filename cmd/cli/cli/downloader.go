// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles download jobs in the cluster.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmd/cli/tmpls"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/dloader"
	"github.com/urfave/cli"
	"github.com/vbauerster/mpb/v4"
	"github.com/vbauerster/mpb/v4/decor"
)

type (
	downloadingResult struct {
		totalFiles        int
		finishedFiles     int
		errFiles          int
		downloadingErrors map[string]string
		finished          bool
		aborted           bool
	}

	fileDownloadingState struct {
		total      int64
		downloaded int64
		bar        *mpb.Bar
		lastTime   time.Time
	}

	downloaderPB struct {
		id          string
		apiBP       api.BaseParams
		refreshTime time.Duration

		states map[string]*fileDownloadingState

		p        *mpb.Progress
		totalBar *mpb.Bar

		totalFiles     int
		finishedFiles  int
		scheduledFiles int
		errFiles       int

		aborted       bool
		allDispatched bool
	}
)

const (
	totalBarText          = "Files downloaded:"
	unknownTotalIncrement = 2048
	progressBarWidth      = 64
	minTotalCnt           = 10
)

func (d downloadingResult) String() string {
	if d.aborted {
		return "Download was aborted."
	}

	if d.finished {
		if d.errFiles == 0 {
			return "All files successfully downloaded."
		}
		return fmt.Sprintf("Download jobs finished with errors.\nObjects downloaded: %d, failed downloads: %d.",
			d.finishedFiles, d.errFiles)
	}

	var sb strings.Builder

	if d.totalFiles > 0 {
		sb.WriteString(fmt.Sprintf("Downloaded %d out of %d files.", d.finishedFiles, d.totalFiles))
	} else {
		sb.WriteString(fmt.Sprintf("Downloaded %d files.", d.finishedFiles))
	}
	if len(d.downloadingErrors) > 0 {
		sb.WriteString("\nFollowing files caused downloading errors:")
		for file, err := range d.downloadingErrors {
			sb.WriteString(fmt.Sprintf("\n\t%s: %s", file, err))
		}
	}

	return sb.String()
}

func newDownloaderPB(baseParams api.BaseParams, id string, refreshTime time.Duration) *downloaderPB {
	return &downloaderPB{
		id:          id,
		apiBP:       baseParams,
		refreshTime: refreshTime,
		states:      make(map[string]*fileDownloadingState),
		p:           mpb.New(mpb.WithWidth(progressBarWidth)),
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

		resp, err := api.DownloadStatus(b.apiBP, b.id, true)
		if err != nil {
			b.cleanBars()
			return downloadingResult{}, err
		}
		if resp.Aborted {
			b.aborted = true
			break
		}

		b.updateBars(resp)
	}

	b.cleanBars()
	return b.result(), nil
}

func (b *downloaderPB) start() (bool, error) {
	resp, err := api.DownloadStatus(b.apiBP, b.id, true)
	if err != nil {
		return false, err
	}

	b.finishedFiles = resp.FinishedCnt
	b.totalFiles = resp.Total
	b.scheduledFiles = resp.ScheduledCnt
	b.errFiles = resp.ErrorCnt

	b.allDispatched = resp.AllDispatched
	b.aborted = resp.Aborted

	if b.jobFinished() {
		return true, err
	}

	barText := totalBarText
	if b.totalFilesCnt() != b.totalFiles {
		barText += " (total estimated)"
	}

	if b.totalFilesCnt() > 1 {
		b.totalBar = b.p.AddBar(
			int64(b.totalFilesCnt()),
			mpb.BarRemoveOnComplete(),
			mpb.PrependDecorators(
				decor.Name(barText, decor.WC{W: len(totalBarText) + 2, C: decor.DSyncWidthR}),
				decor.CountersNoUnit("%d/%d", decor.WCSyncWidth),
			),
			mpb.AppendDecorators(decor.Percentage(decor.WCSyncWidth)),
		)
		b.totalBar.IncrBy(b.finishedFiles + b.errFiles)
	}

	b.updateBars(resp)

	return false, nil
}

func (b *downloaderPB) updateBars(downloadStatus *dloader.StatusResp) {
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
	if downloadStatus.TotalCnt() > 1 {
		b.updateTotalBar(downloadStatus.FinishedCnt+downloadStatus.ErrorCnt, downloadStatus.TotalCnt())
		b.finishedFiles = downloadStatus.FinishedCnt
		b.errFiles = downloadStatus.ErrorCnt
	}
}

func (b *downloaderPB) updateFinishedFiles(fileStates []dloader.TaskDlInfo) {
	// The finished files are those that are in b.states, but were not included in CurrentTasks of the status response
	fileStatesMap := make(cos.StrSet)
	for _, file := range fileStates {
		fileStatesMap.Add(file.Name)
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

func (b *downloaderPB) trackNewFile(state dloader.TaskDlInfo) {
	bar := b.p.AddBar(
		state.Total,
		mpb.BarStyle("[=>-|"),
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Name(state.Name+" ", decor.WC{W: len(state.Name) + 1, C: decor.DSyncWidthR}),
			decor.Counters(decor.UnitKiB, "%.1f/%.1f", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(
			decor.EwmaETA(decor.ET_STYLE_HHMMSS, 90),
			decor.Name(" ] "),
			decor.EwmaSpeed(decor.UnitKiB, "% .1f", 60, decor.WC{W: 10}),
		),
	)

	bar.IncrBy(int(state.Downloaded))

	b.states[state.Name] = &fileDownloadingState{
		downloaded: state.Downloaded,
		total:      state.Total,
		bar:        bar,
		lastTime:   time.Now(),
	}
}

func (*downloaderPB) updateFileBar(newState dloader.TaskDlInfo, state *fileDownloadingState) {
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
		state.bar.IncrBy(int(progress), time.Since(state.lastTime))
	} else {
		// If progress was reverted due error and retry we must update progress
		state.bar.SetCurrent(state.downloaded)
		state.bar.AdjustAverageDecorators(time.Now())
	}
	state.lastTime = time.Now()
}

func (b *downloaderPB) updateTotalBar(newFinished, total int) {
	progress := newFinished - (b.finishedFiles + b.errFiles)
	if progress > 0 {
		b.totalBar.IncrBy(progress)
	}

	b.totalBar.SetTotal(int64(total), false)
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
	return downloadingResult{
		totalFiles:    b.totalFiles,
		finishedFiles: b.finishedFiles,
		errFiles:      b.errFiles,
		finished:      b.jobFinished(),
		aborted:       b.aborted,
	}
}

func (b *downloaderPB) jobFinished() bool {
	return b.aborted || (b.allDispatched && b.scheduledFiles == b.finishedFiles+b.errFiles)
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
	list, err := api.DownloadGetList(apiBP, regex, !flagIsSet(c, allJobsFlag))
	if err != nil {
		return err
	}

	sort.Slice(list, func(i int, j int) bool {
		if !list[i].JobFinished() && (list[j].JobFinished() || list[j].Aborted) {
			return true
		}
		if (list[i].JobFinished() || list[i].Aborted) && !list[j].JobFinished() {
			return false
		}

		if !list[i].JobFinished() && !list[j].JobFinished() {
			return list[i].StartedTime.Before(list[j].StartedTime)
		}
		if list[i].JobFinished() && list[j].JobFinished() {
			return list[i].FinishedTime.Before(list[j].StartedTime)
		}

		return true
	})

	return tmpls.Print(list, c.App.Writer, tmpls.DownloadListTmpl, nil, flagIsSet(c, jsonFlag))
}

func downloadJobStatus(c *cli.Context, id string) error {
	// with progress bar
	if flagIsSet(c, progressBarFlag) {
		refreshRate := calcRefreshRate(c)
		downloadingResult, err := newDownloaderPB(apiBP, id, refreshRate).run()
		if err != nil {
			return err
		}

		fmt.Fprintln(c.App.Writer, downloadingResult)
		return nil
	}

	resp, err := api.DownloadStatus(apiBP, id, !flagIsSet(c, allJobsFlag))
	if err != nil {
		return err
	}

	// without progress bar
	printDownloadStatus(c.App.Writer, resp, flagIsSet(c, verboseFlag))
	return nil
}

func printDownloadStatus(w io.Writer, d *dloader.StatusResp, verbose bool) {
	if d.Aborted {
		fmt.Fprintln(w, "Download aborted")
		return
	}

	if d.JobFinished() {
		if d.SkippedCnt > 0 {
			fmt.Fprintf(w, "Done: %d file%s downloaded (skipped: %d), %d error%s\n",
				d.FinishedCnt, cos.NounEnding(d.FinishedCnt), d.SkippedCnt, d.ErrorCnt, cos.NounEnding(d.ErrorCnt))
		} else {
			fmt.Fprintf(w, "Done: %d file%s downloaded, %d error%s\n",
				d.FinishedCnt, cos.NounEnding(d.FinishedCnt), d.ErrorCnt, cos.NounEnding(d.ErrorCnt))
		}

		if verbose && len(d.Errs) > 0 {
			fmt.Fprintln(w, "Errors:")
			for _, e := range d.Errs {
				fmt.Fprintf(w, "\t%s: %s\n", e.Name, e.Err)
			}
		}
		return
	}

	var (
		doneCnt  = d.DoneCnt()
		totalCnt = d.TotalCnt()
	)
	if totalCnt < minTotalCnt {
		verbose = true
	}
	if totalCnt == 0 {
		fmt.Fprintf(w, "Download progress: 0/?\n")
	} else {
		progressMsg := fmt.Sprintf("Download progress: %d/%d", doneCnt, totalCnt)
		if totalCnt >= minTotalCnt {
			pctDone := 100 * float64(doneCnt) / float64(totalCnt)
			progressMsg = fmt.Sprintf("%s (%0.2f%%)", progressMsg, pctDone)
		}
		fmt.Fprintln(w, progressMsg)
	}
	if verbose {
		if len(d.CurrentTasks) > 0 {
			sort.Slice(d.CurrentTasks, func(i, j int) bool {
				return d.CurrentTasks[i].Name < d.CurrentTasks[j].Name
			})

			fmt.Fprintln(w, "Progress of files that are currently being downloaded:")
			for _, task := range d.CurrentTasks {
				fmt.Fprintf(w, "\t%s: ", task.Name)
				if task.Total == 0 {
					fmt.Fprintln(w, cos.B2S(task.Downloaded, 2))
				} else {
					pctDownloaded := 100 * float64(task.Downloaded) / float64(task.Total)
					fmt.Fprintf(w, "%s/%s (%.2f%%)\n",
						cos.B2S(task.Downloaded, 2), cos.B2S(task.Total, 2), pctDownloaded)
				}
			}
		}
		if d.ErrorCnt > 0 {
			fmt.Fprintln(w, "Errors:")
			for _, e := range d.Errs {
				fmt.Fprintf(w, "\t%s: %s\n", e.Name, e.Err)
			}
		}
	} else if d.ErrorCnt > 0 {
		fmt.Fprintf(w, "Errors (%d) occurred during the download. To see detailed info run `ais show job download %s -v`\n", d.ErrorCnt, d.ID)
	}
}
