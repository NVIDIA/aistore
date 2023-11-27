// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles download jobs in the cluster.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ext/dload"
	"github.com/NVIDIA/aistore/xact"
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
		p:           mpb.New(mpb.WithWidth(barWidth)),
	}
}

func (b *downloaderPB) run() (downloadingResult, error) {
	finishedEarly, err := b.start()
	if err != nil {
		return downloadingResult{}, err
	}
	if finishedEarly {
		return b.result(), nil
	}

	// All files = finished ones + ones that had downloading errors
	// TODO: factor in:
	// 1) no-change in downloaded stats for more than `timeoutNoChange`
	// 2) resp.JobFinished()
	for !b.jobFinished() {
		time.Sleep(b.refreshTime)

		resp, err := api.DownloadStatus(b.apiBP, b.id, true)
		if err != nil {
			b.cleanBars()
			return downloadingResult{}, V(err)
		}
		if resp.Aborted {
			b.aborted = true
			break
		}
		b.updateBarsAndStatus(resp)
	}

	b.cleanBars()
	return b.result(), nil
}

func (b *downloaderPB) start() (bool /*finishedE early*/, error) {
	resp, err := api.DownloadStatus(b.apiBP, b.id, true)
	if err != nil {
		return false, V(err)
	}

	b.updateStatus(resp)

	if b.jobFinished() {
		return true, err
	}

	barText := totalBarText
	if b.totalFilesCnt() != b.totalFiles {
		barText += " (total estimated)"
	}

	if cnt := b.totalFilesCnt(); cnt > 1 {
		b.totalBar = b.p.AddBar(
			int64(cnt),
			mpb.BarRemoveOnComplete(),
			mpb.PrependDecorators(
				decor.Name(barText, decor.WC{W: len(totalBarText) + 2, C: decor.DSyncWidthR}),
				decor.CountersNoUnit("%d/%d", decor.WCSyncWidth),
			),
			mpb.AppendDecorators(decor.Percentage(decor.WCSyncWidth)),
		)
		b.totalBar.IncrBy(b.finishedFiles + b.errFiles)
	}

	b.updateBarsAndStatus(resp)
	return false, nil
}

func (b *downloaderPB) updateStatus(resp *dload.StatusResp) {
	b.finishedFiles = resp.FinishedCnt
	b.totalFiles = resp.Total
	b.scheduledFiles = resp.ScheduledCnt
	b.errFiles = resp.ErrorCnt

	b.allDispatched = resp.AllDispatched
	b.aborted = resp.Aborted
}

func (b *downloaderPB) updateBarsAndStatus(downloadStatus *dload.StatusResp) {
	fileStates := downloadStatus.CurrentTasks

	b.updateFinishedFiles(fileStates)

	for i := range fileStates {
		newState := fileStates[i]
		oldState, ok := b.states[newState.Name]
		if !ok {
			b.trackNewFile(&newState)
			continue
		}

		b.updateFileBar(&newState, oldState)
	}

	b.updateStatus(downloadStatus)

	if downloadStatus.TotalCnt() > 1 {
		b.updateTotalBar(downloadStatus.FinishedCnt+downloadStatus.ErrorCnt, downloadStatus.TotalCnt())
	}
}

// finished files are those that are in b.states
// but were not included in CurrentTasks of the status response
func (b *downloaderPB) updateFinishedFiles(fileStates []dload.TaskDlInfo) {
	var (
		fileStatesMap = make(cos.StrSet, len(fileStates))
		finishedFiles []string
	)
	for _, file := range fileStates {
		fileStatesMap.Add(file.Name)
	}
	for name, state := range b.states {
		_, ok := fileStatesMap[name]
		if !ok {
			state.bar.SetTotal(state.total, true) // This completes the bar
			finishedFiles = append(finishedFiles, name)
		}
	}
	for _, f := range finishedFiles {
		delete(b.states, f)
	}
}

func (b *downloaderPB) trackNewFile(state *dload.TaskDlInfo) {
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

func (*downloaderPB) updateFileBar(newState *dload.TaskDlInfo, state *fileDownloadingState) {
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

	if b.totalBar != nil {
		b.totalBar.SetTotal(int64(b.totalFiles), true)
	}

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

func downloadJobsList(c *cli.Context, regex string, caption bool) (int, error) {
	onlyActive := !flagIsSet(c, allJobsFlag)
	list, err := api.DownloadGetList(apiBP, regex, onlyActive)
	if err != nil || len(list) == 0 {
		return 0, V(err)
	}
	if caption {
		jobCptn(c, cmdDownload, onlyActive, "", false)
	}
	l := len(list)
	if l == 0 {
		return 0, nil
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

	var (
		hideHeader  = flagIsSet(c, noHeaderFlag)
		units, errU = parseUnitsFlag(c, unitsFlag)
		opts        = teb.Opts{AltMap: teb.FuncMapUnits(units), UseJSON: flagIsSet(c, jsonFlag)}
		verbose     = flagIsSet(c, verboseJobFlag)
	)
	debug.AssertNoErr(errU)

	if !verbose {
		if hideHeader {
			return l, teb.Print(list, teb.DownloadListNoHdrTmpl, opts)
		}
		return l, teb.Print(list, teb.DownloadListTmpl, opts)
	}
	// verbose - one at a time
	for _, j := range list {
		if err := teb.Print(dload.JobInfos{j}, teb.DownloadListTmpl, opts); err != nil {
			return l, err
		}
		xargs := xact.ArgsMsg{ID: j.XactID, Kind: apc.ActDownload}
		if _, err := xactList(c, &xargs, false /*caption*/); err != nil {
			return l, err
		}
		fmt.Fprintln(c.App.Writer)
	}
	return l, nil
}

func downloadJobStatus(c *cli.Context, id string) error {
	debug.Assert(strings.HasPrefix(id, dload.PrefixJobID), id)

	// with progress bar
	if flagIsSet(c, progressFlag) {
		refreshRate := _refreshRate(c)
		downloadingResult, err := newDownloaderPB(apiBP, id, refreshRate).run()
		if err != nil {
			return err
		}

		fmt.Fprintln(c.App.Writer, downloadingResult)
		return nil
	}

	resp, err := api.DownloadStatus(apiBP, id, false /*onlyActive*/)
	if err != nil {
		return V(err)
	}

	// without progress bar
	printDownloadStatus(c, resp, flagIsSet(c, verboseJobFlag))
	return nil
}

func printDownloadStatus(c *cli.Context, d *dload.StatusResp, verbose bool) {
	w := c.App.Writer
	if d.Aborted {
		fmt.Fprintf(w, "Download %s\n", d.String())
		return
	}

	if d.JobFinished() {
		var skipped, errs string
		if d.SkippedCnt > 0 {
			skipped = fmt.Sprintf(", skipped: %d", d.SkippedCnt)
		}
		if d.ErrorCnt > 0 {
			errs = fmt.Sprintf(", error%s: %d", cos.Plural(d.ErrorCnt), d.ErrorCnt)
		}
		fmt.Fprintf(w, "Done: %d file%s downloaded%s%s\n", d.FinishedCnt, cos.Plural(d.FinishedCnt), skipped, errs)

		if len(d.Errs) == 0 {
			debug.Assert(d.ErrorCnt == 0)
			return
		}
		if verbose {
			fmt.Fprintln(w, "Errors:")
			for _, e := range d.Errs {
				fmt.Fprintf(w, "\t%s: %s\n", e.Name, e.Err)
			}
		} else {
			const hint = "Use %s option to list all errors.\n"
			fmt.Fprintf(w, hint, qflprn(verboseFlag))
		}
		return
	}

	// running
	var (
		doneCnt  = d.DoneCnt()
		totalCnt = d.TotalCnt()
	)
	if totalCnt < minTotalCnt {
		verbose = true
	}
	if totalCnt == 0 {
		fmt.Fprintf(w, "Download %s progress: 0/?\n", d.ID)
	} else {
		progressMsg := fmt.Sprintf("%s progress: downloaded %d file%s (out of %d) ", d.ID, doneCnt, cos.Plural(doneCnt), totalCnt)
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
			for _, task := range d.CurrentTasks {
				fmt.Fprintf(w, "\t%s: ", task.Name)
				if task.Total == 0 {
					fmt.Fprintln(w, cos.ToSizeIEC(task.Downloaded, 2))
				} else {
					pctDownloaded := 100 * float64(task.Downloaded) / float64(task.Total)
					fmt.Fprintf(w, "%s/%s (%.2f%%)\n",
						cos.ToSizeIEC(task.Downloaded, 2), cos.ToSizeIEC(task.Total, 2), pctDownloaded)
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
		fmt.Fprintf(w, "Encountered %d error%s during download %s\n", d.ErrorCnt, cos.Plural(d.ErrorCnt), d.ID)
		fmt.Fprintf(w, "For details, run 'ais show job %s -v'\n", d.ID)
	}
}
