// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with objects in the cluster
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"archive/tar"
	"compress/gzip"
	"encoding/hex"
	jsonStd "encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/tmpls"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ext/dsort"
	"github.com/urfave/cli"
	"github.com/vbauerster/mpb/v4"
	"github.com/vbauerster/mpb/v4/decor"
)

var phasesOrdered = []string{
	dsort.ExtractionPhase,
	dsort.SortingPhase,
	dsort.CreationPhase,
}

func phaseToBarText(phase string) string {
	return strings.ToUpper(phase[:1]) + phase[1:] + " phase: "
}

func createTar(w io.Writer, ext string, start, end, fileCnt int, fileSize int64) error {
	var (
		gzw       *gzip.Writer
		tw        *tar.Writer
		random    = cos.NowRand()
		buf       = make([]byte, fileSize)
		randBytes = make([]byte, 10)
	)

	if ext == ".tgz" {
		gzw = gzip.NewWriter(w)
		tw = tar.NewWriter(gzw)
		defer gzw.Close()
	} else {
		tw = tar.NewWriter(w)
	}
	defer tw.Close()

	for fileNum := start; fileNum < end; fileNum++ {
		// Generate random name
		random.Read(randBytes)
		name := fmt.Sprintf("%s-%0*d.test", hex.EncodeToString(randBytes), len(strconv.Itoa(fileCnt)), fileNum)

		h := &tar.Header{
			Typeflag: tar.TypeReg,
			Size:     fileSize,
			Name:     name,
			Uid:      os.Getuid(),
			Gid:      os.Getgid(),
			Mode:     int64(cos.PermRWR),
		}
		if err := tw.WriteHeader(h); err != nil {
			return err
		}

		if _, err := io.CopyBuffer(tw, io.LimitReader(random, fileSize), buf); err != nil {
			return err
		}
	}

	return nil
}

// Creates bucket if not exists. If exists uses it or deletes and creates new
// one if cleanup flag was set.
func setupBucket(c *cli.Context, bck cmn.Bck) error {
	exists, err := api.QueryBuckets(apiBP, cmn.QueryBcks(bck), apc.FltPresent)
	if err != nil {
		return err
	}

	cleanup := flagIsSet(c, cleanupFlag)
	if exists && cleanup {
		if err := api.DestroyBucket(apiBP, bck); err != nil {
			return err
		}
	}

	if !exists || cleanup {
		if err := api.CreateBucket(apiBP, bck, nil); err != nil {
			return err
		}
	}

	return nil
}

type dsortResult struct {
	dur      time.Duration
	created  int64
	errors   []string
	warnings []string
	aborted  bool
}

func (d dsortResult) String() string {
	if d.aborted {
		return dsort.DSortName + " job was aborted"
	}

	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("Created %d new shards. Job duration: %s", d.created, d.dur))
	if len(d.errors) > 0 {
		sb.WriteString("\nFollowing errors occurred during execution: ")
		sb.WriteString(strings.Join(d.errors, ","))
	}
	if len(d.warnings) > 0 {
		sb.WriteString("\nFollowing warnings occurred during execution: ")
		sb.WriteString(strings.Join(d.warnings, ","))
	}

	return sb.String()
}

type dsortPhaseState struct {
	total    int64
	progress int64
	bar      *mpb.Bar
}

type dsortPB struct {
	id          string
	apiBP       api.BaseParams
	refreshTime time.Duration

	p *mpb.Progress

	dur     time.Duration
	phases  map[string]*dsortPhaseState
	aborted bool

	warnings []string
	errors   []string
}

func newPhases() map[string]*dsortPhaseState {
	phases := make(map[string]*dsortPhaseState, 3)
	phases[dsort.ExtractionPhase] = &dsortPhaseState{}
	phases[dsort.SortingPhase] = &dsortPhaseState{}
	phases[dsort.CreationPhase] = &dsortPhaseState{}
	return phases
}

func newDSortPB(baseParams api.BaseParams, id string, refreshTime time.Duration) *dsortPB {
	return &dsortPB{
		id:          id,
		apiBP:       baseParams,
		refreshTime: refreshTime,
		p:           mpb.New(mpb.WithWidth(progressBarWidth)),
		phases:      newPhases(),
		errors:      make([]string, 0),
		warnings:    make([]string, 0),
	}
}

func (b *dsortPB) run() (dsortResult, error) {
	finished, err := b.start()
	if err != nil {
		return dsortResult{}, err
	}
	if finished {
		return b.result(), nil
	}

	for !finished {
		time.Sleep(b.refreshTime)

		metrics, err := api.MetricsDSort(b.apiBP, b.id)
		if err != nil {
			b.cleanBars()
			return dsortResult{}, err
		}

		var targetMetrics *dsort.Metrics
		for _, targetMetrics = range metrics {
			break
		}

		if targetMetrics.Aborted.Load() {
			b.aborted = true
			break
		}

		b.warnings, b.errors = make([]string, 0), make([]string, 0)
		for _, targetMetrics := range metrics {
			b.warnings = append(b.warnings, targetMetrics.Warnings...)
			b.errors = append(b.errors, targetMetrics.Errors...)
		}
		finished = b.updateBars(metrics)
	}

	b.cleanBars()
	return b.result(), nil
}

func (b *dsortPB) start() (bool, error) {
	metrics, err := api.MetricsDSort(b.apiBP, b.id)
	if err != nil {
		return false, err
	}

	finished := b.updateBars(metrics)
	return finished, nil
}

func (b *dsortPB) updateBars(metrics map[string]*dsort.Metrics) bool {
	var (
		targetMetrics *dsort.Metrics
		finished      = true
		phases        = newPhases()
	)

	for _, targetMetrics = range metrics {
		phases[dsort.ExtractionPhase].progress += targetMetrics.Extraction.ExtractedCnt

		phases[dsort.SortingPhase].progress = cos.MaxI64(phases[dsort.SortingPhase].progress, targetMetrics.Sorting.RecvStats.Count)

		phases[dsort.CreationPhase].progress += targetMetrics.Creation.CreatedCnt
		phases[dsort.CreationPhase].total += targetMetrics.Creation.ToCreate

		finished = finished && targetMetrics.Creation.Finished
	}

	phases[dsort.ExtractionPhase].total = targetMetrics.Extraction.TotalCnt
	phases[dsort.SortingPhase].total = int64(math.Log2(float64(len(metrics))))

	// Create progress bars if necessary and/or update them.
	for _, phaseName := range phasesOrdered {
		phase := phases[phaseName]
		barPhase := b.phases[phaseName]

		diff := phase.progress - barPhase.progress

		// If not finished also update the progress bars.
		if !finished {
			if phases[phaseName].progress > 0 && barPhase.bar == nil {
				text := phaseToBarText(phaseName)
				barPhase.bar = b.p.AddBar(
					phase.total,
					mpb.PrependDecorators(
						decor.Name(text, decor.WC{W: len(text) + 2, C: decor.DSyncWidthR}),
						decor.CountersNoUnit("%d/%d", decor.WCSyncWidth),
					),
					mpb.AppendDecorators(decor.Percentage(decor.WCSyncWidth)),
				)
			}

			if barPhase.bar != nil && diff > 0 {
				if phase.total > barPhase.total {
					barPhase.bar.SetTotal(phase.total, false)
				}
				barPhase.bar.IncrBy(int(diff))
			}
		}

		barPhase.progress = phase.progress
		barPhase.total = phase.total
	}

	b.dur = targetMetrics.Creation.End.Sub(targetMetrics.Extraction.Start)
	return finished
}

func (b *dsortPB) cleanBars() {
	for _, phase := range b.phases {
		if phase.bar != nil {
			phase.bar.SetTotal(phase.total, true) // complete the bar
		}
	}
	b.p.Wait()
}

func (b *dsortPB) result() dsortResult {
	var created int64
	if phase, ok := b.phases[dsort.CreationPhase]; ok {
		created = phase.total
	}

	return dsortResult{
		dur:      b.dur,
		created:  created,
		warnings: b.warnings,
		errors:   b.errors,
		aborted:  b.aborted,
	}
}

func printMetrics(w io.Writer, jobID string, daemonIds []string) (aborted, finished bool, err error) {
	resp, err := api.MetricsDSort(apiBP, jobID)
	if err != nil {
		return false, false, err
	}

	aborted = false
	finished = true
	if len(daemonIds) > 0 {
		// Check if targets exist in the metric output.
		for _, daemonID := range daemonIds {
			if _, ok := resp[daemonID]; !ok {
				return false, false, fmt.Errorf("invalid node id %q ", daemonID)
			}
		}
		// Filter metrics only for requested targets.
		filterMap := map[string]*dsort.Metrics{}
		for _, daemonID := range daemonIds {
			filterMap[daemonID] = resp[daemonID]
		}
		resp = filterMap
	}
	for _, targetMetrics := range resp {
		aborted = aborted || targetMetrics.Aborted.Load()
		finished = finished && targetMetrics.Creation.Finished
	}
	// NOTE: because of the still-open issue we use here Go-standard json, not jsoniter
	// https://github.com/json-iterator/go/issues/331
	b, err := jsonStd.MarshalIndent(resp, "", "    ")
	if err != nil {
		return false, false, err
	}

	_, err = fmt.Fprintf(w, "%s\n", string(b))
	return
}

func printCondensedStats(w io.Writer, id string, errhint bool) error {
	var (
		elapsedTime       time.Duration
		extractionTime    time.Duration
		sortingTime       time.Duration
		creationTime      time.Duration
		finished, aborted bool
	)
	resp, err := api.MetricsDSort(apiBP, id)
	if err != nil {
		return err
	}
	finished = true
	for _, tm := range resp {
		aborted = aborted || tm.Aborted.Load()
		finished = finished && tm.Creation.Finished

		elapsedTime = cos.MaxDuration(elapsedTime, tm.ElapsedTime())
		if tm.Extraction.Finished {
			extractionTime = cos.MaxDuration(extractionTime, tm.Extraction.End.Sub(tm.Extraction.Start))
		} else {
			extractionTime = cos.MaxDuration(extractionTime, time.Since(tm.Extraction.Start))
		}

		if tm.Sorting.Finished {
			sortingTime = cos.MaxDuration(sortingTime, tm.Sorting.End.Sub(tm.Sorting.Start))
		} else if tm.Sorting.Running {
			sortingTime = cos.MaxDuration(sortingTime, time.Since(tm.Sorting.Start))
		}

		if tm.Creation.Finished {
			creationTime = cos.MaxDuration(creationTime, tm.Creation.End.Sub(tm.Creation.Start))
		} else if tm.Creation.Running {
			creationTime = cos.MaxDuration(creationTime, time.Since(tm.Creation.Start))
		}
	}

	if aborted {
		fmt.Fprintf(w, "DSort job %s aborted.", id)
		if errhint {
			fmt.Fprintf(w, " For details, run 'ais show job %s -v'\n", id)
		} else {
			fmt.Fprintln(w)
		}
		return nil
	}
	if finished {
		fmt.Fprintf(w, "DSort job successfully finished in %v:\n  "+
			"Longest extraction:\t%v\n  Longest sorting:\t%v\n  Longest creation:\t%v\n",
			elapsedTime, extractionTime, sortingTime, creationTime,
		)
		return nil
	}

	fmt.Fprintf(w, "DSort job currently running:\n  Extraction:\t%v", extractionTime)
	if sortingTime.Seconds() > 0 {
		fmt.Fprintf(w, "\n  Sorting:\t%v", sortingTime)
	}
	if creationTime.Seconds() > 0 {
		fmt.Fprintf(w, "\n  Creation:\t%v", creationTime)
	}
	fmt.Fprint(w, "\n")
	return nil
}

func dsortJobsList(c *cli.Context, list []*dsort.JobInfo, usejs bool) error {
	sort.Slice(list, func(i int, j int) bool {
		if list[i].IsRunning() && !list[j].IsRunning() {
			return true
		}
		if !list[i].IsRunning() && list[j].IsRunning() {
			return false
		}
		if !list[i].Aborted && list[j].Aborted {
			return true
		}
		if list[i].Aborted && !list[j].Aborted {
			return false
		}

		return list[i].StartedTime.Before(list[j].StartedTime)
	})

	hideHeader := flagIsSet(c, noHeaderFlag)
	if hideHeader {
		return tmpls.Print(list, c.App.Writer, tmpls.DSortListNoHdrTmpl, nil, usejs)
	}

	return tmpls.Print(list, c.App.Writer, tmpls.DSortListTmpl, nil, usejs)
}

func dsortJobStatus(c *cli.Context, id string) error {
	var (
		verbose = flagIsSet(c, verboseFlag)
		refresh = flagIsSet(c, refreshFlag)
		logging = flagIsSet(c, logFlag)
		usejs   = flagIsSet(c, jsonFlag)
	)

	// Show progress bar.
	if !verbose && refresh && !logging {
		refreshRate := _refreshRate(c)
		dsortResult, err := newDSortPB(apiBP, id, refreshRate).run()
		if err != nil {
			return err
		}

		fmt.Fprintln(c.App.Writer, dsortResult)
		return nil
	}

	// Show metrics just once.
	if !refresh && !logging {
		if usejs || verbose {
			var daemonIds []string
			if c.NArg() == 2 {
				daemonIds = append(daemonIds, c.Args().Get(1))
			}
			if _, _, err := printMetrics(c.App.Writer, id, daemonIds); err != nil {
				return err
			}

			fmt.Fprintf(c.App.Writer, "\n")
			return printCondensedStats(c.App.Writer, id, false)
		}
		return printCondensedStats(c.App.Writer, id, true)
	}

	// Show metrics once in a while.

	w := c.App.Writer

	rate := _refreshRate(c)
	if logging {
		file, err := cos.CreateFile(c.String(logFlag.Name))
		if err != nil {
			return err
		}
		w = file
		defer file.Close()
	}

	var (
		aborted, finished bool
		err               error
	)
	for {
		aborted, finished, err = printMetrics(w, id, nil)
		if err != nil {
			return err
		}
		if aborted || finished {
			break
		}

		time.Sleep(rate)
	}

	fmt.Fprintln(c.App.Writer)
	return printCondensedStats(c.App.Writer, id, false)
}
