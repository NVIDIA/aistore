// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with objects in the cluster
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"bytes"
	jsonStd "encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ext/dsort"
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
	"github.com/vbauerster/mpb/v4"
	"github.com/vbauerster/mpb/v4/decor"
	"gopkg.in/yaml.v2"
)

type (
	dsortResult struct {
		dur      time.Duration
		created  int64
		errors   []string
		warnings []string
		aborted  bool
	}
	dsortPhaseState struct {
		total    int64
		progress int64
		bar      *mpb.Bar
	}
	dsortPB struct {
		id          string
		apiBP       api.BaseParams
		refreshTime time.Duration
		p           *mpb.Progress
		dur         time.Duration
		phases      map[string]*dsortPhaseState
		warnings    []string
		errors      []string
		aborted     bool
	}
)

var phasesOrdered = []string{
	dsort.ExtractionPhase,
	dsort.SortingPhase,
	dsort.CreationPhase,
}

func startDsortHandler(c *cli.Context) (err error) {
	var (
		id             string
		specPath       string
		specBytes      []byte
		shift          int
		srcbck, dstbck cmn.Bck
		spec           dsort.RequestSpec
	)
	// parse command line
	specPath = parseStrFlag(c, dsortSpecFlag)
	if c.NArg() == 0 && specPath == "" {
		return fmt.Errorf("missing %q argument (see %s for details and usage examples)",
			c.Command.ArgsUsage, qflprn(cli.HelpFlag))
	}
	if specPath == "" {
		// spec is inline
		specBytes = []byte(c.Args().Get(0))
		shift = 1
	}
	if c.NArg() > shift {
		srcbck, err = parseBckURI(c, c.Args().Get(shift), true)
		if err != nil {
			return fmt.Errorf("failed to parse source bucket: %v\n(see %s for details)",
				err, qflprn(cli.HelpFlag))
		}
	}
	if c.NArg() > shift+1 {
		dstbck, err = parseBckURI(c, c.Args().Get(shift+1), true)
		if err != nil {
			return fmt.Errorf("failed to parse destination bucket: %v\n(see %s for details)",
				err, qflprn(cli.HelpFlag))
		}
	}

	// load spec from file or standard input
	if specPath != "" {
		var r io.Reader
		if specPath == fileStdIO {
			r = os.Stdin
		} else {
			f, err := os.Open(specPath)
			if err != nil {
				return err
			}
			defer f.Close()
			r = f
		}

		var b bytes.Buffer
		// Read at most 1MB so we don't blow up when reading don't know what
		if _, errV := io.CopyN(&b, r, cos.MiB); errV == nil {
			return errors.New("file too big")
		} else if errV != io.EOF {
			return errV
		}
		specBytes = b.Bytes()
	}
	if errj := jsoniter.Unmarshal(specBytes, &spec); errj != nil {
		if erry := yaml.Unmarshal(specBytes, &spec); erry != nil {
			return fmt.Errorf(
				"failed to determine the type of the job specification, errs: (%v, %v)",
				errj, erry,
			)
		}
	}

	// NOTE: args SRC_BUCKET and DST_BUCKET, if defined, override specBytes/specPath (`dsort.RequestSpec`)
	if !srcbck.IsEmpty() {
		spec.Bck = srcbck
	}
	if !dstbck.IsEmpty() {
		spec.OutputBck = dstbck
	}

	// print resulting spec TODO -- FIXME
	if flagIsSet(c, verboseFlag) {
		flat := _flattenSpec(&spec)
		err = teb.Print(flat, teb.FlatTmpl)
		time.Sleep(time.Second / 2)
	}

	// execute
	if id, err = api.StartDSort(apiBP, &spec); err == nil {
		fmt.Fprintln(c.App.Writer, id)
	}
	return
}

// with minor editing
func _flattenSpec(spec *dsort.RequestSpec) (flat nvpairList) {
	var src, dst cmn.Bck
	cmn.IterFields(spec, func(tag string, field cmn.IterField) (error, bool) {
		v := _toStr(field.Value())
		switch {
		case tag == "bck.name":
			src.Name = v
		case tag == "bck.provider":
			src.Provider = v
		case tag == "output_bck.name":
			dst.Name = v
		case tag == "output_bck.provider":
			dst.Provider = v
		default:
			flat = append(flat, nvpair{tag, v})
		}
		return nil, false
	})
	if dst.IsEmpty() {
		dst = src
	}
	flat = append(flat, nvpair{"bck", src.Cname("")}, nvpair{"output_bck", dst.Cname("")})
	sort.Slice(flat, func(i, j int) bool {
		di, dj := flat[i], flat[j]
		return di.Name < dj.Name
	})
	return
}

// Creates bucket if not exists. If exists uses it or deletes and creates new
// one if cleanup flag was set.
func setupBucket(c *cli.Context, bck cmn.Bck) error {
	exists, err := api.QueryBuckets(apiBP, cmn.QueryBcks(bck), apc.FltPresent)
	if err != nil {
		return V(err)
	}

	cleanup := flagIsSet(c, cleanupFlag)
	if exists && cleanup {
		if err := api.DestroyBucket(apiBP, bck); err != nil {
			return V(err)
		}
	}

	if !exists || cleanup {
		if err := api.CreateBucket(apiBP, bck, nil); err != nil {
			return V(err)
		}
	}

	return nil
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
		p:           mpb.New(mpb.WithWidth(barWidth)),
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
			return dsortResult{}, V(err)
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
		return false, V(err)
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
				text := strings.ToUpper(phaseName[:1]) + phaseName[1:] + " phase: "
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

func printMetrics(w io.Writer, jobID string, daemonIds []string) (aborted, finished bool, errV error) {
	resp, err := api.MetricsDSort(apiBP, jobID)
	if err != nil {
		return false, false, V(err)
	}
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

	// compute `aborted` and `finished`
	finished = true
	for _, targetMetrics := range resp {
		aborted = aborted || targetMetrics.Aborted.Load()
		finished = finished && targetMetrics.Creation.Finished
	}
	// NOTE: because of the still-open issue we are using Go-standard json, not jsoniter
	// https://github.com/json-iterator/go/issues/331
	b, err := jsonStd.MarshalIndent(resp, "", "    ")
	if err != nil {
		return false, false, err
	}
	fmt.Fprintln(w, string(b))
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
		return V(err)
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
		return teb.Print(list, teb.DSortListNoHdrTmpl, teb.Jopts(usejs))
	}

	return teb.Print(list, teb.DSortListTmpl, teb.Jopts(usejs))
}

func dsortJobStatus(c *cli.Context, id string) error {
	var (
		verbose = flagIsSet(c, verboseFlag)
		refresh = flagIsSet(c, refreshFlag)
		logging = flagIsSet(c, dsortLogFlag)
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
		file, err := cos.CreateFile(c.String(dsortLogFlag.Name))
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
