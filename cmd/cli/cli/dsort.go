// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with objects in the cluster
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ext/dsort"
	"github.com/NVIDIA/aistore/xact"
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
	"github.com/vbauerster/mpb/v4"
	"github.com/vbauerster/mpb/v4/decor"
	"gopkg.in/yaml.v2"
)

const (
	dsortExampleJ = `$ ais start dsort '{
			"input_extension": ".tar",
			"input_bck": {"name": "dsort-testing"},
			"input_format": {"template": "shard-{0..9}"},
			"output_shard_size": "200KB",
			"description": "pack records into categorized shards",
			"order_file": "http://website.web/static/order_file.txt",
			"order_file_sep": " "
		}'`
	dsortExampleY = `$ ais start dsort -f - <<EOM
			input_extension: .tar
			input_bck:
			    name: dsort-testing
			input_format:
			    template: shard-{0..9}
			output_format: new-shard-{0000..1000}
			output_shard_size: 10KB
			description: shuffle shards from 0 to 9
			algorithm:
			    kind: shuffle
			EOM`
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

var dsortStartCmd = cli.Command{
	Name: cmdDsort,
	Usage: "start " + apc.ActDsort + " job\n" +
		indent1 + "Required parameters:\n" +
		indent1 + "\t- input_bck: source bucket (used as both source and destination if the latter is not specified)\n" +
		indent1 + "\t- input_format: see docs and examples below\n" +
		indent1 + "\t- output_format: ditto\n" +
		indent1 + "\t- output_shard_size: (as the name implies)\n" +
		indent1 + "E.g. inline JSON spec:\n" +
		indent4 + "\t  " + dsortExampleJ + "\n" +
		indent1 + "E.g. inline YAML spec:\n" +
		indent4 + "\t  " + dsortExampleY + "\n" +
		indent1 + "Tip: use '--dry-run' to see the results without making any changes\n" +
		indent1 + "Tip: use '--verbose' to print the spec (with all its parameters including applied defaults)\n" +
		indent1 + "See also: docs/dsort.md, docs/cli/dsort.md, and ais/test/scripts/dsort*",
	ArgsUsage: dsortSpecArgument,
	Flags:     startSpecialFlags[cmdDsort],
	Action:    startDsortHandler,
}

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
		spec.InputBck = srcbck
	}
	if !dstbck.IsEmpty() {
		spec.OutputBck = dstbck
	}

	if flagIsSet(c, verboseFlag) {
		flat, config := _flattenSpec(&spec)
		if flagIsSet(c, noHeaderFlag) {
			err = teb.Print(flat, teb.PropValTmplNoHdr)
		} else {
			err = teb.Print(flat, teb.PropValTmpl)
		}
		if err != nil {
			actionWarn(c, err.Error())
		}
		if len(config) == 0 {
			fmt.Fprintln(c.App.Writer, "Config override:\t\t none")
		} else {
			fmt.Fprintln(c.App.Writer, "Config override:")
			if flagIsSet(c, noHeaderFlag) {
				_ = teb.Print(config, teb.PropValTmplNoHdr)
			} else {
				_ = teb.Print(config, teb.PropValTmpl)
			}
		}
		briefPause(1)
		fmt.Fprintln(c.App.Writer)
	}

	// execute
	if id, err = api.StartDsort(apiBP, &spec); err == nil {
		fmt.Fprintln(c.App.Writer, id)
	}
	return
}

// with minor editing
func _flattenSpec(spec *dsort.RequestSpec) (flat, config nvpairList) {
	var src, dst cmn.Bck
	cmn.IterFields(spec, func(tag string, field cmn.IterField) (error, bool) {
		v := _toStr(field.Value())
		// add config override in a 2nd pass
		if tag[0] == '.' {
			if v != "" && v != "0" && v != "0s" {
				config = append(config, nvpair{tag, v})
			}
			return nil, false
		}
		switch {
		case tag == "input_bck.name":
			src.Name = v
		case tag == "input_bck.provider":
			src.Provider = v
		case tag == "output_bck.name":
			dst.Name = v
		case tag == "output_bck.provider":
			dst.Provider = v
		default:
			// defaults
			switch tag {
			case "input_format.objnames":
				if v == `[]` {
					v = teb.NotSetVal
				}
			case "algorithm.kind":
				if v == "" {
					v = dsort.Alphanumeric
				}
			case "order_file_sep":
				if v == "" {
					v = `\t`
				}
			default:
				if v == "" {
					v = teb.NotSetVal
				}
			}
			flat = append(flat, nvpair{tag, v})
		}
		return nil, false
	})
	if dst.IsEmpty() {
		dst = src
	}
	flat = append(flat, nvpair{"input_bck", src.Cname("")}, nvpair{"output_bck", dst.Cname("")})
	sort.Slice(flat, func(i, j int) bool {
		di, dj := flat[i], flat[j]
		return di.Name < dj.Name
	})
	if len(config) > 0 {
		sort.Slice(config, func(i, j int) bool {
			di, dj := config[i], config[j]
			return di.Name < dj.Name
		})
	}
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
		return apc.ActDsort + " job was aborted"
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

func newDsortPB(baseParams api.BaseParams, id string, refreshTime time.Duration) *dsortPB {
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

		jmetrics, err := api.MetricsDsort(b.apiBP, b.id)
		if err != nil {
			b.cleanBars()
			return dsortResult{}, V(err)
		}

		var targetMetrics *dsort.JobInfo
		for _, targetMetrics = range jmetrics {
			break
		}

		if targetMetrics.Metrics.Aborted.Load() {
			b.aborted = true
			break
		}

		b.warnings, b.errors = make([]string, 0), make([]string, 0)
		for _, targetMetrics := range jmetrics {
			b.warnings = append(b.warnings, targetMetrics.Metrics.Warnings...)
			b.errors = append(b.errors, targetMetrics.Metrics.Errors...)
		}
		finished = b.updateBars(jmetrics)
	}

	b.cleanBars()
	return b.result(), nil
}

func (b *dsortPB) start() (bool, error) {
	metrics, err := api.MetricsDsort(b.apiBP, b.id)
	if err != nil {
		return false, V(err)
	}
	finished := b.updateBars(metrics)
	return finished, nil
}

func (b *dsortPB) updateBars(jmetrics map[string]*dsort.JobInfo) bool {
	var (
		targetMetrics *dsort.JobInfo
		finished      = true
		phases        = newPhases()
	)

	for _, targetMetrics = range jmetrics {
		phases[dsort.ExtractionPhase].progress += targetMetrics.Metrics.Extraction.ExtractedCnt

		phases[dsort.SortingPhase].progress = max(phases[dsort.SortingPhase].progress, targetMetrics.Metrics.Sorting.RecvStats.Count)

		phases[dsort.CreationPhase].progress += targetMetrics.Metrics.Creation.CreatedCnt
		phases[dsort.CreationPhase].total += targetMetrics.Metrics.Creation.ToCreate

		finished = finished && targetMetrics.Metrics.Creation.Finished
	}

	phases[dsort.ExtractionPhase].total = targetMetrics.Metrics.Extraction.TotalCnt
	phases[dsort.SortingPhase].total = int64(math.Log2(float64(len(jmetrics))))

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

	b.dur = targetMetrics.Metrics.Creation.End.Sub(targetMetrics.Metrics.Extraction.Start)
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
	resp, err := api.MetricsDsort(apiBP, jobID)
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
		filterMap := map[string]*dsort.JobInfo{}
		for _, daemonID := range daemonIds {
			filterMap[daemonID] = resp[daemonID]
		}
		resp = filterMap
	}

	// compute `aborted` and `finished`
	finished = true
	for _, targetMetrics := range resp {
		aborted = aborted || targetMetrics.Metrics.Aborted.Load()
		finished = finished && targetMetrics.Metrics.Creation.Finished
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

func printCondensedStats(c *cli.Context, id, units string, errhint bool) error {
	resp, err := api.MetricsDsort(apiBP, id)
	if err != nil {
		return V(err)
	}

	// aggregate
	var jobInfo dsort.JobInfo
	for _, job := range resp {
		if jobInfo.ID == "" {
			jobInfo = *job
			continue
		}
		jobInfo.Aggregate(job)
	}
	opts := teb.Opts{AltMap: teb.FuncMapUnits(units)}
	err = teb.Print([]*dsort.JobInfo{&jobInfo}, teb.DsortListTmpl, opts)
	if err != nil {
		return err
	}

	var (
		elapsedTime    time.Duration
		extractionTime time.Duration
		sortingTime    time.Duration
		creationTime   time.Duration
		description    string
		aborted        bool
		finished       = true
	)
	for _, jmetrics := range resp {
		tm := jmetrics.Metrics
		if description != "" {
			if description != tm.Description {
				fmt.Fprintf(c.App.ErrWriter, "dsort[%s] has two descriptions? (%q, %q)\n", id, description, tm.Description)
			}
		} else {
			description = tm.Description
		}
		aborted = aborted || tm.Aborted.Load()
		finished = finished && tm.Creation.Finished

		elapsedTime = max(elapsedTime, tm.ElapsedTime())
		if tm.Extraction.Finished {
			extractionTime = max(extractionTime, tm.Extraction.End.Sub(tm.Extraction.Start))
		} else {
			extractionTime = max(extractionTime, time.Since(tm.Extraction.Start))
		}

		if tm.Sorting.Finished {
			sortingTime = max(sortingTime, tm.Sorting.End.Sub(tm.Sorting.Start))
		} else if tm.Sorting.Running {
			sortingTime = max(sortingTime, time.Since(tm.Sorting.Start))
		}

		if tm.Creation.Finished {
			creationTime = max(creationTime, tm.Creation.End.Sub(tm.Creation.Start))
		} else if tm.Creation.Running {
			creationTime = max(creationTime, time.Since(tm.Creation.Start))
		}
	}

	fmt.Fprintf(c.App.Writer, "DESCRIPTION: [%s]\n", description)
	switch {
	case aborted:
		fmt.Fprintf(c.App.Writer, "dsort[%s] aborted.", id)
		if errhint {
			fmt.Fprintf(c.App.Writer, " For details, run 'ais show job %s -v'\n", id)
		}
	case finished:
		fmt.Fprintf(c.App.Writer, "dsort[%s] successfully finished in %v:\n"+
			indent1+"Longest extraction:\t%v\n"+
			indent1+"Longest sorting:\t%v\n"+
			indent1+"Longest creation:\t%v\n",
			id, elapsedTime, extractionTime, sortingTime, creationTime,
		)
	default:
		fmt.Fprintf(c.App.Writer, "dsort[%s] is currently running:\n"+
			id, indent1+"Extraction:\t%v", extractionTime)
		if sortingTime.Seconds() > 0 {
			fmt.Fprintln(c.App.Writer)
			fmt.Fprintf(c.App.Writer, indent1+"Sorting:\t%v", sortingTime)
		}
		if creationTime.Seconds() > 0 {
			fmt.Fprintln(c.App.Writer)
			fmt.Fprintf(c.App.Writer, indent1+"Creation:\t%v", creationTime)
		}
	}
	fmt.Fprintln(c.App.Writer)
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

	var (
		hideHeader  = flagIsSet(c, noHeaderFlag)
		units, errU = parseUnitsFlag(c, unitsFlag)
		opts        = teb.Opts{AltMap: teb.FuncMapUnits(units), UseJSON: usejs}
		verbose     = flagIsSet(c, verboseJobFlag)
	)
	debug.AssertNoErr(errU)

	if !verbose {
		if hideHeader {
			return teb.Print(list, teb.DsortListNoHdrTmpl, opts)
		}
		return teb.Print(list, teb.DsortListTmpl, opts)
	}
	// verbose - one at a time, each with header and an extra
	for _, j := range list {
		if err := teb.Print([]*dsort.JobInfo{j}, teb.DsortListVerboseTmpl, opts); err != nil {
			return err
		}
		// super-verbose
		if cliConfVerbose() {
			xargs := xact.ArgsMsg{ID: j.ID, Kind: apc.ActDsort}
			if _, err := xactList(c, &xargs, false /*caption*/); err != nil {
				return err
			}
		}
		fmt.Fprintln(c.App.Writer)
	}
	return nil
}

func dsortJobStatus(c *cli.Context, id string) error {
	var (
		verbose     = flagIsSet(c, verboseJobFlag)
		refresh     = flagIsSet(c, refreshFlag)
		logging     = flagIsSet(c, dsortLogFlag)
		usejs       = flagIsSet(c, jsonFlag)
		units, errU = parseUnitsFlag(c, unitsFlag)
	)
	debug.AssertNoErr(errU)

	// Show progress bar.
	if !verbose && refresh && !logging {
		refreshRate := _refreshRate(c)
		dsortResult, err := newDsortPB(apiBP, id, refreshRate).run()
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
			if false { // TODO: revisit
				daemonIds = append(daemonIds, c.Args().Get(1))
			}
			if _, _, err := printMetrics(c.App.Writer, id, daemonIds); err != nil {
				return err
			}

			fmt.Fprintf(c.App.Writer, "\n")
			return printCondensedStats(c, id, units, false)
		}
		return printCondensedStats(c, id, units, true)
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
	return printCondensedStats(c, id, units, false)
}
