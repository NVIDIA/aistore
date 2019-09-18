// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that interact with objects in the cluster
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/urfave/cli"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
	"golang.org/x/sync/errgroup"
)

var (
	phasesOrdered = []string{dsort.ExtractionPhase, dsort.SortingPhase, dsort.CreationPhase}

	dsortFlags = map[string][]cli.Flag{
		dsortGen: {
			extFlag,
			dsortBucketFlag,
			dsortTemplateFlag,
			fileSizeFlag,
			fileCountFlag,
			cleanupFlag,
			concurrencyFlag,
		},
		dsortStart: {},
		dsortStatus: {
			refreshFlag,
			verboseFlag,
			logFlag,
		},
		dsortAbort:  {},
		dsortRemove: {},
		dsortList: {
			regexFlag,
		},
	}

	dSortCmds = []cli.Command{
		{
			Name:  commandDsort,
			Usage: "command that manages distributed sort jobs",
			Subcommands: []cli.Command{
				{
					Name:         dsortGen,
					Usage:        fmt.Sprintf("put randomly generated shards which then can be used for %s testing", cmn.DSortName),
					ArgsUsage:    noArgumentsText,
					Flags:        dsortFlags[dsortGen],
					Action:       dsortHandler,
					BashComplete: flagList,
				},
				{
					Name:         dsortStart,
					Usage:        fmt.Sprintf("start new %s job with provided specification", cmn.DSortName),
					ArgsUsage:    jsonSpecArgumentText,
					Flags:        dsortFlags[dsortStart],
					Action:       dsortHandler,
					BashComplete: flagList,
				},
				{
					Name:         dsortStatus,
					Usage:        fmt.Sprintf("retrieve statistics and metrics of currently running %s job", cmn.DSortName),
					ArgsUsage:    idArgumentText,
					Flags:        dsortFlags[dsortStatus],
					Action:       dsortHandler,
					BashComplete: dsortIDListAll,
				},
				{
					Name:         dsortAbort,
					Usage:        fmt.Sprintf("abort currently running %s job", cmn.DSortName),
					ArgsUsage:    idArgumentText,
					Flags:        dsortFlags[dsortAbort],
					Action:       dsortHandler,
					BashComplete: dsortIDListRunning,
				},
				{
					Name:         dsortRemove,
					Usage:        fmt.Sprintf("remove finished %s job from the list", cmn.DSortName),
					ArgsUsage:    idArgumentText,
					Flags:        dsortFlags[dsortRemove],
					Action:       dsortHandler,
					BashComplete: dsortIDListFinished,
				},
				{
					Name:         dsortList,
					Usage:        fmt.Sprintf("list all %s jobs and their states", cmn.DSortName),
					ArgsUsage:    noArgumentsText,
					Flags:        dsortFlags[dsortList],
					Action:       dsortHandler,
					BashComplete: flagList,
				},
			},
		},
	}
)

func phaseToBarText(phase string) string {
	return strings.Title(phase) + " phase: "
}

func createTar(w io.Writer, ext string, start, end, fileCnt int, fileSize int64) error {
	var (
		gzw       *gzip.Writer
		tw        *tar.Writer
		random    = cmn.NowRand()
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
			Mode:     0664,
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
func setupBucket(c *cli.Context, baseParams *api.BaseParams, bucket string) error {
	cleanup := flagIsSet(c, cleanupFlag)

	exists, err := api.DoesBucketExist(baseParams, bucket)
	if err != nil {
		return err
	}

	if exists && cleanup {
		err := api.DestroyBucket(baseParams, bucket)
		if err != nil {
			return err
		}
	}

	if !exists || cleanup {
		if err := api.CreateBucket(baseParams, bucket); err != nil {
			return err
		}
	}

	return nil
}

func dsortGenHandler(c *cli.Context, baseParams *api.BaseParams) error {
	var (
		ext       = parseStrFlag(c, extFlag)
		bucket    = parseStrFlag(c, dsortBucketFlag)
		template  = parseStrFlag(c, dsortTemplateFlag)
		fileCnt   = parseIntFlag(c, fileCountFlag)
		concLimit = parseIntFlag(c, concurrencyFlag)

		fileSize int64
	)
	fileSize, err := parseByteFlagToInt(c, fileSizeFlag)
	if err != nil {
		return err
	}

	supportedExts := []string{".tar", ".tgz"}
	if !cmn.StringInSlice(ext, supportedExts) {
		return fmt.Errorf("extension is invalid: %s, should be one of: %s", ext, supportedExts)
	}

	mem := &memsys.Mem2{Name: "dsort-cli"}
	if err := mem.Init(false); err != nil {
		return err
	}

	pt, err := cmn.ParseBashTemplate(template)
	if err != nil {
		return err
	}

	if err := setupBucket(c, baseParams, bucket); err != nil {
		return err
	}

	// Progress bar
	text := "Shards created: "
	progress := mpb.New(mpb.WithWidth(progressBarWidth))
	bar := progress.AddBar(
		int64(pt.Count()),
		mpb.PrependDecorators(
			decor.Name(text, decor.WC{W: len(text) + 2, C: decor.DSyncWidthR}),
			decor.CountersNoUnit("%d/%d", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(decor.Percentage(decor.WCSyncWidth)),
	)

	concSemaphore := make(chan struct{}, concLimit)
	group, ctx := errgroup.WithContext(context.Background())
	shardIt := pt.Iter()
	shardNum := 0
CreateShards:
	for shardName, hasNext := shardIt(); hasNext; shardName, hasNext = shardIt() {
		select {
		case concSemaphore <- struct{}{}:
		case <-ctx.Done():
			break CreateShards
		}

		group.Go(func(i int, name string) func() error {
			return func() error {
				defer func() {
					bar.Increment()
					<-concSemaphore
				}()

				name := fmt.Sprintf("%s%s", name, ext)
				sgl := mem.NewSGL(fileSize * int64(fileCnt))
				defer sgl.Free()

				if err := createTar(sgl, ext, i*fileCnt, (i+1)*fileCnt, fileCnt, fileSize); err != nil {
					return err
				}

				putArgs := api.PutObjectArgs{
					BaseParams: baseParams,
					Bucket:     bucket,
					Object:     name,
					Reader:     sgl,
				}
				if err := api.PutObject(putArgs); err != nil {
					return err
				}

				return nil
			}
		}(shardNum, shardName))
		shardNum++
	}

	if err := group.Wait(); err != nil {
		progress.Abort(bar, true)
		return err
	}

	progress.Wait()
	return nil
}

func dsortHandler(c *cli.Context) error {
	var (
		baseParams  = cliAPIParams(ClusterURL)
		id          = c.Args().First()
		regex       = parseStrFlag(c, regexFlag)
		commandName = c.Command.Name
	)

	if commandName == dsortStatus || commandName == dsortAbort || commandName == dsortRemove {
		if c.NArg() < 1 {
			return missingArgumentsError(c, cmn.DSortName+" job ID")
		}
		if id == "" {
			return errors.New(cmn.DSortName + " job ID can't be empty")
		}
	}

	switch commandName {
	case dsortGen:
		return dsortGenHandler(c, baseParams)
	case dsortStart:
		if c.NArg() == 0 {
			return missingArgumentsError(c, "job specification")
		}

		var rs dsort.RequestSpec
		body := c.Args().First()
		if err := json.Unmarshal([]byte(body), &rs); err != nil {
			return err
		}

		id, err := api.StartDSort(baseParams, rs)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintln(c.App.Writer, id)
	case dsortStatus:
		var (
			verbose = flagIsSet(c, verboseFlag)
			refresh = flagIsSet(c, refreshFlag)
			logging = flagIsSet(c, logFlag)
		)

		// Show progress bar.
		if !verbose && refresh && !logging {
			refreshRate, err := calcRefreshRate(c)
			if err != nil {
				return err
			}
			dsortResult, err := newDSortProgressBar(baseParams, id, refreshRate).run()
			if err != nil {
				return err
			}

			_, _ = fmt.Fprintln(c.App.Writer, dsortResult)
			return nil
		}

		// Show metrics just once.
		if !refresh && !logging {
			if verbose {
				if _, _, err := printMetrics(c.App.Writer, baseParams, id); err != nil {
					return err
				}

				_, _ = fmt.Fprintf(c.App.Writer, "\n")
			}

			return printCondensedStats(c.App.Writer, baseParams, id)
		}

		// Show metrics once in a while.
		var (
			w = c.App.Writer
		)

		rate, err := calcRefreshRate(c)
		if err != nil {
			return err
		}

		if logging {
			file, err := cmn.CreateFile(c.String(logFlag.Name))
			if err != nil {
				return err
			}
			w = file
			defer file.Close()
		}

		var (
			aborted  bool
			finished bool
		)

		for {
			aborted, finished, err = printMetrics(w, baseParams, id)
			if err != nil {
				return err
			}
			if aborted || finished {
				break
			}

			time.Sleep(rate)
		}

		_, _ = fmt.Fprintf(c.App.Writer, "\n")
		return printCondensedStats(c.App.Writer, baseParams, id)
	case dsortAbort:
		if err := api.AbortDSort(baseParams, id); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(c.App.Writer, "%s job aborted: %s\n", cmn.DSortName, id)
	case dsortRemove:
		if err := api.RemoveDSort(baseParams, id); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(c.App.Writer, "%s job removed: %s\n", cmn.DSortName, id)
	case dsortList:
		list, err := api.ListDSort(baseParams, regex)
		if err != nil {
			return err
		}

		sort.Slice(list, func(i, j int) bool {
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

		err = templates.DisplayOutput(list, c.App.Writer, templates.DSortListTmpl)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid command name '%s'", commandName)
	}
	return nil
}

type dsortResult struct {
	dur      time.Duration
	created  int
	errors   []string
	warnings []string
	aborted  bool
}

func (d dsortResult) String() string {
	if d.aborted {
		return fmt.Sprintf("%s job was aborted.", cmn.DSortName)
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
	total    int
	progress int
	bar      *mpb.Bar
}

type dsortProgressBar struct {
	id          string
	params      *api.BaseParams
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

func newDSortProgressBar(baseParams *api.BaseParams, id string, refreshTime time.Duration) *dsortProgressBar {
	return &dsortProgressBar{
		id:          id,
		params:      baseParams,
		refreshTime: refreshTime,
		p:           mpb.New(mpb.WithWidth(progressBarWidth)),
		phases:      newPhases(),
		errors:      make([]string, 0),
		warnings:    make([]string, 0),
	}
}

func (b *dsortProgressBar) run() (dsortResult, error) {
	finished, err := b.start()
	if err != nil {
		return dsortResult{}, err
	}
	if finished {
		return b.result(), nil
	}

	for !finished {
		time.Sleep(b.refreshTime)

		metrics, err := api.MetricsDSort(b.params, b.id)
		if err != nil {
			b.cleanBars()
			return dsortResult{}, err
		}

		var targetMetrics *dsort.Metrics
		for _, targetMetrics = range metrics {
			break
		}

		if targetMetrics.Aborted {
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

func (b *dsortProgressBar) start() (bool, error) {
	metrics, err := api.MetricsDSort(b.params, b.id)
	if err != nil {
		return false, err
	}

	finished := b.updateBars(metrics)
	return finished, nil
}

func (b *dsortProgressBar) updateBars(metrics map[string]*dsort.Metrics) bool {
	var (
		targetMetrics *dsort.Metrics
		finished      = true
		phases        = newPhases()
	)

	for _, targetMetrics = range metrics {
		phases[dsort.ExtractionPhase].progress += targetMetrics.Extraction.ExtractedCnt

		phases[dsort.SortingPhase].progress = cmn.Max(phases[dsort.SortingPhase].progress, int(targetMetrics.Sorting.RecvStats.Count))

		phases[dsort.CreationPhase].progress += targetMetrics.Creation.CreatedCnt
		phases[dsort.CreationPhase].total += targetMetrics.Creation.ToCreate

		finished = finished && targetMetrics.Creation.Finished
	}

	phases[dsort.ExtractionPhase].total = targetMetrics.Extraction.TotalCnt
	phases[dsort.SortingPhase].total = int(math.Log2(float64(len(metrics))))

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
					int64(phase.total),
					mpb.PrependDecorators(
						decor.Name(text, decor.WC{W: len(text) + 2, C: decor.DSyncWidthR}),
						decor.CountersNoUnit("%d/%d", decor.WCSyncWidth),
					),
					mpb.AppendDecorators(decor.Percentage(decor.WCSyncWidth)),
				)
			}

			if barPhase.bar != nil && diff > 0 {
				if phase.total > barPhase.total {
					barPhase.bar.SetTotal(int64(phase.total), false)
				}
				barPhase.bar.IncrBy(diff)
			}
		}

		barPhase.progress = phase.progress
		barPhase.total = phase.total
	}

	b.dur = targetMetrics.Creation.End.Sub(targetMetrics.Extraction.Start)
	return finished
}

func (b *dsortProgressBar) cleanBars() {
	for _, phase := range b.phases {
		if phase.bar != nil {
			phase.bar.SetTotal(int64(phase.total), true) // complete the bar
		}
	}
	b.p.Wait()
}

func (b *dsortProgressBar) result() dsortResult {
	var created int
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

func printMetrics(w io.Writer, baseParams *api.BaseParams, id string) (aborted, finished bool, err error) {
	resp, err := api.MetricsDSort(baseParams, id)
	if err != nil {
		return false, false, err
	}

	aborted = false
	finished = true
	for _, targetMetrics := range resp {
		aborted = aborted || targetMetrics.Aborted
		finished = finished && targetMetrics.Creation.Finished
	}

	b, err := json.MarshalIndent(resp, "", "\t")
	if err != nil {
		return false, false, err
	}

	_, err = fmt.Fprintf(w, "%s\n", string(b))
	return
}

func printCondensedStats(w io.Writer, baseParams *api.BaseParams, id string) error {
	resp, err := api.MetricsDSort(baseParams, id)
	if err != nil {
		return err
	}

	var (
		aborted  bool
		finished = true

		elapsedTime    time.Duration
		extractionTime time.Duration
		sortingTime    time.Duration
		creationTime   time.Duration
	)
	for _, tm := range resp {
		aborted = aborted || tm.Aborted
		finished = finished && tm.Creation.Finished

		elapsedTime = cmn.MaxDuration(elapsedTime, tm.ElapsedTime())
		if tm.Extraction.Finished {
			extractionTime = cmn.MaxDuration(extractionTime, tm.Extraction.End.Sub(tm.Extraction.Start))
		} else {
			extractionTime = cmn.MaxDuration(extractionTime, time.Since(tm.Extraction.Start))
		}

		if tm.Sorting.Finished {
			sortingTime = cmn.MaxDuration(sortingTime, tm.Sorting.End.Sub(tm.Sorting.Start))
		} else if tm.Sorting.Running {
			sortingTime = cmn.MaxDuration(sortingTime, time.Since(tm.Sorting.Start))
		}

		if tm.Creation.Finished {
			creationTime = cmn.MaxDuration(creationTime, tm.Creation.End.Sub(tm.Creation.Start))
		} else if tm.Creation.Running {
			creationTime = cmn.MaxDuration(creationTime, time.Since(tm.Creation.Start))
		}
	}

	if aborted {
		_, _ = fmt.Fprintf(w, "DSort job was aborted. Check detailed metrics for encountered errors.\n")
		return nil
	}

	if finished {
		_, _ = fmt.Fprintf(
			w,
			"DSort job has finished successfully in %v:\n  Longest extraction:\t%v\n  Longest sorting:\t%v\n  Longest creation:\t%v\n",
			elapsedTime, extractionTime, sortingTime, creationTime,
		)
		return nil
	}

	_, _ = fmt.Fprintf(w, "DSort job currently running:\n  Extraction:\t%v", extractionTime)
	if sortingTime.Seconds() > 0 {
		_, _ = fmt.Fprintf(w, "\n  Sorting:\t%v", sortingTime)
	}
	if creationTime.Seconds() > 0 {
		_, _ = fmt.Fprintf(w, "\n  Creation:\t%v", creationTime)
	}
	_, _ = fmt.Fprint(w, "\n")
	return nil
}

func dsortJobsList(c *cli.Context, baseParams *api.BaseParams, regex string) error {
	list, err := api.ListDSort(baseParams, regex)
	if err != nil {
		return err
	}

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

	return templates.DisplayOutput(list, c.App.Writer, templates.DSortListTmpl)
}

func dsortJobStatus(c *cli.Context, baseParams *api.BaseParams, id string) error {
	var (
		verbose = flagIsSet(c, verboseFlag)
		refresh = flagIsSet(c, refreshFlag)
		logging = flagIsSet(c, logFlag)
	)

	// Show progress bar.
	if !verbose && refresh && !logging {
		refreshRate, err := calcRefreshRate(c)
		if err != nil {
			return err
		}
		dsortResult, err := newDSortProgressBar(baseParams, id, refreshRate).run()
		if err != nil {
			return err
		}

		fmt.Fprintln(c.App.Writer, dsortResult)
		return nil
	}

	// Show metrics just once.
	if !refresh && !logging {
		if verbose {
			if _, _, err := printMetrics(c.App.Writer, baseParams, id); err != nil {
				return err
			}

			fmt.Fprintf(c.App.Writer, "\n")
		}

		return printCondensedStats(c.App.Writer, baseParams, id)
	}

	// Show metrics once in a while.
	var (
		w = c.App.Writer
	)

	rate, err := calcRefreshRate(c)
	if err != nil {
		return err
	}

	if logging {
		file, err := cmn.CreateFile(c.String(logFlag.Name))
		if err != nil {
			return err
		}
		w = file
		defer file.Close()
	}

	var (
		aborted  bool
		finished bool
	)

	for {
		aborted, finished, err = printMetrics(w, baseParams, id)
		if err != nil {
			return err
		}
		if aborted || finished {
			break
		}

		time.Sleep(rate)
	}

	fmt.Fprintf(c.App.Writer, "\n")
	return printCondensedStats(c.App.Writer, baseParams, id)
}
