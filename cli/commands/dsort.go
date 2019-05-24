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
	"math/rand"
	"os"
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

const (
	dsortGen    = "gen"
	dsortStart  = "start"
	dsortStatus = "status"
	dsortAbort  = "abort"
	dsortRemove = "rm"
	dsortList   = "ls"
)

var (
	phasesOrdered = []string{dsort.ExtractionPhase, dsort.SortingPhase, dsort.CreationPhase}

	logFlag           = cli.StringFlag{Name: "log", Usage: "path to file where the metrics will be saved"}
	extFlag           = cli.StringFlag{Name: "ext", Value: ".tar", Usage: "extension for shards (either '.tar' or '.tgz')"}
	dsortBucketFlag   = cli.StringFlag{Name: "bucket", Value: cmn.DSortNameLowercase + "-testing", Usage: "bucket where shards will be put"}
	dsortTemplateFlag = cli.StringFlag{Name: "template", Value: "shard-{0..9}", Usage: "template of input shard name"}
	fileSizeFlag      = cli.StringFlag{Name: "fsize", Value: "1024", Usage: "single file size inside the shard"}
	fileCountFlag     = cli.IntFlag{Name: "fcount", Value: 5, Usage: "number of files inside single shard"}
	cleanupFlag       = cli.BoolFlag{Name: "cleanup", Usage: "when set, the old bucket will be deleted and created again"}
	concurrencyFlag   = cli.IntFlag{Name: "conc", Value: 10, Usage: "limits number of concurrent put requests and number of concurrent shards created"}

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
			progressBarFlag,
			refreshFlag,
			logFlag,
		},
		dsortAbort:  {},
		dsortRemove: {},
		dsortList: {
			regexFlag,
		},
	}

	dsortGenUsage    = fmt.Sprintf("%s %s %s [FLAGS...]", cliName, cmn.DSortNameLowercase, dsortGen)
	dsortStartUsage  = fmt.Sprintf("%s %s %s <json_specification>", cliName, cmn.DSortNameLowercase, dsortStart)
	dsortStatusUsage = fmt.Sprintf("%s %s %s <id> [STATUS FLAGS...]", cliName, cmn.DSortNameLowercase, dsortStatus)
	dsortAbortUsage  = fmt.Sprintf("%s %s %s <id>", cliName, cmn.DSortNameLowercase, dsortAbort)
	dsortRemoveUsage = fmt.Sprintf("%s %s %s <id>", cliName, cmn.DSortNameLowercase, dsortRemove)
	dsortListUsage   = fmt.Sprintf("%s %s %s --regex <value>", cmn.DSortNameLowercase, cliName, dsortList)

	dSortCmds = []cli.Command{
		{
			Name:  cmn.DSortNameLowercase,
			Usage: "command that manages distributed sort jobs",
			Subcommands: []cli.Command{
				{
					Name:         dsortGen,
					Usage:        fmt.Sprintf("put randomly generated shards which then can be used for %s testing", cmn.DSortName),
					UsageText:    dsortGenUsage,
					Flags:        dsortFlags[dsortGen],
					Action:       dsortHandler,
					BashComplete: flagList,
				},
				{
					Name:         dsortStart,
					Usage:        fmt.Sprintf("start new %s job with provided specification", cmn.DSortName),
					UsageText:    dsortStartUsage,
					Flags:        dsortFlags[dsortStart],
					Action:       dsortHandler,
					BashComplete: flagList,
				},
				{
					Name:         dsortStatus,
					Usage:        fmt.Sprintf("retrieve statistics and metrics of currently running %s job", cmn.DSortName),
					UsageText:    dsortStatusUsage,
					Flags:        dsortFlags[dsortStatus],
					Action:       dsortHandler,
					BashComplete: flagList,
				},
				{
					Name:         dsortAbort,
					Usage:        fmt.Sprintf("abort currently running %s job", cmn.DSortName),
					UsageText:    dsortAbortUsage,
					Flags:        dsortFlags[dsortAbort],
					Action:       dsortHandler,
					BashComplete: flagList,
				},
				{
					Name:         dsortRemove,
					Usage:        fmt.Sprintf("remove finished %s job from the list", cmn.DSortName),
					UsageText:    dsortRemoveUsage,
					Flags:        dsortFlags[dsortRemove],
					Action:       dsortHandler,
					BashComplete: flagList,
				},
				{
					Name:         dsortList,
					Usage:        fmt.Sprintf("list all %s jobs and their states", cmn.DSortName),
					UsageText:    dsortListUsage,
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
		random    = rand.New(rand.NewSource(time.Now().UnixNano()))
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

	exists, err := api.DoesLocalBucketExist(baseParams, bucket)
	if err != nil {
		return err
	}

	if exists && cleanup {
		err := api.DestroyLocalBucket(baseParams, bucket)
		if err != nil {
			return err
		}
	}

	if !exists || cleanup {
		if err := api.CreateLocalBucket(baseParams, bucket); err != nil {
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

	mem := &memsys.Mem2{}
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
			return fmt.Errorf("starting %s job requires specification in JSON format", cmn.DSortName)
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
		fmt.Println(id)
	case dsortStatus:
		showProgressBar := flagIsSet(c, progressBarFlag)
		if showProgressBar {
			refreshRate, err := calcRefreshRate(c)
			if err != nil {
				return err
			}
			dsortResult, err := newDSortProgressBar(baseParams, id, refreshRate).run()
			if err != nil {
				return err
			}

			fmt.Println(dsortResult)
		} else {
			if !flagIsSet(c, refreshFlag) {
				// show metrics just once
				if _, err := showMetrics(baseParams, id, os.Stdout); err != nil {
					return err
				}
			} else {
				// show metrics once in a while
				var (
					w = os.Stdout
				)

				rate, err := calcRefreshRate(c)
				if err != nil {
					return err
				}

				if flagIsSet(c, logFlag) {
					file, err := cmn.CreateFile(c.String(logFlag.Name))
					if err != nil {
						return err
					}
					w = file
					defer file.Close()
				}

				for {
					finished, err := showMetrics(baseParams, id, w)
					if err != nil {
						return err
					}
					if finished {
						break
					}

					time.Sleep(rate)
				}

				fmt.Printf("%s has finished.", cmn.DSortName)
			}
		}
	case dsortAbort:
		if err := api.AbortDSort(baseParams, id); err != nil {
			return err
		}
		fmt.Printf("%s job aborted: %s\n", cmn.DSortName, id)
	case dsortRemove:
		if err := api.RemoveDSort(baseParams, id); err != nil {
			return err
		}
		fmt.Printf("%s job removed: %s\n", cmn.DSortName, id)
	case dsortList:
		list, err := api.ListDSort(baseParams, regex)
		if err != nil {
			return err
		}

		err = templates.DisplayOutput(list, templates.DSortListTmpl)
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

func showMetrics(baseParams *api.BaseParams, id string, w io.Writer) (bool, error) {
	resp, err := api.MetricsDSort(baseParams, id)
	if err != nil {
		return false, err
	}

	finished := true
	for _, targetMetrics := range resp {
		finished = finished && targetMetrics.Creation.Finished
	}

	b, err := json.MarshalIndent(resp, "", "\t")
	if err != nil {
		return false, err
	}

	_, err = fmt.Fprintf(w, "%s\n", string(b))
	if err != nil {
		return false, err
	}
	return finished, nil
}
