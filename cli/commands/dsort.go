// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that interact with objects in the cluster
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/urfave/cli"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
)

const (
	dsortStart  = "start"
	dsortStatus = "status"
	dsortAbort  = "abort"
	dsortRemove = "rm"
	dsortList   = "ls"
)

var (
	phasesOrdered = []string{dsort.ExtractionPhase, dsort.SortingPhase, dsort.CreationPhase}

	dsortIDFlag          = cli.StringFlag{Name: cmn.URLParamID, Usage: "id of the dSort job, eg: '5JjIuGemR'"}
	dsortDescriptionFlag = cli.StringFlag{Name: cmn.URLParamDescription + ",desc", Usage: "description of the job - can be useful when listing all dSort jobs"}
	logFlag              = cli.StringFlag{Name: "log", Usage: "path to file where the metrics will be saved"}

	dsortFlags = map[string][]cli.Flag{
		dsortStart: {
			dsortDescriptionFlag,
		},
		dsortStatus: {
			dsortIDFlag,
			progressBarFlag,
			refreshFlag,
			logFlag,
		},
		dsortAbort: {
			dsortIDFlag,
		},
		dsortRemove: {
			dsortIDFlag,
		},
		dsortList: {
			regexFlag,
		},
	}

	dsortStartUsage  = fmt.Sprintf("%s dsort <json_specification>", cliName)
	dsortStatusUsage = fmt.Sprintf("%s dsort %s --id <value> [STATUS FLAGS...]", cliName, dsortStatus)
	dsortAbortUsage  = fmt.Sprintf("%s dsort %s --id <value>", cliName, dsortAbort)
	dsortRemoveUsage = fmt.Sprintf("%s dsort %s --id <value>", cliName, dsortRemove)
	dsortListUsage   = fmt.Sprintf("%s dsort %s --regex <value>", cliName, dsortList)

	dSortCmds = []cli.Command{
		{
			Name:  "dsort",
			Usage: "command that manages distributed sort jobs",
			Subcommands: []cli.Command{
				{
					Name:         dsortStart,
					Usage:        "start new dSort job with provided specification",
					UsageText:    dsortStartUsage,
					Flags:        dsortFlags[dsortStart],
					Action:       dsortHandler,
					BashComplete: flagList,
				},
				{
					Name:         dsortStatus,
					Usage:        "retrieve statistics and metrics of currently running dSort job",
					UsageText:    dsortStatusUsage,
					Flags:        dsortFlags[dsortStatus],
					Action:       dsortHandler,
					BashComplete: flagList,
				},
				{
					Name:         dsortAbort,
					Usage:        "abort currently running dSort job",
					UsageText:    dsortAbortUsage,
					Flags:        dsortFlags[dsortAbort],
					Action:       dsortHandler,
					BashComplete: flagList,
				},
				{
					Name:         dsortRemove,
					Usage:        "remove finished dSort job from the list",
					UsageText:    dsortRemoveUsage,
					Flags:        dsortFlags[dsortRemove],
					Action:       dsortHandler,
					BashComplete: flagList,
				},
				{
					Name:         dsortList,
					Usage:        "list all dSort jobs and their states",
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

func dsortHandler(c *cli.Context) error {
	var (
		baseParams = cliAPIParams(ClusterURL)
		id         = parseStrFlag(c, idFlag)
		regex      = parseStrFlag(c, regexFlag)
	)

	commandName := c.Command.Name
	switch commandName {
	case dsortStart:
		if c.NArg() == 0 {
			return fmt.Errorf("starting dSort job requires specification in JSON format")
		}

		var rs dsort.RequestSpec
		body := c.Args().First()
		if err := json.Unmarshal([]byte(body), &rs); err != nil {
			return err
		}

		id, err := api.StartDSort(baseParams, rs)
		if err != nil {
			return errorHandler(err)
		}
		fmt.Println(id)
	case dsortStatus:
		if err := checkFlags(c, idFlag); err != nil {
			return err
		}

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

				fmt.Printf("DSort has finished.")
			}
		}
	case dsortAbort:
		if err := checkFlags(c, idFlag); err != nil {
			return err
		}

		if err := api.AbortDSort(baseParams, id); err != nil {
			return errorHandler(err)
		}
		fmt.Printf("DSort job aborted: %s\n", id)
	case dsortRemove:
		if err := checkFlags(c, idFlag); err != nil {
			return err
		}

		if err := api.RemoveDSort(baseParams, id); err != nil {
			return errorHandler(err)
		}
		fmt.Printf("DSort job removed: %s\n", id)
	case dsortList:
		list, err := api.ListDSort(baseParams, regex)
		if err != nil {
			return errorHandler(err)
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
		return "DSort job was aborted."
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
			return dsortResult{}, errorHandler(err)
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
		return false, errorHandler(err)
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
		phases[dsort.ExtractionPhase].progress += targetMetrics.Extraction.SeenCnt

		phases[dsort.SortingPhase].progress = cmn.Max(phases[dsort.SortingPhase].progress, int(targetMetrics.Sorting.RecvStats.Count))

		phases[dsort.CreationPhase].progress += targetMetrics.Creation.CreatedCnt
		phases[dsort.CreationPhase].total += targetMetrics.Creation.ToCreate

		finished = finished && targetMetrics.Creation.Finished
	}

	phases[dsort.ExtractionPhase].progress /= len(metrics)
	phases[dsort.ExtractionPhase].total = targetMetrics.Extraction.ToSeenCnt
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
		return false, errorHandler(err)
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
