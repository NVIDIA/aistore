// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that interact with cluster xactions
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"net/http"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"

	"github.com/urfave/cli"
)

const (
	lruStart  = cmn.ActXactStart
	lruStop   = cmn.ActXactStop
	lruStatus = "status"
)

var (
	lruCmds = []cli.Command{
		{
			Name:  cmn.ActLRU,
			Usage: "manages LRU",
			Subcommands: []cli.Command{
				{
					Name:   lruStart,
					Usage:  "starts LRU in the whole cluster",
					Action: lruHandler,
				},
				{
					Name:   lruStop,
					Usage:  "stops LRU in the whole cluster",
					Action: lruHandler,
				},
				{
					Name:   lruStatus,
					Usage:  "reports status of LRU for each target",
					Action: lruHandler,
				},
			},
		},
	}
)

func lruHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		command    = c.Command.Name
	)

	lruStatsMap, err := api.MakeXactGetRequest(baseParams, cmn.ActLRU, command, "", false)
	if err != nil && command != lruStatus {
		return err
	}

	switch command {
	case lruStart:
		_, _ = fmt.Fprintln(c.App.Writer, "Started LRU.")
	case lruStop:
		_, _ = fmt.Fprintln(c.App.Writer, "Stopped LRU.")
	case lruStatus:
		if err != nil {
			httpErr, ok := err.(*cmn.HTTPError)
			if !ok || httpErr.Status != http.StatusNotFound {
				return err
			}
			_, _ = fmt.Fprintln(c.App.Writer, "LRU is not running, it has not started yet.")
			return nil
		}

		for key, val := range lruStatsMap {
			if len(val) == 0 {
				continue
			}
			stat := val[0]
			if !stat.EndTime().IsZero() {
				_, _ = fmt.Fprintf(c.App.Writer, "Target %s: LRU not running. Started %s, finished %s, removed %d (%s) objects.\n",
					key, stat.StartTime(), stat.EndTime(), stat.ObjCount(), cmn.B2S(stat.BytesCount(), 1))
			} else {
				_, _ = fmt.Fprintf(c.App.Writer, "Target %s: LRU running. Started %s, removed %d (%s) objects.\n",
					key, stat.StartTime(), stat.ObjCount(), cmn.B2S(stat.BytesCount(), 3))
			}
		}
	}
	return err
}
