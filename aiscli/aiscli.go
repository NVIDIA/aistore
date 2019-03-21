//This file is used as command-line interpreter for AIS
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"fmt"
	"os"
	"sort"

	"github.com/NVIDIA/aistore/aiscli/commands"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

func main() {
	if err := commands.TestAISURL(commands.ClusterURL); err != nil {
		fmt.Printf("Could not connect to AIS cluster: %v\n", err)
		os.Exit(1)
	}
	aisCLI := commands.New()

	aisCLI.Commands = []cli.Command{
		// AIS API Query Commands
		{
			Name:         cmn.GetWhatSmap,
			Usage:        "returns cluster map (Smap)",
			Action:       commands.GetQueryHandler,
			Flags:        []cli.Flag{commands.JSONFlag},
			BashComplete: commands.DaemonList,
		},
		{
			Name:         cmn.GetWhatDaemonStatus,
			Usage:        "returns status of AIS Daemon",
			Action:       commands.DaemonStatus,
			Flags:        []cli.Flag{commands.JSONFlag},
			BashComplete: commands.DaemonList,
		},
		{
			Name:         cmn.GetWhatConfig,
			Usage:        "returns config of AIS daemon",
			Action:       commands.GetQueryHandler,
			Flags:        []cli.Flag{commands.JSONFlag},
			BashComplete: commands.DaemonList,
		},
		{
			Name:         cmn.GetWhatStats,
			Usage:        "returns stats of AIS daemon",
			Action:       commands.GetQueryHandler,
			Flags:        []cli.Flag{commands.JSONFlag},
			BashComplete: commands.DaemonList,
		},
		{
			Name:    "list",
			Aliases: []string{"ls"},
			Usage:   "returns list of AIS Daemons",
			Action:  commands.ListAIS,
			Flags:   []cli.Flag{commands.VerboseFlag},
		},
		// Downloader
		{
			Name:  "download",
			Usage: "allows downloading objects from external source",
			Flags: []cli.Flag{
				commands.BucketFlag,
				commands.BckProviderFlag,
				commands.TimeoutFlag,
			},
			Subcommands: []cli.Command{
				{
					Name:  commands.DownloadSingle,
					Usage: "downloads single object into provided bucket",
					Flags: []cli.Flag{
						commands.LinkFlag,
						commands.ObjNameFlag,
					},
					Action: commands.DownloadHandler,
				},
				{
					Name:  commands.DownloadRange,
					Usage: "downloads range of objects specified in template parameter",
					Flags: []cli.Flag{
						commands.BaseFlag,
						commands.TemplateFlag,
					},
					Action: commands.DownloadHandler,
				},
				{
					Name:  commands.DownloadStatus,
					Usage: "fetches status of download job with given id",
					Flags: []cli.Flag{
						commands.IDFlag,
						commands.ProgressBarFlag,
					},
					Action: commands.DownloadAdminHandler,
				},
				{
					Name:  commands.DownloadCancel,
					Usage: "cancels download job with given id",
					Flags: []cli.Flag{
						commands.IDFlag,
					},
					Action: commands.DownloadAdminHandler,
				},
			},
		},
		// Object Commands
		{
			Name:  "object",
			Usage: "commands that interact with objects",
			Flags: []cli.Flag{commands.BckProviderFlag},
			Subcommands: []cli.Command{
				{
					Name:      commands.ObjGet,
					Usage:     "gets the object from the specified bucket",
					UsageText: fmt.Sprintf(commands.ObjectGetPutUsage, commands.ObjGet),
					Flags:     commands.ObjectFlag[commands.ObjGet],
					Action:    commands.ObjectHandler,
				},
				{
					Name:      commands.ObjPut,
					Usage:     "puts the object to the specified bucket",
					UsageText: fmt.Sprintf(commands.ObjectGetPutUsage, commands.ObjPut),
					Flags:     commands.ObjectFlag[commands.ObjPut],
					Action:    commands.ObjectHandler,
				},
				{
					Name:      commands.ObjDel,
					Usage:     "deletes the object from the specified bucket",
					UsageText: fmt.Sprintf(commands.ObjectDelUsage, commands.ObjDel),
					Flags:     commands.ObjectFlag[commands.ObjDel],
					Action:    commands.DeleteObject,
				},
				{
					Name:      commands.ObjRename,
					Usage:     "renames a local object",
					UsageText: fmt.Sprintf(commands.ObjectRenameUsage, commands.ObjRename),
					Flags:     commands.ObjectFlag[commands.ObjRename],
					Action:    commands.ObjectHandler,
				},
			},
		},
	}

	sort.Sort(cli.CommandsByName(aisCLI.Commands))
	err := aisCLI.Run(os.Args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
