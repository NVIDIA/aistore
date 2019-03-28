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
			Flags:        commands.DaemonFlags[cmn.GetWhatSmap],
			BashComplete: commands.DaemonList,
		},
		{
			Name:         cmn.GetWhatDaemonStatus,
			Usage:        "returns status of AIS Daemon",
			Action:       commands.DaemonStatus,
			Flags:        commands.DaemonFlags[cmn.GetWhatDaemonStatus],
			BashComplete: commands.DaemonList,
		},
		{
			Name:         cmn.GetWhatConfig,
			Usage:        "returns config of AIS daemon",
			Action:       commands.GetQueryHandler,
			Flags:        commands.DaemonFlags[cmn.GetWhatConfig],
			BashComplete: commands.DaemonList,
		},
		{
			Name:         cmn.GetWhatStats,
			Usage:        "returns stats of AIS daemon",
			Action:       commands.GetQueryHandler,
			Flags:        commands.DaemonFlags[cmn.GetWhatStats],
			BashComplete: commands.DaemonList,
		},
		{
			Name:    commands.CommandList,
			Aliases: []string{"ls"},
			Usage:   "returns list of AIS Daemons",
			Action:  commands.ListAIS,
			Flags:   commands.DaemonFlags[commands.CommandList],
		},
		// Downloader
		{
			Name:  "download",
			Usage: "allows downloading objects from external source",
			Flags: commands.BaseDownloadFlags,
			Subcommands: []cli.Command{
				{
					Name:         commands.DownloadSingle,
					Usage:        "downloads single object into provided bucket",
					UsageText:    commands.DownloadSingleUsage,
					Flags:        commands.DownloadFlags[commands.DownloadSingle],
					Action:       commands.DownloadHandler,
					BashComplete: commands.FlagList,
				},
				{
					Name:         commands.DownloadRange,
					Usage:        "downloads range of objects specified in template parameter",
					UsageText:    commands.DownloadRangeUsage,
					Flags:        commands.DownloadFlags[commands.DownloadRange],
					Action:       commands.DownloadHandler,
					BashComplete: commands.FlagList,
				},
				{
					Name:         commands.DownloadCloud,
					Usage:        "downloads objects from cloud bucket matching provided prefix and suffix",
					UsageText:    commands.DownloadCloudUsage,
					Flags:        commands.DownloadFlags[commands.DownloadCloud],
					Action:       commands.DownloadHandler,
					BashComplete: commands.FlagList,
				},
				{
					Name:         commands.DownloadStatus,
					Usage:        "fetches status of download job with given id",
					UsageText:    commands.DownloadStatusUsage,
					Flags:        commands.DownloadFlags[commands.DownloadStatus],
					Action:       commands.DownloadAdminHandler,
					BashComplete: commands.FlagList,
				},
				{
					Name:         commands.DownloadCancel,
					Usage:        "cancels download job with given id",
					UsageText:    commands.DownloadCancelUsage,
					Flags:        commands.DownloadFlags[commands.DownloadCancel],
					Action:       commands.DownloadAdminHandler,
					BashComplete: commands.FlagList,
				},
			},
		},
		// Object Commands
		{
			Name:  "object",
			Usage: "commands that interact with objects",
			Flags: commands.BaseObjectFlags,
			Subcommands: []cli.Command{
				{
					Name:         commands.ObjGet,
					Usage:        "gets the object from the specified bucket",
					UsageText:    commands.ObjectGetUsage,
					Flags:        commands.ObjectFlags[commands.ObjGet],
					Action:       commands.ObjectHandler,
					BashComplete: commands.FlagList,
				},
				{
					Name:         commands.ObjPut,
					Usage:        "puts the object to the specified bucket",
					UsageText:    commands.ObjectPutUsage,
					Flags:        commands.ObjectFlags[commands.ObjPut],
					Action:       commands.ObjectHandler,
					BashComplete: commands.FlagList,
				},
				{
					Name:         commands.ObjDel,
					Usage:        "deletes the object from the specified bucket",
					UsageText:    commands.ObjectDelUsage,
					Flags:        commands.ObjectFlags[commands.ObjDel],
					Action:       commands.DeleteObject,
					BashComplete: commands.FlagList,
				},
				{
					Name:         commands.CommandRename,
					Usage:        "renames the local object",
					UsageText:    commands.ObjectRenameUsage,
					Flags:        commands.ObjectFlags[commands.CommandRename],
					Action:       commands.ObjectHandler,
					BashComplete: commands.FlagList,
				},
			},
		},
		// Bucket Commands
		{
			Name:  cmn.URLParamBucket,
			Usage: "commands that interact with objects",
			Flags: commands.BaseBucketFlags,
			Subcommands: []cli.Command{
				{
					Name:         commands.BucketCreate,
					Usage:        "creates the local bucket",
					UsageText:    commands.BucketCreateText,
					Flags:        commands.BucketFlags[commands.BucketCreate],
					Action:       commands.BucketHandler,
					BashComplete: commands.FlagList,
				},
				{
					Name:         commands.BucketDestroy,
					Usage:        "destroys the local bucket",
					UsageText:    commands.BucketDelText,
					Flags:        commands.BucketFlags[commands.BucketDestroy],
					Action:       commands.BucketHandler,
					BashComplete: commands.FlagList,
				},
				{
					Name:         commands.CommandRename,
					Usage:        "renames the local bucket",
					UsageText:    commands.BucketRenameText,
					Flags:        commands.BucketFlags[commands.CommandRename],
					Action:       commands.BucketHandler,
					BashComplete: commands.FlagList,
				},
				{
					Name:         commands.BucketNames,
					Usage:        "returns all bucket names",
					UsageText:    commands.BucketNamesText,
					Flags:        commands.BucketFlags[commands.BucketNames],
					Action:       commands.BucketHandler,
					BashComplete: commands.FlagList,
				},
				{
					Name:         commands.CommandList,
					Usage:        "returns all objects from bucket",
					UsageText:    commands.BucketListText,
					Flags:        commands.BucketFlags[commands.CommandList],
					Action:       commands.ListBucket,
					BashComplete: commands.FlagList,
				},
				{
					Name:         commands.CommandSetProps,
					Usage:        "sets bucket properties",
					UsageText:    commands.BucketPropsText,
					Flags:        commands.BucketFlags[commands.CommandSetProps],
					Action:       commands.SetBucketProps,
					BashComplete: commands.FlagList,
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
