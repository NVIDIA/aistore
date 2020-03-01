// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that list cluster metadata information.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"strings"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

var (
	listObjectFlags = []cli.Flag{
		regexFlag,
		templateFlag,
		prefixFlag,
		pageSizeFlag,
		objPropsFlag,
		objLimitFlag,
		showUnmatchedFlag,
		allItemsFlag,
		fastFlag,
		noHeaderFlag,
		pagedFlag,
		maxPagesFlag,
		markerFlag,
		cachedFlag,
	}

	listCmdsFlags = map[string][]cli.Flag{
		commandList:     listObjectFlags,
		subcmdListAIS:   listObjectFlags,
		subcmdListCloud: listObjectFlags,
		subcmdListBckProps: {
			providerFlag,
			jsonFlag,
		},
		subcmdListConfig: {
			jsonFlag,
		},
		subcmdListSmap: {
			jsonFlag,
		},
	}

	// add subcommand names for completion
	listSubcmds = []string{
		subcmdListAIS,
		subcmdListCloud,
		subcmdListBckProps,
		subcmdListConfig,
		subcmdListSmap,
	}

	listCmds = []cli.Command{
		{
			Name:         commandList,
			Usage:        "list cluster metadata information",
			Action:       defaultListHandler,
			ArgsUsage:    listCommandArgument,
			Flags:        listCmdsFlags[commandList],
			BashComplete: listCompletions,
			Subcommands: []cli.Command{
				{
					Name:         subcmdListAIS,
					Usage:        "list ais buckets",
					ArgsUsage:    optionalBucketArgument,
					Flags:        listCmdsFlags[subcmdListAIS],
					Action:       listAISBucketsHandler,
					BashComplete: bucketCompletions([]cli.BashCompleteFunc{}, false /* multiple */, false /* separator */, cmn.ProviderAIS),
				},
				{
					Name:         subcmdListCloud,
					Usage:        "list cloud buckets",
					ArgsUsage:    optionalBucketArgument,
					Flags:        listCmdsFlags[subcmdListCloud],
					Action:       listCloudBucketsHandler,
					BashComplete: bucketCompletions([]cli.BashCompleteFunc{}, false /* multiple */, false /* separator */, cmn.Cloud),
				},
				{
					Name:         subcmdListBckProps,
					Usage:        "list bucket properties",
					ArgsUsage:    bucketArgument,
					Flags:        listCmdsFlags[subcmdListBckProps],
					Action:       listBckPropsHandler,
					BashComplete: bucketCompletions([]cli.BashCompleteFunc{}, false /* multiple */, false /* separator */),
				},
				{
					Name:         subcmdListConfig,
					Usage:        "list daemon configuration",
					ArgsUsage:    listConfigArgument,
					Flags:        listCmdsFlags[subcmdListConfig],
					Action:       listConfigHandler,
					BashComplete: daemonConfigSectionCompletions(false /* daemon optional */, true /* config optional */),
				},
				{
					Name:         subcmdListSmap,
					Usage:        "display an smap copy of a node",
					ArgsUsage:    optionalDaemonIDArgument,
					Flags:        listCmdsFlags[subcmdListSmap],
					Action:       listSmapHandler,
					BashComplete: daemonCompletions(true /* optional */, false /* omit proxies */),
				},
			},
		},
	}
)

// Note: This handler ignores aisBucketEnvVar and aisProviderEnvVar
// because the intention is to list all buckets or auto-detect bucket provider
// for a given bucket.
func defaultListHandler(c *cli.Context) (err error) {
	bck := cmn.Bck{
		Name:     c.Args().First(),
		Provider: "", /* any provider*/
	}
	if bck.Name == "" {
		return listBucketNames(c, bck)
	}

	bck.Name = strings.TrimSuffix(bck.Name, "/")
	return listBucketObj(c, bck)
}

// Note: This handler ignores aisBucketEnvVar because the intention
// is to list ais bucket names if bucket name isn't given.
func listAISBucketsHandler(c *cli.Context) (err error) {
	bck := cmn.Bck{
		Name:     c.Args().First(),
		Provider: cmn.ProviderAIS,
	}
	if bck.Name == "" {
		return listBucketNames(c, bck)
	}

	bck.Name = strings.TrimSuffix(bck.Name, "/")
	return listBucketObj(c, bck)
}

// Note: This handler ignores aisBucketEnvVar because the intention
// is to list cloud bucket names if bucket name isn't given.
func listCloudBucketsHandler(c *cli.Context) (err error) {
	bck := cmn.Bck{
		Name:     c.Args().First(),
		Provider: cmn.Cloud,
	}
	if bck.Name == "" {
		return listBucketNames(c, bck)
	}

	bck.Name = strings.TrimSuffix(bck.Name, "/")
	return listBucketObj(c, bck)
}

func listBckPropsHandler(c *cli.Context) (err error) {
	return listBucketProps(c)
}

func listConfigHandler(c *cli.Context) (err error) {
	if _, err = fillMap(); err != nil {
		return
	}
	return getDaemonConfig(c)
}

func listSmapHandler(c *cli.Context) (err error) {
	var (
		daemonID    = c.Args().First()
		primarySmap *cluster.Smap
	)

	if primarySmap, err = fillMap(); err != nil {
		return
	}

	return clusterSmap(c, primarySmap, daemonID, flagIsSet(c, jsonFlag))
}
