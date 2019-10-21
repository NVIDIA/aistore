// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles bash completions for the CLI.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/urfave/cli"
)

/////////////
// General //
/////////////

func noSuggestionCompletions(numArgs int) cli.BashCompleteFunc {
	return func(c *cli.Context) {
		if c.NArg() >= numArgs {
			flagCompletions(c)
		}
	}
}

//////////
// Flag //
//////////

func flagCompletions(c *cli.Context) {
	suggestFlags(c)
}

func suggestFlags(c *cli.Context, flagsToSkip ...string) {
	for _, flag := range c.Command.Flags {
		flagName := cleanFlag(flag.GetName())

		if c.IsSet(flagName) {
			continue
		}
		if cmn.StringInSlice(flagName, flagsToSkip) {
			continue
		}

		fmt.Printf("--%s\n", flagName)
	}
}

//////////////////////
// Cluster / Daemon //
//////////////////////

func daemonCompletions(optional bool, omitProxies bool) cli.BashCompleteFunc {
	return func(c *cli.Context) {
		// daemon already given as argument
		if c.NArg() >= 1 {
			flagCompletions(c)
			return
		}

		suggestDaemon(omitProxies)

		if optional {
			flagCompletions(c)
		}
	}
}

func daemonConfigSectionCompletions(daemonOptional bool, configOptional bool) cli.BashCompleteFunc {
	return func(c *cli.Context) {
		// daemon and config already given as arguments
		if c.NArg() >= 2 {
			flagCompletions(c)
			return
		}

		// daemon already given as argument
		if c.NArg() == 1 {
			suggestConfigSection(c, configOptional)
			return
		}

		// no arguments given
		suggestDaemon(false /* omit proxies */)
		if daemonOptional {
			suggestConfigSection(c, configOptional)
		}
	}
}

func setConfigCompletions(c *cli.Context) {
	if c.NArg() == 0 {
		suggestDaemon(false /* omit proxies */)
	}
	suggestUpdatableConfig(c)
	flagCompletions(c)
}

func suggestDaemon(omitProxies bool) {
	smap, err := api.GetClusterMap(cliAPIParams(clusterURL))
	if err != nil {
		return
	}
	if !omitProxies {
		for dae := range smap.Pmap {
			fmt.Println(dae)
		}
	}
	for dae := range smap.Tmap {
		fmt.Println(dae)
	}
}

func suggestConfigSection(c *cli.Context, optional bool) {
	for k := range templates.ConfigSectionTmpl {
		fmt.Println(k)
	}
	if optional {
		flagCompletions(c)
	}
}

func suggestUpdatableConfig(c *cli.Context) {
	for prop, readonly := range cmn.ConfigPropList {
		if !readonly && !cmn.AnyHasPrefixInSlice(prop, c.Args()) {
			fmt.Println(prop)
		}
	}
}

////////////
// Bucket //
////////////

// The function lists buckets names if the first argument was not yet given, otherwise it lists flags and additional completions
// Bucket names will also be listed after the first argument was given if true is passed to the 'multiple' param
// Bucket names will contain a path separator '/' if true is passed to the 'separator' param
func bucketCompletions(additionalCompletions []cli.BashCompleteFunc, multiple bool, separator bool, provider ...string) cli.BashCompleteFunc {
	return func(c *cli.Context) {
		if c.NArg() >= 1 && !multiple {
			for _, f := range additionalCompletions {
				f(c)
			}
			flagCompletions(c)
			return
		}

		provider, err := bucketProvider(c, provider...)
		if err != nil {
			return
		}
		baseParams := cliAPIParams(clusterURL)
		bucketNames, err := api.GetBucketNames(baseParams, provider)
		if err != nil {
			return
		}

		sep := ""
		if separator {
			sep = "/"
		}

		printNotUsedBuckets := func(buckets []string) {
			for _, bucket := range buckets {
				alreadyListed := false
				if multiple {
					for _, bucketArg := range c.Args() {
						if bucketArg == bucket {
							alreadyListed = true
							break
						}
					}
				}

				if !alreadyListed {
					fmt.Printf("%s%s\n", bucket, sep)
				}
			}
		}

		if cmn.IsProviderAIS(provider) || provider == "" {
			printNotUsedBuckets(bucketNames.AIS)
		}
		if cmn.IsProviderCloud(provider) || provider == "" {
			printNotUsedBuckets(bucketNames.Cloud)
		}
	}
}

// The function lists bucket names for commands that require old and new bucket name
func oldAndNewBucketCompletions(additionalCompletions []cli.BashCompleteFunc, separator bool, provider ...string) cli.BashCompleteFunc {
	return func(c *cli.Context) {
		if c.NArg() >= 2 {
			for _, f := range additionalCompletions {
				f(c)
			}
			flagCompletions(c)
			return
		}

		if c.NArg() == 1 {
			return
		}

		suggestBucket(c, separator, provider...)
	}
}

func propCompletions(c *cli.Context) {
	for prop, readonly := range cmn.BucketPropList {
		if !readonly && !cmn.AnyHasPrefixInSlice(prop, c.Args()) {
			fmt.Println(prop)
		}
	}
}

func suggestBucket(c *cli.Context, separator bool, provider ...string) {
	prov, err := bucketProvider(c, provider...)
	if err != nil {
		return
	}

	baseParams := cliAPIParams(clusterURL)
	bucketNames, err := api.GetBucketNames(baseParams, prov)
	if err != nil {
		return
	}

	sep := ""
	if separator {
		sep = "/"
	}

	printBuckets := func(buckets []string) {
		for _, bucket := range buckets {
			fmt.Printf("%s%s\n", bucket, sep)
		}
	}

	if cmn.IsProviderAIS(prov) || prov == "" {
		printBuckets(bucketNames.AIS)
	}
	if cmn.IsProviderCloud(prov) || prov == "" {
		printBuckets(bucketNames.Cloud)
	}
}

////////////
// Object //
////////////

func putPromoteObjectCompletions(c *cli.Context) {
	if c.NArg() == 0 {
		// waiting for file|directory as first arg
		return
	}
	if c.NArg() == 1 {
		suggestBucket(c, true /* separator */)
		return
	}
	flagCompletions(c)
}

//////////
// List //
//////////

func listCompletions(c *cli.Context) {
	if c.NArg() == 0 {
		for _, subcmd := range listSubcmds {
			fmt.Println(subcmd)
		}
		suggestBucket(c, true /* separator */)
		return
	}
}

/////////////
// Xaction //
/////////////

func xactionCompletions(c *cli.Context) {
	if c.NArg() == 0 {
		for key := range cmn.XactKind {
			fmt.Println(key)
		}
		return
	}

	xactName := c.Args().First()
	if bucketXactions.Contains(xactName) {
		suggestBucket(c, false /* separator */)
		return
	}
	flagCompletions(c)
}

//////////////////////
// Download / dSort //
//////////////////////

func downloadIDAllCompletions(c *cli.Context) {
	suggestDownloadID(c, func(*cmn.DlJobInfo) bool { return true })
}

func downloadIDRunningCompletions(c *cli.Context) {
	suggestDownloadID(c, (*cmn.DlJobInfo).IsRunning)
}

func downloadIDFinishedCompletions(c *cli.Context) {
	suggestDownloadID(c, (*cmn.DlJobInfo).IsFinished)
}

func suggestDownloadID(c *cli.Context, filter func(*cmn.DlJobInfo) bool) {
	if c.NArg() > 0 {
		flagCompletions(c)
		return
	}

	baseParams := cliAPIParams(clusterURL)

	list, _ := api.DownloadGetList(baseParams, "")

	for _, job := range list {
		if filter(&job) {
			fmt.Println(job.ID)
		}
	}
}

func dsortIDAllCompletions(c *cli.Context) {
	suggestDsortID(c, func(*dsort.JobInfo) bool { return true })
}

func dsortIDRunningCompletions(c *cli.Context) {
	suggestDsortID(c, (*dsort.JobInfo).IsRunning)
}

func dsortIDFinishedCompletions(c *cli.Context) {
	suggestDsortID(c, (*dsort.JobInfo).IsFinished)
}

func suggestDsortID(c *cli.Context, filter func(*dsort.JobInfo) bool) {
	if c.NArg() > 0 {
		flagCompletions(c)
		return
	}

	baseParams := cliAPIParams(clusterURL)

	list, _ := api.ListDSort(baseParams, "")

	for _, job := range list {
		if filter(job) {
			fmt.Println(job.ID)
		}
	}
}
