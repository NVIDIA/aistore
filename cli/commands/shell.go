// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles bash completions for the CLI
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/urfave/cli"
)

func suggestDaemon(omitProxies bool) {
	smap, err := api.GetClusterMap(cliAPIParams(ClusterURL))
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
		flagList(c)
	}
}

func suggestUpdatableConfig(c *cli.Context) {
	for prop, readonly := range cmn.ConfigPropList {
		if !readonly && !cmn.AnyHasPrefixInSlice(prop, c.Args()) {
			fmt.Println(prop)
		}
	}
}

func daemonSuggestions(optional bool, omitProxies bool) cli.BashCompleteFunc {
	return func(c *cli.Context) {
		// daemon already given as argument
		if c.NArg() >= 1 {
			flagList(c)
			return
		}

		suggestDaemon(omitProxies)

		if optional {
			flagList(c)
		}
	}
}

func daemonConfigSectionSuggestions(daemonOptional bool, configOptional bool) cli.BashCompleteFunc {
	return func(c *cli.Context) {
		// daemon and config already given as arguments
		if c.NArg() >= 2 {
			flagList(c)
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

func setConfigSuggestions(c *cli.Context) {
	if c.NArg() == 0 {
		suggestDaemon(false /* omit proxies */)
	}
	suggestUpdatableConfig(c)
	flagList(c)
}

// Returns flags for command
func flagList(c *cli.Context) {
	listFlags(c)
}

func flagListNoBucket(c *cli.Context) {
	listFlags(c, bucketFlag.Name)
}

func listFlags(c *cli.Context, flagsToSkip ...string) {
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

// The function lists available providers if the first argument of the command was not yet specified.
// Additionally, it will list available flags if the provider argument is optional and wasn't specified,
// or it will list them anyway in case it was.
func providerList(optional bool) cli.BashCompleteFunc {
	return func(c *cli.Context) {
		if c.NArg() < 1 {
			fmt.Printf("%s\n", cmn.ProviderAIS)
			fmt.Printf("%s\n", cmn.ProviderAmazon)
			fmt.Printf("%s\n", cmn.ProviderGoogle)
			if !optional {
				return
			}
		}
		flagList(c)
	}
}

// Function returns the bucket provider either from:
// 1) Function argument provider (highest priority)
// 2) Command line flag --provider
// 3) Environment variable (lowest priority)
func bucketProvider(c *cli.Context, provider ...string) (string, error) {
	bckProvider := ""
	if len(provider) > 0 {
		bckProvider = provider[0]
	}
	if bckProvider == "" {
		bckProvider = parseStrFlag(c, bckProviderFlag)
	}
	if bckProvider == "" {
		bckProvider = os.Getenv(aisBucketProviderEnvVar)
	}
	return cmn.ProviderFromStr(bckProvider)
}

// The function will list bucket names if the first argument to the command was not yet specified, otherwise it will
// list flags and everything that `additionalCompletions` list.
// By default it tries to read `provider` from flag `--provider` or AIS_BUCKET_PROVIDER env variable. If none
// is provided it lists all buckets.
// Optional parameter `provider` can be used to specify which buckets will be listed - only local or only cloud.
func bucketList(additionalCompletions []cli.BashCompleteFunc, multiple bool, separator bool, provider ...string) cli.BashCompleteFunc {
	return func(c *cli.Context) {
		if c.NArg() >= 1 && !multiple {
			for _, f := range additionalCompletions {
				f(c)
			}
			flagList(c)
			return
		}

		bckProvider, err := bucketProvider(c, provider...)
		if err != nil {
			return
		}
		baseParams := cliAPIParams(ClusterURL)
		bucketNames, err := api.GetBucketNames(baseParams, bckProvider)
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

		if cmn.IsProviderAIS(bckProvider) || bckProvider == "" {
			printNotUsedBuckets(bucketNames.AIS)
		}
		if cmn.IsProviderCloud(bckProvider) || bckProvider == "" {
			printNotUsedBuckets(bucketNames.Cloud)
		}
	}
}

func oldAndNewBucketList(additionalCompletions []cli.BashCompleteFunc, separator bool, provider ...string) cli.BashCompleteFunc {
	return func(c *cli.Context) {
		if c.NArg() >= 2 {
			for _, f := range additionalCompletions {
				f(c)
			}
			flagList(c)
			return
		}

		if c.NArg() == 1 {
			return
		}

		// suggest existing buckets

		bckProvider, err := bucketProvider(c, provider...)
		if err != nil {
			return
		}

		baseParams := cliAPIParams(ClusterURL)
		bucketNames, err := api.GetBucketNames(baseParams, bckProvider)
		if err != nil {
			return
		}

		sep := ""
		if separator {
			sep = "/"
		}

		printNotUsedBuckets := func(buckets []string) {
			for _, bucket := range buckets {
				fmt.Printf("%s%s\n", bucket, sep)
			}
		}

		if cmn.IsProviderAIS(bckProvider) || bckProvider == "" {
			printNotUsedBuckets(bucketNames.AIS)
		}
		if cmn.IsProviderCloud(bckProvider) || bckProvider == "" {
			printNotUsedBuckets(bucketNames.Cloud)
		}
	}
}

func xactList(_ *cli.Context) {
	for key := range cmn.XactKind {
		fmt.Println(key)
	}
}

func xactStartCompletions(c *cli.Context) {
	if c.NArg() == 0 {
		xactList(c)
		return
	}

	xactFlagsBasedOnXactType(c)
}

func xactStopStatsCompletions(c *cli.Context) {
	if c.NArg() == 0 {
		xactList(c)
		flagListNoBucket(c)
		return
	}

	xactFlagsBasedOnXactType(c)
}

func xactFlagsBasedOnXactType(c *cli.Context) {
	xactName := c.Args().First()
	if bucketXactions.Contains(xactName) {
		flagList(c)
		return
	}

	// If this is not a bucket xaction then don't suggest `--bucket` flag
	flagListNoBucket(c)
}

func propList(c *cli.Context) {
	for prop, readonly := range cmn.BucketPropList {
		if !readonly && !cmn.AnyHasPrefixInSlice(prop, c.Args()) {
			fmt.Println(prop)
		}
	}
}

func downloadIDListAll(c *cli.Context) {
	downloadIDList(c, func(*cmn.DlJobInfo) bool { return true })
}

func downloadIDListRunning(c *cli.Context) {
	downloadIDList(c, (*cmn.DlJobInfo).IsRunning)
}

func downloadIDListFinished(c *cli.Context) {
	downloadIDList(c, (*cmn.DlJobInfo).IsFinished)
}

func downloadIDList(c *cli.Context, filter func(*cmn.DlJobInfo) bool) {
	if c.NArg() > 0 {
		flagList(c)
		return
	}

	baseParams := cliAPIParams(ClusterURL)

	list, _ := api.DownloadGetList(baseParams, "")

	for _, job := range list {
		if filter(&job) {
			fmt.Println(job.ID)
		}
	}
}

func dsortIDListAll(c *cli.Context) {
	dsortIDList(c, func(*dsort.JobInfo) bool { return true })
}

func dsortIDListRunning(c *cli.Context) {
	dsortIDList(c, (*dsort.JobInfo).IsRunning)
}

func dsortIDListFinished(c *cli.Context) {
	dsortIDList(c, (*dsort.JobInfo).IsFinished)
}

func dsortIDList(c *cli.Context, filter func(*dsort.JobInfo) bool) {
	if c.NArg() > 0 {
		flagList(c)
		return
	}

	baseParams := cliAPIParams(ClusterURL)

	list, _ := api.ListDSort(baseParams, "")

	for _, job := range list {
		if filter(job) {
			fmt.Println(job.ID)
		}
	}
}

func nRequiredArgsCompletions(n int) cli.BashCompleteFunc {
	return func(c *cli.Context) {
		if c.NArg() >= n {
			flagList(c)
		}
	}
}
