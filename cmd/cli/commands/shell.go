// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles bash completions for the CLI.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmd/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/downloader"
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
		if flag == cli.HelpFlag {
			continue
		}

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

func daemonCompletions(optional, omitProxies bool) cli.BashCompleteFunc {
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

func daemonConfigSectionCompletions(daemonOptional, configOptional bool) cli.BashCompleteFunc {
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
	for _, prop := range cmn.ConfigPropList() {
		if !cmn.AnyHasPrefixInSlice(prop, c.Args()) {
			fmt.Println(prop)
		}
	}
}

////////////
// Bucket //
////////////

type bckCompletionsOpts struct {
	additionalCompletions []cli.BashCompleteFunc
	withProviders         bool
	multiple              bool
	separator             bool
	provider              string
}

// The function lists buckets names if the first argument was not yet given, otherwise it lists flags and additional completions
// Bucket names will also be listed after the first argument was given if true is passed to the 'multiple' param
// Bucket names will contain a path separator '/' if true is passed to the 'separator' param
func bucketCompletions(args ...bckCompletionsOpts) cli.BashCompleteFunc {
	return func(c *cli.Context) {
		var (
			multiple, separator, withProviders bool
			argsProvider                       string
			additionalCompletions              []cli.BashCompleteFunc

			bucketNames []string
			providers   []string
		)

		if len(args) > 0 {
			multiple, separator, withProviders = args[0].multiple, args[0].separator, args[0].withProviders
			argsProvider = args[0].provider
			additionalCompletions = args[0].additionalCompletions
		}

		if c.NArg() >= 1 && !multiple {
			for _, f := range additionalCompletions {
				f(c)
			}
			flagCompletions(c)
			return
		}

		bck := cmn.Bck{
			Provider: bucketProvider(argsProvider),
		}

		if bck.Provider == "" {
			providers = []string{cmn.ProviderAIS, cmn.Cloud}
		} else {
			providers = []string{bck.Provider}
		}

		if withProviders {
			for _, p := range providers {
				bucketNames = append(bucketNames, fmt.Sprintf("%s\\://", p))
			}
		}

		for _, provider := range providers {
			bck.Provider = provider
			buckets, err := api.ListBuckets(defaultAPIParams, bck)
			if err != nil {
				return
			}
			for _, b := range buckets {
				if b.Ns.IsGlobal() {
					bucketNames = append(bucketNames, fmt.Sprintf("%s\\://%s", b.Provider, b.Name))
				} else {
					bucketNames = append(bucketNames, fmt.Sprintf("%s\\://%s/%s", b.Provider, b.Ns, b.Name))
				}
			}
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

		printNotUsedBuckets(bucketNames)
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

		p := ""
		if len(provider) > 0 {
			p = provider[0]
		}
		bucketCompletions(bckCompletionsOpts{separator: separator, provider: p})(c)
	}
}

func propCompletions(c *cli.Context) {
	err := cmn.IterFields(&cmn.BucketPropsToUpdate{}, func(tag string, _ cmn.IterField) (error, bool) {
		if !cmn.AnyHasPrefixInSlice(tag, c.Args()) {
			fmt.Println(tag)
		}
		return nil, false
	})
	cmn.AssertNoErr(err)
}

func bucketAndPropsCompletions(c *cli.Context) {
	if c.NArg() == 0 {
		bucketCompletions()
		return
	} else if c.NArg() == 1 {
		var props []string
		err := cmn.IterFields(cmn.BucketProps{}, func(uniqueTag string, _ cmn.IterField) (err error, b bool) {
			section := strings.Split(uniqueTag, ".")[0]
			props = append(props, section)
			if flagIsSet(c, verboseFlag) {
				props = append(props, uniqueTag)
			}
			return nil, false
		})
		cmn.AssertNoErr(err)
		sort.Strings(props)
		for _, prop := range props {
			fmt.Println(prop)
		}
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
		bucketCompletions(bckCompletionsOpts{separator: true})(c)
		return
	}
	flagCompletions(c)
}

/////////////
// Xaction //
/////////////

func xactionCompletions(cmd string) func(ctx *cli.Context) {
	return func(c *cli.Context) {
		if c.NArg() == 0 {
			for kind, meta := range cmn.XactsMeta {
				if (cmd != cmn.ActXactStart) || (cmd == cmn.ActXactStart && meta.Startable) {
					fmt.Println(kind)
				}
			}
			return
		}

		xactName := c.Args().First()
		if cmn.IsXactTypeBck(xactName) {
			bucketCompletions()(c)
			return
		}
		flagCompletions(c)
	}
}

func xactionDesc(cmd string) string {
	xactKinds := make([]string, 0, len(cmn.XactsMeta))
	for kind, meta := range cmn.XactsMeta {
		if (cmd != cmn.ActXactStart) || (cmd == cmn.ActXactStart && meta.Startable) {
			xactKinds = append(xactKinds, kind)
		}
	}
	sort.Strings(xactKinds)
	return fmt.Sprintf("%s can be one of: %q", xactionArgument, strings.Join(xactKinds, ", "))
}

//////////////////////
// Download / dSort //
//////////////////////

func downloadIDAllCompletions(c *cli.Context) {
	suggestDownloadID(c, func(*downloader.DlJobInfo) bool { return true })
}

func downloadIDRunningCompletions(c *cli.Context) {
	suggestDownloadID(c, (*downloader.DlJobInfo).IsRunning)
}

func downloadIDFinishedCompletions(c *cli.Context) {
	suggestDownloadID(c, (*downloader.DlJobInfo).IsFinished)
}

func suggestDownloadID(c *cli.Context, filter func(*downloader.DlJobInfo) bool) {
	if c.NArg() > 0 {
		flagCompletions(c)
		return
	}

	list, _ := api.DownloadGetList(defaultAPIParams, "")

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

	list, _ := api.ListDSort(defaultAPIParams, "")

	for _, job := range list {
		if filter(job) {
			fmt.Println(job.ID)
		}
	}
}
