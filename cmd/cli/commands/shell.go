// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles bash completions for the CLI.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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

//////////////////////
// Cluster / Daemon //
//////////////////////

type daemonKindCompletion = int

const (
	completeTargets daemonKindCompletion = iota
	completeProxies
	completeAllDaemons
)

func daemonCompletions(what daemonKindCompletion) cli.BashCompleteFunc {
	return func(c *cli.Context) {
		if c.Command.Name != subcmdDsort && c.NArg() >= 1 {
			// Daemon already given as argument
			if c.NArg() >= 1 {
				return
			}
		}
		suggestDaemon(what)
	}
}

func daemonConfigSectionCompletions(daemonOptional bool) cli.BashCompleteFunc {
	return func(c *cli.Context) {
		// Daemon and config already given as arguments
		if c.NArg() >= 2 {
			return
		}

		// Daemon already given as argument
		if c.NArg() == 1 {
			suggestConfigSection()
			return
		}

		// No arguments given
		suggestDaemon(completeAllDaemons)
		if daemonOptional {
			suggestConfigSection()
		}
	}
}

func setConfigCompletions(c *cli.Context) {
	if c.NArg() == 0 {
		suggestDaemon(completeAllDaemons)
	}
	suggestUpdatableConfig(c)
}

func suggestDaemon(what daemonKindCompletion) {
	smap, err := api.GetClusterMap(cliAPIParams(clusterURL))
	if err != nil {
		return
	}
	if what != completeTargets {
		for dae := range smap.Pmap {
			fmt.Println(dae)
		}
	}
	if what != completeProxies {
		for dae := range smap.Tmap {
			fmt.Println(dae)
		}
	}
}

func suggestConfigSection() {
	for k := range templates.ConfigSectionTmpl {
		fmt.Println(k)
	}
}

func suggestUpdatableConfig(c *cli.Context) {
	props := append(cmn.ConfigPropList(), cmn.ActTransient)
	for _, prop := range props {
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

	// Index in args array where first bucket name is.
	// For command "ais ls bck1 bck2" value should be set to 0
	// For command "ais put file bck1" value should be set to 1
	firstBucketIdx int
}

// The function lists buckets names if the first argument was not yet given, otherwise it lists flags and additional completions
// Bucket names will also be listed after the first argument was given if true is passed to the 'multiple' param
// Bucket names will contain a path separator '/' if true is passed to the 'separator' param
func bucketCompletions(args ...bckCompletionsOpts) cli.BashCompleteFunc {
	return func(c *cli.Context) {
		var (
			multiple, separator, withProviders bool
			argsProvider                       string
			firstBucketIdx                     int
			additionalCompletions              []cli.BashCompleteFunc

			bucketNames []string
			providers   []string
		)

		if len(args) > 0 {
			multiple, separator, withProviders = args[0].multiple, args[0].separator, args[0].withProviders
			argsProvider = args[0].provider
			additionalCompletions = args[0].additionalCompletions
			firstBucketIdx = args[0].firstBucketIdx
		}

		if c.NArg() > firstBucketIdx && !multiple {
			if c.Args()[c.NArg()-1] == cmn.HeaderObjCksumType {
				checksums := cmn.SupportedChecksums()
				for _, tag := range checksums {
					if !cmn.AnyHasPrefixInSlice(tag, c.Args()) {
						fmt.Println(tag)
					}
				}
				return
			}
			for _, f := range additionalCompletions {
				f(c)
			}
			return
		}

		query := cmn.QueryBcks{
			Provider: argsProvider,
		}

		if query.Provider == "" {
			providers = []string{cmn.ProviderAIS, cmn.AnyCloud}
		} else {
			providers = []string{query.Provider}
		}

		if withProviders {
			for _, p := range providers {
				bucketNames = append(bucketNames, fmt.Sprintf("%s\\://", p))
			}
		}

		for _, provider := range providers {
			query.Provider = provider
			buckets, err := api.ListBuckets(defaultAPIParams, query)
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
		// Waiting for file|directory as first arg
		return
	}
	if c.NArg() == 1 {
		bucketCompletions(bckCompletionsOpts{separator: true, firstBucketIdx: 1 /* bucket arg after file arg*/})(c)
		return
	}
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
	suggestDownloadID(c, (*downloader.DlJobInfo).JobRunning)
}

func downloadIDFinishedCompletions(c *cli.Context) {
	suggestDownloadID(c, (*downloader.DlJobInfo).JobFinished)
}

func suggestDownloadID(c *cli.Context, filter func(*downloader.DlJobInfo) bool) {
	if c.NArg() > 0 {
		return
	}

	list, _ := api.DownloadGetList(defaultAPIParams, "")
	for _, job := range list {
		if filter(job) {
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
		return
	}

	list, _ := api.ListDSort(defaultAPIParams, "")

	for _, job := range list {
		if filter(job) {
			fmt.Println(job.ID)
		}
	}
}

func isClusterID(cluList []*cmn.AuthCluster, id string) bool {
	if id == "" {
		return false
	}
	for _, clu := range cluList {
		if clu.ID == id || clu.Alias == id {
			return true
		}
	}
	return false
}

func roleCluPermCompletions(c *cli.Context) {
	if c.NArg() == 0 {
		return
	}

	cluList, err := api.GetClusterAuthN(authParams, cmn.AuthCluster{})
	if err != nil {
		return
	}

	args := c.Args()
	last := c.Args().Get(c.NArg() - 1)
	if isClusterID(cluList, last) {
		for _, perm := range []string{"no", "ro", "rw", "admin"} {
			fmt.Println(perm)
		}
		return
	}
	for _, clu := range cluList {
		if cmn.StringInSlice(clu.ID, args) || cmn.StringInSlice(clu.Alias, args) {
			continue
		}
		fmt.Println(cmn.Either(clu.Alias, clu.ID))
	}
}

func oneRoleCompletions(c *cli.Context) {
	if c.NArg() > 0 {
		return
	}

	roleList, err := api.GetRolesAuthN(authParams)
	if err != nil {
		return
	}

	for _, role := range roleList {
		fmt.Println(role.Name)
	}
}

func multiRoleCompletions(c *cli.Context) {
	if c.NArg() < 2 {
		return
	}

	roleList, err := api.GetRolesAuthN(authParams)
	if err != nil {
		return
	}

	args := c.Args()[2:]
	for _, role := range roleList {
		if cmn.StringInSlice(role.Name, args) {
			continue
		}
		fmt.Println(role.Name)
	}
}

func oneUserCompletions(c *cli.Context) {
	if c.NArg() > 0 {
		return
	}

	userList, err := api.GetUsersAuthN(authParams)
	if err != nil {
		return
	}

	for _, user := range userList {
		fmt.Println(user.ID)
	}
}

func oneClusterCompletions(c *cli.Context) {
	if c.NArg() > 0 {
		return
	}

	cluList, err := api.GetClusterAuthN(authParams, cmn.AuthCluster{})
	if err != nil {
		return
	}

	for _, clu := range cluList {
		fmt.Println(cmn.Either(clu.Alias, clu.ID))
	}
}
