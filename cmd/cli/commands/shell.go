// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles bash completions for the CLI.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/authn"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/xact"
	"github.com/urfave/cli"
)

//////////////////////
// Cluster / Daemon //
//////////////////////

type daemonKindCompletion int

const (
	completeTargets daemonKindCompletion = iota
	completeProxies
	completeAllDaemons
)

var (
	supportedBool = []string{"true", "false"}
	propCmpls     = map[string][]string{
		apc.PropBucketAccessAttrs:             apc.SupportedPermissions(),
		apc.HdrObjCksumType:                   cos.SupportedChecksums(),
		"md_write":                            apc.SupportedWritePolicy,
		"ec.compression":                      apc.SupportedCompression,
		"compression.checksum":                apc.SupportedCompression,
		"rebalance.compression":               apc.SupportedCompression,
		"distributed_sort.compression":        apc.SupportedCompression,
		"distributed_sort.duplicated_records": cmn.SupportedReactions,
		"distributed_sort.ekm_malformed_line": cmn.SupportedReactions,
		"distributed_sort.ekm_missing_key":    cmn.SupportedReactions,
		"distributed_sort.missing_shards":     cmn.SupportedReactions,
		"auth.enabled":                        supportedBool,
		"checksum.enabl_read_range":           supportedBool,
		"checksum.validate_cold_get":          supportedBool,
		"checksum.validate_warm_get":          supportedBool,
		"checksum.validate_obj_move":          supportedBool,
		"ec.enabled":                          supportedBool,
		"fshc.enabled":                        supportedBool,
		"lru.enabled":                         supportedBool,
		"mirror.enabled":                      supportedBool,
		"rebalance.enabled":                   supportedBool,
		"resilver.enabled":                    supportedBool,
		"versioning.enabled":                  supportedBool,
		"replication.on_cold_get":             supportedBool,
		"replication.on_lru_eviction":         supportedBool,
		"replication.on_put":                  supportedBool,
	}
)

// Returns true if the last argument is any of permission constants
func lastValueIsAccess(c *cli.Context) bool {
	if c.NArg() == 0 {
		return false
	}
	lastArg := c.Args()[c.NArg()-1]
	for _, access := range propCmpls[apc.PropBucketAccessAttrs] {
		if access == lastArg {
			return true
		}
	}
	return false
}

// Completes command line with not-yet-used permission constants
func accessCompletions(c *cli.Context) bool {
	typedList := c.Args()
	printed := 0
	for _, access := range propCmpls[apc.PropBucketAccessAttrs] {
		found := false
		for _, typed := range typedList {
			if access == typed {
				found = true
				break
			}
		}
		if !found {
			fmt.Println(access)
		}
	}
	return printed == 0
}

func propValueCompletion(c *cli.Context) bool {
	if c.NArg() == 0 {
		return false
	}
	lastIsAccess := lastValueIsAccess(c)
	if lastIsAccess {
		return accessCompletions(c)
	}
	list, ok := propCmpls[c.Args()[c.NArg()-1]]
	if !ok {
		return false
	}
	for _, val := range list {
		fmt.Println(val)
	}
	return !lastIsAccess
}

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

func daemonConfigSectionCompletions(c *cli.Context) {
	// Daemon and config already given as arguments
	if c.NArg() >= 2 {
		return
	}
	if c.Args().First() == subcmdCLI {
		return
	}
	if c.NArg() == 1 {
		suggestConfigSection(c)
		return
	}

	// No arguments given
	fmt.Println(subcmdCluster)
	fmt.Println(subcmdCLI)
	suggestDaemon(completeAllDaemons)
}

func suggestConfigSection(c *cli.Context) {
	// Daemon already given as argument; suggest only config
	props := cos.NewStringSet()
	err := cmn.IterFields(cmn.ClusterConfig{}, func(uniqueTag string, _ cmn.IterField) (err error, b bool) {
		section := strings.Split(uniqueTag, ".")[0]
		props.Add(section)
		return nil, false
	})
	cos.AssertNoErr(err)

	if c.Args().Get(c.NArg()-1) != subcmdCluster {
		// add node's local config: fspath, network, etc.
		err := cmn.IterFields(cmn.LocalConfig{}, func(uniqueTag string, _ cmn.IterField) (err error, b bool) {
			section := strings.Split(uniqueTag, ".")[0]
			props.Add(section)
			return nil, false
		})
		cos.AssertNoErr(err)
	}
	for prop := range props {
		fmt.Println(prop)
	}
}

func cluConfigCompletions(c *cli.Context) {
	if c.NArg() == 0 {
		suggestDaemon(completeAllDaemons)
	} else {
		suggestUpdatableConfig(c)
	}
}

func suggestDaemon(what daemonKindCompletion) {
	smap, err := api.GetClusterMap(cliAPIParams(clusterURL))
	if err != nil {
		return
	}
	if what != completeTargets {
		for dae := range smap.Pmap {
			fmt.Println(cluster.Pname(dae))
		}
	}
	if what != completeProxies {
		for dae := range smap.Tmap {
			fmt.Println(cluster.Tname(dae))
		}
	}
}

func suggestUpdatableConfig(c *cli.Context) {
	if propValueCompletion(c) {
		return
	}
	scope := apc.Cluster
	if c.NArg() > 0 && !isConfigProp(c.Args().First()) {
		scope = apc.Daemon
	}

	props := append(cmn.ConfigPropList(scope), apc.ActTransient)
	for _, prop := range props {
		if !cos.AnyHasPrefixInSlice(prop, c.Args()) {
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
	// For command "ais bucket ls bck1 bck2" value should be set to 0
	// For command "ais object put file bck1" value should be set to 1
	firstBucketIdx int
}

// The function lists buckets names if the first argument was not yet given, otherwise it lists flags and additional completions
// Buckets will also be listed after the first argument was given if true is passed to the 'multiple' param
// Buckets will contain a path separator '/' if true is passed to the 'separator' param
func bucketCompletions(args ...bckCompletionsOpts) cli.BashCompleteFunc {
	return func(c *cli.Context) {
		var (
			multiple, separator, withProviders bool
			argsProvider                       string
			firstBucketIdx                     int
			additionalCompletions              []cli.BashCompleteFunc

			bucketsToPrint []cmn.Bck
			providers      []string
		)

		if len(args) > 0 {
			multiple, separator, withProviders = args[0].multiple, args[0].separator, args[0].withProviders
			argsProvider = args[0].provider
			additionalCompletions = args[0].additionalCompletions
			firstBucketIdx = args[0].firstBucketIdx
		}

		if c.NArg() > firstBucketIdx && !multiple {
			if propValueCompletion(c) {
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
			config, err := api.GetClusterConfig(defaultAPIParams)
			if err != nil {
				return
			}
			providers = []string{apc.ProviderAIS, apc.ProviderHTTP}
			for provider := range config.Backend.Conf {
				providers = append(providers, provider)
			}
		} else {
			providers = []string{query.Provider}
		}

		for _, provider := range providers {
			query.Provider = provider
			buckets, err := api.ListBuckets(defaultAPIParams, query)
			if err != nil {
				return
			}

			bucketsToPrint = append(bucketsToPrint, buckets...)
		}

		sep := ""
		if separator {
			sep = "/"
		}

		printNotUsedBuckets := func(buckets []cmn.Bck) {
			for _, bckToPrint := range buckets {
				alreadyListed := false
				if multiple {
					for _, argBck := range c.Args() {
						parsedArgBck, err := parseBckURI(c, argBck)
						if err != nil {
							return
						}
						if parsedArgBck.Equal(bckToPrint) {
							alreadyListed = true
							break
						}
					}
				}

				if !alreadyListed {
					var bckStr string
					if bckToPrint.Ns.IsGlobal() {
						bckStr = fmt.Sprintf("%s\\://%s", bckToPrint.Provider, bckToPrint.Name)
					} else {
						bckStr = fmt.Sprintf("%s\\://%s/%s", bckToPrint.Provider, bckToPrint.Ns, bckToPrint.Name)
					}
					fmt.Printf("%s%s\n", bckStr, sep)
				}
			}
		}

		if withProviders {
			for _, p := range providers {
				fmt.Printf("%s\\://\n", p)
			}
		}

		printNotUsedBuckets(bucketsToPrint)
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

func manyBucketsCompletions(additionalCompletions []cli.BashCompleteFunc, firstBckIdx, bucketsCnt int) cli.BashCompleteFunc {
	return func(c *cli.Context) {
		if c.NArg() < firstBckIdx || c.NArg() >= firstBckIdx+bucketsCnt {
			// If before a bucket completion, suggest different.
			for _, f := range additionalCompletions {
				f(c)
			}
		}

		if c.NArg() >= firstBckIdx && c.NArg() < firstBckIdx+bucketsCnt {
			bucketCompletions(bckCompletionsOpts{firstBucketIdx: firstBckIdx, multiple: true})(c)
			return
		}
	}
}

func propCompletions(c *cli.Context) {
	err := cmn.IterFields(&cmn.BucketPropsToUpdate{}, func(tag string, _ cmn.IterField) (error, bool) {
		if !cos.AnyHasPrefixInSlice(tag, c.Args()) {
			fmt.Println(tag)
		}
		return nil, false
	})
	cos.AssertNoErr(err)
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
		cos.AssertNoErr(err)
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

func daemonXactionCompletions(ctx *cli.Context) {
	if ctx.NArg() > 2 {
		return
	}
	xactSet := ctx.NArg() != 0
	xactName := ctx.Args().First()
	if ctx.NArg() == 0 {
		daemonCompletions(completeTargets)(ctx)
	} else {
		smap, err := api.GetClusterMap(cliAPIParams(clusterURL))
		if err != nil {
			return
		}
		if node := smap.GetTarget(ctx.Args().First()); node != nil {
			xactSet = false
			xactName = ctx.Args().Get(1)
		}
	}
	if !xactSet {
		for kind := range xact.Table {
			fmt.Println(kind)
		}
		return
	}
	if xact.IsBckScope(xactName) {
		bucketCompletions()(ctx)
		return
	}
}

func xactionCompletions(cmd string) func(ctx *cli.Context) {
	return func(c *cli.Context) {
		if c.NArg() == 0 {
			for kind, dtor := range xact.Table {
				if (cmd != apc.ActXactStart) || (cmd == apc.ActXactStart && dtor.Startable) {
					fmt.Println(kind)
				}
			}
			return
		}
		xactName := c.Args().First()
		if xact.IsBckScope(xactName) {
			bucketCompletions()(c)
			return
		}
	}
}

func xactionDesc(onlyStartable bool) string {
	xactKinds := listXactions(onlyStartable)
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
	if flagIsSet(c, allJobsFlag) {
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

func roleCluPermCompletions(c *cli.Context) {
	if c.NArg() == 0 {
		return
	}

	cluList, err := api.GetClusterAuthN(authParams, authn.Cluster{})
	if err != nil {
		return
	}

	args := c.Args()
	if c.NArg() > 1 {
		accessCompletions(c)
		return
	}
	for _, clu := range cluList {
		if cos.StringInSlice(clu.ID, args) || cos.StringInSlice(clu.Alias, args) {
			continue
		}
		fmt.Println(cos.Either(clu.Alias, clu.ID))
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
		if cos.StringInSlice(role.Name, args) {
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

	cluList, err := api.GetClusterAuthN(authParams, authn.Cluster{})
	if err != nil {
		return
	}

	for _, clu := range cluList {
		fmt.Println(cos.Either(clu.Alias, clu.ID))
	}
}

func authNConfigPropList() []string {
	propList := []string{}
	emptyCfg := authn.ConfigToUpdate{Server: &authn.ServerConfToUpdate{}}
	cmn.IterFields(emptyCfg, func(tag string, field cmn.IterField) (error, bool) {
		propList = append(propList, tag)
		return nil, false
	})
	return propList
}

func suggestUpdatableAuthNConfig(c *cli.Context) {
	props := authNConfigPropList()
	lastIsProp := c.NArg() != 0
	if c.NArg() != 0 {
		lastVal := c.Args().Get(c.NArg() - 1)
		lastIsProp = cos.StringInSlice(lastVal, props)
	}
	if lastIsProp {
		return
	}

	for _, prop := range props {
		if !cos.AnyHasPrefixInSlice(prop, c.Args()) {
			fmt.Println(prop)
		}
	}
}

func suggestRemote(_ *cli.Context) {
	aisCloudInfo, err := api.GetRemoteAIS(defaultAPIParams)
	if err != nil {
		return
	}
	for uuid, info := range aisCloudInfo {
		fmt.Println(uuid)
		if info.Alias != "" {
			fmt.Println(info.Alias)
		}
	}
}

func cliPropCompletions(c *cli.Context) {
	err := cmn.IterFields(cfg, func(tag string, _ cmn.IterField) (error, bool) {
		if !cos.AnyHasPrefixInSlice(tag, c.Args()) {
			fmt.Println(tag)
		}
		return nil, false
	})
	cos.AssertNoErr(err)
}
