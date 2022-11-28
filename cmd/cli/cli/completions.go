// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles bash completions for the CLI.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/ext/dload"
	"github.com/NVIDIA/aistore/ext/dsort"
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
		feat.FeaturesPropName:                 append(feat.All, NilValue),
		"write_policy.data":                   apc.SupportedWritePolicy,
		"write_policy.md":                     apc.SupportedWritePolicy,
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

func lastValueIsAccess(c *cli.Context) bool {
	return lastValueIs(c, propCmpls[apc.PropBucketAccessAttrs])
}

func lastValueIsFeatures(c *cli.Context) bool {
	return lastValueIs(c, propCmpls[feat.FeaturesPropName])
}

// Returns true if the last arg is any of the enumerated constants
func lastValueIs(c *cli.Context, values []string) bool {
	if c.NArg() == 0 {
		return false
	}
	lastArg := argLast(c)
	for _, v := range values {
		if v == lastArg {
			return true
		}
	}
	return false
}

// Completes command line with not-yet-typed permission constant
func accessCompletions(c *cli.Context) {
	enumCompletions(c, propCmpls[apc.PropBucketAccessAttrs])
}

// Completes command line with not-yet-typed feature constant
func featureCompletions(c *cli.Context) {
	enumCompletions(c, propCmpls[feat.FeaturesPropName])
}

func enumCompletions(c *cli.Context, values []string) {
	typedList := c.Args()
outer:
	for _, v := range values {
		for _, typedV := range typedList {
			if v == typedV {
				continue outer
			}
		}
		fmt.Println(v)
	}
}

func propValueCompletion(c *cli.Context) bool {
	if c.NArg() == 0 {
		return false
	}
	if lastValueIsAccess(c) {
		accessCompletions(c)
		return true
	}
	if lastValueIsFeatures(c) {
		featureCompletions(c)
		return true
	}
	list, ok := propCmpls[argLast(c)]
	if !ok {
		return false
	}
	for _, val := range list {
		fmt.Println(val)
	}
	return true
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

func showConfigCompletions(c *cli.Context) {
	if c.NArg() == 0 {
		fmt.Println(subcmdCluster)
		fmt.Println(subcmdCLI)
		suggestDaemon(completeAllDaemons)
		return
	}
	if c.Args().First() == subcmdCLI {
		return
	}
	if c.Args().First() == subcmdCluster {
		if c.NArg() == 1 {
			configSectionCompletions(c, subcmdCluster)
		}
		return
	}
	if c.NArg() == 1 { // node id only
		fmt.Println(cfgScopeInherited)
		fmt.Println(cfgScopeLocal)
		return
	}
	configSectionCompletions(c, argLast(c))
}

func configSectionCompletions(_ *cli.Context, cfgScope string) {
	var (
		err    error
		config     = &cmn.Config{}
		v      any = &config.ClusterConfig
		props      = cos.NewStrSet()
	)
	if cfgScope == cfgScopeLocal {
		v = &config.LocalConfig
	}
	err = cmn.IterFields(v, func(uniqueTag string, _ cmn.IterField) (err error, b bool) {
		section := strings.Split(uniqueTag, ".")[0]
		props.Add(section)
		return nil, false
	})
	debug.AssertNoErr(err)
	for prop := range props {
		fmt.Println(prop)
	}
}

func setNodeConfigCompletions(c *cli.Context) {
	if c.NArg() == 0 {
		suggestDaemon(completeAllDaemons)
		return
	}
	if c.NArg() == 1 { // node id only
		fmt.Println(cfgScopeInherited)
		fmt.Println(cfgScopeLocal)
		return
	}
	var (
		config     = &cmn.Config{}
		v      any = &config.ClusterConfig
		props      = cos.NewStrSet()
	)
	if c.NArg() == 2 { // node id and scope
		if argLast(c) == cfgScopeLocal {
			v = &config.LocalConfig
		} else if argLast(c) == cfgScopeInherited {
			fmt.Println(subcmdReset)
		}
		err := cmn.IterFields(v, func(uniqueTag string, _ cmn.IterField) (err error, b bool) {
			props.Add(uniqueTag)
			return nil, false
		})
		debug.AssertNoErr(err)
		for prop := range props {
			if !cos.AnyHasPrefixInSlice(prop, c.Args()) {
				fmt.Println(prop)
			}
		}
		return
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
			fmt.Println(cluster.Pname(dae))
		}
	}
	if what != completeProxies {
		for dae := range smap.Tmap {
			fmt.Println(cluster.Tname(dae))
		}
	}
}

func setCluConfigCompletions(c *cli.Context) {
	var (
		config   cmn.Config
		propList = make([]string, 0, 48)
	)
	err := cmn.IterFields(&config.ClusterConfig, func(tag string, _ cmn.IterField) (err error, b bool) {
		propList = append(propList, tag)
		return
	}, cmn.IterOpts{Allowed: apc.Cluster})
	debug.AssertNoErr(err)

	if propValueCompletion(c) {
		return
	}
	for _, prop := range propList {
		if !cos.AnyHasPrefixInSlice(prop, c.Args()) {
			fmt.Println(prop)
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

	props := append(configPropList(scope), apc.ActTransient)
	for _, prop := range props {
		if !cos.AnyHasPrefixInSlice(prop, c.Args()) {
			fmt.Println(prop)
		}
	}
}

//
// Bucket
//

type bcmplop struct {
	additionalCompletions []cli.BashCompleteFunc
	provider              string

	// Index in args array where first bucket name is.
	// For command "ais bucket ls bck1 bck2" value should be set to 0
	// For command "ais object put file bck1" value should be set to 1
	firstBucketIdx int
	multiple       bool
	separator      bool
}

// The function lists buckets names if the first argument was not yet given, otherwise it lists flags and additional completions
// Multiple buckets will also be listed if 'multiple'
// Printed names will end with '/' if 'separator'
func bucketCompletions(opts bcmplop) cli.BashCompleteFunc {
	return func(c *cli.Context) {
		var (
			multiple, separator   bool
			argsProvider          string
			firstBucketIdx        int
			additionalCompletions []cli.BashCompleteFunc
			buckets               []cmn.Bck
		)
		multiple, separator = opts.multiple, opts.separator
		argsProvider = opts.provider
		additionalCompletions = opts.additionalCompletions
		firstBucketIdx = opts.firstBucketIdx
		if c.NArg() > firstBucketIdx && !multiple {
			if propValueCompletion(c) {
				return
			}
			for _, f := range additionalCompletions {
				f(c)
			}
			return
		}

		query := cmn.QueryBcks{Provider: argsProvider}
		buckets, err := api.ListBuckets(apiBP, query, apc.FltPresent) // NOTE: present only
		if err != nil {
			return
		}
		if query.Provider == "" {
			config, err := api.GetClusterConfig(apiBP)
			if err != nil {
				return
			}
			for provider := range config.Backend.Conf {
				if provider == apc.AIS {
					qbck := cmn.QueryBcks{Provider: apc.AIS, Ns: cmn.NsAnyRemote}
					fmt.Println(qbck)
				} else {
					fmt.Printf("%s://\n", apc.ToScheme(provider))
				}
			}
		}
		printNotUsedBuckets(c, buckets, separator, multiple)
	}
}

func printNotUsedBuckets(c *cli.Context, buckets []cmn.Bck, separator, multiple bool) {
	var sep string
	if separator {
		sep = "/"
	}
mloop:
	for _, b := range buckets {
		if multiple {
			for _, argBck := range c.Args() {
				parsedArgBck, err := parseBckURI(c, argBck)
				if err != nil {
					return
				}
				if parsedArgBck.Equal(&b) {
					continue mloop // already listed
				}
			}
		}
		fmt.Printf("%s%s\n", b.DisplayName(), sep)
	}
}

func manyBucketsCompletions(additionalCompletions []cli.BashCompleteFunc, firstBckIdx, bucketsCnt int) cli.BashCompleteFunc {
	return func(c *cli.Context) {
		if c.NArg() < firstBckIdx || c.NArg() >= firstBckIdx+bucketsCnt {
			// suggest different if before bucket completion
			for _, f := range additionalCompletions {
				f(c)
			}
		}
		if c.NArg() >= firstBckIdx && c.NArg() < firstBckIdx+bucketsCnt {
			bucketCompletions(bcmplop{firstBucketIdx: firstBckIdx, multiple: true})(c)
			return
		}
	}
}

func bpropCompletions(c *cli.Context) {
	err := cmn.IterFields(&cmn.BucketPropsToUpdate{}, func(tag string, _ cmn.IterField) (error, bool) {
		if !cos.AnyHasPrefixInSlice(tag, c.Args()) {
			if bpropsFilterExtra(c, tag) {
				fmt.Println(tag)
			}
		}
		return nil, false
	})
	debug.AssertNoErr(err)
}

func bpropsFilterExtra(c *cli.Context, tag string) bool {
	if !strings.HasPrefix(tag, "extra.") {
		return true
	}
	switch c.Args().First() {
	case apc.S3Scheme, apc.AWS:
		return strings.HasPrefix(tag, "extra.aws")
	case apc.HTTP:
		return strings.HasPrefix(tag, "extra.http")
	case apc.HDFS:
		return strings.HasPrefix(tag, "extra.hdfs")
	}
	return false
}

func bucketAndPropsCompletions(c *cli.Context) {
	if c.NArg() == 0 {
		f := bucketCompletions(bcmplop{})
		f(c)
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
		debug.AssertNoErr(err)
		sort.Strings(props)
		for _, prop := range props {
			fmt.Println(prop)
		}
	}
}

//
// Object
//

func putPromoteObjectCompletions(c *cli.Context) {
	if c.NArg() == 0 {
		// Waiting for file|directory as first arg
		return
	}
	if c.NArg() == 1 {
		f := bucketCompletions(bcmplop{
			separator:      true,
			firstBucketIdx: 1 /* bucket arg after file arg*/},
		)
		f(c)
		return
	}
}

//
// Xaction
//

func daemonXactionCompletions(c *cli.Context) {
	if c.NArg() > 2 {
		return
	}
	xactSet, xactKind := c.NArg() != 0, c.Args().First()
	if c.NArg() == 0 {
		daemonCompletions(completeTargets)(c)
	} else {
		smap, err := api.GetClusterMap(cliAPIParams(clusterURL))
		if err != nil {
			return
		}
		if node := smap.GetTarget(c.Args().First()); node != nil {
			xactSet = false
			xactKind = c.Args().Get(1)
		}
	}
	if !xactSet {
		xs := xact.ListDisplayNames(false /*onlyStartable*/)
		for name := range xs {
			fmt.Println(name)
		}
		return
	}
	if xact.IsSameScope(xactKind, xact.ScopeB, xact.ScopeGB) {
		bucketCompletions(bcmplop{})(c)
		return
	}
}

func xactionCompletions(cmd string) func(ctx *cli.Context) {
	return func(c *cli.Context) {
		if c.NArg() == 0 {
			xs := xact.ListDisplayNames(cmd == apc.ActXactStart /*onlyStartable*/)
			for name := range xs {
				fmt.Println(name)
			}
			return
		}
		name := c.Args().First()
		if xact.IsSameScope(name, xact.ScopeB, xact.ScopeGB) {
			bucketCompletions(bcmplop{})(c)
			return
		}
	}
}

func xactionDesc(onlyStartable bool) string {
	xs := xact.ListDisplayNames(onlyStartable)
	return fmt.Sprintf("%s can be one of: %s", xactionArgument, strings.Join(xs, ", "))
}

//
// Download & dSort
//

func downloadIDAllCompletions(c *cli.Context) {
	suggestDownloadID(c, func(*dload.Job) bool { return true })
}

func downloadIDRunningCompletions(c *cli.Context) {
	suggestDownloadID(c, (*dload.Job).JobRunning)
}

func downloadIDFinishedCompletions(c *cli.Context) {
	suggestDownloadID(c, (*dload.Job).JobFinished)
}

func suggestDownloadID(c *cli.Context, filter func(*dload.Job) bool) {
	if c.NArg() > 0 {
		return
	}
	if flagIsSet(c, allJobsFlag) {
		return
	}
	list, _ := api.DownloadGetList(apiBP, "", false /*onlyActive*/)
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

	list, _ := api.ListDSort(apiBP, "", false /*onlyActive*/)

	for _, job := range list {
		if filter(job) {
			fmt.Println(job.ID)
		}
	}
}

func addRoleCompletions(c *cli.Context) {
	if c.NArg() > 0 {
		accessCompletions(c)
	}
}

func setRoleCompletions(c *cli.Context) {
	if c.NArg() == 0 {
		oneRoleCompletions(c)
		return
	}
	accessCompletions(c)
}

func oneRoleCompletions(c *cli.Context) {
	roleList, err := authn.GetAllRoles(authParams)
	if err != nil {
		return
	}
	for _, role := range roleList {
		if role.ID == c.Args().First() {
			return
		}
	}
	for _, role := range roleList {
		fmt.Println(role.ID)
	}
}

func multiRoleCompletions(c *cli.Context) {
	roleList, err := authn.GetAllRoles(authParams)
	if err != nil {
		return
	}
	args := c.Args()
	for _, role := range roleList {
		if cos.StringInSlice(role.ID, args) {
			continue
		}
		fmt.Println(role.ID)
	}
}

func oneUserCompletions(c *cli.Context) {
	if c.NArg() > 0 {
		return
	}
	userList, err := authn.GetAllUsers(authParams)
	if err != nil {
		return
	}
	for _, user := range userList {
		fmt.Println(user.ID)
	}
}

func oneUserCompletionsWithRoles(c *cli.Context) {
	if c.NArg() == 0 {
		oneUserCompletions(c)
		return
	}
	userList, err := authn.GetAllUsers(authParams)
	if err != nil {
		return
	}

	for _, user := range userList {
		if user.ID == c.Args().First() {
			multiRoleCompletions(c)
			return
		}
	}

	for _, user := range userList {
		fmt.Println(user.ID)
	}
}

func oneClusterCompletions(c *cli.Context) {
	if c.NArg() > 0 {
		return
	}
	cluList, err := authn.GetRegisteredClusters(authParams, authn.CluACL{})
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
		lastVal := argLast(c)
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
	all, err := api.GetRemoteAIS(apiBP)
	if err != nil {
		return
	}
	for _, remais := range all.A {
		fmt.Println(remais.UUID)
		if remais.Alias != "" {
			fmt.Println(remais.Alias)
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
	debug.AssertNoErr(err)
}
