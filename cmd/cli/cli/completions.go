// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"sort"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ext/dload"
	"github.com/NVIDIA/aistore/ext/dsort"
	"github.com/NVIDIA/aistore/xact"

	"github.com/urfave/cli"
)

// This source handles Bash and zsh completions for the CLI.

const (
	// Log level doubles as level per se and (s)modules, the latter enumerated
	confLogLevel   = "log.level"
	confLogModules = "log.modules"
)

var (
	supportedBool = []string{"true", "false"}
	propCmpls     = map[string][]string{
		// log modules
		confLogModules: append(cos.Smodules[:], apc.NilValue),
		// checksums
		apc.HdrObjCksumType: cos.SupportedChecksums(),
		// access
		cmn.PropBucketAccessAttrs: apc.SupportedPermissions(),
		// feature flags
		clusterFeatures: append(feat.Cluster[:], apc.NilValue),
		bucketFeatures:  append(feat.Bucket[:], apc.NilValue),
		// rest
		"write_policy.data":                   apc.SupportedWritePolicy[:],
		"write_policy.md":                     apc.SupportedWritePolicy[:],
		"ec.compression":                      apc.SupportedCompression[:],
		"compression.checksum":                apc.SupportedCompression[:],
		"rebalance.compression":               apc.SupportedCompression[:],
		"distributed_sort.compression":        apc.SupportedCompression[:],
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

func lastIsSmodule(c *cli.Context) bool {
	if argLast(c) == confLogModules {
		return true
	}
	return _lastv(c, propCmpls[confLogModules])
}

func lastIsAccess(c *cli.Context) bool {
	if argLast(c) == cmn.PropBucketAccessAttrs {
		return true
	}
	return _lastv(c, propCmpls[cmn.PropBucketAccessAttrs])
}

func lastIsFeature(c *cli.Context, bucketScope bool) bool {
	if argLast(c) == feat.PropName {
		return true
	}
	if bucketScope {
		return _lastv(c, propCmpls[bucketFeatures])
	}
	return _lastv(c, propCmpls[clusterFeatures])
}

// Returns true if the last arg is any of the enumerated constants
func _lastv(c *cli.Context, values []string) bool {
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

// not-yet-typed:
// - log modules
// - access permissions
// - features
func smoduleCompletions(c *cli.Context) { remaining(c, propCmpls[confLogModules]) }
func accessCompletions(c *cli.Context)  { remaining(c, propCmpls[cmn.PropBucketAccessAttrs]) }

func featureCompletions(c *cli.Context, bucketScope bool) {
	if bucketScope {
		remaining(c, propCmpls[bucketFeatures])
	} else {
		remaining(c, propCmpls[clusterFeatures])
	}
}

func remaining(c *cli.Context, values []string) {
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

func propValueCompletion(c *cli.Context, bucketScope bool) bool {
	switch {
	case c.NArg() == 0:
		return false
	case lastIsAccess(c):
		accessCompletions(c)
		return true
	case lastIsFeature(c, bucketScope):
		featureCompletions(c, bucketScope)
		return true
	case lastIsSmodule(c):
		smoduleCompletions(c)
		return true
	}
	// default
	list, ok := propCmpls[argLast(c)]
	if !ok {
		return false
	}
	for _, val := range list {
		fmt.Println(val)
	}
	return true
}

func showConfigCompletions(c *cli.Context) {
	if c.NArg() == 0 {
		fmt.Println(cmdCluster)
		fmt.Println(cmdCLI)
		suggestAllNodes(c)
		return
	}
	if c.Args().Get(0) == cmdCLI {
		return
	}
	if c.Args().Get(0) == cmdCluster {
		if c.NArg() == 1 {
			configSectionCompletions(c, cmdCluster)
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
		section := strings.Split(uniqueTag, cmn.IterFieldNameSepa)[0]
		props.Set(section)
		return nil, false
	})
	debug.AssertNoErr(err)
	for prop := range props {
		fmt.Println(prop)
	}
}

func setNodeConfigCompletions(c *cli.Context) {
	if c.NArg() == 0 {
		suggestNode(c, allNodes)
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
			fmt.Println(cmdReset)
			fmt.Println("backend") // NOTE special case: custom marshaling (ref 080235)
		}
		err := cmn.IterFields(v, func(tag string, _ cmn.IterField) (err error, b bool) {
			props.Set(tag)
			if tag == confLogLevel {
				props.Set(confLogModules) // (ref 836)
			}
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

const (
	allTargets = iota
	allProxies
	allNodes
)

func suggestTargets(c *cli.Context)  { suggestNode(c, allTargets) }
func suggestProxies(c *cli.Context)  { suggestNode(c, allProxies) }
func suggestAllNodes(c *cli.Context) { suggestNode(c, allNodes) }

func suggestNode(c *cli.Context, ty int) {
	smap, err := getClusterMap(c)
	if err != nil {
		completionErr(c, err)
		return
	}
	if _, _, err = getNode(c, argLast(c)); err == nil {
		return // node already selected
	}
	if ty != allTargets {
		for sid := range smap.Pmap {
			fmt.Println(meta.Pname(sid))
		}
	}
	if ty != allProxies {
		for sid := range smap.Tmap {
			fmt.Println(meta.Tname(sid))
		}
	}
}

func suggestNodesInMaint(c *cli.Context) {
	smap, err := getClusterMap(c)
	if err != nil {
		completionErr(c, err)
		return
	}
	if _, _, err = getNode(c, argLast(c)); err == nil {
		return // node already selected
	}
	for _, psi := range smap.Pmap {
		if psi.InMaint() {
			fmt.Println(meta.Pname(psi.ID()))
		}
	}
	for _, tsi := range smap.Tmap {
		if tsi.InMaint() {
			fmt.Println(meta.Tname(tsi.ID()))
		}
	}
}

func showClusterCompletions(c *cli.Context) {
	switch c.NArg() {
	case 0:
		fmt.Println(apc.Proxy, apc.Target, cmdSmap, cmdBMD, cmdConfig, cmdShowStats)
	case 1:
		switch c.Args().Get(0) {
		case apc.Proxy:
			suggestProxies(c)
		case apc.Target:
			suggestTargets(c)
		default:
			suggestAllNodes(c)
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
		if tag == confLogLevel {
			propList = append(propList, confLogModules) // insert to assign separately and combine below (ref 836)
		}
		return
	}, cmn.IterOpts{Allowed: apc.Cluster})
	debug.AssertNoErr(err)

	if propValueCompletion(c, false /*bucket scope*/) {
		return
	}

	// NOTE special case: custom marshaling (ref 080235)
	if c.NArg() == 0 {
		propList = append(propList, "backend")
	}

	for _, prop := range propList {
		if !cos.AnyHasPrefixInSlice(prop, c.Args()) {
			fmt.Println(prop)
		}
	}
}

func suggestUpdatableConfig(c *cli.Context) {
	if propValueCompletion(c, false /*bucket scope*/) {
		return
	}
	scope := apc.Cluster
	if c.NArg() > 0 && !isConfigProp(c.Args().Get(0)) {
		scope = apc.Daemon
	}

	props := append(configPropList(scope), apc.QparamTransient)
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

func (opts *bcmplop) buckets(c *cli.Context) {
	var (
		additionalCompletions []cli.BashCompleteFunc
		buckets               []cmn.Bck
	)
	additionalCompletions = opts.additionalCompletions
	if c.NArg() > opts.firstBucketIdx && !opts.multiple {
		if propValueCompletion(c, true /*bucket scope*/) {
			return
		}
		for _, f := range additionalCompletions {
			f(c)
		}
		return
	}

	qbck := cmn.QueryBcks{Provider: opts.provider}
	buckets, err := api.ListBuckets(apiBP, qbck, apc.FltPresent) // NOTE: `present` only
	if err != nil {
		completionErr(c, err)
		return
	}
	if qbck.Provider == "" {
		config, err := api.GetClusterConfig(apiBP)
		if err != nil {
			completionErr(c, err)
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
	printNotUsedBuckets(c, buckets, opts.separator, opts.multiple)
}

func suggestProvider(*cli.Context) {
	fmt.Println(fcyan(scopeAll))
	for p := range apc.Providers {
		fmt.Println(p)
	}
}

func (opts *bcmplop) remoteBuckets(c *cli.Context) {
	var (
		buckets []cmn.Bck
	)
	for provider := range apc.Providers {
		if !apc.IsCloudProvider(provider) {
			// Filter out non-cloud providers
			continue
		}
		qbck := cmn.QueryBcks{Provider: provider}
		bcks, err := api.ListBuckets(apiBP, qbck, apc.FltPresent) // NOTE: `present` only
		if err != nil {
			completionErr(c, err)
			return
		}
		if len(bcks) == 0 {
			continue
		}
		if len(buckets) == 0 {
			buckets = bcks
			continue
		}
		buckets = append(buckets, bcks...)
	}
	qbck := cmn.QueryBcks{Provider: apc.AIS, Ns: cmn.NsAnyRemote}
	if bcks, err := api.ListBuckets(apiBP, qbck, apc.FltPresent); err == nil && len(bcks) > 0 {
		buckets = append(buckets, bcks...)
	}

	printNotUsedBuckets(c, buckets, opts.separator, opts.multiple)
}

// The function lists buckets names if the first argument was not yet given, otherwise it lists flags and additional completions
// Multiple buckets will also be listed if 'multiple'
// Printed names will end with '/' if 'separator'
func bucketCompletions(opts bcmplop) cli.BashCompleteFunc {
	return opts.buckets
}

func remoteBucketCompletions(opts bcmplop) cli.BashCompleteFunc {
	return opts.remoteBuckets
}

func printNotUsedBuckets(c *cli.Context, bcks cmn.Bcks, separator, multiple bool) {
	var sep string
	if separator {
		sep = "/"
	}
mloop:
	for i := range bcks {
		b := bcks[i]
		if multiple {
			args := c.Args()
			for a := range args {
				argBck := args[a]
				parsedArgBck, err := parseBckURI(c, argBck, false)
				if err != nil {
					return
				}
				if parsedArgBck.Equal(&b) {
					continue mloop // already listed
				}
			}
		}
		fmt.Printf("%s%s\n", b.Cname(""), sep)
	}
}

func manyBucketsCompletions(additionalCompletions []cli.BashCompleteFunc, firstBckIdx int) cli.BashCompleteFunc {
	const bucketsCnt = 2 // always expect 2 buckets
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
	err := cmn.IterFields(&cmn.BpropsToSet{}, func(tag string, _ cmn.IterField) (error, bool) {
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
	switch c.Args().Get(0) {
	case apc.S3Scheme, apc.AWS:
		return strings.HasPrefix(tag, "extra.aws")
	case apc.HT:
		return strings.HasPrefix(tag, "extra.http")
	}
	return false
}

func bucketAndPropsCompletions(c *cli.Context) {
	if c.NArg() == 0 {
		f := bucketCompletions(bcmplop{})
		f(c)
		return
	} else if c.NArg() == 1 {
		var sections []string
		err := cmn.IterFields(cmn.Bprops{}, func(uniqueTag string, _ cmn.IterField) (err error, b bool) {
			section := strings.Split(uniqueTag, cmn.IterFieldNameSepa)[0]
			sections = append(sections, section)
			if flagIsSet(c, verboseFlag) {
				sections = append(sections, uniqueTag)
			}
			return nil, false
		})
		// NOTE: do not have bprops at this point, may miss remote backend (prop)
		if bck, err := parseBckURI(c, c.Args().Get(0), false); err == nil {
			p := apc.NormalizeProvider(bck.Provider)
			cmn.IterFields(&cmn.BpropsToSet{}, func(tag string, _ cmn.IterField) (e error, f bool) {
				if strings.Contains(tag, "."+p) { // e.g., "extra.aws"
					sections = append(sections, "extra")
				}
				return
			})
		}

		debug.AssertNoErr(err)
		sort.Strings(sections)
		for _, prop := range sections {
			fmt.Println(prop)
		}
	}
}

//
// Object
//

func putPromApndCompletions(c *cli.Context) {
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
// Job
//

// complete to:
// - NAME [running job or xaction ID] [TARGET], or
// - NAME [TARGET]
func runningJobCompletions(c *cli.Context) {
	switch c.NArg() {
	case 0: // 1. NAME
		if flagIsSet(c, allJobsFlag) {
			names := xact.ListDisplayNames(false /*only-startable*/)
			debug.Assert(!cos.StringInSlice(apc.ActDsort, names))
			names = append(names, apc.ActDsort)
			fmt.Println(strings.Join(names, " "))
			return
		}
		kindIDs, err := api.GetAllRunningXactions(apiBP, "")
		if err != nil {
			completionErr(c, err)
			return
		}
		already := cos.StrSet{}
		for _, ki := range kindIDs {
			i := strings.IndexByte(ki, xact.LeftID[0])
			kind := ki[0:i]
			if !already.Contains(kind) {
				xname, _ := xact.GetKindName(kind)
				fmt.Println(xname)
				already.Set(kind)
			}
		}
		// NOTE: dsort is the only exception - not an xaction
		list, err := api.ListDsort(apiBP, "", true /*onlyActive*/)
		if err != nil {
			completionErr(c, err)
			return
		}
		if len(list) > 0 {
			fmt.Println(cmdDsort)
		}
		return
	case 1: // ID
		name := c.Args().Get(0)
		switch name {
		case cmdDownload:
			suggestDownloadID(c, (*dload.Job).JobRunning, 1 /*shift*/)
			return
		case cmdDsort:
			suggestDsortID(c, (*dsort.JobInfo).IsRunning, 1 /*shift*/)
			return
		case commandETL:
			suggestEtlName(c, 1 /*shift*/)
			return
		}
		// complete xid
		xactIDs, err := api.GetAllRunningXactions(apiBP, name)
		if err != nil {
			completionErr(c, err)
			return
		}
		if len(xactIDs) == 0 {
			suggestTargets(c)
			return
		}
		for _, ki := range xactIDs {
			i := strings.IndexByte(ki, xact.LeftID[0])
			fmt.Println(ki[i+1 : len(ki)-1]) // extract UUID from "name[UUID]"
		}
		return
	case 2: // TARGET
		suggestTargets(c)
	}
}

// - [running rebalance ID] [TARGET], or
// - [TARGET]
func rebalanceCompletions(c *cli.Context) {
	switch c.NArg() {
	case 0:
		xactIDs, err := api.GetAllRunningXactions(apiBP, apc.ActRebalance)
		if err != nil {
			completionErr(c, err)
			return
		}
		if len(xactIDs) == 0 {
			suggestTargets(c)
			return
		}
		for _, ki := range xactIDs {
			i := strings.IndexByte(ki, xact.LeftID[0])
			fmt.Println(ki[i+1 : len(ki)-1]) // extract UUID from "name[UUID]"
		}
		return
	case 1:
		suggestTargets(c)
	}
}

//
// Download & dsort
//

func downloadIDFinishedCompletions(c *cli.Context) { suggestDownloadID(c, (*dload.Job).JobFinished, 0) }

func suggestDownloadID(c *cli.Context, filter func(*dload.Job) bool, shift int) {
	if c.NArg() > shift {
		return
	}
	if flagIsSet(c, allJobsFlag) {
		return
	}
	list, err := api.DownloadGetList(apiBP, "", false /*onlyActive*/)
	if err != nil {
		completionErr(c, err)
		return
	}
	for _, job := range list {
		if filter(job) {
			fmt.Println(job.ID)
		}
	}
}

func dsortIDFinishedCompletions(c *cli.Context) { suggestDsortID(c, (*dsort.JobInfo).IsFinished, 0) }

func suggestDsortID(c *cli.Context, filter func(*dsort.JobInfo) bool, shift int) {
	if c.NArg() > shift {
		return
	}
	list, _ := api.ListDsort(apiBP, "", false /*onlyActive*/)
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
		if role.Name == c.Args().Get(0) {
			return
		}
	}
	for _, role := range roleList {
		fmt.Println(role.Name)
	}
}

func multiRoleCompletions(c *cli.Context) {
	roleList, err := authn.GetAllRoles(authParams)
	if err != nil {
		return
	}
	args := c.Args()
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
		if user.ID == c.Args().Get(0) {
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
		fmt.Println(cos.Left(clu.Alias, clu.ID))
	}
}

func authNConfigPropList() []string {
	propList := []string{}
	emptyCfg := authn.ConfigToUpdate{Server: &authn.ServerConfToSet{}}
	cmn.IterFields(emptyCfg, func(tag string, _ cmn.IterField) (error, bool) {
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
	err := cmn.IterFields(gcfg, func(tag string, _ cmn.IterField) (error, bool) {
		if !cos.AnyHasPrefixInSlice(tag, c.Args()) {
			fmt.Println(tag)
		}
		return nil, false
	})
	debug.AssertNoErr(err)
}

func suggestMpathEnable(c *cli.Context) { _suggestMpath(c, cmdMpathEnable) }
func suggestMpathActive(c *cli.Context) { _suggestMpath(c, "select-active") } // local usage
func suggestMpathDetach(c *cli.Context) { _suggestMpath(c, cmdMpathDetach) }

func _suggestMpath(c *cli.Context, cmd string) {
	switch c.NArg() {
	case 0:
		suggestTargets(c)
	case 1:
		node, _, err := arg0Node(c)
		if node == nil || err != nil {
			return
		}
		mpl, err := api.GetMountpaths(apiBP, node)
		if err != nil {
			return
		}
		switch cmd {
		case cmdMpathEnable:
			for _, mpath := range mpl.Disabled {
				fmt.Println(mpath)
			}
			for _, mpath := range mpl.WaitingDD {
				fmt.Println(mpath)
			}
		case cmdMpathDetach:
			for _, mpath := range mpl.Available {
				fmt.Println(mpath)
			}
			for _, mpath := range mpl.Disabled {
				fmt.Println(mpath)
			}
		case "select-active":
			for _, mpath := range mpl.Available {
				fmt.Println(mpath)
			}
		}
	}
}

func suggestCloudProvider(c *cli.Context) {
	if c.NArg() > 0 {
		return
	}
	for provider := range apc.Providers {
		if apc.IsCloudProvider(provider) {
			fmt.Println(provider)
		}
	}
}
