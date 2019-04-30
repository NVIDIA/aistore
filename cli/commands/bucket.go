// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that interact with buckets in the cluster
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

const (
	bucketCreate     = "create"
	bucketDestroy    = "destroy"
	bucketNames      = "names"
	bucketSetProps   = cmn.ActSetProps
	bucketResetProps = cmn.ActResetProps
	bucketGetProps   = commandProps
	bucketNWayMirror = cmn.ActMakeNCopies
	bucketEvict      = commandEvict
)

var (
	showUnmatchedFlag = cli.BoolTFlag{Name: "show-unmatched", Usage: "also list files that were not matched by regex and template"}
	allFlag           = cli.BoolTFlag{Name: "all", Usage: "list all files in bucket (including EC replicas) and print their status"}
	newBucketFlag     = cli.StringFlag{Name: "new-bucket", Usage: "new name of bucket"}
	pageSizeFlag      = cli.StringFlag{Name: "page-size", Usage: "maximum number of entries by list bucket call", Value: "1000"}
	objPropsFlag      = cli.StringFlag{Name: "props", Usage: "properties to return with object names, comma separated", Value: "size,version"}
	objLimitFlag      = cli.StringFlag{Name: "limit", Usage: "limit object count", Value: "0"}
	templateFlag      = cli.StringFlag{Name: "template", Usage: "template for matching object names"}
	copiesFlag        = cli.IntFlag{Name: "copies", Usage: "number of object replicas", Value: 1}

	baseBucketFlags = []cli.Flag{
		bucketFlag,
		bckProviderFlag,
	}

	bucketFlags = map[string][]cli.Flag{
		bucketCreate:  []cli.Flag{bucketFlag},
		bucketNames:   []cli.Flag{regexFlag, bckProviderFlag},
		bucketDestroy: []cli.Flag{bucketFlag},
		commandRename: append(
			[]cli.Flag{
				newBucketFlag,
			},
			baseBucketFlags...),
		commandList: append(
			[]cli.Flag{
				regexFlag,
				templateFlag,
				prefixFlag,
				pageSizeFlag,
				objPropsFlag,
				objLimitFlag,
				showUnmatchedFlag,
				allFlag,
			},
			baseBucketFlags...),
		bucketSetProps: append(
			[]cli.Flag{jsonFlag},
			baseBucketFlags...),
		bucketResetProps: baseBucketFlags,
		bucketGetProps: append(
			[]cli.Flag{jsonFlag},
			baseBucketFlags...),
		bucketNWayMirror: append(
			[]cli.Flag{copiesFlag},
			baseBucketFlags...),
		bucketEvict: []cli.Flag{bucketFlag},
	}

	bucketGeneric        = "%s bucket %s --bucket <value>"
	bucketCreateText     = fmt.Sprintf(bucketGeneric, cliName, bucketCreate)
	bucketDelText        = fmt.Sprintf(bucketGeneric, cliName, bucketDestroy)
	bucketListText       = fmt.Sprintf(bucketGeneric, cliName, commandList)
	bucketResetPropsText = fmt.Sprintf(bucketGeneric, cliName, bucketResetProps)
	bucketGetPropsText   = fmt.Sprintf(bucketGeneric, cliName, bucketGetProps)
	bucketEvictText      = fmt.Sprintf(bucketGeneric, cliName, bucketEvict)
	bucketNamesText      = fmt.Sprintf("%s bucket %s", cliName, bucketNames)
	bucketRenameText     = fmt.Sprintf("%s bucket %s --bucket <value> --new-bucket <value>", cliName, commandRename)
	bucketSetPropsText   = fmt.Sprintf("%s bucket %s --bucket <value> key=value ...", cliName, bucketSetProps)
	bucketNWayMirrorText = fmt.Sprintf("%s bucket %s --bucket <value> --copies <value>", cliName, bucketNWayMirror)

	BucketCmds = []cli.Command{
		{
			Name:  cmn.URLParamBucket,
			Usage: "interact with buckets",
			Flags: baseBucketFlags,
			Subcommands: []cli.Command{
				{
					Name:         bucketCreate,
					Usage:        "creates the local bucket",
					UsageText:    bucketCreateText,
					Flags:        bucketFlags[bucketCreate],
					Action:       bucketHandler,
					BashComplete: flagList,
				},
				{
					Name:         bucketDestroy,
					Usage:        "destroys the local bucket",
					UsageText:    bucketDelText,
					Flags:        bucketFlags[bucketDestroy],
					Action:       bucketHandler,
					BashComplete: flagList,
				},
				{
					Name:         commandRename,
					Usage:        "renames the local bucket",
					UsageText:    bucketRenameText,
					Flags:        bucketFlags[commandRename],
					Action:       bucketHandler,
					BashComplete: flagList,
				},
				{
					Name:         bucketNames,
					Usage:        "returns all bucket names",
					UsageText:    bucketNamesText,
					Flags:        bucketFlags[bucketNames],
					Action:       bucketHandler,
					BashComplete: flagList,
				},
				{
					Name:         commandList,
					Usage:        "returns all objects from bucket",
					UsageText:    bucketListText,
					Flags:        bucketFlags[commandList],
					Action:       bucketHandler,
					BashComplete: flagList,
				},
				{
					Name:         bucketSetProps,
					Usage:        "sets bucket properties",
					UsageText:    bucketSetPropsText,
					Flags:        bucketFlags[bucketSetProps],
					Action:       bucketHandler,
					BashComplete: flagList,
				},
				{
					Name:         bucketResetProps,
					Usage:        "resets bucket properties",
					UsageText:    bucketResetPropsText,
					Flags:        bucketFlags[bucketResetProps],
					Action:       bucketHandler,
					BashComplete: flagList,
				},
				{
					Name:         bucketGetProps,
					Usage:        "gets bucket properties",
					UsageText:    bucketGetPropsText,
					Flags:        bucketFlags[bucketGetProps],
					Action:       bucketHandler,
					BashComplete: flagList,
				},
				{
					Name:         bucketNWayMirror,
					Usage:        "configures the bucket for n-way mirroring",
					UsageText:    bucketNWayMirrorText,
					Flags:        bucketFlags[bucketNWayMirror],
					Action:       bucketHandler,
					BashComplete: flagList,
				},
				{
					Name:         bucketEvict,
					Usage:        "evicts a cloud bucket",
					UsageText:    bucketEvictText,
					Flags:        bucketFlags[bucketEvict],
					Action:       bucketHandler,
					BashComplete: flagList,
				},
			},
		},
	}
)

func bucketHandler(c *cli.Context) (err error) {
	baseParams := cliAPIParams(ClusterURL)
	command := c.Command.Name
	if err = checkFlags(c, bucketFlag); err != nil && command != bucketNames {
		return err
	}

	bucket := parseFlag(c, bucketFlag)

	switch command {
	case bucketCreate:
		err = createBucket(baseParams, bucket)
	case bucketDestroy:
		err = destroyBucket(baseParams, bucket)
	case commandRename:
		err = renameBucket(c, baseParams, bucket)
	case bucketEvict:
		err = evictBucket(baseParams, bucket)
	case bucketNames:
		err = listBucketNames(c, baseParams)
	case commandList:
		err = listBucketObj(c, baseParams, bucket)
	case bucketSetProps:
		err = setBucketProps(c, baseParams, bucket)
	case bucketResetProps:
		err = resetBucketProps(c, baseParams, bucket)
	case bucketGetProps:
		err = bucketProps(c, baseParams, bucket)
	case bucketNWayMirror:
		err = configureNCopies(c, baseParams, bucket)
	default:
		return fmt.Errorf(invalidCmdMsg, command)
	}
	return errorHandler(err)
}

// Creates new local bucket
func createBucket(baseParams *api.BaseParams, bucket string) (err error) {
	if err = api.CreateLocalBucket(baseParams, bucket); err != nil {
		return
	}
	fmt.Printf("%s bucket created\n", bucket)
	return
}

// Destroy local bucket
func destroyBucket(baseParams *api.BaseParams, bucket string) (err error) {
	if err = api.DestroyLocalBucket(baseParams, bucket); err != nil {
		return
	}
	fmt.Printf("%s bucket destroyed\n", bucket)
	return
}

// Rename local bucket
func renameBucket(c *cli.Context, baseParams *api.BaseParams, bucket string) (err error) {
	if err = checkFlags(c, newBucketFlag); err != nil {
		return
	}
	newBucket := parseFlag(c, newBucketFlag)
	if err = api.RenameLocalBucket(baseParams, bucket, newBucket); err != nil {
		return
	}
	fmt.Printf("%s bucket renamed to %s\n", bucket, newBucket)
	return
}

// Evict a cloud bucket
func evictBucket(baseParams *api.BaseParams, bucket string) (err error) {
	query := url.Values{cmn.URLParamBckProvider: []string{cmn.CloudBs}}
	if err = api.EvictCloudBucket(baseParams, bucket, query); err != nil {
		return
	}
	fmt.Printf("%s bucket evicted\n", bucket)
	return
}

// Lists bucket names
func listBucketNames(c *cli.Context, baseParams *api.BaseParams) (err error) {
	bckProvider, err := cmn.BckProviderFromStr(parseFlag(c, bckProviderFlag))
	if err != nil {
		return
	}
	bucketNames, err := api.GetBucketNames(baseParams, bckProvider)
	if err != nil {
		return
	}
	printBucketNames(bucketNames, parseFlag(c, regexFlag), bckProvider)
	return
}

// Lists objects in bucket
func listBucketObj(c *cli.Context, baseParams *api.BaseParams, bucket string) error {
	objectListFilter, err := newObjectListFilter(c)
	if err != nil {
		return err
	}

	prefix := parseFlag(c, prefixFlag)
	props := "name," + parseFlag(c, objPropsFlag)
	if flagIsSet(c, allFlag) && !strings.Contains(props, "status") {
		// If `all` flag is set print status of the file so that the output is easier to understand -
		// there might be multiple files with the same name listed (e.g EC replicas)
		props = fmt.Sprintf("%s,%s", props, "status")
	}
	pagesize, err := strconv.Atoi(parseFlag(c, pageSizeFlag))
	if err != nil {
		return err
	}
	limit, err := strconv.Atoi(parseFlag(c, objLimitFlag))
	if err != nil {
		return err
	}

	query := url.Values{}
	query.Add(cmn.URLParamBckProvider, parseFlag(c, bckProviderFlag))
	query.Add(cmn.URLParamPrefix, prefix)

	msg := &cmn.SelectMsg{PageSize: pagesize, Props: props}
	objList, err := api.ListBucket(baseParams, bucket, msg, limit, query)
	if err != nil {
		return err
	}

	showUnmatched := flagIsSet(c, showUnmatchedFlag)

	return printObjectProps(objList.Entries, objectListFilter, props, showUnmatched)
}

// Sets bucket properties
func setBucketProps(c *cli.Context, baseParams *api.BaseParams, bucket string) (err error) {
	if c.NArg() < 1 {
		return errors.New("expected at least one argument")
	}

	bckProvider, err := cmn.BckProviderFromStr(parseFlag(c, bckProviderFlag))
	if err != nil {
		return
	}
	query := url.Values{cmn.URLParamBckProvider: []string{bckProvider}}

	if err = canReachBucket(baseParams, bucket, bckProvider); err != nil {
		return
	}

	// For setting bucket props via action message
	if flagIsSet(c, jsonFlag) {
		props := cmn.BucketProps{}
		inputProps := []byte(c.Args().First())
		if err = json.Unmarshal(inputProps, &props); err != nil {
			return
		}
		if err = api.SetBucketPropsMsg(baseParams, bucket, props, query); err != nil {
			return
		}

		fmt.Printf("Bucket props set for %s bucket\n", bucket)
		return
	}

	// For setting bucket props via URL query string
	nvs, err := makeKVS(c.Args(), "=")
	if err != nil {
		return
	}
	if err = api.SetBucketProps(baseParams, bucket, nvs, query); err != nil {
		return
	}
	fmt.Printf("%d properties set for %s bucket\n", c.NArg(), bucket)
	return
}

// Resets bucket props
func resetBucketProps(c *cli.Context, baseParams *api.BaseParams, bucket string) (err error) {
	bckProvider, err := cmn.BckProviderFromStr(parseFlag(c, bckProviderFlag))
	if err != nil {
		return
	}
	query := url.Values{cmn.URLParamBckProvider: []string{bckProvider}}
	if err = canReachBucket(baseParams, bucket, bckProvider); err != nil {
		return
	}

	if err = api.ResetBucketProps(baseParams, bucket, query); err != nil {
		return
	}

	fmt.Printf("Reset %s bucket properties\n", bucket)
	return
}

// Get bucket props
func bucketProps(c *cli.Context, baseParams *api.BaseParams, bucket string) (err error) {
	bckProvider, err := cmn.BckProviderFromStr(parseFlag(c, bckProviderFlag))
	if err != nil {
		return
	}
	query := url.Values{cmn.URLParamBckProvider: []string{bckProvider}}
	bckProps, err := api.HeadBucket(baseParams, bucket, query)
	if err != nil {
		return
	}

	return templates.DisplayOutput(bckProps, templates.BucketPropsTmpl, flagIsSet(c, jsonFlag))
}

// Configure bucket as n-way mirror
func configureNCopies(c *cli.Context, baseParams *api.BaseParams, bucket string) (err error) {
	bckProvider, err := cmn.BckProviderFromStr(parseFlag(c, bckProviderFlag))
	if err != nil {
		return
	}
	if err = checkFlags(c, copiesFlag); err != nil {
		return
	}
	if err = canReachBucket(baseParams, bucket, bckProvider); err != nil {
		return
	}
	copies := c.Int(copiesFlag.Name)
	if err = api.MakeNCopies(baseParams, bucket, copies); err != nil {
		return
	}
	fmt.Printf("Configured %s to replicate %d copies of its objects\n", bucket, copies)
	return
}

// HELPERS
func printBucketNames(bucketNames *cmn.BucketNames, regex, bckProvider string) {
	if bckProvider == cmn.LocalBs || bckProvider == "" {
		localBuckets := regexFilter(regex, bucketNames.Local)
		fmt.Printf("Local Buckets (%d)\n", len(localBuckets))
		for _, bucket := range localBuckets {
			fmt.Println(bucket)
		}
		if bckProvider == cmn.LocalBs {
			return
		}
		fmt.Println()
	}

	cloudBuckets := regexFilter(regex, bucketNames.Cloud)
	fmt.Printf("Cloud Buckets (%d)\n", len(cloudBuckets))
	for _, bucket := range cloudBuckets {
		fmt.Println(bucket)
	}
}

func buildOutputTemplate(props string) (string, error) {
	var (
		headSb strings.Builder
		bodySb strings.Builder

		propsList = makeList(props, ",")
	)
	bodySb.WriteString("{{range $obj := .}}")

	for _, field := range propsList {
		if _, ok := templates.ObjectPropsMap[field]; !ok {
			return "", fmt.Errorf("%q is not a valid property", field)
		}
		headSb.WriteString(field + "\t")
		bodySb.WriteString(templates.ObjectPropsMap[field])
	}
	headSb.WriteString("\n")
	bodySb.WriteString("\n{{end}}\n")

	return headSb.String() + bodySb.String(), nil
}

func printObjectProps(entries []*cmn.BucketEntry, objectFilter *objectListFilter, props string, showUnmatched bool) error {
	outputTemplate, err := buildOutputTemplate(props)
	if err != nil {
		return err
	}

	matchingEntries, rest := objectFilter.filter(entries)

	err = templates.DisplayOutput(matchingEntries, outputTemplate)
	if err != nil {
		return err
	}

	if showUnmatched {
		outputTemplate = "Unmatched files:\n" + outputTemplate
		err = templates.DisplayOutput(rest, outputTemplate)
	}

	return err
}

type (
	entryFilter func(*cmn.BucketEntry) bool

	objectListFilter struct {
		predicates []entryFilter
	}
)

func (o *objectListFilter) addFilter(f entryFilter) {
	o.predicates = append(o.predicates, f)
}

func (o *objectListFilter) matchesAll(obj *cmn.BucketEntry) bool {
	// Check if object name matches *all* specified predicates
	for _, predicate := range o.predicates {
		if !predicate(obj) {
			return false
		}
	}
	return true
}

func (o *objectListFilter) filter(entries []*cmn.BucketEntry) (matching []cmn.BucketEntry, rest []cmn.BucketEntry) {
	for _, obj := range entries {
		if o.matchesAll(obj) {
			matching = append(matching, *obj)
		} else {
			rest = append(rest, *obj)
		}
	}
	return
}

func newObjectListFilter(c *cli.Context) (*objectListFilter, error) {
	objFilter := &objectListFilter{}

	if !flagIsSet(c, allFlag) {
		// Filter out files with status different than OK
		objFilter.addFilter(func(obj *cmn.BucketEntry) bool { return obj.Status == cmn.ObjStatusOK })
	}

	if regexStr := parseFlag(c, regexFlag); regexStr != "" {
		regex, err := regexp.Compile(regexStr)
		if err != nil {
			return nil, err
		}

		objFilter.addFilter(func(obj *cmn.BucketEntry) bool { return regex.MatchString(obj.Name) })
	}

	if bashTemplate := parseFlag(c, templateFlag); bashTemplate != "" {
		pt, err := cmn.ParseBashTemplate(bashTemplate)
		if err != nil {
			return nil, err
		}

		matchingObjectNames := make(cmn.StringSet)

		linksIt := pt.Iter()
		for objName, hasNext := linksIt(); hasNext; objName, hasNext = linksIt() {
			matchingObjectNames[objName] = struct{}{}
		}
		objFilter.addFilter(func(obj *cmn.BucketEntry) bool { _, ok := matchingObjectNames[obj.Name]; return ok })
	}

	return objFilter, nil
}
