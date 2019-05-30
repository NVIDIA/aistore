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
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

const (
	bucketCreate     = "create"
	bucketDestroy    = "destroy"
	bucketNWayMirror = cmn.ActMakeNCopies
	bucketEvict      = commandEvict
	bucketSummary    = "summary"
	bucketObjects    = "objects"

	commandBucketProps = "props"
	propsSet           = "set"
	propsReset         = "reset"

	readonlyBucketAccess  = "ro"
	readwriteBucketAccess = "rw"

	// max wait time for a function finishes before printing "Please wait"
	longCommandTime = time.Second * 10

	bucketArgumentText       = "BUCKET_NAME"
	bucketRenameArgumentText = bucketArgumentText + " NEW_NAME"
	bucketPropsArgumentText  = bucketArgumentText + " " + atLeastOneKeyValuePairArgumentsText
)

var (
	showUnmatchedFlag = cli.BoolTFlag{Name: "show-unmatched", Usage: "also list objects that were not matched by regex and template"}
	allFlag           = cli.BoolTFlag{Name: "all", Usage: "list all objects in a bucket and their details; include EC replicas"}
	propsFlag         = cli.BoolFlag{Name: "props", Usage: "properties of a bucket"}
	pageSizeFlag      = cli.StringFlag{Name: "page-size", Usage: "maximum number of entries by list bucket call", Value: "1000"}
	objPropsFlag      = cli.StringFlag{Name: "props", Usage: "properties to return with object names, comma separated", Value: "size,version"}
	objLimitFlag      = cli.StringFlag{Name: "limit", Usage: "limit object count", Value: "0"}
	templateFlag      = cli.StringFlag{Name: "template", Usage: "template for matching object names"}
	copiesFlag        = cli.IntFlag{Name: "copies", Usage: "number of object replicas", Value: 1}
	fastFlag          = cli.BoolTFlag{Name: "fast", Usage: "use fast API to list all object names in a bucket. Flags 'props', 'all', 'limit', and 'page-size' are ignored in this mode"}
	jsonspecFlag      = cli.StringFlag{Name: "jsonspec", Usage: "bucket properties in JSON format"}
	pagedFlag         = cli.BoolFlag{Name: "paged", Usage: "fetch and print the bucket list page by page, ignored in fast mode"}
	maxPagesFlag      = cli.IntFlag{Name: "max-pages", Usage: "display up to this number pages of bucket objects"}
	markerFlag        = cli.StringFlag{Name: "marker", Usage: "start listing bucket objects starting from the object that follows the marker(alphabetically), ignored in fast mode"}

	baseBucketFlags = []cli.Flag{
		bckProviderFlag,
	}

	bucketFlags = map[string][]cli.Flag{
		bucketCreate: {},
		commandList: {
			regexFlag,
			bckProviderFlag,
			noHeaderFlag,
		},
		bucketDestroy: {},
		commandRename: {},
		bucketObjects: append(
			baseBucketFlags,
			regexFlag,
			templateFlag,
			prefixFlag,
			pageSizeFlag,
			objPropsFlag,
			objLimitFlag,
			showUnmatchedFlag,
			allFlag,
			fastFlag,
			noHeaderFlag,
			pagedFlag,
			maxPagesFlag,
			markerFlag,
		),
		bucketNWayMirror: append(
			baseBucketFlags,
			copiesFlag,
		),
		bucketEvict: {},
		bucketSummary: append(
			baseBucketFlags,
			regexFlag,
			prefixFlag,
			pageSizeFlag,
		),
	}

	bucketPropsFlags = map[string][]cli.Flag{
		commandList: append(
			baseBucketFlags,
			jsonFlag,
		),
		propsSet: append(
			baseBucketFlags,
			jsonspecFlag,
		),
		propsReset: baseBucketFlags,
	}

	bucketCmds = []cli.Command{
		{
			Name:  cmn.URLParamBucket,
			Usage: "operate on buckets",
			Subcommands: []cli.Command{
				{
					Name:  commandBucketProps,
					Usage: "operate on bucket properties",
					Subcommands: []cli.Command{
						{
							Name:         commandList,
							Usage:        "lists bucket properties",
							ArgsUsage:    bucketArgumentText,
							Flags:        bucketPropsFlags[commandList],
							Action:       bucketPropsHandler,
							BashComplete: bucketList([]cli.BashCompleteFunc{}),
						},
						{
							Name:         propsSet,
							Usage:        "sets bucket properties",
							ArgsUsage:    bucketPropsArgumentText,
							Flags:        bucketPropsFlags[propsSet],
							Action:       bucketPropsHandler,
							BashComplete: bucketList([]cli.BashCompleteFunc{propList}),
						},
						{
							Name:         propsReset,
							Usage:        "resets bucket properties",
							ArgsUsage:    bucketArgumentText,
							Flags:        bucketPropsFlags[propsReset],
							Action:       bucketPropsHandler,
							BashComplete: bucketList([]cli.BashCompleteFunc{}),
						},
					},
				},
				{
					Name:         bucketCreate,
					Usage:        "creates the local bucket",
					ArgsUsage:    bucketArgumentText,
					Flags:        bucketFlags[bucketCreate],
					Action:       bucketHandler,
					BashComplete: flagList,
				},
				{
					Name:         bucketDestroy,
					Usage:        "destroys the local bucket",
					ArgsUsage:    bucketArgumentText,
					Flags:        bucketFlags[bucketDestroy],
					Action:       bucketHandler,
					BashComplete: bucketList([]cli.BashCompleteFunc{}, cmn.LocalBs),
				},
				{
					Name:         commandRename,
					Usage:        "renames the local bucket",
					ArgsUsage:    bucketRenameArgumentText,
					Flags:        bucketFlags[commandRename],
					Action:       bucketHandler,
					BashComplete: bucketList([]cli.BashCompleteFunc{}, cmn.LocalBs),
				},
				{
					Name:         commandList,
					Usage:        "returns all bucket names",
					ArgsUsage:    noArgumentsText,
					Flags:        bucketFlags[commandList],
					Action:       bucketHandler,
					BashComplete: flagList,
				},
				{
					Name:         bucketObjects,
					Usage:        "returns all objects from bucket",
					ArgsUsage:    bucketArgumentText,
					Flags:        bucketFlags[bucketObjects],
					Action:       bucketHandler,
					BashComplete: bucketList([]cli.BashCompleteFunc{}),
				},
				{
					Name:         bucketNWayMirror,
					Usage:        "configures the bucket for n-way mirroring",
					ArgsUsage:    bucketArgumentText,
					Flags:        bucketFlags[bucketNWayMirror],
					Action:       bucketHandler,
					BashComplete: bucketList([]cli.BashCompleteFunc{}),
				},
				{
					Name:         bucketEvict,
					Usage:        "evicts a cloud bucket",
					ArgsUsage:    bucketArgumentText,
					Flags:        bucketFlags[bucketEvict],
					Action:       bucketHandler,
					BashComplete: bucketList([]cli.BashCompleteFunc{}, cmn.CloudBs),
				},
				{
					Name:         bucketSummary,
					Usage:        "displays bucket stats",
					ArgsUsage:    bucketArgumentText,
					Flags:        bucketFlags[bucketSummary],
					Action:       bucketHandler,
					BashComplete: bucketList([]cli.BashCompleteFunc{}),
				},
			},
		},
	}
)

func bucketHandler(c *cli.Context) (err error) {
	baseParams := cliAPIParams(ClusterURL)
	command := c.Command.Name

	bucket, err := bucketFromArgsOrEnv(c)
	// In case of commandRename validation will be done inside renameBucket
	if err != nil && command != commandList && command != commandRename {
		return err
	}

	switch command {
	case bucketCreate:
		err = createBucket(c, baseParams, bucket)
	case bucketDestroy:
		err = destroyBucket(c, baseParams, bucket)
	case commandRename:
		err = renameBucket(c, baseParams)
	case bucketEvict:
		err = evictBucket(c, baseParams, bucket)
	case commandList:
		err = listBucketNames(c, baseParams)
	case bucketObjects:
		err = listBucketObj(c, baseParams, bucket)
	case bucketNWayMirror:
		err = configureNCopies(c, baseParams, bucket)
	case bucketSummary:
		err = statBucket(c, baseParams, bucket)
	default:
		return fmt.Errorf(invalidCmdMsg, command)
	}
	return err
}

func bucketPropsHandler(c *cli.Context) (err error) {
	baseParams := cliAPIParams(ClusterURL)
	command := c.Command.Name

	switch command {
	case commandList:
		err = bucketProps(c, baseParams)
	case propsSet:
		err = setBucketProps(c, baseParams)
	case propsReset:
		err = resetBucketProps(c, baseParams)
	default:
		return fmt.Errorf(invalidCmdMsg, command)
	}

	return err
}

// Creates new local bucket
func createBucket(c *cli.Context, baseParams *api.BaseParams, bucket string) (err error) {
	if err = api.CreateLocalBucket(baseParams, bucket); err != nil {
		return
	}
	_, _ = fmt.Fprintf(c.App.Writer, "%s bucket created\n", bucket)
	return
}

// Destroy local bucket
func destroyBucket(c *cli.Context, baseParams *api.BaseParams, bucket string) (err error) {
	if err = api.DestroyLocalBucket(baseParams, bucket); err != nil {
		return
	}
	_, _ = fmt.Fprintf(c.App.Writer, "%s bucket destroyed\n", bucket)
	return
}

// Rename local bucket
func renameBucket(c *cli.Context, baseParams *api.BaseParams) (err error) {
	bucket, newBucket, err := getRenameBucketParameters(c)
	if err != nil {
		return err
	}

	if err = api.RenameLocalBucket(baseParams, bucket, newBucket); err != nil {
		return
	}
	_, _ = fmt.Fprintf(c.App.Writer, "%s bucket renamed to %s\n", bucket, newBucket)
	return
}

// Evict a cloud bucket
func evictBucket(c *cli.Context, baseParams *api.BaseParams, bucket string) (err error) {
	query := url.Values{cmn.URLParamBckProvider: []string{cmn.CloudBs}}
	if err = api.EvictCloudBucket(baseParams, bucket, query); err != nil {
		return
	}
	_, _ = fmt.Fprintf(c.App.Writer, "%s bucket evicted\n", bucket)
	return
}

// Lists bucket names
func listBucketNames(c *cli.Context, baseParams *api.BaseParams) (err error) {
	bckProvider, err := cmn.BckProviderFromStr(parseStrFlag(c, bckProviderFlag))
	if err != nil {
		return
	}
	bucketNames, err := api.GetBucketNames(baseParams, bckProvider)
	if err != nil {
		return
	}
	printBucketNames(c, bucketNames, parseStrFlag(c, regexFlag), bckProvider, !flagIsSet(c, noHeaderFlag))
	return
}

// Lists objects in bucket
func listBucketObj(c *cli.Context, baseParams *api.BaseParams, bucket string) error {
	objectListFilter, err := newObjectListFilter(c)
	if err != nil {
		return err
	}

	prefix := parseStrFlag(c, prefixFlag)
	showUnmatched := flagIsSet(c, showUnmatchedFlag)
	props := "name,"
	if parseStrFlag(c, objPropsFlag) == "all" {
		props += strings.Join(cmn.GetPropsAll, ",")
	} else {
		props += parseStrFlag(c, objPropsFlag)
		if flagIsSet(c, allFlag) && !strings.Contains(props, "status") {
			// If `all` flag is set print status of the file so that the output is easier to understand -
			// there might be multiple files with the same name listed (e.g EC replicas)
			props += ",status"
		}
	}

	query := url.Values{}
	query.Add(cmn.URLParamBckProvider, parseStrFlag(c, bckProviderFlag))
	query.Add(cmn.URLParamPrefix, prefix)

	msg := &cmn.SelectMsg{Props: props, Prefix: prefix}
	if flagIsSet(c, fastFlag) {
		msg.Fast = true
		objList, err := api.ListBucketFast(baseParams, bucket, msg, query)
		if err != nil {
			return err
		}

		return printObjectNames(c, objList.Entries, objectListFilter, showUnmatched, !flagIsSet(c, noHeaderFlag))
	}

	if flagIsSet(c, markerFlag) {
		msg.PageMarker = parseStrFlag(c, markerFlag)
	}
	pagesize, err := strconv.Atoi(parseStrFlag(c, pageSizeFlag))
	if err != nil {
		return err
	}
	limit, err := strconv.Atoi(parseStrFlag(c, objLimitFlag))
	if err != nil {
		return err
	}

	// set page size to limit if limit is less than page size
	msg.PageSize = pagesize
	if limit > 0 && (limit < pagesize || (limit < 1000 && pagesize == 0)) {
		msg.PageSize = limit
	}

	// retrieve the bucket content page by page and print on the fly
	if flagIsSet(c, pagedFlag) {
		pageCounter, maxPages, toShow := 0, parseIntFlag(c, maxPagesFlag), limit
		for {
			objList, err := api.ListBucketPage(baseParams, bucket, msg, query)
			if err != nil {
				return err
			}

			// print exact number of objects if it is `limit`ed: in case of
			// limit > page size, the last page is printed partially
			var toPrint []*cmn.BucketEntry
			if limit > 0 && toShow < len(objList.Entries) {
				toPrint = objList.Entries[:toShow]
			} else {
				toPrint = objList.Entries
			}
			err = printObjectProps(c, toPrint, objectListFilter, props, showUnmatched, !flagIsSet(c, noHeaderFlag))
			if err != nil {
				return err
			}

			// interrupt the loop if:
			// 1. the last page is printed
			// 2. maximum pages are printed
			// 3. printed `limit` number of objects
			if msg.PageMarker == "" {
				return nil
			}
			pageCounter++
			if maxPages > 0 && pageCounter >= maxPages {
				return nil
			}
			if limit > 0 {
				toShow -= len(objList.Entries)
				if toShow <= 0 {
					return nil
				}
			}
		}
	}

	// retrieve the entire bucket list and print it
	objList, err := api.ListBucket(baseParams, bucket, msg, limit, query)
	if err != nil {
		return err
	}

	return printObjectProps(c, objList.Entries, objectListFilter, props, showUnmatched, !flagIsSet(c, noHeaderFlag))
}

// show bucket statistics
func statBucket(c *cli.Context, baseParams *api.BaseParams, bucket string) error {
	// make new function to capture arguments
	fStats := func() error {
		return statsBucketSync(c, baseParams, bucket)
	}
	return cmn.WaitForFunc(fStats, longCommandTime)
}

// show bucket statistics
func statsBucketSync(c *cli.Context, baseParams *api.BaseParams, bucket string) error {
	prefix := parseStrFlag(c, prefixFlag)
	props := "name,size,status,copies,iscached"

	query := url.Values{}
	query.Add(cmn.URLParamBckProvider, parseStrFlag(c, bckProviderFlag))
	query.Add(cmn.URLParamPrefix, prefix)

	msg := &cmn.SelectMsg{Props: props, Prefix: prefix}
	if flagIsSet(c, pageSizeFlag) {
		pagesize, err := strconv.Atoi(parseStrFlag(c, pageSizeFlag))
		if err != nil {
			return err
		}
		msg.PageSize = pagesize
	}
	objList, err := api.ListBucket(baseParams, bucket, msg, 0, query)
	if err != nil {
		return err
	}

	var regex *regexp.Regexp
	if regexStr := parseStrFlag(c, regexFlag); regexStr != "" {
		regex, err = regexp.Compile(regexStr)
		if err != nil {
			return err
		}
	}

	totalCnt, localCnt, dataCnt := int64(0), int64(0), int64(0)
	totalSz, localSz, dataSz := int64(0), int64(0), int64(0)
	for _, obj := range objList.Entries {
		if regex != nil && !regex.MatchString(obj.Name) {
			continue
		}

		totalCnt++
		totalSz += obj.Size
		if obj.Status == cmn.ObjStatusOK {
			dataCnt++
			dataSz += obj.Size
		}
		if obj.IsCached {
			localCnt++
			localSz += obj.Size
		}
	}

	statList := []struct {
		Name  string
		Total string
		Data  string
		Local string
	}{
		{"Object count", strconv.FormatInt(totalCnt, 10), strconv.FormatInt(dataCnt, 10), strconv.FormatInt(localCnt, 10)},
		{"Object size", cmn.B2S(totalSz, 2), cmn.B2S(dataSz, 2), cmn.B2S(localSz, 2)},
	}

	return templates.DisplayOutput(statList, c.App.Writer, templates.BucketStatTmpl)
}

// replace user-friendly properties like `access=ro` with real values
// like `aattrs = GET | HEAD`. All numbers are passed to API as is
func reformatBucketProps(baseParams *api.BaseParams, bucket string, query url.Values, nvs cmn.SimpleKVs) error {
	if v, ok := nvs[cmn.HeaderBucketAccessAttrs]; ok {
		props, err := api.HeadBucket(baseParams, bucket, query)
		if err != nil {
			return err
		}

		writeAccess := uint64(cmn.AccessPUT | cmn.AccessDELETE | cmn.AccessColdGET)
		switch v {
		case readwriteBucketAccess:
			aattrs := cmn.MakeAccess(props.AccessAttrs, cmn.AllowAccess, writeAccess)
			nvs[cmn.HeaderBucketAccessAttrs] = strconv.FormatUint(aattrs, 10)
		case readonlyBucketAccess:
			aattrs := cmn.MakeAccess(props.AccessAttrs, cmn.DenyAccess, writeAccess)
			nvs[cmn.HeaderBucketAccessAttrs] = strconv.FormatUint(aattrs, 10)
		default:
			if _, err := strconv.ParseUint(v, 10, 64); err != nil {
				return fmt.Errorf("invalid bucket access %q, must be an integer, %q or %q",
					v, readonlyBucketAccess, readwriteBucketAccess)
			}
		}
	}
	return nil
}

// Sets bucket properties
func setBucketProps(c *cli.Context, baseParams *api.BaseParams) (err error) {
	// For setting bucket props via action message
	if flagIsSet(c, jsonspecFlag) {
		return setBucketPropsJSON(c, baseParams)
	}

	var (
		propsArgs = c.Args().Tail()
		bucket    = c.Args().First()
	)
	bckProvider, err := cmn.BckProviderFromStr(parseStrFlag(c, bckProviderFlag))
	if err != nil {
		return
	}

	if strings.Contains(bucket, keyAndValueSeparator) {
		// First argument is a key-value pair -> bucket should be read from env variable
		bucketEnv, ok := os.LookupEnv(aisBucketEnvVar)
		if !ok {
			return missingArgumentsError(c, "bucket name")
		}
		bucket = bucketEnv
		propsArgs = c.Args()
	}

	if len(propsArgs) == 0 {
		return errors.New("expected at least one key-value pair")
	}

	if err = canReachBucket(baseParams, bucket, bckProvider); err != nil {
		return
	}

	// For setting bucket props via URL query string
	nvs, err := makePairs(propsArgs)
	if err != nil {
		return
	}

	query := url.Values{cmn.URLParamBckProvider: []string{bckProvider}}
	if err = reformatBucketProps(baseParams, bucket, query, nvs); err != nil {
		return
	}
	if err = api.SetBucketProps(baseParams, bucket, nvs, query); err != nil {
		return
	}
	_, _ = fmt.Fprintln(c.App.Writer)
	return
}

func setBucketPropsJSON(c *cli.Context, baseParams *api.BaseParams) error {
	bucket, err := bucketFromArgsOrEnv(c)
	if err != nil {
		return err
	}
	bckProvider, err := cmn.BckProviderFromStr(parseStrFlag(c, bckProviderFlag))
	if err != nil {
		return err
	}
	inputProps := parseStrFlag(c, jsonspecFlag)

	var props cmn.BucketProps
	if err := json.Unmarshal([]byte(inputProps), &props); err != nil {
		return err
	}

	query := url.Values{cmn.URLParamBckProvider: []string{bckProvider}}
	if err := api.SetBucketPropsMsg(baseParams, bucket, props, query); err != nil {
		return err
	}

	_, _ = fmt.Fprintln(c.App.Writer)
	return nil
}

// Resets bucket props
func resetBucketProps(c *cli.Context, baseParams *api.BaseParams) (err error) {
	bucket, err := bucketFromArgsOrEnv(c)
	if err != nil {
		return err
	}

	bckProvider, err := cmn.BckProviderFromStr(parseStrFlag(c, bckProviderFlag))
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

	_, _ = fmt.Fprintf(c.App.Writer, "Reset %s bucket properties\n", bucket)
	return
}

// Get bucket props
func bucketProps(c *cli.Context, baseParams *api.BaseParams) (err error) {
	bucket, err := bucketFromArgsOrEnv(c)
	if err != nil {
		return err
	}

	bckProvider, err := cmn.BckProviderFromStr(parseStrFlag(c, bckProviderFlag))
	if err != nil {
		return
	}
	query := url.Values{cmn.URLParamBckProvider: []string{bckProvider}}
	bckProps, err := api.HeadBucket(baseParams, bucket, query)
	if err != nil {
		return
	}

	if flagIsSet(c, jsonFlag) {
		return templates.DisplayOutput(bckProps, c.App.Writer, templates.BucketPropsTmpl, true)
	}

	return printBckHeadTable(c, bckProps)
}

func printBckHeadTable(c *cli.Context, props *cmn.BucketProps) error {
	// List instead of map to keep properties in the same order always.
	// All names are one word ones - for easier parsing
	propList := []struct {
		Name string
		Val  string
	}{
		{"Provider", props.CloudProvider},
		{"Access", props.AccessToStr()},
		{"Checksum", props.Cksum.String()},
		{"Mirror", props.Mirror.String()},
		{"EC", props.EC.String()},
		{"LRU", props.LRU.String()},
		{"Versioning", props.Versioning.String()},
		{"Tiering", props.Tiering.String()},
	}

	return templates.DisplayOutput(propList, c.App.Writer, templates.BucketPropsSimpleTmpl)
}

// Configure bucket as n-way mirror
func configureNCopies(c *cli.Context, baseParams *api.BaseParams, bucket string) (err error) {
	bckProvider, err := cmn.BckProviderFromStr(parseStrFlag(c, bckProviderFlag))
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
	_, _ = fmt.Fprintf(c.App.Writer, "Configured %s to replicate %d copies of its objects\n", bucket, copies)
	return
}

// HELPERS

// Rename bucket expects 2 arguments - bucket name and new bucket name.
// This function returns bucket name and new bucket name based on arguments provided to the command
// and AIS_BUCKET env variable. In case something is missing it also generates a meaningful error message.
func getRenameBucketParameters(c *cli.Context) (bucket, newBucket string, err error) {
	var (
		args     = c.Args()
		argCount = c.NArg()

		bucketEnv, envVarSet = os.LookupEnv(aisBucketEnvVar)
	)

	if argCount == 0 && envVarSet || (argCount == 1 && !envVarSet) {
		return "", "", missingArgumentsError(c, "new bucket name")
	} else if argCount == 0 {
		return "", "", missingArgumentsError(c, "bucket name", "new bucket name")
	}

	if argCount == 1 {
		bucket = bucketEnv      // AIS_BUCKET was set - treat it as the value of the first argument
		newBucket = args.Get(0) // Treat the only argument as name of new bucket
	} else {
		bucket = args.Get(0)
		newBucket = args.Get(1)
	}

	return bucket, newBucket, nil
}

func printBucketNames(c *cli.Context, bucketNames *cmn.BucketNames, regex, bckProvider string, showHeaders bool) {
	if bckProvider == cmn.LocalBs || bckProvider == "" {
		localBuckets := regexFilter(regex, bucketNames.Local)
		if showHeaders {
			_, _ = fmt.Fprintf(c.App.Writer, "Local Buckets (%d)\n", len(localBuckets))
		}
		for _, bucket := range localBuckets {
			_, _ = fmt.Fprintln(c.App.Writer, bucket)
		}
		if bckProvider == cmn.LocalBs {
			return
		}
		if showHeaders {
			_, _ = fmt.Fprintln(c.App.Writer)
		}
	}

	cloudBuckets := regexFilter(regex, bucketNames.Cloud)
	if showHeaders {
		_, _ = fmt.Fprintf(c.App.Writer, "Cloud Buckets (%d)\n", len(cloudBuckets))
	}
	for _, bucket := range cloudBuckets {
		_, _ = fmt.Fprintln(c.App.Writer, bucket)
	}
}

func buildOutputTemplate(props string, showHeaders bool) (string, error) {
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
		headSb.WriteString(strings.Title(field) + "\t")
		bodySb.WriteString(templates.ObjectPropsMap[field])
	}
	headSb.WriteString("\n")
	bodySb.WriteString("\n{{end}}")

	if showHeaders {
		return headSb.String() + bodySb.String() + "\n", nil
	}

	return bodySb.String(), nil
}

func printObjectProps(c *cli.Context, entries []*cmn.BucketEntry, objectFilter *objectListFilter, props string, showUnmatched, showHeaders bool) error {
	outputTemplate, err := buildOutputTemplate(props, showHeaders)
	if err != nil {
		return err
	}

	matchingEntries, rest := objectFilter.filter(entries)

	err = templates.DisplayOutput(matchingEntries, c.App.Writer, outputTemplate)
	if err != nil {
		return err
	}

	if showHeaders && showUnmatched {
		outputTemplate = "Unmatched objects:\n" + outputTemplate
		err = templates.DisplayOutput(rest, c.App.Writer, outputTemplate)
	}

	return err
}

func printObjectNames(c *cli.Context, entries []*cmn.BucketEntry, objectFilter *objectListFilter, showUnmatched, showHeaders bool) error {
	outputTemplate := "Name\n{{range $obj := .}}{{$obj.Name}}\n{{end}}\n"
	if !showHeaders {
		outputTemplate = "{{range $obj := .}}{{$obj.Name}}\n{{end}}"
	}
	matchingEntries, rest := objectFilter.filter(entries)

	err := templates.DisplayOutput(matchingEntries, c.App.Writer, outputTemplate)
	if err != nil {
		return err
	}

	if showHeaders && showUnmatched {
		outputTemplate = "Unmatched objects:\n" + outputTemplate
		err = templates.DisplayOutput(rest, c.App.Writer, outputTemplate)
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

	// if fastFlag is enabled, allFlag is enabled automatically because obj.Status is unset
	if !flagIsSet(c, allFlag) && !flagIsSet(c, fastFlag) {
		// Filter out files with status different than OK
		objFilter.addFilter(func(obj *cmn.BucketEntry) bool { return obj.Status == cmn.ObjStatusOK })
	}

	if regexStr := parseStrFlag(c, regexFlag); regexStr != "" {
		regex, err := regexp.Compile(regexStr)
		if err != nil {
			return nil, err
		}

		objFilter.addFilter(func(obj *cmn.BucketEntry) bool { return regex.MatchString(obj.Name) })
	}

	if bashTemplate := parseStrFlag(c, templateFlag); bashTemplate != "" {
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
