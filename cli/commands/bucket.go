// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles bucket operations.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

const (
	readonlyBucketAccess  = "ro"
	readwriteBucketAccess = "rw"

	// max wait time for a function finishes before printing "Please wait"
	longCommandTime = 10 * time.Second
)

func validateBucket(c *cli.Context, baseParams *api.BaseParams, b, tag string, optional bool) (bucket, provider string, err error) {
	if provider, err = bucketProvider(c); err != nil {
		return
	}
	bucket = b
	if bucket == "" {
		if optional {
			return
		}
		if tag != "" {
			err = incorrectUsageError(c, fmt.Errorf("'%s': missing bucket name", tag))
		} else {
			err = incorrectUsageError(c, errors.New("missing bucket name"))
		}
		return
	}
	err = canReachBucket(baseParams, bucket, provider)
	return
}

// Creates new ais buckets
func createBuckets(c *cli.Context, baseParams *api.BaseParams, buckets []string) (err error) {
	// TODO: on "soft" error (bucket already exists) we will
	// emit zero exit code - this may be problematic when using
	// in scripts.
	for _, bucket := range buckets {
		if err := api.CreateBucket(baseParams, bucket); err != nil {
			if herr, ok := err.(*cmn.HTTPError); ok {
				if herr.Status == http.StatusConflict {
					fmt.Fprintf(c.App.Writer, "Bucket %q already exists\n", bucket)
					continue
				}
			}
			return err
		}
		fmt.Fprintf(c.App.Writer, "%s bucket created\n", bucket)
	}
	return nil
}

// Destroy ais buckets
func destroyBuckets(c *cli.Context, baseParams *api.BaseParams, buckets []string) (err error) {
	// TODO: on "soft" error (bucket does not exist) we will
	// emit zero exit code - this may be problematic when using
	// in scripts.
	for _, bucket := range buckets {
		if err := api.DestroyBucket(baseParams, bucket); err != nil {
			if herr, ok := err.(*cmn.HTTPError); ok {
				if herr.Status == http.StatusNotFound {
					fmt.Fprintf(c.App.Writer, "Bucket %q does not exist\n", bucket)
					continue
				}
			}
			return err
		}
		fmt.Fprintf(c.App.Writer, "%s bucket destroyed\n", bucket)
	}
	return nil
}

// Rename ais bucket
func renameBucket(c *cli.Context, baseParams *api.BaseParams, bucket, newBucket string) (err error) {
	if err = canReachBucket(baseParams, bucket, cmn.AIS); err != nil {
		return
	}
	if err = api.RenameBucket(baseParams, bucket, newBucket); err != nil {
		return
	}

	msgFmt := "Renaming bucket %s to %s in progress.\nTo check the status, run: ais show xaction %s %s\n"
	fmt.Fprintf(c.App.Writer, msgFmt, bucket, newBucket, cmn.ActRenameLB, bucket)
	return
}

// Copy ais bucket
func copyBucket(c *cli.Context, baseParams *api.BaseParams, bucket, newBucket string) (err error) {
	if err = api.CopyBucket(baseParams, bucket, newBucket); err != nil {
		return
	}

	msgFmt := "Copying bucket %s to %s in progress.\nTo check the status, run: ais show xaction %s %s\n"
	fmt.Fprintf(c.App.Writer, msgFmt, bucket, newBucket, cmn.ActCopyBucket, bucket)
	return
}

// Evict a cloud bucket
func evictBucket(c *cli.Context, baseParams *api.BaseParams, bucket string) (err error) {
	query := url.Values{cmn.URLParamProvider: []string{cmn.Cloud}}
	if err = api.EvictCloudBucket(baseParams, bucket, query); err != nil {
		return
	}
	fmt.Fprintf(c.App.Writer, "%s bucket evicted\n", bucket)
	return
}

// List bucket names
func listBucketNames(c *cli.Context, baseParams *api.BaseParams, provider string) (err error) {
	bucketNames, err := api.GetBucketNames(baseParams, provider)
	if err != nil {
		return
	}
	printBucketNames(c, bucketNames, parseStrFlag(c, regexFlag), provider, !flagIsSet(c, noHeaderFlag))
	return
}

// Lists objects in bucket
func listBucketObj(c *cli.Context, baseParams *api.BaseParams, bucket string, provider string) error {
	err := canReachBucket(baseParams, bucket, provider)
	if err != nil {
		return err
	}

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
		if flagIsSet(c, allItemsFlag) && !strings.Contains(props, "status") {
			// If `all` flag is set print status of the file so that the output is easier to understand -
			// there might be multiple files with the same name listed (e.g EC replicas)
			props += ",status"
		}
	}

	query := url.Values{}
	query.Add(cmn.URLParamProvider, parseStrFlag(c, providerFlag))
	query.Add(cmn.URLParamPrefix, prefix)
	if flagIsSet(c, cachedFlag) {
		if provider == cmn.AIS {
			fmt.Fprintf(c.App.ErrWriter, "warning: ignoring %s flag: irrelevant for ais buckets\n", cachedFlag.Name)
		} else {
			query.Add(cmn.URLParamCached, "true")
		}
	}

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

func bucketDetails(c *cli.Context, baseParams *api.BaseParams, bucket, provider string, fast bool) error {
	fDetails := func() error {
		return bucketDetailsSync(c, baseParams, bucket, provider, fast)
	}
	return cmn.WaitForFunc(fDetails, longCommandTime)
}

// The function shows bucket details
func bucketDetailsSync(c *cli.Context, baseParams *api.BaseParams, bucket, provider string, fast bool) error {
	// Request bucket summaries
	msg := &cmn.SelectMsg{Fast: fast}
	summaries, err := api.GetBucketsSummaries(baseParams, bucket, provider, msg)
	if err != nil {
		return err
	}
	return templates.DisplayOutput(summaries, c.App.Writer, templates.BucketsSummariesTmpl)
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
	var (
		provider  string
		propsArgs = c.Args().Tail()
		bucket    = c.Args().First()
	)

	// For setting bucket props via action message
	if flagIsSet(c, jsonspecFlag) {
		return setBucketPropsJSON(c, baseParams)
	}
	if strings.Contains(bucket, keyAndValueSeparator) {
		// First argument is a key-value pair -> bucket should be read from env variable
		bucket = ""
		propsArgs = c.Args()
	}
	if len(propsArgs) == 0 {
		return missingArgumentsError(c, "property key-value pairs")
	}
	if bucket, provider, err = validateBucket(c, baseParams, bucket, "", false /* optional */); err != nil {
		return
	}

	// For setting bucket props via URL query string
	nvs, err := makePairs(propsArgs)
	if err != nil {
		return
	}

	query := url.Values{cmn.URLParamProvider: []string{provider}}
	if err = reformatBucketProps(baseParams, bucket, query, nvs); err != nil {
		return
	}
	if err = api.SetBucketProps(baseParams, bucket, nvs, query); err != nil {
		return
	}
	fmt.Fprintln(c.App.Writer, "Bucket props have been successfully updated.")
	return
}

func setBucketPropsJSON(c *cli.Context, baseParams *api.BaseParams) (err error) {
	var (
		provider   string
		props      cmn.BucketProps
		bucket     = c.Args().First()
		inputProps = parseStrFlag(c, jsonspecFlag)
	)
	if bucket, provider, err = validateBucket(c, baseParams, bucket, "", false /* optional */); err != nil {
		return
	}
	if err := json.Unmarshal([]byte(inputProps), &props); err != nil {
		return err
	}
	query := url.Values{cmn.URLParamProvider: []string{provider}}
	if err := api.SetBucketPropsMsg(baseParams, bucket, props, query); err != nil {
		return err
	}

	fmt.Fprintln(c.App.Writer)
	return nil
}

// Resets bucket props
func resetBucketProps(c *cli.Context, baseParams *api.BaseParams) (err error) {
	var (
		provider string
		bucket   = c.Args().First()
	)
	if bucket, provider, err = validateBucket(c, baseParams, bucket, "", false /* optional */); err != nil {
		return
	}
	query := url.Values{cmn.URLParamProvider: []string{provider}}
	if err = canReachBucket(baseParams, bucket, provider); err != nil {
		return
	}

	if err = api.ResetBucketProps(baseParams, bucket, query); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "Reset %s bucket properties\n", bucket)
	return
}

// Get bucket props
func listBucketProps(c *cli.Context, baseParams *api.BaseParams) (err error) {
	var (
		provider string
		bucket   = c.Args().First()
	)
	if bucket, provider, err = validateBucket(c, baseParams, bucket, "", false /* optional */); err != nil {
		return
	}
	query := url.Values{cmn.URLParamProvider: []string{provider}}
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
	provider, err := bucketProvider(c)
	if err != nil {
		return
	}
	if err = checkFlags(c, []cli.Flag{copiesFlag}); err != nil {
		return
	}
	if err = canReachBucket(baseParams, bucket, provider); err != nil {
		return
	}
	copies := c.Int(copiesFlag.Name)
	if err = api.MakeNCopies(baseParams, bucket, copies); err != nil {
		return
	}
	_, _ = fmt.Fprintf(c.App.Writer, "Configured %s to replicate %d copies of its objects\n", bucket, copies)
	return
}

// Makes every object in a bucket erasure coded
func ecEncode(c *cli.Context, baseParams *api.BaseParams, bucket string) (err error) {
	provider, err := bucketProvider(c)
	if err != nil {
		return
	}
	if err = canReachBucket(baseParams, bucket, provider); err != nil {
		return
	}
	if err = api.ECEncodeBucket(baseParams, bucket); err != nil {
		return
	}
	fmt.Fprintf(c.App.Writer, "Encoding %s objects is running, use '%s %s %s %s %s' to see the progress\n",
		bucket, cliName, commandShow, subcmdXaction, cmn.ActECEncode, bucket)
	return
}

// This function returns bucket name and new bucket name based on arguments provided to the command.
// In case something is missing it also generates a meaningful error message.
func getOldNewBucketName(c *cli.Context) (bucket, newBucket string, err error) {
	if c.NArg() == 0 {
		return "", "", missingArgumentsError(c, "bucket name", "new bucket name")
	}
	if c.NArg() == 1 {
		return "", "", missingArgumentsError(c, "new bucket name")
	}

	bucket, newBucket = c.Args().Get(0), c.Args().Get(1)
	return
}

func printBucketNames(c *cli.Context, bucketNames *cmn.BucketNames, regex, provider string, showHeaders bool) {
	isAISBck := cmn.IsProviderAIS(provider)
	if isAISBck || provider == "" {
		aisBuckets := regexFilter(regex, bucketNames.AIS)
		sort.Strings(aisBuckets) // sort by name
		if showHeaders {
			fmt.Fprintf(c.App.Writer, "AIS Buckets (%d)\n\\_\n", len(aisBuckets))
		}
		for _, bucket := range aisBuckets {
			fmt.Fprintf(c.App.Writer, "  %s\n", bucket)
		}
		if isAISBck {
			return
		}
		if showHeaders {
			fmt.Fprintln(c.App.Writer)
		}
	}

	cloudBuckets := regexFilter(regex, bucketNames.Cloud)
	sort.Strings(cloudBuckets) // sort by name
	if showHeaders {
		fmt.Fprintf(c.App.Writer, "Cloud Buckets (%d)\n\\_\n", len(cloudBuckets))
	}
	for _, bucket := range cloudBuckets {
		fmt.Fprintf(c.App.Writer, "  %s\n", bucket)
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
		return headSb.String() + bodySb.String(), nil
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
	outputTemplate := "Name\n{{range $obj := .}}{{$obj.Name}}\n{{end}}"
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
	if !flagIsSet(c, allItemsFlag) && !flagIsSet(c, fastFlag) {
		// Filter out files with status different than OK
		objFilter.addFilter(func(obj *cmn.BucketEntry) bool { return obj.IsStatusOK() })
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
