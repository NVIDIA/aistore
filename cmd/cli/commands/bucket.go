// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles bucket operations.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/fatih/color"
	"github.com/urfave/cli"
	"k8s.io/apimachinery/pkg/util/duration"
)

const (
	emptyOrigin = "none"

	// max wait time for a function finishes before printing "Please wait"
	longCommandTime = 10 * time.Second

	fmtXactFailed    = "Failed to %s (%q => %q)\n"
	fmtXactSucceeded = "Done.\n"

	fmtXactStarted     = "%s (%q => %q) ...\n"
	fmtXactStatusCheck = "%s (%q => %q) is in progress.\nTo check the status, run: ais show job xaction %s %s\n"
)

// Creates new ais bucket
func createBucket(c *cli.Context, bck cmn.Bck, props *cmn.BucketPropsToUpdate) (err error) {
	if err = api.CreateBucket(defaultAPIParams, bck, props); err != nil {
		if herr, ok := err.(*cmn.ErrHTTP); ok {
			if herr.Status == http.StatusConflict {
				desc := fmt.Sprintf("Bucket %q already exists", bck)
				if flagIsSet(c, ignoreErrorFlag) {
					fmt.Fprint(c.App.Writer, desc)
					return nil
				}
				return errors.New(desc)
			}
			return fmt.Errorf("failed to create %q: %s", bck, herr.Message)
		}
		return fmt.Errorf("failed to create %q: %v", bck, err)
	}
	if props == nil {
		fmt.Fprintf(c.App.Writer,
			"%q created (see %s/blob/master/docs/bucket.md#default-bucket-properties)\n", bck, cmn.GitHubHome)
	} else {
		fmt.Fprintf(c.App.Writer, "%q created\n", bck)
	}
	return
}

// Destroy ais buckets
func destroyBuckets(c *cli.Context, buckets []cmn.Bck) (err error) {
	for _, bck := range buckets {
		if err = api.DestroyBucket(defaultAPIParams, bck); err != nil {
			if cmn.IsStatusNotFound(err) {
				desc := fmt.Sprintf("Bucket %q does not exist", bck)
				if !flagIsSet(c, ignoreErrorFlag) {
					return errors.New(desc)
				}
				fmt.Fprint(c.App.Writer, desc)
				continue
			}
			return err
		}
		fmt.Fprintf(c.App.Writer, "%q bucket destroyed\n", bck)
	}
	return nil
}

// Mv ais bucket
func mvBucket(c *cli.Context, fromBck, toBck cmn.Bck) (err error) {
	var xactID string

	if _, err = headBucket(fromBck); err != nil {
		return
	}

	if xactID, err = api.RenameBucket(defaultAPIParams, fromBck, toBck); err != nil {
		return
	}

	if !flagIsSet(c, waitFlag) {
		fmt.Fprintf(c.App.Writer, fmtXactStatusCheck, "Renaming bucket", fromBck, toBck, apc.ActMoveBck, toBck)
		return
	}

	fmt.Fprintf(c.App.Writer, fmtXactStarted, "Renaming bucket", fromBck, toBck)
	if err = waitForXactionCompletion(defaultAPIParams, api.XactReqArgs{ID: xactID}); err != nil {
		fmt.Fprintf(c.App.Writer, fmtXactFailed, "rename", fromBck, toBck)
	} else {
		fmt.Fprint(c.App.Writer, fmtXactSucceeded)
	}
	return
}

// Copy ais bucket
func copyBucket(c *cli.Context, fromBck, toBck cmn.Bck, msg *cmn.CopyBckMsg) (err error) {
	var xactID string

	if xactID, err = api.CopyBucket(defaultAPIParams, fromBck, toBck, msg); err != nil {
		return
	}

	if !flagIsSet(c, waitFlag) {
		fmt.Fprintf(c.App.Writer, fmtXactStatusCheck, "Copying bucket", fromBck, toBck, apc.ActCopyBck, toBck)
		return
	}

	fmt.Fprintf(c.App.Writer, fmtXactStarted, "Copying bucket", fromBck, toBck)
	if err = waitForXactionCompletion(defaultAPIParams, api.XactReqArgs{ID: xactID}); err != nil {
		fmt.Fprintf(c.App.Writer, fmtXactFailed, "copy", fromBck, toBck)
	} else {
		fmt.Fprint(c.App.Writer, fmtXactSucceeded)
	}
	return
}

// Evict remote bucket
func evictBucket(c *cli.Context, bck cmn.Bck) (err error) {
	if flagIsSet(c, dryRunFlag) {
		fmt.Fprintf(c.App.Writer, "EVICT: %q\n", bck)
		return
	}
	if err = ensureHasProvider(bck, c.Command.Name); err != nil {
		return
	}
	if err = api.EvictRemoteBucket(defaultAPIParams, bck, flagIsSet(c, keepMDFlag)); err != nil {
		return
	}
	fmt.Fprintf(c.App.Writer, "%q bucket evicted\n", bck)
	return
}

type (
	bucketFilter func(cmn.Bck) bool
)

func listBuckets(c *cli.Context, query cmn.QueryBcks) (err error) {
	// TODO: Think if there is a need to make generic filter for buckets as well ?
	var (
		filter = func(_ cmn.Bck) bool { return true }
		regex  *regexp.Regexp
	)
	if regexStr := parseStrFlag(c, regexFlag); regexStr != "" {
		regex, err = regexp.Compile(regexStr)
		if err != nil {
			return
		}
		filter = func(bck cmn.Bck) bool { return regex.MatchString(bck.Name) }
	}

	bcks, err := api.ListBuckets(defaultAPIParams, query)
	if err != nil {
		return
	}
	printBuckets(c, bcks, !flagIsSet(c, noHeaderFlag), filter)
	return
}

// Lists objects in a bucket; include archived content if requested
func listObjects(c *cli.Context, bck cmn.Bck) error {
	prefix := parseStrFlag(c, prefixFlag)
	listArch := flagIsSet(c, listArchFlag)
	return _doListObj(c, bck, prefix, listArch)
}

// TODO: refactor and split options
func _doListObj(c *cli.Context, bck cmn.Bck, prefix string, listArch bool) error {
	var (
		showUnmatched         = flagIsSet(c, showUnmatchedFlag)
		msg                   = &apc.ListObjsMsg{Prefix: prefix}
		objectListFilter, err = newObjectListFilter(c)
	)
	if err != nil {
		return err
	}
	if flagIsSet(c, listCachedFlag) {
		msg.SetFlag(apc.LsPresent)
	}
	if listArch {
		msg.SetFlag(apc.LsArchDir)
	}
	if flagIsSet(c, allItemsFlag) {
		msg.SetFlag(apc.LsMisplaced)
	}
	props := strings.Split(parseStrFlag(c, objPropsFlag), ",")
	if flagIsSet(c, nameOnlyFlag) {
		msg.SetFlag(apc.LsNameOnly)
		msg.Props = apc.GetPropsName
		if cos.StringInSlice("all", props) || cos.StringInSlice(apc.GetPropsStatus, props) {
			msg.AddProps(apc.GetPropsStatus)
		}
	} else {
		if cos.StringInSlice("all", props) {
			msg.AddProps(apc.GetPropsAll...)
		} else {
			msg.AddProps(apc.GetPropsName)
			msg.AddProps(props...)
		}
	}
	if flagIsSet(c, allItemsFlag) {
		// If `all` flag is set print status of the file. Multiple files with
		// the same name can be displayed (e.g, due to mirroring, EC), so
		// the status helps to tell the real object from its replicas.
		msg.AddProps(apc.GetPropsStatus)
	}
	if flagIsSet(c, startAfterFlag) {
		msg.StartAfter = parseStrFlag(c, startAfterFlag)
	}
	pageSize := parseIntFlag(c, pageSizeFlag)
	limit := parseIntFlag(c, objLimitFlag)
	if pageSize < 0 {
		return fmt.Errorf("page size (%d) cannot be negative", pageSize)
	}
	if limit < 0 {
		return fmt.Errorf("max object count (%d) cannot be negative", limit)
	}
	// set page size to limit if limit is less than page size
	msg.PageSize = uint(pageSize)
	if limit > 0 && (limit < pageSize || (limit < 1000 && pageSize == 0)) {
		msg.PageSize = uint(limit)
	}

	// retrieve the bucket content page by page and print on the fly
	if flagIsSet(c, pagedFlag) {
		pageCounter, maxPages, toShow := 0, parseIntFlag(c, maxPagesFlag), limit
		for {
			objList, err := api.ListObjectsPage(defaultAPIParams, bck, msg)
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
			err = printObjectProps(c, toPrint, objectListFilter, msg.Props, showUnmatched, !flagIsSet(c, noHeaderFlag))
			if err != nil {
				return err
			}

			// interrupt the loop if:
			// 1. the last page is printed
			// 2. maximum pages are printed
			// 3. printed `limit` number of objects
			if msg.ContinuationToken == "" {
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

	cb := func(ctx *api.ProgressContext) {
		fmt.Fprintf(c.App.Writer, "\rFetched %d objects (elapsed: %s)",
			ctx.Info().Count, duration.HumanDuration(ctx.Elapsed()))
		// If it is a final message, move to new line, to keep output tidy
		if ctx.IsFinished() {
			fmt.Fprintln(c.App.Writer)
		}
	}
	ctx := api.NewProgressContext(cb, longCommandTime)

	// retrieve the entire bucket list and print it
	objList, err := api.ListObjectsWithOpts(defaultAPIParams, bck, msg, uint(limit), ctx, false /*don't-lookup-remote*/)
	if err != nil {
		return err
	}

	return printObjectProps(c, objList.Entries, objectListFilter, msg.Props, showUnmatched, !flagIsSet(c, noHeaderFlag))
}

func fetchSummaries(query cmn.QueryBcks, fast, cached bool) (summaries cmn.BckSummaries, err error) {
	fDetails := func() (err error) {
		msg := &cmn.BckSummMsg{Cached: cached, Fast: fast}
		summaries, err = api.GetBucketsSummaries(defaultAPIParams, query, msg)
		return
	}
	err = cmn.WaitForFunc(fDetails, longCommandTime)
	return
}

// Replace user-friendly properties like:
//  * `backend_bck=gcp://bucket_name` with `backend_bck.name=bucket_name` and
//    `backend_bck.provider=gcp` so they match the expected fields in structs.
//  * `backend_bck=none` with `backend_bck.name=""` and `backend_bck.provider=""`.
func reformatBackendProps(c *cli.Context, nvs cos.SimpleKVs) (err error) {
	var (
		originBck cmn.Bck
		v         string
		ok        bool
	)

	if v, ok = nvs[apc.PropBackendBck]; ok {
		delete(nvs, apc.PropBackendBck)
	} else if v, ok = nvs[apc.PropBackendBckName]; !ok {
		goto validate
	}

	if v != emptyOrigin {
		if originBck, err = parseBckURI(c, v, true /*requireProviderInURI*/); err != nil {
			return fmt.Errorf("invalid %q: %v", apc.PropBackendBck, err)
		}
	}

	nvs[apc.PropBackendBckName] = originBck.Name
	if v, ok = nvs[apc.PropBackendBckProvider]; ok && v != "" {
		nvs[apc.PropBackendBckProvider], err = cmn.NormalizeProvider(v)
	} else {
		nvs[apc.PropBackendBckProvider] = originBck.Provider
	}

validate:
	if nvs[apc.PropBackendBckProvider] != "" && nvs[apc.PropBackendBckName] == "" {
		return fmt.Errorf("invalid %q: bucket name cannot be empty when bucket provider (%q) is set",
			apc.PropBackendBckName, apc.PropBackendBckProvider)
	}
	return err
}

// Sets bucket properties
func setBucketProps(c *cli.Context, bck cmn.Bck, props *cmn.BucketPropsToUpdate) (err error) {
	if _, err = api.SetBucketProps(defaultAPIParams, bck, props); err != nil {
		return
	}
	fmt.Fprintln(c.App.Writer, "Bucket props successfully updated")
	return
}

// Resets bucket props
func resetBucketProps(c *cli.Context, bck cmn.Bck) (err error) {
	if _, err = api.ResetBucketProps(defaultAPIParams, bck); err != nil {
		return
	}

	fmt.Fprintln(c.App.Writer, "Bucket props successfully reset to cluster defaults")
	return
}

// Get bucket props
func showBucketProps(c *cli.Context) (err error) {
	var (
		bck cmn.Bck
		p   *cmn.BucketProps
	)

	if c.NArg() > 2 {
		return incorrectUsageMsg(c, "too many arguments or unrecognized option '%+v'", c.Args()[2:])
	}

	section := c.Args().Get(1)
	if bck, err = parseBckURI(c, c.Args().First()); err != nil {
		return
	}
	if p, err = headBucket(bck); err != nil {
		return
	}
	if flagIsSet(c, jsonFlag) {
		return templates.DisplayOutput(p, c.App.Writer, "", true)
	}
	defProps, err := defaultBckProps(bck)
	if err != nil {
		return err
	}
	return printBckHeadTable(c, p, defProps, section)
}

func printBckHeadTable(c *cli.Context, props, defProps *cmn.BucketProps, section string) error {
	var (
		defList []prop
		colored = !flagIsSet(c, noColorFlag)
		compact = flagIsSet(c, compactPropFlag)
	)
	// List instead of map to keep properties in the same order always.
	// All names are one word ones - for easier parsing.
	propList := bckPropList(props, !compact)
	if section != "" {
		tmpPropList := propList[:0]
		for _, v := range propList {
			if strings.HasPrefix(v.Name, section) {
				tmpPropList = append(tmpPropList, v)
			}
		}
		propList = tmpPropList
	}

	if colored {
		defList = bckPropList(defProps, !compact)
		highlight := color.New(color.FgCyan).SprintfFunc()
		for idx, p := range propList {
			for _, def := range defList {
				if def.Name != p.Name {
					continue
				}
				if def.Name == apc.PropBucketCreated {
					ts, err := cos.S2UnixNano(p.Value)
					if err == nil {
						p.Value = cos.FormatUnixNano(ts, "" /*RFC822*/)
					}
					propList[idx] = p
				}
				if def.Value != p.Value {
					p.Value = highlight(p.Value)
					propList[idx] = p
				}
				break
			}
		}
	}

	return templates.DisplayOutput(propList, c.App.Writer, templates.PropsSimpleTmpl)
}

// Configure bucket as n-way mirror
func configureNCopies(c *cli.Context, bck cmn.Bck, copies int) (err error) {
	var xactID string
	if xactID, err = api.MakeNCopies(defaultAPIParams, bck, copies); err != nil {
		return
	}
	var baseMsg string
	if copies > 1 {
		baseMsg = fmt.Sprintf("Configured %q as %d-way mirror,", bck, copies)
	} else {
		baseMsg = fmt.Sprintf("Configured %q for single-replica (no redundancy),", bck)
	}
	fmt.Fprintln(c.App.Writer, baseMsg, xactProgressMsg(xactID))
	return
}

// erasure code the entire bucket
func ecEncode(c *cli.Context, bck cmn.Bck, data, parity int) (err error) {
	var xactID string
	if xactID, err = api.ECEncodeBucket(defaultAPIParams, bck, data, parity); err != nil {
		return
	}
	fmt.Fprintf(c.App.Writer, "Erasure-coding bucket %s, ", bck)
	fmt.Fprintln(c.App.Writer, xactProgressMsg(xactID))
	return
}

// This function returns buckets based on arguments provided to the command.
// In case something is missing it also generates a meaningful error message.
func parseBcks(c *cli.Context) (bckFrom, bckTo cmn.Bck, err error) {
	if c.NArg() == 0 {
		return bckFrom, bckTo, missingArgumentsError(c, "bucket name", "new bucket name")
	}
	if c.NArg() == 1 {
		return bckFrom, bckTo, missingArgumentsError(c, "new bucket name")
	}

	bcks := make([]cmn.Bck, 0, 2)
	for i := 0; i < 2; i++ {
		bck, err := parseBckURI(c, c.Args().Get(i))
		if err != nil {
			return bckFrom, bckTo, err
		}
		bcks = append(bcks, bck)
	}
	return bcks[0], bcks[1], nil
}

func printBuckets(c *cli.Context, bcks cmn.Bcks, showHeaders bool, matches bucketFilter) {
	providerList := make([]string, 0, len(apc.Providers))
	for provider := range apc.Providers {
		providerList = append(providerList, provider)
	}
	sort.Strings(providerList)
	for _, provider := range providerList {
		query := cmn.QueryBcks{Provider: provider}
		bcks := bcks.Select(query)
		if len(bcks) == 0 {
			continue
		}
		filtered := bcks[:0]
		for _, bck := range bcks {
			if matches(bck) {
				filtered = append(filtered, bck)
			}
		}
		if showHeaders {
			dspProvider := provider
			if provider == apc.ProviderHTTP {
				dspProvider = "HTTP(S)"
			}
			fmt.Fprintf(c.App.Writer, "%s Buckets (%d)\n", strings.ToUpper(dspProvider), len(filtered))
		}
		for _, bck := range filtered {
			if provider == apc.ProviderHTTP {
				if props, err := api.HeadBucket(defaultAPIParams, bck); err == nil {
					fmt.Fprintf(c.App.Writer, "  %s (%s)\n", bck, props.Extra.HTTP.OrigURLBck)
					continue
				}
			}
			fmt.Fprintf(c.App.Writer, "  %s\n", bck)
		}
	}
}

func buildOutputTemplate(props string, showHeaders bool) string {
	var (
		headSb strings.Builder
		bodySb strings.Builder

		propsList = makeList(props)
	)

	bodySb.WriteString("{{range $obj := .}}")
	for _, field := range propsList {
		if _, ok := templates.ObjectPropsMap[field]; !ok {
			continue
		}
		columnName := strings.ReplaceAll(strings.ToUpper(field), "_", " ")
		headSb.WriteString(columnName + "\t ")
		bodySb.WriteString(templates.ObjectPropsMap[field] + "\t ")
	}
	headSb.WriteString("\n")
	bodySb.WriteString("\n{{end}}")

	if showHeaders {
		return headSb.String() + bodySb.String()
	}

	return bodySb.String()
}

func printObjectProps(c *cli.Context, entries []*cmn.BucketEntry, objectFilter *objectListFilter, props string, showUnmatched, showHeaders bool) error {
	var (
		outputTemplate        = buildOutputTemplate(props, showHeaders)
		matchingEntries, rest = objectFilter.filter(entries)
	)
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

func (o *objectListFilter) filter(entries []*cmn.BucketEntry) (matching, rest []cmn.BucketEntry) {
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

	if !flagIsSet(c, allItemsFlag) {
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
		pt, err := cos.ParseBashTemplate(bashTemplate)
		if err != nil {
			return nil, err
		}

		matchingObjectNames := make(cos.StringSet)

		linksIt := pt.Iter()
		for objName, hasNext := linksIt(); hasNext; objName, hasNext = linksIt() {
			matchingObjectNames[objName] = struct{}{}
		}
		objFilter.addFilter(func(obj *cmn.BucketEntry) bool { _, ok := matchingObjectNames[obj.Name]; return ok })
	}

	return objFilter, nil
}
