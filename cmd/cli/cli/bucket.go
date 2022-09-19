// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles bucket operations.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"text/template"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/tmpls"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
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
		var empty bool
		empty, err = isBucketEmpty(bck)
		if err == nil && !empty {
			if !flagIsSet(c, yesFlag) {
				if ok := confirm(c, fmt.Sprintf("Proceed to destroy %s?", bck)); !ok {
					continue
				}
			}
		}
		if err = api.DestroyBucket(defaultAPIParams, bck); err == nil {
			fmt.Fprintf(c.App.Writer, "%q destroyed\n", bck)
			continue
		}
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
	return nil
}

// Rename ais bucket
func mvBucket(c *cli.Context, fromBck, toBck cmn.Bck) (err error) {
	var xactID string

	if _, err = headBucket(fromBck, true /* don't add */); err != nil {
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
func copyBucket(c *cli.Context, fromBck, toBck cmn.Bck, msg *apc.CopyBckMsg) (err error) {
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

func listBuckets(c *cli.Context, qbck cmn.QueryBcks, fltPresence int) (err error) {
	var (
		regex  *regexp.Regexp
		filter = func(_ cmn.Bck) bool { return true }
	)
	if regexStr := parseStrFlag(c, regexFlag); regexStr != "" {
		regex, err = regexp.Compile(regexStr)
		if err != nil {
			return
		}
		filter = func(bck cmn.Bck) bool { return regex.MatchString(bck.Name) }
	}
	bcks, err := api.ListBuckets(defaultAPIParams, qbck, fltPresence)
	if err != nil {
		return
	}
	for _, provider := range selectProviders(bcks) {
		lsBckTable(c, provider, bcks, filter)
		fmt.Fprintln(c.App.Writer)
	}
	return
}

type lstally struct {
	nb, nbp, pct int
	nobj, size   uint64
}

func lsBckTable(c *cli.Context, provider string, bcks cmn.Bcks, matches func(cmn.Bck) bool /*filter*/) {
	var (
		lst      lstally
		filtered = make(cmn.Bcks, 0, len(bcks))
	)
	for _, bck := range bcks {
		if bck.Provider == provider && matches(bck) {
			filtered = append(filtered, bck)
		}
	}
	if len(filtered) == 0 {
		return
	}
	hideHeader := flagIsSet(c, noHeaderFlag)
	hideFooter := flagIsSet(c, noFooterFlag)
	data := make([]tmpls.ListBucketsTemplateHelper, 0, len(filtered))
	for _, bck := range filtered {
		props, info, err := api.GetBucketInfo(defaultAPIParams, bck)
		if err != nil {
			if httpErr, ok := err.(*cmn.ErrHTTP); ok {
				fmt.Fprintf(c.App.Writer, "  %s, err: %s\n", bck, httpErr.Message)
			}
			continue
		}
		lst.nb++
		if info.Present {
			lst.nbp++
			lst.nobj += info.ObjCount
			lst.size += info.Size
			lst.pct += int(info.UsedPct)
		}
		if bck.IsHTTP() {
			bck.Name += " (URL: " + props.Extra.HTTP.OrigURLBck + ")"
		}
		data = append(data, tmpls.ListBucketsTemplateHelper{Bck: bck, Props: props, Info: info})
	}

	var altMap template.FuncMap
	if flagIsSet(c, sizeInBytesFlag) {
		altMap = tmpls.AltFuncMapSizeBytes()
	}
	if hideHeader {
		tmpls.DisplayOutput(data, c.App.Writer, tmpls.ListBucketsBody, altMap, false)
	} else {
		tmpls.DisplayOutput(data, c.App.Writer, tmpls.ListBucketsTmpl, altMap, false)
	}
	if !hideFooter && lst.nbp > 1 {
		foot := fmt.Sprintf("=======\t[%s buckets: \t%d(%d), objects %d, size %s, used %d%%] =======",
			apc.DisplayProvider(provider), lst.nb, lst.nbp, lst.nobj, cos.UnsignedB2S(lst.size, 2), lst.pct)
		fmt.Fprintln(c.App.Writer, fcyan(foot))
	}
}

func listObjects(c *cli.Context, bck cmn.Bck, prefix string, listArch bool) error {
	var (
		msg                   = &apc.ListObjsMsg{Prefix: prefix}
		objectListFilter, err = newObjectListFilter(c)
	)
	if err != nil {
		return err
	}
	if flagIsSet(c, listCachedFlag) || /*same*/ flagIsSet(c, listPresentFlag) {
		msg.SetFlag(apc.LsPresent)
	}
	if flagIsSet(c, listAnonymousFlag) {
		msg.SetFlag(apc.LsTryHeadRemB)
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
			err = printObjProps(c, toPrint, objectListFilter, msg.Props)
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
		if ctx.IsFinished() {
			fmt.Fprintln(c.App.Writer)
		}
	}

	// list all pages up to a limit, show progress
	ctx := api.NewProgressContext(cb, longCommandTime)
	objList, err := api.ListObjectsWithOpts(defaultAPIParams, bck, msg, uint(limit), ctx)
	if err != nil {
		return err
	}
	return printObjProps(c, objList.Entries, objectListFilter, msg.Props)
}

func getSummaries(qbck cmn.QueryBcks, fast, cached bool) (summaries cmn.BckSummaries, err error) {
	fDetails := func() (err error) {
		msg := &apc.BckSummMsg{Cached: cached, Fast: fast}
		summaries, err = api.GetBucketsSummaries(defaultAPIParams, qbck, msg)
		return
	}
	err = cmn.WaitForFunc(fDetails, longCommandTime)
	return
}

// If both backend_bck.name and backend_bck.provider are present, use them.
// Otherwise, replace as follows:
//   - e.g., `backend_bck=gcp://bucket_name` with `backend_bck.name=bucket_name` and
//     `backend_bck.provider=gcp` to match expected fields.
//   - `backend_bck=none` with `backend_bck.name=""` and `backend_bck.provider=""`.
func reformatBackendProps(c *cli.Context, nvs cos.SimpleKVs) (err error) {
	var (
		originBck cmn.Bck
		v         string
		ok        bool
	)

	if v, ok = nvs[apc.PropBackendBckName]; ok && v != "" {
		if v, ok = nvs[apc.PropBackendBckProvider]; ok && v != "" {
			nvs[apc.PropBackendBckProvider], err = cmn.NormalizeProvider(v)
			return
		}
	}

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
	if p, err = headBucket(bck, true /* don't add */); err != nil {
		return
	}
	if flagIsSet(c, jsonFlag) {
		return tmpls.DisplayOutput(p, c.App.Writer, "", nil, true)
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
					p.Value = fcyan(p.Value)
					propList[idx] = p
				}
				break
			}
		}
	}

	return tmpls.DisplayOutput(propList, c.App.Writer, tmpls.PropsSimpleTmpl, nil, false)
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

func printObjProps(c *cli.Context, entries []*cmn.BucketEntry, objectFilter *objectListFilter, props string) error {
	var (
		altMap         template.FuncMap
		tmpl           = objPropsTemplate(c, props)
		matched, other = objectFilter.filter(entries)
	)
	if flagIsSet(c, sizeInBytesFlag) {
		altMap = tmpls.AltFuncMapSizeBytes()
	}
	err := tmpls.DisplayOutput(matched, c.App.Writer, tmpl, altMap, false)
	if err != nil {
		return err
	}
	if flagIsSet(c, showUnmatchedFlag) {
		unmatched := fcyan("\nNames that don't match:")
		if len(other) == 0 {
			fmt.Fprintln(c.App.Writer, unmatched+" none")
		} else {
			tmpl = unmatched + "\n" + tmpl
			err = tmpls.DisplayOutput(other, c.App.Writer, tmpl, altMap, false)
		}
	}
	return err
}

func objPropsTemplate(c *cli.Context, props string) string {
	var (
		headSb    strings.Builder
		bodySb    strings.Builder
		propsList = makeList(props)
	)
	bodySb.WriteString("{{range $obj := .}}")
	for _, field := range propsList {
		if _, ok := tmpls.ObjectPropsMap[field]; !ok {
			continue
		}
		columnName := strings.ReplaceAll(strings.ToUpper(field), "_", " ")
		headSb.WriteString(columnName + "\t ")
		bodySb.WriteString(tmpls.ObjectPropsMap[field] + "\t ")
	}
	headSb.WriteString("\n")
	bodySb.WriteString("\n{{end}}")

	if flagIsSet(c, noHeaderFlag) {
		return bodySb.String()
	}
	return headSb.String() + bodySb.String()
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

		pt.InitIter()
		for objName, hasNext := pt.Next(); hasNext; objName, hasNext = pt.Next() {
			matchingObjectNames[objName] = struct{}{}
		}
		objFilter.addFilter(func(obj *cmn.BucketEntry) bool { _, ok := matchingObjectNames[obj.Name]; return ok })
	}

	return objFilter, nil
}
