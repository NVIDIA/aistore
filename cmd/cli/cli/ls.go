// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"regexp"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/urfave/cli"
)

type (
	lsbFooter struct {
		pct        int
		nb, nbp    int
		pobj, robj uint64
		size       uint64 // apparent
	}

	entryFilter func(*cmn.LsoEntry) bool

	lstFilter struct {
		predicates []entryFilter
	}
)

// `ais ls`, `ais ls s3:` and similar
func listBckTable(c *cli.Context, qbck cmn.QueryBcks, bcks cmn.Bcks, matches func(cmn.Bck) bool,
	fltPresence int, countRemoteObjs bool) (cnt int) {
	var (
		filtered = make(cmn.Bcks, 0, len(bcks))
		unique   = make(cos.StrSet, len(bcks))
	)
	for _, bck := range bcks {
		if qbck.Contains(&bck) && matches(bck) {
			uname := bck.MakeUname("")
			if _, ok := unique[uname]; !ok {
				filtered = append(filtered, bck)
				unique[uname] = struct{}{}
			}
		}
	}
	if cnt = len(filtered); cnt == 0 {
		return
	}
	if flagIsSet(c, bckSummaryFlag) {
		listBckTableWithSummary(c, qbck, filtered, fltPresence, countRemoteObjs)
	} else {
		listBckTableNoSummary(c, qbck, filtered, fltPresence)
	}
	return
}

func listBckTableNoSummary(c *cli.Context, qbck cmn.QueryBcks, filtered []cmn.Bck, fltPresence int) {
	var (
		bmd        *meta.BMD
		err        error
		footer     lsbFooter
		hideHeader = flagIsSet(c, noHeaderFlag)
		hideFooter = flagIsSet(c, noFooterFlag)
		data       = make([]teb.ListBucketsHelper, 0, len(filtered))
	)
	if !apc.IsFltPresent(fltPresence) {
		bmd, err = api.GetBMD(apiBP)
		if err != nil {
			fmt.Fprintln(c.App.ErrWriter, err)
			return
		}
	}
	for _, bck := range filtered {
		var (
			info  cmn.BsummResult
			props *cmn.BucketProps
		)
		if apc.IsFltPresent(fltPresence) {
			info.IsBckPresent = true
		} else {
			props, info.IsBckPresent = bmd.Get(meta.CloneBck(&bck))
		}
		footer.nb++
		if info.IsBckPresent {
			footer.nbp++
		}
		if bck.IsHTTP() {
			if bmd == nil {
				bmd, err = api.GetBMD(apiBP)
				if err != nil {
					fmt.Fprintln(c.App.ErrWriter, err)
					return
				}
			}
			props, _ = bmd.Get(meta.CloneBck(&bck))
			bck.Name += " (URL: " + props.Extra.HTTP.OrigURLBck + ")"
		}
		data = append(data, teb.ListBucketsHelper{Bck: bck, Props: props, Info: &info})
	}
	if hideHeader {
		teb.Print(data, teb.ListBucketsBodyNoSummary)
	} else {
		teb.Print(data, teb.ListBucketsTmplNoSummary)
	}
	if hideFooter {
		return
	}

	var s string
	if !apc.IsFltPresent(fltPresence) {
		s = fmt.Sprintf(" (%d present)", footer.nbp)
	}
	p := apc.DisplayProvider(qbck.Provider)
	if qbck.IsRemoteAIS() {
		p = qbck.DisplayProvider()
	}
	foot := fmt.Sprintf("Total: [%s bucket%s: %d%s] ========", p, cos.Plural(footer.nb), footer.nb, s)
	fmt.Fprintln(c.App.Writer, fcyan(foot))
}

// (compare with `showBucketSummary` and `bsummSlow`)
func listBckTableWithSummary(c *cli.Context, qbck cmn.QueryBcks, filtered []cmn.Bck, fltPresence int, countRemoteObjs bool) {
	var (
		footer      lsbFooter
		hideHeader  = flagIsSet(c, noHeaderFlag)
		hideFooter  = flagIsSet(c, noFooterFlag)
		data        = make([]teb.ListBucketsHelper, 0, len(filtered))
		units, errU = parseUnitsFlag(c, unitsFlag)
	)
	if errU != nil {
		actionWarn(c, errU.Error())
		units = ""
	}
	var (
		opts = teb.Opts{AltMap: teb.FuncMapUnits(units)}
		prev = mono.NanoTime()
		max  = listObjectsWaitTime
	)
	for i, bck := range filtered {
		props, info, err := api.GetBucketInfo(apiBP, bck, fltPresence, countRemoteObjs)
		if err != nil {
			err = V(err)
			warn := fmt.Sprintf("%s: %v\n", bck.Cname(""), err)
			actionWarn(c, warn)
			continue
		}
		footer.nb++
		if info.IsBckPresent {
			footer.nbp++
			footer.pobj += info.ObjCount.Present
			footer.robj += info.ObjCount.Remote
			footer.size += info.TotalSize.OnDisk
			footer.pct += int(info.UsedPct)
		}
		if bck.IsHTTP() {
			bck.Name += " (URL: " + props.Extra.HTTP.OrigURLBck + ")"
		}
		data = append(data, teb.ListBucketsHelper{Bck: bck, Props: props, Info: info})

		now := mono.NanoTime()
		if elapsed := time.Duration(now - prev); elapsed < max && i < len(filtered)-1 {
			continue
		}
		// print
		if hideHeader {
			teb.Print(data, teb.ListBucketsSummBody, opts)
		} else {
			teb.Print(data, teb.ListBucketsSummTmpl, opts)
		}
		data = data[:0]
		prev = now
		if i < len(filtered)-1 {
			fmt.Fprintln(c.App.Writer)
		}
	}

	if footer.robj == 0 && apc.IsRemoteProvider(qbck.Provider) && !countRemoteObjs {
		fmt.Fprintln(c.App.Writer)
		note := fmt.Sprintf("to count and size remote buckets and objects outside ais cluster, use %s switch, see '--help' for details",
			qflprn(allObjsOrBcksFlag))
		actionNote(c, note)
	}

	if hideFooter || footer.nbp <= 1 {
		return
	}

	var s, foot string
	if !apc.IsFltPresent(fltPresence) {
		s = fmt.Sprintf(" (%d present)", footer.nbp)
	}
	p := apc.DisplayProvider(qbck.Provider)
	if qbck.IsRemoteAIS() {
		p = qbck.DisplayProvider()
	}
	apparentSize := teb.FmtSize(int64(footer.size), units, 2)
	if footer.pobj+footer.robj != 0 {
		foot = fmt.Sprintf("Total: [%s bucket%s: %d%s, objects %d(%d), apparent size %s, used capacity %d%%] ========",
			p, cos.Plural(footer.nb), footer.nb, s, footer.pobj, footer.robj, apparentSize, footer.pct)
	} else {
		foot = fmt.Sprintf("Total: [%s bucket%s: %d%s, apparent size %s, used capacity %d%%] ========",
			p, cos.Plural(footer.nb), footer.nb, s, apparentSize, footer.pct)
	}
	fmt.Fprintln(c.App.Writer, fcyan(foot))
}

func listObjects(c *cli.Context, bck cmn.Bck, prefix string, listArch bool) error {
	// prefix and filter
	lstFilter, prefixFromTemplate, err := newLstFilter(c)
	if err != nil {
		return err
	}
	if prefixFromTemplate != "" {
		if prefix != "" && prefix != prefixFromTemplate {
			return fmt.Errorf("which prefix to use: %q (from %s) or %q (from %s)?",
				prefix, qflprn(listObjPrefixFlag), prefixFromTemplate, qflprn(templateFlag))
		}
		prefix = prefixFromTemplate
	}

	// lsmsg
	msg := &apc.LsoMsg{Prefix: prefix}
	addCachedCol := bck.IsRemote()
	if flagIsSet(c, listObjCachedFlag) {
		msg.SetFlag(apc.LsObjCached)
		addCachedCol = false
	}
	if flagIsSet(c, listAnonymousFlag) {
		msg.SetFlag(apc.LsTryHeadRemote)
	}
	if listArch {
		msg.SetFlag(apc.LsArchDir)
	}
	if flagIsSet(c, allObjsOrBcksFlag) {
		msg.SetFlag(apc.LsAll)
	}

	var (
		props    []string
		propsStr = parseStrFlag(c, objPropsFlag)
	)
	if propsStr != "" {
		debug.Assert(apc.LsPropsSepa == ",", "',' is documented in 'objPropsFlag' usage and elsewhere")
		props = splitCsv(propsStr) // split apc.LsPropsSepa
	}
	// NOTE: compare w/ `showObjProps()`
	if flagIsSet(c, nameOnlyFlag) {
		if len(props) > 2 {
			warn := fmt.Sprintf("flag %s is incompatible with the value of %s",
				qflprn(nameOnlyFlag), qflprn(objPropsFlag))
			actionWarn(c, warn)
		}
		msg.SetFlag(apc.LsNameOnly)
		msg.Props = apc.GetPropsName
	} else if len(props) == 0 {
		msg.AddProps(apc.GetPropsMinimal...)
	} else {
		if cos.StringInSlice(allPropsFlag.GetName(), props) {
			msg.AddProps(apc.GetPropsAll...)
		} else {
			msg.AddProps(apc.GetPropsName)
			msg.AddProps(props...)
		}
	}
	if flagIsSet(c, allObjsOrBcksFlag) {
		// Show status. Object name can then be displayed multiple times
		// (due to mirroring, EC). The status helps to tell an object from its replica(s).
		msg.AddProps(apc.GetPropsStatus)
	}
	if flagIsSet(c, startAfterFlag) {
		msg.StartAfter = parseStrFlag(c, startAfterFlag)
	}

	pageSize, limit, err := _setPage(c, bck)
	if err != nil {
		return err
	}
	msg.PageSize = uint(pageSize)

	// list page by page, print pages one at a time
	if flagIsSet(c, pagedFlag) {
		pageCounter, maxPages, toShow := 0, parseIntFlag(c, maxPagesFlag), limit
		for {
			objList, err := api.ListObjectsPage(apiBP, bck, msg)
			if err != nil {
				return V(err)
			}

			// print exact number of objects if it is `limit`ed: in case of
			// limit > page size, the last page is printed partially
			var toPrint cmn.LsoEntries
			if limit > 0 && toShow < len(objList.Entries) {
				toPrint = objList.Entries[:toShow]
			} else {
				toPrint = objList.Entries
			}
			err = printObjProps(c, toPrint, lstFilter, msg.Props, addCachedCol)
			if err != nil {
				return err
			}

			// interrupt the loop if:
			// 1. the last page is printed
			// 2. maximum pages are printed
			// 3. printed the `limit` number of objects
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
		fmt.Fprintf(c.App.Writer, "\rListed %d objects (elapsed: %s)",
			ctx.Info().Count, teb.FormatDuration(ctx.Elapsed()))
		if ctx.IsFinished() {
			fmt.Fprintln(c.App.Writer)
		}
	}

	// list all pages up to a limit, show progress
	callAfter := listObjectsWaitTime
	if flagIsSet(c, refreshFlag) {
		callAfter = parseDurationFlag(c, refreshFlag)
	}
	ctx := api.NewProgressContext(cb, callAfter)
	objList, err := api.ListObjects(apiBP, bck, msg, api.ListArgs{Num: uint(limit), Progress: ctx})
	if err != nil {
		return V(err)
	}
	return printObjProps(c, objList.Entries, lstFilter, msg.Props, addCachedCol)
}

func _setPage(c *cli.Context, bck cmn.Bck) (pageSize, limit int, err error) {
	defaultPageSize := apc.DefaultPageSizeCloud
	if bck.IsAIS() || bck.IsRemoteAIS() {
		defaultPageSize = apc.DefaultPageSizeAIS
	}
	pageSize = parseIntFlag(c, pageSizeFlag)
	if pageSize < 0 {
		err = fmt.Errorf("page size (%d) cannot be negative", pageSize)
		return
	}
	limit = parseIntFlag(c, objLimitFlag)
	if limit < 0 {
		err = fmt.Errorf("max object count (%d) cannot be negative", limit)
		return
	}
	if limit == 0 {
		return
	}
	// when limit "wins"
	if limit < pageSize || (limit < defaultPageSize && pageSize == 0) {
		pageSize = limit
	}
	return
}

///////////////
// lstFilter //
///////////////

func newLstFilter(c *cli.Context) (flt *lstFilter, prefix string, _ error) {
	flt = &lstFilter{}
	if !flagIsSet(c, allObjsOrBcksFlag) {
		// filter objects with not OK status
		flt.addFilter(func(obj *cmn.LsoEntry) bool { return obj.IsStatusOK() })
	}
	if regexStr := parseStrFlag(c, regexLsAnyFlag); regexStr != "" {
		regex, err := regexp.Compile(regexStr)
		if err != nil {
			return nil, "", err
		}
		flt.addFilter(func(obj *cmn.LsoEntry) bool { return regex.MatchString(obj.Name) })
	}
	if bashTemplate := parseStrFlag(c, templateFlag); bashTemplate != "" {
		pt, err := cos.NewParsedTemplate(bashTemplate)
		if err != nil && err != cos.ErrEmptyTemplate { // NOTE: empty template => entire bucket
			return nil, "", err
		}
		if len(pt.Ranges) == 0 {
			prefix, pt.Prefix = pt.Prefix, "" // NOTE: when template is a "pure" prefix
		} else {
			matchingObjectNames := make(cos.StrSet)
			pt.InitIter()
			for objName, hasNext := pt.Next(); hasNext; objName, hasNext = pt.Next() {
				matchingObjectNames[objName] = struct{}{}
			}
			flt.addFilter(func(obj *cmn.LsoEntry) bool { _, ok := matchingObjectNames[obj.Name]; return ok })
		}
	}
	return flt, prefix, nil
}

func (o *lstFilter) addFilter(f entryFilter) {
	o.predicates = append(o.predicates, f)
}

func (o *lstFilter) matchesAll(obj *cmn.LsoEntry) bool {
	// Check if object name matches *all* specified predicates
	for _, predicate := range o.predicates {
		if !predicate(obj) {
			return false
		}
	}
	return true
}

func (o *lstFilter) filter(entries cmn.LsoEntries) (matching, rest []cmn.LsoEntry) {
	for _, obj := range entries {
		if o.matchesAll(obj) {
			matching = append(matching, *obj)
		} else {
			rest = append(rest, *obj)
		}
	}
	return
}
