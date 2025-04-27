// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core/meta"

	"github.com/urfave/cli"
)

const listedText = "Listed"

type (
	lsbFooter struct {
		pct        int
		nb, nbp    int
		pobj, robj uint64
		size       uint64 // apparent
	}

	entryFilter func(*cmn.LsoEnt) bool

	lstFilter struct {
		predicates []entryFilter
	}
)

// `ais ls`, `ais ls s3:`, `ais ls s3://aaa --prefix bbb --summary` and similar
func listBckTable(c *cli.Context, qbck cmn.QueryBcks, bcks cmn.Bcks, lsb lsbCtx) (cnt int) {
	if flagIsSet(c, bckSummaryFlag) {
		args := api.BinfoArgs{
			FltPresence:   lsb.fltPresence,     // all-buckets part in the `allObjsOrBcksFlag`
			WithRemote:    lsb.countRemoteObjs, // all-objects part --/--
			Summarize:     true,
			DontAddRemote: flagIsSet(c, dontAddRemoteFlag),
		}
		prefix := cos.Left(parseStrFlag(c, listObjPrefixFlag), lsb.prefix /*as in: bucket/[prefix]*/)
		cnt = listBckTableWithSummary(c, qbck, bcks, args, prefix)
	} else {
		cnt = listBckTableNoSummary(c, qbck, bcks, lsb.fltPresence)
	}
	return
}

func listBckTableNoSummary(c *cli.Context, qbck cmn.QueryBcks, bcks cmn.Bcks, fltPresence int) int {
	var (
		bmd        *meta.BMD
		err        error
		footer     lsbFooter
		hideHeader = flagIsSet(c, noHeaderFlag)
		hideFooter = flagIsSet(c, noFooterFlag)
		data       = make([]teb.ListBucketsHelper, 0, len(bcks))
	)
	if !apc.IsFltPresent(fltPresence) {
		bmd, err = api.GetBMD(apiBP)
		if err != nil {
			fmt.Fprintln(c.App.ErrWriter, err)
			return 0
		}
	}
	for i := range bcks {
		bck := bcks[i]
		if !qbck.Contains(&bck) {
			continue
		}
		var (
			info  cmn.BsummResult
			props *cmn.Bprops
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
		if bck.IsHT() {
			if bmd == nil {
				bmd, err = api.GetBMD(apiBP)
				if err != nil {
					fmt.Fprintln(c.App.ErrWriter, err)
					return 0
				}
			}
			props, _ = bmd.Get(meta.CloneBck(&bck))
			bck.Name += " (URL: " + props.Extra.HTTP.OrigURLBck + ")"
		}
		data = append(data, teb.ListBucketsHelper{Bck: bck, Props: props, Info: &info})
	}
	if footer.nb == 0 {
		return 0
	}
	if hideHeader {
		teb.Print(data, teb.ListBucketsBodyNoSummary)
	} else {
		teb.Print(data, teb.ListBucketsTmplNoSummary)
	}
	if hideFooter {
		return footer.nb
	}

	var s string
	if !apc.IsFltPresent(fltPresence) {
		s = fmt.Sprintf(" (%d present)", footer.nbp)
	}
	p := apc.DisplayProvider(qbck.Provider)
	if qbck.IsRemoteAIS() {
		p = "Remote " + p
	}
	foot := fmt.Sprintf("Total: [%s bucket%s: %d%s] ========", p, cos.Plural(footer.nb), footer.nb, s)
	fmt.Fprintln(c.App.Writer, fcyan(foot))
	return footer.nb
}

// compare with `showBucketSummary`
func listBckTableWithSummary(c *cli.Context, qbck cmn.QueryBcks, bcks cmn.Bcks, args api.BinfoArgs, prefix string) int {
	var (
		footer     lsbFooter
		hideHeader = flagIsSet(c, noHeaderFlag)
		hideFooter = flagIsSet(c, noFooterFlag)
		maxwait    = listObjectsWaitTime

		objCached  = flagIsSet(c, listCachedFlag)
		bckPresent = flagIsSet(c, allObjsOrBcksFlag)
	)
	debug.Assert(args.Summarize)
	ctx, err := newBsummCtxMsg(c, qbck, prefix, objCached, bckPresent)
	if err != nil {
		actionWarn(c, err.Error()+"\n")
	}

	// because api.BinfoArgs (left) contains api.BsummArgs (right)
	args.CallAfter = ctx.args.CallAfter
	args.Callback = ctx.args.Callback
	args.Prefix = prefix

	// one at a time
	prev := ctx.started
	opts := teb.Opts{AltMap: teb.FuncMapUnits(ctx.units, false /*incl. calendar date*/)}
	data := make([]teb.ListBucketsHelper, 0, len(bcks))
	for i := range bcks {
		bck := bcks[i]
		if !qbck.Contains(&bck) {
			continue
		}
		ctx.qbck = cmn.QueryBcks(bck)
		xid, props, info, err := api.GetBucketInfo(apiBP, bck, &args)
		if err != nil {
			var partial bool
			if herr, ok := err.(*cmn.ErrHTTP); ok {
				if herr.Status == http.StatusAccepted {
					continue
				}
				partial = herr.Status == http.StatusPartialContent
			}
			actionWarn(c, notV(err).Error())
			if !partial {
				continue
			}
			warn := fmt.Sprintf("%s[%s] %s - still running, showing partial results", cmdSummary, xid, bck.Cname(""))
			actionWarn(c, warn)
		}
		footer.nb++
		if info.IsBckPresent {
			footer.nbp++
			footer.pobj += info.ObjCount.Present
			footer.robj += info.ObjCount.Remote
			footer.size += info.TotalSize.OnDisk
			footer.pct += int(info.UsedPct)
		}
		if bck.IsHT() {
			bck.Name += " (URL: " + props.Extra.HTTP.OrigURLBck + ")"
		}
		data = append(data, teb.ListBucketsHelper{XactID: xid, Bck: bck, Props: props, Info: info})

		now := mono.NanoTime()
		if elapsed := time.Duration(now - prev); elapsed < maxwait && i < len(bcks)-1 {
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
		if i < len(bcks)-1 {
			fmt.Fprintln(c.App.Writer)
		}
	}
	if footer.nb == 0 {
		return 0
	}

	if footer.robj == 0 && apc.IsRemoteProvider(qbck.Provider) && !args.WithRemote {
		fmt.Fprintln(c.App.Writer)
		note := fmt.Sprintf("to count and size remote buckets and objects outside ais cluster, use %s switch, see %s for details",
			qflprn(allObjsOrBcksFlag), qflprn(cli.HelpFlag))
		actionNote(c, note)
	}

	if hideFooter || footer.nbp <= 1 {
		return footer.nb
	}

	//
	// totals
	//
	var s, foot string
	if !apc.IsFltPresent(args.FltPresence) {
		s = fmt.Sprintf(" (%d present)", footer.nbp)
	}
	p := apc.DisplayProvider(qbck.Provider)
	if qbck.IsRemoteAIS() {
		p = "Remote " + p
	}
	apparentSize := teb.FmtSize(int64(footer.size), ctx.units, 2)
	if footer.pobj+footer.robj != 0 {
		foot = fmt.Sprintf("Total: [%s bucket%s: %d%s, objects %d(%d), apparent size %s, used capacity %d%%] ========",
			p, cos.Plural(footer.nb), footer.nb, s, footer.pobj, footer.robj, apparentSize, footer.pct)
	} else {
		foot = fmt.Sprintf("Total: [%s bucket%s: %d%s, apparent size %s, used capacity %d%%] ========",
			p, cos.Plural(footer.nb), footer.nb, s, apparentSize, footer.pct)
	}
	fmt.Fprintln(c.App.Writer, fcyan(foot))
	return footer.nb
}

func listObjects(c *cli.Context, bck cmn.Bck, prefix string, listArch, printEmpty bool) error {
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

	// when prefix crosses shard boundary
	if external, internal := splitPrefixShardBoundary(prefix); internal != "" {
		origPrefix := prefix
		prefix = external
		lstFilter._add(func(obj *cmn.LsoEnt) bool { return strings.HasPrefix(obj.Name, origPrefix) })
	}

	// lsmsg
	var (
		msg          = &apc.LsoMsg{Prefix: prefix}
		addCachedCol bool
	)
	if bck.IsRemote() {
		addCachedCol = true           // preliminary; may change below
		msg.SetFlag(apc.LsBckPresent) // default
	}
	if listArch {
		msg.SetFlag(apc.LsArchDir)

		if bck.IsRemote() && !msg.IsFlagSet(apc.LsCached) {
			const warn = "listing the contents of archives (\"shards\") is currently only supported for in-cluster (\"cached\") objects."
			actionWarn(c, warn)
			msg.SetFlag(apc.LsCached)
		}
	}

	if flagIsSet(c, diffFlag) {
		if bck.IsAIS() {
			return fmt.Errorf("flag %s requires remote bucket (have: %s)", qflprn(diffFlag), bck.String())
		}
		if !bck.HasVersioningMD() {
			return fmt.Errorf("flag %s only applies to remote backends that maintain at least some form of versioning information (have: %s)",
				qflprn(diffFlag), bck.String())
		}
		if flagIsSet(c, listNotCachedFlag) {
			return fmt.Errorf(errFmtExclusive, qflprn(diffFlag), qflprn(listNotCachedFlag))
		}
		msg.SetFlag(apc.LsDiff)
	}

	if flagIsSet(c, listCachedFlag) {
		if flagIsSet(c, diffFlag) {
			actionWarn(c, "checking remote versions may take some time...\n")
			briefPause(1)
		}
		if flagIsSet(c, listNotCachedFlag) {
			return fmt.Errorf(errFmtExclusive, qflprn(listCachedFlag), qflprn(listNotCachedFlag))
		}
		msg.SetFlag(apc.LsCached)
		// addCachedCol: correction #1
		addCachedCol = false
	}
	if flagIsSet(c, listNotCachedFlag) {
		if bck.IsAIS() {
			return fmt.Errorf("flag %s requires remote bucket (have: %s)", qflprn(listNotCachedFlag), bck.String())
		}
		msg.SetFlag(apc.LsNotCached)
	}

	// NOTE: `--all` combines two separate meanings:
	// - list missing obj-s (with existing copies)
	// - list obj-s from a remote bucket outside cluster
	if flagIsSet(c, allObjsOrBcksFlag) {
		msg.SetFlag(apc.LsMissing)
		msg.ClearFlag(apc.LsBckPresent)
	}
	if flagIsSet(c, dontHeadRemoteFlag) {
		msg.SetFlag(apc.LsDontHeadRemote)
	}
	if flagIsSet(c, dontAddRemoteFlag) {
		msg.SetFlag(apc.LsDontAddRemote)
	}
	if flagIsSet(c, noRecursFlag) {
		msg.SetFlag(apc.LsNoRecursion)
	}
	if flagIsSet(c, noDirsFlag) {
		msg.SetFlag(apc.LsNoDirs)
	}

	var (
		props    []string
		propsStr = parseStrFlag(c, objPropsFlag)
		catOnly  = flagIsSet(c, countAndTimeFlag)
	)
	if propsStr != "" {
		debug.Assert(apc.LsPropsSepa == ",", "',' is documented in 'objPropsFlag' usage and elsewhere")
		props = splitCsv(propsStr) // split apc.LsPropsSepa

		for i := range props {
			for j := range props {
				if i == j {
					continue
				}
				if props[i] == props[j] {
					return fmt.Errorf("'%s %s' contains duplication: %q", flprn(objPropsFlag), propsStr, props[i])
				}
			}
		}
	}

	// add _implied_ props into control lsmsg
	switch {
	case flagIsSet(c, nameOnlyFlag):
		if flagIsSet(c, diffFlag) {
			return fmt.Errorf(errFmtExclusive, qflprn(diffFlag), qflprn(nameOnlyFlag))
		}
		if len(props) > 2 {
			warn := fmt.Sprintf("flag %s is incompatible with the value of %s", qflprn(nameOnlyFlag), qflprn(objPropsFlag))
			actionWarn(c, warn)
		}
		msg.SetFlag(apc.LsNameOnly)
		msg.Props = apc.GetPropsName
	case len(props) == 0:
		switch {
		case flagIsSet(c, dontAddRemoteFlag):
			msg.AddProps(apc.GetPropsName)
			msg.AddProps(apc.GetPropsSize)
			msg.SetFlag(apc.LsNameSize)
		case catOnly:
			msg.SetFlag(apc.LsNameOnly)
			msg.Props = apc.GetPropsName
		default:
			msg.AddProps(apc.GetPropsMinimal...)
		}
	case propsStr == apc.GetPropsName:
		msg.SetFlag(apc.LsNameOnly)
		msg.Props = apc.GetPropsName
	case len(props) == 2 &&
		((props[0] == apc.GetPropsName || props[1] == apc.GetPropsName) && (props[0] == apc.GetPropsSize || props[1] == apc.GetPropsSize)):
		msg.SetFlag(apc.LsNameSize)
		msg.AddProps([]string{apc.GetPropsName, apc.GetPropsSize}...)
	default:
		if cos.StringInSlice(allPropsFlag.GetName(), props) {
			msg.AddProps(apc.GetPropsAll...)
		} else {
			msg.AddProps(apc.GetPropsName)
			msg.AddProps(props...)
		}
	}

	// addCachedCol: correction #2
	if addCachedCol && (msg.IsFlagSet(apc.LsNameOnly) || msg.IsFlagSet(apc.LsNameSize)) {
		addCachedCol = false
	}

	// when props are _not_ explicitly specified
	// but (somewhat ambiguous) flag `--all` is
	if len(props) == 0 && flagIsSet(c, allObjsOrBcksFlag) {
		msg.AddProps(apc.GetPropsStatus)
	}
	propsStr = msg.Props // show these and _only_ these props
	// finally:
	if flagIsSet(c, diffFlag) {
		if !msg.WantProp(apc.GetPropsCustom) {
			msg.AddProps(apc.GetPropsCustom)
		}
		if !msg.WantProp(apc.GetPropsVersion) {
			msg.AddProps(apc.GetPropsVersion)
		}
	}

	// set page size, limit
	if flagIsSet(c, startAfterFlag) {
		msg.StartAfter = parseStrFlag(c, startAfterFlag)
	}
	pageSize, maxPages, limit, err := _setPage(c, bck)
	if err != nil {
		return err
	}
	msg.PageSize = pageSize

	// finally, setup lsargs
	lsargs := api.ListArgs{Limit: limit}
	if flagIsSet(c, useInventoryFlag) {
		lsargs.Header = http.Header{
			apc.HdrInventory: []string{"true"},
			apc.HdrInvName:   []string{parseStrFlag(c, invNameFlag)},
			apc.HdrInvID:     []string{parseStrFlag(c, invIDFlag)},
		}
	}

	var (
		now int64
	)
	if catOnly && flagIsSet(c, noFooterFlag) {
		warn := fmt.Sprintf(errFmtExclusive, qflprn(countAndTimeFlag), qflprn(noFooterFlag))
		actionWarn(c, warn)
	}
	// list (and immediately show) pages, one page at a time
	if flagIsSet(c, pagedFlag) {
		var (
			pageCounter int
			toShow      = int(limit)
		)
		for {
			if catOnly {
				now = mono.NanoTime()
			}
			lst, err := api.ListObjectsPage(apiBP, bck, msg, lsargs)
			if err != nil {
				return lsoErr(msg, err)
			}

			// print exact number of objects if it is `limit`ed: in case of
			// limit > page size, the last page is printed partially
			var toPrint cmn.LsoEntries
			if limit > 0 && toShow < len(lst.Entries) {
				toPrint = lst.Entries[:toShow]
			} else {
				toPrint = lst.Entries
			}
			err = printLso(c, toPrint, lstFilter, propsStr, nil /*_listed*/, now,
				pageCounter+1, addCachedCol, bck.IsRemote(), msg.IsFlagSet(apc.LsDiff))
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
			if maxPages > 0 && pageCounter >= int(maxPages) {
				return nil
			}
			if limit > 0 {
				toShow -= len(lst.Entries)
				if toShow <= 0 {
					return nil
				}
			}
		}
	}

	// alternatively (when `--paged` not specified) list all pages up to a limit, show progress
	var (
		callAfter = listObjectsWaitTime
		_listed   = &_listed{c: c, bck: &bck, msg: msg, limit: int(limit)}
	)
	if flagIsSet(c, refreshFlag) {
		callAfter = parseDurationFlag(c, refreshFlag)
	}
	if catOnly {
		now = mono.NanoTime()
	}
	lsargs.Callback = _listed.cb
	lsargs.CallAfter = callAfter
	lst, err := api.ListObjects(apiBP, bck, msg, lsargs)
	if err != nil {
		return lsoErr(msg, err)
	}
	if len(lst.Entries) == 0 && !printEmpty {
		return fmt.Errorf("%s/%s not found", bck.Cname(""), msg.Prefix)
	}
	return printLso(c, lst.Entries, lstFilter, propsStr, _listed, now, 0, /*npage*/
		addCachedCol, bck.IsRemote(), msg.IsFlagSet(apc.LsDiff))
}

func lsoErr(msg *apc.LsoMsg, err error) error {
	if herr, ok := err.(*cmn.ErrHTTP); ok && msg.IsFlagSet(apc.LsBckPresent) {
		if herr.TypeCode == "ErrRemoteBckNotFound" {
			err = V(err)
			return fmt.Errorf("%v\nTip: use %s to list all objects including remote", V(err), qflprn(allObjsOrBcksFlag))
		}
	}
	return V(err)
}

// usage:
// - list-objects
// - get multiple (`getMultiObj`)
// - scrub
func _setPage(c *cli.Context, bck cmn.Bck) (pageSize, maxPages, limit int64, err error) {
	maxPages = int64(parseIntFlag(c, maxPagesFlag))
	b := meta.CloneBck(&bck)
	if flagIsSet(c, pageSizeFlag) {
		pageSize = int64(parseIntFlag(c, pageSizeFlag))
		if pageSize < 0 {
			return 0, 0, 0, fmt.Errorf("invalid %s: page size (%d) cannot be negative", qflprn(pageSizeFlag), pageSize)
		}
		if pageSize > b.MaxPageSize() {
			if b.Props == nil {
				if b.Props, err = headBucket(bck, true /* don't add */); err != nil {
					return 0, 0, 0, err
				}
			}
			// still?
			if pageSize > b.MaxPageSize() {
				err := fmt.Errorf("invalid %s: page size (%d) cannot exceed the maximum (%d)",
					qflprn(pageSizeFlag), pageSize, b.MaxPageSize())
				return 0, 0, 0, err
			}
		}
	}

	limit = int64(parseIntFlag(c, objLimitFlag))
	if limit < 0 {
		err := fmt.Errorf("invalid %s=%d: max number of objects to list cannot be negative", qflprn(objLimitFlag), limit)
		return 0, 0, 0, err
	}
	if limit == 0 && maxPages > 0 {
		limit = maxPages * b.MaxPageSize()
	}
	if limit == 0 {
		return pageSize, maxPages, 0, nil
	}

	// when limit "wins"
	if limit < pageSize || (limit < b.MaxPageSize() && pageSize == 0) {
		pageSize = limit
	}
	return pageSize, maxPages, limit, nil
}

// NOTE: in addition to CACHED, may also dynamically add STATUS column
func printLso(c *cli.Context, entries cmn.LsoEntries, lstFilter *lstFilter, props string, _listed *_listed, now int64, npage int,
	addCachedCol, isRemote, addStatusCol bool) error {
	var (
		numCached      = -1
		hideHeader     = flagIsSet(c, noHeaderFlag)
		hideFooter     = flagIsSet(c, noFooterFlag)
		matched, other = lstFilter.apply(entries)
		units, errU    = parseUnitsFlag(c, unitsFlag)
	)
	if errU != nil {
		return errU
	}

	propsList := splitCsv(props)

	// validate props
	for _, prop := range propsList {
		if _, ok := teb.ObjectPropsMap[prop]; !ok {
			return fmt.Errorf("unknown object property %q (expecting one of: %v)",
				prop, cos.StrKVs(teb.ObjectPropsMap).Keys())
		}
	}

	// remote bucket: count cached; additional condition to add status
	if isRemote && addCachedCol {
		numCached = 0
		for _, en := range matched {
			if en.IsPresent() {
				numCached++
			}
			if en.IsAnyFlagSet(apc.EntryVerChanged | apc.EntryVerRemoved) {
				addStatusCol = true
			}
		}
	}

	// count-and-time only
	if now > 0 {
		// unless caption already printed
		if _listed != nil && _listed.cptn {
			return nil
		}
		lsCptn(c, npage, len(matched), numCached, mono.Since(now))
		return nil
	}

	// otherwise, print but NOTE:
	// * the flow: teb.LsoTemplate generated template to display list objects output
	// * when applied to listed objects - the `matched` below -
	//   - teb.ObjectPropsMap(prop) => FormatNameDirArch
	// * the latter looks at whether the EntryIsDir, among other things
	// * two related flags and semantics:
	//   - https://github.com/NVIDIA/aistore/blob/main/docs/howto_virt_dirs.md

	tmpl := teb.LsoTemplate(propsList, hideHeader, addCachedCol, addStatusCol)
	opts := teb.Opts{AltMap: teb.FuncMapUnits(units, false /*incl. calendar date*/)}
	if err := teb.Print(matched, tmpl, opts); err != nil {
		return err
	}

	if !hideFooter && len(matched) > 10 {
		lsCptn(c, npage, len(matched), numCached, 0)
	}
	if flagIsSet(c, showUnmatchedFlag) && len(other) > 0 {
		unmatched := fcyan("\nNames that didn't match: ") + strconv.Itoa(len(other))
		tmpl = unmatched + "\n" + tmpl
		if err := teb.Print(other, tmpl, opts); err != nil {
			return err
		}
	}
	return nil
}

func lsCptn(c *cli.Context, npage, ntotal, ncached int, elapsed time.Duration) {
	var (
		prompt = listedText
		names  = "names"
	)
	if npage > 0 {
		prompt = "Page " + strconv.Itoa(npage) + ":"
	}
	if ncached == 0 {
		names += " (in-cluster: none)"
	} else if ncached > 0 && ncached != ntotal {
		debug.Assert(ncached < ntotal, ncached, " vs ", ntotal)
		names += " (in-cluster: " + cos.FormatBigInt(ncached) + ")"
	}
	if elapsed > 0 {
		fmt.Fprintln(c.App.Writer, fblue(prompt), cos.FormatBigInt(ntotal), names, "in", teb.FormatDuration(elapsed))
	} else {
		fmt.Fprintln(c.App.Writer, fblue(prompt), cos.FormatBigInt(ntotal), names)
	}
}

///////////////
// lstFilter //
///////////////

func newLstFilter(c *cli.Context) (flt *lstFilter, prefix string, _ error) {
	flt = &lstFilter{}
	if !flagIsSet(c, allObjsOrBcksFlag) {
		// filter objects that are (any of the below):
		// - apc.LocMisplacedNode
		// - apc.LocMisplacedMountpath
		// - apc.LocIsCopy
		// - apc.LocIsCopyMissingObj
		flt._add(func(obj *cmn.LsoEnt) bool { return obj.IsStatusOK() })
	}
	if regexStr := parseStrFlag(c, regexLsAnyFlag); regexStr != "" {
		regex, err := regexp.Compile(regexStr)
		if err != nil {
			return nil, "", err
		}
		flt._add(func(obj *cmn.LsoEnt) bool { return regex.MatchString(obj.Name) })
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
			flt._add(func(obj *cmn.LsoEnt) bool { _, ok := matchingObjectNames[obj.Name]; return ok })
		}
	}
	return flt, prefix, nil
}

func (o *lstFilter) _add(f entryFilter) { o.predicates = append(o.predicates, f) }
func (o *lstFilter) _len() int          { return len(o.predicates) }

func (o *lstFilter) and(obj *cmn.LsoEnt) bool {
	for _, predicate := range o.predicates {
		if !predicate(obj) {
			return false
		}
	}
	return true
}

func (o *lstFilter) apply(entries cmn.LsoEntries) (matching, rest cmn.LsoEntries) {
	if o._len() == 0 {
		return entries, nil
	}
	for _, obj := range entries {
		if o.and(obj) {
			matching = append(matching, obj)
		} else {
			rest = append(rest, obj)
		}
	}
	return
}

// prefix that crosses shard boundary, e.g.:
// `ais ls bucket --prefix virt-subdir/A.tar.gz/dir-or-prefix-inside`
func splitPrefixShardBoundary(prefix string) (external, internal string) {
	if prefix == "" {
		return
	}
	external = prefix
	for _, ext := range archive.FileExtensions {
		i := strings.Index(prefix, ext+"/")
		if i <= 0 {
			continue
		}
		internal = prefix[i+len(ext)+1:]
		external = prefix[:i+len(ext)]
		break
	}
	return
}

func splitObjnameShardBoundary(fullName string) (objName, fileName string) {
	for _, ext := range archive.FileExtensions {
		i := strings.Index(fullName, ext+"/")
		if i <= 0 {
			continue
		}
		fileName = fullName[i+len(ext)+1:]
		objName = fullName[:i+len(ext)]
		break
	}
	return
}

/////////////
// _listed //
/////////////

type _listed struct {
	c     *cli.Context
	bck   *cmn.Bck
	msg   *apc.LsoMsg
	limit int
	l     int
	done  bool
	cptn  bool
}

func (u *_listed) cb(lsoCounter *api.LsoCounter) {
	if lsoCounter.Count() < 0 || u.done {
		return
	}
	if lsoCounter.IsFinished() || (u.limit > 0 && u.limit <= lsoCounter.Count()) {
		u.done = true
		if !flagIsSet(u.c, noFooterFlag) {
			// (compare w/ lsCptn)
			elapsed := teb.FormatDuration(lsoCounter.Elapsed())
			fmt.Fprintf(u.c.App.Writer, "\r%s %s names in %s\n", listedText, cos.FormatBigInt(lsoCounter.Count()), elapsed)
			u.cptn = true
			briefPause(1)
		}
		return
	}

	var (
		sb strings.Builder
	)
	sb.Grow(128)
	sb.WriteString(listedText)
	sb.WriteByte(' ')
	sb.WriteString(cos.FormatBigInt(lsoCounter.Count()))
	sb.WriteString(" names")
	l := sb.Len()
	if u.l == 0 {
		u.l = l + 3
		// tip
		if u.msg.IsFlagSet(apc.LsCached) {
			tip := fmt.Sprintf("consider using %s to show pages one at a time (tip)", qflprn(pagedFlag))
			actionNote(u.c, tip)
		} else if u.bck.IsRemote() {
			tip := fmt.Sprintf("use %s to speed up and/or %s to show pages", qflprn(listCachedFlag), qflprn(pagedFlag))
			note := fmt.Sprintf("listing remote objects in %s may take a while\n(Tip: %s)\n", u.bck.Cname(""), tip)
			actionNote(u.c, note)
		}
	} else if l > u.l {
		u.l = l + 2
	}
	for range u.l - l {
		sb.WriteByte(' ')
	}
	fmt.Fprintf(u.c.App.Writer, "\r%s", sb.String())
}
