// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
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

// `ais ls`, `ais ls s3:` and similar
func listBckTable(c *cli.Context, qbck cmn.QueryBcks, bcks cmn.Bcks, lsb lsbCtx) (cnt int) {
	if flagIsSet(c, bckSummaryFlag) {
		args := api.BinfoArgs{
			FltPresence:   lsb.fltPresence,     // all-buckets part in the `allObjsOrBcksFlag`
			WithRemote:    lsb.countRemoteObjs, // all-objects part --/--
			Summarize:     true,
			DontAddRemote: flagIsSet(c, dontAddRemoteFlag),
		}
		cnt = listBckTableWithSummary(c, qbck, bcks, args)
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
		if bck.IsHTTP() {
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
func listBckTableWithSummary(c *cli.Context, qbck cmn.QueryBcks, bcks cmn.Bcks, args api.BinfoArgs) int {
	var (
		footer     lsbFooter
		hideHeader = flagIsSet(c, noHeaderFlag)
		hideFooter = flagIsSet(c, noFooterFlag)
		maxwait    = listObjectsWaitTime

		objCached  = flagIsSet(c, listObjCachedFlag)
		bckPresent = flagIsSet(c, allObjsOrBcksFlag)
		prefix     = parseStrFlag(c, listObjPrefixFlag)
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
	opts := teb.Opts{AltMap: teb.FuncMapUnits(ctx.units)}
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
		if bck.IsHTTP() {
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
		addCachedCol = true
		msg.SetFlag(apc.LsBckPresent) // default
	}
	if flagIsSet(c, verChangedFlag) {
		if bck.IsAIS() {
			return fmt.Errorf("flag %s requires remote bucket (have: %s)", qflprn(verChangedFlag), bck)
		}
		if !bck.HasVersioningMD() {
			return fmt.Errorf("flag %s only applies to remote backends that maintain at least some form of versioning information (have: %s)",
				qflprn(verChangedFlag), bck)
		}
		msg.SetFlag(apc.LsVerChanged)
	}

	if flagIsSet(c, listObjCachedFlag) {
		if flagIsSet(c, verChangedFlag) {
			actionWarn(c, "checking remote versions may take some time...\n")
			briefPause(1)
		}
		msg.SetFlag(apc.LsObjCached)
		addCachedCol = false // redundant
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
	if listArch {
		msg.SetFlag(apc.LsArchDir)
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
	}

	// add _implied_ props into control lsmsg
	if flagIsSet(c, nameOnlyFlag) {
		if flagIsSet(c, verChangedFlag) {
			return fmt.Errorf(errFmtExclusive, qflprn(verChangedFlag), qflprn(nameOnlyFlag))
		}
		if len(props) > 2 {
			warn := fmt.Sprintf("flag %s is incompatible with the value of %s", qflprn(nameOnlyFlag), qflprn(objPropsFlag))
			actionWarn(c, warn)
		}
		msg.SetFlag(apc.LsNameOnly)
		msg.Props = apc.GetPropsName
	} else if len(props) == 0 {
		if flagIsSet(c, dontAddRemoteFlag) {
			msg.AddProps(apc.GetPropsName)
			msg.AddProps(apc.GetPropsSize)
			msg.SetFlag(apc.LsNameSize)
		} else if catOnly {
			msg.SetFlag(apc.LsNameOnly)
			msg.Props = apc.GetPropsName
		} else {
			msg.AddProps(apc.GetPropsMinimal...)
		}
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
	propsStr = msg.Props // show these and _only_ these props
	// finally:
	if flagIsSet(c, verChangedFlag) {
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
		pageCounter, toShow := 0, int(limit)
		for {
			if catOnly {
				now = mono.NanoTime()
			}
			objList, err := api.ListObjectsPage(apiBP, bck, msg, lsargs)
			if err != nil {
				return lsoErr(msg, err)
			}

			// print exact number of objects if it is `limit`ed: in case of
			// limit > page size, the last page is printed partially
			var toPrint cmn.LsoEntries
			if limit > 0 && toShow < len(objList.Entries) {
				toPrint = objList.Entries[:toShow]
			} else {
				toPrint = objList.Entries
			}
			err = printLso(c, toPrint, lstFilter, propsStr, nil /*_listed*/, now,
				addCachedCol, bck.IsRemote(), msg.IsFlagSet(apc.LsVerChanged))
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
				toShow -= len(objList.Entries)
				if toShow <= 0 {
					return nil
				}
			}
		}
	}

	// alternatively (when `--paged` not specified) list all pages up to a limit, show progress
	var (
		callAfter = listObjectsWaitTime
		_listed   = &_listed{c: c, bck: &bck, limit: int(limit)}
	)
	if flagIsSet(c, refreshFlag) {
		callAfter = parseDurationFlag(c, refreshFlag)
	}
	if catOnly {
		now = mono.NanoTime()
	}
	lsargs.Callback = _listed.cb
	lsargs.CallAfter = callAfter
	objList, err := api.ListObjects(apiBP, bck, msg, lsargs)
	if err != nil {
		return lsoErr(msg, err)
	}
	return printLso(c, objList.Entries, lstFilter, propsStr, _listed, now,
		addCachedCol, bck.IsRemote(), msg.IsFlagSet(apc.LsVerChanged))
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

func _setPage(c *cli.Context, bck cmn.Bck) (pageSize, maxPages, limit int64, err error) {
	maxPages = int64(parseIntFlag(c, maxPagesFlag))
	b := meta.CloneBck(&bck)
	if flagIsSet(c, pageSizeFlag) {
		pageSize = int64(parseIntFlag(c, pageSizeFlag))
		if pageSize < 0 {
			err = fmt.Errorf("invalid %s: page size (%d) cannot be negative", qflprn(pageSizeFlag), pageSize)
			return
		}
		if pageSize > b.MaxPageSize() {
			if b.Props == nil {
				if b.Props, err = headBucket(bck, true /* don't add */); err != nil {
					return
				}
			}
			// still?
			if pageSize > b.MaxPageSize() {
				err = fmt.Errorf("invalid %s: page size (%d) cannot exceed the maximum (%d)",
					qflprn(pageSizeFlag), pageSize, b.MaxPageSize())
				return
			}
		}
	}

	limit = int64(parseIntFlag(c, objLimitFlag))
	if limit < 0 {
		err = fmt.Errorf("invalid %s=%d: max number of objects to list cannot be negative", qflprn(objLimitFlag), limit)
		return
	}
	if limit == 0 && maxPages > 0 {
		limit = maxPages * b.MaxPageSize()
	}
	if limit == 0 {
		return
	}

	// when limit "wins"
	if limit < pageSize || (limit < b.MaxPageSize() && pageSize == 0) {
		pageSize = limit
	}
	return
}

// NOTE: in addition to CACHED, may also dynamically add STATUS column
func printLso(c *cli.Context, entries cmn.LsoEntries, lstFilter *lstFilter, props string, _listed *_listed, now int64,
	addCachedCol, isRemote, addStatusCol bool) error {
	var (
		hideHeader     = flagIsSet(c, noHeaderFlag)
		hideFooter     = flagIsSet(c, noFooterFlag)
		matched, other = lstFilter.apply(entries)
		units, errU    = parseUnitsFlag(c, unitsFlag)
	)
	if errU != nil {
		return errU
	}

	propsList := splitCsv(props)
	if isRemote && !addStatusCol {
		if addCachedCol && !cos.StringInSlice(apc.GetPropsStatus, propsList) {
			for _, e := range entries {
				if e.IsVerChanged() {
					addStatusCol = true
					break
				}
			}
		}
	}

	// validate props for typos
	for _, prop := range propsList {
		if _, ok := teb.ObjectPropsMap[prop]; !ok {
			return fmt.Errorf("unknown object property %q (expecting one of: %v)",
				prop, cos.StrKVs(teb.ObjectPropsMap).Keys())
		}
	}

	// count-and-time only
	if now > 0 {
		// unless caption already printed
		if _listed != nil && _listed.cptn {
			return nil
		}
		elapsed := teb.FormatDuration(mono.Since(now))
		fmt.Fprintln(c.App.Writer, listedText, cos.FormatBigNum(len(matched)), "names in", elapsed)
		return nil
	}

	// otherwise, print names
	tmpl := teb.LsoTemplate(propsList, hideHeader, addCachedCol, addStatusCol)
	opts := teb.Opts{AltMap: teb.FuncMapUnits(units)}
	if err := teb.Print(matched, tmpl, opts); err != nil {
		return err
	}

	if !hideFooter && len(matched) > 10 {
		fmt.Fprintln(c.App.Writer, fblue(listedText), cos.FormatBigNum(len(matched)), "names")
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

///////////////
// lstFilter //
///////////////

func newLstFilter(c *cli.Context) (flt *lstFilter, prefix string, _ error) {
	flt = &lstFilter{}
	if !flagIsSet(c, allObjsOrBcksFlag) {
		// filter objects that are "not OK" (e.g., misplaced)
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
	limit int
	l     int
	done  bool
	cptn  bool
}

func (u *_listed) cb(ctx *api.LsoCounter) {
	if ctx.Count() < 0 || u.done {
		return
	}
	if ctx.IsFinished() || (u.limit > 0 && u.limit <= ctx.Count()) {
		u.done = true
		if !flagIsSet(u.c, noFooterFlag) {
			elapsed := teb.FormatDuration(ctx.Elapsed())
			fmt.Fprintf(u.c.App.Writer, "\r%s %s names in %s\n", listedText, cos.FormatBigNum(ctx.Count()), elapsed)
			u.cptn = true
			briefPause(1)
		}
		return
	}

	s := listedText + " " + cos.FormatBigNum(ctx.Count()) + " names"
	if u.l == 0 {
		u.l = len(s) + 3
		if u.bck.IsRemote() {
			var tip string
			if flagIsSet(u.c, listObjCachedFlag) {
				tip = fmt.Sprintf("use %s to show pages immediately - one page at a time", qflprn(pagedFlag))
			} else {
				tip = fmt.Sprintf("use %s to speed up and/or %s to show pages", qflprn(listObjCachedFlag), qflprn(pagedFlag))
			}
			note := fmt.Sprintf("listing remote objects in %s may take a while\n(Tip: %s)\n", u.bck.Cname(""), tip)
			actionNote(u.c, note)
		}
	} else if len(s) > u.l {
		u.l = len(s) + 2
	}
	s += strings.Repeat(" ", u.l-len(s))
	fmt.Fprintf(u.c.App.Writer, "\r%s", s)
}
