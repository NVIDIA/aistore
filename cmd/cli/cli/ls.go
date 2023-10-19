// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
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
	for _, bck := range bcks {
		if !qbck.Contains(&bck) {
			continue
		}
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
		footer      lsbFooter
		hideHeader  = flagIsSet(c, noHeaderFlag)
		hideFooter  = flagIsSet(c, noFooterFlag)
		data        = make([]teb.ListBucketsHelper, 0, len(bcks))
		units, errU = parseUnitsFlag(c, unitsFlag)
	)
	if errU != nil {
		actionWarn(c, errU.Error())
		units = ""
	}
	var (
		opts       = teb.Opts{AltMap: teb.FuncMapUnits(units)}
		maxwait    = listObjectsWaitTime
		bckPresent = apc.IsFltPresent(args.FltPresence) // all-buckets part in the `allObjsOrBcksFlag`
		ctx        = newBsummContext(c, units, qbck, bckPresent)
		prev       = ctx.started
	)
	debug.Assert(args.Summarize)
	args.CallAfter = ctx.args.CallAfter
	args.Callback = ctx.args.Callback // reusing bsummCtx.progress()

	// one at a time
	for i, bck := range bcks {
		if !qbck.Contains(&bck) {
			continue
		}
		ctx.qbck = cmn.QueryBcks(bck)
		props, info, err := api.GetBucketInfo(apiBP, bck, args)
		if err != nil {
			actionWarn(c, notV(err).Error())
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
	apparentSize := teb.FmtSize(int64(footer.size), units, 2)
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
	if external, internal := splitPrefixShardBoundary(prefix); len(internal) > 0 {
		origPrefix := prefix
		prefix = external
		lstFilter._add(func(obj *cmn.LsoEntry) bool { return strings.HasPrefix(obj.Name, origPrefix) })
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
	if flagIsSet(c, listObjCachedFlag) {
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
		if flagIsSet(c, dontAddRemoteFlag) {
			msg.AddProps(apc.GetPropsName)
			msg.AddProps(apc.GetPropsSize)
			msg.SetFlag(apc.LsNameSize)
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

	// list all pages up to a limit, show progress
	var (
		callAfter = listObjectsWaitTime
		u         = &_listed{c: c, bck: &bck}
	)
	if flagIsSet(c, refreshFlag) {
		callAfter = parseDurationFlag(c, refreshFlag)
	}
	args := api.ListArgs{Callback: u.cb, CallAfter: callAfter, Limit: uint(limit)}
	objList, err := api.ListObjects(apiBP, bck, msg, args)
	if err != nil {
		return lsoErr(msg, err)
	}
	return printObjProps(c, objList.Entries, lstFilter, msg.Props, addCachedCol)
}

func lsoErr(msg *apc.LsoMsg, err error) error {
	if herr, ok := err.(*cmn.ErrHTTP); ok && msg.IsFlagSet(apc.LsBckPresent) {
		if herr.TypeCode == "ErrRemoteBckNotFound" {
			err = V(err)
			return fmt.Errorf("%v\nTip: use %s option to list _all_ objects", V(err), qflprn(allObjsOrBcksFlag))
		}
	}
	return V(err)
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
		// filter objects that are "not OK" (e.g., misplaced)
		flt._add(func(obj *cmn.LsoEntry) bool { return obj.IsStatusOK() })
	}
	if regexStr := parseStrFlag(c, regexLsAnyFlag); regexStr != "" {
		regex, err := regexp.Compile(regexStr)
		if err != nil {
			return nil, "", err
		}
		flt._add(func(obj *cmn.LsoEntry) bool { return regex.MatchString(obj.Name) })
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
			flt._add(func(obj *cmn.LsoEntry) bool { _, ok := matchingObjectNames[obj.Name]; return ok })
		}
	}
	return flt, prefix, nil
}

func (o *lstFilter) _add(f entryFilter) { o.predicates = append(o.predicates, f) }
func (o *lstFilter) _len() int          { return len(o.predicates) }

func (o *lstFilter) and(obj *cmn.LsoEntry) bool {
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
	c   *cli.Context
	bck *cmn.Bck
	l   int
}

func (u *_listed) cb(ctx *api.LsoCounter) {
	if ctx.Count() < 0 {
		return
	}
	if !ctx.IsFinished() {
		s := "Listed " + cos.FormatBigNum(ctx.Count()) + " objects"
		if u.l == 0 {
			u.l = len(s) + 3
			if u.bck.IsRemote() && !flagIsSet(u.c, listObjCachedFlag) {
				note := fmt.Sprintf("listing remote objects in %s may take a while (tip: use %s to speed up)\n",
					u.bck.Cname(""), qflprn(listObjCachedFlag))
				actionNote(u.c, note)
			}
		} else if len(s) > u.l {
			u.l = len(s) + 2
		}
		s += strings.Repeat(" ", u.l-len(s))
		fmt.Fprintf(u.c.App.Writer, "\r%s", s)
	} else {
		elapsed := teb.FormatDuration(ctx.Elapsed())
		fmt.Fprintf(u.c.App.Writer, "\rListed %s objects in %v\n", cos.FormatBigNum(ctx.Count()), elapsed)
		time.Sleep(time.Second)
	}
}
