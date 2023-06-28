// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/xact"
	"github.com/urfave/cli"
	"k8s.io/apimachinery/pkg/util/duration"
)

const (
	fmtXactFailed      = "Failed to %s (%q => %q)\n"
	fmtXactSucceeded   = "Done.\n"
	fmtXactWaitStarted = "%s %s => %s ...\n"
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

// Creates new ais bucket
func createBucket(c *cli.Context, bck cmn.Bck, props *cmn.BucketPropsToUpdate) (err error) {
	if err = api.CreateBucket(apiBP, bck, props); err != nil {
		if herr, ok := err.(*cmn.ErrHTTP); ok {
			if herr.Status == http.StatusConflict {
				desc := fmt.Sprintf("Bucket %q already exists", bck)
				if flagIsSet(c, ignoreErrorFlag) {
					fmt.Fprint(c.App.Writer, desc)
					return nil
				}
				return errors.New(desc)
			}
			if verbose() {
				herr.Message = herr.StringEx()
			}
			return fmt.Errorf("failed to create %q: %s", bck, herr.Message)
		}
		return fmt.Errorf("failed to create %q: %v", bck, err)
	}
	// NOTE: see docs/bucket.md#default-bucket-properties
	fmt.Fprintf(c.App.Writer, "%q created\n", bck.Cname(""))
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
		if err = api.DestroyBucket(apiBP, bck); err == nil {
			fmt.Fprintf(c.App.Writer, "%q destroyed\n", bck.Cname(""))
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
func mvBucket(c *cli.Context, bckFrom, bckTo cmn.Bck) error {
	if _, err := headBucket(bckFrom, true /* don't add */); err != nil {
		return err
	}
	xid, err := api.RenameBucket(apiBP, bckFrom, bckTo)
	if err != nil {
		return V(err)
	}
	_, xname := xact.GetKindName(apc.ActMoveBck)
	text := fmt.Sprintf("%s[%s] %s => %s", xname, xid, bckFrom, bckTo)
	if !flagIsSet(c, waitFlag) && !flagIsSet(c, waitJobXactFinishedFlag) {
		actionDone(c, text+". "+toMonitorMsg(c, xid, ""))
		return nil
	}

	// wait
	var timeout time.Duration
	if flagIsSet(c, waitJobXactFinishedFlag) {
		timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}
	fmt.Fprintln(c.App.Writer, text+" ...")
	xargs := xact.ArgsMsg{ID: xid, Kind: apc.ActMoveBck, Timeout: timeout}
	if err := waitXact(apiBP, xargs); err != nil {
		fmt.Fprintf(c.App.Writer, fmtXactFailed, "rename", bckFrom, bckTo)
		return err
	}
	fmt.Fprint(c.App.Writer, fmtXactSucceeded)
	return nil
}

// Evict remote bucket
func evictBucket(c *cli.Context, bck cmn.Bck) (err error) {
	if flagIsSet(c, dryRunFlag) {
		fmt.Fprintf(c.App.Writer, "Evict: %q\n", bck.Cname(""))
		return
	}
	if err = ensureHasProvider(bck); err != nil {
		return
	}
	if err = api.EvictRemoteBucket(apiBP, bck, flagIsSet(c, keepMDFlag)); err != nil {
		return
	}
	fmt.Fprintf(c.App.Writer, "%q bucket evicted\n", bck.Cname(""))
	return
}

func listBuckets(c *cli.Context, qbck cmn.QueryBcks, fltPresence int, countRemoteObjs bool) (err error) {
	var (
		regex  *regexp.Regexp
		fmatch = func(_ cmn.Bck) bool { return true }
	)
	if regexStr := parseStrFlag(c, regexLsAnyFlag); regexStr != "" {
		regex, err = regexp.Compile(regexStr)
		if err != nil {
			return
		}
		fmatch = func(bck cmn.Bck) bool { return regex.MatchString(bck.Name) }
	}
	bcks, errV := api.ListBuckets(apiBP, qbck, fltPresence)
	if errV != nil {
		return V(errV)
	}

	// NOTE:
	// typing `ls ais://@` (with an '@' symbol) to query remote ais buckets may not be
	// very obvious (albeit documented); thus, for the sake of usability making
	// an exception - extending ais queries to include remote ais

	if !qbck.IsRemoteAIS() && (qbck.Provider == apc.AIS || qbck.Provider == "") {
		if _, err := api.GetRemoteAIS(apiBP); err == nil {
			qrais := qbck
			qrais.Ns = cmn.NsAnyRemote
			if brais, err := api.ListBuckets(apiBP, qrais, fltPresence); err == nil && len(brais) > 0 {
				bcks = append(bcks, brais...)
			}
		}
	}

	if len(bcks) == 0 && apc.IsFltPresent(fltPresence) && !qbck.IsAIS() {
		const hint = "Use %s option to list _all_ buckets.\n"
		if qbck.IsEmpty() {
			fmt.Fprintf(c.App.Writer, "No buckets in the cluster. "+hint, qflprn(allObjsOrBcksFlag))
		} else {
			fmt.Fprintf(c.App.Writer, "No %q matching buckets in the cluster. "+hint, qbck, qflprn(allObjsOrBcksFlag))
		}
		return
	}
	var total int
	for _, provider := range selectProvidersExclRais(bcks) {
		qbck = cmn.QueryBcks{Provider: provider}
		if provider == apc.AIS {
			qbck.Ns = cmn.NsGlobal // "local" cluster
		}
		cnt := listBckTable(c, qbck, bcks, fmatch, fltPresence, countRemoteObjs)
		if cnt > 0 {
			fmt.Fprintln(c.App.Writer)
			total += cnt
		}
	}
	// finally, list remote ais buckets, if any
	qbck = cmn.QueryBcks{Provider: apc.AIS, Ns: cmn.NsAnyRemote}
	cnt := listBckTable(c, qbck, bcks, fmatch, fltPresence, countRemoteObjs)
	if cnt > 0 || total == 0 {
		fmt.Fprintln(c.App.Writer)
	}
	return
}

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
			ctx.Info().Count, duration.HumanDuration(ctx.Elapsed()))
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

// If both backend_bck.name and backend_bck.provider are present, use them.
// Otherwise, replace as follows:
//   - e.g., `backend_bck=gcp://bucket_name` with `backend_bck.name=bucket_name` and
//     `backend_bck.provider=gcp` to match expected fields.
//   - `backend_bck=none` with `backend_bck.name=""` and `backend_bck.provider=""`.
func reformatBackendProps(c *cli.Context, nvs cos.StrKVs) (err error) {
	var (
		originBck cmn.Bck
		v         string
		ok        bool
	)

	if v, ok = nvs[cmn.PropBackendBckName]; ok && v != "" {
		if v, ok = nvs[cmn.PropBackendBckProvider]; ok && v != "" {
			nvs[cmn.PropBackendBckProvider], err = cmn.NormalizeProvider(v)
			return
		}
	}

	if v, ok = nvs[cmn.PropBackendBck]; ok {
		delete(nvs, cmn.PropBackendBck)
	} else if v, ok = nvs[cmn.PropBackendBckName]; !ok {
		goto validate
	}

	if v != NilValue {
		if originBck, err = parseBckURI(c, v, true /*error only*/); err != nil {
			return fmt.Errorf("invalid '%s=%s': expecting %q to be a valid bucket name",
				cmn.PropBackendBck, v, v)
		}
	}

	nvs[cmn.PropBackendBckName] = originBck.Name
	if v, ok = nvs[cmn.PropBackendBckProvider]; ok && v != "" {
		nvs[cmn.PropBackendBckProvider], err = cmn.NormalizeProvider(v)
	} else {
		nvs[cmn.PropBackendBckProvider] = originBck.Provider
	}

validate:
	if nvs[cmn.PropBackendBckProvider] != "" && nvs[cmn.PropBackendBckName] == "" {
		return fmt.Errorf("invalid %q: bucket name cannot be empty when bucket provider (%q) is set",
			cmn.PropBackendBckName, cmn.PropBackendBckProvider)
	}
	return err
}

// Get bucket props
func showBucketProps(c *cli.Context) (err error) {
	var (
		bck cmn.Bck
		p   *cmn.BucketProps
	)

	if c.NArg() > 2 {
		return incorrectUsageMsg(c, "", c.Args()[2:])
	}

	section := c.Args().Get(1)

	if bck, err = parseBckURI(c, c.Args().Get(0), false); err != nil {
		return
	}
	if p, err = headBucket(bck, true /* don't add */); err != nil {
		return
	}

	if bck.IsRemoteAIS() {
		if all, err := api.GetRemoteAIS(apiBP); err == nil {
			for _, remais := range all.A {
				if remais.Alias != bck.Ns.UUID && remais.UUID != bck.Ns.UUID {
					continue
				}
				altbck := bck
				if remais.Alias == bck.Ns.UUID {
					altbck.Ns.UUID = remais.UUID
				} else {
					altbck.Ns.UUID = remais.Alias
				}
				fmt.Fprintf(c.App.Writer, "remote cluster alias:\t\t%s\n", fcyan(remais.Alias))
				fmt.Fprintf(c.App.Writer, "remote cluster UUID:\t\t%s\n", fcyan(remais.UUID))
				fmt.Fprintf(c.App.Writer, "alternative bucket name:\t%s\n\n", fcyan(altbck.String()))
				break
			}
		}
	}

	if flagIsSet(c, jsonFlag) {
		opts := teb.Jopts(true)
		return teb.Print(p, "", opts)
	}

	defProps, err := defaultBckProps(bck)
	if err != nil {
		return err
	}
	return HeadBckTable(c, p, defProps, section)
}

func HeadBckTable(c *cli.Context, props, defProps *cmn.BucketProps, section string) error {
	var (
		defList nvpairList
		colored = !cfg.NoColor
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
				if def.Name == cmn.PropBucketCreated {
					if p.Value != teb.NotSetVal {
						created, err := cos.S2UnixNano(p.Value)
						if err == nil {
							p.Value = fmtBucketCreatedTime(created)
						}
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

	return teb.Print(propList, teb.PropsSimpleTmpl)
}

// Configure bucket as n-way mirror
func configureNCopies(c *cli.Context, bck cmn.Bck, copies int) (err error) {
	var xid string
	if xid, err = api.MakeNCopies(apiBP, bck, copies); err != nil {
		return
	}
	var baseMsg string
	if copies > 1 {
		baseMsg = fmt.Sprintf("Configured %s as %d-way mirror. ", bck.Cname(""), copies)
	} else {
		baseMsg = fmt.Sprintf("Configured %s for single-replica (no redundancy). ", bck.Cname(""))
	}
	actionDone(c, baseMsg+toMonitorMsg(c, xid, ""))
	return
}

// erasure code the entire bucket
func ecEncode(c *cli.Context, bck cmn.Bck, data, parity int) (err error) {
	var xid string
	if xid, err = api.ECEncodeBucket(apiBP, bck, data, parity); err != nil {
		return
	}
	msg := fmt.Sprintf("Erasure-coding bucket %s. ", bck.Cname(""))
	actionDone(c, msg+toMonitorMsg(c, xid, ""))
	return
}

func printObjProps(c *cli.Context, entries cmn.LsoEntries, objectFilter *lstFilter, props string, addCachedCol bool) error {
	var (
		hideHeader     = flagIsSet(c, noHeaderFlag)
		matched, other = objectFilter.filter(entries)
		units, errU    = parseUnitsFlag(c, unitsFlag)
	)
	if errU != nil {
		return errU
	}

	propsList := splitCsv(props)
	tmpl := teb.ObjPropsTemplate(propsList, hideHeader, addCachedCol)
	opts := teb.Opts{AltMap: teb.FuncMapUnits(units)}
	if err := teb.Print(matched, tmpl, opts); err != nil {
		return err
	}

	if flagIsSet(c, showUnmatchedFlag) {
		unmatched := fcyan("\nNames that don't match:")
		if len(other) == 0 {
			fmt.Fprintln(c.App.Writer, unmatched+" none")
		} else {
			tmpl = unmatched + "\n" + tmpl
			if err := teb.Print(other, tmpl, opts); err != nil {
				return err
			}
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
