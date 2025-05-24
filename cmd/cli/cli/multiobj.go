// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles object operations.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/xact"

	"github.com/urfave/cli"
)

const dryRunExamplesCnt = 10

type lrCtx struct {
	listObjs, tmplObjs string
	bck                cmn.Bck
}

func _warnProgress(c *cli.Context) {
	actionWarn(c, "cannot show progress bar with an empty list/range type option - "+NIY)
}

// x-TCO: multi-object transform or copy
func runTCO(c *cli.Context, bckFrom, bckTo cmn.Bck, listObjs, tmplObjs, etlName string) error {
	var (
		lrMsg        apc.ListRange
		numObjs      int64
		isPrefix     bool
		showProgress = flagIsSet(c, progressFlag)
	)
	// 1. list or template
	switch {
	case listObjs != "":
		lrMsg.ObjNames = splitCsv(listObjs)
		numObjs = int64(len(lrMsg.ObjNames))
	case tmplObjs == "":
		// motivation:
		// - copy the entire bucket via x-tco rather than x-tcb
		// - compare with copying or transforming not "cached" data from remote buckets
		isPrefix = true
	default:
		pt, err := cos.NewParsedTemplate(tmplObjs)
		if err != nil && err != cos.ErrEmptyTemplate { // NOTE same as above: empty => entire bucket
			return err
		}
		if len(pt.Ranges) > 0 {
			numObjs = pt.Count()
		} else {
			isPrefix = true
		}
		lrMsg.Template = tmplObjs
	}
	if showProgress && numObjs == 0 {
		_warnProgress(c)
		showProgress = false
	}

	// 2. TCO message
	msg := cmn.TCOMsg{ToBck: bckTo}
	{
		msg.ListRange = lrMsg
		msg.DryRun = flagIsSet(c, copyDryRunFlag)
		if flagIsSet(c, etlObjectRequestTimeout) {
			msg.Timeout = cos.Duration(etlObjectRequestTimeout.Value)
		}
		msg.LatestVer = flagIsSet(c, latestVerFlag)
		msg.Sync = flagIsSet(c, syncFlag)

		msg.NonRecurs = flagIsSet(c, nonRecursFlag)
		if msg.NonRecurs && !isPrefix {
			return fmt.Errorf("option %s is incompatible with the specified [list %q, range %q]",
				nonRecursFlag, listObjs, tmplObjs)
		}

		msg.ContinueOnError = flagIsSet(c, continueOnErrorFlag)
		msg.Prepend = parseStrFlag(c, copyPrependFlag)
		if flagIsSet(c, numWorkersFlag) {
			msg.NumWorkers = parseIntFlag(c, numWorkersFlag)
		}
	}
	// 3. start copying/transforming
	var (
		xid   string
		xkind string
		err   error
		text  = "Copying objects"
	)
	if etlName != "" {
		msg.Name = etlName
		text = "Transforming objects"
		xkind = apc.ActETLObjects
		xid, err = api.ETLMultiObj(apiBP, bckFrom, &msg)
	} else {
		xkind = apc.ActCopyObjects
		xid, err = api.CopyMultiObj(apiBP, bckFrom, &msg)
	}
	if err != nil {
		return err
	}

	// 4. progress bar, if requested
	if showProgress {
		var cpr = cprCtx{
			xid:  xid,
			from: bckFrom.Cname(""),
			to:   bckTo.Cname(""),
		}
		_, cpr.xname = xact.GetKindName(xkind)
		cpr.totals.objs = numObjs
		cpr.loghdr = fmt.Sprintf("%s %s => %s", xact.Cname(cpr.xname, cpr.xid), cpr.from, cpr.to)
		return cpr.multiobj(c, text)
	}

	// done
	if !flagIsSet(c, waitFlag) && !flagIsSet(c, waitJobXactFinishedFlag) {
		if flagIsSet(c, nonverboseFlag) {
			fmt.Fprintln(c.App.Writer, xid)
		} else {
			actionDone(c, tcbtcoCptn(text, bckFrom, bckTo)+". "+toMonitorMsg(c, xid, ""))
		}
		return nil
	}

	// or wait
	var timeout time.Duration

	fmt.Fprint(c.App.Writer, tcbtcoCptn(text, bckFrom, bckTo)+" ...")

	if flagIsSet(c, waitJobXactFinishedFlag) {
		timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}
	xargs := xact.ArgsMsg{ID: xid, Kind: xkind, Timeout: timeout}
	if err = waitXact(&xargs); err != nil {
		fmt.Fprintf(c.App.ErrWriter, fmtXactFailed, text, bckFrom.String(), bckTo.String())
	} else {
		fmt.Fprint(c.App.Writer, fmtXactSucceeded)
	}
	return err
}

//
// evict, rm, prefetch ------------------------------------------------------------------------
//

func evictHandler(c *cli.Context) error {
	if flagIsSet(c, verboseFlag) && flagIsSet(c, nonverboseFlag) {
		return incorrectUsageMsg(c, errFmtExclusive, qflprn(verboseFlag), qflprn(nonverboseFlag))
	}
	if flagIsSet(c, dryRunFlag) {
		dryRunCptn(c)
	}
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	for shift := range c.Args() {
		if err := _evictOne(c, shift); err != nil {
			return err
		}
	}
	return nil
}

// handle one BUCKET[/OBJECT_NAME_or_TEMPLATE] (command line may contain multiple of those)
func _evictOne(c *cli.Context, shift int) error {
	uri := preparseBckObjURI(c.Args().Get(shift))
	bck, objNameOrTmpl, err := parseBckObjURI(c, uri, true /*emptyObjnameOK*/)
	if err != nil {
		// Only try bucket query parsing for "provider-only" patterns like "s3", "gs:", etc.
		// These fail parsing as regular buckets but are valid bucket queries
		if strings.Contains(err.Error(), cos.OnlyPlus) || strings.Contains(err.Error(), "missing bucket name") {
			qbck, _, errN := parseQueryBckURI(uri)
			if errN == nil {
				return evictMultipleBuckets(c, qbck)
			}
		}
		return err
	}
	if !bck.IsRemote() {
		const msg = "evicting objects from AIS buckets (ie., buckets with no remote backends) is not allowed."
		return errors.New(msg + "\n(Tip:  consider 'ais object rm' or 'ais rmb', see --help for details)")
	}
	if shouldHeadRemote(c, bck) {
		if _, err := headBucket(bck, false /* don't add */); err != nil {
			return err
		}
	}

	// Special case: evict entire bucket when no object/prefix specified
	if objNameOrTmpl == "" {
		return evictBucket(c, bck)
	}

	oltp, err := dopOLTP(c, bck, objNameOrTmpl)
	if err != nil {
		return err
	}

	// Convert objName to list when there's no list or template (same pattern as prefetch)
	if oltp.list == "" && oltp.tmpl == "" {
		oltp.list = oltp.objName
	}

	lrCtx := &lrCtx{oltp.list, oltp.tmpl, bck}
	return lrCtx.do(c)
}

// Evict remote bucket
func evictBucket(c *cli.Context, bck cmn.Bck) error {
	if flagIsSet(c, dryRunFlag) {
		fmt.Fprintf(c.App.Writer, "%s %s\n", dryRunHeader(), bck.Cname(""))
		return nil
	}

	keepMD := flagIsSet(c, keepMDFlag)
	err := api.EvictRemoteBucket(apiBP, bck, keepMD)
	if err != nil {
		return V(err)
	}

	msg := fmt.Sprintf("Evicted bucket %s from aistore", bck.Cname(""))
	if keepMD {
		msg += " (metadata preserved)"
	}
	fmt.Fprintln(c.App.Writer, msg)
	return nil
}

// Evict multiple remote buckets based on query
func evictMultipleBuckets(c *cli.Context, qbck cmn.QueryBcks) error {
	// List buckets that match the query
	bcks, err := api.ListBuckets(apiBP, qbck, apc.FltPresent)
	if err != nil {
		return V(err)
	}

	if len(bcks) == 0 {
		return fmt.Errorf("no buckets found matching %q", qbck.String())
	}

	// Check if all buckets are remote
	for _, bck := range bcks {
		if !bck.IsRemote() {
			return fmt.Errorf("evicting objects from AIS buckets is not allowed; bucket %s has no remote backend", bck.Cname(""))
		}
	}

	if flagIsSet(c, dryRunFlag) {
		dryRunCptn(c)
	}

	// Evict each bucket using the single bucket function
	evicted := make([]string, 0, len(bcks)) // Pre-allocate with capacity
	for _, bck := range bcks {
		if err := evictBucket(c, bck); err != nil {
			actionWarn(c, fmt.Sprintf("failed to evict %s: %v", bck.Cname(""), err))
			continue
		}
		evicted = append(evicted, bck.Cname(""))
	}

	if len(evicted) == 0 {
		return errors.New("failed to evict any buckets")
	}

	return nil
}

func rmHandler(c *cli.Context) error {
	if flagIsSet(c, verboseFlag) && flagIsSet(c, nonverboseFlag) {
		return incorrectUsageMsg(c, errFmtExclusive, qflprn(verboseFlag), qflprn(nonverboseFlag))
	}
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	for shift := range c.Args() {
		if err := _rmOne(c, shift); err != nil {
			return err
		}
	}
	return nil
}

// handle one BUCKET[/OBJECT_NAME_or_TEMPLATE] (command line may contain multiple of those)
func _rmOne(c *cli.Context, shift int) error {
	uri := preparseBckObjURI(c.Args().Get(shift))
	bck, objNameOrTmpl, err := parseBckObjURI(c, uri, true /*emptyObjnameOK*/)
	if err != nil {
		return err
	}
	if shouldHeadRemote(c, bck) {
		bprops, err := headBucket(bck, false /* don't add */)
		if err != nil {
			return err
		}
		bck.Props = bprops
	}
	// [NOTE]
	// - passing empty bck _not_ to interpret embedded objName as prefix
	// - instead of HEAD(obj) do list-objects(prefix=objNameOrTmpl)  - here and everywhere
	oltp, err := dopOLTP(c, bck, objNameOrTmpl)
	if err != nil {
		return err
	}

	switch {
	case oltp.list != "" || oltp.tmpl != "": // 1. multi-obj
		lrCtx := &lrCtx{oltp.list, oltp.tmpl, bck}
		return lrCtx.do(c)
	case oltp.objName == "": // 2. all objects
		if flagIsSet(c, rmrfFlag) {
			if !flagIsSet(c, yesFlag) {
				warn := fmt.Sprintf("will remove all objects from %s. The operation cannot be undone!", bck.String())
				if ok := confirm(c, "Proceed?", warn); !ok {
					return nil
				}
			}
			return rmRfAllObjects(c, bck)
		}
		return incorrectUsageMsg(c, "to select objects to be removed use one of: (%s or %s or %s)",
			qflprn(listFlag), qflprn(templateFlag), qflprn(rmrfFlag))
	default: // 3. one obj
		err := api.DeleteObject(apiBP, bck, oltp.objName)
		if err == nil && bck.IsCloud() && oltp.notFound {
			// [NOTE]
			// - certain backends return OK when specified object does not exist (see aws.go)
			// - compensate here
			return cos.NewErrNotFound(nil, bck.Cname(oltp.objName))
		}
		if err == nil {
			if !flagIsSet(c, nonverboseFlag) {
				fmt.Fprintf(c.App.Writer, "deleted %q from %s\n", oltp.objName, bck.Cname(""))
			}
			return nil
		}
		herr, ok := err.(*cmn.ErrHTTP)
		if !ok || herr.Status != http.StatusNotFound {
			return V(err)
		}
		// not found
		var suffix string
		if c.NArg() > 1 {
			suffix = " (hint: missing double or single quotes?)"
		}
		return &errDoesNotExist{name: bck.Cname(oltp.objName), suffix: suffix}
	}
}

func startPrefetchHandler(c *cli.Context) error {
	if flagIsSet(c, dryRunFlag) {
		dryRunCptn(c)
	}
	if c.NArg() == 0 {
		return incorrectUsageMsg(c, c.Command.ArgsUsage)
	}
	for shift := range c.Args() {
		if err := _prefetchOne(c, shift); err != nil {
			return err
		}
	}
	return nil
}

// ditto
func _prefetchOne(c *cli.Context, shift int) error {
	uri := preparseBckObjURI(c.Args().Get(shift))
	bck, objNameOrTmpl, err := parseBckObjURI(c, uri, true /*emptyObjnameOK*/)

	if err != nil {
		return err
	}
	if shouldHeadRemote(c, bck) {
		if bck.Props, err = headBucket(bck, true /* add */); err != nil {
			return err
		}
	}
	if !bck.IsRemote() {
		return fmt.Errorf("expecting remote bucket (have %s)", bck.Cname(""))
	}
	oltp, err := dopOLTP(c, bck, objNameOrTmpl)
	if err != nil {
		return err
	}
	if oltp.notFound { // (true only when list-objects says so)
		err := cos.NewErrNotFound(nil, "\""+uri+"\"")
		if !flagIsSet(c, yesFlag) {
			if ok := confirm(c, err.Error()+" - proceed anyway?"); !ok {
				return err
			}
		} else {
			actionWarn(c, err.Error()+" - proceeding anyway")
		}
	}

	if oltp.list == "" && oltp.tmpl == "" {
		oltp.list = oltp.objName // ("prefetch" is not one of those primitive verbs)
	}
	lrCtx := &lrCtx{oltp.list, oltp.tmpl, bck}
	return lrCtx.do(c)
}

//
// lrCtx: evict, rm, prefetch
//

func (lr *lrCtx) do(c *cli.Context) error {
	var (
		fileList      []string
		kind          string
		pt            cos.ParsedTemplate
		emptyTemplate bool
	)
	// 1. parse
	if lr.listObjs != "" {
		fileList = splitCsv(lr.listObjs)
	} else {
		var err error
		pt, err = cos.NewParsedTemplate(lr.tmplObjs) // NOTE: prefix w/ no range is fine
		if err != nil {
			if err != cos.ErrEmptyTemplate {
				fmt.Fprintf(c.App.Writer, "invalid template %q: %v\n", lr.tmplObjs, err)
				return err
			}
			emptyTemplate = true // NOTE: empty tmplObjs means "all objects"
		}
	}

	// 2. [DRY-RUN]
	if flagIsSet(c, dryRunFlag) {
		lr.dry(c, fileList, &pt)
		return nil
	}

	// 3. do
	xid, kind, action, errV := lr._do(c, fileList)
	if errV != nil {
		return V(errV)
	}

	// 4. format
	var (
		xname, text string
		num         int64
	)
	if lr.listObjs != "" {
		num = int64(len(fileList))
		s := fmt.Sprintf("%v", fileList)
		if num > 4 {
			s = fmt.Sprintf("%v...", fileList[:4])
		}
		_, xname = xact.GetKindName(kind)
		text = fmt.Sprintf("%s: %s %s from %s", xact.Cname(xname, xid), s, action, lr.bck.Cname(""))
	} else {
		if lr.tmplObjs != "" && !emptyTemplate && len(pt.Ranges) != 0 {
			num = pt.Count()
		}
		_, xname = xact.GetKindName(kind)
		if emptyTemplate {
			text = fmt.Sprintf("%s: %s entire bucket %s", xact.Cname(xname, xid), action, lr.bck.Cname(""))
		} else {
			text = fmt.Sprintf("%s: %s %q from %s", xact.Cname(xname, xid), action, lr.tmplObjs, lr.bck.Cname(""))
		}
	}

	// 5. progress
	showProgress := flagIsSet(c, progressFlag)
	if showProgress && num == 0 {
		_warnProgress(c)
		showProgress = false
	}
	if showProgress {
		var cpr = cprCtx{
			xname:  xname,
			xid:    xid,
			from:   lr.bck.Cname(""),
			loghdr: text,
		}
		cpr.totals.objs = num
		return cpr.multiobj(c, text)
	}

	// 6. otherwise, wait or exit
	if !flagIsSet(c, waitFlag) && !flagIsSet(c, waitJobXactFinishedFlag) {
		if xid != "" {
			text += ". " + toMonitorMsg(c, xid, "")
		}
		fmt.Fprintln(c.App.Writer, text)
		return nil
	}

	// wait
	var timeout time.Duration
	if flagIsSet(c, waitJobXactFinishedFlag) {
		timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}
	fmt.Fprintln(c.App.Writer, text+" ...")
	xargs := xact.ArgsMsg{ID: xid, Kind: xname, Timeout: timeout}
	if err := waitXact(&xargs); err != nil {
		return err
	}
	fmt.Fprint(c.App.Writer, fmtXactSucceeded)
	return nil
}

// [DRY-RUN]
func (lr *lrCtx) dry(c *cli.Context, fileList []string, pt *cos.ParsedTemplate) {
	if len(fileList) > 0 {
		limitedLineWriter(c.App.Writer,
			dryRunExamplesCnt, strings.ToUpper(c.Command.Name)+" "+lr.bck.Cname("")+"/%s\n", fileList)
		return
	}
	objs := pt.ToSlice(dryRunExamplesCnt)
	limitedLineWriter(c.App.Writer,
		dryRunExamplesCnt, strings.ToUpper(c.Command.Name)+" "+lr.bck.Cname("")+"/%s", objs)
	if pt.Count() > dryRunExamplesCnt {
		fmt.Fprintf(c.App.Writer, "(and %d more)\n", pt.Count()-dryRunExamplesCnt)
	}
}

func (lr *lrCtx) _do(c *cli.Context, fileList []string) (xid, kind, action string, err error) {
	verb := c.Command.Name
	if isAlias(c) {
		verb = lastAliasedWord(c)
	}

	switch verb {
	case commandRemove:
		msg := &apc.EvdMsg{
			ListRange: apc.ListRange{ObjNames: fileList, Template: lr.tmplObjs},
			NonRecurs: flagIsSet(c, nonRecursFlag),
		}
		xid, err = api.DeleteMultiObj(apiBP, lr.bck, msg)
		kind = apc.ActDeleteObjects
		action = "rm"
	case commandPrefetch:
		if err := ensureRemoteProvider(lr.bck); err != nil {
			return "", "", "", err
		}
		var msg apc.PrefetchMsg
		{
			msg.ObjNames = fileList
			msg.Template = lr.tmplObjs
			msg.LatestVer = flagIsSet(c, latestVerFlag)
			msg.NonRecurs = flagIsSet(c, nonRecursFlag)
			if flagIsSet(c, blobThresholdFlag) {
				msg.BlobThreshold, err = parseSizeFlag(c, blobThresholdFlag)
				if err != nil {
					return "", "", "", err
				}
			}
			if flagIsSet(c, numWorkersFlag) {
				msg.NumWorkers = parseIntFlag(c, numWorkersFlag)
			}
		}
		xid, err = api.Prefetch(apiBP, lr.bck, &msg)
		kind = apc.ActPrefetchObjects
		action = "prefetch"
	case commandEvict:
		if err := ensureRemoteProvider(lr.bck); err != nil {
			return "", "", "", err
		}
		msg := &apc.EvdMsg{
			ListRange: apc.ListRange{ObjNames: fileList, Template: lr.tmplObjs},
			NonRecurs: flagIsSet(c, nonRecursFlag),
		}
		xid, err = api.EvictMultiObj(apiBP, lr.bck, msg)
		kind = apc.ActEvictObjects
		action = "evict"
	default:
		debug.Assert(false, "invalid subcommand: ", verb)
	}

	return xid, kind, action, err
}
