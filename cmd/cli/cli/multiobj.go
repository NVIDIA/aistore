// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles object operations.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
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

// x-TCO: multi-object transform or copy
func runTCO(c *cli.Context, bckFrom, bckTo cmn.Bck, listObjs, tmplObjs, etlName string) error {
	var (
		lrMsg        apc.ListRange
		numObjs      int64
		showProgress = flagIsSet(c, progressFlag)
	)
	// 1. list or template
	if listObjs != "" {
		lrMsg.ObjNames = splitCsv(listObjs)
		numObjs = int64(len(lrMsg.ObjNames))
	} else if tmplObjs == "" {
		// motivation: copy the entire bucket via x-tco rather than x-tcb
		// (compare with copying or transforming not "cached" data from remote buckets, etc.)
	} else {
		pt, err := cos.NewParsedTemplate(tmplObjs)
		if err != nil && err != cos.ErrEmptyTemplate { // NOTE same as above: empty => entire bucket
			return err
		}
		if len(pt.Ranges) > 0 {
			numObjs = pt.Count()
		}
		lrMsg.Template = tmplObjs
	}
	if showProgress && numObjs == 0 {
		actionWarn(c, "cannot show progress bar with an empty list/range type option - not implemented yet")
		showProgress = false
	}

	// 2. TCO message
	msg := cmn.TCObjsMsg{ToBck: bckTo}
	{
		msg.ListRange = lrMsg
		msg.DryRun = flagIsSet(c, copyDryRunFlag)
		if flagIsSet(c, etlBucketRequestTimeout) {
			msg.Timeout = cos.Duration(etlBucketRequestTimeout.Value)
		}
		msg.ContinueOnError = flagIsSet(c, continueOnErrorFlag)
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
		cpr.loghdr = fmt.Sprintf("%s[%s] %s => %s", cpr.xname, cpr.xid, cpr.from, cpr.to)
		return cpr.multiobj(c, text)
	}

	// done
	if !flagIsSet(c, waitFlag) && !flagIsSet(c, waitJobXactFinishedFlag) {
		actionDone(c, tcbtcoCptn(text, bckFrom, bckTo)+". "+toMonitorMsg(c, xid, ""))
		return nil
	}

	// or wait
	var timeout time.Duration

	fmt.Fprintf(c.App.Writer, tcbtcoCptn(text, bckFrom, bckTo)+" ...")

	if flagIsSet(c, waitJobXactFinishedFlag) {
		timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}
	xargs := xact.ArgsMsg{ID: xid, Kind: xkind, Timeout: timeout}
	if err = waitXact(apiBP, &xargs); err != nil {
		fmt.Fprintf(c.App.Writer, fmtXactFailed, text, bckFrom, bckTo)
	} else {
		fmt.Fprint(c.App.Writer, fmtXactSucceeded)
	}
	return err
}

// evict, rm, prefetch
func listrange(c *cli.Context, bck cmn.Bck, listObjs, tmplObjs string) (err error) {
	var (
		xid, xname string
		text       string
		num        int64
	)
	if listObjs != "" {
		xid, xname, text, num, err = _listOp(c, bck, listObjs)
	} else {
		xid, xname, text, num, err = _rangeOp(c, bck, tmplObjs)
	}
	if err != nil {
		return
	}

	// progress bar
	showProgress := flagIsSet(c, progressFlag)
	if showProgress {
		var cpr = cprCtx{
			xname:  xname,
			xid:    xid,
			from:   bck.Cname(""),
			loghdr: text,
		}
		cpr.totals.objs = num
		return cpr.multiobj(c, text)
	}

	// otherwise, wait or exit
	if !flagIsSet(c, waitFlag) && !flagIsSet(c, waitJobXactFinishedFlag) {
		if xid != "" {
			text += ". " + toMonitorMsg(c, xid, "")
		}
		fmt.Fprintln(c.App.Writer, text)
		return
	}

	// wait
	var timeout time.Duration
	if flagIsSet(c, waitJobXactFinishedFlag) {
		timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}
	fmt.Fprintln(c.App.Writer, text+" ...")
	xargs := xact.ArgsMsg{ID: xid, Kind: xname, Timeout: timeout}
	if err := waitXact(apiBP, &xargs); err != nil {
		return err
	}
	fmt.Fprint(c.App.Writer, fmtXactSucceeded)
	return
}

// `--list` flag
func _listOp(c *cli.Context, bck cmn.Bck, arg string) (xid, xname, text string, num int64, err error) {
	var (
		kind     string
		fileList = splitCsv(arg)
	)
	if flagIsSet(c, dryRunFlag) {
		limitedLineWriter(c.App.Writer,
			dryRunExamplesCnt, strings.ToUpper(c.Command.Name)+" "+bck.Cname("")+"/%s\n", fileList)
		return
	}
	var action string
	switch c.Command.Name {
	case commandRemove:
		xid, err = api.DeleteList(apiBP, bck, fileList)
		kind = apc.ActDeleteObjects
		action = "rm"
	case commandPrefetch:
		if err = ensureHasProvider(bck); err != nil {
			return
		}
		xid, err = api.PrefetchList(apiBP, bck, fileList)
		kind = apc.ActPrefetchObjects
		action = "prefetch"
	case commandEvict:
		if err = ensureHasProvider(bck); err != nil {
			return
		}
		xid, err = api.EvictList(apiBP, bck, fileList)
		kind = apc.ActEvictObjects
		action = "evict"
	default:
		debug.Assert(false, c.Command.Name)
		return
	}
	if err != nil {
		return
	}
	num = int64(len(fileList))
	s := fmt.Sprintf("%v", fileList)
	if num > 4 {
		s = fmt.Sprintf("%v...", fileList[:4])
	}
	_, xname = xact.GetKindName(kind)
	text = fmt.Sprintf("%s[%s]: %s %s from %s", xname, xid, s, action, bck.Cname(""))
	return
}

// `--range` flag
func _rangeOp(c *cli.Context, bck cmn.Bck, rangeStr string) (xid, xname, text string, num int64, err error) {
	var (
		kind          string
		pt            cos.ParsedTemplate
		emptyTemplate bool
	)
	pt, err = cos.NewParsedTemplate(rangeStr) // NOTE: prefix w/ no range is fine
	if err != nil {
		if err != cos.ErrEmptyTemplate {
			fmt.Fprintf(c.App.Writer, "invalid template %q: %v\n", rangeStr, err)
			return
		}
		err, emptyTemplate = nil, true
	}
	// [DRY-RUN]
	if flagIsSet(c, dryRunFlag) {
		objs := pt.ToSlice(dryRunExamplesCnt)
		limitedLineWriter(c.App.Writer,
			dryRunExamplesCnt, strings.ToUpper(c.Command.Name)+" "+bck.Cname("")+"/%s", objs)
		if pt.Count() > dryRunExamplesCnt {
			fmt.Fprintf(c.App.Writer, "(and %d more)\n", pt.Count()-dryRunExamplesCnt)
		}
		return
	}

	var action string
	switch c.Command.Name {
	case commandRemove:
		xid, err = api.DeleteRange(apiBP, bck, rangeStr)
		kind = apc.ActDeleteObjects
		action = "rm"
	case commandPrefetch:
		if err = ensureHasProvider(bck); err != nil {
			return
		}
		xid, err = api.PrefetchRange(apiBP, bck, rangeStr)
		kind = apc.ActPrefetchObjects
		action = "prefetch"
	case commandEvict:
		if err = ensureHasProvider(bck); err != nil {
			return
		}
		xid, err = api.EvictRange(apiBP, bck, rangeStr)
		kind = apc.ActEvictObjects
		action = "evict"
	default:
		debug.Assert(false, c.Command.Name)
		return
	}
	if err != nil {
		return
	}
	num = pt.Count()
	_, xname = xact.GetKindName(kind)
	if emptyTemplate {
		text = fmt.Sprintf("%s[%s]: %s entire bucket %s", xname, xid, action, bck.Cname(""))
	} else {
		text = fmt.Sprintf("%s[%s]: %s %q from %s", xname, xid, action, rangeStr, bck.Cname(""))
	}
	return xid, xname, text, num, nil
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
		return err
	}
	if !bck.IsRemote() {
		const msg = "evicting objects from AIS buckets (ie., buckets with no remote backends) is not allowed."
		return errors.New(msg + "\n(Tip:  consider 'ais object rm' or 'ais rmb', see --help for details)")
	}
	if _, err := headBucket(bck, false /* don't add */); err != nil {
		return err
	}
	objName, listObjs, tmplObjs, err := parseObjListTemplate(c, objNameOrTmpl)
	if err != nil {
		return err
	}

	switch {
	case listObjs != "" || tmplObjs != "": // 1. multi-obj
		return listrange(c, bck, listObjs, tmplObjs)
	case objName == "": // 2. entire bucket
		return evictBucket(c, bck)
	default: // 3. one(?) obj to evict
		err := api.EvictObject(apiBP, bck, objName)
		if err == nil {
			if !flagIsSet(c, nonverboseFlag) {
				fmt.Fprintf(c.App.Writer, "evicted %q from %s\n", objName, bck.Cname(""))
			}
			return nil
		}
		herr, ok := err.(*cmn.ErrHTTP)
		if !ok || herr.Status != http.StatusNotFound {
			return V(err)
		}
		// not found
		suffix := " (not \"cached\")"
		if c.NArg() > 1 {
			suffix = " (hint: missing double or single quotes?)"
		}
		return &errDoesNotExist{what: "object", name: bck.Cname(objName), suffix: suffix}
	}
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
	if _, err := headBucket(bck, false /* don't add */); err != nil {
		return err
	}
	objName, listObjs, tmplObjs, err := parseObjListTemplate(c, objNameOrTmpl)
	if err != nil {
		return err
	}

	switch {
	case listObjs != "" || tmplObjs != "": // 1. multi-obj
		return listrange(c, bck, listObjs, tmplObjs)
	case objName == "": // 2. all objects
		if flagIsSet(c, rmrfFlag) {
			if !flagIsSet(c, yesFlag) {
				warn := fmt.Sprintf("will remove all objects from %s. The operation cannot be undone!", bck)
				if ok := confirm(c, "Proceed?", warn); !ok {
					return nil
				}
			}
			return rmRfAllObjects(c, bck)
		}
		return incorrectUsageMsg(c, "use one of: (%s or %s or %s) to indicate _which_ objects to remove",
			qflprn(listFlag), qflprn(templateFlag), qflprn(rmrfFlag))
	default: // 3. one obj
		err := api.DeleteObject(apiBP, bck, objName)
		if err == nil {
			if !flagIsSet(c, nonverboseFlag) {
				fmt.Fprintf(c.App.Writer, "deleted %q from %s\n", objName, bck.Cname(""))
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
		return &errDoesNotExist{what: "object", name: bck.Cname(objName), suffix: suffix}
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
	if bck.IsAIS() {
		return fmt.Errorf("cannot prefetch from ais buckets (the operation applies to remote buckets only)")
	}
	if _, err = headBucket(bck, false /* don't add */); err != nil {
		return err
	}
	objName, listObjs, tmplObjs, err := parseObjListTemplate(c, objNameOrTmpl)
	if err != nil {
		return err
	}

	if listObjs == "" && tmplObjs == "" {
		listObjs = objName
	}
	return listrange(c, bck, listObjs, tmplObjs) // NOTE: empty tmplObjs means "all objects"
}
