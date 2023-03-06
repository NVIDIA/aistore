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

// x-TCO: multi-object transform or copy
func multiobjTCO(c *cli.Context, fromBck, toBck cmn.Bck, listObjs, tmplObjs, etlName string) error {
	var (
		lrMsg   cmn.SelectObjsMsg
		numObjs int64
	)
	debug.Assert((listObjs == "" && tmplObjs != "") || (listObjs != "" && tmplObjs == ""))

	// 1. list or template
	if listObjs != "" {
		lrMsg.ObjNames = splitCsv(listObjs)
		numObjs = int64(len(lrMsg.ObjNames))
	} else {
		pt, err := cos.NewParsedTemplate(tmplObjs)
		if err != nil {
			return err
		}
		numObjs = pt.Count()
		lrMsg.Template = tmplObjs
	}

	// 2. TCO message
	var msg = cmn.TCObjsMsg{SelectObjsMsg: lrMsg, ToBck: toBck}
	msg.DryRun = flagIsSet(c, copyDryRunFlag)
	if flagIsSet(c, etlBucketRequestTimeout) {
		msg.Timeout = cos.Duration(etlBucketRequestTimeout.Value)
	}
	msg.ContinueOnError = flagIsSet(c, continueOnErrorFlag)

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
		xid, err = api.ETLMultiObj(apiBP, fromBck, msg)
	} else {
		xkind = apc.ActCopyObjects
		xid, err = api.CopyMultiObj(apiBP, fromBck, msg)
	}
	if err != nil {
		return err
	}

	// 4. progress bar, if requested
	var showProgress = flagIsSet(c, progressFlag)
	if showProgress {
		var cpr = cprCtx{
			xid:  xid,
			from: fromBck.DisplayName(),
			to:   toBck.DisplayName(),
		}
		_, cpr.xname = xact.GetKindName(xkind)
		cpr.totals.objs = numObjs
		cpr.loghdr = fmt.Sprintf("%s[%s] %s => %s", cpr.xname, cpr.xid, cpr.from, cpr.to)
		return cpr.multiobj(c, text)
	}

	// done
	if !flagIsSet(c, waitFlag) && !flagIsSet(c, waitJobXactFinishedFlag) {
		baseMsg := fmt.Sprintf("%s %s => %s. ", text, fromBck, toBck)
		actionDone(c, baseMsg+toMonitorMsg(c, xid, ""))
		return nil
	}

	// or wait
	var timeout time.Duration

	fmt.Fprintf(c.App.Writer, fmtXactWaitStarted, text, fromBck, toBck)

	if flagIsSet(c, waitJobXactFinishedFlag) {
		timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}
	xargs := xact.ArgsMsg{ID: xid, Kind: xkind, Timeout: timeout}
	if err = waitXact(apiBP, xargs); err != nil {
		fmt.Fprintf(c.App.Writer, fmtXactFailed, text, fromBck, toBck)
	} else {
		fmt.Fprint(c.App.Writer, fmtXactSucceeded)
	}
	return err
}

func listrange(c *cli.Context, bck cmn.Bck) (err error) {
	var (
		xid, xname string
		text       string
		num        int64
	)
	if flagIsSet(c, listFlag) && flagIsSet(c, templateFlag) {
		return incorrectUsageMsg(c, errFmtExclusive, qflprn(listFlag), qflprn(templateFlag))
	}
	debug.Assert(flagIsSet(c, listFlag) || flagIsSet(c, templateFlag))
	if flagIsSet(c, listFlag) {
		xid, xname, text, num, err = _listOp(c, bck)
	} else {
		xid, xname, text, num, err = _rangeOp(c, bck)
	}
	if err != nil {
		return
	}

	// progress bar
	var showProgress = flagIsSet(c, progressFlag)
	if showProgress {
		var cpr = cprCtx{
			xname:  xname,
			xid:    xid,
			from:   bck.DisplayName(),
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
	if err := waitXact(apiBP, xargs); err != nil {
		return err
	}
	fmt.Fprint(c.App.Writer, fmtXactSucceeded)
	return
}

// `--list` flag
func _listOp(c *cli.Context, bck cmn.Bck) (xid, xname, text string, num int64, err error) {
	var (
		kind     string
		arg      = parseStrFlag(c, listFlag)
		fileList = splitCsv(arg)
	)
	if flagIsSet(c, dryRunFlag) {
		limitedLineWriter(c.App.Writer, dryRunExamplesCnt, strings.ToUpper(c.Command.Name)+" "+bck.DisplayName()+"/%s\n", fileList)
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
	text = fmt.Sprintf("%s[%s]: %s %s from %s", xname, xid, s, action, bck.DisplayName())
	return
}

// `--range` flag
func _rangeOp(c *cli.Context, bck cmn.Bck) (xid, xname, text string, num int64, err error) {
	var (
		kind     string
		rangeStr = parseStrFlag(c, templateFlag)
		pt       cos.ParsedTemplate
	)
	pt, err = cos.ParseBashTemplate(rangeStr)
	if err != nil {
		fmt.Fprintf(c.App.Writer, "invalid template %q: %v", rangeStr, err)
		return
	}
	// [DRY-RUN]
	if flagIsSet(c, dryRunFlag) {
		objs := pt.ToSlice(dryRunExamplesCnt)
		limitedLineWriter(c.App.Writer, dryRunExamplesCnt, strings.ToUpper(c.Command.Name)+" "+bck.DisplayName()+"/%s", objs)
		if pt.Count() > dryRunExamplesCnt {
			fmt.Fprintf(c.App.Writer, "(and %d more)", pt.Count()-dryRunExamplesCnt)
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
	text = fmt.Sprintf("%s[%s]: %s %q from %s", xname, xid, action, rangeStr, bck.DisplayName())
	return
}

// Multiple objects in the command line - multiobj _argument_ handler
func multiobjArg(c *cli.Context, command string) error {
	// stops iterating if encounters error
	for _, uri := range c.Args() {
		bck, objName, err := parseBckObjectURI(c, uri)
		if err != nil {
			return err
		}
		if _, err = headBucket(bck, false /* don't add */); err != nil {
			return err
		}

		switch command {
		case commandRemove:
			if err := api.DeleteObject(apiBP, bck, objName); err != nil {
				return err
			}
			fmt.Fprintf(c.App.Writer, "deleted %q from %s\n", objName, bck.DisplayName())
		case commandEvict:
			if !bck.IsRemote() {
				const msg = "evicting objects from AIS buckets (ie., buckets with no remote backends) is not allowed."
				return errors.New(msg + "\n(Hint: use 'ais object rm' command to delete)")
			}
			if flagIsSet(c, dryRunFlag) {
				fmt.Fprintf(c.App.Writer, "EVICT: %s/%s\n", bck.DisplayName(), objName)
				continue
			}
			if err := api.EvictObject(apiBP, bck, objName); err != nil {
				if herr, ok := err.(*cmn.ErrHTTP); ok && herr.Status == http.StatusNotFound {
					err = fmt.Errorf("object %s/%s does not exist (ie., not present or \"cached\")",
						bck.DisplayName(), objName)
				}
				return err
			}
			fmt.Fprintf(c.App.Writer, "evicted %q from %s\n", objName, bck.DisplayName())
		}
	}
	return nil
}
