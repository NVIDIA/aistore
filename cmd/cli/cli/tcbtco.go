// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
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
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
)

//
// copy -----------------------------------------------------------------------------
//

// via (I) x-copy-bucket ("full bucket") _or_ (II) x-copy-listrange ("multi-object")
// Notice a certain usable redundancy:
//
//	(I) `ais cp from to --prefix abc" is the same as (II) `ais cp from to --template abc"
//
// Also, note [CONVENTIONS] below.
func copyBucketHandler(c *cli.Context) (err error) {
	var (
		bckFrom, bckTo cmn.Bck
		objFrom        string
	)
	switch {
	case c.NArg() == 0:
		err = missingArgumentsError(c, c.Command.ArgsUsage)
	case c.NArg() == 1:
		bckFrom, objFrom, err = parseBckObjURI(c, c.Args().Get(0), true /*emptyObjnameOK*/)
	default:
		bckFrom, bckTo, objFrom, err = parseBcks(c, bucketSrcArgument, bucketDstArgument, 0 /*shift*/, true /*optionalSrcObjname*/)
	}
	if err != nil {
		return err
	}

	// [CONVENTIONS]
	// 1. '--sync' and '--latest' both require aistore to reach out for remote metadata and, therefore,
	//   if destination is omitted both options imply namesake in-cluster destination (ie., bckTo = bckFrom)
	// 2. in addition, '--sync' also implies '--all' (will traverse non-present)

	if bckTo.IsEmpty() {
		if !flagIsSet(c, syncFlag) && !flagIsSet(c, latestVerFlag) {
			var hint string
			if bckFrom.IsRemote() {
				hint = fmt.Sprintf(" (or, did you mean 'ais cp %s %s' or 'ais cp %s %s'?)",
					c.Args().Get(0), flprn(syncFlag), c.Args().Get(0), flprn(latestVerFlag))
			}
			return incorrectUsageMsg(c, "missing destination bucket%s", hint)
		}
		bckTo = bckFrom
	}

	// NOTE: copyAllObjsFlag forces 'x-list' to list the remote one, and vice versa
	return copyTransform(c, "" /*etlName*/, objFrom, bckFrom, bckTo, flagIsSet(c, copyAllObjsFlag))
}

//
// main function: (cp | etl) & (bucket | multi-object)
//

func copyTransform(c *cli.Context, etlName, objNameOrTmpl string, bckFrom, bckTo cmn.Bck, allIncludingRemote bool) (err error) {
	text1, text2 := "copy", "Copying"
	if etlName != "" {
		text1, text2 = "transform", "Transforming"
	}

	objName, listObjs, tmplObjs, err := parseObjListTemplate(c, objNameOrTmpl)
	if err != nil {
		return err
	}

	// HEAD(from)
	if _, err = headBucket(bckFrom, true /* don't add */); err != nil {
		return err
	}
	empty, err := isBucketEmpty(bckFrom, !bckFrom.IsRemote() || !allIncludingRemote /*cached*/)
	debug.AssertNoErr(err)
	if empty {
		if bckFrom.IsRemote() && !allIncludingRemote {
			hint := "(tip: use option %s to " + text1 + " remote objects from the backend store)\n"
			note := fmt.Sprintf("source %s appears to be empty "+hint, bckFrom, qflprn(copyAllObjsFlag))
			actionNote(c, note)
			return nil
		}
		note := fmt.Sprintf("source %s is empty, nothing to do\n", bckFrom)
		actionNote(c, note)
		return nil
	}

	// HEAD(to)
	if _, err = api.HeadBucket(apiBP, bckTo, true /* don't add */); err != nil {
		if herr, ok := err.(*cmn.ErrHTTP); !ok || herr.Status != http.StatusNotFound {
			return err
		}
		warn := fmt.Sprintf("destination %s doesn't exist and will be created with configuration copied from the source (%s))",
			bckTo, bckFrom)
		actionWarn(c, warn)
	}

	dryRun := flagIsSet(c, copyDryRunFlag)

	// either 1. copy/transform bucket (x-tcb)
	if objName == "" && listObjs == "" && tmplObjs == "" {
		// NOTE: e.g. 'ais cp gs://abc gs:/abc' to sync remote bucket => aistore
		if bckFrom.Equal(&bckTo) && !bckFrom.IsRemote() {
			return incorrectUsageMsg(c, errFmtSameBucket, commandCopy, bckTo)
		}
		if dryRun {
			// TODO: show object names with destinations, make the output consistent with etl dry-run
			dryRunCptn(c)
			actionDone(c, text2+" the entire bucket")
		}
		if etlName != "" {
			return etlBucket(c, etlName, bckFrom, bckTo, allIncludingRemote)
		}
		return copyBucket(c, bckFrom, bckTo, allIncludingRemote)
	}

	// or 2. multi-object x-tco
	if listObjs == "" && tmplObjs == "" {
		listObjs = objName // NOTE: "pure" prefix comment in parseObjListTemplate (above)
	}
	if dryRun {
		var prompt string
		if listObjs != "" {
			prompt = fmt.Sprintf("%s %q ...\n", text2, listObjs)
		} else {
			prompt = fmt.Sprintf("%s objects that match the pattern %q ...\n", text2, tmplObjs)
		}
		dryRunCptn(c) // TODO: ditto
		actionDone(c, prompt)
	}
	return runTCO(c, bckFrom, bckTo, listObjs, tmplObjs, etlName)
}

func _iniCopyBckMsg(c *cli.Context, msg *apc.CopyBckMsg) (err error) {
	{
		msg.Prepend = parseStrFlag(c, copyPrependFlag)
		msg.Prefix = parseStrFlag(c, verbObjPrefixFlag)
		msg.DryRun = flagIsSet(c, copyDryRunFlag)
		msg.Force = flagIsSet(c, forceFlag)
		msg.LatestVer = flagIsSet(c, latestVerFlag)
		msg.Sync = flagIsSet(c, syncFlag)
	}
	if msg.Sync && msg.Prepend != "" {
		err = fmt.Errorf("prepend option (%q) is incompatible with %s (the latter requires identical source/destination naming)",
			msg.Prepend, qflprn(progressFlag))
	}
	return err
}

func copyBucket(c *cli.Context, bckFrom, bckTo cmn.Bck, allIncludingRemote bool) error {
	var (
		msg          apc.CopyBckMsg
		showProgress = flagIsSet(c, progressFlag)
		from, to     = bckFrom.Cname(""), bckTo.Cname("")
	)
	if showProgress && flagIsSet(c, copyDryRunFlag) {
		warn := fmt.Sprintf("dry-run option is incompatible with %s - not implemented yet", qflprn(progressFlag))
		actionWarn(c, warn)
		showProgress = false
	}
	// copy: with/wo progress/wait
	if err := _iniCopyBckMsg(c, &msg); err != nil {
		return err
	}

	// by default, copying objects in the cluster, with an option to override
	// TODO: FltExistsOutside maybe later
	fltPresence := apc.FltPresent
	if allIncludingRemote {
		fltPresence = apc.FltExists
	}

	if showProgress {
		var cpr cprCtx
		_, cpr.xname = xact.GetKindName(apc.ActCopyBck)
		cpr.from, cpr.to = bckFrom.Cname(""), bckTo.Cname("")
		return cpr.copyBucket(c, bckFrom, bckTo, &msg, fltPresence)
	}

	xid, err := api.CopyBucket(apiBP, bckFrom, bckTo, &msg, fltPresence)
	if err != nil {
		return V(err)
	}
	// NOTE: may've transitioned TCB => TCO
	kind := apc.ActCopyBck
	if !apc.IsFltPresent(fltPresence) {
		kind, _, err = getKindNameForID(xid, kind)
		if err != nil {
			return err
		}
	}

	if !flagIsSet(c, waitFlag) && !flagIsSet(c, waitJobXactFinishedFlag) {
		/// TODO: unify vs e2e: ("%s[%s] %s => %s", kind, xid, from, to)
		if flagIsSet(c, nonverboseFlag) {
			fmt.Fprintln(c.App.Writer, xid)
		} else {
			actionDone(c, tcbtcoCptn("Copying", bckFrom, bckTo)+". "+toMonitorMsg(c, xid, ""))
		}
		return nil
	}

	// wait
	var timeout time.Duration
	if flagIsSet(c, waitJobXactFinishedFlag) {
		timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}
	fmt.Fprintf(c.App.Writer, tcbtcoCptn("Copying", bckFrom, bckTo)+" ...")
	xargs := xact.ArgsMsg{ID: xid, Kind: kind, Timeout: timeout}
	if err := waitXact(&xargs); err != nil {
		fmt.Fprintf(c.App.ErrWriter, fmtXactFailed, "copy", from, to)
		return err
	}
	actionDone(c, fmtXactSucceeded)
	return nil
}

func tcbtcoCptn(action string, bckFrom, bckTo cmn.Bck) string {
	from, to := bckFrom.Cname(""), bckTo.Cname("")
	if bckFrom.Equal(&bckTo) {
		return fmt.Sprintf("%s %s", action, from)
	}
	return fmt.Sprintf("%s %s => %s", action, from, to)
}

//
// etl -------------------------------------------------------------------------------
//

func etlBucketHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	etlName := c.Args().Get(0)
	bckFrom, bckTo, objFrom, err := parseBcks(c, bucketSrcArgument, bucketDstArgument, 1 /*shift*/, true /*optionalSrcObjname*/)
	if err != nil {
		return err
	}
	return copyTransform(c, etlName, objFrom, bckFrom, bckTo, flagIsSet(c, etlAllObjsFlag))
}

func etlBucket(c *cli.Context, etlName string, bckFrom, bckTo cmn.Bck, allIncludingRemote bool) error {
	var msg = apc.TCBMsg{
		Transform: apc.Transform{Name: etlName},
	}
	if err := _iniCopyBckMsg(c, &msg.CopyBckMsg); err != nil {
		return err
	}
	if flagIsSet(c, etlExtFlag) {
		mapStr := parseStrFlag(c, etlExtFlag)
		extMap := make(cos.StrKVs, 1)
		err := jsoniter.UnmarshalFromString(mapStr, &extMap)
		if err != nil {
			// add quotation marks and reparse
			tmp := strings.ReplaceAll(mapStr, " ", "")
			tmp = strings.ReplaceAll(tmp, "{", "{\"")
			tmp = strings.ReplaceAll(tmp, "}", "\"}")
			tmp = strings.ReplaceAll(tmp, ":", "\":\"")
			tmp = strings.ReplaceAll(tmp, ",", "\",\"")
			if jsoniter.UnmarshalFromString(tmp, &extMap) == nil {
				err = nil
			}
		}
		if err != nil {
			return fmt.Errorf("Invalid format --%s=%q. Usage examples: {jpg:txt}, \"{in1:out1,in2:out2}\"",
				etlExtFlag.GetName(), mapStr)
		}
		msg.Ext = extMap
	}

	// by default, copying objects in the cluster, with an option to override
	// TODO: FltExistsOutside maybe later
	fltPresence := apc.FltPresent
	if allIncludingRemote {
		fltPresence = apc.FltExists
	}

	xid, err := api.ETLBucket(apiBP, bckFrom, bckTo, &msg, fltPresence)
	if errV := handleETLHTTPError(err, etlName); errV != nil {
		return errV
	}

	_, xname := xact.GetKindName(apc.ActETLBck)
	text := fmt.Sprintf("%s %s => %s", xact.Cname(xname, xid), bckFrom, bckTo)
	if !flagIsSet(c, waitFlag) && !flagIsSet(c, waitJobXactFinishedFlag) {
		fmt.Fprintln(c.App.Writer, text)
		return nil
	}

	// wait
	var timeout time.Duration
	if flagIsSet(c, waitJobXactFinishedFlag) {
		timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}
	fmt.Fprintln(c.App.Writer, text+" ...")
	xargs := xact.ArgsMsg{ID: xid, Kind: apc.ActETLBck, Timeout: timeout}
	if err := waitXact(&xargs); err != nil {
		return err
	}
	if !flagIsSet(c, copyDryRunFlag) {
		return nil
	}

	// [DRY-RUN]
	snaps, err := api.QueryXactionSnaps(apiBP, &xargs)
	if err != nil {
		return V(err)
	}
	dryRunCptn(c)
	locObjs, outObjs, inObjs := snaps.ObjCounts(xid)
	fmt.Fprintf(c.App.Writer, "ETL object counts:\t transformed=%d, sent=%d, received=%d", locObjs, outObjs, inObjs)
	locBytes, outBytes, inBytes := snaps.ByteCounts(xid)
	fmt.Fprintf(c.App.Writer, "ETL byte stats:\t transformed=%d, sent=%d, received=%d", locBytes, outBytes, inBytes)
	return nil
}

func handleETLHTTPError(err error, etlName string) error {
	if err == nil {
		return nil
	}
	if herr, ok := err.(*cmn.ErrHTTP); ok {
		// TODO: How to find out if it's transformation not found, and not object not found?
		if herr.Status == http.StatusNotFound && strings.Contains(herr.Error(), etlName) {
			return fmt.Errorf("ETL[%s] not found; try starting new ETL with:\nais %s %s <spec>",
				etlName, commandETL, cmdInit)
		}
	}
	return V(err)
}
