// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
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
// copy
//

// via (I) x-copy-bucket ("full bucket") _or_ (II) x-copy-listrange ("multi-object")
// Notice a certain usable redundancy:
// (I)  `ais cp from to --prefix abc"
// is the same as:
// (II) `ais cp from to --template abc"
func copyBucketHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	bckFrom, bckTo, err := parseBcks(c, bucketSrcArgument, bucketDstArgument, 0 /*shift*/)
	if err != nil {
		return err
	}
	return tcbtco(c, "", bckFrom, bckTo)
}

func copyBucket(c *cli.Context, bckFrom, bckTo cmn.Bck) error {
	var (
		showProgress = flagIsSet(c, progressFlag)
		from, to     = bckFrom.Cname(""), bckTo.Cname("")
	)
	if showProgress && flagIsSet(c, copyDryRunFlag) {
		warn := fmt.Sprintf("dry-run option is incompatible with %s - not implemented yet", qflprn(progressFlag))
		actionWarn(c, warn)
		showProgress = false
	}
	// copy: with/wo progress/wait
	msg := &apc.CopyBckMsg{
		Prepend: parseStrFlag(c, copyPrependFlag),
		Prefix:  parseStrFlag(c, copyObjPrefixFlag),
		DryRun:  flagIsSet(c, copyDryRunFlag),
		Force:   flagIsSet(c, forceFlag),
	}

	// by default, copying objects in the cluster, with an option to override
	// TODO: FltExistsOutside maybe later
	fltPresence := apc.FltPresent
	if flagIsSet(c, copyAllObjsFlag) {
		fltPresence = apc.FltExists
	}

	if showProgress {
		var cpr cprCtx
		_, cpr.xname = xact.GetKindName(apc.ActCopyBck)
		cpr.from, cpr.to = bckFrom.Cname(""), bckTo.Cname("")
		return cpr.copyBucket(c, bckFrom, bckTo, msg, fltPresence)
	}

	xid, err := api.CopyBucket(apiBP, bckFrom, bckTo, msg, fltPresence)
	if err != nil {
		return err
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
		baseMsg := fmt.Sprintf("Copying %s => %s. ", from, to)
		actionDone(c, baseMsg+toMonitorMsg(c, xid, ""))
		return nil
	}

	// wait
	var timeout time.Duration
	if flagIsSet(c, waitJobXactFinishedFlag) {
		timeout = parseDurationFlag(c, waitJobXactFinishedFlag)
	}
	fmt.Fprintf(c.App.Writer, fmtXactWaitStarted, "Copying", from, to)
	xargs := xact.ArgsMsg{ID: xid, Kind: kind, Timeout: timeout}
	if err := waitXact(apiBP, xargs); err != nil {
		fmt.Fprintf(c.App.ErrWriter, fmtXactFailed, "copy", from, to)
		return err
	}
	actionDone(c, fmtXactSucceeded)
	return nil
}

//
// etl
//

func etlBucketHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	etlName := c.Args().Get(0)
	bckFrom, bckTo, err := parseBcks(c, bucketSrcArgument, bucketDstArgument, 1 /*shift*/)
	if err != nil {
		return err
	}
	return tcbtco(c, etlName, bckFrom, bckTo)
}

func etlBucket(c *cli.Context, etlName string, bckFrom, bckTo cmn.Bck) error {
	debug.Assert(!flagIsSet(c, listFlag) && !flagIsSet(c, templateFlag))
	debug.Assert(etlName != "")
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	msg := &apc.TCBMsg{
		Transform: apc.Transform{Name: etlName},
		CopyBckMsg: apc.CopyBckMsg{
			Prepend: parseStrFlag(c, copyPrependFlag),
			Prefix:  parseStrFlag(c, copyObjPrefixFlag),
			DryRun:  flagIsSet(c, copyDryRunFlag),
			Force:   flagIsSet(c, forceFlag),
		},
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
	if flagIsSet(c, copyAllObjsFlag) {
		fltPresence = apc.FltExists
	}

	xid, err := api.ETLBucket(apiBP, bckFrom, bckTo, msg, fltPresence)
	if errV := handleETLHTTPError(err, etlName); errV != nil {
		return errV
	}

	_, xname := xact.GetKindName(apc.ActETLBck)
	text := fmt.Sprintf("%s[%s] %s => %s", xname, xid, bckFrom, bckTo)
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
	if err := waitXact(apiBP, xargs); err != nil {
		return err
	}
	if !flagIsSet(c, copyDryRunFlag) {
		return nil
	}

	// [DRY-RUN]
	snaps, err := api.QueryXactionSnaps(apiBP, xargs)
	if err != nil {
		return err
	}
	fmt.Fprintln(c.App.Writer, dryRunHeader+" "+dryRunExplanation)
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
	return err
}

//
// common for both (cp | etl)
//

func tcbtco(c *cli.Context, etlName string, bckFrom, bckTo cmn.Bck) (err error) {
	if flagIsSet(c, listFlag) && flagIsSet(c, templateFlag) {
		return incorrectUsageMsg(c, errFmtExclusive, qflprn(listFlag), qflprn(templateFlag))
	}
	if _, err = headBucket(bckFrom, true /* don't add */); err != nil {
		return err
	}
	empty, err := isBucketEmpty(bckFrom)
	debug.AssertNoErr(err)
	if empty {
		if bckFrom.IsAIS() {
			note := fmt.Sprintf("source %s is empty, nothing to do\n", bckFrom)
			actionNote(c, note)
			return nil
		}
		if bckFrom.IsRemote() && !flagIsSet(c, copyAllObjsFlag) {
			hint := "(hint: use option %s to copy remote objects from the backend store)\n"
			note := fmt.Sprintf("source %s appears to be empty "+hint, bckFrom, qflprn(copyAllObjsFlag))
			actionNote(c, note)
			return nil
		}
	}
	if _, err = api.HeadBucket(apiBP, bckTo, true /* don't add */); err != nil {
		if herr, ok := err.(*cmn.ErrHTTP); !ok || herr.Status != http.StatusNotFound {
			return err
		}
		warn := fmt.Sprintf("destination %s doesn't exist and will be created with configuration copied from the source (%s))",
			bckFrom, bckTo)
		actionWarn(c, warn)
	}

	dryRun := flagIsSet(c, copyDryRunFlag)

	// (I) TCB
	if !flagIsSet(c, listFlag) && !flagIsSet(c, templateFlag) {
		if bckFrom.Equal(&bckTo) {
			return incorrectUsageMsg(c, errFmtSameBucket, commandCopy, bckTo)
		}
		if dryRun {
			// TODO: show object names with destinations, make the output consistent with etl dry-run
			fmt.Fprintln(c.App.Writer, dryRunHeader+" "+dryRunExplanation)
			actionDone(c, "[dry-run] Copying the entire bucket")
		}
		if etlName != "" {
			return etlBucket(c, etlName, bckFrom, bckTo)
		}
		return copyBucket(c, bckFrom, bckTo)
	}

	// (II) multi-object TCO
	listObjs := parseStrFlag(c, listFlag)
	tmplObjs := parseStrFlag(c, templateFlag)

	if dryRun {
		var msg string
		if listObjs != "" {
			msg = fmt.Sprintf("[dry-run] Copying %q ...\n", listObjs)
		} else {
			msg = fmt.Sprintf("[dry-run] Copying objects that match the pattern %q ...\n", tmplObjs)
		}
		fmt.Fprintln(c.App.Writer, dryRunHeader+" "+dryRunExplanation) // ditto
		actionDone(c, msg)
	}
	return multiobjTCO(c, bckFrom, bckTo, listObjs, tmplObjs, etlName)
}
