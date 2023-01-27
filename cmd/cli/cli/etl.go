// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that control running jobs in the cluster.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/tmpls"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/xact"
	"github.com/fatih/color"
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
)

var (
	// flags
	etlSubFlags = map[string][]cli.Flag{
		subcmdCode: {
			fromFileFlag,
			depsFileFlag,
			runtimeFlag,
			commTypeFlag,
			funcTransformFlag,
			chunkSizeFlag,
			waitTimeoutFlag,
			etlNameFlag,
		},
		subcmdSpec: {
			fromFileFlag,
			commTypeFlag,
			etlNameFlag,
			waitTimeoutFlag,
		},
		subcmdStop: {
			allRunningJobsFlag,
		},
		subcmdBucket: {
			etlExtFlag,
			cpBckPrefixFlag,
			cpBckDryRunFlag,
			waitFlag,
			etlBucketRequestTimeout,
			templateFlag,
			listFlag,
			continueOnErrorFlag,
		},
		subcmdStart: {},
	}
	showCmdETL = cli.Command{
		Name:   commandShow,
		Usage:  "show ETL(s)",
		Action: etlListHandler,
		Subcommands: []cli.Command{
			{
				Name:      commandSource,
				Usage:     "show ETL code/spec",
				ArgsUsage: etlNameArgument,
				Action:    etlShowInitMsgHandler,
			},
		},
	}
	stopCmdETL = cli.Command{
		Name:         subcmdStop,
		Usage:        "stop ETL",
		ArgsUsage:    etlNameListArgument,
		Action:       etlStopHandler,
		BashComplete: etlIDCompletions,
		Flags:        etlSubFlags[subcmdStop],
	}
	startCmdETL = cli.Command{
		Name:         subcmdStart,
		Usage:        "start ETL",
		ArgsUsage:    etlNameArgument,
		Action:       etlStartHandler,
		BashComplete: etlIDCompletions,
		Flags:        etlSubFlags[subcmdStart],
	}
	initCmdETL = cli.Command{
		Name: subcmdInit,
		Subcommands: []cli.Command{
			{
				Name:   subcmdSpec,
				Usage:  "start ETL job with YAML Pod specification",
				Flags:  etlSubFlags[subcmdSpec],
				Action: etlInitSpecHandler,
			},
			{
				Name:   subcmdCode,
				Usage:  "start ETL job using the specified transforming function or script",
				Flags:  etlSubFlags[subcmdCode],
				Action: etlInitCodeHandler,
			},
		},
	}
	objCmdETL = cli.Command{
		Name:         subcmdObject,
		Usage:        "transform object",
		ArgsUsage:    etlNameArgument + " " + objectArgument + " OUTPUT",
		Action:       etlObjectHandler,
		BashComplete: etlIDCompletions,
	}
	bckCmdETL = cli.Command{
		Name:         subcmdBucket,
		Usage:        "perform bucket-to-bucket transform (\"offline transformation\")",
		ArgsUsage:    etlNameArgument + " " + bucketSrcArgument + " " + bucketDstArgument,
		Action:       etlBucketHandler,
		Flags:        etlSubFlags[subcmdBucket],
		BashComplete: manyBucketsCompletions([]cli.BashCompleteFunc{etlIDCompletions}, 1, 2),
	}
	logsCmdETL = cli.Command{
		Name:         subcmdLogs,
		Usage:        "retrieve ETL logs",
		ArgsUsage:    etlNameArgument + " " + optionalTargetIDArgument,
		Action:       etlLogsHandler,
		BashComplete: etlIDCompletions,
	}
	// subcommands
	etlCmd = cli.Command{
		Name:  commandETL,
		Usage: "execute custom transformations on objects",
		Subcommands: []cli.Command{
			initCmdETL,
			showCmdETL,
			logsCmdETL,
			startCmdETL,
			stopCmdETL,
			objCmdETL,
			bckCmdETL,
		},
	}
)

func etlIDCompletions(c *cli.Context) {
	suggestEtlName(c, 0)
}

func suggestEtlName(c *cli.Context, shift int) {
	if c.NArg() > shift {
		return
	}
	list, err := api.ETLList(apiBP)
	if err != nil {
		return
	}
	for _, l := range list {
		fmt.Print(l.Name)
	}
}

func etlAlreadyExists(etlName string) (err error) {
	if l := findETL(etlName, ""); l != nil {
		return fmt.Errorf("ETL[%s] already exists", etlName)
	}
	return
}

// either by name or xaction ID
func findETL(etlName, xid string) *etl.Info {
	list, err := api.ETLList(apiBP)
	if err != nil {
		return nil
	}
	for _, l := range list {
		if etlName != "" && l.Name == etlName {
			return &l
		}
		if xid != "" && l.XactID == xid {
			return &l
		}
	}
	return nil
}

func etlInitSpecHandler(c *cli.Context) (err error) {
	fromFile := parseStrFlag(c, fromFileFlag)
	if fromFile == "" {
		return fmt.Errorf("flag %s must be specified", qflprn(fromFileFlag))
	}
	spec, err := os.ReadFile(fromFile)
	if err != nil {
		return err
	}

	msg := &etl.InitSpecMsg{}
	{
		msg.IDX = parseStrFlag(c, etlNameFlag)
		msg.CommTypeX = parseStrFlag(c, commTypeFlag)
		msg.Spec = spec
	}
	if err = msg.Validate(); err != nil {
		return err
	}

	// msg.ID is `metadata.name` from podSpec
	if err = etlAlreadyExists(msg.Name()); err != nil {
		return
	}

	xid, err := api.ETLInit(apiBP, msg)
	if err != nil {
		return err
	}
	fmt.Fprintf(c.App.Writer, "ETL[%s]: job %q\n", msg.Name(), xid)
	return nil
}

func etlInitCodeHandler(c *cli.Context) (err error) {
	msg := &etl.InitCodeMsg{}

	fromFile := parseStrFlag(c, fromFileFlag)
	if fromFile == "" {
		return fmt.Errorf("flag %s cannot be empty", qflprn(fromFileFlag))
	}

	msg.IDX = parseStrFlag(c, etlNameFlag)
	if msg.Name() != "" {
		if err = k8s.ValidateEtlName(msg.Name()); err != nil {
			return
		}
		if err = etlAlreadyExists(msg.Name()); err != nil {
			return
		}
	}

	if msg.Code, err = os.ReadFile(fromFile); err != nil {
		return fmt.Errorf("failed to read %q: %v", fromFile, err)
	}

	depsFile := parseStrFlag(c, depsFileFlag)
	if depsFile != "" {
		if msg.Deps, err = os.ReadFile(depsFile); err != nil {
			return fmt.Errorf("failed to read %q: %v", depsFile, err)
		}
	}

	msg.Runtime = parseStrFlag(c, runtimeFlag)
	msg.CommTypeX = parseStrFlag(c, commTypeFlag)

	if flagIsSet(c, chunkSizeFlag) {
		msg.ChunkSize, err = parseByteFlagToInt(c, chunkSizeFlag)
		if err != nil {
			return err
		}
	}

	if msg.CommTypeX != "" {
		// Missing `/` at the end, eg. `hpush:/` (should be `hpush://`)
		if strings.HasSuffix(msg.CommTypeX, ":/") {
			msg.CommTypeX += "/"
		}
		// Missing `://` at the end, eg. `hpush` (should be `hpush://`)
		if !strings.HasSuffix(msg.CommTypeX, "://") {
			msg.CommTypeX += "://"
		}
	}
	msg.Timeout = cos.Duration(parseDurationFlag(c, waitTimeoutFlag))

	// funcs
	msg.Funcs.Transform = parseStrFlag(c, funcTransformFlag)

	// validate
	if err := msg.Validate(); err != nil {
		return err
	}

	// start
	xid, err := api.ETLInit(apiBP, msg)
	if err != nil {
		return err
	}
	fmt.Fprintf(c.App.Writer, "ETL[%s]: job %q\n", msg.Name(), xid)
	return nil
}

func etlListHandler(c *cli.Context) (err error) {
	_, err = etlList(c, false)
	return
}

func showETLs(c *cli.Context, etlName string, caption bool) (int, error) {
	if etlName == "" {
		return etlList(c, caption)
	}

	return 1, etlPrintInitMsg(c, etlName) // TODO: extend to show Status and runtime stats
}

func etlList(c *cli.Context, caption bool) (int, error) {
	list, err := api.ETLList(apiBP)
	l := len(list)
	if err != nil || l == 0 {
		return l, err
	}
	if caption {
		onlyActive := !flagIsSet(c, allJobsFlag)
		jobCptn(c, commandETL, onlyActive, "", false)
	}

	hideHeader := flagIsSet(c, noHeaderFlag)
	if hideHeader {
		return l, tmpls.Print(list, c.App.Writer, tmpls.TransformListNoHdrTmpl, nil, false)
	}

	return l, tmpls.Print(list, c.App.Writer, tmpls.TransformListTmpl, nil, false)
}

func etlShowInitMsgHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	id := c.Args().Get(0)
	return etlPrintInitMsg(c, id)
}

func etlPrintInitMsg(c *cli.Context, id string) error {
	msg, err := api.ETLGetInitMsg(apiBP, id)
	if err != nil {
		return err
	}
	if initMsg, ok := msg.(*etl.InitCodeMsg); ok {
		fmt.Fprintln(c.App.Writer, string(initMsg.Code))
		return nil
	}
	if initMsg, ok := msg.(*etl.InitSpecMsg); ok {
		fmt.Fprintln(c.App.Writer, string(initMsg.Spec))
		return nil
	}
	err = fmt.Errorf("invalid response [%+v, %T]", msg, msg)
	debug.AssertNoErr(err)
	return err
}

func etlLogsHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}

	var (
		id       = c.Args().Get(0)
		targetID = c.Args().Get(1) // optional
	)

	logs, err := api.ETLLogs(apiBP, id, targetID)
	if err != nil {
		return err
	}

	if targetID != "" {
		fmt.Fprintln(c.App.Writer, string(logs[0].Logs))
		return nil
	}

	for idx, log := range logs {
		if idx > 0 {
			fmt.Fprintln(c.App.Writer)
		}
		fmt.Fprintf(c.App.Writer, "%s:\n%s\n", log.TargetID, string(log.Logs))
	}

	return nil
}

func etlStopHandler(c *cli.Context) error {
	return stopETLs(c, "")
}

func stopETLs(c *cli.Context, name string) (err error) {
	var etlNames []string
	switch {
	case name != "":
		etlNames = append(etlNames, name)
	case flagIsSet(c, allRunningJobsFlag):
		if c.NArg() > 0 {
			etlNames = c.Args()[0:]
			return incorrectUsageMsg(c, "flag %s cannot be used together with %s %v",
				qflprn(allRunningJobsFlag), etlNameArgument, etlNames)
		}
		res, err := api.ETLList(apiBP)
		if err != nil {
			return err
		}
		for _, etlInfo := range res {
			etlNames = append(etlNames, etlInfo.Name)
		}
	default:
		if c.NArg() == 0 {
			return missingArgumentsError(c, c.Command.ArgsUsage)
		}
		etlNames = c.Args()[0:]
	}
	for _, name := range etlNames {
		msg := fmt.Sprintf("ETL[%s]", name)
		if err := api.ETLStop(apiBP, name); err != nil {
			if herr, ok := err.(*cmn.ErrHTTP); ok && herr.Status == http.StatusNotFound {
				actionWarn(c, msg+" not found, nothing to do")
				continue
			}
			return err
		}
		actionDone(c, msg+" stopped")
	}
	return nil
}

func etlStartHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	etlName := c.Args()[0]
	if err := api.ETLStart(apiBP, etlName); err != nil {
		if herr, ok := err.(*cmn.ErrHTTP); ok && herr.Status == http.StatusNotFound {
			color.New(color.FgYellow).Fprintf(c.App.Writer, "ETL[%s] not found", etlName)
		}
		return err
	}
	fmt.Fprintf(c.App.Writer, "ETL[%s] started successfully\n", etlName)
	return nil
}

func etlObjectHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	} else if c.NArg() == 1 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	} else if c.NArg() == 2 {
		return missingArgumentsError(c, "OUTPUT")
	}

	var (
		etlName    = c.Args().Get(0)
		uri        = c.Args().Get(1)
		outputDest = c.Args().Get(2)
	)
	bck, objName, errV := parseBckObjectURI(c, uri)
	if errV != nil {
		return errV
	}

	var w io.Writer
	if outputDest == "-" {
		w = os.Stdout
	} else {
		f, err := os.Create(outputDest)
		if err != nil {
			return err
		}
		w = f
		defer f.Close()
	}

	err := api.ETLObject(apiBP, etlName, bck, objName, w)
	return handleETLHTTPError(err, etlName)
}

func etlBucketHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	etlName := c.Args().Get(0)
	fromBck, toBck, err := parseBcks(c, bucketSrcArgument, bucketDstArgument, 1 /*shift*/)
	if err != nil {
		return err
	}
	if fromBck.Equal(&toBck) {
		return fmt.Errorf("cannot transform bucket %q onto itself", fromBck)
	}

	msg := &apc.TCBMsg{
		Transform: apc.Transform{Name: etlName},
		CopyBckMsg: apc.CopyBckMsg{
			Prefix: parseStrFlag(c, cpBckPrefixFlag),
			DryRun: flagIsSet(c, cpBckDryRunFlag),
		},
	}

	if flagIsSet(c, etlExtFlag) {
		mapStr := parseStrFlag(c, etlExtFlag)
		extMap := make(cos.StrKVs, 1)
		if err = jsoniter.UnmarshalFromString(mapStr, &extMap); err != nil {
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

	tmplObjs := parseStrFlag(c, templateFlag)
	listObjs := parseStrFlag(c, listFlag)
	if listObjs != "" || tmplObjs != "" {
		return multiObjBckCopy(c, fromBck, toBck, listObjs, tmplObjs, etlName)
	}

	xid, err := api.ETLBucket(apiBP, fromBck, toBck, msg)
	if errV := handleETLHTTPError(err, etlName); errV != nil {
		return errV
	}

	if !flagIsSet(c, waitFlag) {
		fmt.Fprintln(c.App.Writer, xid)
		return nil
	}

	if _, err := api.WaitForXactionIC(apiBP, xact.ArgsMsg{ID: xid}); err != nil {
		return err
	}
	if !flagIsSet(c, cpBckDryRunFlag) {
		return nil
	}

	snaps, err := api.QueryXactionSnaps(apiBP, xact.ArgsMsg{ID: xid})
	if err != nil {
		return err
	}

	fmt.Fprintln(c.App.Writer, dryRunHeader+" "+dryRunExplanation)

	locObjs, outObjs, inObjs := snaps.ObjCounts(xid)
	fmt.Fprintf(c.App.Writer, "ETL object stats: locally transformed=%d, sent=%d, received=%d", locObjs, outObjs, inObjs)
	locBytes, outBytes, inBytes := snaps.ByteCounts(xid)
	fmt.Fprintf(c.App.Writer, "ETL byte stats: locally transformed=%d, sent=%d, received=%d", locBytes, outBytes, inBytes)
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
				etlName, commandETL, subcmdInit)
		}
	}
	return err
}
