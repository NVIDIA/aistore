// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that control running jobs in the cluster.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/fatih/color"
	"github.com/urfave/cli"
)

var (
	// flags
	etlSubFlags = map[string][]cli.Flag{
		cmdCode: {
			fromFileFlag,
			depsFileFlag,
			runtimeFlag,
			commTypeFlag,
			funcTransformFlag,
			argTypeFlag,
			chunkSizeFlag,
			waitPodReadyTimeoutFlag,
			etlNameFlag,
		},
		cmdSpec: {
			fromFileFlag,
			commTypeFlag,
			argTypeFlag,
			waitPodReadyTimeoutFlag,
			etlNameFlag,
		},
		cmdStop: {
			allRunningJobsFlag,
		},
		cmdBucket: {
			etlAllObjsFlag,
			continueOnErrorFlag,
			etlExtFlag,
			forceFlag,
			copyPrependFlag,
			copyObjPrefixFlag,
			copyDryRunFlag,
			etlBucketRequestTimeout,
			templateFlag,
			listFlag,
			// TODO: progressFlag,
			waitFlag,
			waitJobXactFinishedFlag,
		},
		cmdStart: {},
	}
	showCmdETL = cli.Command{
		Name:   commandShow,
		Usage:  "show ETL(s)",
		Action: etlListHandler,
		Subcommands: []cli.Command{
			{
				Name:      cmdDetails,
				Usage:     "show ETL details",
				ArgsUsage: etlNameArgument,
				Action:    etlShowDetailsHandler,
			},
		},
	}
	stopCmdETL = cli.Command{
		Name:         cmdStop,
		Usage:        "stop ETL",
		ArgsUsage:    etlNameListArgument,
		Action:       etlStopHandler,
		BashComplete: etlIDCompletions,
		Flags:        etlSubFlags[cmdStop],
	}
	startCmdETL = cli.Command{
		Name:         cmdStart,
		Usage:        "start ETL",
		ArgsUsage:    etlNameArgument,
		Action:       etlStartHandler,
		BashComplete: etlIDCompletions,
		Flags:        etlSubFlags[cmdStart],
	}
	initCmdETL = cli.Command{
		Name:  cmdInit,
		Usage: "start ETL job: 'spec' job (requires pod yaml specification) or 'code' job (with transforming function or script in a local file)",
		Subcommands: []cli.Command{
			{
				Name:   cmdSpec,
				Usage:  "start ETL job with YAML Pod specification",
				Flags:  etlSubFlags[cmdSpec],
				Action: etlInitSpecHandler,
			},
			{
				Name:   cmdCode,
				Usage:  "start ETL job using the specified transforming function or script",
				Flags:  etlSubFlags[cmdCode],
				Action: etlInitCodeHandler,
			},
		},
	}
	objCmdETL = cli.Command{
		Name:         cmdObject,
		Usage:        "transform object",
		ArgsUsage:    etlNameArgument + " " + objectArgument + " OUTPUT",
		Action:       etlObjectHandler,
		BashComplete: etlIDCompletions,
	}
	bckCmdETL = cli.Command{
		Name:         cmdBucket,
		Usage:        "transform entire bucket or selected objects (to select, use '--list' or '--template')",
		ArgsUsage:    etlNameArgument + " " + bucketSrcArgument + " " + bucketDstArgument,
		Action:       etlBucketHandler,
		Flags:        etlSubFlags[cmdBucket],
		BashComplete: manyBucketsCompletions([]cli.BashCompleteFunc{etlIDCompletions}, 1, 2),
	}
	logsCmdETL = cli.Command{
		Name:         cmdViewLogs,
		Usage:        "view ETL logs",
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
		msg.ArgTypeX = parseStrFlag(c, argTypeFlag)
		msg.Spec = spec
	}
	if !strings.HasSuffix(msg.CommTypeX, etl.CommTypeSeparator) {
		msg.CommTypeX += etl.CommTypeSeparator
	}
	if err = msg.Validate(); err != nil {
		if e, ok := err.(*cmn.ErrETL); ok {
			err = errors.New(e.Reason)
		}
		return err
	}

	// msg.ID is `metadata.name` from podSpec
	if err = etlAlreadyExists(msg.Name()); err != nil {
		return
	}

	xid, err := api.ETLInit(apiBP, msg)
	if err != nil {
		return V(err)
	}
	fmt.Fprintf(c.App.Writer, "ETL[%s]: job %q\n", msg.Name(), xid)
	return nil
}

func etlInitCodeHandler(c *cli.Context) (err error) {
	var (
		msg      = &etl.InitCodeMsg{}
		fromFile = parseStrFlag(c, fromFileFlag)
	)
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
	if !strings.HasSuffix(msg.CommTypeX, etl.CommTypeSeparator) {
		msg.CommTypeX += etl.CommTypeSeparator
	}
	msg.ArgTypeX = parseStrFlag(c, argTypeFlag)

	if flagIsSet(c, chunkSizeFlag) {
		msg.ChunkSize, err = parseSizeFlag(c, chunkSizeFlag)
		if err != nil {
			return err
		}
	}

	msg.Timeout = cos.Duration(parseDurationFlag(c, waitPodReadyTimeoutFlag))

	// funcs
	msg.Funcs.Transform = parseStrFlag(c, funcTransformFlag)

	// validate
	if err := msg.Validate(); err != nil {
		if e, ok := err.(*cmn.ErrETL); ok {
			err = errors.New(e.Reason)
		}
		return err
	}

	// start
	xid, err := api.ETLInit(apiBP, msg)
	if err != nil {
		return V(err)
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

	return 1, etlPrintDetails(c, etlName) // TODO: extend to show Status and runtime stats
}

func etlList(c *cli.Context, caption bool) (int, error) {
	list, err := api.ETLList(apiBP)
	l := len(list)
	if err != nil || l == 0 {
		return l, V(err)
	}
	if caption {
		onlyActive := !flagIsSet(c, allJobsFlag)
		jobCptn(c, commandETL, onlyActive, "", false)
	}

	hideHeader := flagIsSet(c, noHeaderFlag)
	if hideHeader {
		return l, teb.Print(list, teb.TransformListNoHdrTmpl)
	}

	return l, teb.Print(list, teb.TransformListTmpl)
}

func etlShowDetailsHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	id := c.Args().Get(0)
	return etlPrintDetails(c, id)
}

func etlPrintDetails(c *cli.Context, id string) error {
	msg, err := api.ETLGetInitMsg(apiBP, id)
	if err != nil {
		return V(err)
	}

	fmt.Fprintln(c.App.Writer, fblue("NAME: "), msg.Name())
	fmt.Fprintln(c.App.Writer, fblue("COMMUNICATION TYPE: "), msg.CommType())
	fmt.Fprintln(c.App.Writer, fblue("ARGUMENT TYPE: "), msg.ArgType())

	if initMsg, ok := msg.(*etl.InitCodeMsg); ok {
		fmt.Fprintln(c.App.Writer, fblue("RUNTIME: "), initMsg.Runtime)
		fmt.Fprintln(c.App.Writer, fblue("CODE: "))
		fmt.Fprintln(c.App.Writer, string(initMsg.Code))
		fmt.Fprintln(c.App.Writer, fblue("DEPS: "), string(initMsg.Deps))
		fmt.Fprintln(c.App.Writer, fblue("CHUNK SIZE: "), initMsg.ChunkSize)
		return nil
	}
	if initMsg, ok := msg.(*etl.InitSpecMsg); ok {
		fmt.Fprintln(c.App.Writer, fblue("SPEC: "))
		fmt.Fprintln(c.App.Writer, string(initMsg.Spec))
		return nil
	}
	err = fmt.Errorf("invalid response [%+v, %T]", msg, msg)
	debug.AssertNoErr(err)
	return err
}

// TODO: initial, see "download logs"
func etlLogsHandler(c *cli.Context) (err error) {
	var (
		id       = c.Args().Get(0)
		targetID = c.Args().Get(1) // optional
	)
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	logs, err := api.ETLLogs(apiBP, id, targetID)
	if err != nil {
		return V(err)
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
			return V(err)
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
			return V(err)
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
		return V(err)
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
	bck, objName, errV := parseBckObjURI(c, uri, false)
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
