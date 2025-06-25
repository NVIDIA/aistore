// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that control running jobs in the cluster.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/xact"

	"github.com/urfave/cli"
	"gopkg.in/yaml.v3"
)

const etlShowErrorsUsage = "Show ETL job errors.\n" +
	indent1 + "\t- 'ais etl show errors <ETL_NAME>': display errors for inline object transformation failures.\n" +
	indent1 + "\t- 'ais etl show errors <ETL_NAME> <JOB-ID>': display errors for a specific offline (bucket-to-bucket) transform job."

const etlStartUsage = "Start ETL.\n" +
	indent1 + "\t- 'ais etl start <ETL_NAME>'\t start the specified ETL (transitions from stopped to running state)."

const etlStopUsage = "Stop ETL.\n" +
	indent1 + "\t- 'ais etl stop <ETL_NAME>'\t\t stop the specified ETL (transitions from running to stopped state).\n" +
	indent1 + "\t- 'ais etl stop --all'\t\t\t stop all running ETL jobs.\n" +
	indent1 + "\t- 'ais etl stop <ETL_NAME> <ETL_NAME2>'\t stop multiple ETL jobs by name."

const etlRemoveUsage = "Remove ETL.\n" +
	indent1 + "\t- 'ais etl rm <ETL_NAME>'\t remove (delete) the specified ETL.\n" +
	indent1 + "\t  NOTE: If the ETL is in 'running' state, it will be automatically stopped before removal."

const etlShowUsage = "Show ETL(s).\n" +
	indent1 + "\t- 'ais etl show'\t\t\t list all ETL jobs.\n" +
	indent1 + "\t- 'ais etl show details <ETL_NAME>'\t show detailed specification for specified ETL.\n" +
	indent1 + "\t- 'ais etl show errors <ETL_NAME>'\t show transformation errors for specified ETL."

const etlObjectUsage = "Transform an object.\n" +
	indent1 + "\t- 'ais etl object <ETL_NAME> <BUCKET/OBJECT_NAME> <OUTPUT>'\t transform object and save to file.\n" +
	indent1 + "\t- 'ais etl object <ETL_NAME> <BUCKET/OBJECT_NAME> -'\t\t transform and output to stdout."

const etlBucketUsage = "Transform entire bucket or selected objects (to select, use '--list', '--template', or '--prefix').\n" +
	indent1 + "\t- 'ais etl bucket <ETL_NAME> <SRC_BUCKET> <DST_BUCKET>'\t\t transform all objects from source to destination bucket.\n" +
	indent1 + "\t- 'ais etl bucket <ETL_NAME> <SRC_BUCKET> <DST_BUCKET> --prefix <PREFIX>'\t transform objects with specified prefix."

const etlLogsUsage = "View ETL logs.\n" +
	indent1 + "\t- 'ais etl view-logs <ETL_NAME>'\t\t show logs from all target nodes for specified ETL.\n" +
	indent1 + "\t- 'ais etl view-logs <ETL_NAME> <TARGET_ID>'\t show logs from specific target node."

var (
	// flags
	etlSubFlags = map[string][]cli.Flag{
		cmdSpec: {
			fromFileFlag,
			commTypeFlag,
			argTypeFlag,
			waitPodReadyTimeoutFlag,
			etlObjectRequestTimeout,
		},
		cmdStop: {
			allRunningJobsFlag,
		},
		cmdObject: {
			etlTransformArgsFlag,
		},
		cmdBucket: {
			etlAllObjsFlag,
			continueOnErrorFlag,
			etlExtFlag,
			forceFlag,
			copyPrependFlag,
			copyDryRunFlag,
			listFlag,
			templateFlag,
			numWorkersFlag,
			verbObjPrefixFlag,
			// TODO: progressFlag,
			waitFlag,
			waitJobXactFinishedFlag,
		},
		commandShow: {
			noHeaderFlag,
		},
		cmdStart: {},
		commandRemove: {
			allRunningJobsFlag,
		},
	}
	showCmdETL = cli.Command{
		Name:   commandShow,
		Usage:  etlShowUsage,
		Action: etlListHandler,
		Subcommands: []cli.Command{
			{
				Name:         cmdDetails,
				Usage:        "Show ETL specification details",
				ArgsUsage:    etlNameArgument,
				Action:       etlShowDetailsHandler,
				BashComplete: etlIDCompletions,
				Flags:        sortFlags(etlSubFlags[commandShow]),
			},
			{
				Name:         cmdErrors,
				Usage:        etlShowErrorsUsage,
				ArgsUsage:    etlNameWithJobIDArgument,
				Action:       etlShowErrorsHandler,
				BashComplete: etlIDCompletions,
				Flags:        sortFlags(etlSubFlags[commandShow]),
			},
		},
	}
	stopCmdETL = cli.Command{
		Name:         cmdStop,
		Usage:        etlStopUsage,
		ArgsUsage:    etlNameListArgument,
		Action:       etlStopHandler,
		BashComplete: etlIDCompletions,
		Flags:        sortFlags(etlSubFlags[cmdStop]),
	}
	startCmdETL = cli.Command{
		Name:         cmdStart,
		Usage:        etlStartUsage,
		ArgsUsage:    etlNameArgument,
		Action:       etlStartHandler,
		BashComplete: etlIDCompletions,
		Flags:        sortFlags(etlSubFlags[cmdStart]),
	}
	removeCmdETL = cli.Command{
		Name:         commandRemove,
		Usage:        etlRemoveUsage,
		ArgsUsage:    optionalETLNameArgument,
		Action:       etlRemoveHandler,
		BashComplete: etlIDCompletions,
		Flags:        sortFlags(etlSubFlags[commandRemove]),
	}
	initCmdETL = cli.Command{
		Name: cmdInit,
		Usage: "Initialize ETL using a runtime spec or full Kubernetes Pod spec YAML file (local or remote).\n" +
			indent1 + "\t- 'ais etl init -f <spec-file.yaml>'\t deploy ETL from a local YAML file.\n" +
			indent1 + "\t- 'ais etl init -f <URL>'\t deploy ETL from a remote YAML file.\n",
		Action: etlInitSpecHandler,
		Flags:  sortFlags(etlSubFlags[cmdSpec]),
		Subcommands: []cli.Command{
			{
				Name:   cmdSpec,
				Usage:  "Start ETL job with YAML Pod specification",
				Flags:  sortFlags(etlSubFlags[cmdSpec]),
				Action: etlInitSpecHandler,
			},
		},
	}
	objCmdETL = cli.Command{
		Name:         cmdObject,
		Usage:        etlObjectUsage,
		ArgsUsage:    etlNameArgument + " " + objectArgument + " OUTPUT",
		Action:       etlObjectHandler,
		Flags:        sortFlags(etlSubFlags[cmdObject]),
		BashComplete: etlIDCompletions,
	}
	bckCmdETL = cli.Command{
		Name:         cmdBucket,
		Usage:        etlBucketUsage,
		ArgsUsage:    etlNameArgument + " " + bucketObjectSrcArgument + " " + bucketDstArgument,
		Action:       etlBucketHandler,
		Flags:        sortFlags(etlSubFlags[cmdBucket]),
		BashComplete: manyBucketsCompletions([]cli.BashCompleteFunc{etlIDCompletions}, 1, 2),
	}
	logsCmdETL = cli.Command{
		Name:         cmdViewLogs,
		Usage:        etlLogsUsage,
		ArgsUsage:    etlNameArgument + " " + optionalTargetIDArgument,
		Action:       etlLogsHandler,
		BashComplete: etlIDCompletions,
	}
	// subcommands
	etlCmd = cli.Command{
		Name:  commandETL,
		Usage: "Execute custom transformations on objects",
		Subcommands: []cli.Command{
			initCmdETL,
			showCmdETL,
			logsCmdETL,
			startCmdETL,
			stopCmdETL,
			removeCmdETL,
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
		fmt.Println(l.Name)
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

func etlInitSpecHandler(c *cli.Context) error {
	fromFile := parseStrFlag(c, fromFileFlag)
	if fromFile == "" {
		return fmt.Errorf("flag %s must be specified", qflprn(fromFileFlag))
	}
	reader, err := readFileOrURL(fromFile)
	if err != nil {
		return err
	}
	defer reader.Close()

	decoder := yaml.NewDecoder(reader)
	first := true
	for {
		var node yaml.Node // decode one YAML document for each iteration
		if err := decoder.Decode(&node); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to decode YAML: %w", err)
		}

		if !first {
			fmt.Fprintln(c.App.Writer, separatorLine)
		}

		if err := processSpecNode(c, &node); err != nil {
			fmt.Fprintf(c.App.ErrWriter, "Skipping document: %v\n", err)
			continue
		}

		first = false
	}
	return nil
}

func processSpecNode(c *cli.Context, node *yaml.Node) error {
	var (
		specInf map[string]any
		msg     etl.InitMsg
	)

	if err := node.Decode(&specInf); err != nil {
		return err
	}

	switch {
	case specInf[etl.Runtime] != nil: // ETL runtime spec
		var etlSpec etl.ETLSpecMsg
		if err := node.Decode(&etlSpec); err != nil {
			return err
		}
		if err := populateCommonFields(c, &etlSpec); err != nil {
			return fmt.Errorf("populate fields (ETLSpecMsg): %w", err)
		}
		msg = &etlSpec
	case specInf[etl.Spec] != nil: // full Kubernetes Pod spec
		var initSpec etl.InitSpecMsg
		raw, err := yaml.Marshal(node)
		if err != nil {
			return fmt.Errorf("marshal to raw bytes: %w", err)
		}
		initSpec.Spec = raw
		if err := populateCommonFields(c, &initSpec); err != nil {
			return fmt.Errorf("populate fields (InitSpecMsg): %w", err)
		}
		msg = &initSpec
	default:
		return errors.New("unknown document (missing 'runtime' or 'spec')")
	}

	if err := msg.Validate(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}
	if err := etlAlreadyExists(msg.Name()); err != nil {
		return fmt.Errorf("duplicate ETL name %q: %w", msg.Name(), err)
	}
	xid, err := api.ETLInit(apiBP, msg)
	if err != nil {
		return fmt.Errorf("ETL init failed: %w", err)
	}

	fmt.Fprintf(c.App.Writer, "ETL[%s]: job %q\n", msg.Name(), xid)
	return printETLDetailsFromMsg(c, msg)
}

func etlListHandler(c *cli.Context) (err error) {
	_, err = etlList(c, false)
	return
}

func showETLs(c *cli.Context, xid string, caption bool) (int, error) {
	if xid == "" {
		return etlList(c, caption)
	}
	list, err := api.ETLList(apiBP)
	if err != nil {
		return 0, err
	}
	for _, entry := range list {
		if xid == entry.XactID {
			if caption {
				showETLJobDetails(c, &entry)
			}
			return 1, etlPrintDetails(c, entry.Name)
		}
	}
	return 0, fmt.Errorf("ETL with job ID %q not found", xid)
}

// showETLJobDetails displays ETL configuration options for a specific ETL job
func showETLJobDetails(c *cli.Context, etlInfo *etl.Info) {
	details, err := api.ETLGetDetail(apiBP, etlInfo.Name, "" /*xid*/)
	if err != nil {
		jobCptn(c, commandETL, etlInfo.XactID, "" /*ctlmsg*/, false /*onlyActive*/, false /*byTarget*/)
		return
	}

	// Extract ETL configuration options
	options := make([]string, 0, 6)

	msg := details.InitMsg
	if commType := msg.CommType(); commType != "" {
		options = append(options, "comm-type: "+commType)
	}
	if argType := msg.ArgType(); argType != "" {
		options = append(options, "arg-type: "+argType)
	}

	if initMsg, ok := msg.(*etl.ETLSpecMsg); ok {
		options = append(options, "image: "+initMsg.Runtime.Image)
	}

	if initTimeout, objTimeout := msg.Timeouts(); initTimeout > 0 || objTimeout > 0 {
		if initTimeout > 0 {
			options = append(options, "init-timeout: "+initTimeout.String())
		}
		if objTimeout > 0 {
			options = append(options, "object-timeout: "+objTimeout.String())
		}
	}

	// Build the control message from options
	ctlmsg := strings.Join(options, ", ")

	var xname string
	if etlInfo.XactID != "" {
		_, xname = xact.GetKindName(apc.ActETLInline)
	} else {
		xname = commandETL
	}

	jobCptn(c, xname, etlInfo.XactID, ctlmsg, false /*onlyActive*/, false /*byTarget*/)
}

func etlList(c *cli.Context, caption bool) (int, error) {
	list, err := api.ETLList(apiBP)
	l := len(list)
	if err != nil || l == 0 {
		return l, V(err)
	}
	if caption {
		onlyActive := !flagIsSet(c, allJobsFlag)
		jobCptn(c, commandETL, "" /*xid*/, "" /*ctlmsg*/, onlyActive, false)
	}

	hideHeader := flagIsSet(c, noHeaderFlag)
	if hideHeader {
		return l, teb.Print(list, teb.ETLListNoHdrTmpl)
	}

	return l, teb.Print(list, teb.ETLListTmpl)
}

func etlShowErrorsHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	var (
		etlName    = c.Args().Get(0)
		offlineXid = c.Args().Get(1)
	)
	details, err := api.ETLGetDetail(apiBP, etlName, offlineXid)
	if err != nil {
		return V(err)
	}

	hideHeader := flagIsSet(c, noHeaderFlag)
	if hideHeader {
		return teb.Print(details.ObjErrs, teb.ETLObjErrorsNoHdrTmpl)
	}
	return teb.Print(details.ObjErrs, teb.ETLObjErrorsTmpl)
}

func etlShowDetailsHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	id := c.Args().Get(0)
	return etlPrintDetails(c, id)
}

func etlPrintDetails(c *cli.Context, id string) error {
	details, err := api.ETLGetDetail(apiBP, id, "" /*xid*/)
	if err != nil {
		return V(err)
	}
	return printETLDetailsFromMsg(c, details.InitMsg)
}

func printETLDetailsFromMsg(c *cli.Context, msg etl.InitMsg) error {
	fmt.Fprintln(c.App.Writer, fblue(etl.Name+": "), msg.Name())
	fmt.Fprintln(c.App.Writer, fblue(etl.CommunicationType+": "), msg.CommType())
	fmt.Fprintln(c.App.Writer, fblue(etl.ArgType+": "), msg.ArgType())

	switch initMsg := msg.(type) {
	case *etl.InitSpecMsg:
		fmt.Fprintln(c.App.Writer, fblue(etl.Spec+": "))
		fmt.Fprintln(c.App.Writer, string(initMsg.Spec))
		return nil
	case *etl.ETLSpecMsg:
		fmt.Fprintln(c.App.Writer, fblue(etl.Runtime+": "))
		fmt.Fprintln(c.App.Writer, indent1+fblue(etl.Image+": "), initMsg.Runtime.Image)
		if len(initMsg.Runtime.Command) > 0 {
			fmt.Fprintf(c.App.Writer, indent1+"%s %v\n", fblue(etl.Command+": "), initMsg.Runtime.Command)
		}
		if len(initMsg.Runtime.Env) > 0 {
			fmt.Fprintln(c.App.Writer, indent1+fblue(etl.Env+": "), initMsg.FormatEnv())
		}
	default:
		err := fmt.Errorf("invalid response [%+v, %T]", msg, msg)
		debug.AssertNoErr(err)
		return err
	}

	return nil
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
		return V(err)
	}
	fmt.Fprintf(c.App.Writer, "ETL[%s] started successfully\n", etlName)
	return nil
}

func etlRemoveHandler(c *cli.Context) (err error) {
	var etlNames []string
	switch {
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
		if len(res) == 0 {
			fmt.Fprintln(c.App.Writer, "No ETL jobs found to remove")
			return nil
		}
		for _, etlInfo := range res {
			etlNames = append(etlNames, etlInfo.Name)
		}
	default:
		if c.NArg() == 0 {
			cli.ShowCommandHelp(c, c.Command.Name)
			return nil
		}
		etlNames = c.Args()[0:]
	}
	for _, name := range etlNames {
		msg := fmt.Sprintf("ETL[%s]", name)
		if err := api.ETLDelete(apiBP, name); err != nil {
			if herr, ok := err.(*cmn.ErrHTTP); ok && herr.Status == http.StatusNotFound {
				actionWarn(c, msg+" not found, nothing to do")
				continue
			}
			return V(err)
		}
		actionDone(c, msg+" successfully deleted")
	}
	return nil
}

func etlObjectHandler(c *cli.Context) error {
	switch c.NArg() {
	case 0, 1:
		return missingArgumentsError(c, c.Command.ArgsUsage)
	case 2:
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

	etlArgs := &api.ETLObjArgs{ETLName: etlName}
	if transformArgs := parseStrFlag(c, etlTransformArgsFlag); transformArgs != "" {
		etlArgs.TransformArgs = transformArgs
	}

	var w io.Writer
	switch {
	case outputDest == "-":
		w = os.Stdout
	case discardOutput(outputDest):
		w = io.Discard
	default:
		f, err := os.Create(outputDest)
		if err != nil {
			return err
		}
		w = f
		defer f.Close()
	}

	_, err := api.ETLObject(apiBP, etlArgs, bck, objName, w)
	return handleETLHTTPError(err, etlName)
}

// populate `EtlName` and then call `_populate()`
func populateCommonFields(c *cli.Context, initMsg etl.InitMsg) error {
	switch msg := initMsg.(type) {
	case *etl.ETLSpecMsg: // ETL runtime spec
		_populate(c, &msg.InitMsgBase)
	case *etl.InitSpecMsg: // full Kubernetes Pod spec
		pod, err := msg.ParsePodSpec()
		if err != nil {
			return err
		}
		if pod.ObjectMeta.Name != "" && msg.Name() == "" {
			msg.EtlName = pod.ObjectMeta.Name
		}
		if pod.ObjectMeta.Annotations[etl.CommTypeAnnotation] != "" && msg.CommType() == "" {
			msg.CommTypeX = pod.ObjectMeta.Annotations[etl.CommTypeAnnotation]
		}
		if pod.ObjectMeta.Annotations[etl.SupportDirectPutAnnotation] != "" {
			msg.SupportDirectPut, err = cos.ParseBool(pod.ObjectMeta.Annotations[etl.SupportDirectPutAnnotation])
			if err != nil {
				return err
			}
		}
		if pod.ObjectMeta.Annotations[etl.WaitTimeoutAnnotation] != "" {
			t, err := time.ParseDuration(pod.ObjectMeta.Annotations[etl.WaitTimeoutAnnotation])
			if err != nil {
				return err
			}
			msg.InitTimeout = cos.Duration(t)
		}
		_populate(c, &msg.InitMsgBase)
	}
	return nil
}

// populates `commTypeFlag`, `argTypeFlag`, `waitPodReadyTimeoutFlag`, `etlObjectRequestTimeout`
func _populate(c *cli.Context, base *etl.InitMsgBase) {
	if flagIsSet(c, etlNameFlag) {
		base.EtlName = parseStrFlag(c, etlNameFlag)
	}
	if flagIsSet(c, commTypeFlag) {
		base.CommTypeX = parseStrFlag(c, commTypeFlag)
	}
	if flagIsSet(c, argTypeFlag) {
		base.ArgTypeX = parseStrFlag(c, argTypeFlag)
	}
	if flagIsSet(c, waitPodReadyTimeoutFlag) {
		base.InitTimeout = cos.Duration(parseDurationFlag(c, waitPodReadyTimeoutFlag))
	}
	if flagIsSet(c, etlObjectRequestTimeout) {
		base.ObjTimeout = cos.Duration(parseDurationFlag(c, etlObjectRequestTimeout))
	}
}
