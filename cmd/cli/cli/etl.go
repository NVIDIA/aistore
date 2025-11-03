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

const etlInitUsage = "Initialize ETL using a runtime spec or full Kubernetes Pod spec YAML file (local or remote).\n" +
	indent1 + "Examples:\n" +
	indent1 + "\t- 'ais etl init -f my-etl.yaml'\t deploy ETL from a local YAML file;\n" +
	indent1 + "\t- 'ais etl init -f https://example.com/etl.yaml'\t deploy ETL from a remote YAML file;\n" +
	indent1 + "\t- 'ais etl init -f multi-etl.yaml'\t deploy multiple ETLs from a single file (separated by '---');\n" +
	indent1 + "\t- 'ais etl init -f spec.yaml --name my-custom-etl'\t override ETL name from command line;\n" +
	indent1 + "\t- 'ais etl init -f spec.yaml --comm-type hpull'\t override communication type;\n" +
	indent1 + "\t- 'ais etl init -f spec.yaml --object-timeout 30s'\t set custom object transformation timeout.\n" +
	indent1 + "\t- 'ais etl init --spec <file|URL>'\t deploy ETL jobs from a local spec file, remote URL, or multi-ETL YAML.\n" +
	indent1 + "\nAdditional Info:\n" +
	indent1 + "- You may define multiple ETLs in a single spec file using YAML document separators ('---').\n" +
	indent1 + "- CLI flags like '--name' or '--comm-type' can override values in the spec, but not when multiple ETLs are defined.\n"

const etlStartUsage = "Start ETL.\n" +
	indent1 + "Examples:\n" +
	indent1 + "\t- 'ais etl start my-etl'\t start the specified ETL (transitions from stopped to running state);\n" +
	indent1 + "\t- 'ais etl start my-etl another-etl'\t start multiple ETL jobs by name;\n" +
	indent1 + "\t- 'ais etl start -f spec.yaml'\t start ETL jobs defined in a local YAML file;\n" +
	indent1 + "\t- 'ais etl start -f https://example.com/etl.yaml'\t start ETL jobs defined in a remote YAML file;\n" +
	indent1 + "\t- 'ais etl start -f multi-etl.yaml'\t start all ETL jobs defined in a multi-ETL file;\n" +
	indent1 + "\t- 'ais etl start --spec <file|URL>'\t start ETL jobs from a local spec file, remote URL, or multi-ETL YAML.\n"

const etlStopUsage = "Stop ETL. Also aborts related offline jobs and can be used to terminate ETLs stuck in 'initializing' state.\n" +
	indent1 + "Examples:\n" +
	indent1 + "\t- 'ais etl stop my-etl'\t\t stop the specified ETL (transitions from running to stopped state);\n" +
	indent1 + "\t- 'ais etl stop my-etl another-etl'\t\t stop multiple ETL jobs by name;\n" +
	indent1 + "\t- 'ais etl stop --all'\t\t stop all running ETL jobs;\n" +
	indent1 + "\t- 'ais etl stop -f spec.yaml'\t\t stop ETL jobs defined in a local YAML file;\n" +
	indent1 + "\t- 'ais etl stop -f https://example.com/etl.yaml'\t\t stop ETL jobs defined in a remote YAML file;\n" +
	indent1 + "\t- 'ais etl stop stuck-etl'\t\t terminate ETL that is stuck in 'initializing' state;\n" +
	indent1 + "\t- 'ais etl stop --spec <file|URL>'\t\t stop ETL jobs from a local spec file, remote URL, or multi-ETL YAML.\n"

const etlRemoveUsage = "Remove ETL.\n" +
	indent1 + "Examples:\n" +
	indent1 + "\t- 'ais etl rm my-etl'\t\t remove (delete) the specified ETL;\n" +
	indent1 + "\t- 'ais etl rm my-etl another-etl'\t\t remove multiple ETL jobs by name;\n" +
	indent1 + "\t- 'ais etl rm --all'\t\t remove all ETL jobs;\n" +
	indent1 + "\t- 'ais etl rm -f spec.yaml'\t\t remove ETL jobs defined in a local YAML file;\n" +
	indent1 + "\t- 'ais etl rm -f https://example.com/etl.yaml'\t\t remove ETL jobs defined in a remote YAML file;\n" +
	indent1 + "\t- 'ais etl rm running-etl'q\t\t remove ETL that is currently running (will be stopped first).\n" +
	indent1 + "\t- 'ais etl rm --spec <file|URL>'\t\t remove ETL jobs from a local spec file, remote URL, or multi-ETL YAML.\n" +
	indent1 + "\t  NOTE: If an ETL is in 'running' state, it will be stopped automatically before removal."

const etlShowUsage = "Show ETL(s).\n" +
	indent1 + "Examples:\n" +
	indent1 + "\t- 'ais etl show'\t\t\t list all ETL jobs with their status and details;\n" +
	indent1 + "\t- 'ais etl show my-etl'\t\t\t show detailed specification for a specific ETL job;\n" +
	indent1 + "\t- 'ais etl show my-etl another-etl'\t\t\t show detailed specifications for multiple ETL jobs;\n" +
	indent1 + "\t- 'ais etl show errors my-etl'\t\t\t show transformation errors for inline object transformations;\n" +
	indent1 + "\t- 'ais etl show errors my-etl job-123'\t\t\t show errors for a specific offline (bucket-to-bucket) transform job."

const etlObjectUsage = "Transform an object.\n" +
	indent1 + "Examples:\n" +
	indent1 + "\t- 'ais etl object my-etl ais://src/image.jpg /tmp/output.jpg'\t transform object and save to file;\n" +
	indent1 + "\t- 'ais etl object my-etl ais://src/data.json -'\t transform and output to stdout;\n" +
	indent1 + "\t- 'ais etl object my-etl ais://src/doc.pdf /dev/null'\t transform and discard output;\n" +
	indent1 + "\t- 'ais etl object \"etl-1>>etl-2>>etl-3\" ais://src/data.txt output.txt'\t transform object using multiple ETLs run in a pipeline;\n" +
	indent1 + "\t- 'ais etl object my-etl cp ais://src/image.jpg ais://dst/'\t transform and copy to another bucket;\n" +
	indent1 + "\t- 'ais etl object \"etl-1>>etl-2\" cp ais://src/data ais://dst/'\t transform and copy object using multiple ETLs run in a pipeline;\n" +
	indent1 + "\t- 'ais etl object my-etl ais://src/data.xml output.json --args \"format=json\"'\t transform with custom arguments."

const etlBucketUsage = "Transform entire bucket or selected objects (to select, use '--list', '--template', or '--prefix').\n" +
	indent1 + "Examples:\n" +
	indent1 + "\t- 'ais etl bucket my-etl ais://src ais://dst'\t transform all objects from source to destination bucket;\n" +
	indent1 + "\t- 'ais etl bucket \"etl-1>>etl-2>>etl-3\" ais://src ais://dst'\t transform all objects from source to destination bucket using multiple ETLs run in a pipeline;\n" +
	indent1 + "\t- 'ais etl bucket my-etl ais://src ais://dst --prefix images/'\t transform objects with prefix 'images/';\n" +
	indent1 + "\t- 'ais etl bucket my-etl ais://src ais://dst --template \"shard-{0001..0999}.tar\"'\t transform objects matching the template;\n" +
	indent1 + "\t- 'ais etl bucket \"etl-1>>etl-2\" s3://remote-src ais://dst --all'\t transform all objects including non-cached ones using multiple ETLs run in a pipeline;\n" +
	indent1 + "\t- 'ais etl bucket my-etl ais://src ais://dst --dry-run'\t preview transformation without executing;\n" +
	indent1 + "\t- 'ais etl bucket my-etl ais://src ais://dst --num-workers 8'\t use 8 concurrent workers for transformation;\n" +
	indent1 + "\t- 'ais etl bucket my-etl ais://src ais://dst --prepend processed/'\t add prefix to transformed object names."

const etlLogsUsage = "View ETL logs.\n" +
	indent1 + "Examples:\n" +
	indent1 + "\t- 'ais etl view-logs my-etl'\t show logs from all target nodes for the specified ETL;\n" +
	indent1 + "\t- 'ais etl view-logs my-etl target-001'\t show logs from a specific target node;\n" +
	indent1 + "\t- 'ais etl view-logs data-converter target-002'\t view logs from target-002 for data-converter ETL."

var (
	// flags
	etlSubFlags = map[string][]cli.Flag{
		cmdSpec: {
			specFlag,
			etlNameFlag,
			commTypeFlag,
			waitPodReadyTimeoutFlag,
			etlObjectRequestTimeout,
		},
		cmdStop: {
			specFlag,
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
		cmdStart: {
			specFlag,
		},
		commandRemove: {
			specFlag,
			allRunningJobsFlag,
		},
	}
	showCmdETL = cli.Command{
		Name:         commandShow,
		Usage:        etlShowUsage,
		Action:       etlListHandler,
		ArgsUsage:    etlNameListArgument,
		BashComplete: etlIDCompletions,
		Subcommands: []cli.Command{
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
		ArgsUsage:    etlNameOrSelectorArgs,
		Action:       etlStopHandler,
		BashComplete: etlIDCompletions,
		Flags:        sortFlags(etlSubFlags[cmdStop]),
	}
	startCmdETL = cli.Command{
		Name:         cmdStart,
		Usage:        etlStartUsage,
		ArgsUsage:    etlNameListArgument,
		Action:       etlStartHandler,
		BashComplete: etlIDCompletions,
		Flags:        sortFlags(etlSubFlags[cmdStart]),
	}
	removeCmdETL = cli.Command{
		Name:         commandRemove,
		Usage:        etlRemoveUsage,
		ArgsUsage:    etlNameOrSelectorArgs,
		Action:       etlRemoveHandler,
		BashComplete: etlIDCompletions,
		Flags:        sortFlags(etlSubFlags[commandRemove]),
	}
	initCmdETL = cli.Command{
		Name:   cmdInit,
		Usage:  etlInitUsage,
		Action: etlInitSpecHandler,
		Flags:  sortFlags(etlSubFlags[cmdSpec]),
		Subcommands: []cli.Command{
			{
				Name:   cmdSpec,
				Usage:  "Start an ETL job using a YAML Pod specification (equivalent to 'ais etl init -f <spec-file.yaml|URL>')",
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
		BashComplete: manyBucketsCompletions([]cli.BashCompleteFunc{etlIDCompletions}, 1),
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
		Usage: "Manage and execute custom ETL (Extract, Transform, Load) jobs",
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

// checkOverrideFlags checks if any override flags are set when multiple ETL documents are present
func checkOverrideFlags(c *cli.Context, nodes []*yaml.Node) error {
	// If there are multiple docs, disallow any override flags
	if len(nodes) > 1 {
		overrideFlags := []struct {
			name string
			set  bool
		}{
			{etlNameFlag.GetName(), flagIsSet(c, etlNameFlag)},
			{commTypeFlag.GetName(), flagIsSet(c, commTypeFlag)},
			{waitPodReadyTimeoutFlag.GetName(), flagIsSet(c, waitPodReadyTimeoutFlag)},
			{etlObjectRequestTimeout.GetName(), flagIsSet(c, etlObjectRequestTimeout)},
		}

		var used []string
		for _, f := range overrideFlags {
			if f.set {
				used = append(used, "--"+f.name)
			}
		}

		if len(used) > 0 {
			return fmt.Errorf(
				"cannot use override flags %v with a multi-ETL file; remove these flags or split into separate files",
				used,
			)
		}
	}
	return nil
}

func etlInitSpecHandler(c *cli.Context) error {
	fromFile := parseStrFlag(c, specFlag)
	if fromFile == "" {
		return fmt.Errorf("flag %s must be specified", qflprn(specFlag))
	}
	reader, err := openFileOrURL(fromFile)
	if err != nil {
		return err
	}
	defer reader.Close()

	// Read all YAML docs into memory
	decoder := yaml.NewDecoder(reader)
	nodes := make([]*yaml.Node, 0, 4)
	for {
		node := &yaml.Node{}
		if err := decoder.Decode(node); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to decode YAML: %w", err)
		}
		nodes = append(nodes, node)
	}

	if len(nodes) == 0 {
		return errors.New("empty YAML spec")
	}

	// Check if any override flags are set when multiple ETL documents are present
	if err := checkOverrideFlags(c, nodes); err != nil {
		return err
	}

	// Now process each document
	first := true
	for _, node := range nodes {
		if !first {
			fmt.Fprintln(c.App.Writer, separatorLine)
		}
		if err := processSpecNode(c, node); err != nil {
			fmt.Fprintf(c.App.ErrWriter, "Skipping document: %v\n", err)
			continue
		}
		first = false
	}
	return nil
}

// parseSpecNode only decodes into the correct InitMsg type.
func parseSpecNode(node *yaml.Node) (etl.InitMsg, error) {
	specInf := make(map[string]any)
	if err := node.Decode(&specInf); err != nil {
		return nil, fmt.Errorf("failed to decode spec metadata: %w", err)
	}

	// ETL runtime spec
	if specInf[etl.Runtime] != nil {
		var msg etl.ETLSpecMsg
		if err := node.Decode(&msg); err != nil {
			return nil, fmt.Errorf("failed to decode ETLSpecMsg: %w", err)
		}
		return &msg, nil
	}

	// Full Kubernetes Pod spec
	if specInf[etl.Spec] != nil {
		var initSpec etl.InitSpecMsg
		raw, err := yaml.Marshal(node)
		if err != nil {
			return nil, fmt.Errorf("marshal to raw bytes: %w", err)
		}
		initSpec.Spec = raw
		return &initSpec, nil
	}

	return nil, errors.New("unknown document (missing '" + etl.Runtime + "' or '" + etl.Spec + "')")
}

// processSpecNode now handles common-fields population right after parsing,
// then validation, duplicate checks, and the actual init call.
func processSpecNode(c *cli.Context, node *yaml.Node) error {
	// 1) Decode into the proper InitMsg
	msg, err := parseSpecNode(node)
	if err != nil {
		return fmt.Errorf("parse spec node: %w", err)
	}

	// 2) Populate CLI flags / common fields
	if err := populateCommonFields(c, msg); err != nil {
		return fmt.Errorf("populate common fields: %w", err)
	}

	// 3) Validation
	if err := msg.Validate(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// 4) Duplicate‐name guard
	name := msg.Name()
	if err := etlAlreadyExists(name); err != nil {
		return fmt.Errorf("duplicate ETL name %q: %w", name, err)
	}

	// 5) Perform ETL init
	xid, err := api.ETLInit(apiBP, msg)
	if err != nil {
		return fmt.Errorf("ETL init failed: %w", err)
	}

	fmt.Fprintf(c.App.Writer, "ETL[%s]: job %q\n", name, xid)
	return printETLDetailsFromMsg(c, msg)
}

func etlListHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		_, err = etlList(c, false)
		return err
	}

	etlNames := c.Args()[0:]
	first := true
	for _, name := range etlNames {
		if !first {
			fmt.Fprintln(c.App.Writer, separatorLine)
		}
		err = etlPrintDetails(c, name)
		if err != nil {
			fmt.Fprintln(c.App.ErrWriter, err)
		}
		first = false
	}
	return nil
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
	case flagIsSet(c, specFlag):
		etlNames, err = getETLNamesFromFile(c)
		if err != nil {
			return err
		}
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

func etlStartHandler(c *cli.Context) error {
	var (
		etlNames []string
		err      error
	)

	switch {
	case flagIsSet(c, specFlag):
		etlNames, err = getETLNamesFromFile(c)
		if err != nil {
			return err
		}
		if len(etlNames) == 0 {
			return fmt.Errorf("no ETL names found in the file provided via %s", qflprn(specFlag))
		}
	case c.NArg() == 0:
		return missingArgumentsError(c, c.Command.ArgsUsage)
	default:
		etlNames = c.Args()[0:]
	}
	for _, name := range etlNames {
		if err := api.ETLStart(apiBP, name); err != nil {
			fmt.Fprintln(c.App.ErrWriter, err)
			continue
		}
		fmt.Fprintf(c.App.Writer, "ETL[%s] started successfully\n", name)
	}
	return nil
}

// getETLNamesFromFile reads ETL names from a YAML file or URL.
func getETLNamesFromFile(c *cli.Context) ([]string, error) {
	etlNames := make([]string, 0, 4)
	if c.NArg() > 0 {
		etlNames = c.Args()[0:]
		return nil, incorrectUsageMsg(c, "flag %s cannot be used together with %s %v",
			qflprn(specFlag), etlNameArgument, etlNames)
	}
	fromFile := parseStrFlag(c, specFlag)
	if fromFile == "" {
		return nil, fmt.Errorf("flag %s must be specified", qflprn(specFlag))
	}
	reader, err := openFileOrURL(fromFile)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	decoder := yaml.NewDecoder(reader)
	for {
		node := &yaml.Node{}
		if err := decoder.Decode(node); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to decode YAML: %w", err)
		}
		msg, err := parseSpecNode(node)
		if err != nil {
			return nil, err
		}
		etlNames = append(etlNames, msg.Name())
	}
	return etlNames, nil
}

func etlRemoveHandler(c *cli.Context) (err error) {
	var etlNames []string
	switch {
	case flagIsSet(c, specFlag):
		etlNames, err = getETLNamesFromFile(c)
		if err != nil {
			return err
		}
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

// parseETLPipeline creates an ETL pipeline from a string containing '>>' operators
// or returns a single ETL if no pipeline syntax is detected
func parseETLPipeline(etlNameOrPipeline string) (*api.ETL, error) {
	etlNames, err := parseETLNames(etlNameOrPipeline)
	if err != nil {
		return nil, err
	}

	// Build pipeline by chaining ETLs
	etl := &api.ETL{ETLName: etlNames[0]}
	for i, name := range etlNames[1:] {
		if name == "" {
			return nil, fmt.Errorf("empty ETL name at position %d in pipeline", i+2)
		}
		etl = etl.Chain(&api.ETL{ETLName: name})
	}
	return etl, nil
}

func etlObjectHandler(c *cli.Context) error {
	// Check if this is a copy operation
	if c.NArg() >= 2 && c.Args().Get(1) == "cp" {
		return etlObjectCopyHandler(c)
	}

	// Regular transform operation
	switch c.NArg() {
	case 0, 1:
		return missingArgumentsError(c, c.Command.ArgsUsage)
	case 2:
		return missingArgumentsError(c, "OUTPUT")
	}

	var (
		etlNameOrPipeline = c.Args().Get(0)
		uri               = c.Args().Get(1)
		outputDest        = c.Args().Get(2)
	)
	bck, objName, errV := parseBckObjURI(c, uri, false)
	if errV != nil {
		return errV
	}

	// Parse ETL pipeline or single ETL
	etlArgs, err := parseETLPipeline(etlNameOrPipeline)
	if err != nil {
		return fmt.Errorf("failed to parse ETL name or pipeline: %v", err)
	}

	if transformArgs := parseStrFlag(c, etlTransformArgsFlag); transformArgs != "" {
		etlArgs.TransformArgs = transformArgs
	}

	w, wfh, err := createDstFile(c, outputDest, true /*allow stdout*/)
	if err != nil {
		if err == errUserCancel {
			return nil
		}
		return err
	}

	_, err = api.ETLObject(apiBP, etlArgs, bck, objName, w)
	if wfh != nil {
		cos.Close(wfh)
		if err != nil {
			if e := cos.RemoveFile(outputDest); e != nil {
				actionWarn(c, fmt.Sprintf("failed to delete %s: %v", outputDest, e))
			}
		}
	}
	return handleETLHTTPError(err, etlNameOrPipeline)
}

func etlObjectCopyHandler(c *cli.Context) error {
	// Handle: ais etl object <ETL_NAME_OR_PIPELINE> cp <SRC> <DST>
	if c.NArg() < 4 {
		return missingArgumentsError(c, "<ETL_NAME_OR_PIPELINE> cp <SRC_BUCKET/OBJECT> <DST_BUCKET[/OBJECT]>")
	}

	etlNameOrPipeline := c.Args().Get(0)
	// Skip index 1 which is "cp"
	srcBck, dstBck, srcObjName, dstObjName, err := parseFromToURIs(c, objectArgument, bucketDstArgument, 2, true, true)
	if err != nil {
		return err
	}

	// Parse ETL pipeline or single ETL
	etlArgs, err := parseETLPipeline(etlNameOrPipeline)
	if err != nil {
		return fmt.Errorf("failed to parse ETL pipeline: %v", err)
	}

	// Call the transform API
	err = api.TransformObject(apiBP, &api.TransformArgs{
		CopyArgs: api.CopyArgs{
			FromBck:     srcBck,
			FromObjName: srcObjName,
			ToBck:       dstBck,
			ToObjName:   dstObjName,
		},
		ETL: *etlArgs,
	})

	if err != nil {
		return handleETLHTTPError(err, etlNameOrPipeline)
	}

	dstObjName = cos.Left(dstObjName, srcObjName)
	fmt.Fprintf(c.App.Writer, "ETL[%s]: %s => %s\n", etlNameOrPipeline, srcBck.Cname(srcObjName), dstBck.Cname(dstObjName))
	return nil
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

// populates `commTypeFlag`, `waitPodReadyTimeoutFlag`, `etlObjectRequestTimeout`
func _populate(c *cli.Context, base *etl.InitMsgBase) {
	if flagIsSet(c, etlNameFlag) {
		base.EtlName = parseStrFlag(c, etlNameFlag)
	}
	if flagIsSet(c, commTypeFlag) {
		base.CommTypeX = parseStrFlag(c, commTypeFlag)
	}
	if flagIsSet(c, waitPodReadyTimeoutFlag) {
		base.InitTimeout = cos.Duration(parseDurationFlag(c, waitPodReadyTimeoutFlag))
	}
	if flagIsSet(c, etlObjectRequestTimeout) {
		base.ObjTimeout = cos.Duration(parseDurationFlag(c, etlObjectRequestTimeout))
	}
}

func handleETLHTTPError(err error, etlName string) error {
	if err == nil {
		return nil
	}
	if herr := cmn.AsErrHTTP(err); herr != nil {
		// TODO: How to find out if it's transformation not found, and not object not found?
		if herr.Status == http.StatusNotFound && strings.Contains(herr.Error(), etlName) {
			return fmt.Errorf("ETL[%s] not found; try starting new ETL with:\nais %s %s <spec>",
				etlName, commandETL, cmdInit)
		}
	}
	return V(err)
}
