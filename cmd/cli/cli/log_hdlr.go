// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/urfave/cli"
)

var (
	nodeLogFlags = map[string][]cli.Flag{
		commandShow: append(
			longRunFlags,
			logSevFlag,
			logFlushFlag,
		),
		commandGet: append(
			longRunFlags,
			logSevFlag,
			yesFlag,
			allLogsFlag,
		),
	}

	// 'show log' and 'log show'
	showCmdLog = cli.Command{
		Name: cmdLog,
		Usage: fmt.Sprintf("for a given node: show its current log (use %s to update, %s for details)",
			qflprn(refreshFlag), qflprn(cli.HelpFlag)),
		ArgsUsage:    showLogArgument,
		Flags:        nodeLogFlags[commandShow],
		Action:       showNodeLogHandler,
		BashComplete: suggestAllNodes,
	}
	getCmdLog = cli.Command{
		Name: commandGet,
		Usage: "for a given node: download its current log or all logs, e.g.:\n" +
			indent4 + "\t - 'ais get log NODE_ID /tmp' - download the node's current log to the specified directory;\n" +
			indent4 + "\t - 'ais get log abcdefgh /tmp/target-abcdefgh --refresh 10' - update (ie., append) every 10s",
		ArgsUsage:    getLogArgument,
		Flags:        nodeLogFlags[commandGet],
		Action:       getNodeLogHandler,
		BashComplete: suggestAllNodes,
	}

	// top-level
	logCmd = cli.Command{
		Name:  commandLog,
		Usage: "view ais node's log in real time; download one or all logs",
		Subcommands: []cli.Command{
			makeAlias(showCmdLog, "", true, commandShow),
			getCmdLog,
		},
	}
)

func showNodeLogHandler(c *cli.Context) error {
	return _logHandler(c)
}

func getNodeLogHandler(c *cli.Context) error {
	//
	// 1. get the current log
	//
	if !flagIsSet(c, allLogsFlag) {
		return _logHandler(c)
	}
	if c.NArg() < 1 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	if flagIsSet(c, refreshFlag) {
		return incorrectUsageMsg(c, errFmtExclusive, qflprn(allLogsFlag), qflprn(refreshFlag))
	}

	//
	// 2. get archived logs
	//
	sev, err := parseLogSev(c)
	if err != nil {
		return err
	}
	node, sname, err := getNode(c, c.Args().Get(0))
	if err != nil {
		return err
	}
	var (
		outFile    = c.Args().Get(1)
		tempdir, s string
		file       *os.File
		confirmed  bool
	)
	if outFile == fileStdIO {
		return fmt.Errorf("cannot deliver archived logs to standard output")
	}
	if outFile == "" {
		tempdir = filepath.Join(os.TempDir(), "aislogs-"+node.ID())
		err = cos.CreateDir(tempdir)
		if err != nil {
			return fmt.Errorf("failed to create temp dir %s: %v", tempdir, err)
		}
		if node.IsTarget() {
			outFile = filepath.Join(tempdir, apc.Target+archive.ExtTarGz) // see also: targzLogs
		} else {
			outFile = filepath.Join(tempdir, apc.Proxy+archive.ExtTarGz) // ditto
		}
	} else {
		outFile, confirmed = _logDestName(c, node, outFile)
		if !confirmed {
			return nil
		}
		if outFile != discardIO {
			if !strings.HasSuffix(outFile, archive.ExtTarGz) && !strings.HasSuffix(outFile, archive.ExtTgz) {
				outFile += archive.ExtTarGz
			}
		}
	}
	if file, err = os.Create(outFile); err != nil {
		return fmt.Errorf("failed to create destination %s: %v", outFile, err)
	}

	if sev == apc.LogErr || sev == apc.LogWarn {
		s = " (errors and warnings)"
	}
	if outFile != discardIO {
		fmt.Fprintf(c.App.Writer, "Downloading %s%s logs as %s ...\n", sname, s, outFile)
	} else {
		fmt.Fprintf(c.App.Writer, "Downloading (and discarding) %s%s logs ...\n", sname, s)
	}

	// call api
	args := api.GetLogInput{Writer: file, Severity: sev, All: true}
	_, err = api.GetDaemonLog(apiBP, node, args)
	file.Close()
	if err == nil {
		actionDone(c, "Done")
	}
	return V(err)
}

// common (show, get) one log
func _logHandler(c *cli.Context) error {
	if c.NArg() < 1 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	node, sname, err := getNode(c, c.Args().Get(0))
	if err != nil {
		return err
	}
	// destination
	outFile := c.Args().Get(1)

	sev, err := parseLogSev(c)
	if err != nil {
		return err
	}

	firstIteration := setLongRunParams(c, 0)
	if firstIteration && flagIsSet(c, logFlushFlag) {
		var (
			flushRate = parseDurationFlag(c, logFlushFlag)
			nvs       = make(cos.StrKVs)
		)
		config, err := api.GetDaemonConfig(apiBP, node)
		if err != nil {
			return V(err)
		}
		if config.Log.FlushTime.D() != flushRate {
			nvs[nodeLogFlushName] = flushRate.String()
			if err := api.SetDaemonConfig(apiBP, node.ID(), nvs, true /*transient*/); err != nil {
				return V(err)
			}
			warn := fmt.Sprintf("run 'ais config node %s inherited %s %s' to change it back",
				sname, nodeLogFlushName, config.Log.FlushTime)
			actionWarn(c, warn)
			time.Sleep(2 * time.Second)
			fmt.Fprintln(c.App.Writer)
		}
	}

	var (
		file     *os.File
		readsize int64
		s        string
		writer   = os.Stdout // default
		args     = api.GetLogInput{Severity: sev, Offset: getLongRunOffset(c)}
	)
	if outFile != fileStdIO && outFile != "" /* empty => standard output */ {
		var confirmed bool
		outFile, confirmed = _logDestName(c, node, outFile)
		if !confirmed {
			return nil
		}
		if args.Offset == 0 {
			if file, err = os.Create(outFile); err != nil {
				return err
			}
			setLongRunOutfile(c, file)
			if sev == apc.LogErr || sev == apc.LogWarn {
				s = " (errors and warnings)"
			}
			if outFile != discardIO {
				fmt.Fprintf(c.App.Writer, "Downloading %s%s log as %s ...\n", sname, s, outFile)
			} else {
				fmt.Fprintf(c.App.Writer, "Downloading (and discarding) %s%s log ...\n", sname, s)
			}
		} else {
			file = getLongRunOutfile(c)
		}
		writer = file
	}

	// call api
	args.Writer = writer
	readsize, err = api.GetDaemonLog(apiBP, node, args)
	if err == nil {
		if isLongRun(c) {
			addLongRunOffset(c, readsize)
		} else if file != nil {
			file.Close()
			actionDone(c, "Done")
		}
	} else if file != nil {
		if off, _ := file.Seek(0, io.SeekCurrent); off == 0 {
			file.Close()
			os.Remove(outFile)
			setLongRunOutfile(c, nil)
			file = nil
		}
		if file != nil && !isLongRun(c) {
			file.Close()
		}
	}
	return V(err)
}

func parseLogSev(c *cli.Context) (sev string, err error) {
	sev = strings.ToLower(parseStrFlag(c, logSevFlag))
	if sev != "" {
		switch sev[0] {
		case apc.LogInfo[0]:
			sev = apc.LogInfo
		case apc.LogWarn[0]:
			sev = apc.LogWarn
		case apc.LogErr[0]:
			sev = apc.LogErr
		default:
			err = fmt.Errorf("invalid log severity, expecting empty string or one of: %s, %s, %s",
				apc.LogInfo, apc.LogWarn, apc.LogErr)
		}
	}
	return
}

func _logDestName(c *cli.Context, node *meta.Snode, outFile string) (string, bool) {
	if outFile == discardIO {
		return outFile, true
	}
	finfo, errEx := os.Stat(outFile)
	if errEx != nil {
		return outFile, true
	}
	// destination: directory | file (confirm overwrite)
	if finfo.IsDir() {
		if node.IsTarget() {
			outFile = filepath.Join(outFile, "ais"+apc.Target+"-"+node.ID())
		} else {
			outFile = filepath.Join(outFile, "ais"+apc.Proxy+"-"+node.ID())
		}
		// TODO: strictly speaking: fstat again and confirm if exists
	} else if finfo.Mode().IsRegular() && !flagIsSet(c, yesFlag) {
		warn := fmt.Sprintf("overwrite existing %q", outFile)
		if ok := confirm(c, warn); !ok {
			return outFile, false
		}
	}
	return outFile, true
}
