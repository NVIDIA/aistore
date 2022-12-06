// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmd/cli/config"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/fatih/color"
	"github.com/urfave/cli"
)

const (
	cliName  = "ais"
	ua       = "ais/cli"
	metadata = "md"
	cliDescr = `If [Tab] completion doesn't work:
   * download ` + cmn.GitHubHome + `/tree/master/cmd/cli/autocomplete
   * and run 'install.sh'.
   To install CLI directly from GitHub, use ` + cmn.GitHubHome + `/blob/master/deploy/scripts/install_from_binaries.sh`
)

type (
	acli struct {
		app       *cli.App
		outWriter io.Writer
		errWriter io.Writer
		longRun   *longRun
	}
	longRun struct {
		count       int
		footer      int
		refreshRate time.Duration
		offset      int64
	}
)

var (
	cfg         *config.Config
	buildTime   string
	k8sDetected bool
)

// color
var (
	fred, fcyan func(a ...any) string
)

// main method
func Run(version, buildtime string, args []string) error {
	a := acli{app: cli.NewApp(), outWriter: os.Stdout, errWriter: os.Stderr, longRun: &longRun{}}
	buildTime = buildtime
	a.init(version)
	// run
	if err := a.runOnce(args); err != nil {
		return err
	}
	if !a.longRun.isSet() {
		return nil
	}
	if a.longRun.isForever() {
		return a.runForever(args)
	}
	return a.runNTimes(args)
}

func (a *acli) runOnce(args []string) error {
	err := a.app.Run(args)
	return a.formatErr(err)
}

func (a *acli) runForever(args []string) error {
	rate := a.longRun.refreshRate
	for {
		time.Sleep(rate)
		if a.longRun.footer > 0 {
			fmt.Fprintln(a.outWriter, fcyan(strings.Repeat("-", a.longRun.footer)))
		}
		if err := a.runOnce(args); err != nil {
			return err
		}
	}
}

func (a *acli) runNTimes(args []string) error {
	var (
		countdown = a.longRun.count - 1
		rate      = a.longRun.refreshRate
	)
	for ; countdown > 0; countdown-- {
		time.Sleep(rate)
		fmt.Fprintln(a.outWriter, fcyan("--------"))
		if err := a.runOnce(args); err != nil {
			return err
		}
	}
	return nil
}

func isUnreachableError(err error) (msg string, unreachable bool) {
	switch err := err.(type) {
	case *cmn.ErrHTTP:
		errHTTP := cmn.Err2HTTPErr(err)
		msg = errHTTP.Message
		unreachable = cos.IsUnreachable(err, err.Status) || strings.Contains(msg, cmn.EmptyProtoSchemeForURL)
	case *errUsage, *errAdditionalInfo:
		return "", false
	default:
		msg = err.Error()
		regx := regexp.MustCompile("dial.*(timeout|refused)")
		if unreachable = regx.MatchString(msg); unreachable {
			i := strings.Index(msg, "dial")
			debug.Assert(i >= 0)
			msg = msg[i:]
		}
	}
	return
}

func redErr(err error) error {
	msg := strings.TrimRight(err.Error(), "\n")
	return errors.New(fred("Error: ") + msg)
}

// Formats error message
func (a *acli) formatErr(err error) error {
	if err == nil {
		return nil
	}
	if _, unreachable := isUnreachableError(err); unreachable {
		errmsg := fmt.Sprintf("AIStore cannot be reached at %s\n", clusterURL)
		errmsg += fmt.Sprintf("Make sure that environment variable %s points to an AIS gateway "+
			"(any AIS gateway in the cluster)\n"+
			"For defaults, see CLI config at %s (or run `ais show config cli`)",
			env.AIS.Endpoint, config.Path())
		return redErr(errors.New(errmsg))
	}
	switch err := err.(type) {
	case *cmn.ErrHTTP:
		return redErr(err)
	case *errUsage:
		return err
	case *errAdditionalInfo:
		err.baseErr = a.formatErr(err.baseErr)
		return err
	default:
		return redErr(err)
	}
}

func onBeforeCommand(c *cli.Context) error {
	// While `color.NoColor = flagIsSet(c, noColorFlag)` looks shorter and
	// better, it may ruin some output. Why: the library automatically
	// disables coloring if TERM="dumb" or Stdout is redirected. In those
	// cases, we should not override `NoColor` with `false` when the flag
	// is not defined. So, here we can only disable coloring manually.
	if flagIsSet(c, noColorFlag) {
		color.NoColor = true
	}
	return nil
}

func (a *acli) init(version string) {
	app := a.app

	fcyan = color.New(color.FgHiCyan).SprintFunc()
	fred = color.New(color.FgHiRed).SprintFunc()

	app.Name = cliName
	app.Usage = "AIS CLI: command-line management utility for AIStore"
	app.Version = version
	app.EnableBashCompletion = true
	app.HideHelp = true
	app.Flags = []cli.Flag{cli.HelpFlag, noColorFlag}
	app.CommandNotFound = commandNotFoundHandler
	app.OnUsageError = incorrectUsageHandler
	app.Metadata = map[string]any{metadata: a.longRun}
	app.Writer = a.outWriter
	app.ErrWriter = a.errWriter
	app.Before = onBeforeCommand // to disable colors if `no-colors' is set
	app.Description = cliDescr
	cli.VersionFlag = cli.BoolFlag{
		Name:  "version, V",
		Usage: "print only the version",
	}
	initJobSub()
	a.setupCommands()
}

func (a *acli) setupCommands() {
	app := a.app

	// Note: order of commands below is the order shown in "ais help"
	app.Commands = []cli.Command{
		bucketCmd,
		objectCmd,
		clusterCmd,
		configCmd,
		etlCmd,
		jobCmd,
		authCmd,
		showCmd,
		helpCommand,
		advancedCmd,
		storageCmd,
		archCmd,
		logCmd,
		rebalanceCmd,
		remClusterCmd,
		a.getAliasCmd(),
	}

	if k8sDetected {
		app.Commands = append(app.Commands, k8sCmd)
	}
	app.Commands = append(app.Commands, a.initAliases()...)
	setupCommandHelp(app.Commands)
	a.enableSearch()
}

func (a *acli) enableSearch() {
	initSearch(a.app)
	a.app.Commands = append(a.app.Commands, searchCommands...)
}

func setupCommandHelp(commands []cli.Command) {
	helps := strings.Split(cli.HelpFlag.GetName(), ",")
	helpName := strings.TrimSpace(helps[0])
	for i := range commands {
		command := &commands[i]

		// Get rid of 'h'/'help' subcommands
		// and add the help flag manually
		command.HideHelp = true
		// (but only if there isn't one already)
		if !hasHelpFlag(command.Flags, helpName) {
			command.Flags = append(command.Flags, cli.HelpFlag)
		}
		command.OnUsageError = incorrectUsageHandler

		// recursively
		setupCommandHelp(command.Subcommands)
	}
}

func hasHelpFlag(commandFlags []cli.Flag, helpName string) bool {
	for _, flag := range commandFlags {
		for _, name := range strings.Split(flag.GetName(), ",") {
			name = strings.TrimSpace(name)
			if name == helpName {
				return true
			}
		}
	}
	return false
}

// This is a copy-paste from urfave/cli/help.go. It is done to remove the 'h' alias of the 'help' command
var helpCommand = cli.Command{
	Name:      "help",
	Usage:     "show a list of commands; show help for a given command",
	ArgsUsage: "[COMMAND]",
	Action: func(c *cli.Context) error {
		args := c.Args()
		if args.Present() {
			return cli.ShowCommandHelp(c, args.First())
		}

		cli.ShowAppHelp(c)
		return nil
	},
	BashComplete: func(c *cli.Context) {
		for _, cmd := range c.App.Commands {
			fmt.Println(cmd.Name)
		}
	},
}

// Print error and terminate
func commandNotFoundHandler(c *cli.Context, cmd string) {
	if cmd == "version" {
		fmt.Fprintf(c.App.Writer, "version %s (build %s)\n", c.App.Version, buildTime)
		return
	}
	err := commandNotFoundError(c, cmd)
	fmt.Fprint(c.App.ErrWriter, err.Error())
	os.Exit(1)
}

/////////////
// longRun //
/////////////

func (p *longRun) isForever() bool {
	return p.count == countUnlimited
}

func (p *longRun) isSet() bool {
	return p.refreshRate != 0
}

func setLongRunParams(c *cli.Context, footer ...int) bool {
	params := c.App.Metadata[metadata].(*longRun)
	if params.isSet() {
		return false
	}
	params.footer = 8
	if len(footer) > 0 {
		params.footer = footer[0]
	}
	if flagIsSet(c, refreshFlag) {
		params.refreshRate = parseDurationFlag(c, refreshFlag)
		params.count = countUnlimited // unless counted (below)
	}
	if flagIsSet(c, countFlag) {
		params.count = parseIntFlag(c, countFlag)
		if params.count <= 0 {
			warn := fmt.Sprintf("option '--%s=%d' is invalid (must be >= 1). Proceeding with '--%s=%d' (default).",
				countFlag.Name, params.count, countFlag.Name, countDefault)
			actionWarn(c, warn)
			params.count = countDefault
		}
	}
	return true
}

func addLongRunOffset(c *cli.Context, off int64) {
	params := c.App.Metadata[metadata].(*longRun)
	params.offset += off
}

func getLongRunOffset(c *cli.Context) int64 {
	params := c.App.Metadata[metadata].(*longRun)
	return params.offset
}
