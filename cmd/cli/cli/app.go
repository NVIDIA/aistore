// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cmd/cli/config"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/fatih/color"
	"github.com/urfave/cli"
)

const (
	cliName  = "ais"
	ua       = "ais/cli"
	metadata = "md"
	cliDescr = `If <TAB-TAB> completion doesn't work:
   * download ` + cmn.GitHubHome + `/tree/master/cmd/cli/autocomplete
   * and run 'install.sh'.
   To install CLI directly from GitHub: ` + cmn.GitHubHome + `/blob/master/deploy/scripts/install_from_binaries.sh`
)

const (
	warnRemAisOffline = `remote ais cluster %s is crrrently unreachable.
Run 'ais config cluster backend.conf --json' - to show the respective configuration;
    'ais config cluster backend.conf <new JSON formatted value>' - to reconfigure or remove.
For details and usage examples, see: docs/cli/config.md`
)

const (
	// currently _required_ - e.g., `ais://mmm` cannot be reduced to `mmm`
	// in the future, we may fully support cfg.DefaultProvider but not yet
	// * see also: cmd/cli/config/config.go
	providerRequired = true
)

type (
	acli struct {
		app       *cli.App
		outWriter io.Writer
		errWriter io.Writer
		longRun   *longRun
	}
	longRun struct {
		count            int
		footer           int
		refreshRate      time.Duration
		offset           int64
		mapBegin, mapEnd teb.StstMap
	}
)

var (
	cfg         *config.Config
	buildTime   string
	k8sDetected bool
)

// color
var (
	fred, fcyan, fblue, fgreen func(a ...any) string
)

// `ais help [COMMAND]`
var helpCommand = cli.Command{
	Name:      "help",
	Usage:     "show a list of commands; show help for a given command",
	ArgsUsage: "[COMMAND]",
	Action:    helpCmdHandler,
	BashComplete: func(c *cli.Context) {
		for _, cmd := range c.App.Commands {
			fmt.Println(cmd.Name)
		}
	},
}

func verbose() bool { return cfg.Verbose } // more warnings, errors with backtraces and details

func helpCmdHandler(c *cli.Context) error {
	args := c.Args()
	if args.Present() {
		return cli.ShowCommandHelp(c, args.First())
	}

	cli.ShowAppHelp(c)
	return nil
}

// main method
func Run(version, buildtime string, args []string) error {
	a := acli{app: cli.NewApp(), outWriter: os.Stdout, errWriter: os.Stderr, longRun: &longRun{}}
	buildTime = buildtime
	a.init(version)

	teb.Init(os.Stdout, cfg.NoColor)

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
	return formatErr(err)
}

func (a *acli) runForever(args []string) error {
	rate := a.longRun.refreshRate
	for {
		time.Sleep(rate)
		printLongRunFooter(a.outWriter, a.longRun.footer)
		if err := a.runOnce(args); err != nil {
			return err
		}
		a.longRun.mapBegin = a.longRun.mapEnd
		a.longRun.mapEnd = nil
	}
}

func printLongRunFooter(w io.Writer, repeat int) {
	if repeat > 0 {
		fmt.Fprintln(w, fcyan(strings.Repeat("-", repeat)))
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

func (a *acli) init(version string) {
	app := a.app

	if cfg.NoColor {
		fcyan = fmt.Sprint
		fred = fmt.Sprint
		fblue = fmt.Sprint
		fgreen = fmt.Sprint
	} else {
		fcyan = color.New(color.FgHiCyan).SprintFunc()
		fred = color.New(color.FgHiRed).SprintFunc()
		fblue = color.New(color.FgHiBlue).SprintFunc()
		fgreen = color.New(color.FgHiGreen).SprintFunc()
	}

	app.Name = cliName
	app.Usage = "AIS CLI: command-line management utility for AIStore"
	app.Version = version
	app.EnableBashCompletion = true
	app.HideHelp = true
	app.Flags = []cli.Flag{cli.HelpFlag}
	app.CommandNotFound = commandNotFoundHandler
	app.OnUsageError = onUsageErrorHandler
	app.Metadata = map[string]any{metadata: a.longRun}
	app.Writer = a.outWriter
	app.ErrWriter = a.errWriter
	app.Description = cliDescr

	a.setupCommands()
}

func (a *acli) setupCommands() {
	app := a.app

	// Note: order of commands below is the order shown in "ais help"
	appendJobSub(&jobCmd)
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
		perfCmd,
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
	lst := splitCsv(cli.HelpFlag.GetName())
	helpName := lst[0]
	for i := range commands {
		command := &commands[i]

		// Get rid of 'h'/'help' subcommands
		// and add the help flag manually
		command.HideHelp = true
		// (but only if there isn't one already)
		if !hasHelpFlag(command.Flags, helpName) {
			command.Flags = append(command.Flags, cli.HelpFlag)
		}
		command.OnUsageError = onUsageErrorHandler

		// recursively
		setupCommandHelp(command.Subcommands)
	}
}

func hasHelpFlag(commandFlags []cli.Flag, helpName string) bool {
	for _, flag := range commandFlags {
		lst := splitCsv(flag.GetName())
		for _, name := range lst {
			if name == helpName {
				return true
			}
		}
	}
	return false
}

//
// cli.App error callbacks
//

func commandNotFoundHandler(c *cli.Context, cmd string) {
	if cmd == "version" {
		fmt.Fprintf(c.App.Writer, "version %s (build %s)\n", c.App.Version, buildTime)
		return
	}
	err := commandNotFoundError(c, cmd)
	fmt.Fprint(c.App.ErrWriter, err.Error())
	os.Exit(1)
}

func onUsageErrorHandler(c *cli.Context, err error, _ bool) error {
	if c == nil {
		return err
	}
	return cannotExecuteError(c, err, "")
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

func (p *longRun) init(c *cli.Context, runOnce bool) {
	if flagIsSet(c, refreshFlag) {
		p.refreshRate = parseDurationFlag(c, refreshFlag)
		p.count = countUnlimited // unless counted (below)
	} else if runOnce {
		p.count = 1 // unless --count spec-ed (below)
	}
	if flagIsSet(c, countFlag) {
		p.count = parseIntFlag(c, countFlag)
		if p.count <= 0 {
			n := flprn(countFlag)
			warn := fmt.Sprintf("option '%s=%d' is invalid (must be >= 1). Proceeding with '%s=%d' (default).",
				n, p.count, n, countDefault)
			actionWarn(c, warn)
			p.count = countDefault
		}
	}
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
	params.init(c, false)
	return true
}

func getLongRunParams(c *cli.Context) *longRun {
	params := c.App.Metadata[metadata].(*longRun)
	if !params.isSet() {
		return nil
	}
	return params
}

func addLongRunOffset(c *cli.Context, off int64) {
	params := c.App.Metadata[metadata].(*longRun)
	params.offset += off
}

func getLongRunOffset(c *cli.Context) int64 {
	params := c.App.Metadata[metadata].(*longRun)
	return params.offset
}
