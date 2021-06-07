// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cmd/cli/config"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/fatih/color"
	"github.com/urfave/cli"
)

const (
	cliName  = "ais"
	metadata = "md"
	cliDescr = `If [Tab] completion doesn't work:
   * download https://github.com/NVIDIA/aistore/tree/master/cmd/cli/autocomplete
   * and run 'install.sh'.
   For more information, please refer to https://github.com/NVIDIA/aistore/blob/master/cmd/cli/README.md`
)

var (
	// Global config object
	cfg              *config.Config
	unreachableRegex = regexp.MustCompile("dial.*(timeout|refused)")
	k8sDetected      bool
)

// AISCLI represents an instance of an AIS command line interface
type AISCLI struct {
	app *cli.App

	outWriter io.Writer
	errWriter io.Writer

	longRunParams *longRunParams
}

// New returns a new, initialized AISCLI instance
func New(build, version string) *AISCLI {
	aisCLI := AISCLI{
		app:           cli.NewApp(),
		outWriter:     os.Stdout,
		errWriter:     os.Stderr,
		longRunParams: defaultLongRunParams(),
	}
	aisCLI.init(build, version)
	return &aisCLI
}

// Run runs the CLI
func (aisCLI *AISCLI) Run(input []string) error {
	if err := aisCLI.runOnce(input); err != nil {
		return err
	}

	if aisCLI.longRunParams.isInfiniteRun() {
		return aisCLI.runForever(input)
	}

	return aisCLI.runNTimes(input)
}

func (aisCLI *AISCLI) runOnce(input []string) error {
	return aisCLI.handleCLIError(aisCLI.app.Run(input))
}

func (aisCLI *AISCLI) runForever(input []string) error {
	rate := aisCLI.longRunParams.refreshRate

	for {
		time.Sleep(rate)

		_, _ = fmt.Fprintln(aisCLI.outWriter)
		if err := aisCLI.runOnce(input); err != nil {
			return err
		}
	}
}

func (aisCLI *AISCLI) runNTimes(input []string) error {
	n := aisCLI.longRunParams.count - 1
	rate := aisCLI.longRunParams.refreshRate

	for ; n > 0; n-- {
		time.Sleep(rate)

		_, _ = fmt.Fprintln(aisCLI.outWriter)
		if err := aisCLI.runOnce(input); err != nil {
			return err
		}
	}
	return nil
}

func isUnreachableError(err error) bool {
	switch err := err.(type) {
	case *cmn.ErrHTTP:
		return cos.IsUnreachable(err, err.Status)
	case *errUsage, *errAdditionalInfo:
		return false
	default:
		return unreachableRegex.MatchString(err.Error())
	}
}

// Formats the error message to a nice string
func (aisCLI *AISCLI) handleCLIError(err error) error {
	if err == nil {
		return nil
	}

	var (
		red          = color.New(color.FgRed).SprintFunc()
		prepareError = func(msg string) error {
			msg = cos.CapitalizeString(msg)
			msg = strings.TrimRight(msg, "\n") // Remove newlines if any.
			return errors.New(red(msg))
		}
	)

	if isUnreachableError(err) {
		return fmt.Errorf(
			red("AIStore proxy unreachable at %s. You may need to update environment variable AIS_ENDPOINT"),
			clusterURL)
	}

	switch err := err.(type) {
	case *cmn.ErrHTTP:
		return prepareError(err.Message)
	case *errUsage:
		return err
	case *errAdditionalInfo:
		err.baseErr = aisCLI.handleCLIError(err.baseErr)
		return err
	default:
		return prepareError(err.Error())
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

func (aisCLI *AISCLI) init(build, version string) {
	app := aisCLI.app

	app.Name = cliName
	app.Usage = "AIS CLI: command-line management utility for AIStore(tm)"
	app.Version = fmt.Sprintf("%s (build %s)", version, build)
	app.EnableBashCompletion = true
	app.HideHelp = true
	app.Flags = []cli.Flag{cli.HelpFlag, noColorFlag}
	app.CommandNotFound = commandNotFoundHandler
	app.OnUsageError = incorrectUsageHandler
	app.Metadata = map[string]interface{}{metadata: aisCLI.longRunParams}
	app.Writer = aisCLI.outWriter
	app.ErrWriter = aisCLI.errWriter
	app.Before = onBeforeCommand // to disable colors if `no-colors' is set
	app.Description = cliDescr
	cli.VersionFlag = cli.BoolFlag{
		Name:  "version, V",
		Usage: "print only the version",
	}

	aisCLI.setupCommands()
}

func (aisCLI *AISCLI) setupCommands() {
	app := aisCLI.app

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
		aisCLI.getAliasCmd(),
	}

	if k8sDetected {
		app.Commands = append(app.Commands, k8sCmd)
	}
	app.Commands = append(app.Commands, aisCLI.initAliases()...)
	setupCommandHelp(app.Commands)
	aisCLI.enableSearch()
}

func (aisCLI *AISCLI) enableSearch() {
	app := aisCLI.app
	initSearch(app)
	app.Commands = append(app.Commands, searchCommands...)
}

func setupCommandHelp(commands []cli.Command) {
	for i := range commands {
		command := &commands[i]

		// Get rid of 'h'/'help' subcommands
		command.HideHelp = true
		// Need to set up the help flag manually when setting HideHelp = true
		command.Flags = append(command.Flags, cli.HelpFlag)

		command.OnUsageError = incorrectUsageHandler

		setupCommandHelp(command.Subcommands)
	}
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

func commandNotFoundHandler(c *cli.Context, cmd string) {
	err := commandNotFoundError(c, cmd)
	// The function has no return value (can't return an error), so it has to print the error here
	fmt.Fprint(c.App.ErrWriter, err.Error())
	os.Exit(1)
}

func incorrectUsageHandler(c *cli.Context, err error, _ bool) error {
	return incorrectUsageError(c, err)
}
