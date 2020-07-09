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
	"sort"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmd/cli/config"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

const (
	cliName  = "ais"
	metadata = "md"
)

// Global config object
var cfg *config.Config

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
	if err := testAISURL(); err != nil {
		return err
	}

	if err := aisCLI.runOnce(input); err != nil {
		return aisCLI.handleCLIError(err)
	}

	if aisCLI.longRunParams.isInfiniteRun() {
		return aisCLI.runForever(input)
	}

	return aisCLI.runNTimes(input)
}

func (aisCLI *AISCLI) runOnce(input []string) error {
	return aisCLI.app.Run(input)
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

// Formats the error message to a nice string
func (aisCLI *AISCLI) handleCLIError(err error) error {
	switch err := err.(type) {
	case *cmn.HTTPError:
		return errors.New(cmn.CapitalizeString(err.Message))
	case *usageError:
		return err
	case *additionalInfoError:
		err.baseErr = aisCLI.handleCLIError(err.baseErr)
		return err
	default:
		return errors.New(cmn.CapitalizeString(err.Error()))
	}
}

func (aisCLI *AISCLI) init(build, version string) {
	app := aisCLI.app

	app.Name = cliName
	app.Usage = "AIS CLI: command-line management utility for AIStore(tm)"
	app.Version = fmt.Sprintf("%s (build %s)", version, build)
	app.EnableBashCompletion = true
	app.HideHelp = true
	app.Flags = []cli.Flag{cli.HelpFlag}
	app.CommandNotFound = commandNotFoundHandler
	app.OnUsageError = incorrectUsageHandler
	app.Metadata = map[string]interface{}{metadata: aisCLI.longRunParams}
	app.Writer = aisCLI.outWriter
	app.ErrWriter = aisCLI.errWriter

	cli.VersionFlag = cli.BoolFlag{
		Name:  "version, V",
		Usage: "print only the version",
	}

	aisCLI.setupCommands()
}

func (aisCLI *AISCLI) setupCommands() {
	app := aisCLI.app

	app.Commands = append(app.Commands, dSortCmds...)
	app.Commands = append(app.Commands, helpCommand)
	app.Commands = append(app.Commands, authCmds...)
	app.Commands = append(app.Commands, listCmds...)
	app.Commands = append(app.Commands, createCmds...)
	app.Commands = append(app.Commands, renameCmds...)
	app.Commands = append(app.Commands, removeCmds...)
	app.Commands = append(app.Commands, copyCmds...)
	app.Commands = append(app.Commands, setCmds...)
	app.Commands = append(app.Commands, attachCmds...)
	app.Commands = append(app.Commands, detachCmds...)
	app.Commands = append(app.Commands, controlCmds...)
	app.Commands = append(app.Commands, cluSpecificCmds...)
	app.Commands = append(app.Commands, showCmds...)
	app.Commands = append(app.Commands, waitCmds...)
	app.Commands = append(app.Commands, objectSpecificCmds...)
	app.Commands = append(app.Commands, transformCmds...)
	sort.Sort(cli.CommandsByName(app.Commands))

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
	Usage:     "show a list of commands or help for one command",
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

// Checks if URL is valid by trying to get Smap
func testAISURL() (err error) {
	_, err = api.GetClusterMap(defaultAPIParams)
	if err != nil {
		return fmt.Errorf("AIStore proxy unreachable at %s. You may need to update URL in %s",
			clusterURL, config.Location())
	}
	return
}
