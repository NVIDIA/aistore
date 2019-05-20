// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

type AISCLI struct {
	app *cli.App
}

const (
	cliName       = "ais"
	commandList   = "list"
	commandRename = "rename"
	commandEvict  = "evict"

	invalidCmdMsg    = "invalid command name '%s'"
	invalidDaemonMsg = "%s is not a valid DAEMON_ID"

	countDefault = 1

	refreshRateDefault = 1 * time.Second
	refreshRateMin     = 500 * time.Millisecond

	durationParseErrorFmt = "could not convert %q to time duration: %v"
)

var (
	// Common Flags
	refreshFlag = cli.StringFlag{Name: "refresh", Usage: "refresh period", Value: refreshRateDefault.String()}
	countFlag   = cli.IntFlag{Name: "count", Usage: "total number of generated reports", Value: countDefault}

	jsonFlag     = cli.BoolFlag{Name: "json,j", Usage: "json input/output"}
	verboseFlag  = cli.BoolFlag{Name: "verbose,v", Usage: "verbose"}
	checksumFlag = cli.BoolFlag{Name: cmn.GetPropsChecksum, Usage: "validate checksum"}
	waitFlag     = cli.BoolTFlag{Name: "wait", Usage: "wait for operation to finish before returning response"}

	bucketFlag      = cli.StringFlag{Name: cmn.URLParamBucket, Usage: "bucket where the objects are stored, eg. 'imagenet'"}
	bckProviderFlag = cli.StringFlag{Name: "provider",
		Usage: "determines which bucket ('local' or 'cloud') should be used. By default, locality is determined automatically"}
	regexFlag    = cli.StringFlag{Name: cmn.URLParamRegex, Usage: "regex pattern for matching"}
	noHeaderFlag = cli.BoolFlag{Name: "no-headers,H", Usage: "display tables without headers"}
)

var AISHelpTemplate = `DESCRIPTION:
  {{.Name}}{{if .Usage}} - {{.Usage}}{{end}} 
  Ver. {{if .Version}}{{if not .HideVersion}}{{.Version}}{{end}}{{end}}
  {{if .Description}}{{.Description}}{{end}}{{if len .Authors}}

USAGE:
{{if .UsageText}}{{.UsageText}}{{else}}{{.HelpName}} {{if .VisibleFlags}}[global options]{{end}}{{if .Commands}} command [command options]{{end}} {{if .ArgsUsage}}{{.ArgsUsage}}{{else}}[arguments...]{{end}}{{end}}

AUTHOR{{with $length := len .Authors}}{{if ne 1 $length}}S{{end}}{{end}}:
{{range $index, $author := .Authors}}{{if $index}}
{{end}}{{$author}}{{end}}{{end}}{{if .VisibleCommands}}

COMMANDS:{{range .VisibleCategories}}{{if .Name}}
{{.Name}}:{{end}}{{range .VisibleCommands}}
  {{join .Names ", "}}{{"\t"}}{{.Usage}}{{end}}{{end}}{{end}}{{if .VisibleFlags}}

GLOBAL OPTIONS:
{{range $index, $option := .VisibleFlags}}{{if $index}}
{{end}}{{$option}}{{end}}{{end}}{{if .Copyright}}

COPYRIGHT:
{{.Copyright}}{{end}}
`

// This is a copy-paste from urfave/cli/help.go. It is done to remove the 'h' alias of the 'help' command
var helpCommand = cli.Command{
	Name:      "help",
	Usage:     "shows a list of commands or help for one command",
	ArgsUsage: "[command]",
	Action: func(c *cli.Context) error {
		args := c.Args()
		if args.Present() {
			return cli.ShowCommandHelp(c, args.First())
		}

		cli.ShowAppHelp(c)
		return nil
	},
}

func New(build, version string) AISCLI {
	aisCLI := AISCLI{app: cli.NewApp()}
	aisCLI.Init(build, version)
	return aisCLI
}

func (aisCLI AISCLI) Init(build, version string) {
	app := aisCLI.app

	app.Name = cliName
	app.Usage = "AIS CLI: command-line management utility for AIStore(tm)"
	app.Version = fmt.Sprintf("%s (build %s)", version, build)
	app.EnableBashCompletion = true
	app.HideHelp = true
	app.Flags = []cli.Flag{cli.HelpFlag}
	cli.VersionFlag = cli.BoolFlag{
		Name:  "version, V",
		Usage: "print only the version",
	}
	cli.AppHelpTemplate = AISHelpTemplate
	app.Description = `
	The CLI has shell autocomplete functionality. To enable this feature, either source
	the 'ais_autocomplete' file in the project's 'cli/' directory or, for a permanent option, copy 
	the 'ais_autocomplete' file to the '/etc/bash_completion.d/ais' directory.`

	aisCLI.setupCommands()
}

func (aisCLI AISCLI) setupCommands() {
	app := aisCLI.app

	app.Commands = append(app.Commands, downloaderCmds...)
	app.Commands = append(app.Commands, dSortCmds...)
	app.Commands = append(app.Commands, objectCmds...)
	app.Commands = append(app.Commands, bucketCmds...)
	app.Commands = append(app.Commands, daeCluCmds...)
	app.Commands = append(app.Commands, configCmds...)
	app.Commands = append(app.Commands, xactCmds...)
	app.Commands = append(app.Commands, helpCommand)
	sort.Sort(cli.CommandsByName(app.Commands))

	setupCommandHelp(app.Commands)
}

func setupCommandHelp(commands []cli.Command) {
	for i := range commands {
		command := &commands[i]

		// Get rid of 'h'/'help' subcommands
		command.HideHelp = true
		// Need to set up the help flag manually when setting HideHelp = true
		command.Flags = append(command.Flags, cli.HelpFlag)

		setupCommandHelp(command.Subcommands)
	}
}

func (aisCLI AISCLI) Run(input []string) error {
	if err := aisCLI.runOnce(input); err != nil {
		// If the command failed, check if it failed because AIS is unreachable
		if err := TestAISURL(); err != nil {
			return err
		}
		return err
	}

	rate, err := time.ParseDuration(refreshRate)
	if err != nil {
		return fmt.Errorf(durationParseErrorFmt, refreshRate, err)
	}

	if count == Infinity {
		return aisCLI.runForever(input, rate)
	}

	return aisCLI.runNTimes(input, rate)
}

func (aisCLI AISCLI) runOnce(input []string) error {
	if err := aisCLI.app.Run(input); err != nil {
		return handleCLIError(err)
	}

	return nil
}

func (aisCLI AISCLI) runForever(input []string, rate time.Duration) error {
	for {
		time.Sleep(rate)

		fmt.Println()
		if err := aisCLI.runOnce(input); err != nil {
			return err
		}
	}
}

func (aisCLI AISCLI) runNTimes(input []string, rate time.Duration) error {
	// Be careful - count is a global variable set by aisCLI.app.Run(), so a new variable is needed here
	for runCount := count - 1; runCount > 0; runCount-- {
		time.Sleep(rate)

		fmt.Println()
		if err := aisCLI.runOnce(input); err != nil {
			return err
		}
	}
	return nil
}

// Formats the error message from HTTPErrors (see http.go)
func handleCLIError(err error) error {
	switch err := err.(type) {
	case *cmn.HTTPError:
		return fmt.Errorf("%s", err.Message)
	default:
		return err
	}
}

func flagIsSet(c *cli.Context, flag cli.Flag) bool {
	// If the flag name has multiple values, take first one
	flagName := cleanFlag(flag.GetName())
	return c.GlobalIsSet(flagName) || c.IsSet(flagName)
}

// Returns the value of a string flag (either parent or local scope)
func parseStrFlag(c *cli.Context, flag cli.Flag) string {
	flagName := cleanFlag(flag.GetName())
	if c.GlobalIsSet(flagName) {
		return c.GlobalString(flagName)
	}
	return c.String(flagName)
}

// Returns the value of an int flag (either parent or local scope)
func parseIntFlag(c *cli.Context, flag cli.Flag) int {
	flagName := cleanFlag(flag.GetName())
	if c.GlobalIsSet(flagName) {
		return c.GlobalInt(flagName)
	}
	return c.Int(flagName)
}

func parseByteFlagToInt(c *cli.Context, flag cli.Flag) (int64, error) {
	flagValue := parseStrFlag(c, flag)
	b, err := cmn.S2B(flagValue)
	if err != nil {
		return 0, fmt.Errorf("%s (%s) is invalid, expected either a number or a number with a size suffix (kb, MB, GiB, ...)", flag.GetName(), flagValue)
	}

	return b, nil
}

func checkFlags(c *cli.Context, flag ...cli.Flag) error {
	missingFlags := make([]string, 0)

	for _, f := range flag {
		if !flagIsSet(c, f) {
			missingFlags = append(missingFlags, f.GetName())
		}
	}

	missingFlagCount := len(missingFlags)

	if missingFlagCount == 1 {
		return fmt.Errorf("required flag %q is not set", missingFlags[0])
	}
	if missingFlagCount > 1 {
		return fmt.Errorf("required flags %q are not set", strings.Join(missingFlags, ", "))
	}

	return nil
}

func calcRefreshRate(c *cli.Context) (time.Duration, error) {
	refreshRate := refreshRateDefault

	if flagIsSet(c, refreshFlag) {
		flagStr := parseStrFlag(c, refreshFlag)
		flagDuration, err := time.ParseDuration(flagStr)
		if err != nil {
			return 0, fmt.Errorf(durationParseErrorFmt, flagStr, err)
		}

		refreshRate = flagDuration
		if refreshRate < refreshRateMin {
			refreshRate = refreshRateMin
		}
	}

	return refreshRate, nil
}

func chooseTmpl(tmplShort, tmplLong string, useShort bool) string {
	if useShort {
		return tmplShort
	}
	return tmplLong
}
