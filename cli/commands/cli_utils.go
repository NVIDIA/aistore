// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

type AISCLI struct {
	app *cli.App

	outWriter io.Writer
	errWriter io.Writer

	count   int
	refresh time.Duration
}

const (
	cliName = "ais"

	commandRename = "rename"
	commandEvict  = "evict"
	commandStart  = "start"
	commandStatus = "status"
	commandAbort  = "abort"
	commandRemove = "remove"
	commandList   = "ls"

	invalidCmdMsg    = "invalid command name '%s'"
	invalidDaemonMsg = "%s is not a valid DAEMON_ID"

	countDefault = 1

	refreshRateDefault = time.Second
	refreshRateMin     = 500 * time.Millisecond

	durationParseErrorFmt = "error converting refresh flag value %q to time duration: %v"

	aisBucketEnvVar         = "AIS_BUCKET"
	aisBucketProviderEnvVar = "AIS_BUCKET_PROVIDER"

	metadata = "md"

	idArgumentText                      = "ID"
	noArgumentsText                     = " "
	atLeastOneKeyValuePairArgumentsText = "KEY=VALUE [KEY=VALUE...]"
)

var (
	// Common Flags
	refreshFlag = cli.StringFlag{Name: "refresh", Usage: "refresh period", Value: refreshRateDefault.String()}
	countFlag   = cli.IntFlag{Name: "count", Usage: "total number of generated reports", Value: countDefault}

	jsonFlag     = cli.BoolFlag{Name: "json,j", Usage: "json input/output"}
	verboseFlag  = cli.BoolFlag{Name: "verbose,v", Usage: "verbose"}
	checksumFlag = cli.BoolFlag{Name: cmn.GetPropsChecksum, Usage: "validate checksum"}
	waitFlag     = cli.BoolTFlag{Name: "wait", Usage: "wait for operation to finish before returning response"}

	bucketFlag      = cli.StringFlag{Name: cmn.URLParamBucket, Usage: "bucket where the objects are stored, eg. 'imagenet'", EnvVar: aisBucketEnvVar}
	bckProviderFlag = cli.StringFlag{Name: "provider",
		Usage:  "determines which bucket ('local' or 'cloud') should be used. By default, locality is determined automatically",
		EnvVar: aisBucketProviderEnvVar,
	}
	regexFlag    = cli.StringFlag{Name: cmn.URLParamRegex, Usage: "regex pattern for matching"}
	noHeaderFlag = cli.BoolFlag{Name: "no-headers,H", Usage: "display tables without headers"}
)

var AISHelpTemplate = `DESCRIPTION:
   {{ .Name }}{{ if .Usage }} - {{ .Usage }}{{end}}
   Ver. {{ if .Version }}{{ if not .HideVersion }}{{ .Version }}{{end}}{{end}}

USAGE:
   {{ .Name }} [GLOBAL OPTIONS] COMMAND

COMMANDS:{{ range .VisibleCategories }}{{ if .Name }}
   {{ .Name }}:{{end}}{{ range .VisibleCommands }}
     {{ join .Names ", " }}{{ "\t" }}{{ .Usage }}{{end}}{{end}}

GLOBAL OPTIONS:
   {{ range $index, $option := .VisibleFlags }}{{ if $index }}
   {{end}}{{ $option }}{{end}}
`

var AISCommandHelpTemplate = `NAME:
   {{.HelpName}} - {{.Usage}}

USAGE:
   {{if .UsageText}}{{.UsageText}}{{else}}{{.HelpName}}{{if .VisibleFlags}} [OPTIONS]{{end}} {{if .ArgsUsage}}{{.ArgsUsage}}{{else}}[arguments...]{{end}}{{end}}{{if .Description}}

DESCRIPTION:
   {{.Description}}{{end}}{{if .VisibleFlags}}

OPTIONS:
   {{range .VisibleFlags}}{{.}}
   {{end}}{{end}}
`

var AISSubcommandHelpTemplate = `NAME:
   {{.HelpName}} - {{if .Description}}{{.Description}}{{else}}{{.Usage}}{{end}}

USAGE:
   {{if .UsageText}}{{.UsageText}}{{else}}{{.HelpName}} COMMAND{{if .VisibleFlags}} [COMMAND OPTIONS]{{end}}{{end}}

COMMANDS:{{range .VisibleCategories}}{{if .Name}}
   {{.Name}}:{{end}}{{range .VisibleCommands}}
     {{join .Names ", "}}{{"\t"}}{{.Usage}}{{end}}
{{end}}{{if .VisibleFlags}}
OPTIONS:
   {{range .VisibleFlags}}{{.}}
   {{end}}{{end}}
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

func commandNotFoundHandler(c *cli.Context, cmd string) {
	err := commandNotFoundError(c, cmd)
	// The function has no return value (can't return an error), so it has to print the error here
	_, _ = fmt.Fprintf(c.App.ErrWriter, err.Error())
}

func incorrectUsageHandler(c *cli.Context, err error, _ bool) error {
	return incorrectUsageError(c, err)
}

func New(build, version string) *AISCLI {
	aisCLI := AISCLI{
		app:       cli.NewApp(),
		outWriter: os.Stdout,
		errWriter: os.Stderr,
		count:     countDefault,
		refresh:   refreshRateDefault,
	}
	aisCLI.Init(build, version)
	return &aisCLI
}

func (aisCLI *AISCLI) Init(build, version string) {
	app := aisCLI.app

	app.Name = cliName
	app.Usage = "AIS CLI: command-line management utility for AIStore(tm)"
	app.Version = fmt.Sprintf("%s (build %s)", version, build)
	app.EnableBashCompletion = true
	app.HideHelp = true
	app.Flags = []cli.Flag{cli.HelpFlag}
	app.CommandNotFound = commandNotFoundHandler
	app.OnUsageError = incorrectUsageHandler
	app.Metadata = map[string]interface{}{metadata: aisCLI}
	app.Writer = aisCLI.outWriter
	app.ErrWriter = aisCLI.errWriter

	cli.VersionFlag = cli.BoolFlag{
		Name:  "version, V",
		Usage: "print only the version",
	}
	cli.AppHelpTemplate = AISHelpTemplate
	cli.CommandHelpTemplate = AISCommandHelpTemplate
	cli.SubcommandHelpTemplate = AISSubcommandHelpTemplate

	aisCLI.setupCommands()
}

func (aisCLI *AISCLI) setupCommands() {
	app := aisCLI.app

	app.Commands = append(app.Commands, downloaderCmds...)
	app.Commands = append(app.Commands, dSortCmds...)
	app.Commands = append(app.Commands, objectCmds...)
	app.Commands = append(app.Commands, bucketCmds...)
	app.Commands = append(app.Commands, daeCluCmds...)
	app.Commands = append(app.Commands, configCmds...)
	app.Commands = append(app.Commands, xactCmds...)
	app.Commands = append(app.Commands, helpCommand)
	app.Commands = append(app.Commands, lruCmds...)
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

		command.OnUsageError = incorrectUsageHandler

		setupCommandHelp(command.Subcommands)
	}
}

func (aisCLI *AISCLI) Run(input []string) error {
	if err := aisCLI.runOnce(input); err != nil {
		// If the command failed, check if it failed because AIS is unreachable
		if err := TestAISURL(); err != nil {
			return err
		}
		return aisCLI.handleCLIError(err)
	}

	if aisCLI.count == Infinity {
		return aisCLI.runForever(input, aisCLI.refresh)
	}

	return aisCLI.runNTimes(input, aisCLI.count-1, aisCLI.refresh)
}

func (aisCLI *AISCLI) runOnce(input []string) error {
	if err := aisCLI.app.Run(input); err != nil {
		return err
	}

	return nil
}

func (aisCLI *AISCLI) runForever(input []string, rate time.Duration) error {
	for {
		time.Sleep(rate)

		_, _ = fmt.Fprintln(aisCLI.outWriter)
		if err := aisCLI.runOnce(input); err != nil {
			return err
		}
	}
}

func (aisCLI *AISCLI) runNTimes(input []string, n int, rate time.Duration) error {
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
		return errors.New(cmn.StrToSentence(err.Message))
	case *usageError:
		return err
	default:
		return errors.New(cmn.StrToSentence(err.Error()))
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

func checkFlags(c *cli.Context, flag []cli.Flag) error {
	missingFlags := make([]string, 0)

	for _, f := range flag {
		if !flagIsSet(c, f) {
			missingFlags = append(missingFlags, f.GetName())
		}
	}

	missingFlagCount := len(missingFlags)

	if missingFlagCount >= 1 {
		return missingFlagsError(c, missingFlags)
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
