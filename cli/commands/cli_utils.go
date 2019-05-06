// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

type AISCLI struct {
	*cli.App
}

const (
	cliName          = "ais"
	commandList      = "list"
	commandRename    = "rename"
	commandProps     = "props"
	commandEvict     = "evict"
	paramBckProvider = "bucket-provider"

	invalidCmdMsg    = "invalid command name '%s'"
	invalidDaemonMsg = "%s is not a valid DAEMON_ID"

	refreshRateUnit    = time.Millisecond
	refreshRateDefault = 1000
	refreshRateMin     = 500

	countDefault = 1
)

var (
	// Common Flags
	refreshFlag = cli.StringFlag{Name: "refresh", Usage: "refresh period", Value: "5s"}
	countFlag   = cli.IntFlag{Name: "count", Usage: "total number of generated reports", Value: countDefault}

	jsonFlag     = cli.BoolFlag{Name: "json,j", Usage: "json input/output"}
	verboseFlag  = cli.BoolFlag{Name: "verbose,v", Usage: "verbose"}
	checksumFlag = cli.BoolFlag{Name: cmn.GetPropsChecksum, Usage: "validate checksum"}
	propsFlag    = cli.BoolFlag{Name: commandProps, Usage: "properties of resource (object, bucket)"}
	waitFlag     = cli.BoolTFlag{Name: "wait", Usage: "wait for operation to finish before returning response"}

	bucketFlag      = cli.StringFlag{Name: cmn.URLParamBucket, Usage: "bucket where the objects are saved to, eg. 'imagenet'"}
	bckProviderFlag = cli.StringFlag{Name: paramBckProvider,
		Usage: "determines which bucket ('local' or 'cloud') should be used. By default, locality is determined automatically"}
	regexFlag = cli.StringFlag{Name: cmn.URLParamRegex, Usage: "regex pattern for matching"}
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

func New(build, version string) AISCLI {
	aisCLI := AISCLI{cli.NewApp()}
	aisCLI.Init(build, version)
	return aisCLI
}

func (aisCLI AISCLI) Init(build, version string) {
	aisCLI.Name = cliName
	aisCLI.Usage = "CLI for AIStore"
	aisCLI.Version = fmt.Sprintf("%s (build %s)", version, build)
	aisCLI.EnableBashCompletion = true
	cli.VersionFlag = cli.BoolFlag{
		Name:  "version, V",
		Usage: "print only the version",
	}
	cli.AppHelpTemplate = AISHelpTemplate
	aisCLI.Description = `
	The CLI has shell autocomplete functionality. To enable this feature, either source
	the 'ais_autocomplete' file in the project's 'cli/' directory or, for a permanent option, copy 
	the 'ais_autocomplete' file to the '/etc/bash_completion.d/ais' directory.`
}

func (aisCLI AISCLI) RunLong(input []string) error {
	if err := aisCLI.Run(input); err != nil {
		return err
	}

	rate, err := time.ParseDuration(refreshRate)
	if err != nil {
		return fmt.Errorf("could not convert %q to time duration: %v", refreshRate, err)
	}

	if count == Infinity {
		return runForever(aisCLI, input, rate)
	}

	return runNTimes(aisCLI, input, rate)
}

func runForever(aisCLI AISCLI, input []string, rate time.Duration) error {
	for {
		time.Sleep(rate)

		fmt.Println()
		if err := aisCLI.Run(input); err != nil {
			return err
		}
	}
}

func runNTimes(aisCLI AISCLI, input []string, rate time.Duration) error {
	// Be careful - count is a global variable set by aisCLI.Run(), so a new variable is needed here
	for runCount := count - 1; runCount > 0; runCount-- {
		time.Sleep(rate)

		fmt.Println()
		if err := aisCLI.Run(input); err != nil {
			return err
		}
	}

	return nil
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

func checkFlags(c *cli.Context, flag ...cli.Flag) error {
	for _, f := range flag {
		if !flagIsSet(c, f) {
			return fmt.Errorf("`%s` flag is not set", f.GetName())
		}
	}
	return nil
}

func calcRefreshRate(c *cli.Context) time.Duration {
	refreshRate := refreshRateDefault

	if flagIsSet(c, refreshRateFlag) {
		if flagVal := c.Int(refreshRateFlag.Name); flagVal > 0 {
			refreshRate = cmn.Max(flagVal, refreshRateMin)
		}
	}

	return time.Duration(refreshRate) * refreshRateUnit
}
