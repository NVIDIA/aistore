// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
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
)

var (
	// Common Flags
	watchFlag   = cli.BoolFlag{Name: "watch", Usage: "watch an action"}
	refreshFlag = cli.StringFlag{Name: "refresh", Usage: "refresh period", Value: "5s"}

	jsonFlag     = cli.BoolFlag{Name: "json,j", Usage: "json input/output"}
	verboseFlag  = cli.BoolFlag{Name: "verbose,v", Usage: "verbose"}
	checksumFlag = cli.BoolFlag{Name: cmn.GetPropsChecksum, Usage: "validate checksum"}
	propsFlag    = cli.BoolFlag{Name: commandProps, Usage: "properties of resource (object, bucket)"}
	waitFlag     = cli.BoolTFlag{Name: "wait", Usage: "wait for operation to finish before returning response"}

	bucketFlag      = cli.StringFlag{Name: cmn.URLParamBucket, Usage: "bucket where the objects are saved to, eg. 'imagenet'"}
	bckProviderFlag = cli.StringFlag{Name: paramBckProvider,
		Usage: "determines which bucket ('local' or 'cloud') should be used. By default, locality is determined automatically"}
	regexFlag = cli.StringFlag{Name: cmn.URLParamRegex, Usage: "regex pattern for matching"}

	clear map[string]func()
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

func init() {
	clear = make(map[string]func())
	clear["linux"] = func() {
		cmd := exec.Command("clear")
		cmd.Stdout = os.Stdout
		cmd.Run()
	}
	clear["windows"] = func() {
		cmd := exec.Command("cmd", "/c", "cls")
		cmd.Stdout = os.Stdout
		cmd.Run()
	}
}

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

func clearScreen() error {
	clearFunc, ok := clear[runtime.GOOS]
	if !ok {
		return fmt.Errorf("%s is not supported", runtime.GOOS)
	}
	clearFunc()
	return nil
}

func (aisCLI AISCLI) RunLong(input []string) error {
	if err := aisCLI.Run(input); err != nil {
		return err
	}

	rate, err := time.ParseDuration(refreshRate)
	if err != nil {
		return fmt.Errorf("Could not convert %q to time duration: %v", refreshRate, err)
	}

	for watch {
		time.Sleep(rate)
		if err := clearScreen(); err != nil {
			return err
		}
		fmt.Printf("Refreshing every %s (CTRL+C to stop): %s\n", refreshRate, input)
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

// Returns the value of flag (either parent or local scope)
func parseFlag(c *cli.Context, flag cli.Flag) string {
	flagName := cleanFlag(flag.GetName())
	if c.GlobalIsSet(flagName) {
		return c.GlobalString(flagName)
	}
	return c.String(flagName)
}

func checkFlags(c *cli.Context, flag ...cli.Flag) error {
	for _, f := range flag {
		if !flagIsSet(c, f) {
			return fmt.Errorf("`%s` flag is not set", f.GetName())
		}
	}
	return nil
}
