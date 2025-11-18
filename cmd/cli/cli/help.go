// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"text/template"

	"github.com/NVIDIA/aistore/cmn/cos"

	"github.com/urfave/cli"
)

// custom cli.AppHelpTemplate
const (
	// plain
	appHelpTemplate = `NAME:
   {{.Name}}{{if .Usage}} - {{.Usage}}{{end}}

USAGE:
   {{if .UsageText}}{{.UsageText}}{{else}}{{.HelpName}} {{if .VisibleFlags}}[global options]{{end}} {{if .ArgsUsage}}{{.ArgsUsage}}{{else}}[arguments...]{{end}} {{if .Commands}} command [command options]{{end}} {{end}} {{if .Version}}{{if not .HideVersion}}

VERSION:
   {{.Version}}{{end}}{{end}}{{if .Description}}

TAB completions (Bash and Zsh):
   {{.Description}}{{end}}{{if len .Authors}}

AUTHOR{{with $length := len .Authors}}{{if ne 1 $length}}S{{end}}{{end}}:
   {{range $index, $author := .Authors}}{{if $index}}
   {{end}}{{$author}}{{end}}{{end}}{{if .VisibleCommands}}

COMMANDS:{{range .VisibleCategories}}{{if .Name}}

   {{.Name}}:{{range .VisibleCommands}}
     {{join .Names ", "}}{{"\t"}}{{.Usage}}{{end}}{{else}}{{range .VisibleCommands}}
   {{join .Names ", "}}{{"\t"}}{{.Usage}}{{end}}{{end}}{{end}}{{end}}{{if .VisibleFlags}}

GLOBAL OPTIONS:
   {{range $index, $option := .VisibleFlags}}{{if $index}}
   {{end}}{{$option}}{{end}}{{end}}{{if .Copyright}}

COPYRIGHT:
   {{.Copyright}}{{end}}
`

	// colored
	appColoredHelpTemplate = `NAME:
   {{.Name}}{{if .Usage}} - {{.Usage}}{{end}}

USAGE:
   {{if .UsageText}}{{.UsageText}}{{else}}{{.HelpName}} {{if .VisibleFlags}}[global options]{{end}} {{if .ArgsUsage}}{{.ArgsUsage}}{{else}}[arguments...]{{end}} {{if .Commands}} command [command options]{{end}} {{end}} {{if .Version}}{{if not .HideVersion}}

VERSION:
   {{.Version}}{{end}}{{end}}{{if .Description}}

TAB completions (Bash and Zsh):
   {{.Description}}{{end}}{{if len .Authors}}

AUTHOR{{with $length := len .Authors}}{{if ne 1 $length}}S{{end}}{{end}}:
   {{range $index, $author := .Authors}}{{if $index}}
   {{end}}{{$author}}{{end}}{{end}}{{if .VisibleCommands}}

COMMANDS:{{range .VisibleCategories}}{{if .Name}}

   {{.Name}}:{{range .VisibleCommands}}
     {{colorJoin .Names}}{{"\t"}}{{.Usage}}{{end}}{{else}}{{range .VisibleCommands}}
   {{colorJoin .Names}}{{"\t"}}{{.Usage}}{{end}}{{end}}{{end}}{{end}}{{if .VisibleFlags}}

GLOBAL OPTIONS:
   {{range $index, $option := .VisibleFlags}}{{if $index}}
   {{end}}{{$option}}{{end}}{{end}}{{if .Copyright}}

COPYRIGHT:
   {{.Copyright}}{{end}}
`
)

// custom cli.CommandHelpTemplate:
const (
	// plain
	commandHelpTemplate = `NAME:
   {{.HelpName}} - {{.Usage}}

USAGE:
   {{if .UsageText}}{{.UsageText}}{{else}}{{.HelpName}} {{if .ArgsUsage}}{{.ArgsUsage}}{{else}}[arguments...]{{end}}{{if .VisibleFlags}} [command options]{{end}}{{end}}{{if .Category}}

CATEGORY:
   {{.Category}}{{end}}{{if .Description}}

DESCRIPTION:
   {{.Description}}{{end}}{{if .VisibleFlags}}

OPTIONS:
   {{range .VisibleFlags}}{{.}}
   {{end}}{{end}}
`

	// colored
	commandColoredHelpTemplate = `NAME:
   {{colorStr .HelpName}} - {{.Usage}}

USAGE:
   {{if .UsageText}}{{.UsageText}}{{else}}{{.HelpName}} {{if .ArgsUsage}}{{.ArgsUsage}}{{else}}[arguments...]{{end}}{{if .VisibleFlags}} [command options]{{end}}{{end}}{{if .Category}}

CATEGORY:
   {{.Category}}{{end}}{{if .Description}}

DESCRIPTION:
   {{.Description}}{{end}}{{if .VisibleFlags}}

OPTIONS:
   {{range .VisibleFlags}}{{ colorFlag . }}
   {{end}}{{end}}
`
)

// custom cli.SubcommandHelpTemplate:
const (
	// plain
	subcommandHelpTemplate = `NAME:
   {{.HelpName}} - {{if .Description}}{{.Description}}{{else}}{{.Usage}}{{end}}

USAGE:
   {{if .UsageText}}{{.UsageText}}{{else}}{{.HelpName}} command {{if .ArgsUsage}}{{.ArgsUsage}}{{else}}[arguments...]{{end}} {{if .VisibleFlags}} [command options]{{end}}{{end}}

COMMANDS:{{range .VisibleCategories}}{{if .Name}}

   {{.Name}}:{{range .VisibleCommands}}
     {{join .Names ", "}}{{"\t"}}{{.Usage}}{{end}}{{else}}{{range .VisibleCommands}}
   {{join .Names ", "}}{{"\t"}}{{.Usage}}{{end}}{{end}}{{end}}{{if .VisibleFlags}}

OPTIONS:
   {{range .VisibleFlags}}{{.}}
   {{end}}{{end}}
`

	// colored
	subcommandColoredHelpTemplate = `NAME:
   {{colorStr .HelpName}} - {{if .Description}}{{.Description}}{{else}}{{.Usage}}{{end}}

USAGE:
   {{if .UsageText}}{{.UsageText}}{{else}}{{.HelpName}} command {{if .ArgsUsage}}{{.ArgsUsage}}{{else}}[arguments...]{{end}} {{if .VisibleFlags}} [command options]{{end}}{{end}}

COMMANDS:{{range .VisibleCategories}}{{if .Name}}

   {{.Name}}:{{range .VisibleCommands}}
     {{join .Names ", "}}{{"\t"}}{{colorLine .Usage}}{{end}}{{else}}{{range .VisibleCommands}}
   {{join .Names ", "}}{{"\t"}}{{colorLine .Usage}}{{end}}{{end}}{{end}}{{if .VisibleFlags}}

OPTIONS:
   {{range .VisibleFlags}}{{.}}
   {{end}}{{end}}
`
)

var (
	// `ais help [COMMAND]`
	helpCommand = cli.Command{
		Name:      "help",
		Usage:     "Show a list of commands; show help for a given command",
		ArgsUsage: "[COMMAND]",
		Action:    helpCmdHandler,
		BashComplete: func(c *cli.Context) {
			for _, cmd := range c.App.Commands {
				fmt.Println(cmd.Name)
			}
		},
	}

	// colored help via helpPrinter override
	funcColorMap = template.FuncMap{
		"join":      strings.Join,
		"colorJoin": colorJoin,
		"colorLine": colorLine,
		"colorFlag": colorFlag,
		"colorStr":  colorStr,
	}
	errHelpFuncMap = template.FuncMap{
		"FlagName": func(f cli.Flag) string { return strings.SplitN(f.GetName(), ",", 2)[0] },
		"Mod":      func(a, mod int) int { return a % mod },
		// copy funcColorMap
		"join":      strings.Join,
		"colorJoin": colorJoin,
		"colorLine": colorLine,
		"colorFlag": colorFlag,
		"colorStr":  colorStr,
	}
)

func helpCmdHandler(c *cli.Context) error {
	args := c.Args()
	if args.Present() {
		return cli.ShowCommandHelp(c, args.First())
	}
	return cli.ShowAppHelp(c)
}

// paginate long helps using `more` and, possibly,
// color printout via `funcColorMap`
func helpMorePrinter(_ io.Writer, templ string, data any) {
	buffer := bytes.NewBuffer(make([]byte, cos.KiB<<2))
	buffer.Reset()

	r, w, err := os.Pipe()
	if err != nil {
		exitln("os.pipe:", err)
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		if _, err := io.Copy(buffer, r); err != nil {
			exitln("write sgl:", err)
		}
		r.Close()
	})

	cli.HelpPrinterCustom(w, templ, data, funcColorMap)

	w.Close()
	wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, "more")
	cmd.Stdin = buffer
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		exitln("cmd more:", err)
	}
}

func helpErrMessage(template string, data any) string {
	buffer := bytes.NewBuffer(make([]byte, cos.KiB<<2))
	buffer.Reset()
	w := bufio.NewWriter(buffer)

	cli.HelpPrinterCustom(w, template, data, errHelpFuncMap)
	_ = w.Flush()

	return buffer.String()
}

func getFlagUsage(flag cli.Flag) string {
	v := reflect.ValueOf(flag)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	usageFld := v.FieldByName("Usage")
	if usageFld.IsValid() && usageFld.Kind() == reflect.String {
		return usageFld.String()
	}
	return ""
}

// via "colorFlag"
func colorFlag(fl cli.Flag) string {
	usage := getFlagUsage(fl)
	if usage == "" {
		return fl.String()
	}
	i := strings.IndexByte(usage, '\n')
	if i <= 0 {
		return fl.GetName() + "\t" + fgreen(usage)
	}
	return fl.GetName() + "\t" + fgreen(usage[:i]) + usage[i:]
}

// via "colorStr"
func colorStr(s string) string {
	return fgreen(s)
}

// via "colorJoin"
func colorJoin(names []string) string {
	return fgreen(strings.Join(names, ", "))
}

// via "colorLine"
func colorLine(s string) string {
	i := strings.IndexByte(s, '\n')
	if i <= 0 {
		return fgreen(s)
	}
	return fgreen(s[:i]) + s[i:]
}
