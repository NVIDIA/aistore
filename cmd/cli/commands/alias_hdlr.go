// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file provides aliases to frequently used commands that are inside other top level commands.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/cmd/cli/config"
	"github.com/urfave/cli"
)

func (aisCLI *AISCLI) getAliasCmd() cli.Command {
	return cli.Command{
		Name:      commandAlias,
		Usage:     "create top-level alias to a CLI command",
		ArgsUsage: aliasCmdArgument,
		Flags:     []cli.Flag{aliasFlag},
		Action:    aisCLI.aliasHandler,
	}
}

func (aisCLI *AISCLI) aliasHandler(c *cli.Context) (err error) {
	args := c.Args()
	orig := strings.Join(args, " ")
	alias := parseStrFlag(c, aliasFlag)
	if cmd := aisCLI.resolveCmd(args); cmd == nil {
		return fmt.Errorf("command %q not found", orig)
	}

	if len(strings.Split(alias, " ")) != 1 {
		return fmt.Errorf("multi-word aliases are not supported- please use only one word for your alias")
	}

	cfg.Aliases[alias] = orig
	if err := config.Save(cfg); err != nil {
		return err
	}

	fmt.Fprintf(c.App.Writer, "aliased %q=%q\n", alias, orig)
	return
}

// initAliases reads cfg.Aliases and returns all aliases.
func (aisCLI *AISCLI) initAliases() (aliasCmds []cli.Command) {
	for alias, orig := range cfg.Aliases {
		origArgs := strings.Split(orig, " ")
		cmd := aisCLI.resolveCmd(cli.Args(origArgs))

		if cmd != nil {
			aliasCmds = append(aliasCmds, makeAlias(*cmd, orig, false, alias))
		}
	}

	return
}

// resolveCmd() traverses the command tree and returns the cli.Command matching `args`
// similar to cli.App.Command(), but looking through subcommands as well.
func (aisCLI *AISCLI) resolveCmd(args cli.Args) *cli.Command {
	if !args.Present() {
		return nil
	}
	var (
		app      = aisCLI.app
		toplevel = args.First()
		tlCmd    = app.Command(toplevel)
	)
	if len(args) == 1 {
		return tlCmd
	}

	currCmd := tlCmd
	for _, input := range args.Tail() {
		consumed := false
		for i := range currCmd.Subcommands {
			c := &currCmd.Subcommands[i]
			if c.HasName(input) {
				currCmd = c
				consumed = true
				break
			}
		}
		if !consumed {
			return currCmd
		}
	}

	return currCmd
}

// makeAlias returns a copy of cmd with some changes:
// 1. command name is changed if provided.
// 2. category set to "ALIASES" if specified.
// 3. "alias for" message added to usage if not a silent alias.
func makeAlias(cmd cli.Command, aliasFor string, silentAlias bool, newName ...string) cli.Command {
	if len(newName) != 0 {
		cmd.Name = newName[0]
	}
	if !silentAlias {
		cmd.Category = "ALIASES"
		cmd.Usage = fmt.Sprintf("(alias for %q) %s", aliasFor, cmd.Usage)
	}

	// help is already added to the original, remove from cmd and all subcmds
	cmd.HideHelp = true
	if len(cmd.Subcommands) != 0 {
		aliasSubcmds := make([]cli.Command, len(cmd.Subcommands))
		for i := range cmd.Subcommands {
			aliasSubcmds[i] = makeAlias(cmd.Subcommands[i], "", true)
		}
		cmd.Subcommands = aliasSubcmds
	}

	return cmd
}
